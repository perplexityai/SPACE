//! Postgres-backed [`super::LostStartedSink`].

use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use pplx_space_events::{SandboxEventDetails, SandboxEventTrigger};
use pplx_space_node_registration_store::locks;
use pplx_space_sandbox_store::lost::{
    DEAD_UPLOAD_SETTLE, mark_started_lost, started_lost_candidates, write_off_lost_sandboxes,
};
use pplx_space_sandbox_store::row_locks;
use pplx_space_snapshot_store::lost::{
    fail_quiet_full_uploads_from_node, lock_creating_full_uploads,
};
use sqlx::PgPool;

use super::{LostCandidate, LostStartedSink, SourceNode};
use crate::gone_node_write_off::{emit_lost_and_failed, node_identity_holds};

pub struct LostStartedDbStore {
    pool: PgPool,
}

impl LostStartedDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl LostStartedSink for LostStartedDbStore {
    async fn list_candidates(
        &self,
        limit: u32,
        after: Option<SandboxId>,
    ) -> Result<Vec<LostCandidate>, sqlx::Error> {
        let rows = started_lost_candidates(&self.pool, after, limit, DEAD_UPLOAD_SETTLE).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let source_node = match (row.node_row_exists, row.k8s_node_name) {
                    (false, _) => SourceNode::RowDeleted,
                    (true, Some(k8s_node_name)) => SourceNode::Registered {
                        k8s_node_name,
                        k8s_node_uid: row.k8s_node_uid,
                    },
                    (true, None) => SourceNode::Unverifiable,
                };
                LostCandidate {
                    sandbox_id: row.sandbox_id,
                    generation: row.generation,
                    source_node_id: row.active_node_id,
                    source_node,
                }
            })
            .collect())
    }

    async fn mark_lost(&self, candidate: &LostCandidate) -> Result<bool, sqlx::Error> {
        let node_id = candidate.source_node_id;
        let mut tx = self.pool.begin().await?;

        // Re-check that the node is gone the way the scan found it, before
        // taking the sandbox lock: pod placement and the drained-node give-up
        // both lock the node rows before the sandbox row, so this order can't
        // deadlock against them.
        let gone = match &candidate.source_node {
            // `FOR SHARE` on a missing row locks nothing, so a registration
            // committing after this read isn't fenced. A row that reappears
            // before the read refuses the flip; one that reappears after it
            // can't make the flip wrong. `nodes.id` is derived from the node
            // name and role, so the row is a new instance under the old name,
            // and the sandbox's runtime and local disk went with the old one.
            SourceNode::RowDeleted => !locks::share_lock_node(&mut tx, node_id).await?,
            // Reached only after the tick found the node absent from
            // Kubernetes with no live EC2 instance. The row must still be
            // outside any drain and carry the uid the scan observed, so a
            // same-name replacement refuses.
            SourceNode::Registered { k8s_node_uid, .. } => {
                node_identity_holds(&mut tx, node_id, None, k8s_node_uid.as_deref()).await?
            }
            SourceNode::Unverifiable => false,
        };
        if !gone {
            tx.rollback().await?;
            return Ok(false);
        }

        // Started recovery's claim takes the sandbox row lock before it re-arms
        // the execution, so a wake either commits first (the flip's execution
        // check then refuses) or waits for this transaction, then finds the
        // row `failed` and claims nothing.
        let locked = row_locks::lock_lifecycle(&mut *tx, candidate.sandbox_id).await?;
        if locked.as_deref() != Some("started") {
            tx.rollback().await?;
            return Ok(false);
        }

        // Lock the sandbox's in-flight uploads before the flip evaluates its
        // restore-source condition. A finalize that committed first is visible
        // to the flip's statement snapshot and spares the sandbox. A finalize
        // racing this transaction updates its snapshot row before it locks the
        // sandbox row, the opposite order, so Postgres aborts one of the two.
        // An aborted flip is retried on a later tick. An aborted finalize from
        // the gone node isn't retried, so the flip lands and the uploaded
        // bytes are left for the storage sweeper: the residual the drained-node
        // give-up accepts for the same race.
        let _locked_uploads: Vec<SnapshotId> =
            lock_creating_full_uploads(&mut *tx, candidate.sandbox_id).await?;

        if !mark_started_lost(
            &mut tx,
            candidate.sandbox_id,
            node_id,
            candidate.generation,
            DEAD_UPLOAD_SETTLE,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(false);
        }
        fail_quiet_full_uploads_from_node(
            &mut *tx,
            node_id,
            &[candidate.sandbox_id],
            DEAD_UPLOAD_SETTLE,
        )
        .await?;
        write_off_lost_sandboxes(&mut tx, node_id, &[candidate.sandbox_id]).await?;
        tx.commit().await?;

        let details = SandboxEventDetails::default()
            .with_from_state("started")
            .with_to_state("failed")
            .source_node_id(node_id.to_string())
            .execution_generation(candidate.generation.to_string())
            .reason(candidate.source_node.gone_reason())
            .trigger(SandboxEventTrigger::System);
        emit_lost_and_failed(candidate.sandbox_id, details, "lost_started_sandboxes");
        Ok(true)
    }
}
