//! Postgres-backed [`super::LostStartedSink`].

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use std::sync::LazyLock;

use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    SandboxRestoreProvenance,
};
use pplx_space_sandbox_portability::started_restore_source_absent;
use pplx_space_sandbox_store::lost::{
    DEAD_UPLOAD_SETTLE, LostStartedRollback, mark_started_lost, roll_back_started_to_suspend,
    write_off_lost_sandboxes,
};
use pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::{LostCandidate, LostDisposition, LostStartedSink, SourceNode};
use crate::gone_node_write_off::{emit_lost_and_failed, node_identity_holds};
use crate::node_drain::store::retire_reverted_resident;

type CandidateRow = (SandboxId, i64, NodeId, bool, Option<String>, Option<String>);

/// `$1` = [`SubstrateKind::Kubernetes`], `$2` = scan cursor (nullable),
/// `$3` = limit, `$4` = [`DEAD_UPLOAD_SETTLE`] seconds. The candidate scan.
///
/// The execution must track no pod. A recovery that materialized a
/// replacement pod records its identity before the pod's placement moves
/// `active_node_id`, so a tracked pod may be live on another node; the
/// failed-recovery convergence likewise waits for an authoritative absence
/// check to clear it.
///
/// A sandbox whose source node is in any drain phase is left to `node_drain`
/// and `drained_node_terminator`, which own residents of a drained node. The
/// restore-source condition is the one the flip re-checks
/// ([`started_restore_source_absent`]).
static LIST_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
    let no_restore_source = started_restore_source_absent("s", "$4");
    sql!(
        format!(
            "SELECT s.id,
                    e.generation,
                    s.active_node_id,
                    n.id IS NOT NULL AS node_row_exists,
                    nk.k8s_node_name,
                    nk.k8s_node_uid
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.active
                AND e.state = 'recovery_failed'
                AND NULLIF(BTRIM(e.pod_uid), '') IS NULL
                AND e.pod_ip IS NULL
                AND e.k8s_node_name IS NULL
               LEFT JOIN nodes n ON n.id = s.active_node_id
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE s.managed_by = $1
                AND s.lifecycle = 'started'
                AND s.active_node_id IS NOT NULL
                AND (n.id IS NULL OR (n.managed_by = $1 AND n.drain_phase IS NULL))
                AND ($2::UUID IS NULL OR s.id > $2)
                AND {no_restore_source}
              ORDER BY s.id
              LIMIT $3"
        )
        .as_str()
    )
});

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
        let rows: Vec<CandidateRow> = sqlx::query_as(sqlx::AssertSqlSafe(LIST_CANDIDATES.as_str()))
            .bind(SubstrateKind::Kubernetes)
            .bind(after)
            .bind(i64::from(limit))
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(sandbox_id, generation, source_node_id, node_row_exists, name, uid)| {
                    let source_node = match (node_row_exists, name) {
                        (false, _) => SourceNode::RowDeleted,
                        (true, Some(k8s_node_name)) => SourceNode::Registered {
                            k8s_node_name,
                            k8s_node_uid: uid,
                        },
                        (true, None) => SourceNode::Unverifiable,
                    };
                    LostCandidate {
                        sandbox_id,
                        generation,
                        source_node_id,
                        source_node,
                    }
                },
            )
            .collect())
    }

    async fn mark_lost(&self, candidate: &LostCandidate) -> Result<LostDisposition, sqlx::Error> {
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
            SourceNode::RowDeleted => {
                let row: Option<i32> =
                    sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE"))
                        .bind(node_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                row.is_none()
            }
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
            return Ok(LostDisposition::Refused);
        }

        // Started recovery's claim takes the sandbox row lock before it re-arms
        // the execution, so a wake either commits first (the flip's execution
        // check then refuses) or waits for this transaction, then finds the
        // row `failed` and claims nothing.
        let locked: Option<String> = sqlx::query_scalar(sql!(
            "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        if locked.as_deref() != Some("started") {
            tx.rollback().await?;
            return Ok(LostDisposition::Refused);
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
        let _locked_uploads: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
        ))
        .bind(candidate.sandbox_id)
        .fetch_all(&mut *tx)
        .await?;

        // Fail the gone node's dead uploads before either flip reads the
        // snapshots. Their worker died with the instance, so a row quiet for
        // the settle window can't finalize; failing it first keeps it from
        // blocking the rollback below, which refuses any newer full snapshot
        // that isn't `failed`. Rows still inside the window are untouched
        // and keep vetoing both flips. A transaction that flips nothing rolls
        // this back with everything else.
        fail_quiet_full_uploads_from_node(
            &mut tx,
            node_id,
            &[candidate.sandbox_id],
            DEAD_UPLOAD_SETTLE,
        )
        .await?;

        // Prefer the rollback. The writes since the sandbox last woke died
        // with the node, but the suspend it woke from is usually still in
        // object storage. Reverting to `suspended` on that copy keeps
        // everything up to the suspend; failing the sandbox would throw it
        // away too.
        if let Some(rollback) = roll_back_started_to_suspend(
            &mut tx,
            candidate.sandbox_id,
            node_id,
            candidate.generation,
            DEAD_UPLOAD_SETTLE,
        )
        .await?
        {
            sqlx::query(sql!(
                "DELETE FROM node_local_backup_pending
                  WHERE node_id = $1 AND sandbox_id = $2"
            ))
            .bind(node_id)
            .bind(candidate.sandbox_id)
            .execute(&mut *tx)
            .await?;
            retire_reverted_resident(&mut tx, candidate.sandbox_id).await?;
            tx.commit().await?;
            emit_rolled_back(candidate, rollback);
            return Ok(LostDisposition::RolledBack(rollback));
        }

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
            return Ok(LostDisposition::Refused);
        }
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
        Ok(LostDisposition::Failed)
    }
}

/// Record a rollback: the sandbox event that says the sandbox is `suspended`
/// again, and a warning with the window of activity that died with the node,
/// so the loss is visible per sandbox rather than only as a count.
fn emit_rolled_back(candidate: &LostCandidate, rollback: LostStartedRollback) {
    let lost_window_secs = rollback
        .last_activity_at
        .map(|last| (last - rollback.snapshot_timestamp).whole_seconds().max(0));
    tracing::warn!(
        sandbox_id = %candidate.sandbox_id,
        source_node_id = %candidate.source_node_id,
        generation = candidate.generation,
        reason = candidate.source_node.gone_reason(),
        snapshot_timestamp = %rollback.snapshot_timestamp,
        last_activity_at = ?rollback.last_activity_at,
        lost_window_secs = ?lost_window_secs,
        "lost_started_sandboxes: source node gone; rolled the sandbox back to its last \
         durable suspend snapshot"
    );
    let details = SandboxEventDetails::default()
        .with_from_state("started")
        .with_to_state("suspended")
        .source_node_id(candidate.source_node_id.to_string())
        .execution_generation(candidate.generation.to_string())
        .reason(candidate.source_node.gone_reason())
        .operation("lost_started_sandboxes_rollback")
        .restore_provenance(SandboxRestoreProvenance::StaleAfterNodeLoss)
        .trigger(SandboxEventTrigger::System);
    // `sandbox_lost`, not a suspend event: the running state was lost, and
    // the sandbox lands in `suspended` only because its last suspend survives.
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::SandboxLost,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}
