//! Postgres-backed [`super::LostStartedSink`].

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_events::{SandboxEventDetails, SandboxEventTrigger};
use pplx_space_sandbox_portability::started_restore_source_absent;
use pplx_space_sandbox_store::gone_node::{self, StartedSource, WriteOff};
use sqlx::PgPool;

use super::{LostCandidate, LostStartedSink, SourceNode};
use crate::drained_node_terminator::store::{DEAD_UPLOAD_SETTLE, emit_lost_and_failed};

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

    async fn mark_lost(&self, candidate: &LostCandidate) -> Result<bool, sqlx::Error> {
        let node_id = candidate.source_node_id;
        let source = match &candidate.source_node {
            SourceNode::RowDeleted => StartedSource::RowDeleted,
            // Reached only after the tick found the node absent from
            // Kubernetes with no live EC2 instance.
            SourceNode::Registered { k8s_node_uid, .. } => StartedSource::Registered {
                expected_uid: k8s_node_uid.as_deref(),
            },
            SourceNode::Unverifiable => return Ok(false),
        };
        let mut tx = self.pool.begin().await?;
        let flipped = gone_node::write_off_on_gone_node(
            &mut tx,
            node_id,
            WriteOff::Started {
                sandbox_id: candidate.sandbox_id,
                generation: candidate.generation,
                source,
            },
            DEAD_UPLOAD_SETTLE,
        )
        .await?;
        tx.commit().await?;
        if flipped.is_empty() {
            return Ok(false);
        }

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
