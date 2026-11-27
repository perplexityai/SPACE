//! Postgres-backed [`QueueGcSink`]. The candidate scan surfaces non-`ready`,
//! non-draining k8s nodes with stale live refs or triggers (the detector then
//! decides gone-ness). The guarded evaluation rechecks node state, drain
//! phase, and identity (`k8s_node_uid`) under a `FOR UPDATE` lock. A fresh
//! node-ctl queue claim vetoes the entire node. Dry-run and live share the
//! same eligibility statement; live alone executes its bounded deletes. The
//! node row is never deleted.

use pplx_space_accounts::NodeId;
use pplx_space_cluster_trigger_queue::queue_gc;
use pplx_space_node_registration_store::stale_reconcile_work;

use std::time::Duration;

use crate::metrics::reconcile::QueueGcMode;
use sqlx::PgPool;

use super::{GoneCandidate, GoneNodeEvaluation, PruneCounts, QueueGcSink};

pub struct QueueDbStore {
    pool: PgPool,
}

impl QueueDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl QueueGcSink for QueueDbStore {
    async fn prune_dead(
        &self,
        retention: Duration,
        limit: u32,
    ) -> Result<PruneCounts, sqlx::Error> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let limit = i64::from(limit);
        let pruned = queue_gc::prune_dead(&self.pool, retention_secs, limit).await?;
        Ok(PruneCounts {
            queue: u64::try_from(pruned.queue).unwrap_or(0),
            triggers: u64::try_from(pruned.triggers).unwrap_or(0),
        })
    }

    async fn list_gone_candidates(
        &self,
        retention: Duration,
        limit: u32,
        after: Option<NodeId>,
    ) -> Result<Vec<GoneCandidate>, sqlx::Error> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        // Only non-`ready` nodes are eligible: a stale-`ready` gone node is a
        // lifecycle-tracking failure to correct elsewhere, not queue debt to
        // reap. Detectors then decide gone-ness among these.
        let rows = stale_reconcile_work::unready_nodes_with_stale_reconcile_work(
            &self.pool,
            after,
            retention_secs,
            i64::from(limit),
        )
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| GoneCandidate {
                node_id: row.id,
                k8s_node_name: row.k8s_node_name,
                k8s_node_uid: row.k8s_node_uid,
            })
            .collect())
    }

    async fn evaluate_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
        retention: Duration,
        claim_active_grace: Duration,
        limit: u32,
        mode: QueueGcMode,
    ) -> Result<GoneNodeEvaluation, sqlx::Error> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let claim_grace_secs = i64::try_from(claim_active_grace.as_secs()).unwrap_or(i64::MAX);
        // Dry-run and live share the statement and all its predicates; the
        // mode only gates the deletes.
        let debt = queue_gc::prune_gone_node_debt(
            &self.pool,
            node_id,
            expected_uid.as_deref(),
            retention_secs,
            i64::from(limit),
            claim_grace_secs,
            mode == QueueGcMode::Live,
        )
        .await?;
        Ok(GoneNodeEvaluation {
            eligible: PruneCounts {
                queue: u64::try_from(debt.eligible_queue).unwrap_or(0),
                triggers: u64::try_from(debt.eligible_triggers).unwrap_or(0),
            },
            deleted: PruneCounts {
                queue: u64::try_from(debt.deleted_queue).unwrap_or(0),
                triggers: u64::try_from(debt.deleted_triggers).unwrap_or(0),
            },
        })
    }
}
