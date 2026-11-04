//! Postgres-backed [`QueueGcSink`]. The candidate scan surfaces non-`ready`,
//! non-draining k8s nodes with stale live refs (the detector then decides
//! gone-ness). The guarded evaluation rechecks node state, drain phase, and
//! identity (`k8s_node_uid`) under a `FOR UPDATE` lock. A fresh node-ctl queue
//! claim vetoes the entire node. Dry-run and live share the same eligibility
//! statement; live alone executes its bounded deletes. The node row is never
//! deleted.

use pplx_space_common_core::types::{NodeId, SubstrateKind};

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_metrics::reconcile::QueueGcMode;
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
        let (queue, triggers): (i64, i64) = sqlx::query_as(sql!(
            "WITH doomed AS MATERIALIZED (
                 SELECT 'queue'::TEXT AS record_kind,
                        q.node_id,
                        q.facet,
                        q.element,
                        q.requested_at
                 FROM node_reconcile_queue q
                 WHERE q.dead_at IS NOT NULL
                   AND q.dead_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND q.requested_at <= NOW() - ($1 * INTERVAL '1 second')
                 UNION ALL
                 SELECT 'trigger'::TEXT AS record_kind,
                        t.node_id,
                        ''::TEXT AS facet,
                        ''::TEXT AS element,
                        t.requested_at
                 FROM node_reconcile_triggers t
                 WHERE t.dead_at IS NOT NULL
                   AND t.dead_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND t.updated_at <= NOW() - ($1 * INTERVAL '1 second')
                 ORDER BY requested_at ASC
                 LIMIT $2
             ),
             deleted_queue AS (
                 DELETE FROM node_reconcile_queue q
                 USING doomed d
                 WHERE d.record_kind = 'queue'
                   AND q.node_id = d.node_id
                   AND q.facet = d.facet
                   AND q.element = d.element
                   AND q.dead_at IS NOT NULL
                   AND q.dead_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND q.requested_at <= NOW() - ($1 * INTERVAL '1 second')
                 RETURNING 1
             ),
             deleted_triggers AS (
                 DELETE FROM node_reconcile_triggers t
                 USING doomed d
                 WHERE d.record_kind = 'trigger'
                   AND t.node_id = d.node_id
                   AND t.dead_at IS NOT NULL
                   AND t.dead_at <= NOW() - ($1 * INTERVAL '1 second')
                   AND t.updated_at <= NOW() - ($1 * INTERVAL '1 second')
                 RETURNING 1
             )
             SELECT (SELECT COUNT(*)::BIGINT FROM deleted_queue),
                    (SELECT COUNT(*)::BIGINT FROM deleted_triggers)"
        ))
        .bind(retention_secs)
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;
        Ok(PruneCounts {
            queue: u64::try_from(queue).unwrap_or(0),
            triggers: u64::try_from(triggers).unwrap_or(0),
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
        let rows: Vec<(NodeId, String, Option<String>)> = sqlx::query_as(sql!(
            "SELECT n.id, k.k8s_node_name, k.k8s_node_uid
             FROM nodes n
             JOIN node_k8s k ON k.node_id = n.id
             WHERE n.managed_by = $4
               AND n.state IS DISTINCT FROM 'ready'
               AND n.drain_phase IS NULL
               AND ($1::UUID IS NULL OR n.id > $1)
               AND (
                   EXISTS (
                       SELECT 1
                       FROM node_reconcile_queue q
                       WHERE q.node_id = n.id
                         AND q.dead_at IS NULL
                         AND q.requested_at <= NOW() - ($2 * INTERVAL '1 second')
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM node_reconcile_triggers t
                       WHERE t.node_id = n.id
                         AND t.dead_at IS NULL
                         AND t.requested_at <= NOW() - ($2 * INTERVAL '1 second')
                   )
               )
             ORDER BY n.id ASC
             LIMIT $3"
        ))
        .bind(after)
        .bind(retention_secs)
        .bind(i64::from(limit))
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(node_id, k8s_node_name, k8s_node_uid)| GoneCandidate {
                node_id,
                k8s_node_name,
                k8s_node_uid,
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
        // Rechecks `state != 'ready'`, drain phase, and the kube node UID under
        // the row lock so a node that flipped to `ready`, entered a drain, or
        // was replaced (new UID) between scan and evaluation is preserved. All
        // live queue rows are locked before eligibility is decided, making the
        // node-wide active-claim veto race-safe against a concurrent node-ctl
        // claim. Trigger claims remain per-row because cluster-controller owns
        // them. Dry-run and live share this statement and all predicates; $6
        // only gates the DELETEs.
        let (eligible_queue, eligible_triggers, deleted_queue, deleted_triggers): (
            i64,
            i64,
            i64,
            i64,
        ) = sqlx::query_as(sql!(
            "WITH candidate_node AS MATERIALIZED (
                 SELECT n.id
                 FROM nodes n
                 JOIN node_k8s k ON k.node_id = n.id
                 WHERE n.id = $1
                   AND n.managed_by = $7
                   AND n.state IS DISTINCT FROM 'ready'
                   AND n.drain_phase IS NULL
                   AND COALESCE(k.k8s_node_uid, '') = COALESCE($2, '')
                 FOR UPDATE OF n, k
             ),
             locked_queue AS MATERIALIZED (
                 SELECT q.node_id,
                        q.facet,
                        q.element,
                        q.requested_at,
                        q.claim_token,
                        q.claimed_at
                 FROM node_reconcile_queue q
                 JOIN candidate_node n ON n.id = q.node_id
                 WHERE q.dead_at IS NULL
                 FOR UPDATE OF q
             ),
             eligible_node AS MATERIALIZED (
                 SELECT n.id
                 FROM candidate_node n
                 WHERE NOT EXISTS (
                       SELECT 1
                       FROM locked_queue active_q
                       WHERE active_q.node_id = n.id
                         AND active_q.claim_token <> ''
                         AND active_q.claimed_at
                             > NOW() - ($5 * INTERVAL '1 second')
                   )
             ),
             doomed AS MATERIALIZED (
                 SELECT 'queue'::TEXT AS record_kind,
                        q.node_id,
                        q.facet,
                        q.element,
                        q.requested_at
                 FROM locked_queue q
                 JOIN eligible_node n ON n.id = q.node_id
                 WHERE q.requested_at <= NOW() - ($3 * INTERVAL '1 second')
                   AND (q.claim_token = '' OR q.claimed_at IS NULL
                        OR q.claimed_at <= NOW() - ($5 * INTERVAL '1 second'))
                 UNION ALL
                 SELECT 'trigger'::TEXT AS record_kind,
                        t.node_id,
                        ''::TEXT AS facet,
                        ''::TEXT AS element,
                        t.requested_at
                 FROM node_reconcile_triggers t
                 JOIN eligible_node n ON n.id = t.node_id
                 WHERE t.dead_at IS NULL
                   AND t.requested_at <= NOW() - ($3 * INTERVAL '1 second')
                   AND (t.claim_token = '' OR t.claimed_at IS NULL
                        OR t.claimed_at <= NOW() - ($5 * INTERVAL '1 second'))
                 ORDER BY requested_at ASC
                 LIMIT $4
             ),
             deleted_queue AS (
                 DELETE FROM node_reconcile_queue q
                 USING doomed d
                 WHERE d.record_kind = 'queue'
                   AND $6::BOOL
                   AND q.node_id = d.node_id
                   AND q.facet = d.facet
                   AND q.element = d.element
                   AND q.dead_at IS NULL
                   AND q.requested_at <= NOW() - ($3 * INTERVAL '1 second')
                   AND (q.claim_token = '' OR q.claimed_at IS NULL
                        OR q.claimed_at <= NOW() - ($5 * INTERVAL '1 second'))
                 RETURNING 1
             ),
             deleted_triggers AS (
                 DELETE FROM node_reconcile_triggers t
                 USING doomed d
                 WHERE d.record_kind = 'trigger'
                   AND $6::BOOL
                   AND t.node_id = d.node_id
                   AND t.dead_at IS NULL
                   AND t.requested_at <= NOW() - ($3 * INTERVAL '1 second')
                   AND (t.claim_token = '' OR t.claimed_at IS NULL
                        OR t.claimed_at <= NOW() - ($5 * INTERVAL '1 second'))
                 RETURNING 1
             )
             SELECT (SELECT COUNT(*)::BIGINT FROM doomed WHERE record_kind = 'queue'),
                    (SELECT COUNT(*)::BIGINT FROM doomed WHERE record_kind = 'trigger'),
                    (SELECT COUNT(*)::BIGINT FROM deleted_queue),
                    (SELECT COUNT(*)::BIGINT FROM deleted_triggers)"
        ))
        .bind(node_id)
        .bind(expected_uid)
        .bind(retention_secs)
        .bind(i64::from(limit))
        .bind(claim_grace_secs)
        .bind(mode == QueueGcMode::Live)
        .bind(SubstrateKind::Kubernetes)
        .fetch_one(&self.pool)
        .await?;
        Ok(GoneNodeEvaluation {
            eligible: PruneCounts {
                queue: u64::try_from(eligible_queue).unwrap_or(0),
                triggers: u64::try_from(eligible_triggers).unwrap_or(0),
            },
            deleted: PruneCounts {
                queue: u64::try_from(deleted_queue).unwrap_or(0),
                triggers: u64::try_from(deleted_triggers).unwrap_or(0),
            },
        })
    }
}
