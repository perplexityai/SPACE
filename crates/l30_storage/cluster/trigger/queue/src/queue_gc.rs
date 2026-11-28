//! The reconcile queue GC's deletes: refs and triggers dead past retention,
//! and the live debt of a Kubernetes node that is gone. The node row is
//! never deleted.
//!
//! Each verb runs on the caller's executor and leaves `NOW()` in its
//! statement; retention and grace periods are bound in seconds.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// How many dead refs and dead triggers [`prune_dead`] deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeadPruneCounts {
    pub queue: i64,
    pub triggers: i64,
}

/// Intent: delete up to `limit` refs and triggers together, oldest request
/// first, that were dead-lettered at least `retention_secs` ago and have not
/// been requested (a ref) or touched (a trigger) since.
pub async fn prune_dead(
    executor: impl Executor<'_>,
    retention_secs: i64,
    limit: i64,
) -> Result<DeadPruneCounts, DbError> {
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
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(DeadPruneCounts { queue, triggers })
}

/// What [`prune_gone_node_debt`] found eligible on the node and what it
/// deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GoneNodeDebt {
    pub eligible_queue: i64,
    pub eligible_triggers: i64,
    pub deleted_queue: i64,
    pub deleted_triggers: i64,
}

/// Guarded transition: count, and when `delete` is set delete, up to `limit`
/// of `node_id`'s live refs and triggers requested at least `retention_secs`
/// ago and not claimed within `claim_grace_secs`, oldest request first.
///
/// Under a row lock on the node, it re-checks that the node is still a
/// Kubernetes node that is not `ready`, not draining, and registered with
/// `expected_uid`. A node that became `ready`, entered a drain or was
/// replaced (a new UID) since the caller chose it matches nothing: every
/// count is zero. All the node's live refs are locked before eligibility is
/// decided, so a ref claimed within `claim_grace_secs` vetoes the whole node,
/// race-free against a concurrent claim. Trigger claims are checked per row,
/// because the cluster controller, not the node, claims triggers. A dry run
/// (`delete` unset) evaluates the same predicates and deletes nothing. Its
/// row locks serve only this statement: on a pool they end with it, and
/// inside a caller's transaction they last until that transaction ends.
pub async fn prune_gone_node_debt(
    executor: impl Executor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    retention_secs: i64,
    limit: i64,
    claim_grace_secs: i64,
    delete: bool,
) -> Result<GoneNodeDebt, DbError> {
    let (eligible_queue, eligible_triggers, deleted_queue, deleted_triggers): (i64, i64, i64, i64) =
        sqlx::query_as(sql!(
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
        .bind(limit)
        .bind(claim_grace_secs)
        .bind(delete)
        .bind(SubstrateKind::Kubernetes)
        .fetch_one(executor.into_sqlx())
        .await?;
    Ok(GoneNodeDebt {
        eligible_queue,
        eligible_triggers,
        deleted_queue,
        deleted_triggers,
    })
}

#[cfg(test)]
mod tests;
