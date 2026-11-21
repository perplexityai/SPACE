//! [`TriggerQueueStore`], a handle over one pool that offers the trigger
//! claim, target, completion, dead-letter and sweep calls a fanout loop makes,
//! and the reads behind the reconcile and trigger queue gauges. The reads return
//! typed rows and leave metric names, tags and logging to the caller.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::{NODE_RECONCILE_CLAIM_STALE_SECS, RECONCILE_FACETS};
use sqlx::PgPool;
use std::collections::HashSet;

use crate::{StateReconciliationTriggerClaim, TriggerTargets};

/// The trigger and reconcile queues over one pool, for a crate that reaches
/// them without a database driver of its own.
#[derive(Debug, Clone)]
pub struct TriggerQueueStore {
    pool: PgPool,
}

impl TriggerQueueStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_triggers(
        &self,
        limit: u32,
    ) -> Result<StateReconciliationTriggerClaim, sqlx::Error> {
        crate::claim_triggers(&self.pool, limit).await
    }

    pub async fn fetch_trigger_targets(
        &self,
        node_ids: &[NodeId],
        live_node_names: Option<&HashSet<String>>,
    ) -> Result<TriggerTargets, sqlx::Error> {
        crate::fetch_trigger_targets(&self.pool, node_ids, live_node_names).await
    }

    pub async fn complete_claim(
        &self,
        claim_token: &str,
        succeeded: &[NodeId],
        failed: &[(NodeId, String)],
    ) -> Result<(), sqlx::Error> {
        crate::complete_claim(&self.pool, claim_token, succeeded, failed).await
    }

    pub async fn dead_letter_claim(
        &self,
        claim_token: &str,
        failed: &[(NodeId, String)],
    ) -> Result<(), sqlx::Error> {
        crate::dead_letter_claim(&self.pool, claim_token, failed).await
    }

    pub async fn sweep_dead_triggers(
        &self,
        older_than_secs: i64,
        limit: i64,
    ) -> Result<u64, sqlx::Error> {
        crate::sweep_dead_triggers(&self.pool, older_than_secs, limit).await
    }

    pub async fn node_queue_depth_counts(&self) -> Result<Vec<NodeQueueDepthRow>, sqlx::Error> {
        node_queue_depth_counts(&self.pool).await
    }

    pub async fn node_queue_stale_claim_counts(
        &self,
    ) -> Result<Vec<NodeQueueStaleClaimRow>, sqlx::Error> {
        node_queue_stale_claim_counts(&self.pool).await
    }

    pub async fn node_queue_lag_seconds(&self) -> Result<Vec<NodeQueueLagRow>, sqlx::Error> {
        node_queue_lag_seconds(&self.pool).await
    }

    pub async fn trigger_depth_counts(&self) -> Result<Vec<TriggerDepthRow>, sqlx::Error> {
        trigger_depth_counts(&self.pool).await
    }
}

/// How many `node_reconcile_queue` rows of one facet are in one state:
/// `dead`, `claimed`, `backoff` or `ready`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeQueueDepthRow {
    pub facet: String,
    pub state: String,
    pub count: i64,
}

/// How many claimed, live `node_reconcile_queue` rows of one facet hold a
/// claim that has gone stale.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeQueueStaleClaimRow {
    pub facet: String,
    pub count: i64,
}

/// Age in seconds of the oldest actionable ref of one facet, on the
/// claim-stage and retry-stage clocks.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct NodeQueueLagRow {
    pub facet: String,
    pub claim_lag_seconds: f64,
    pub retry_lag_seconds: f64,
}

/// How many `node_reconcile_triggers` rows are in one state: `dead`,
/// `claimed`, `backoff`, `retry` or `ready`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TriggerDepthRow {
    pub state: String,
    pub count: i64,
}

/// `node_reconcile_queue` row counts by facet and state.
async fn node_queue_depth_counts(pool: &PgPool) -> Result<Vec<NodeQueueDepthRow>, sqlx::Error> {
    sqlx::query_as::<_, NodeQueueDepthRow>(sql!(
        "SELECT facet,
                CASE
                    WHEN dead_at IS NOT NULL THEN 'dead'
                    WHEN claim_token != '' THEN 'claimed'
                    WHEN next_attempt_at > NOW() THEN 'backoff'
                    ELSE 'ready'
                END AS state,
                COUNT(*)::BIGINT AS count
         FROM node_reconcile_queue
         GROUP BY facet, state"
    ))
    .fetch_all(pool)
    .await
}

/// Claimed, live `node_reconcile_queue` rows whose claim has gone stale, by
/// facet.
async fn node_queue_stale_claim_counts(
    pool: &PgPool,
) -> Result<Vec<NodeQueueStaleClaimRow>, sqlx::Error> {
    sqlx::query_as::<_, NodeQueueStaleClaimRow>(sql!(
        "SELECT facet, COUNT(*)::BIGINT AS count
         FROM node_reconcile_queue
         WHERE claim_token != ''
           AND claimed_at <= NOW() - ($1 * INTERVAL '1 second')
           AND dead_at IS NULL
         GROUP BY facet"
    ))
    .bind(NODE_RECONCILE_CLAIM_STALE_SECS)
    .fetch_all(pool)
    .await
}

/// Age of the oldest actionable ref per facet, on the claim-stage and
/// retry-stage clocks. See the cluster
/// reconciler's `QueueLagStage` for what each of the two clocks measures and
/// why one number for both misreports queue health.
///
/// The claim clock answers exactly one question — "how long is the oldest ref
/// that `ClaimNodeReconcileRefs` would hand out right now been available for a
/// node to take?" — so its `FILTER` mirrors that RPC's own predicate: past
/// `next_attempt_at`, and either unclaimed or holding a claim that has gone
/// stale. A ref inside its backoff window, or one a node is actively working,
/// is not waiting on a claimer and must not read as a stall.
///
/// The clock starts at the latest moment the ref *became* claimable:
///
/// * never failed (`attempts = 0`) → `requested_at`. Deliberately not
///   `next_attempt_at`: the revival upsert re-stamps `next_attempt_at = NOW()`
///   on an unclaimed never-failed row while preserving `requested_at`, so
///   keying off it would restart the clock on every re-enqueue and let a
///   frequently-republished element hide a wedged node indefinitely.
/// * failed at least once → the backoff expiry. A ref sitting past an expired
///   window is genuinely waiting on a claimer at any `attempts` count, and its
///   wait belongs here rather than hidden in the retry clock.
/// * claim gone stale → when it went stale, since that is when the RPC started
///   offering it again.
///
/// `GREATEST` ignores NULLs, so the stale-claim term drops out for unclaimed
/// rows. Both columns `COALESCE` to zero on the row the `LEFT JOIN` already
/// guarantees per facet, so every facet emits both series even with an empty
/// queue — a facet going quiet reads as a drop to zero rather than a missing
/// series.
///
/// Known limitation, deliberate: a never-failed row that is re-enqueued *while
/// claimed* and then released by a success whose DELETE missed on
/// `generation = claim_generation` ends up in a state identical, column for
/// column, to a never-failed row that was re-enqueued while unclaimed —
/// `attempts = 0`, `claim_token = ''`, `claimed_at IS NULL`,
/// `next_attempt_at = NOW()`, original `requested_at`. Nothing on the row can
/// tell the two apart, so the claim clock cannot start at the release for the
/// first without also restarting on every re-enqueue for the second. This picks
/// the first: it over-reports by the preceding apply's duration for an element
/// hot enough to have every claim raced, which is a bounded false positive,
/// rather than pinning the series near zero while a wedged node claims nothing,
/// which is an unbounded false negative on an alerting signal. Distinguishing
/// them needs a `claimable_since` column maintained by every queue writer.
async fn node_queue_lag_seconds(pool: &PgPool) -> Result<Vec<NodeQueueLagRow>, sqlx::Error> {
    let facets = RECONCILE_FACETS
        .iter()
        .map(|facet| facet.as_str())
        .collect::<Vec<_>>();
    sqlx::query_as::<_, NodeQueueLagRow>(sql!(
        "SELECT known_facets.facet,
                COALESCE(
                    EXTRACT(EPOCH FROM (
                        NOW() - MIN(GREATEST(
                            CASE
                                WHEN q.attempts = 0 THEN q.requested_at
                                ELSE GREATEST(q.requested_at, q.next_attempt_at)
                            END,
                            CASE
                                WHEN q.claim_token != ''
                                THEN q.claimed_at + ($2 * INTERVAL '1 second')
                            END
                        )) FILTER (
                            WHERE q.next_attempt_at <= NOW()
                              AND (
                                  q.claim_token = ''
                                  OR q.claimed_at <= NOW() - ($2 * INTERVAL '1 second')
                              )
                        )
                    ))::DOUBLE PRECISION,
                    0.0::DOUBLE PRECISION
                ) AS claim_lag_seconds,
                COALESCE(
                    EXTRACT(EPOCH FROM (
                        NOW() - MIN(q.requested_at) FILTER (WHERE q.attempts > 0)
                    ))::DOUBLE PRECISION,
                    0.0::DOUBLE PRECISION
                ) AS retry_lag_seconds
         FROM UNNEST($1::TEXT[]) AS known_facets(facet)
         LEFT JOIN (
             node_reconcile_queue q
             INNER JOIN nodes n
               ON n.id = q.node_id
              AND n.state = 'ready'
         ) ON q.facet = known_facets.facet
            AND q.dead_at IS NULL
         GROUP BY known_facets.facet"
    ))
    .bind(facets)
    .bind(NODE_RECONCILE_CLAIM_STALE_SECS)
    .fetch_all(pool)
    .await
}

/// `node_reconcile_triggers` row counts by state.
async fn trigger_depth_counts(pool: &PgPool) -> Result<Vec<TriggerDepthRow>, sqlx::Error> {
    sqlx::query_as::<_, TriggerDepthRow>(sql!(
        "SELECT CASE
                    WHEN dead_at IS NOT NULL THEN 'dead'
                    WHEN claim_token != '' THEN 'claimed'
                    WHEN next_attempt_at > NOW() THEN 'backoff'
                    WHEN attempts > 0 THEN 'retry'
                    ELSE 'ready'
                END AS state,
                COUNT(*)::BIGINT AS count
         FROM node_reconcile_triggers
         GROUP BY state"
    ))
    .fetch_all(pool)
    .await
}

#[cfg(test)]
mod tests;
