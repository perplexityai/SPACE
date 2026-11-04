use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_svc::types::{NODE_RECONCILE_CLAIM_STALE_SECS, RECONCILE_FACETS};
use pplx_space_metrics::reconcile::QueueLagStage;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

const DEFAULT_QUEUE_METRICS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct QueueMetricsConfig {
    pub interval: Duration,
}

impl Default for QueueMetricsConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_QUEUE_METRICS_INTERVAL,
        }
    }
}

pub async fn run_queue_metrics(
    pool: PgPool,
    config: QueueMetricsConfig,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        interval_secs = config.interval.as_secs(),
        "starting state-reconciliation queue metrics emitter"
    );

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                emit_node_queue_depth(&pool).await;
                emit_node_queue_stale_claims(&pool).await;
                emit_node_queue_lag(&pool).await;
                emit_trigger_depth(&pool).await;
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("state-reconciliation queue metrics emitter stopping");
                return;
            }
        }
    }
}

pub async fn emit_node_queue_depth(pool: &PgPool) {
    let rows = node_queue_depth_counts(pool).await;
    match rows {
        Ok(rows) => {
            for (facet, state, count) in rows {
                pplx_space_metrics::reconcile::queue_depth(
                    &facet,
                    &state,
                    u64::try_from(count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile queue depth metrics");
        }
    }
}

async fn node_queue_depth_counts(pool: &PgPool) -> Result<Vec<(String, String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, String, i64)>(sql!(
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

pub async fn emit_node_queue_stale_claims(pool: &PgPool) {
    let rows = node_queue_stale_claim_counts(pool).await;
    match rows {
        Ok(rows) => {
            for (facet, count) in rows {
                pplx_space_metrics::reconcile::queue_stale_claims(
                    &facet,
                    u64::try_from(count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile stale claim metrics");
        }
    }
}

async fn node_queue_stale_claim_counts(pool: &PgPool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(sql!(
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

pub async fn emit_node_queue_lag(pool: &PgPool) {
    let rows = node_queue_lag_seconds(pool).await;
    match rows {
        Ok(rows) => {
            for (facet, claim_seconds, retry_seconds) in rows {
                pplx_space_metrics::reconcile::queue_lag_seconds(
                    &facet,
                    QueueLagStage::Claim,
                    claim_seconds,
                );
                pplx_space_metrics::reconcile::queue_lag_seconds(
                    &facet,
                    QueueLagStage::Retry,
                    retry_seconds,
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile queue lag metrics");
        }
    }
}

/// Age of the oldest actionable ref per facet as
/// `(facet, claim-stage seconds, retry-stage seconds)`. See [`QueueLagStage`]
/// for what each of the two clocks measures and why one number for both
/// misreports queue health.
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
async fn node_queue_lag_seconds(pool: &PgPool) -> Result<Vec<(String, f64, f64)>, sqlx::Error> {
    let facets = RECONCILE_FACETS
        .iter()
        .map(|facet| facet.as_str())
        .collect::<Vec<_>>();
    sqlx::query_as::<_, (String, f64, f64)>(sql!(
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

pub async fn emit_trigger_depth(pool: &PgPool) {
    let rows = sqlx::query_as::<_, (String, i64)>(sql!(
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
    .await;
    match rows {
        Ok(rows) => {
            for (state, count) in rows {
                pplx_space_metrics::reconcile::trigger_depth(
                    &state,
                    u64::try_from(count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile trigger depth metrics");
        }
    }
}

#[cfg(test)]
mod tests;
