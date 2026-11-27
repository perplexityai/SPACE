//! How long the oldest actionable `node_reconcile_queue` ref has waited, per
//! facet, for the queue metrics. Each age is taken at the database clock.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::{NODE_RECONCILE_CLAIM_STALE_SECS, RECONCILE_FACETS};
use sqlx::FromRow;

/// One facet's queue lag; see [`reconcile_queue_lag`].
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct ReconcileQueueLag {
    pub facet: String,
    /// Seconds the oldest claimable ref has been claimable.
    pub claim_lag_seconds: f64,
    /// Seconds since the oldest ref that has failed at least once was
    /// requested.
    pub retry_lag_seconds: f64,
}

/// Age of the oldest actionable ref per facet, at the claim and at the retry
/// stage. cluster-reconciler's `QueueLagStage` says what each of the two
/// clocks measures and why one number for both misreports queue health.
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
pub async fn reconcile_queue_lag(
    executor: impl Executor<'_>,
) -> Result<Vec<ReconcileQueueLag>, DbError> {
    let facets = RECONCILE_FACETS
        .iter()
        .map(|facet| facet.as_str())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, ReconcileQueueLag>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows)
}
