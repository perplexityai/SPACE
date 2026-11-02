//! A queued `node_reconcile_queue` ref that a reconciler claimed after two
//! failures, for the tests that pin what each internal-api enqueue does to it.

use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_test_env::TestResult;

/// The synthetic caller context the enqueues under test run in.
pub(crate) const CALLER_TRACEPARENT: &str =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const CALLER_TRACE: &str = "00-0af7651916cd43dd8448eb211c80319c-";

/// Queue `(facet, element)` on `node_id` as a ref a reconciler claimed after
/// two failures, once its backoff ran out: generation 3, requested an hour
/// ago, next attempt ten minutes ago.
pub(crate) async fn queue_claimed_retrying_ref(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, generation, attempts, last_error,
             claim_token, claim_generation, claimed_at, next_attempt_at,
             requested_at, updated_at
         )
         VALUES (
             $1, $2, $3, 3, 2, 'synthetic failure',
             'claimant', 3, NOW(), NOW() - INTERVAL '10 minutes',
             NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
         )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct QueuedRefProgress {
    pub(crate) generation: i64,
    pub(crate) attempts: i32,
    pub(crate) last_error: String,
    pub(crate) claim_token: String,
    /// The enqueue restamped `requested_at`.
    pub(crate) requested_now: bool,
    /// The enqueue made the ref due at once.
    pub(crate) due_now: bool,
}

/// The ref's progress, and whether it saved a context in [`CALLER_TRACEPARENT`]'s trace.
pub(crate) async fn queued_ref_progress(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult<(QueuedRefProgress, bool)> {
    let (progress, traceparent): (QueuedRefProgress, Option<String>) = sqlx::query_as::<
        _,
        (i64, i32, String, String, bool, bool, Option<String>),
    >(sql!(
        "SELECT generation, attempts, last_error, claim_token,
                requested_at = updated_at, next_attempt_at = updated_at, traceparent
         FROM node_reconcile_queue
         WHERE node_id = $1 AND facet = $2 AND element = $3"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .fetch_one(pool)
    .await
    .map(
        |(generation, attempts, last_error, claim_token, requested_now, due_now, traceparent)| {
            (
                QueuedRefProgress {
                    generation,
                    attempts,
                    last_error,
                    claim_token,
                    requested_now,
                    due_now,
                },
                traceparent,
            )
        },
    )?;
    Ok((
        progress,
        traceparent.is_some_and(|saved| saved.starts_with(CALLER_TRACE)),
    ))
}
