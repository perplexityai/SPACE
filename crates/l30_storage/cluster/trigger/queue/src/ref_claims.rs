//! A node reconciler's claim on its queued `node_reconcile_queue` refs, and
//! the transitions that end the claim.
//!
//! A claim stamps each picked ref with the claimant's token and the ref's
//! `generation` at that moment (`claim_generation`). An enqueue after the
//! claim moves `generation` on, so the completing transitions, guarded by
//! `generation = claim_generation`, match nothing for that ref, and
//! [`release_claimed_ref`] hands it back to run again. Every guarded
//! transition returns how many rows it changed, and zero is a normal
//! outcome, never an error, as when another claim took the ref over, a
//! refresh moved it on (the completing transitions), or a completing
//! transition already ended the claim (the release).
//!
//! Every verb runs on the caller's executor: the claim on the pool, the
//! completion's transitions in the completion's transaction. `NOW()` stays
//! in each statement, so it compares against that executor's clock.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;

/// A claimed ref, its saved trace context, and when it was first queued:
/// the claim's own ordering key.
#[derive(Debug, Clone, FromRow)]
pub struct ClaimedReconcileRef {
    pub facet: String,
    /// `None` for a whole-facet ref.
    pub element: Option<String>,
    pub traceparent: Option<String>,
    pub requested_at: OffsetDateTime,
}

/// Intent: claim up to `limit` of `node_id`'s due, live refs for
/// `claim_token`, oldest request first, taking over a claim older than
/// `stale_secs`. A ref whose row another transaction has locked is skipped.
/// The statement's row locks serve only this statement: on a pool they end
/// with it, and inside a caller's transaction they last until that
/// transaction ends.
pub async fn claim_node_refs(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    limit: i64,
    stale_secs: i64,
    claim_token: &str,
) -> Result<Vec<ClaimedReconcileRef>, sqlx::Error> {
    sqlx::query_as::<_, ClaimedReconcileRef>(sql!(
        "WITH picked AS (
             SELECT node_id, facet, element, requested_at
             FROM node_reconcile_queue
             WHERE node_id = $1
               AND next_attempt_at <= NOW()
               AND dead_at IS NULL
               AND (
                   claim_token = ''
                   OR claimed_at <= NOW() - ($3 * INTERVAL '1 second')
               )
             ORDER BY requested_at ASC, facet ASC, element ASC
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         UPDATE node_reconcile_queue q
         SET claim_token = $4,
             claim_generation = q.generation,
             claimed_at = NOW(),
             updated_at = NOW()
         FROM picked
         WHERE q.node_id = picked.node_id
           AND q.facet = picked.facet
           AND q.element = picked.element
         RETURNING q.facet,
                   NULLIF(q.element, '') AS element,
                   q.traceparent,
                   picked.requested_at"
    ))
    .bind(node_id)
    .bind(limit)
    .bind(stale_secs)
    .bind(claim_token)
    .fetch_all(executor)
    .await
}

/// Guarded transition: delete `node_id`'s `(facet, element)` ref (`None`,
/// the whole-facet ref) once applied, while `claim_token` holds it and no
/// enqueue has refreshed it since the claim. Returns the rows deleted.
pub async fn complete_claimed_ref(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
    claim_token: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM node_reconcile_queue
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND claim_token = $4
           AND generation = claim_generation"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(claim_token)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Guarded transition: give up `claim_token`'s claim on `node_id`'s
/// `(facet, element)` ref, due at once, whatever its generation. After a
/// completing transition it matches only a ref refreshed since the claim.
/// Returns the rows released.
pub async fn release_claimed_ref(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
    claim_token: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET claim_token = '',
             claim_generation = 0,
             claimed_at = NULL,
             next_attempt_at = NOW(),
             updated_at = NOW()
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND claim_token = $4"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(claim_token)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Guarded transition: count a failed attempt of `node_id`'s
/// `(facet, element)` ref with `error` and dead-letter it, while
/// `claim_token` holds it and no enqueue has refreshed it since the claim.
/// Returns the rows dead-lettered.
pub async fn dead_letter_claimed_ref(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
    claim_token: &str,
    error: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET attempts = attempts + 1,
             last_error = $5,
             dead_at = NOW(),
             dead_reason = $5,
             claim_token = '',
             claim_generation = 0,
             claimed_at = NULL,
             updated_at = NOW()
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND claim_token = $4
           AND generation = claim_generation"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(claim_token)
    .bind(error)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Guarded transition: count a failed attempt of `node_id`'s
/// `(facet, element)` ref with `error` and release it to run again
/// `delay_secs` from now, while `claim_token` holds it and no enqueue has
/// refreshed it since the claim. Returns the rows scheduled for retry.
pub async fn retry_claimed_ref(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    facet: &str,
    element: Option<&str>,
    claim_token: &str,
    error: &str,
    delay_secs: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE node_reconcile_queue
         SET attempts = attempts + 1,
             last_error = $5,
             claim_token = '',
             claim_generation = 0,
             claimed_at = NULL,
             next_attempt_at = NOW() + ($6 * INTERVAL '1 second'),
             updated_at = NOW()
         WHERE node_id = $1
           AND facet = $2
           AND element = COALESCE($3, '')
           AND claim_token = $4
           AND generation = claim_generation"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(claim_token)
    .bind(error)
    .bind(delay_secs)
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests;
