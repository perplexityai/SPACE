//! How many refs and triggers are queued, by state, for the queue metrics.
//! Each state is classified at the database clock.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::NODE_RECONCILE_CLAIM_STALE_SECS;
use sqlx::FromRow;

/// The number of `node_reconcile_queue` refs of `facet` in `state`: `dead`,
/// `claimed`, `backoff` or `ready`, the first that applies.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ReconcileQueueDepth {
    pub facet: String,
    pub state: String,
    pub count: i64,
}

/// The number of `node_reconcile_triggers` in `state`: `dead`, `claimed`,
/// `backoff`, `retry` or `ready`, the first that applies.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TriggerQueueDepth {
    pub state: String,
    pub count: i64,
}

/// Every node's refs counted by facet and state; a pair with none is
/// absent. Takes no lock.
pub async fn reconcile_queue_depth(
    executor: impl Executor<'_>,
) -> Result<Vec<ReconcileQueueDepth>, DbError> {
    let rows = sqlx::query_as::<_, ReconcileQueueDepth>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows)
}

/// Every node's triggers counted by state; a state with none is absent.
/// Takes no lock.
pub async fn trigger_queue_depth(
    executor: impl Executor<'_>,
) -> Result<Vec<TriggerQueueDepth>, DbError> {
    let rows = sqlx::query_as::<_, TriggerQueueDepth>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows)
}

/// The number of live `node_reconcile_queue` refs of `facet` whose claim is
/// older than [`NODE_RECONCILE_CLAIM_STALE_SECS`] at the database clock.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ReconcileQueueStaleClaims {
    pub facet: String,
    pub count: i64,
}

/// Every node's stale-claimed refs counted by facet; a facet with none is
/// absent. Takes no lock.
pub async fn reconcile_queue_stale_claims(
    executor: impl Executor<'_>,
) -> Result<Vec<ReconcileQueueStaleClaims>, DbError> {
    let rows = sqlx::query_as::<_, ReconcileQueueStaleClaims>(sql!(
        "SELECT facet, COUNT(*)::BIGINT AS count
         FROM node_reconcile_queue
         WHERE claim_token != ''
           AND claimed_at <= NOW() - ($1 * INTERVAL '1 second')
           AND dead_at IS NULL
         GROUP BY facet"
    ))
    .bind(NODE_RECONCILE_CLAIM_STALE_SECS)
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows)
}
