//! How many refs and triggers are queued, by state, for the queue metrics.
//! Each state is classified at the database clock.

use pplx_space_sql_locator::sql;
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
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<ReconcileQueueDepth>, sqlx::Error> {
    sqlx::query_as::<_, ReconcileQueueDepth>(sql!(
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
    .fetch_all(executor)
    .await
}

/// Every node's triggers counted by state; a state with none is absent.
/// Takes no lock.
pub async fn trigger_queue_depth(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<TriggerQueueDepth>, sqlx::Error> {
    sqlx::query_as::<_, TriggerQueueDepth>(sql!(
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
    .fetch_all(executor)
    .await
}
