//! Postgres `NOTIFY` signals that wake a cluster-controller loop before its
//! next periodic sweep. A signal is advisory: the consumer re-derives its work
//! from the database, so a lost or spurious one only affects latency.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_rpc_contracts::node_drain_signals::EVACUATION_RESTORE_READY_CHANNEL;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// `NOTIFY` the evacuation restore channel with `sandbox_id`, sent once its
/// suspend snapshot is durable, so the node-drain loop runs its restore sweep
/// now rather than at its next periodic tick.
pub async fn notify_evacuation_restore_ready(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), DbError> {
    sqlx::query(sql!("SELECT pg_notify($1, $2)"))
        .bind(EVACUATION_RESTORE_READY_CHANNEL)
        .bind(sandbox_id.to_string())
        .execute(executor.into_sqlx())
        .await?;
    Ok(())
}
