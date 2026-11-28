//! The cluster controller's recovery of an abandoned quarantine publication
//! after its only execution disappeared: the lock of the sandbox's
//! executions, and the retire of the execution the recovery proved gone.
//!
//! The recovery keeps its transaction and `lock_timeout`, its lock order
//! (sandbox, quarantine episode, executions, then the snapshot rows), and the
//! re-read of its candidate under those locks that decides whether the
//! retire runs.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Lock every execution row of the sandbox, active or not, `FOR UPDATE`.
/// The lock lasts until the caller's transaction ends. The caller must pass
/// its open transaction.
pub async fn lock_executions<C>(tx: &mut C, sandbox_id: SandboxId) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT id FROM sandbox_k8s_executions WHERE sandbox_id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Retire the execution `execution_id` as `suspended`. **Operation step** of
/// the recovery: its `WHERE` checks only the id, because the caller's locks
/// ([`lock_executions`] among them) and its re-read of the candidate guard
/// it. `false` when there is no such execution, and nothing changes. The
/// caller must pass its open transaction.
pub async fn retire_recovered_execution<C>(tx: &mut C, execution_id: Uuid) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'suspended', pod_ip = NULL,
             terminated_at = COALESCE(terminated_at, NOW()), updated_at = NOW()
          WHERE id = $1"
    ))
    .bind(execution_id)
    .execute(tx.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
