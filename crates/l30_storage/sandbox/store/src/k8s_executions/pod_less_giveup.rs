//! The cluster controller's node-drain pod-less give-up: the lock of the
//! sandbox's executions and the retire of its active one when the give-up
//! reverts a pod-less resident to `suspended`.
//!
//! The give-up keeps its transaction and `lock_timeout`, its lock order (the
//! node row shared, the sandbox row, then the executions in generation
//! order), its guard re-reads under those locks
//! ([`crate::pod_less_giveup::read_guards`], which reads the give-up's guards),
//! and the revert of the sandbox row that decides whether the retire runs.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Lock every execution row of the sandbox, active or not, `FOR UPDATE`, in
/// generation order. The lock lasts until the caller's transaction ends.
/// The caller must pass its open transaction.
pub async fn lock_executions_in_generation_order<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT 1
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation
            FOR UPDATE"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Retire the sandbox's active execution as `suspended`. **Operation step**
/// of the give-up, run after its sandbox revert in the same transaction: its
/// `WHERE` checks only `active`, because the caller's locks
/// ([`lock_executions_in_generation_order`] among them) and guard re-reads
/// guard it. Returns how many executions it retired: `0` when none was
/// active, and nothing changes. The caller must pass its open transaction.
pub async fn retire_active_executions<C>(tx: &mut C, sandbox_id: SandboxId) -> Result<u64, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions e
            SET active = FALSE,\n\
                state = 'suspended',\n\
                pod_ip = NULL,\n\
                terminated_at = COALESCE(e.terminated_at, NOW()),\n\
                updated_at = NOW()
          WHERE e.sandbox_id = $1
            AND e.active"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests;
