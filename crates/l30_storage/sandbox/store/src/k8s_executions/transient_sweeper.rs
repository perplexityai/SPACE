//! The cluster controller's transient lifecycle sweeper: the failed-cleanup
//! rotation, the retire of a reverted restore's execution, and the
//! execution lock the revert takes.
//!
//! The sweeper keeps its transactions, the sandbox row lock it takes before
//! the execution's (sandbox before execution), its handling of a contended
//! `NOWAIT`, and what it does with each outcome.

use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};

/// Rotate a failed-cleanup candidate behind the others by bumping its active
/// execution's `updated_at`, while its sandbox is still a `failed` row
/// managed by `managed_by`. Returns how many rows it bumped: `0` when the
/// sandbox no longer has that shape, and nothing changes.
pub async fn note_failed_cleanup_attempt(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: SubstrateKind,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions e
         SET updated_at = NOW()
         FROM sandboxes s
         WHERE e.sandbox_id = s.id
           AND s.id = $1
           AND s.managed_by = $2
           AND s.lifecycle = 'failed'
           AND e.active"
    ))
    .bind(sandbox_id)
    .bind(managed_by)
    .execute(executor)
    .await?
    .rows_affected())
}

/// Retire the reverted restore's active execution as `replaced`, fenced on
/// the identity the sweeper's candidate select recorded: its `pod_name`, and
/// NULL-safe its `generation`, `activation_attempt_id`, trimmed `pod_uid` and
/// `route_activation_id`. **Operation step** of the revert, run in the
/// revert's transaction. `false` when the active execution is no longer that
/// one, and nothing changes.
pub async fn retire_reverted_restore_execution(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    pod_name: &str,
    generation: Option<i64>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    pod_uid: Option<&str>,
    route_activation_id: Option<SandboxActivationAttemptId>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'replaced',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND active
            AND pod_name = $2
            AND generation IS NOT DISTINCT FROM $3
            AND activation_attempt_id IS NOT DISTINCT FROM $4
            AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $5::TEXT
            AND route_activation_id IS NOT DISTINCT FROM $6"
    ))
    .bind(sandbox_id)
    .bind(pod_name)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(pod_uid)
    .bind(route_activation_id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

/// Lock the sandbox's active execution `FOR UPDATE NOWAIT`, the lock its
/// pod-IP writers take. The lock lasts until the caller's transaction ends.
/// A sandbox with no active execution locks nothing and still succeeds.
/// Fails at once with `lock_not_available` (`55P03`) when another
/// transaction holds the row; the caller decides what that means.
pub async fn lock_active_nowait(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND active
                FOR UPDATE NOWAIT"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
