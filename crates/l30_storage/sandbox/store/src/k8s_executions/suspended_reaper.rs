//! The cluster controller's suspended-execution reaper: the retire of a
//! `suspended` sandbox's leaked active execution whose pod is gone.
//!
//! The reaper keeps its transaction, the sandbox row lock it takes first
//! ([`crate::row_locks::lock_lifecycle`]), its re-check of the lifecycle under
//! that lock, and the decision of what to do when the retire matches nothing.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Retire the sandbox's active execution as `suspended`, guarded on the
/// exact `generation`, `pod_name` and `pod_uid` the reaper's scan observed
/// and its pod check verified missing (`pod_uid` compared NULL-safe).
/// **Operation step**: the reaper ends the execution its suspend left
/// behind. `false` when the active execution is no longer that one, as when
/// a wake replaced it, and nothing changes.
pub async fn retire_stale_execution(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_name: &str,
    pod_uid: Option<&str>,
) -> Result<bool, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'suspended',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND active
            AND generation = $2
            AND pod_name = $3
            AND pod_uid IS NOT DISTINCT FROM $4"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .bind(pod_uid)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
