//! The execution lock and writes of a Kubernetes suspend's commit and
//! finalization, on the suspended pod's execution.
//!
//! The `sandboxes` row's suspend writes are the top-level
//! [`crate::suspend`] module's. The caller locks that row first, keeps its
//! transaction and its re-checks between statements, and maps each outcome.
//! Each write is a guarded transition, its generation, pod UID, `active` and
//! state guard whole and bound to the values the caller decided on.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Lock `sandbox_id`'s execution at `generation` tracking `pod_uid` while it
/// is in `suspended_state`, `FOR UPDATE`, and return whether it is still
/// active. `None`: there is no such execution. The lock lasts until the
/// caller's transaction ends. The caller must pass its open transaction.
pub async fn lock_suspended_execution<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
    suspended_state: &str,
) -> Result<Option<bool>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND pod_uid = $3
                AND state = $4
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(suspended_state)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Mark the suspend of `sandbox_id`'s active execution at `generation`
/// tracking `pod_uid` committed: move it from `running_state` to
/// `suspended_state`, or touch it when it is already there. Returns the
/// number of rows written: `0` when no such execution is in either state.
pub async fn mark_committed(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
    suspended_state: &str,
    running_state: &str,
) -> Result<u64, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3
                AND state IN ($4, $5)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(suspended_state)
    .bind(running_state)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected())
}

/// Mark the suspend committed on `sandbox_id`'s execution at `generation`
/// tracking `pod_uid` after pod cleanup already retired it as `forgotten`:
/// move it to `suspended_state`, only while the sandbox is `suspending` and
/// no newer execution exists. Returns the number of rows written: `0` when
/// the execution, the sandbox or a successor rules it out.
pub async fn mark_forgotten_committed(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
    suspended_state: &str,
) -> Result<u64, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions e
                    SET state = $4,
                        updated_at = NOW()
                   FROM sandboxes s
                  WHERE e.sandbox_id = $1
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND NOT e.active
                    AND e.state = 'forgotten'
                    AND s.id = e.sandbox_id
                    AND s.lifecycle = 'suspending'
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions successor
                         WHERE successor.sandbox_id = e.sandbox_id
                           AND successor.generation > e.generation
                    )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(suspended_state)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests;
