//! State transitions of a sandbox's active execution: a `FOR UPDATE` read
//! of its state, and the state compare-and-set.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgExecutor};

/// Lock `sandbox_id`'s active execution at `generation` `FOR UPDATE` and
/// return its stored state, unparsed; `None` when there is no such
/// execution. The lock lasts until the caller's transaction ends.
///
/// `FOR UPDATE` follows the update chain to the latest committed row version
/// and re-evaluates the predicate, so a retirement that commits while this
/// waits on the lock reads as `None`, unlike a plain read, whose
/// statement-start snapshot could still show the active version.
pub async fn lock_active_state(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT state
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active
             FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(conn)
    .await
}

/// Move `sandbox_id`'s active execution at `generation` to `state` while it
/// is still in `expected_state`, touching `updated_at`. Returns the number of
/// rows written: `0` when the execution isn't active at `generation` or has
/// left `expected_state`, which it leaves as it is.
pub async fn compare_and_set_active_state(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    expected_state: &str,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                 SET state = $3, updated_at = NOW()
                 WHERE sandbox_id = $1 AND generation = $2 AND active AND state = $4"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(state)
    .bind(expected_state)
    .execute(executor)
    .await?
    .rows_affected())
}

#[cfg(test)]
mod tests;
