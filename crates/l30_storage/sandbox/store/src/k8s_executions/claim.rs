//! Claims of a sandbox's execution generations: the retired generation 1 a
//! warm-pool reservation leaves behind, the generation minter, the `creating`
//! claim insert that follows it, and the reset of a claim back to `creating`.
//!
//! The minter and the insert are one claim: the minter is correct only under
//! the caller's lock on the `sandboxes` row, with the insert run next in the
//! same transaction. The `(sandbox_id) WHERE active`,
//! `(sandbox_id, generation)` and `(pod_name)` unique indexes are the
//! backstop the insert's `ON CONFLICT DO NOTHING` reports as `None`.

use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::{PgConnection, PgExecutor};

use super::ActiveExecutionRecord;

/// Record generation 1 of `sandbox_id`, named `pod_name` in `pod_namespace`,
/// as already retired (`replaced`, inactive), so its first execution takes
/// generation 2. `false` when the sandbox already has execution rows, or
/// another execution already has `pod_name`; it leaves those rows as they
/// are.
pub async fn retire_pool_reserved_generation(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    pod_name: &str,
    pod_namespace: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active,
                 terminated_at
             )
             SELECT $1, 1, $2, $3, 'replaced', FALSE, NOW()
             WHERE NOT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1
             )
             ON CONFLICT DO NOTHING"
    ))
    .bind(sandbox_id)
    .bind(pod_name)
    .bind(pod_namespace)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

/// Mint the generation of `sandbox_id`'s next execution: one past the
/// highest it has recorded, `1` for none.
///
/// Correct only under the caller's `FOR UPDATE` lock on the sandbox's
/// `sandboxes` row, with [`insert_creating_claim`] run next in the same
/// transaction: the lock keeps a second claim from minting until this one
/// commits or rolls back, and the insert records the generation before the
/// lock ends. It takes the caller's open transaction for that reason.
/// [`super::unclaimed_generation`] reads the same allocation as a lock-free
/// hint; change them together.
pub async fn mint_generation(
    conn: &mut PgConnection,
    sandbox_id: SandboxId,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COALESCE(MAX(generation), 0) + 1
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(conn)
    .await
}

/// Insert the active `creating` execution of `sandbox_id` at the
/// `generation` [`mint_generation`] minted, named `pod_name` in
/// `pod_namespace`, and return it in [`super::active_execution`]'s shape,
/// with `pod_materialized` and `create_age_expired` `false`. `None` when the
/// insert conflicts with a unique index: the sandbox already has an active
/// execution or one at that generation, or another execution already has
/// `pod_name`. It leaves those rows as they are.
pub async fn insert_creating_claim(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_name: &str,
    pod_namespace: &str,
) -> Result<Option<ActiveExecutionRecord>, sqlx::Error> {
    sqlx::query_as::<_, ActiveExecutionRecord>(sql!(
        "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, state, active
             )
             VALUES ($1, $2, $3, $4, 'creating', TRUE)
             ON CONFLICT DO NOTHING
             RETURNING generation, pod_name, FALSE AS pod_materialized, state,
                       FALSE AS create_age_expired, activation_attempt_id,
                       route_activation_id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .bind(pod_namespace)
    .fetch_optional(executor)
    .await
}

/// Reset `sandbox_id`'s active execution at `generation`, still held by
/// `activation_attempt_id` (`None` matches an execution holding none), to an
/// unmaterialized `creating` claim with no pod UID or IP. Only a `creating`
/// or `created` execution with no pod IP qualifies. Clearing a pod UID stamps
/// `pod_materialized_at` first if the row held one. `false` when no execution
/// matches, which it leaves as it is.
pub async fn reset_to_creating(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 pod_materialized_at = COALESCE(
                     pod_materialized_at,
                     CASE WHEN NULLIF(BTRIM(pod_uid), '') IS NOT NULL THEN NOW() END
                 ),
                 state = 'creating',
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND pod_ip IS NULL
               AND state IN ('creating', 'created')"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
