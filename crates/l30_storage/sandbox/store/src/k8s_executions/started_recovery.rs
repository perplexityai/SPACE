//! The execution writes and the execution lock of a started sandbox's
//! recovery: claiming it, handing it to a successor execution, and completing
//! or failing it.
//!
//! Each write is a guarded transition: its `WHERE` re-checks the generation,
//! `active`, the state and, where it has one, the activation attempt, bound
//! to the values the caller decided on, and matching no row is a normal
//! outcome, not an error. [`claim`], [`lock_recovering_execution`],
//! [`hand_off_to_successor`] and [`fail`] run in the caller's transaction,
//! after its lock of the `sandboxes` row, and the caller keeps its re-checks
//! between them. [`complete`] relies on its guard alone: its caller runs it
//! on the pool, outside any transaction and without that lock. The
//! database-clock comparisons and lease ends stay in the SQL, so each verb
//! runs on the executor its caller passes.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Claim the recovery of `sandbox_id`'s active execution at `generation`,
/// moving it to `recovering`, and return that generation. A claim that
/// created the execution (`created`) takes it only while it is still
/// `creating`; one that found it takes it from `created`, `running` or
/// `recovery_failed`, or from a `creating` or `recovering` execution last
/// updated at least `lease_ttl_secs` ago by the database clock. `None`: no
/// such execution, which the caller reads as a recovery in flight.
pub async fn claim(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    created: bool,
    lease_ttl_secs: i64,
) -> Result<Option<i64>, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE sandbox_k8s_executions
             SET state = 'recovering', updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
               AND (
                 ($3 AND state = 'creating')
                 OR (
                   NOT $3
                   AND (
                     state IN ('created', 'running', 'recovery_failed')
                     OR (
                       state IN ('creating', 'recovering')
                       AND updated_at <= NOW() - make_interval(secs => $4)
                     )
                   )
                 )
               )
             RETURNING generation"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(created)
    .bind(lease_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The activation attempt a recovering execution holds, as
/// [`lock_recovering_execution`] reads it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RecoveringActivationAttempt {
    pub activation_attempt_id: Option<Uuid>,
    /// Raw `activation_attempt_target_state`; the caller parses it.
    pub activation_attempt_target_state: Option<String>,
}

/// Lock `sandbox_id`'s active `recovering` execution at `generation`
/// `FOR UPDATE`, while the sandbox is `started`, and return the activation
/// attempt it holds. `None`: there is no such execution, or the sandbox is no
/// longer `started`. The lock covers the execution row only (`OF e`), not the
/// joined `sandboxes` row, which the caller has already locked, and lasts
/// until the caller's transaction ends. The caller must pass its open
/// transaction.
pub async fn lock_recovering_execution<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<Option<RecoveringActivationAttempt>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, RecoveringActivationAttempt>(sql!(
        "SELECT e.activation_attempt_id, e.activation_attempt_target_state
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
              WHERE s.id = $1
                AND s.lifecycle = 'started'
              FOR UPDATE OF e"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Hand a started recovery to the successor execution at `generation`, which
/// the caller just inserted: move it from `creating` to `recovering` with the
/// replaced execution's activation attempt, as both its attempt and its route
/// activation, and `activation_target_state`. The attempt's lease ends
/// `lease_ttl_secs` from now by the database clock, and a missing attempt
/// leaves none. Returns the number of rows written: `0` when the successor is
/// no longer an active `creating` execution.
pub async fn hand_off_to_successor(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<Uuid>,
    activation_target_state: Option<&str>,
    lease_ttl_secs: i64,
) -> Result<u64, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET state = 'recovering',
                    activation_attempt_id = $3,
                    route_activation_id = $3,
                    activation_attempt_expires_at = CASE
                        WHEN $3::UUID IS NULL THEN NULL
                        ELSE NOW() + make_interval(secs => $5)
                    END,
                    activation_attempt_target_state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND state = 'creating'"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(activation_target_state)
    .bind(lease_ttl_secs)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected())
}

/// Complete the started recovery of `sandbox_id`'s active execution at
/// `generation` or newer: move it from `recovering_state` to
/// `completion_state`. Returns the number of rows written: `0` when no such
/// execution is still recovering.
pub async fn complete(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    completion_state: &str,
    recovering_state: &str,
) -> Result<u64, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET state = $3, updated_at = NOW()
             WHERE sandbox_id = $1 AND generation >= $2 AND active AND state = $4"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(completion_state)
    .bind(recovering_state)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected())
}

/// Fail the started recovery of `sandbox_id`'s active execution: move it
/// from `recovering_state` to `failed_state` and release its activation
/// attempt. Without `activation_attempt_id` it matches only the execution at
/// `generation` that holds no attempt; with one, the execution at
/// `generation` or newer that holds that attempt. Returns the failed
/// execution's generation; `None` when no execution matches.
pub async fn fail(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    failed_state: &str,
    recovering_state: &str,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> Result<Option<i64>, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE sandbox_k8s_executions
             SET state = $3,
                 activation_attempt_id = NULL,
                 activation_attempt_expires_at = NULL,
                 activation_attempt_target_state = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND state = $4
               AND (
                 (
                   $5::UUID IS NULL
                   AND activation_attempt_id IS NULL
                   AND generation = $2
                 )
                 OR (
                   $5::UUID IS NOT NULL
                   AND activation_attempt_id = $5
                   AND generation >= $2
                 )
               )
             RETURNING generation"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(failed_state)
    .bind(recovering_state)
    .bind(activation_attempt_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
