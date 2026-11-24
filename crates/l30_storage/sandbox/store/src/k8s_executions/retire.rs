//! Retirements of a sandbox's active execution, each behind its own guard:
//! writes that set `active = FALSE`. Other modules also retire an execution
//! as part of their own transitions.
//!
//! With the `(sandbox_id) WHERE active` unique index these writes keep a
//! sandbox to one live execution: a caller that replaces an execution
//! retires it before it claims the next, in the same transaction, or the
//! claim's insert finds the old one still active and claims nothing. Each
//! verb reports whether it retired a row; the caller decides what follows,
//! such as invalidating the sandbox's open port-exposure observations.

use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// The identity of the execution a [`finish_fenced`] retire may end, as the
/// caller observed it. `None` is a value the caller observed, not an absence
/// of one: each field matches with `IS NOT DISTINCT FROM`. An execution's
/// `generation` is never `NULL`, so a fence with `generation: None` matches
/// none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionFence<'a> {
    pub generation: Option<i64>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    /// Compared against the row's pod UID with blanks read as `NULL`.
    pub pod_uid: Option<&'a str>,
    pub route_activation_id: Option<SandboxActivationAttemptId>,
}

/// Retire `sandbox_id`'s active execution, whichever it is, as `state`,
/// clearing its pod IP and keeping any `terminated_at` it already has.
/// `false` when the sandbox has no active execution.
pub async fn finish_active(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    state: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active"
    ))
    .bind(state)
    .bind(sandbox_id)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_active`], only while the active execution still matches every
/// field of `fence`. `false` when it doesn't, or there is none: a successor
/// that took the generation over is left as it is.
pub async fn finish_fenced(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    fence: ExecutionFence<'_>,
    state: &str,
) -> Result<bool, sqlx::Error> {
    let ExecutionFence {
        generation,
        activation_attempt_id,
        pod_uid,
        route_activation_id,
    } = fence;
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation IS NOT DISTINCT FROM $3
               AND activation_attempt_id IS NOT DISTINCT FROM $4
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $5::TEXT
               AND route_activation_id IS NOT DISTINCT FROM $6"
    ))
    .bind(state)
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(pod_uid)
    .bind(route_activation_id)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_active`], only while the active execution is the one at
/// `generation`. `false` when it isn't, or there is none.
pub async fn finish_generation(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2 AND active AND generation = $3"
    ))
    .bind(state)
    .bind(sandbox_id)
    .bind(generation)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_generation`], only while that execution is still held by
/// `activation_attempt_id`. `false` when it isn't.
pub async fn finish_generation_attempt(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    state: &str,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = $1,
                 pod_ip = NULL,
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $2
               AND active
               AND generation = $3
               AND activation_attempt_id = $4"
    ))
    .bind(state)
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor)
    .await?
    .rows_affected()
        > 0)
}

/// Retire `sandbox_id`'s active execution at `generation` as `replaced`
/// while it is still an unmaterialized `creating` claim (no pod UID or IP)
/// held by `activation_attempt_id` (`None` matches an execution holding
/// none). Its pod IP is already `NULL`, so it is left as it is. `false` when
/// no execution matches, such as one a concurrent attempt materialized.
pub async fn abandon_unmaterialized_create_claim(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET active = FALSE,
                 state = 'replaced',
                 terminated_at = COALESCE(terminated_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND state = 'creating'
               AND pod_uid IS NULL
               AND pod_ip IS NULL"
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
