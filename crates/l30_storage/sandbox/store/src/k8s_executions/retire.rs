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

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;

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
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    state: &str,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_active`], only while the active execution still matches every
/// field of `fence`. `false` when it doesn't, or there is none: a successor
/// that took the generation over is left as it is.
pub async fn finish_fenced(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    fence: ExecutionFence<'_>,
    state: &str,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_active`], only while the active execution is the one at
/// `generation`. `false` when it isn't, or there is none.
pub async fn finish_generation(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// [`finish_generation`], only while that execution is still held by
/// `activation_attempt_id`. `false` when it isn't.
pub async fn finish_generation_attempt(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    state: &str,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

/// Observed pod-tracking state of the execution row a
/// [`finish_generation_observed_attempt`] retire may end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedAttemptPodFence<'a> {
    /// Observed `pod_uid` (`None` when the row carried no non-blank UID).
    pub observed_pod_uid: Option<&'a str>,
    /// When `true` alongside `Some(uid)`, also matches an initial
    /// unmaterialized claim (`pod_uid` blank/`NULL` and `pod_materialized_at`
    /// unset) so cleanup after deleting a UID-less-at-persist pod retires both
    /// a readiness-repaired row (`pod_uid = uid`) and an unrepaired claim,
    /// while rejecting a reset row (`pod_materialized_at IS NOT NULL`) or a
    /// different UID.
    pub allow_unmaterialized_claim: bool,
    /// When `observed_pod_uid` is `None` and `Some(expected)` is supplied,
    /// requires `(pod_materialized_at IS NOT NULL) = expected`.
    pub observed_pod_materialized_at_set: Option<bool>,
    /// When `observed_pod_uid` is `None` and `Some(ts)` is supplied, requires
    /// `updated_at = ts` so a subsequent `reset_to_creating` or
    /// `claim_pod_materialization` cannot be retired by a stale observation.
    pub observed_null_uid_updated_at: Option<time::OffsetDateTime>,
}

impl<'a> ObservedAttemptPodFence<'a> {
    #[must_use]
    pub const fn exact_uid(observed_pod_uid: Option<&'a str>) -> Self {
        Self {
            observed_pod_uid,
            allow_unmaterialized_claim: false,
            observed_pod_materialized_at_set: None,
            observed_null_uid_updated_at: None,
        }
    }

    #[must_use]
    pub const fn observed_row(
        pod_uid: Option<&'a str>,
        pod_materialized_at_set: bool,
        updated_at: time::OffsetDateTime,
    ) -> Self {
        let null_uid = pod_uid.is_none();
        Self {
            observed_pod_uid: pod_uid,
            allow_unmaterialized_claim: false,
            observed_pod_materialized_at_set: if null_uid {
                Some(pod_materialized_at_set)
            } else {
                None
            },
            observed_null_uid_updated_at: if null_uid { Some(updated_at) } else { None },
        }
    }

    #[must_use]
    pub const fn deleted_create_pod(
        pod_uid: Option<&'a str>,
        unpersisted_uid_updated_at: Option<time::OffsetDateTime>,
    ) -> Self {
        Self {
            observed_pod_uid: pod_uid,
            allow_unmaterialized_claim: unpersisted_uid_updated_at.is_some(),
            observed_pod_materialized_at_set: if pod_uid.is_none() { Some(false) } else { None },
            observed_null_uid_updated_at: unpersisted_uid_updated_at,
        }
    }
}

/// [`finish_generation`], only while that execution's `activation_attempt_id`
/// and pod-tracking state still match `pod_fence`.
pub async fn finish_generation_observed_attempt(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    pod_fence: ObservedAttemptPodFence<'_>,
    state: &str,
) -> Result<bool, DbError> {
    let ObservedAttemptPodFence {
        observed_pod_uid,
        allow_unmaterialized_claim,
        observed_pod_materialized_at_set,
        observed_null_uid_updated_at,
    } = pod_fence;
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
               AND activation_attempt_id IS NOT DISTINCT FROM $4
               AND CASE
                   WHEN NULLIF(BTRIM($5::TEXT), '') IS NOT NULL THEN
                       NULLIF(BTRIM(pod_uid), '') = NULLIF(BTRIM($5::TEXT), '')
                       OR (
                           $6::BOOL
                           AND NULLIF(BTRIM(pod_uid), '') IS NULL
                           AND pod_materialized_at IS NULL
                           AND ($8::TIMESTAMPTZ IS NULL OR updated_at = $8::TIMESTAMPTZ)
                       )
                   ELSE
                       NULLIF(BTRIM(pod_uid), '') IS NULL
                       AND ($7::BOOL IS NULL OR (pod_materialized_at IS NOT NULL) = $7::BOOL)
                       AND ($8::TIMESTAMPTZ IS NULL OR updated_at = $8::TIMESTAMPTZ)
               END"
    ))
    .bind(state)
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(observed_pod_uid)
    .bind(allow_unmaterialized_claim)
    .bind(observed_pod_materialized_at_set)
    .bind(observed_null_uid_updated_at)
    .execute(executor.into_sqlx())
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
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
