//! The activation fence on `sandbox_k8s_executions`: the activation attempt
//! that owns an execution (`activation_attempt_id`, its lease
//! `activation_attempt_expires_at` and its `activation_attempt_target_state`),
//! and the route activation it publishes (`route_activation_id`).
//!
//! An activation claims the fence on the active execution under its
//! sandbox's row lock, reading the current owner first with
//! [`lock_attempt_owner`] and then writing one of the claims, chosen by how
//! it publishes its route: [`claim_attempt_with_published_route`],
//! [`claim_attempt_initializing_route`] or
//! [`claim_attempt_adopting_legacy_route`]. A legacy resident Wake publishes
//! its route once the node acknowledged it, with
//! [`publish_acknowledged_legacy_route`]. The fence comes off with
//! [`complete_attempt`] once the sandbox reached the attempt's target,
//! [`release_finished_attempt`] once the attempt is known finished,
//! [`give_up_attempt`] when its Wake was never dispatched, and
//! [`abandon_attempt_superseded_by_suspend`] when a suspend overtook it.
//!
//! Each write keeps its guard whole and returns its outcome: matching no row
//! means another writer moved the execution on, and the Kubernetes store
//! that calls these decides what that means. It keeps the transactions, the
//! sandbox row lock taken before the execution's, and the choice of claim.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Publish `activation_attempt_id` as the route of the active execution at
/// `generation` that tracks `expected_pod_uid`, once a legacy resident's node
/// acknowledged its Wake. Writes an **operation step** (the route's
/// publication). Returns the published route, or `None` when the execution
/// moved on: it is no longer active, another attempt owns it, it already
/// carries a route, or it tracks another pod.
pub async fn publish_acknowledged_legacy_route(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    expected_pod_uid: &str,
) -> Result<Option<Uuid>, DbError> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE sandbox_k8s_executions
                SET route_activation_id = $3,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND activation_attempt_id = $3
                AND route_activation_id IS NULL
                AND pod_uid = $4
              RETURNING route_activation_id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(expected_pod_uid)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Clear the fence `activation_attempt_id` holds on the active execution at
/// `generation`, once the execution has a published route and its sandbox's
/// lifecycle is the attempt's target. Writes an **operation step** (the
/// attempt's completion). Returns the number of rows changed: `0` when the
/// attempt no longer holds the fence, the route is unpublished, or the
/// sandbox is not in the attempt's target lifecycle.
pub async fn complete_attempt(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<u64, DbError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND execution.route_activation_id IS NOT NULL
                AND LOWER(TRIM(sandbox.lifecycle)) = execution.activation_attempt_target_state"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(changed.rows_affected())
}

/// Clear the fence `activation_attempt_id` holds on the active execution at
/// `generation`, whatever the sandbox's lifecycle, once the caller knows the
/// attempt is finished. Writes an **operation step** (the attempt's release).
/// Returns the number of rows changed: `0` when the attempt no longer holds
/// the fence.
pub async fn release_finished_attempt(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<u64, DbError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(changed.rows_affected())
}

/// Clear the fence `activation_attempt_id` holds on the active execution at
/// `generation` when its Wake was never dispatched, keeping
/// `activation_attempt_expires_at` as the time ownership ended: now, or the
/// lease's own expiry if that already passed. Writes an **operation step**
/// (the attempt's give-up). Returns the number of rows changed: `0` when the
/// attempt no longer holds the fence.
pub async fn give_up_attempt(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<u64, DbError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                    activation_attempt_target_state = NULL
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(changed.rows_affected())
}

/// Clear the fence `activation_attempt_id` holds on the active execution at
/// `generation` while its sandbox is `suspended` or `restoring`: a suspend
/// overtook the activation. Writes an **operation step** (the attempt's
/// abandonment). Returns the number of rows changed: `0` when the attempt no
/// longer holds the fence or the sandbox is in any other lifecycle.
pub async fn abandon_attempt_superseded_by_suspend(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<u64, DbError> {
    let changed = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND LOWER(TRIM(sandbox.lifecycle)) IN ('suspended', 'restoring')"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(changed.rows_affected())
}

/// The fence on an active execution, as [`lock_attempt_owner`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct AttemptOwner {
    pub activation_attempt_id: Option<Uuid>,
    /// Raw `activation_attempt_target_state`, as stored.
    pub activation_attempt_target_state: Option<String>,
    /// Seconds left on the attempt's lease by the database clock, never
    /// negative: `0` once the lease expired, or when there is none.
    pub lease_remaining_secs: f64,
    pub route_activation_id: Option<Uuid>,
}

/// Lock the active execution of `sandbox_id` at `generation` `FOR UPDATE`
/// and read its fence. `None` when that generation isn't the active
/// execution. The lock lasts until the caller's transaction ends. The
/// caller must pass its open transaction.
///
/// The caller holds its sandbox's row lock first, then claims the fence
/// with one of the `claim_attempt_…` writes in the same transaction.
pub async fn lock_attempt_owner<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<Option<AttemptOwner>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let row = sqlx::query_as::<_, (Option<Uuid>, Option<String>, f64, Option<Uuid>)>(sql!(
        "SELECT activation_attempt_id,
                    activation_attempt_target_state,
                    GREATEST(
                        EXTRACT(EPOCH FROM (activation_attempt_expires_at - NOW())),
                        0.0
                    )::float8,
                    route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(conn.into_sqlx())
    .await?;
    Ok(row.map(
        |(
            activation_attempt_id,
            activation_attempt_target_state,
            lease_remaining_secs,
            route_activation_id,
        )| AttemptOwner {
            activation_attempt_id,
            activation_attempt_target_state,
            lease_remaining_secs,
            route_activation_id,
        },
    ))
}

/// Claim the fence of the active execution at `generation`, which already
/// carries a published route, for `activation_attempt_id` toward
/// `target_state`, leased for `lease_ttl_secs` from now. Writes an
/// **operation step** (the attempt's claim). Returns the claimed attempt, or
/// `None` when that generation isn't active or has no published route.
///
/// It checks no current owner and replaces whichever attempt holds the
/// fence, so the caller runs it only after [`lock_attempt_owner`], in the
/// same transaction, found no live owner. The caller must pass its open
/// transaction.
pub async fn claim_attempt_with_published_route<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    target_state: &str,
    lease_ttl_secs: i64,
) -> Result<Option<Uuid>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NOT NULL
                      RETURNING activation_attempt_id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(target_state)
    .bind(lease_ttl_secs)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Claim the fence of the active execution at `generation` for
/// `activation_attempt_id` toward `target_state`, leased for
/// `lease_ttl_secs` from now, and publish that attempt as its first route.
/// Only an execution still `creating` or `recovering`, with no pod UID and no
/// route, qualifies. Writes an **operation step** (the attempt's claim and
/// the route's publication). Returns the claimed attempt, or `None` when the
/// execution doesn't qualify.
///
/// It checks no current owner and replaces whichever attempt holds the
/// fence, so the caller runs it only after [`lock_attempt_owner`], in the
/// same transaction, found no live owner. The caller must pass its open
/// transaction.
pub async fn claim_attempt_initializing_route<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    target_state: &str,
    lease_ttl_secs: i64,
) -> Result<Option<Uuid>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            route_activation_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND state IN ('creating', 'recovering')
                        AND NULLIF(BTRIM(pod_uid), '') IS NULL
                        AND route_activation_id IS NULL
                      RETURNING activation_attempt_id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(target_state)
    .bind(lease_ttl_secs)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

/// Claim the fence of a legacy resident, the active execution at
/// `generation` that has a pod UID but no route, for `activation_attempt_id`
/// toward `target_state`, leased for `lease_ttl_secs` from now. The route
/// stays unpublished until [`publish_acknowledged_legacy_route`]. Writes an
/// **operation step** (the attempt's claim). Returns the claimed attempt, or
/// `None` when the execution isn't such a resident.
///
/// It checks no current owner and replaces whichever attempt holds the
/// fence, so the caller runs it only after [`lock_attempt_owner`], in the
/// same transaction, found no live owner. The caller must pass its open
/// transaction.
pub async fn claim_attempt_adopting_legacy_route<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    target_state: &str,
    lease_ttl_secs: i64,
) -> Result<Option<Uuid>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NULL
                        AND NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                      RETURNING activation_attempt_id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(target_state)
    .bind(lease_ttl_secs)
    .fetch_optional(conn.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
