//! Guarded writes the Kubernetes substrate's pod placement makes to the
//! active execution row: refreshing the dial address a selected route reads,
//! and claiming the row for a new pod's materialization.
//!
//! The caller keeps the executor each write runs on, the `sandboxes` row
//! lock it takes before a write that needs one, and what to do with each
//! outcome.
//!
//! [`k8s_executions::materialize::record_pod_metadata`](super::materialize::record_pod_metadata)
//! and [`k8s_executions::claim::reset_to_creating`](super::claim::reset_to_creating)
//! write the same pod columns.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;

/// Record `pod_ip` on the active execution of `sandbox_id` at `generation`,
/// only while the complete selected route still owns it: the row names
/// `pod_name`, its route activation (or, before one is published, its
/// activation attempt) is `route_activation_id`, and it tracks `pod_uid`. A
/// `created` row becomes `running`; every other state is kept, and
/// `updated_at` moves only when the address or the state changes.
///
/// `false` when the route no longer owns the row, and nothing changes.
pub async fn refresh_selected_route_pod_ip(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_name: &str,
    route_activation_id: SandboxActivationAttemptId,
    pod_uid: &str,
    pod_ip: &str,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET pod_ip = $1::INET,
                 state = CASE WHEN state = 'created' THEN 'running' ELSE state END,
                 updated_at = CASE
                     WHEN pod_ip IS DISTINCT FROM $1::INET OR state = 'created' THEN NOW()
                     ELSE updated_at
                 END
             WHERE sandbox_id = $2
               AND generation = $3
               AND active
               AND pod_name = $4
               AND COALESCE(route_activation_id, activation_attempt_id) = $5
               AND NULLIF(BTRIM(pod_uid), '') = $6
             RETURNING 1"
    ))
    .bind(pod_ip)
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .bind(route_activation_id.as_uuid())
    .bind(pod_uid)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(result.is_some())
}

/// Claim the active execution of `sandbox_id` at `generation` for a new
/// pod's materialization: forget its pod UID and IP, and move it to
/// `creating` unless it is `recovering`, which keeps its state. Clearing the
/// UID stamps `pod_materialized_at` first if the row held one, so the record
/// that a pod ran outlives the UID.
///
/// Runs in the caller's transaction, under the `sandboxes` row lock it took
/// for the lifecycle it expects. It refuses a pool because the write is
/// correct only inside that transaction, and the caller must pass its open
/// transaction. `false` when that generation is no longer the active
/// execution, and nothing changes.
pub async fn claim_pod_materialization<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET state = CASE WHEN state = 'recovering' THEN state ELSE 'creating' END,
                 pod_uid = NULL,
                 pod_ip = NULL,
                 pod_materialized_at = COALESCE(
                     pod_materialized_at,
                     CASE WHEN NULLIF(BTRIM(pod_uid), '') IS NOT NULL THEN NOW() END
                 ),
                 updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
             RETURNING 1"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_optional(conn.into_sqlx())
    .await?
    .is_some())
}

#[cfg(test)]
mod tests;
