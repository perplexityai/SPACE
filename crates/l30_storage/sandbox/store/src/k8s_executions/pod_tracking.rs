//! Guarded writes the Kubernetes substrate's pod tracking makes to the
//! active execution row: filling in a pod UID that was never persisted, and
//! clearing the pod a failed recovery no longer has.
//!
//! The `sandboxes` row's own pod-tracking writes are in
//! [`crate::pod_tracking`]. The caller keeps its transaction, the
//! `sandboxes` row lock it takes first when it takes one, and what to do
//! with each outcome.

use pplx_space_sandbox_model::{ExecutionGeneration, SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;

/// Set `pod_uid` on the active execution of `sandbox_id`, and nothing else
/// but the stamps that go with it, only while that row still has no UID and
/// carries exactly the `generation`, `activation_attempt_id` and
/// `route_activation_id` the caller observed (a `NULL` attempt or route
/// matching `None`; every row has a generation, so a `None` generation
/// matches no row). The first UID stamps `pod_materialized_at` if it was
/// unset.
///
/// Runs in the caller's transaction. It takes a connection rather than any
/// executor because the write is correct only inside that transaction.
/// `false` when the row has moved on (another UID, attempt, route or
/// generation, or none active), and nothing changes.
pub async fn backfill_missing_pod_uid(
    conn: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    generation: Option<i64>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    pod_uid: &str,
    route_activation_id: Option<SandboxActivationAttemptId>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET pod_uid = $4,
                    pod_materialized_at = COALESCE(
                        pod_materialized_at,
                        CASE WHEN NULLIF(BTRIM($4::TEXT), '') IS NOT NULL THEN NOW() END
                    ),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation IS NOT DISTINCT FROM $2
                AND active
                AND activation_attempt_id IS NOT DISTINCT FROM $3
                AND NULLIF(BTRIM(pod_uid), '') IS NULL
                AND route_activation_id IS NOT DISTINCT FROM $5"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(pod_uid)
    .bind(route_activation_id)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0)
}

/// Clear the pod UID, IP and Kubernetes node of the active execution of
/// `sandbox_id` at `generation`, only while it is in `state` and still
/// records exactly the pod the caller found absent (`expected_pod_uid`,
/// `expected_pod_ip`, `expected_k8s_node_name`; `NULL`, or a blank UID,
/// matching `None`). Clearing the UID stamps `pod_materialized_at` first if
/// the row held one, so the record that a pod ran outlives the UID.
///
/// Runs in the caller's transaction, under its `sandboxes` row lock. It
/// takes a connection rather than any executor because the write is correct
/// only inside that transaction. `false` when the row no longer matches, and
/// nothing changes.
pub async fn clear_absent_recovery_pod(
    conn: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    state: &str,
    expected_pod_uid: Option<&str>,
    expected_pod_ip: Option<&str>,
    expected_k8s_node_name: Option<&str>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 k8s_node_name = NULL,
                 pod_materialized_at = COALESCE(
                     pod_materialized_at,
                     CASE WHEN NULLIF(BTRIM(pod_uid), '') IS NOT NULL THEN NOW() END
                 ),
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND generation = $2
               AND active
               AND state = $3
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $4
               AND pod_ip IS NOT DISTINCT FROM $5::INET
               AND k8s_node_name IS NOT DISTINCT FROM $6"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(state)
    .bind(expected_pod_uid)
    .bind(expected_pod_ip)
    .bind(expected_k8s_node_name)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0)
}

#[cfg(test)]
mod tests;
