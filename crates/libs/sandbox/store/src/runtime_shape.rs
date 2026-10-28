//! Fill omitted allocations from the current runtime without replacing explicit requests.
//!
//! These are guarded transitions, not desired-configuration updates. Publication
//! retains its transaction so allocation and readiness become visible together.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxActivationAttemptId, SandboxId, SubstrateKind};
use sqlx::{
    PgExecutor,
    types::{Json, JsonValue},
};

/// Allocation and exact execution identity returned by a ready create's node.
pub struct CreatedAllocation<'a> {
    pub sandbox_id: SandboxId,
    pub node_id: NodeId,
    pub generation: i64,
    pub pod_name: &'a str,
    pub pod_uid: &'a str,
    pub activation_attempt_id: SandboxActivationAttemptId,
    pub vcpu: i32,
    pub memory_mib: i32,
}

/// Fill only missing fields if the ready create still owns this execution.
/// A false result means the execution fence was lost.
pub async fn fill_created<'e>(
    executor: impl PgExecutor<'e>,
    allocation: &CreatedAllocation<'_>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes s
         SET vcpu = COALESCE(s.vcpu, $7), memory_mib = COALESCE(s.memory_mib, $8)
         FROM sandbox_k8s_executions e
         WHERE s.id = $1 AND s.active_node_id = $2
           AND s.lifecycle IN ('creating', 'started')
           AND e.sandbox_id = s.id AND e.active
           AND e.generation = $3 AND e.pod_name = $4 AND e.pod_uid = $5
           AND COALESCE(e.activation_attempt_id, e.route_activation_id) = $6"
    ))
    .bind(allocation.sandbox_id)
    .bind(allocation.node_id)
    .bind(allocation.generation)
    .bind(allocation.pod_name)
    .bind(allocation.pod_uid)
    .bind(allocation.activation_attempt_id.as_uuid())
    .bind(allocation.vcpu)
    .bind(allocation.memory_mib)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Fill allocations from an authenticated node's normalized publication rows.
/// Ignore incomplete, unready or superseded activations; the caller publishes
/// lifecycle readiness in the same transaction after this operation.
pub async fn fill_published<'e>(
    executor: impl PgExecutor<'e>,
    node_id: NodeId,
    observations: Json<JsonValue>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes s
         SET vcpu = COALESCE(s.vcpu, input.actual_vcpu),
             memory_mib = COALESCE(s.memory_mib, input.actual_memory_mib)
         FROM jsonb_to_recordset($2::jsonb) AS input(
             sandbox_id UUID, status TEXT, execution_generation BIGINT,
             pod_uid TEXT, activation_attempt_id UUID,
             actual_vcpu INTEGER, actual_memory_mib INTEGER
         ), sandbox_k8s_executions e
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1 AND s.managed_by = $3
           AND s.lifecycle IN ('creating', 'restoring', 'started', 'paused')
           AND (s.vcpu IS NULL OR s.memory_mib IS NULL)
           AND input.status IN ('started', 'paused')
           AND input.actual_vcpu > 0 AND input.actual_memory_mib > 0
           AND e.sandbox_id = s.id AND e.active
           AND e.generation = input.execution_generation
           AND e.pod_uid = input.pod_uid
           AND COALESCE(e.activation_attempt_id, e.route_activation_id) = input.activation_attempt_id"
    ))
    .bind(node_id)
    .bind(observations)
    .bind(SubstrateKind::Kubernetes)
    .execute(executor)
    .await?;
    Ok(())
}
