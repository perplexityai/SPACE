//! The `drop_sandbox_memory` operation: the admin route
//! `POST /admin/api/v1/sandboxes/{id}:drop-memory` enqueues it, and the cluster
//! controller runs it through [`MemoryDrop`], since only the cluster
//! controller has the object store.

use std::time::Duration;

use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicOperationResult, PublicSandboxId,
    PublicSandboxOperationResult, SandboxLifecycleState,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::controllers::sandbox::memory_drop::{MemoryDrop, MemoryDropOutcome, MemoryDropped};
use crate::public_operations::{ClaimedPublicOperation, PublicOperationRequeueReason};
use crate::public_sandbox_operations::PublicOperationExecutionResult;

/// How long a retryable failure waits before the dispatcher claims the
/// operation again.
const MEMORY_DROP_RETRY_AFTER: Duration = Duration::from_secs(10);

/// The operation's payload. `reason` is the operator's, required by the
/// route; it rides along so the operation row records why.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxMemoryDropOperationPayload {
    pub sandbox_id: PublicSandboxId,
    pub reason: String,
}

impl SandboxMemoryDropOperationPayload {
    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("memory drop operation payload serializes")
    }
}

/// Runs `drop_sandbox_memory` operations.
#[derive(Clone)]
pub struct SandboxMemoryDropOperationExecutor {
    pool: PgPool,
    /// `None` when the switch is off on this cluster controller.
    memory_drop: Option<MemoryDrop>,
}

impl SandboxMemoryDropOperationExecutor {
    pub fn new(pool: PgPool, memory_drop: Option<MemoryDrop>) -> Self {
        Self { pool, memory_drop }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        let payload = match serde_json::from_value::<SandboxMemoryDropOperationPayload>(
            operation.payload.clone(),
        ) {
            Ok(payload) => payload,
            Err(error) => {
                return terminal_failure(
                    operation,
                    PublicErrorCode::InvalidRequest,
                    "memory drop operation payload is invalid",
                    serde_json::json!({
                        "operation_id": operation.id.to_string(),
                        "reason": "invalid_operation_payload",
                        "error": error.to_string(),
                    }),
                );
            }
        };
        let sandbox_id = payload.sandbox_id;
        // The route refuses while its own switch is off, but the API and the
        // cluster controller read theirs separately.
        let Some(memory_drop) = &self.memory_drop else {
            return terminal_failure(
                operation,
                PublicErrorCode::ServiceUnavailable,
                "the memory drop is disabled on this cluster controller",
                serde_json::json!({
                    "operation_id": operation.id.to_string(),
                    "sandbox_id": sandbox_id.to_string(),
                    "reason": "memory_drop_disabled",
                }),
            );
        };
        tracing::info!(
            operation_id = %operation.id,
            %sandbox_id,
            reason = payload.reason,
            "dropping a suspended sandbox's memory"
        );
        match memory_drop.drop_memory(&self.pool, sandbox_id.into()).await {
            Ok(MemoryDropOutcome::Dropped(dropped)) => {
                tracing::info!(
                    operation_id = %operation.id,
                    %sandbox_id,
                    disk_snapshot_id = %dropped.disk_snapshot_id,
                    retained_snapshot_id = %dropped.retained_snapshot_id,
                    "dropped a suspended sandbox's memory"
                );
                succeeded(sandbox_id, Some(dropped))
            }
            // A retry after a commit this attempt didn't see.
            Ok(MemoryDropOutcome::AlreadyDropped) => succeeded(sandbox_id, None),
            Ok(MemoryDropOutcome::NotEligible(ineligible)) => {
                let reason = ineligible.reason();
                terminal_failure(
                    operation,
                    PublicErrorCode::Conflict,
                    format!("the sandbox's memory can't be dropped: {reason}"),
                    serde_json::json!({
                        "operation_id": operation.id.to_string(),
                        "sandbox_id": sandbox_id.to_string(),
                        "reason": reason,
                    }),
                )
            }
            Err(error) => {
                tracing::warn!(
                    operation_id = %operation.id,
                    %sandbox_id,
                    %error,
                    "memory drop failed; retrying"
                );
                PublicOperationExecutionResult {
                    state: OperationState::Queued,
                    progress_message: format!("memory drop is retrying: {error}"),
                    result_json: None,
                    terminal_error_code: Some(PublicErrorCode::ServiceUnavailable),
                    terminal_error_json: Some(serde_json::json!({
                        "operation_id": operation.id.to_string(),
                        "sandbox_id": sandbox_id.to_string(),
                        "reason": "memory_drop_retryable_failure",
                        "error": error.to_string(),
                    })),
                    retry_after: Some(MEMORY_DROP_RETRY_AFTER),
                    requeue_reason: PublicOperationRequeueReason::TransientFailure,
                }
            }
        }
    }
}

/// The result of a drop that committed, now or on an earlier attempt. The ids
/// are those of the attempt that committed; a retry that finds the drop done
/// has none to report.
fn succeeded(
    sandbox_id: PublicSandboxId,
    dropped: Option<MemoryDropped>,
) -> PublicOperationExecutionResult {
    let controller_response = dropped.map(|dropped| {
        serde_json::json!({
            "disk_snapshot_id": dropped.disk_snapshot_id.to_string(),
            "disk_artifact_id": dropped.disk_artifact_id.to_string(),
            "retained_snapshot_id": dropped.retained_snapshot_id.to_string(),
            "retained_artifact_id": dropped.retained_artifact_id.to_string(),
        })
    });
    PublicOperationExecutionResult {
        state: OperationState::Succeeded,
        progress_message: "sandbox memory dropped".to_string(),
        result_json: Some(
            serde_json::to_value(PublicOperationResult::Sandbox(
                PublicSandboxOperationResult {
                    sandbox_id,
                    state: SandboxLifecycleState::Suspended,
                    action: None,
                    controller_response,
                },
            ))
            .expect("sandbox operation result serializes"),
        ),
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn terminal_failure(
    operation: &ClaimedPublicOperation,
    code: PublicErrorCode,
    message: impl Into<String>,
    error_json: serde_json::Value,
) -> PublicOperationExecutionResult {
    let message = message.into();
    tracing::warn!(
        operation_id = %operation.id,
        kind = %operation.kind,
        message,
        "memory drop operation failed"
    );
    PublicOperationExecutionResult {
        state: OperationState::Failed,
        progress_message: message,
        result_json: None,
        terminal_error_code: Some(code),
        terminal_error_json: Some(error_json),
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}
