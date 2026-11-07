//! The drain restore operation ([`StoredOperationKind::DrainRestoreSandbox`]):
//! one per sandbox a forced v2 node drain suspended, which brings the sandbox
//! back in the lifecycle it had, `started` or `paused`, from the suspend
//! snapshot the drain left. The drain's v2 loop enqueues it once the node
//! reports the snapshot durable.
//!
//! Each attempt claims the row as a user's restore does, through the shared
//! restore executor, placed as the drain's evacuation is, without a runtime
//! upgrade and exempt from the activation charge
//! (`KubernetesSubstrate::restore_drained_execution`). A failed attempt
//! reverts the row to `suspended` and the operation retries. It ends
//! `succeeded` once the sandbox is back, `canceled` once the restore is no
//! longer the system's to make, and `failed` once it can't be made: the
//! snapshot is lost, or its attempts are spent, the row left `suspended`.
//!
//! A user's lifecycle request supersedes it while it waits (see
//! `public_operations::drain_restore_supersession`); one that arrives while
//! its attempt holds the row waits behind it, or meets that restore.

use std::time::Duration;

use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicOperationResult, PublicSandboxId,
    PublicSandboxOperationResult, SandboxLifecycleAction,
};
use pplx_space_sandbox_store::drain_restore::DrainRestoreRefusal;
use pplx_space_sandbox_store::sweeper::EvacuationRestoreTarget;
use serde::{Deserialize, Serialize};
#[cfg(any(test, feature = "test-hooks"))]
use sqlx::PgPool;
use uuid::Uuid;

use std::sync::Arc;

use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::kubernetes::{
    DrainRestore, DrainRestoreAttempt, KubernetesSubstrate,
};
use crate::public_operations::{
    ClaimedPublicOperation, PublicOperationRequeueReason, StoredOperationKind,
};
use crate::public_sandbox_operations::PublicOperationExecutionResult;
use crate::public_sandbox_projection::public_lifecycle_state;

/// Whether this process runs with the drain's v2 on (`SPACE_DRAIN_V2_ENABLED`),
/// set once at startup. Off, nothing enqueues a drain restore, so a user's
/// call skips looking for one to supersede.
static DRAIN_V2_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Record at startup whether the drain's v2 is on in this process.
pub fn set_drain_v2_enabled(enabled: bool) {
    DRAIN_V2_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Whether the drain's v2 is on in this process ([`set_drain_v2_enabled`]).
pub fn drain_v2_enabled() -> bool {
    DRAIN_V2_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// What a drain restore operation carries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DrainRestoreOperationPayload {
    pub sandbox_id: PublicSandboxId,
    /// The lifecycle to land the sandbox in, `started` or `paused`.
    pub target: EvacuationRestoreTarget,
    /// S, the suspend snapshot the drain left.
    pub snapshot_id: Uuid,
}

impl DrainRestoreOperationPayload {
    pub fn new(sandbox_id: SandboxId, target: EvacuationRestoreTarget, snapshot_id: Uuid) -> Self {
        Self {
            sandbox_id: sandbox_id.into(),
            target,
            snapshot_id,
        }
    }

    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("drain restore operation payload serializes")
    }
}

/// Enqueue the drain restore of `payload`'s sandbox, which `org_id` owns, in
/// the caller's transaction, unless one for the same sandbox and snapshot
/// exists already, in any state: the drain's v2 loop calls this for every
/// evacuation the node reports, on every tick and after a restart, and one
/// restore per snapshot is all it means. The caller holds the sandbox's row
/// lock, which orders two of these calls. The lookup leads with the org and
/// target type, as `idx_public_operations_target` does. Returns the
/// operation and whether this call created it.
pub async fn enqueue_drain_restore_once(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    org_id: pplx_space_common_core::types::OrgId,
    payload: DrainRestoreOperationPayload,
) -> Result<
    (pplx_space_public_api::OperationId, bool),
    crate::public_operations::PublicOperationStoreError,
> {
    use crate::public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationResult, create_operation_tx,
    };
    let target_id = payload.sandbox_id.to_string();
    let existing: Option<Uuid> = sqlx::query_scalar(pplx_space_common_core::sql!(
        "SELECT operation_id
           FROM public_operations
          WHERE org_id = $4
            AND target_type = $5
            AND target_id = $2
            AND kind = $1
            AND payload_json->>'snapshot_id' = $3
          ORDER BY created_at
          LIMIT 1"
    ))
    .bind(StoredOperationKind::DrainRestoreSandbox.as_str())
    .bind(&target_id)
    .bind(payload.snapshot_id.to_string())
    .bind(org_id)
    .bind(pplx_space_public_api::OperationTargetType::Sandbox.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(operation_id) = existing {
        return Ok((
            pplx_space_public_api::OperationId::from_uuid(operation_id),
            false,
        ));
    }
    let (created, _) = create_operation_tx(
        tx,
        CreatePublicOperation {
            org_id,
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: StoredOperationKind::DrainRestoreSandbox,
            target_type: Some(pplx_space_public_api::OperationTargetType::Sandbox),
            target_id: Some(&target_id),
            payload_json: payload.into_json(),
            idempotency: None,
            traceparent: None,
        },
    )
    .await?;
    match created {
        CreatePublicOperationResult::Created(operation_id) => Ok((operation_id, true)),
        other => unreachable!("an insert without idempotency creates: {other:?}"),
    }
}

/// [`enqueue_drain_restore_once`] in a transaction of its own, as tests do.
#[cfg(any(test, feature = "test-hooks"))]
pub async fn enqueue_drain_restore(
    pool: &PgPool,
    org_id: pplx_space_common_core::types::OrgId,
    payload: DrainRestoreOperationPayload,
) -> Result<pplx_space_public_api::OperationId, crate::public_operations::PublicOperationStoreError>
{
    let mut tx = pool.begin().await?;
    let (operation_id, _) = enqueue_drain_restore_once(&mut tx, org_id, payload).await?;
    tx.commit().await?;
    Ok(operation_id)
}

/// How a drain restore's attempts are paced and bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainRestoreRetry {
    /// The attempt at which a restore that keeps failing stops: its
    /// dispatcher's retry budget, so the executor ends it with its own
    /// error before the dispatcher would fail it without one.
    pub max_attempts: i32,
    /// The wait after the first failed attempt, doubling up to `max_delay`.
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// The wait before re-reading the row when the restore is converging: a
    /// lease or execution still holds it, or the snapshot is still uploading.
    pub converging_delay: Duration,
    /// How long one attempt waits for the sandbox to reach its target.
    pub ready_timeout: Duration,
}

impl DrainRestoreRetry {
    /// The v1 drain's restore pacing: a minute after the first failure,
    /// doubling to half an hour, with its per-attempt timeout.
    pub const fn with_max_attempts(max_attempts: i32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(1800),
            converging_delay: Duration::from_secs(10),
            ready_timeout: Duration::from_secs(600),
        }
    }

    /// The wait after failed attempt `attempt` (counting from 1).
    fn backoff(self, attempt: i32) -> Duration {
        let doublings = u32::try_from(attempt.saturating_sub(1).clamp(0, 16)).unwrap_or(16);
        self.base_delay
            .saturating_mul(2_u32.saturating_pow(doublings))
            .min(self.max_delay)
    }
}

#[derive(Clone)]
pub struct DrainRestoreOperationExecutor {
    /// `None` on Direct, which has no forced drain, so nothing there enqueues
    /// a drain restore; one that is enqueued fails.
    kubernetes: Option<Arc<KubernetesSubstrate>>,
    placement: Option<EvacuationPlacement>,
    retry: DrainRestoreRetry,
}

impl DrainRestoreOperationExecutor {
    /// `placement` is the drain's evacuation cohort, when a node-group
    /// rollout confines evacuated sandboxes to one.
    pub fn new(
        kubernetes: Option<Arc<KubernetesSubstrate>>,
        placement: Option<EvacuationPlacement>,
        retry: DrainRestoreRetry,
    ) -> Self {
        Self {
            kubernetes,
            placement,
            retry,
        }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        let request = match DrainRestoreRequest::parse(operation) {
            Ok(request) => request,
            Err(reason) => return failed(operation, reason, None),
        };
        let Some(kubernetes) = self.kubernetes.as_deref() else {
            return failed(operation, "unsupported_substrate", None);
        };
        let outcome = kubernetes
            .restore_drained_execution(
                crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("drain_restore"),
                request.sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: self.retry.ready_timeout,
                },
                self.placement.as_ref(),
                request.restore(),
            )
            .await;
        let converging = |message: String| {
            retry(
                message,
                self.retry.converging_delay,
                PublicOperationRequeueReason::WorkflowProgress,
            )
        };
        match outcome {
            Ok(DrainRestoreAttempt::Landed) => succeeded(&request),
            Ok(DrainRestoreAttempt::Refused(refusal)) => match refusal {
                DrainRestoreRefusal::Deleted => canceled(operation, "sandbox_deleted"),
                DrainRestoreRefusal::Superseded => canceled(operation, "superseded"),
                DrainRestoreRefusal::SourceLost => {
                    failed(operation, "restore_source_unavailable", None)
                }
                DrainRestoreRefusal::SourcePending => converging(
                    "waiting for the drain's suspend snapshot to become restorable".into(),
                ),
                DrainRestoreRefusal::InFlight => {
                    converging("waiting for a restore of the sandbox to settle".into())
                }
            },
            // Something moved the row between the restore and its commit: the
            // next attempt reads it again and decides.
            Ok(DrainRestoreAttempt::Lost) => {
                converging("the sandbox changed while its drain restore ran".into())
            }
            // A lease or execution still holds the row: wait it out without
            // spending an attempt.
            Err(
                error @ (SubstrateError::RestoreInFlight { .. }
                | SubstrateError::ActivationInFlight { .. }
                | SubstrateError::LockContention { .. }
                | SubstrateError::EvacuationClaimLost { .. }),
            ) => converging(format!(
                "drain restore waits for the sandbox's restore to settle: {error}"
            )),
            Err(error) if operation.attempts >= self.retry.max_attempts => failed(
                operation,
                "retry_attempts_exhausted",
                Some(error.to_string()),
            ),
            Err(error) => retry(
                format!("drain restore attempt failed: {error}"),
                self.retry.backoff(operation.attempts),
                PublicOperationRequeueReason::TransientFailure,
            ),
        }
    }
}

/// A claimed drain restore's payload, checked against its operation.
#[derive(Debug, Clone, Copy)]
struct DrainRestoreRequest {
    sandbox_id: SandboxId,
    target: EvacuationRestoreTarget,
    snapshot_id: Uuid,
    /// Every attempt's restore lease token: the operation's own id.
    restore_lease_token: Uuid,
}

impl DrainRestoreRequest {
    fn parse(operation: &ClaimedPublicOperation) -> Result<Self, &'static str> {
        if operation.kind != StoredOperationKind::DrainRestoreSandbox {
            return Err("kind_mismatch");
        }
        let payload: DrainRestoreOperationPayload =
            serde_json::from_value(operation.payload.clone()).map_err(|_| "invalid_payload")?;
        // A stopped sandbox is never restored: its next start materializes
        // it.
        let target = match payload.target {
            target @ (EvacuationRestoreTarget::Started | EvacuationRestoreTarget::Paused) => target,
            EvacuationRestoreTarget::Stopped => return Err("invalid_payload"),
        };
        if operation.target_id.as_deref() != Some(payload.sandbox_id.to_string().as_str()) {
            return Err("target_mismatch");
        }
        Ok(Self {
            sandbox_id: payload.sandbox_id.into(),
            target,
            snapshot_id: payload.snapshot_id,
            restore_lease_token: operation.id.as_uuid(),
        })
    }

    fn restore(self) -> DrainRestore {
        DrainRestore {
            target: self.target,
            snapshot_id: self.snapshot_id,
            restore_lease_token: self.restore_lease_token,
        }
    }
}

/// The result a user's restore of the sandbox reports: a client sees a drain
/// restore as a `restore_sandbox`.
fn succeeded(request: &DrainRestoreRequest) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult {
        state: OperationState::Succeeded,
        progress_message: format!("sandbox restored {}", request.target),
        result_json: Some(
            serde_json::to_value(PublicOperationResult::Sandbox(
                PublicSandboxOperationResult {
                    sandbox_id: request.sandbox_id.into(),
                    state: public_lifecycle_state(request.target.lifecycle()),
                    action: Some(SandboxLifecycleAction::Restore),
                    controller_response: None,
                },
            ))
            .expect("public operation result serializes"),
        ),
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn canceled(
    operation: &ClaimedPublicOperation,
    reason: &'static str,
) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult {
        state: OperationState::Canceled,
        progress_message: format!("Canceled: the restore is no longer needed ({reason})."),
        result_json: None,
        terminal_error_code: Some(PublicErrorCode::OperationFailed),
        terminal_error_json: Some(serde_json::json!({
            "operation_id": operation.id.to_string(),
            "reason": reason,
        })),
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn failed(
    operation: &ClaimedPublicOperation,
    reason: &'static str,
    error: Option<String>,
) -> PublicOperationExecutionResult {
    let mut terminal_error = serde_json::Map::new();
    terminal_error.insert("operation_id".into(), operation.id.to_string().into());
    terminal_error.insert("reason".into(), reason.into());
    if let Some(error) = error {
        terminal_error.insert("error".into(), error.into());
    }
    PublicOperationExecutionResult {
        state: OperationState::Failed,
        progress_message: format!("Sandbox restore failed ({reason})."),
        result_json: None,
        terminal_error_code: Some(PublicErrorCode::OperationFailed),
        terminal_error_json: Some(terminal_error.into()),
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn retry(
    progress_message: impl Into<String>,
    retry_after: Duration,
    reason: PublicOperationRequeueReason,
) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult {
        state: OperationState::Queued,
        progress_message: progress_message.into(),
        result_json: None,
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: Some(retry_after),
        requeue_reason: reason,
    }
}

#[cfg(test)]
mod tests;
