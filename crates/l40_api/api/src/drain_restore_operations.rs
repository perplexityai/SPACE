//! The drain restore operation ([`StoredOperationKind::DrainRestoreSandbox`]):
//! one per sandbox a forced node drain suspended, which brings the sandbox
//! back in the lifecycle it had, from the suspend snapshot the drain left.
//!
//! Only the system enqueues it, and nothing in production does yet. Each
//! attempt reads the row first ([`DrainRestoreGate`]) and then claims it
//! through the shared restore executor, placed as the drain's evacuation is,
//! without a runtime upgrade and exempt from the activation charge. It ends
//! `succeeded` once the sandbox is back, `canceled` once the restore is no
//! longer the system's to make, and `failed` once it can't be made: the
//! snapshot is lost, or its attempts are spent. Ending either of those ways
//! reverts any episode it left open, under its own lease.
//!
//! A user's lifecycle request supersedes it while it waits (see
//! `public_operations::drain_restore_supersession`); one that arrives while
//! its attempt holds the row waits behind it, or meets that restore.

use std::time::Duration;

use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicOperationResult, PublicSandboxId,
    PublicSandboxOperationResult, SandboxLifecycleAction,
};
use pplx_space_sandbox_store::drain_restore::{
    DrainRestoreGateRow, DrainRestoreTarget, read_drain_restore_gate,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use std::sync::Arc;

use crate::controllers::substrate::SubstrateError;
use crate::controllers::substrate::kubernetes::{DrainRestore, KubernetesSubstrate};
use crate::public_operations::{
    ClaimedPublicOperation, PublicOperationRequeueReason, StoredOperationKind,
};
use crate::public_sandbox_operations::PublicOperationExecutionResult;
use crate::public_sandbox_projection::public_lifecycle_state;

/// What a drain restore operation carries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DrainRestoreOperationPayload {
    pub sandbox_id: PublicSandboxId,
    /// The lifecycle to land the sandbox in, `started` or `paused`.
    pub target: String,
    /// S, the suspend snapshot the drain left.
    pub snapshot_id: Uuid,
}

impl DrainRestoreOperationPayload {
    pub fn new(sandbox_id: SandboxId, target: DrainRestoreTarget, snapshot_id: Uuid) -> Self {
        Self {
            sandbox_id: sandbox_id.into(),
            target: target.as_str().to_owned(),
            snapshot_id,
        }
    }

    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("drain restore operation payload serializes")
    }
}

/// Enqueue a drain restore of `payload`'s sandbox, which `org_id` owns. Nothing
/// in production enqueues one yet; tests do, through the same insert every
/// operation in a sandbox's queue takes.
#[cfg(any(test, feature = "test-hooks"))]
pub async fn enqueue_drain_restore(
    pool: &PgPool,
    org_id: pplx_space_common_core::types::OrgId,
    payload: DrainRestoreOperationPayload,
) -> Result<pplx_space_public_api::OperationId, crate::public_operations::PublicOperationStoreError>
{
    use crate::public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationResult, create_operation_tx,
    };
    let target_id = payload.sandbox_id.to_string();
    let mut tx = pool.begin().await?;
    let (created, _) = create_operation_tx(
        &mut tx,
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
    tx.commit().await?;
    match created {
        CreatePublicOperationResult::Created(operation_id) => Ok(operation_id),
        other => unreachable!("an insert without idempotency creates: {other:?}"),
    }
}

/// Whether a drain restore may try its claim, decided from what
/// [`read_drain_restore_gate`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainRestoreGate {
    /// The claim may take the row: it is `suspended`, or `restoring` in this
    /// target's episode, and the restore selector loads the named snapshot.
    /// The claim may still find a live lease or execution.
    Claimable,
    /// The restore is no longer the system's to make.
    Superseded(DrainRestoreSuperseded),
    /// The named snapshot isn't loadable yet, and nothing newer exists: an
    /// upload may still finish it.
    SourcePending,
    /// The named snapshot can't be loaded: it is missing, failed or deleting,
    /// or stored nowhere, and nothing newer exists.
    SourceLost,
    /// The sandbox is gone or being deleted.
    Deleted,
}

/// Why a drain restore is superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainRestoreSuperseded {
    /// The sandbox left `suspended` some other way: a user's call, or an
    /// earlier episode's end.
    Lifecycle,
    /// The v1 drain's marker is set; its own sweep owns the restore.
    EvacuationMarker,
    /// A restore that isn't this target's episode holds the row.
    OtherRestore,
    /// The sandbox has state newer than the named snapshot.
    NewerSnapshot,
}

impl DrainRestoreSuperseded {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle_changed",
            Self::EvacuationMarker => "evacuation_marker",
            Self::OtherRestore => "other_restore_in_flight",
            Self::NewerSnapshot => "newer_snapshot",
        }
    }
}

impl DrainRestoreGate {
    /// Decide for a restore toward `target` from `snapshot_id`; `None` when
    /// no row exists. An `Err` is a stored lifecycle that doesn't parse.
    pub fn decide(
        row: Option<&DrainRestoreGateRow>,
        snapshot_id: Uuid,
        target: DrainRestoreTarget,
    ) -> Result<Self, pplx_space_common_core::types::SandboxLifecycleParseError> {
        let Some(row) = row else {
            return Ok(Self::Deleted);
        };
        let lifecycle = row.lifecycle.parse::<SandboxLifecycle>()?;
        if lifecycle == SandboxLifecycle::Deleting {
            return Ok(Self::Deleted);
        }
        if row.evacuation_marked {
            return Ok(Self::Superseded(DrainRestoreSuperseded::EvacuationMarker));
        }
        let episode = row.restore_episode_target.as_deref();
        match lifecycle {
            SandboxLifecycle::Suspended if episode.is_none() => {}
            SandboxLifecycle::Restoring if episode == Some(target.as_str()) => {}
            SandboxLifecycle::Suspended | SandboxLifecycle::Restoring => {
                return Ok(Self::Superseded(DrainRestoreSuperseded::OtherRestore));
            }
            _ => return Ok(Self::Superseded(DrainRestoreSuperseded::Lifecycle)),
        }
        if row.selected_snapshot == Some(snapshot_id) {
            return Ok(Self::Claimable);
        }
        if row.newer_full_snapshot || !row.source_is_current_artifact {
            return Ok(Self::Superseded(DrainRestoreSuperseded::NewerSnapshot));
        }
        Ok(match row.source_state.as_deref() {
            // A ready, stored, current and newest snapshot is what the
            // selector loads, so a `ready` one here is mid-registration.
            Some("creating") => Self::SourcePending,
            Some("ready") if row.source_located => Self::SourcePending,
            _ => Self::SourceLost,
        })
    }
}

/// How a drain restore's attempts are paced and bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainRestoreRetry {
    /// The attempt at which a restore that keeps failing stops: its
    /// dispatcher's retry budget, so the executor ends it, and reverts its
    /// episode, before the dispatcher would fail it without either.
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
    pool: PgPool,
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
        pool: PgPool,
        kubernetes: Option<Arc<KubernetesSubstrate>>,
        placement: Option<EvacuationPlacement>,
        retry: DrainRestoreRetry,
    ) -> Self {
        Self {
            pool,
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
        let gate = match read_drain_restore_gate(
            &self.pool,
            request.sandbox_id,
            request.snapshot_id,
        )
        .await
        {
            Ok(row) => DrainRestoreGate::decide(row.as_ref(), request.snapshot_id, request.target),
            Err(error) => {
                return retry(
                    format!("drain restore gate read failed: {error}"),
                    self.retry.converging_delay,
                    PublicOperationRequeueReason::TransientFailure,
                );
            }
        };
        let gate = match gate {
            Ok(gate) => gate,
            Err(error) => {
                return failed(
                    operation,
                    "invalid_stored_lifecycle",
                    Some(error.to_string()),
                );
            }
        };
        match gate {
            DrainRestoreGate::Claimable => {}
            DrainRestoreGate::Superseded(reason) => {
                self.revert_open_episode(&request).await;
                return canceled(operation, reason.as_str());
            }
            DrainRestoreGate::Deleted => return canceled(operation, "sandbox_deleted"),
            DrainRestoreGate::SourcePending => {
                return retry(
                    "waiting for the drain's suspend snapshot to become restorable",
                    self.retry.converging_delay,
                    PublicOperationRequeueReason::RetryDeferred,
                );
            }
            DrainRestoreGate::SourceLost => {
                self.revert_open_episode(&request).await;
                return failed(operation, "restore_source_unavailable", None);
            }
        }
        self.restore(operation, &request).await
    }

    async fn restore(
        &self,
        operation: &ClaimedPublicOperation,
        request: &DrainRestoreRequest,
    ) -> PublicOperationExecutionResult {
        let Some(kubernetes) = self.kubernetes.as_deref() else {
            return failed(operation, "unsupported_substrate", None);
        };
        let outcome = kubernetes
            .restore_drained_execution(
                pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity::native("drain_restore"),
                request.sandbox_id,
                crate::controllers::substrate::ActivationOptions {
                    blocking: true,
                    ready_timeout: self.retry.ready_timeout,
                },
                self.placement.as_ref(),
                request.restore(),
            )
            .await;
        match outcome {
            Ok(true) => succeeded(request),
            // Something moved the row between the restore and its commit, or
            // the claim found nothing it may take: the next attempt reads it
            // again and decides.
            Ok(false) | Err(SubstrateError::EvacuationClaimLost { .. }) => retry(
                "the sandbox changed while its drain restore ran; reading it again",
                Duration::ZERO,
                PublicOperationRequeueReason::WorkflowProgress,
            ),
            // A lease or execution still holds the row: wait it out without
            // spending an attempt.
            Err(
                error @ (SubstrateError::RestoreInFlight { .. }
                | SubstrateError::ActivationInFlight { .. }
                | SubstrateError::LockContention { .. }),
            ) => retry(
                format!("drain restore waits for the sandbox's restore to settle: {error}"),
                self.retry.converging_delay,
                PublicOperationRequeueReason::WorkflowProgress,
            ),
            Err(error) if operation.attempts >= self.retry.max_attempts => {
                kubernetes
                    .revert_drain_restore_episode(request.sandbox_id, request.restore())
                    .await;
                failed(
                    operation,
                    "retry_attempts_exhausted",
                    Some(error.to_string()),
                )
            }
            Err(error) => retry(
                format!("drain restore attempt failed: {error}"),
                self.retry.backoff(operation.attempts),
                PublicOperationRequeueReason::TransientFailure,
            ),
        }
    }

    /// Revert an episode this operation left open, before it ends: nothing
    /// else settles one. A no-op unless the row is `restoring` under this
    /// operation's lease.
    async fn revert_open_episode(&self, request: &DrainRestoreRequest) {
        if let Some(kubernetes) = &self.kubernetes {
            kubernetes
                .revert_drain_restore_episode(request.sandbox_id, request.restore())
                .await;
        }
    }
}

/// A claimed drain restore's payload, checked against its operation.
#[derive(Debug, Clone, Copy)]
struct DrainRestoreRequest {
    sandbox_id: SandboxId,
    target: DrainRestoreTarget,
    snapshot_id: Uuid,
    /// Every attempt's restore lease token: the operation's own id, so an
    /// attempt can revert an episode an earlier one left.
    restore_lease_token: Uuid,
}

impl DrainRestoreRequest {
    fn parse(operation: &ClaimedPublicOperation) -> Result<Self, &'static str> {
        if operation.kind != StoredOperationKind::DrainRestoreSandbox {
            return Err("kind_mismatch");
        }
        let payload: DrainRestoreOperationPayload =
            serde_json::from_value(operation.payload.clone()).map_err(|_| "invalid_payload")?;
        let target = payload
            .target
            .parse::<DrainRestoreTarget>()
            .map_err(|_| "invalid_payload")?;
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
