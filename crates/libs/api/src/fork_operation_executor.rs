use pplx_space_common_core::types::{OrgId, SnapshotId};

use std::{
    future::Future,
    time::{Duration, Instant},
};

use pplx_space_common_core::types::{
    ArtifactKind, BackupAttemptId, BackupSource, EcwCredentialEpoch, ExecutionGeneration,
    SandboxId, SandboxLifecycle, SnapshotState,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicForkOperationResult, PublicOperationResult,
};
use sqlx::PgPool;

use crate::controllers::substrate::{
    NodeAddressing, OwnedNodeApiTarget, SnapshotRef, Substrate, SubstrateAdapter,
};
use crate::controllers::{
    ControllerError,
    sandbox::{
        FederatedSandboxCreateContext, RestoreForkDiskAsNewContext, SandboxController,
        fetch_sandbox,
    },
};
use crate::fork_operation_store::{
    CaptureLeaderObservation, ForkOperationStore, PrepareCopiedArtifact,
};
use crate::fork_operations::{
    ForkCaptureProgress, ForkSandboxLifetimeSeconds, ForkSandboxPayloadV1, ForkSourceLifecycle,
    ForkSourceLifecycleParseError, ForkTargetConfig, ForkTargetOutcome, ForkWorkflowPhase,
    decode_fork_payload, fork_restore_activation_lease_seconds,
};
use crate::models::CreateSandboxRequest;
use crate::public_operations::{ClaimedPublicOperation, PublicOperationRequeueReason};
use crate::public_sandbox_operations::PublicOperationExecutionResult;

const RETRY_AFTER: Duration = Duration::from_secs(2);
const MAX_SOURCE_REFREEZES: u8 = 3;
const MAX_BACKUP_ATTEMPTS: u8 = 3;
const CLAIM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const EXTERNAL_WORK_DEADLINE: Duration = Duration::from_secs(600);

struct SuccessfulForkTargetRecord {
    target_id: SandboxId,
    result: ForkTargetOutcome,
    snapshot_id: Option<SnapshotId>,
}

fn active_copy_node_target_is_complete(
    node_addressing: NodeAddressing,
    managed_by_present: bool,
    role_present: bool,
    direct_endpoint_present: bool,
    substrate_node_id_present: bool,
) -> bool {
    managed_by_present
        && role_present
        && match node_addressing {
            NodeAddressing::DirectEndpoint => direct_endpoint_present,
            NodeAddressing::SubstrateNodeId => substrate_node_id_present,
        }
}

fn fork_metric_dimensions(
    payload: &serde_json::Value,
) -> (
    pplx_space_metrics::sandbox_fork::ForkPhase,
    pplx_space_metrics::sandbox_fork::ForkSourceLifecycle,
) {
    let Ok(payload) = decode_fork_payload(payload.clone()) else {
        return (
            pplx_space_metrics::sandbox_fork::ForkPhase::InvalidPayload,
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Unfrozen,
        );
    };
    let phase = match payload.workflow_phase() {
        ForkWorkflowPhase::SourceFreeze => {
            pplx_space_metrics::sandbox_fork::ForkPhase::SourceFreeze
        }
        ForkWorkflowPhase::ArtifactMaterialization => {
            pplx_space_metrics::sandbox_fork::ForkPhase::ArtifactMaterialization
        }
        ForkWorkflowPhase::TargetRestore => {
            pplx_space_metrics::sandbox_fork::ForkPhase::TargetRestore
        }
        ForkWorkflowPhase::Completed => pplx_space_metrics::sandbox_fork::ForkPhase::Completed,
    };
    let source_lifecycle = match payload.source_lifecycle() {
        None => pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Unfrozen,
        Some(ForkSourceLifecycle::Started) => {
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Started
        }
        Some(ForkSourceLifecycle::Paused) => {
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Paused
        }
        Some(ForkSourceLifecycle::Suspended) => {
            pplx_space_metrics::sandbox_fork::ForkSourceLifecycle::Suspended
        }
    };
    (phase, source_lifecycle)
}

fn classify_fork_attempt(
    result: &PublicOperationExecutionResult,
) -> (
    pplx_space_metrics::sandbox_fork::ForkAttemptOutcome,
    Option<pplx_space_metrics::sandbox_fork::ForkRetryKind>,
) {
    let retry_kind = if result.state == OperationState::Queued {
        Some(match result.requeue_reason {
            PublicOperationRequeueReason::TransientFailure => {
                pplx_space_metrics::sandbox_fork::ForkRetryKind::TransientFailure
            }
            PublicOperationRequeueReason::WorkflowProgress => {
                pplx_space_metrics::sandbox_fork::ForkRetryKind::WorkflowProgress
            }
            PublicOperationRequeueReason::ActivationBackpressure => {
                pplx_space_metrics::sandbox_fork::ForkRetryKind::ActivationBackpressure
            }
        })
    } else {
        None
    };
    let outcome = match result.state {
        OperationState::Succeeded => {
            pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::Succeeded
        }
        OperationState::Failed
        | OperationState::Canceled
        | OperationState::Expired
        | OperationState::Other(_) => pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::Failed,
        OperationState::Queued | OperationState::Running => match result.requeue_reason {
            PublicOperationRequeueReason::TransientFailure => {
                pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::TransientRetry
            }
            PublicOperationRequeueReason::WorkflowProgress
            | PublicOperationRequeueReason::ActivationBackpressure => {
                pplx_space_metrics::sandbox_fork::ForkAttemptOutcome::WorkflowWait
            }
        },
    };
    (outcome, retry_kind)
}

#[derive(Clone)]
pub struct ForkSandboxOperationExecutor {
    store: ForkOperationStore,
    substrate: Substrate,
    controller: SandboxController,
}

async fn failed_backup_attempt(
    pool: &PgPool,
    attempt_id: BackupAttemptId,
) -> Result<Option<String>, ForkExecutionError> {
    let row = sqlx::query_as::<_, (String, Option<String>)>(pplx_space_common_core::sql!(
        "SELECT state, failure_reason FROM snapshots WHERE id = $1"
    ))
    .bind(attempt_id.as_uuid())
    .fetch_optional(pool)
    .await
    .map_err(ForkExecutionError::retry_error)?;

    let Some((state, reason)) = row else {
        return Ok(None);
    };
    let state: SnapshotState = state.parse().map_err(ForkExecutionError::retry_error)?;
    Ok(match state {
        SnapshotState::Failed => Some(reason.unwrap_or_else(|| "upload failed".to_string())),
        // A `deleting` row was claimed for prune: the durable artifact is
        // going away, so this attempt is lost and must be re-driven rather
        // than waited on until the operation deadline.
        SnapshotState::Deleting => Some("artifact was claimed for deletion".to_string()),
        SnapshotState::Creating | SnapshotState::Ready => None,
    })
}

impl ForkSandboxOperationExecutor {
    pub fn new(pool: PgPool, substrate: Substrate, controller: SandboxController) -> Self {
        Self {
            store: ForkOperationStore::new(pool),
            substrate,
            controller,
        }
    }

    pub async fn execute(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        let started_at = Instant::now();
        let (phase, source_lifecycle) = fork_metric_dimensions(&operation.payload);
        let result = self.execute_result(claim_token, operation).await;
        let (outcome, retry_kind) = classify_fork_attempt(&result);
        if let Some(retry_kind) = retry_kind {
            pplx_space_metrics::sandbox_fork::retry(phase, source_lifecycle, retry_kind);
        }
        pplx_space_metrics::sandbox_fork::phase_attempt(
            phase,
            source_lifecycle,
            outcome,
            started_at,
        );
        result
    }

    async fn execute_result(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        match self.execute_inner(claim_token, operation).await {
            Ok(result) => result,
            Err(error @ ForkExecutionError::Terminal { .. })
            | Err(error @ ForkExecutionError::DeadlineExceeded { .. }) => {
                let (code, message, outcome) = match error {
                    ForkExecutionError::Terminal { code, message } => (
                        code,
                        message,
                        pplx_space_metrics::sandbox_fork::ForkOutcome::Failed,
                    ),
                    ForkExecutionError::DeadlineExceeded { code, message } => (
                        code,
                        message,
                        pplx_space_metrics::sandbox_fork::ForkOutcome::DeadlineExceeded,
                    ),
                    ForkExecutionError::Retry(_) | ForkExecutionError::Wait(_) => unreachable!(),
                };
                if let Err(error) = self.cleanup_terminal_targets(operation).await {
                    tracing::warn!(
                        operation_id = %operation.id,
                        error = ?error,
                        "terminal fork target cleanup will be retried"
                    );
                    return retry("terminal fork target cleanup is pending");
                }
                match self
                    .store
                    .scrub_claimed_fork_federated_service_grant(claim_token, operation)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return retry(
                            "failed to scrub terminal fork credentials: claim superseded"
                                .to_string(),
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            operation_id = %operation.id,
                            %error,
                            "terminal fork credential scrub will be retried"
                        );
                        return retry("terminal fork credential scrub is pending");
                    }
                }
                let (snapshot_id, storage_location_id) = terminal_copied_artifact_ids(operation.id);
                if let Err(error) = self
                    .store
                    .retire_copied_artifact(snapshot_id, storage_location_id)
                    .await
                {
                    return retry(format!("failed to retire terminal fork artifact: {error}"));
                }
                pplx_space_metrics::sandbox_fork::completed(outcome);
                if let Some(source_id) = operation.target_id.as_deref() {
                    emit_sandbox_event(
                        source_id,
                        SandboxEventType::ForkFailed,
                        SandboxEventSource::ClusterController,
                        SandboxEventDetails::default()
                            .operation(operation.id.to_string())
                            .error(message.clone()),
                    );
                }
                failed(code, message)
            }
            Err(ForkExecutionError::Retry(message)) => retry(message),
            Err(ForkExecutionError::Wait(message)) => wait(message),
        }
    }

    async fn cleanup_terminal_targets(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> Result<(), ForkExecutionError> {
        let Ok(payload) = decode_fork_payload(operation.payload.clone()) else {
            return Ok(());
        };
        for target_id in terminal_cleanup_target_ids(&payload) {
            let Some(target) = fetch_sandbox(self.store.pool(), target_id)
                .await
                .map_err(ForkExecutionError::retry_error)?
            else {
                continue;
            };
            if target.org_id != operation.org_id {
                tracing::error!(
                    operation_id = %operation.id,
                    sandbox_id = %target_id,
                    expected_org_id = %operation.org_id,
                    actual_org_id = %target.org_id,
                    "refusing to clean a terminal fork target owned by another organization"
                );
                continue;
            }
            self.controller
                .delete_control_plane(self.store.pool(), target_id, Some(&target))
                .await
                .map_err(ForkExecutionError::retry_error)?;
        }
        Ok(())
    }
    async fn with_claim_heartbeat<T>(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        future: impl Future<Output = T>,
    ) -> Result<T, ForkExecutionError> {
        if !self
            .store
            .renew_claimed_operation(claim_token, operation)
            .await
            .map_err(ForkExecutionError::retry_error)?
        {
            return Err(ForkExecutionError::Retry(
                "fork operation claim was superseded before external work".to_string(),
            ));
        }
        let mut heartbeat = tokio::time::interval(CLAIM_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let future = bounded_external_work(future, EXTERNAL_WORK_DEADLINE);
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => {
                    return result.map_err(|_| ForkExecutionError::Retry(
                        "fork external work exceeded its execution deadline".to_string(),
                    ));
                }
                _ = heartbeat.tick() => {
                    if !self.store.renew_claimed_operation(claim_token, operation).await
                        .map_err(ForkExecutionError::retry_error)?
                    {
                        return Err(ForkExecutionError::Retry(
                            "fork operation claim was superseded during external work".to_string(),
                        ));
                    }
                }
            }
        }
    }

    async fn execute_inner(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
        let mut payload = decode_fork_payload(operation.payload.clone()).map_err(|error| {
            ForkExecutionError::terminal(format!("invalid fork payload: {error}"))
        })?;
        // The deadline belongs to the admitted operation, not to the current
        // availability gate. A rollout may disable new admissions while
        // previously admitted work still needs to terminalize.
        let remaining = self
            .remaining_deadline(operation, payload.hard_deadline())
            .await?;
        if remaining.is_zero() {
            self.scrub_stored_federated_service_grant(claim_token, operation, &mut payload)
                .await?;
            let (snapshot_id, storage_location_id) = terminal_copied_artifact_ids(operation.id);
            self.store
                .retire_copied_artifact(snapshot_id, storage_location_id)
                .await?;
            return self
                .deadline_result(claim_token, operation, &mut payload)
                .await;
        }
        if payload.target_config().is_none() {
            self.freeze_source(claim_token, operation, &mut payload)
                .await?;
            return Err(ForkExecutionError::Wait(
                "fork source and target configuration frozen".to_string(),
            ));
        }

        if payload.fork_snapshot_id().is_none() {
            self.materialize_artifact(claim_token, operation, &mut payload, remaining)
                .await?;
            return Err(ForkExecutionError::Wait(
                "fork artifact materialization is in progress".to_string(),
            ));
        }

        self.restore_targets(claim_token, operation, &mut payload, remaining)
            .await
    }

    async fn freeze_source(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
    ) -> Result<(), ForkExecutionError> {
        let source = fetch_sandbox(self.store.pool(), payload.source_sandbox_id())
            .await
            .map_err(ForkExecutionError::retry_error)?
            .ok_or_else(|| ForkExecutionError::not_found("source sandbox not found"))?;
        if source.org_id != operation.org_id {
            return Err(ForkExecutionError::forbidden(
                "source sandbox does not belong to the authenticated organization",
            ));
        }
        let org_byok_enabled =
            crate::controllers::sandbox::org_byok_enabled(self.store.pool(), operation.org_id)
                .await
                .map_err(ForkExecutionError::retry_error)?;
        validate_source_encryption(org_byok_enabled, source.volume_encryption)?;
        let lifecycle = source
            .lifecycle
            .parse::<ForkSourceLifecycle>()
            .map_err(classify_fork_source_lifecycle_error)?;
        let source_template_id = source
            .template_id
            .as_deref()
            .ok_or_else(|| ForkExecutionError::terminal("source sandbox has no template"))?
            .to_string();
        let template =
            frozen_source_template_reference(&source_template_id, source.content_hash.as_deref())?;
        let execution_generation =
            frozen_execution_generation(source.active_execution_generation, lifecycle)?;
        let target_config = ForkTargetConfig {
            template,
            source_template_id: Some(source_template_id),
            runtime: None,
            env: source.env.0,
            metadata: source.metadata.0,
            vcpu: source.vcpu.and_then(|value| u32::try_from(value).ok()),
            memory_mib: source
                .memory_mib
                .and_then(|value| u32::try_from(value).ok()),
            disk_mib: source.disk_mib.and_then(|value| u32::try_from(value).ok()),
            auto_lifecycle: source.auto_lifecycle.0,
            network_policy: source.network_policy.map(|policy| policy.0),
        };
        let source_snapshot = if lifecycle == ForkSourceLifecycle::Suspended {
            Some(
                self.store
                    .suspended_source_artifact(payload.source_sandbox_id())
                    .await?
                    .ok_or_else(|| {
                        ForkExecutionError::failed_precondition(
                            "suspended source has no ready durable suspend artifact",
                        )
                    })?,
            )
        } else {
            None
        };
        payload.freeze_source(
            execution_generation,
            lifecycle,
            target_config,
            source_snapshot,
        );
        self.persist_payload(claim_token, operation, payload).await
    }

    async fn materialize_artifact(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
        remaining: Duration,
    ) -> Result<(), ForkExecutionError> {
        if payload.source_lifecycle() == Some(ForkSourceLifecycle::Suspended) {
            return self
                .copy_suspended_artifact(claim_token, operation, payload, remaining)
                .await;
        }
        if payload.capture_operation_id().is_none() {
            let leader = self
                .store
                .select_capture_leader(claim_token, operation, payload)
                .await?;
            if leader != operation.id {
                return Err(ForkExecutionError::Wait(
                    "fork capture leader selected".to_string(),
                ));
            }
        }
        if payload.capture_operation_id() != Some(operation.id) {
            return self
                .observe_capture_leader(claim_token, operation, payload)
                .await;
        }
        let source_id = payload.source_sandbox_id();
        if let Some(local_artifact_id) = payload.local_artifact_id()
            && let Some(artifact) = self
                .store
                .ready_artifact_for_local_id(source_id, local_artifact_id)
                .await?
        {
            payload.resolve_fork_snapshot(artifact.snapshot_id, artifact.storage_location_id);
            self.persist_payload(claim_token, operation, payload)
                .await?;
            emit_fork_event(
                payload.source_sandbox_id(),
                SandboxEventType::ForkArtifactReady,
                operation,
                Some(artifact.snapshot_id),
            );
            return Ok(());
        }

        if let ForkCaptureProgress::ObserveBackup { attempt_id, .. } = payload.capture_progress() {
            if let Some(failure_reason) =
                failed_backup_attempt(self.store.pool(), attempt_id).await?
            {
                if payload.backup_attempt_count() >= MAX_BACKUP_ATTEMPTS {
                    return Err(ForkExecutionError::terminal(format!(
                        "durable fork artifact upload failed after {} attempts: {failure_reason}",
                        payload.backup_attempt_count()
                    )));
                }
                payload.retry_backup();
                self.persist_payload(claim_token, operation, payload)
                    .await?;
                return Err(ForkExecutionError::Wait(
                    "retrying failed durable fork artifact upload".to_string(),
                ));
            }
            return Err(ForkExecutionError::Wait(
                "waiting for the durable fork artifact upload".to_string(),
            ));
        }

        let mut client = self
            .substrate
            .reachable_runtime_client(source_id)
            .await
            .map_err(ForkExecutionError::retry_error)?
            .ok_or_else(|| ForkExecutionError::Retry("source execution is not reachable".into()))?;

        if let ForkCaptureProgress::EnqueueBackup(local_artifact_id) = payload.capture_progress() {
            let enqueued = self
                .with_claim_heartbeat(
                    claim_token,
                    operation,
                    crate::controllers::substrate::enqueue_artifact_backup_with_client(
                        &mut client,
                        "fork-source",
                        source_id,
                        ArtifactKind::Disk,
                        BackupSource::LocalArtifact(local_artifact_id),
                    ),
                )
                .await?
                .map_err(ForkExecutionError::retry_error)?;
            payload.record_backup_attempt(enqueued.attempt_id);
            self.persist_payload(claim_token, operation, payload)
                .await?;
            return Err(ForkExecutionError::Wait(
                "waiting for the durable fork artifact upload".to_string(),
            ));
        }
        let expected_lifecycle = match payload.source_lifecycle() {
            Some(ForkSourceLifecycle::Started) => {
                pplx_space_sbx_mgr_client::proto::SnapshotSourceLifecycle::Started
            }
            Some(ForkSourceLifecycle::Paused) => {
                pplx_space_sbx_mgr_client::proto::SnapshotSourceLifecycle::Paused
            }
            _ => return Err(ForkExecutionError::terminal("invalid live fork lifecycle")),
        };
        let capture_id = payload
            .capture_id()
            .ok_or_else(|| ForkExecutionError::terminal("capture id is not frozen"))?;
        let execution_generation = payload
            .source_execution_generation()
            .ok_or_else(|| ForkExecutionError::terminal("source generation is not frozen"))?;
        let response = tokio::time::timeout(
            remaining,
            self.with_claim_heartbeat(
                claim_token,
                operation,
                client.create_sandbox_snapshot(
                    pplx_space_sbx_mgr_client::proto::CreateSnapshotRequest {
                        sandbox_id: source_id.to_string(),
                        r#type: pplx_space_sbx_mgr_client::proto::SnapshotType::Disk as i32,
                        label: None,
                        creation_identity: Some(
                            pplx_space_sbx_mgr_client::proto::SnapshotCreationIdentity {
                                request_id: capture_id.to_string(),
                            },
                        ),
                        source: Some(
                            pplx_space_sbx_mgr_client::proto::CreateSnapshotSource {
                                source: Some(
                                    pplx_space_sbx_mgr_client::proto::create_snapshot_source::Source::CurrentSandbox(
                                        pplx_space_sbx_mgr_client::proto::CurrentSandboxSnapshotSource {
                                            execution_generation,
                                            lifecycle: expected_lifecycle as i32,
                                        },
                                    ),
                                ),
                            },
                        ),
                    },
                ),
            ),
        )
        .await
        .map_err(|_| ForkExecutionError::Retry("fork capture reached its deadline".into()))??;
        let response = match response {
            Ok(response) => response.into_inner(),
            Err(error) => match classify_snapshot_error(error.code()) {
                SnapshotErrorDisposition::Refreeze => {
                    if payload.source_refreeze_count() >= MAX_SOURCE_REFREEZES {
                        return Err(ForkExecutionError::failed_precondition(
                            "fork source changed too many times during capture",
                        ));
                    }
                    payload.reset_source_for_refreeze();
                    self.persist_payload(claim_token, operation, payload)
                        .await?;
                    return Err(ForkExecutionError::Wait(
                        "fork source changed; source state will be frozen again".to_string(),
                    ));
                }
                SnapshotErrorDisposition::Terminal(code) => {
                    return Err(ForkExecutionError::terminal_with_code(
                        code,
                        error.to_string(),
                    ));
                }
                SnapshotErrorDisposition::Retry => {
                    return Err(ForkExecutionError::retry_error(error));
                }
            },
        };
        let published = response.published_artifact.ok_or_else(|| {
            ForkExecutionError::terminal("snapshot response omitted published artifact identity")
        })?;
        let local_artifact_id = published.local_artifact_id.parse().map_err(|error| {
            ForkExecutionError::terminal(format!(
                "snapshot response contained invalid local artifact identity: {error}"
            ))
        })?;
        payload.record_local_artifact(local_artifact_id);
        self.persist_payload(claim_token, operation, payload)
            .await?;
        let enqueued = self
            .with_claim_heartbeat(
                claim_token,
                operation,
                crate::controllers::substrate::enqueue_artifact_backup_with_client(
                    &mut client,
                    "fork-source",
                    source_id,
                    ArtifactKind::Disk,
                    BackupSource::LocalArtifact(local_artifact_id),
                ),
            )
            .await?
            .map_err(ForkExecutionError::retry_error)?;
        payload.record_backup_attempt(enqueued.attempt_id);
        self.persist_payload(claim_token, operation, payload)
            .await?;
        Err(ForkExecutionError::Wait(
            "waiting for the durable fork artifact upload".to_string(),
        ))
    }

    async fn observe_capture_leader(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
    ) -> Result<(), ForkExecutionError> {
        let leader_id = payload
            .capture_operation_id()
            .ok_or_else(|| ForkExecutionError::terminal("capture leader is not selected"))?;
        let source_generation = payload
            .source_execution_generation()
            .ok_or_else(|| ForkExecutionError::terminal("source generation is not frozen"))?;
        let source_lifecycle = payload
            .source_lifecycle()
            .ok_or_else(|| ForkExecutionError::terminal("source lifecycle is not frozen"))?;
        match self
            .store
            .observe_capture_leader(
                operation.org_id,
                leader_id,
                source_generation,
                source_lifecycle,
            )
            .await?
        {
            CaptureLeaderObservation::ArtifactReady {
                snapshot_id,
                storage_location_id,
            } => {
                payload.resolve_fork_snapshot(snapshot_id, storage_location_id);
                self.persist_payload(claim_token, operation, payload)
                    .await?;
                return Ok(());
            }
            CaptureLeaderObservation::IncompatibleSource => {
                if payload.source_refreeze_count() >= MAX_SOURCE_REFREEZES {
                    return Err(ForkExecutionError::failed_precondition(
                        "fork source changed too many times during shared capture",
                    ));
                }
                payload.reset_source_for_refreeze();
                self.persist_payload(claim_token, operation, payload)
                    .await?;
                return Err(ForkExecutionError::Wait(
                    "fork capture leader changed source state; source will be frozen again"
                        .to_string(),
                ));
            }
            CaptureLeaderObservation::Missing
            | CaptureLeaderObservation::TerminalWithoutArtifact => {
                payload.clear_capture_operation_id();
                self.persist_payload(claim_token, operation, payload)
                    .await?;
            }
            CaptureLeaderObservation::Pending => {}
        }
        Err(ForkExecutionError::Wait(
            "waiting for the shared durable fork capture".to_string(),
        ))
    }

    async fn copy_suspended_artifact(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
        remaining: Duration,
    ) -> Result<(), ForkExecutionError> {
        let source_snapshot_id = payload
            .source_snapshot_id()
            .ok_or_else(|| ForkExecutionError::terminal("source snapshot is not frozen"))?;
        let source_location_id = payload
            .source_snapshot_storage_location_id()
            .ok_or_else(|| ForkExecutionError::terminal("source storage location is not frozen"))?;
        let source_storage_uri = self
            .store
            .source_storage_uri(source_snapshot_id, source_location_id)
            .await?
            .ok_or_else(|| {
                ForkExecutionError::failed_precondition("frozen source artifact disappeared")
            })?;
        let fork_snapshot_id =
            SnapshotId::from_uuid(deterministic_fork_uuid(operation.id.as_uuid(), b"snapshot"));
        let fork_location_id = deterministic_fork_uuid(operation.id.as_uuid(), b"location");
        let destination_storage_uri = fork_destination_uri(
            &source_storage_uri,
            payload.source_sandbox_id(),
            fork_snapshot_id,
        );
        let target_sandbox_id = payload.target_sandbox_id();
        let initial_owner_sandbox_id = copied_snapshot_owner(payload);
        self.store
            .prepare_copied_artifact(PrepareCopiedArtifact {
                snapshot_id: fork_snapshot_id,
                storage_location_id: fork_location_id,
                owner_sandbox_id: initial_owner_sandbox_id,
                org_id: operation.org_id,
                destination_storage_uri: &destination_storage_uri,
            })
            .await?;

        let copy_result = async {
            let source = fetch_sandbox(self.store.pool(), payload.source_sandbox_id())
                .await
                .map_err(ForkExecutionError::retry_error)?
                .ok_or_else(|| ForkExecutionError::not_found("source sandbox disappeared"))?;
            let active_target_is_complete = active_copy_node_target_is_complete(
                self.substrate.capabilities().node_addressing,
                source.active_node_managed_by.is_some(),
                source.active_node_role.is_some(),
                source.active_node_address.is_some(),
                source.active_node_name.is_some(),
            );
            let fallback_node = if !active_target_is_complete {
                Some(
                    self.substrate
                        .select_ready_worker_node_api_target()
                        .await
                        .map_err(ForkExecutionError::retry_error)?,
                )
            } else {
                None
            };
            let managed_by = source
                .active_node_managed_by
                .or_else(|| fallback_node.as_ref().map(|node| node.managed_by))
                .ok_or_else(|| {
                    ForkExecutionError::Retry("no worker is available for artifact copy".into())
                })?;
            let role = source
                .active_node_role
                .or_else(|| fallback_node.as_ref().map(|node| node.role))
                .ok_or_else(|| {
                    ForkExecutionError::Retry("no worker is available for artifact copy".into())
                })?;
            let direct_endpoint = source
                .active_node_address
                .as_ref()
                .map(ToString::to_string)
                .or_else(|| fallback_node.as_ref()?.direct_endpoint.clone());
            let substrate_node_id = source.active_node_name.or_else(|| {
                fallback_node
                    .as_ref()
                    .and_then(|node| node.substrate_node_id.clone())
            });
            let target = OwnedNodeApiTarget {
                managed_by,
                role,
                direct_endpoint,
                substrate_node_id,
            };
            tokio::time::timeout(
                remaining,
                self.with_claim_heartbeat(
                    claim_token,
                    operation,
                    self.substrate.create_snapshot_from_durable_artifact(
                        &target.as_target(),
                        operation.id,
                        payload.source_sandbox_id(),
                        target_sandbox_id,
                        source_storage_uri,
                        destination_storage_uri.clone(),
                    ),
                ),
            )
            .await
            .map_err(|_| {
                ForkExecutionError::Retry("fork artifact copy reached its deadline".into())
            })??
            .map_err(ForkExecutionError::retry_error)
        }
        .await;
        let published = match copy_result {
            Ok(published) => published,
            Err(error) => {
                self.store
                    .retire_copied_artifact(fork_snapshot_id, fork_location_id)
                    .await?;
                return Err(error);
            }
        };
        if published.storage_uri != destination_storage_uri {
            self.store
                .retire_copied_artifact(fork_snapshot_id, fork_location_id)
                .await?;
            return Err(ForkExecutionError::terminal(
                "node returned a different fork artifact destination",
            ));
        }
        if !self
            .store
            .mark_copied_artifact_ready(
                fork_snapshot_id,
                initial_owner_sandbox_id,
                published.bytes_published,
            )
            .await?
        {
            return Err(ForkExecutionError::Retry(
                "fork snapshot publication lost its database row".into(),
            ));
        }
        payload.resolve_fork_snapshot(fork_snapshot_id, fork_location_id);
        self.persist_payload(claim_token, operation, payload)
            .await?;
        emit_fork_event(
            payload.source_sandbox_id(),
            SandboxEventType::ForkArtifactReady,
            operation,
            Some(fork_snapshot_id),
        );
        Ok(())
    }

    async fn restore_targets(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
        remaining: Duration,
    ) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
        let actor_id = operation
            .actor_user_id
            .ok_or_else(|| ForkExecutionError::terminal("fork operation has no actor"))?;
        let actor = self
            .store
            .user(actor_id)
            .await?
            .ok_or_else(|| ForkExecutionError::terminal("fork actor no longer exists"))?;
        if actor.org_id != operation.org_id {
            return Err(ForkExecutionError::terminal(
                "fork actor no longer belongs to the operation organization",
            ));
        }
        let config = payload
            .target_config()
            .ok_or_else(|| ForkExecutionError::terminal("target configuration is not frozen"))?
            .clone();
        if let Some(results) = payload.completed_target_results() {
            self.scrub_stored_federated_service_grant(claim_token, operation, payload)
                .await?;
            let retirement = self.retire_successful_copied_artifact(payload).await;
            emit_completed_target_observability(
                payload.source_sandbox_id(),
                operation.id,
                payload.fork_snapshot_id(),
                &results,
            );
            return successful_fork_result_after_retirement(results, retirement);
        }
        let snapshot_id = payload
            .fork_snapshot_id()
            .ok_or_else(|| ForkExecutionError::terminal("fork snapshot is not resolved"))?;
        let storage_location_id = payload
            .fork_snapshot_storage_location_id()
            .ok_or_else(|| ForkExecutionError::terminal("fork storage location is not resolved"))?;
        let artifact = self
            .store
            .ready_artifact(snapshot_id, storage_location_id)
            .await?
            .ok_or_else(|| ForkExecutionError::Wait("fork artifact is not ready".into()))?;
        let storage_uri = artifact.storage_uri;
        let mut results = Vec::with_capacity(1);
        for target_id in [payload.target_sandbox_id()] {
            if let Some(result) = payload.target_result(target_id) {
                results.push(result.clone());
                continue;
            }
            if let Some(existing) = fetch_sandbox(self.store.pool(), target_id)
                .await
                .map_err(ForkExecutionError::retry_error)?
            {
                let lifecycle =
                    existing
                        .lifecycle
                        .parse::<SandboxLifecycle>()
                        .map_err(|error| {
                            ForkExecutionError::terminal(format!(
                                "fork target {target_id} has invalid lifecycle: {error}"
                            ))
                        })?;
                if let Some(disposition) = classify_owned_existing_target(
                    existing.org_id,
                    operation.org_id,
                    lifecycle,
                    existing.command_ready_pending,
                ) {
                    match disposition {
                        ExistingTargetDisposition::Succeeded => {
                            self.rebind_copied_artifact_owner(payload, target_id)
                                .await?;
                            let result = sandbox_result(
                                target_id,
                                fork_response_template_id(&config),
                                payload.ecw_host_domain(),
                                existing.ecw_credential_epoch,
                            );
                            self.record_successful_target(
                                claim_token,
                                operation,
                                payload,
                                &mut results,
                                SuccessfulForkTargetRecord {
                                    target_id,
                                    result,
                                    snapshot_id: Some(snapshot_id),
                                },
                            )
                            .await?;
                            continue;
                        }
                        ExistingTargetDisposition::Failed => {
                            self.cleanup_restore_target(target_id, operation.org_id)
                                .await?;
                            let result = sandbox_error(
                                500,
                                format!("fork target {target_id} failed during restore"),
                            );
                            payload.record_target_result(target_id, result.clone());
                            self.persist_payload(claim_token, operation, payload)
                                .await?;
                            results.push(result);
                            continue;
                        }
                        ExistingTargetDisposition::Wait => {}
                    }
                }
                return Err(ForkExecutionError::Wait(format!(
                    "target {target_id} is still {}",
                    existing.lifecycle
                )));
            }
            let federated_create = payload
                .federated_create()
                .map(|context| context.restore())
                .transpose()
                .map_err(|error| {
                    ForkExecutionError::terminal(format!(
                        "invalid federated fork create context: {error}"
                    ))
                })?;
            let (federated_owner, federated_service_grant) = federated_create
                .as_ref()
                .map(|(owner, service_grant)| (Some(owner), Some(service_grant)))
                .unwrap_or((None, None));
            let federated =
                FederatedSandboxCreateContext::new(federated_owner, federated_service_grant);
            let request = create_request(&config, payload.requested_timeout_seconds());
            match tokio::time::timeout(
                remaining,
                self.with_claim_heartbeat(
                    claim_token,
                    operation,
                    self.controller.restore_fork_disk_as_new(
                        self.store.pool(),
                        &actor,
                        &request,
                        SandboxApiMetricIdentity::ecw("fork"),
                        RestoreForkDiskAsNewContext {
                            sandbox_id: target_id,
                            federated,
                            operation_id: operation.id,
                            restore_lease_seconds: fork_restore_activation_lease_seconds(
                                payload.hard_deadline(),
                            ),
                            snapshot: SnapshotRef {
                                snapshot_id: &snapshot_id.to_string(),
                                storage_uri: Some(&storage_uri),
                                source_sandbox_id: payload.source_sandbox_id(),
                            },
                        },
                    ),
                ),
            )
            .await
            {
                Ok(Ok(Ok(_))) => {
                    let restored = fetch_sandbox(self.store.pool(), target_id)
                        .await
                        .map_err(ForkExecutionError::retry_error)?
                        .ok_or_else(|| {
                            ForkExecutionError::Retry(format!(
                                "restored fork target {target_id} is not visible yet"
                            ))
                        })?;
                    let lifecycle =
                        restored
                            .lifecycle
                            .parse::<SandboxLifecycle>()
                            .map_err(|error| {
                                ForkExecutionError::terminal(format!(
                                    "fork target {target_id} has invalid lifecycle: {error}"
                                ))
                            })?;
                    if classify_owned_existing_target(
                        restored.org_id,
                        operation.org_id,
                        lifecycle,
                        restored.command_ready_pending,
                    ) != Some(ExistingTargetDisposition::Succeeded)
                    {
                        return Err(ForkExecutionError::Wait(format!(
                            "restored fork target {target_id} is still converging"
                        )));
                    }
                    self.rebind_copied_artifact_owner(payload, target_id)
                        .await?;
                    let result = sandbox_result(
                        target_id,
                        fork_response_template_id(&config),
                        payload.ecw_host_domain(),
                        restored.ecw_credential_epoch,
                    );
                    self.record_successful_target(
                        claim_token,
                        operation,
                        payload,
                        &mut results,
                        SuccessfulForkTargetRecord {
                            target_id,
                            result,
                            snapshot_id: Some(snapshot_id),
                        },
                    )
                    .await?;
                }
                Ok(Ok(Err(error))) => match classify_restore_controller_error(&error) {
                    RestoreControllerErrorDisposition::Reobserve => {
                        return Err(ForkExecutionError::Wait(format!(
                            "re-observing concurrently created fork target {target_id}"
                        )));
                    }
                    RestoreControllerErrorDisposition::Retry => {
                        return Err(ForkExecutionError::retry_error(error));
                    }
                    RestoreControllerErrorDisposition::Terminal => {
                        self.cleanup_restore_target(target_id, operation.org_id)
                            .await?;
                        let result =
                            sandbox_error(i32::from(error.status().as_u16()), error.to_string());
                        payload.record_target_result(target_id, result.clone());
                        self.persist_payload(claim_token, operation, payload)
                            .await?;
                        results.push(result);
                    }
                },
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    self.cleanup_restore_target(target_id, operation.org_id)
                        .await?;
                    let result = sandbox_error(504, "fork target restore reached its deadline");
                    payload.record_target_result(target_id, result.clone());
                    self.persist_payload(claim_token, operation, payload)
                        .await?;
                    results.push(result);
                }
            }
        }
        self.scrub_stored_federated_service_grant(claim_token, operation, payload)
            .await?;
        let retirement = self.retire_successful_copied_artifact(payload).await;
        let outcome = fork_outcome(&results);
        let result = successful_fork_result_after_retirement(results, retirement)?;
        emit_terminal_fork_observability(
            payload.source_sandbox_id(),
            operation.id,
            Some(snapshot_id),
            outcome,
        );
        Ok(result)
    }

    async fn retire_successful_copied_artifact(
        &self,
        payload: &ForkSandboxPayloadV1,
    ) -> Result<(), ForkExecutionError> {
        if payload.source_lifecycle() != Some(ForkSourceLifecycle::Suspended) {
            return Ok(());
        }
        let snapshot_id = payload
            .fork_snapshot_id()
            .ok_or_else(|| ForkExecutionError::terminal("fork snapshot is not resolved"))?;
        let storage_location_id = payload
            .fork_snapshot_storage_location_id()
            .ok_or_else(|| ForkExecutionError::terminal("fork storage location is not resolved"))?;
        self.store
            .retire_copied_artifact(snapshot_id, storage_location_id)
            .await
            .map_err(Into::into)
    }

    async fn rebind_copied_artifact_owner(
        &self,
        payload: &ForkSandboxPayloadV1,
        target_sandbox_id: SandboxId,
    ) -> Result<(), ForkExecutionError> {
        if payload.source_lifecycle() != Some(ForkSourceLifecycle::Suspended) {
            return Ok(());
        }
        let snapshot_id = payload
            .fork_snapshot_id()
            .ok_or_else(|| ForkExecutionError::terminal("fork snapshot is not resolved"))?;
        if !self
            .store
            .rebind_copied_artifact_owner(
                snapshot_id,
                payload.source_sandbox_id(),
                target_sandbox_id,
            )
            .await?
        {
            return Err(ForkExecutionError::Retry(
                "fork snapshot ownership changed during restore".to_string(),
            ));
        }
        Ok(())
    }

    async fn cleanup_restore_target(
        &self,
        target_id: SandboxId,
        operation_org_id: OrgId,
    ) -> Result<(), ForkExecutionError> {
        let sandbox = fetch_sandbox(self.store.pool(), target_id)
            .await
            .map_err(ForkExecutionError::retry_error)?;
        if let Some(sandbox) = sandbox.as_ref() {
            if sandbox.org_id != operation_org_id {
                return Ok(());
            }
            self.controller
                .delete(self.store.pool(), target_id, Some(sandbox))
                .await
                .map_err(ForkExecutionError::retry_error)?;
        }
        crate::reconcile_queue::finalize_sandbox_db_delete_user_initiated(
            self.store.pool(),
            target_id,
        )
        .await
        .map_err(ForkExecutionError::retry_error)
    }

    async fn scrub_stored_federated_service_grant(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
    ) -> Result<(), ForkExecutionError> {
        if payload.scrub_federated_service_grant() {
            self.persist_payload(claim_token, operation, payload)
                .await?;
        }
        Ok(())
    }

    async fn remaining_deadline(
        &self,
        operation: &ClaimedPublicOperation,
        hard_deadline: Duration,
    ) -> Result<Duration, ForkExecutionError> {
        self.store
            .remaining_deadline(operation.id, hard_deadline)
            .await?
            .ok_or_else(|| ForkExecutionError::terminal("fork operation disappeared"))
    }

    async fn deadline_result(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
    ) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
        let template_id = payload
            .target_config()
            .map(fork_response_template_id)
            .unwrap_or_else(|| "seed".to_string());
        let mut results = Vec::with_capacity(1);
        for target_id in [payload.target_sandbox_id()] {
            if let Some(result) = payload.target_result(target_id) {
                results.push(result.clone());
                continue;
            }
            let sandbox = fetch_sandbox(self.store.pool(), target_id)
                .await
                .map_err(ForkExecutionError::retry_error)?;
            if let Some(sandbox) = sandbox {
                let lifecycle = sandbox
                    .lifecycle
                    .parse::<SandboxLifecycle>()
                    .map_err(|error| {
                        ForkExecutionError::terminal(format!(
                            "fork target {target_id} has invalid lifecycle: {error}"
                        ))
                    })?;
                if classify_owned_existing_target(
                    sandbox.org_id,
                    operation.org_id,
                    lifecycle,
                    sandbox.command_ready_pending,
                ) == Some(ExistingTargetDisposition::Succeeded)
                {
                    let result = sandbox_result(
                        target_id,
                        template_id.clone(),
                        payload.ecw_host_domain(),
                        sandbox.ecw_credential_epoch,
                    );
                    self.record_successful_target(
                        claim_token,
                        operation,
                        payload,
                        &mut results,
                        SuccessfulForkTargetRecord {
                            target_id,
                            result,
                            snapshot_id: payload.fork_snapshot_id(),
                        },
                    )
                    .await?;
                    continue;
                }
            }
            results.push(sandbox_error(504, "sandbox fork hard deadline exceeded"));
        }
        let result = deadline_fork_result(results)?;
        emit_terminal_fork_observability(
            payload.source_sandbox_id(),
            operation.id,
            payload.fork_snapshot_id(),
            pplx_space_metrics::sandbox_fork::ForkOutcome::Completed,
        );
        Ok(result)
    }

    async fn record_successful_target(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
        results: &mut Vec<ForkTargetOutcome>,
        target: SuccessfulForkTargetRecord,
    ) -> Result<(), ForkExecutionError> {
        let SuccessfulForkTargetRecord {
            target_id,
            result,
            snapshot_id,
        } = target;
        payload.record_target_result(target_id, result.clone());
        self.persist_payload(claim_token, operation, payload)
            .await?;
        emit_fork_event(
            target_id,
            SandboxEventType::ForkTargetActivated,
            operation,
            snapshot_id,
        );
        results.push(result);
        Ok(())
    }

    async fn persist_payload(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &ForkSandboxPayloadV1,
    ) -> Result<(), ForkExecutionError> {
        self.store
            .persist_payload(claim_token, operation, payload)
            .await
            .map_err(Into::into)
    }
}

fn terminal_cleanup_target_ids(payload: &ForkSandboxPayloadV1) -> Vec<SandboxId> {
    vec![payload.target_sandbox_id()]
}

fn deterministic_fork_uuid(operation_id: uuid::Uuid, label: &[u8]) -> uuid::Uuid {
    uuid::Uuid::new_v5(&operation_id, label)
}

fn terminal_copied_artifact_ids(
    operation_id: pplx_space_public_api::OperationId,
) -> (SnapshotId, uuid::Uuid) {
    (
        SnapshotId::from_uuid(deterministic_fork_uuid(operation_id.as_uuid(), b"snapshot")),
        deterministic_fork_uuid(operation_id.as_uuid(), b"location"),
    )
}

fn emit_fork_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    operation: &ClaimedPublicOperation,
    snapshot_id: Option<SnapshotId>,
) {
    let mut details = SandboxEventDetails::default().operation(operation.id.to_string());
    if let Some(snapshot_id) = snapshot_id {
        details = details.snapshot_id(snapshot_id.to_string());
    }
    emit_sandbox_event(
        &sandbox_id.to_string(),
        event_type,
        SandboxEventSource::ClusterController,
        details,
    );
}

fn fork_destination_uri(
    source: &str,
    source_sandbox_id: SandboxId,
    fork_snapshot_id: SnapshotId,
) -> String {
    match source.split_once('?') {
        Some((path, query)) => {
            format!("{path}/fork/{source_sandbox_id}/{fork_snapshot_id}?{query}")
        }
        None => format!("{source}/fork/{source_sandbox_id}/{fork_snapshot_id}"),
    }
}

fn copied_snapshot_owner(payload: &ForkSandboxPayloadV1) -> SandboxId {
    payload.source_sandbox_id()
}

fn frozen_execution_generation(
    value: Option<i64>,
    lifecycle: ForkSourceLifecycle,
) -> Result<i64, ForkExecutionError> {
    if lifecycle == ForkSourceLifecycle::Suspended {
        return Ok(value.unwrap_or(0));
    }
    value
        .and_then(|value| ExecutionGeneration::new(value).ok())
        .map(ExecutionGeneration::get)
        .ok_or_else(|| {
            ForkExecutionError::failed_precondition(
                "fork source has no active execution generation",
            )
        })
}

fn classify_fork_source_lifecycle_error(
    error: ForkSourceLifecycleParseError,
) -> ForkExecutionError {
    match error {
        ForkSourceLifecycleParseError::Invalid(error) => {
            ForkExecutionError::terminal(format!("source sandbox has invalid lifecycle: {error}"))
        }
        ForkSourceLifecycleParseError::Unsupported {
            lifecycle: SandboxLifecycle::Suspending,
        } => ForkExecutionError::Wait("waiting for the source suspend artifact".to_string()),
        ForkSourceLifecycleParseError::Unsupported { lifecycle } => {
            ForkExecutionError::failed_precondition(format!(
                "sandbox lifecycle {lifecycle} cannot be forked"
            ))
        }
    }
}

fn validate_source_encryption(
    org_byok_enabled: bool,
    volume_encryption: bool,
) -> Result<(), ForkExecutionError> {
    if org_byok_enabled {
        return Err(ForkExecutionError::failed_precondition(
            "sandboxes in BYOK organizations cannot be forked",
        ));
    }
    if volume_encryption {
        return Err(ForkExecutionError::failed_precondition(
            "encrypted sandboxes cannot be forked",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SnapshotErrorDisposition {
    Refreeze,
    Terminal(PublicErrorCode),
    Retry,
}

fn classify_snapshot_error(code: tonic::Code) -> SnapshotErrorDisposition {
    match code {
        tonic::Code::FailedPrecondition | tonic::Code::Aborted => {
            SnapshotErrorDisposition::Refreeze
        }
        tonic::Code::InvalidArgument => {
            SnapshotErrorDisposition::Terminal(PublicErrorCode::InvalidRequest)
        }
        tonic::Code::NotFound => SnapshotErrorDisposition::Terminal(PublicErrorCode::NotFound),
        tonic::Code::AlreadyExists => SnapshotErrorDisposition::Terminal(PublicErrorCode::Conflict),
        tonic::Code::PermissionDenied => {
            SnapshotErrorDisposition::Terminal(PublicErrorCode::Forbidden)
        }
        tonic::Code::Unauthenticated => {
            SnapshotErrorDisposition::Terminal(PublicErrorCode::Unauthorized)
        }
        tonic::Code::OutOfRange | tonic::Code::Unimplemented | tonic::Code::DataLoss => {
            SnapshotErrorDisposition::Terminal(PublicErrorCode::Internal)
        }
        _ => SnapshotErrorDisposition::Retry,
    }
}

async fn bounded_external_work<T>(
    future: impl Future<Output = T>,
    deadline: Duration,
) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(deadline, future).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestoreControllerErrorDisposition {
    Reobserve,
    Retry,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingTargetDisposition {
    Succeeded,
    Failed,
    Wait,
}

const fn classify_existing_target(
    lifecycle: SandboxLifecycle,
    command_ready_pending: bool,
) -> ExistingTargetDisposition {
    if command_ready_pending {
        return ExistingTargetDisposition::Wait;
    }
    match lifecycle {
        SandboxLifecycle::Started | SandboxLifecycle::Paused | SandboxLifecycle::Suspended => {
            ExistingTargetDisposition::Succeeded
        }
        SandboxLifecycle::Failed => ExistingTargetDisposition::Failed,
        _ => ExistingTargetDisposition::Wait,
    }
}

fn classify_owned_existing_target(
    target_org_id: OrgId,
    operation_org_id: OrgId,
    lifecycle: SandboxLifecycle,
    command_ready_pending: bool,
) -> Option<ExistingTargetDisposition> {
    if target_org_id == operation_org_id {
        Some(classify_existing_target(lifecycle, command_ready_pending))
    } else {
        None
    }
}

fn classify_restore_controller_error(error: &ControllerError) -> RestoreControllerErrorDisposition {
    if error.is_sandbox_already_exists() {
        RestoreControllerErrorDisposition::Reobserve
    } else if error.status() == axum::http::StatusCode::TOO_MANY_REQUESTS
        || error.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE
        || (error.status().is_server_error()
            && !crate::public_sandbox_operations::is_non_retryable_create_controller_error(error))
    {
        RestoreControllerErrorDisposition::Retry
    } else {
        RestoreControllerErrorDisposition::Terminal
    }
}

fn create_request(
    config: &ForkTargetConfig,
    requested_timeout: ForkSandboxLifetimeSeconds,
) -> CreateSandboxRequest {
    CreateSandboxRequest {
        name: None,
        template: Some(config.template.clone()),
        runtime: config.runtime.clone(),
        boot_mode: Some("cold".to_string()),
        env: config.env.clone(),
        metadata: config.metadata.clone(),
        vcpu: config.vcpu,
        memory_mib: config.memory_mib,
        disk_mib: config.disk_mib,
        file_changes: None,
        auto_lifecycle: Some(crate::e2b::policy_for_timeout_update(
            &config.auto_lifecycle,
            requested_timeout.get(),
        )),
        wait_until_ready: true,
        network_policy: config.network_policy.clone(),
        node_pool: None,
    }
}

fn frozen_source_template_reference(
    source_template_id: &str,
    source_content_hash: Option<&str>,
) -> Result<pplx_space_common_core::types::TemplateReference, ForkExecutionError> {
    let source_content_hash = source_content_hash
        .ok_or_else(|| ForkExecutionError::terminal("source sandbox has no content hash"))?;
    format!("{source_template_id}@{source_content_hash}")
        .parse()
        .map_err(|error| {
            ForkExecutionError::terminal(format!("source template is invalid: {error}"))
        })
}

fn sandbox_result(
    sandbox_id: SandboxId,
    template_id: String,
    ecw_host_domain: Option<&str>,
    credential_epoch: EcwCredentialEpoch,
) -> ForkTargetOutcome {
    ForkTargetOutcome::Succeeded {
        sandbox_id,
        template_id,
        ecw_host_domain: ecw_host_domain.map(str::to_string),
        credential_epoch,
    }
}

fn fork_response_template_id(config: &ForkTargetConfig) -> String {
    config
        .source_template_id
        .clone()
        .unwrap_or_else(|| config.template.to_string())
}

fn sandbox_error(code: i32, message: impl Into<String>) -> ForkTargetOutcome {
    ForkTargetOutcome::Failed {
        code,
        message: message.into(),
    }
}

fn successful_fork_result(
    results: Vec<ForkTargetOutcome>,
) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
    let results = results
        .into_iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(ForkExecutionError::retry_error)?;
    let result_json =
        serde_json::to_value(PublicOperationResult::Fork(PublicForkOperationResult {
            results,
        }))
        .map_err(ForkExecutionError::retry_error)?;
    Ok(PublicOperationExecutionResult {
        state: OperationState::Succeeded,
        progress_message: "fork completed".to_string(),
        result_json: Some(result_json),
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    })
}

fn deadline_fork_result(
    results: Vec<ForkTargetOutcome>,
) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
    if fork_outcome(&results) != pplx_space_metrics::sandbox_fork::ForkOutcome::Completed {
        return Err(ForkExecutionError::deadline_exceeded(
            "sandbox fork hard deadline exceeded",
        ));
    }
    let mut result = successful_fork_result(results)?;
    result.progress_message = "fork completed at its hard deadline".to_string();
    Ok(result)
}

fn fork_outcome(results: &[ForkTargetOutcome]) -> pplx_space_metrics::sandbox_fork::ForkOutcome {
    if !results.is_empty()
        && results
            .iter()
            .all(|result| matches!(result, ForkTargetOutcome::Succeeded { .. }))
    {
        pplx_space_metrics::sandbox_fork::ForkOutcome::Completed
    } else {
        pplx_space_metrics::sandbox_fork::ForkOutcome::Failed
    }
}

fn emit_completed_target_observability(
    source_sandbox_id: SandboxId,
    operation_id: pplx_space_public_api::OperationId,
    snapshot_id: Option<SnapshotId>,
    results: &[ForkTargetOutcome],
) {
    emit_terminal_fork_observability(
        source_sandbox_id,
        operation_id,
        snapshot_id,
        fork_outcome(results),
    );
}

fn emit_terminal_fork_observability(
    source_sandbox_id: SandboxId,
    operation_id: pplx_space_public_api::OperationId,
    snapshot_id: Option<SnapshotId>,
    outcome: pplx_space_metrics::sandbox_fork::ForkOutcome,
) {
    let (event_type, mut details) = match outcome {
        pplx_space_metrics::sandbox_fork::ForkOutcome::Completed => (
            SandboxEventType::ForkCompleted,
            SandboxEventDetails::default().operation(operation_id.to_string()),
        ),
        pplx_space_metrics::sandbox_fork::ForkOutcome::Failed
        | pplx_space_metrics::sandbox_fork::ForkOutcome::DeadlineExceeded => (
            SandboxEventType::ForkFailed,
            SandboxEventDetails::default()
                .operation(operation_id.to_string())
                .error("one or more fork targets failed"),
        ),
    };
    if let Some(snapshot_id) = snapshot_id {
        details = details.snapshot_id(snapshot_id.to_string());
    }
    emit_sandbox_event(
        &source_sandbox_id.to_string(),
        event_type,
        SandboxEventSource::ClusterController,
        details,
    );
    pplx_space_metrics::sandbox_fork::completed(outcome);
}

fn successful_fork_result_after_retirement(
    results: Vec<ForkTargetOutcome>,
    retirement: Result<(), ForkExecutionError>,
) -> Result<PublicOperationExecutionResult, ForkExecutionError> {
    if let Err(error) = retirement {
        tracing::error!(
            error = ?error,
            "copied fork artifact retirement failed; orphan reconciliation is required"
        );
    }
    successful_fork_result(results)
}

fn retry(message: impl Into<String>) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult {
        state: OperationState::Queued,
        progress_message: message.into(),
        result_json: None,
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: Some(RETRY_AFTER),
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn wait(message: impl Into<String>) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult::workflow_progress(message, RETRY_AFTER)
}

fn failed(code: PublicErrorCode, message: String) -> PublicOperationExecutionResult {
    PublicOperationExecutionResult {
        state: OperationState::Failed,
        progress_message: message.clone(),
        result_json: None,
        terminal_error_code: Some(code),
        terminal_error_json: Some(serde_json::json!({ "message": message })),
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

#[derive(Debug)]
enum ForkExecutionError {
    Terminal {
        code: PublicErrorCode,
        message: String,
    },
    DeadlineExceeded {
        code: PublicErrorCode,
        message: String,
    },
    Retry(String),
    Wait(String),
}

impl From<crate::fork_operation_store::ForkOperationStoreError> for ForkExecutionError {
    fn from(error: crate::fork_operation_store::ForkOperationStoreError) -> Self {
        match error {
            crate::fork_operation_store::ForkOperationStoreError::InvalidCaptureWorkflow
            | crate::fork_operation_store::ForkOperationStoreError::InvalidLeaderState(_) => {
                Self::terminal(error.to_string())
            }
            crate::fork_operation_store::ForkOperationStoreError::Storage(_)
            | crate::fork_operation_store::ForkOperationStoreError::Serialize(_)
            | crate::fork_operation_store::ForkOperationStoreError::PublicOperation(_)
            | crate::fork_operation_store::ForkOperationStoreError::ClaimSuperseded
            | crate::fork_operation_store::ForkOperationStoreError::InvalidLeaderPayload(_) => {
                Self::Retry(error.to_string())
            }
        }
    }
}

impl ForkExecutionError {
    fn terminal(message: impl Into<String>) -> Self {
        Self::terminal_with_code(PublicErrorCode::Internal, message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::terminal_with_code(PublicErrorCode::NotFound, message)
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::terminal_with_code(PublicErrorCode::Forbidden, message)
    }

    fn failed_precondition(message: impl Into<String>) -> Self {
        Self::terminal_with_code(PublicErrorCode::FailedPrecondition, message)
    }

    fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::DeadlineExceeded {
            code: PublicErrorCode::FailedPrecondition,
            message: message.into(),
        }
    }

    fn terminal_with_code(code: PublicErrorCode, message: impl Into<String>) -> Self {
        Self::Terminal {
            code,
            message: message.into(),
        }
    }

    fn retry_error(error: impl std::fmt::Display) -> Self {
        Self::Retry(error.to_string())
    }
}

#[cfg(test)]
mod tests;
