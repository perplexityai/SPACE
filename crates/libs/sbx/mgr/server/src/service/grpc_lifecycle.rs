use pplx_space_common_core::types::{JobId, LocalArtifactId};

use super::activation_support::{
    GuestActivityKind, IdentityFreeDirectActivationAuthority, WakeNetworkContext, WakeStartConfig,
    grpc_timeout_budget, operation_activation_attempt_id_from_wire, sandbox_event_trigger,
    space_api_route_identity_for_runtime_activation,
    space_api_route_identity_from_wake_start_context, space_api_route_identity_to_wire,
    update_state_best_effort, wake_target_state_from_proto,
};
use super::cleanup::{
    AsyncCleanupRequest, CleanupAdmission, CleanupDispatchDisposition, SynchronousCleanupRequest,
};
use super::cleanup_support::{
    CleanupCompletion, CleanupLockTryResult, OwnedCleanupFenceGuard, require_state_write,
};
use super::create_support::SUSPEND_CHECKPOINT;
use super::network_runtime::*;
use super::recovery::SuspendSnapshotPublicationRecoveryOrigin;
use super::suspend_snapshot_publication::{
    SuspendSnapshotPublicationDecision, canonical_suspend_snapshot_dir,
    classify_suspend_snapshot_publication, publish_suspend_snapshot_dir,
    suspend_snapshot_staging_dir, suspend_snapshot_staging_parent_dir,
};
use super::tail_support::*;
use super::validation::optional_positive_or_default;
use super::*;
use crate::guest_filesystem_flush::{
    GuestSnapshotPreparationBudget, prepare_guest_for_vm_snapshot_via_spaced_host,
};
use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, SandboxId};
use pplx_space_events::{
    SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger,
    SandboxEventType, emit_sandbox_event,
};

mod artifact_backup;
mod auto_lifecycle_policy;
mod backup_submission;
mod kill;
mod pause_resume;
mod sandbox_network_policy;
mod suspend;
mod suspend_snapshot;

use self::artifact_backup::reupload_format_from_sidecar;
#[cfg(test)]
use self::artifact_backup::{
    artifact_backup_format, artifact_backup_lookup_error, current_instance_backup_precondition,
    require_artifact_backup_files,
};
#[cfg(test)]
pub(super) use self::artifact_backup::{
    reupload_precondition_for_cluster_api_env, reupload_resolution_to_config,
};
use self::backup_submission::suspend_snapshot_metadata;
use self::pause_resume::pause_with_reconciliation;
#[cfg(test)]
use self::pause_resume::{PauseReconciliationOutcome, pause_reconciliation_outcome};
#[cfg(test)]
pub(super) use self::suspend::classify_suspend_after_wake;
use self::suspend::emit_suspend_failed_event;
#[cfg(test)]
use self::suspend::ensure_completed_suspend_retry_format;
pub(super) use self::suspend::{suspend_superseded_by_wake, suspend_wake_arbitration_enabled};
#[cfg(test)]
use self::suspend_snapshot::encrypted_snapshot_prepare_error_status;
#[cfg(test)]
pub(super) use self::suspend_snapshot::{
    SuspendSnapshotStagingDisposition, failed_encrypted_capture_may_release_volume,
};

pub(super) const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

pub(super) fn teardown_backup_failure_requires_new_attempt(
    failure: Option<pplx_space_common_jobs::JobFailure>,
) -> bool {
    matches!(failure, Some(pplx_space_common_jobs::JobFailure::Retriable))
}

fn teardown_suspend_event_details(
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    trigger: SandboxEventTrigger,
) -> SandboxEventDetails {
    SandboxEventDetails::default()
        .operation("generation_suspend")
        .trigger(trigger)
        .execution_generation(execution_generation.to_string())
}

pub(super) fn teardown_suspend_event_trigger(
    request: &Request<PrepareTeardownSuspendRequest>,
) -> Result<SandboxEventTrigger, Status> {
    match request
        .metadata()
        .get(SUSPEND_TRIGGER_METADATA)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid suspend trigger metadata"))?
    {
        None | Some("system") => Ok(SandboxEventTrigger::System),
        Some("user") => Ok(SandboxEventTrigger::User),
        Some("auto_lifecycle") => Ok(SandboxEventTrigger::AutoLifecycle),
        Some(_) => Err(Status::invalid_argument("unknown suspend trigger metadata")),
    }
}

fn teardown_suspend_prepare_response(
    outcome: TeardownSuspendOutcome,
    reason: Option<String>,
    metric_outcome: pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome,
    started_at: std::time::Instant,
) -> Response<PrepareTeardownSuspendResponse> {
    pplx_space_metrics::sandbox::worker::teardown_suspend_prepare(metric_outcome, started_at);
    let span = tracing::Span::current();
    span.record("grpc.response.outcome", outcome.as_str_name());
    if let Some(reason) = reason.as_deref() {
        span.record("grpc.response.reason", reason);
    }
    Response::new(PrepareTeardownSuspendResponse {
        outcome: outcome as i32,
        reason,
        published_artifact: None,
    })
}

fn durable_teardown_suspend_prepare_response(
    status: &crate::db::TeardownSuspendStatus,
    started_at: std::time::Instant,
) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
    let artifact = status.artifact.as_ref().ok_or_else(|| {
        Status::internal("durable teardown suspend is missing its published artifact")
    })?;
    let artifact_kind = match artifact.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Disk => crate::proto::ArtifactKind::Disk,
        pplx_space_common_core::types::ArtifactKind::Full => crate::proto::ArtifactKind::Full,
    };
    let mut response = teardown_suspend_prepare_response(
        TeardownSuspendOutcome::Durable,
        None,
        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Durable,
        started_at,
    )
    .into_inner();
    response.published_artifact = Some(crate::proto::PublishedArtifact {
        local_artifact_id: artifact.local_artifact_id.to_string(),
        artifact_kind: artifact_kind as i32,
        origin: crate::proto::ArtifactOrigin::Suspend as i32,
        snapshot_name: None,
    });
    Ok(Response::new(response))
}

/// Outcome of [`SandboxManagerService::submit_artifact_backup`].
/// `Enqueued` carries the durable-worker job id and the backup attempt id
/// owned by that job. The attempt may differ from the caller's candidate when
/// a deduplicated submission joins an existing job. `NoUploadConfig` is the
/// silent fall-through when neither the request override nor the env-derived
/// config is present (e.g. local dev without `SPACE_SNAPSHOT_BUCKET`).
#[derive(Debug)]
pub(super) enum SubmitSuspendSnapshotOutcome {
    Enqueued {
        job_id: JobId,
        backup_attempt_id: String,
    },
    NoUploadConfig,
}

/// Outcome of resolving a sandbox's org-level snapshot upload override
/// from the cluster-api. Callers distinguish "definitively no override"
/// (env-default applies) from "lookup failed transiently" (retryable),
/// because backup admission needs that distinction to avoid surfacing
/// a transient cluster-api blip as a terminal `FailedPrecondition` that
/// the drain reconciler would treat as mark-as-failed.
pub(super) enum OrgSnapshotConfigResolution {
    /// Override is present and parses to a valid upload config.
    Resolved(crate::snapshot_upload::SnapshotUploadConfig),
    /// Definitively no override: cluster-api isn't wired (local dev),
    /// the org left it unset, or the returned URI is empty. Callers
    /// fall back to the env-derived default with no logging.
    Absent,
    /// Lookup failed transiently or due to operator-fixable config
    /// (cluster-api unreachable, RPC errored, bearer/endpoint invalid,
    /// override URI didn't parse). Suspend collapses this to silent
    /// fall-through; reupload surfaces it as `Internal` so the
    /// reconciler retries instead of escalating to mark-as-failed.
    LookupFailed(String),
    /// `GetSandboxSnapshotStorageUri` returned `NotFound`: no sandbox row
    /// exists at lookup time. This alone does not distinguish a create race
    /// from deletion. Ordinary suspend permits the create race (the local
    /// row can precede cluster commit); for reupload it indicates divergence,
    /// because the sandbox must already have been suspended at least
    /// once (and therefore registered with the cluster) before reupload
    /// can be invoked. Suspend collapses this to silent fall-through;
    /// reupload surfaces it as `FailedPrecondition` so the reconciler
    /// stops issuing reupload work that the durable worker would
    /// permanently fail at the `BeginArtifactBackup` step. Teardown combines
    /// this absence with a permanent backup refusal for an owned execution
    /// and artifact to close that attempt. It grants no runtime-release proof;
    /// cluster-deleted eviction still independently verifies deletion.
    ClusterRowMissing,
}

/// `SPACE_SUSPEND_DEFAULT_FORMAT=full` is the rollback lever that returns the
/// suspend-codec default from `BtrfsSend` to `Full` for callers that don't
/// supply an explicit `format`. Any other value keeps `BtrfsSend`. Resolved
/// per-request so ops can flip it without restarting workers.
fn env_default_is_full() -> bool {
    matches!(
        std::env::var("SPACE_SUSPEND_DEFAULT_FORMAT").as_deref(),
        Ok("full"),
    )
}

struct LocalArtifactStagingGuard {
    path: std::path::PathBuf,
    armed: bool,
}

impl LocalArtifactStagingGuard {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    async fn cleanup(mut self, node_client: &mut NodeManagerGrpcClient) {
        self.armed = false;
        // An encode RPC error is not proof that its host writer has exited.
        // Node-manager serializes deletion with that writer, including reap.
        if let Err(error) = node_client
            .delete_cache_artifact(
                pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                    local_path: self.path.to_string_lossy().into_owned(),
                },
            )
            .await
        {
            tracing::warn!(path = %self.path.display(), %error,
                "failed to remove suspend staging; retained for inventory recovery or inspection");
        }
    }
}

impl Drop for LocalArtifactStagingGuard {
    fn drop(&mut self) {
        if self.armed {
            // Abandonment is not proof that the remote capture stopped writing.
            // Retain the evidence for recovery or operator inspection rather
            // than performing blocking deletion in Drop.
            tracing::warn!(path = %self.path.display(),
                "suspend publication abandoned; staging retained for inventory inspection");
        }
    }
}

pub(super) struct SuspendSnapshotPublicationContext {
    cleanup_fence: OwnedCleanupFenceGuard,
    sandbox: crate::db::SandboxRow,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    suspend_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    create_codec: &'static str,
    local_artifact_id: LocalArtifactId,
    event_details: SandboxEventDetails,
    started_at: std::time::Instant,
    publish_node_state: bool,
    durability: SuspendDurability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SuspendDurability {
    NodeLocal,
    RemoteBeforeTeardown {
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    },
}

impl SuspendDurability {
    const fn is_remote_before_teardown(self) -> bool {
        matches!(self, Self::RemoteBeforeTeardown { .. })
    }

    const fn execution_generation(
        self,
    ) -> Option<pplx_space_common_core::types::ExecutionGeneration> {
        match self {
            Self::NodeLocal => None,
            Self::RemoteBeforeTeardown {
                execution_generation,
            } => Some(execution_generation),
        }
    }
}

pub(super) struct SuspendRequestContext {
    sandbox_id: String,
    blocking: bool,
    requested_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    suspend_format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    create_codec: &'static str,
    publish_node_state: bool,
    event_trigger: Option<SandboxEventTrigger>,
    durability: SuspendDurability,
}

impl SuspendRequestContext {
    fn guest_preparation_budget(&self) -> GuestSnapshotPreparationBudget {
        if self.event_trigger == Some(SandboxEventTrigger::AutoLifecycle) {
            GuestSnapshotPreparationBudget::AutoLifecycle
        } else {
            GuestSnapshotPreparationBudget::Standard
        }
    }

    pub(super) fn new(
        sandbox_id: String,
        blocking: bool,
        format: i32,
        publish_node_state: bool,
        event_trigger: Option<SandboxEventTrigger>,
    ) -> Self {
        // Per-request rootfs codec. `Unspecified` (the default existing
        // API-substrate callers send) maps to `BtrfsSend`, or to `Full` when
        // `SPACE_SUSPEND_DEFAULT_FORMAT=full` is set on this worker (the
        // rollback lever). Explicit `BtrfsSend`/`Full` on the wire still
        // overrides the default.
        let requested_format =
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::try_from(format)
                .unwrap_or(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified);
        let suspend_format = match requested_format {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified => {
                if env_default_is_full() {
                    pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full
                } else {
                    pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
                }
            }
            other => other,
        };
        // Codec tag for the `snapshot.create.*` metric family. Mirrors
        // the closed vocabulary in `pplx_space_metrics::snapshot`:
        // `full` and `btrfs_send_v1` (note underscore for the tag value
        // vs hyphen in the wire vocabulary).
        let create_codec = match suspend_format {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend => {
                pplx_space_metrics::snapshot::CODEC_BTRFS_SEND_V1
            }
            _ => pplx_space_metrics::snapshot::CODEC_FULL,
        };

        Self {
            sandbox_id,
            blocking,
            requested_format,
            suspend_format,
            create_codec,
            publish_node_state,
            event_trigger,
            durability: SuspendDurability::NodeLocal,
        }
    }

    pub(super) fn for_teardown(
        sandbox_id: String,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Self {
        let mut context = Self::new(
            sandbox_id,
            true,
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Unspecified as i32,
            true,
            Some(SandboxEventTrigger::System),
        );
        context.durability = SuspendDurability::RemoteBeforeTeardown {
            execution_generation,
        };
        context
    }
}

struct ArtifactBackupSubmission<'a> {
    local_artifact_id: LocalArtifactId,
    artifact_kind: pplx_space_common_core::types::ArtifactKind,
    artifact_origin: pplx_space_common_core::types::ArtifactOrigin,
    snapshot_name: Option<&'a str>,
    backup_attempt_id: pplx_space_common_core::types::BackupAttemptId,
    snapshot_dir: &'a str,
    request_override: Option<&'a crate::snapshot_upload::SnapshotUploadConfig>,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
    dedup_key: Option<&'a str>,
    handoff: ArtifactBackupHandoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactBackupHandoff {
    /// Persist the intent and let the durable worker perform remote work.
    DurableWorker,
    /// Stage immutable transfer handles and create the restore-visible cluster
    /// row before returning, then let the durable worker poll and finalize.
    RestoreVisible,
    /// Stage and register the exact artifact before runtime teardown.
    TeardownFence,
}

pub(super) const AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA: &str =
    "x-space-auto-lifecycle-suspend-backup-fence-failed";

#[derive(Debug, thiserror::Error)]
enum ArtifactBackupSubmissionError {
    #[error("{0}")]
    Other(String),
    #[error("begin restore-visible snapshot backup: {0}")]
    Begin(#[source] pplx_space_common_jobs::JobError),
}

impl From<String> for ArtifactBackupSubmissionError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

const TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA: &str =
    "x-space-teardown-backup-failed-artifact-bin";

pub(super) fn artifact_backup_fence_status(error: String) -> Status {
    let mut status = Status::internal(format!("artifact backup submission failed: {error}"));
    status.metadata_mut().insert(
        AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

fn teardown_artifact_backup_status(
    error: ArtifactBackupSubmissionError,
    artifact: LocalArtifactId,
) -> Status {
    let permanent = matches!(&error, ArtifactBackupSubmissionError::Begin(error)
        if error.kind() == pplx_space_common_jobs::JobFailureKind::Permanent);
    let mut status = artifact_backup_fence_status(error.to_string());
    if permanent {
        status = teardown_backup_failure_status(status, artifact);
    }
    status
}

// The shared publication path returns tonic Status. Keep backup provenance on
// its teardown-only errors until the driver has verified control-plane absence.
fn teardown_backup_failure_status(mut status: Status, artifact: LocalArtifactId) -> Status {
    status.metadata_mut().insert_bin(
        TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA,
        tonic::metadata::MetadataValue::from_bytes(artifact.as_uuid().as_bytes()),
    );
    status
}

fn teardown_failed_backup_artifact(status: &Status) -> Option<LocalArtifactId> {
    let value = status
        .metadata()
        .get_bin(TEARDOWN_BACKUP_FAILED_ARTIFACT_METADATA)?
        .to_bytes()
        .ok()?;
    Some(LocalArtifactId::from_uuid(
        uuid::Uuid::from_slice(&value).ok()?,
    ))
}

struct TeardownSuspendBackup<'a> {
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    artifact: &'a crate::db::LocalArtifactRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
}

impl SandboxManagerService {
    pub(super) async fn prepare_teardown_suspend(
        &self,
        request: Request<PrepareTeardownSuspendRequest>,
    ) -> Result<Response<PrepareTeardownSuspendResponse>, Status> {
        let metric_started_at = std::time::Instant::now();
        self.authorize_peer(&request, "PrepareTeardownSuspend", ALLOW_SANDBOX_OPERATOR)?;
        let event_trigger = teardown_suspend_event_trigger(&request)?;
        let req = request.get_ref();
        let sandbox_id: SandboxId = req
            .sandbox_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        // Runtime attachment establishes this node-local lease from the route
        // identity already passed down with the execution.
        let execution_generation =
            pplx_space_common_core::types::ExecutionGeneration::new(req.execution_generation)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let span = tracing::Span::current();
        span.record("sandbox_id", tracing::field::display(sandbox_id));
        span.record("execution_generation", execution_generation.get());
        if let Some(node_id) = self.local_node_id {
            span.record("current_node_id", tracing::field::display(node_id));
        }
        let action = TeardownSuspendAction::try_from(req.action)
            .ok()
            .filter(|action| *action != TeardownSuspendAction::Unspecified)
            .ok_or_else(|| Status::invalid_argument("teardown suspend action is required"))?;

        let sandbox_id_string = sandbox_id.to_string();
        let evidence = {
            let db = self.db_conn()?;
            crate::db::teardown_release_evidence(&db, &sandbox_id_string, execution_generation)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let Some(mut status) = evidence.suspend else {
            if evidence.deleted {
                return Ok(teardown_suspend_prepare_response(
                    TeardownSuspendOutcome::Deleted,
                    Some("a generation-fenced local delete covers this execution".to_string()),
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Deleted,
                    metric_started_at,
                ));
            }
            self.reject_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?;
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                Some("sandbox record is not present on this node".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        };

        if evidence.activation_rolled_back {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a generation-fenced activation rollback retired this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }

        let local_generation = match evidence.runtime_generation {
            Some(generation) => generation,
            None if status.state == SandboxState::Suspended => execution_generation,
            None => {
                self.reject_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?;
                return Ok(teardown_suspend_prepare_response(
                    TeardownSuspendOutcome::Pending,
                    Some("local runtime execution generation is not available".to_string()),
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                    metric_started_at,
                ));
            }
        };
        if local_generation != execution_generation {
            let (outcome, reason, metric_outcome) = if local_generation > execution_generation {
                (
                    TeardownSuspendOutcome::Superseded,
                    "a newer local execution owns generation suspend",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                )
            } else {
                (
                    TeardownSuspendOutcome::Pending,
                    "the requested execution does not own the local runtime",
                    pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                )
            };
            return Ok(teardown_suspend_prepare_response(
                outcome,
                Some(reason.to_string()),
                metric_outcome,
                metric_started_at,
            ));
        }
        // A completed STOP has already released this execution's runtime;
        // retain its disk and let containerd discard only the obsolete task.
        // Cleanup preserves the retired generation as the release witness.
        if status.state == SandboxState::Stopped {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a generation-fenced stop retired this execution".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }
        if status
            .operation
            .as_ref()
            .is_some_and(|operation| operation.execution_generation > execution_generation)
        {
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Superseded,
                Some("a newer local execution owns generation suspend".to_string()),
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                metric_started_at,
            ));
        }
        self.reject_terminal_teardown_suspend(&sandbox_id_string, execution_generation)?;
        if action == TeardownSuspendAction::BeginOrJoin
            && matches!(status.state, SandboxState::Started | SandboxState::Paused)
        {
            let db = self.db_conn()?;
            match crate::db::begin_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
        }
        let has_exact_operation = status.operation.as_ref().is_some_and(|operation| {
            operation.execution_generation == execution_generation
                && operation.status.can_support_release_proof()
        });
        let can_bind_execution = status
            .operation
            .as_ref()
            .is_none_or(|operation| operation.execution_generation <= execution_generation);
        let mut bound_existing_suspend = false;
        if action == TeardownSuspendAction::BeginOrJoin
            && status.state == SandboxState::Suspended
            && !has_exact_operation
            && can_bind_execution
        {
            let db = self.db_conn()?;
            match crate::db::bind_suspended_teardown_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::InvalidQuery) => {
                    return Ok(teardown_suspend_prepare_response(
                        TeardownSuspendOutcome::Superseded,
                        Some("a newer local execution owns generation suspend".to_string()),
                        pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Superseded,
                        metric_started_at,
                    ));
                }
                Err(error) => return Err(Status::internal(format!("db error: {error}"))),
            }
            status = crate::db::teardown_suspend_status(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            bound_existing_suspend = true;
        }
        if bound_existing_suspend {
            emit_sandbox_event(
                &sandbox_id.to_string(),
                SandboxEventType::SuspendRequested,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .with_from_state(SandboxState::Suspended.to_string()),
            );
        }
        if status.is_durable_for(execution_generation) {
            return durable_teardown_suspend_prepare_response(&status, metric_started_at);
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id.to_string(),
                execution_generation,
                event_trigger,
            )?;
            status = {
                let db = self.db_conn()?;
                crate::db::teardown_suspend_status(&db, &sandbox_id_string)
                    .map_err(|error| Status::internal(format!("db error: {error}")))?
                    .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
            };
            if status.is_durable_for(execution_generation) {
                return durable_teardown_suspend_prepare_response(&status, metric_started_at);
            }
            return Err(Status::internal(
                "generation suspend completion did not bind its durable artifact",
            ));
        }
        let state = status.state;
        let resume_persisted = status.operation.as_ref().is_some_and(|operation| {
            operation.status == crate::db::TeardownSuspendOperationStatus::Running
                && operation.execution_generation == execution_generation
        });
        let begin_new = action == TeardownSuspendAction::BeginOrJoin
            && matches!(state, SandboxState::Started | SandboxState::Paused);
        if begin_new || resume_persisted {
            self.spawn_teardown_suspend_driver(sandbox_id, execution_generation, event_trigger);
            return Ok(teardown_suspend_prepare_response(
                TeardownSuspendOutcome::Pending,
                None,
                pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
                metric_started_at,
            ));
        }
        Ok(teardown_suspend_prepare_response(
            TeardownSuspendOutcome::Pending,
            Some(format!(
                "sandbox is {} without a durable teardown artifact",
                state
            )),
            pplx_space_metrics::sandbox::worker::TeardownSuspendPrepareOutcome::Pending,
            metric_started_at,
        ))
    }

    fn reject_terminal_teardown_suspend(
        &self,
        sandbox_id: &str,
        generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        let state = crate::db::get_sandbox(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?
            .map(|row| row.state);
        let operation = crate::db::latest_teardown_suspend_operation(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        // A failed older generation must not block a new execution.
        let exact = operation
            .as_ref()
            .filter(|operation| operation.execution_generation == generation);
        let reason = match state {
            Some(SandboxState::Quarantined) => Some(crate::db::TeardownSuspendFailure::Quarantined),
            Some(SandboxState::Deleting) => Some(crate::db::TeardownSuspendFailure::Deleting),
            None if exact.is_some() => Some(crate::db::TeardownSuspendFailure::Missing),
            _ if exact.is_some_and(|operation| {
                operation.status == crate::db::TeardownSuspendOperationStatus::Failed
            }) && !crate::db::teardown_suspend_retry_admissible(
                &db, sandbox_id, generation,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))? =>
            {
                Some(crate::db::TeardownSuspendFailure::Failed)
            }
            _ => None,
        };
        if let Some(reason) = reason {
            crate::db::fail_teardown_suspend_operation(&db, sandbox_id, generation, reason)
                .map_err(|error| {
                    Status::internal(format!("persist terminal teardown suspend: {error}"))
                })?;
            return Err(Status::failed_precondition(reason.as_str()));
        }
        Ok(())
    }

    pub(super) fn spawn_teardown_suspend_driver(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> TeardownSuspendDriverAdmission {
        let dashmap::mapref::entry::Entry::Vacant(entry) =
            self.teardown_suspend_drivers.entry(sandbox_id)
        else {
            return TeardownSuspendDriverAdmission::Existing;
        };
        let permit = match Arc::clone(&self.teardown_suspend_driver_sem).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                pplx_space_metrics::sandbox::worker::teardown_suspend_driver_saturated();
                tracing::debug!(
                    %sandbox_id,
                        "generation suspend driver capacity is full; caller polling will retry admission"
                );
                return TeardownSuspendDriverAdmission::Saturated;
            }
        };
        entry.insert(permit);

        let driver_slot = TeardownSuspendDriverSlot {
            drivers: Arc::clone(&self.teardown_suspend_drivers),
            sandbox_id,
        };
        let service = self.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.task_tracker.spawn(async move {
            let _driver_slot = driver_slot;
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!(
                        %sandbox_id,
                        "stopped generation suspend driver during service shutdown"
                    );
                }
                result = service.drive_teardown_suspend(
                    sandbox_id,
                    execution_generation,
                    event_trigger,
                ) => {
                    if let Err(error) = result {
                        tracing::warn!(
                            %sandbox_id,
                            code = %error.code(),
                            error = %error.message(),
                            "generation suspend did not become durable"
                        );
                    }
                }
            }
        });
        TeardownSuspendDriverAdmission::Started
    }

    async fn drive_teardown_suspend(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let _state_writer = self.state_writer_lease("PrepareTeardownSuspendBackground")?;
        let result = self
            .drive_teardown_suspend_inner(sandbox_id, execution_generation, event_trigger)
            .await;
        if let Err(error) = &result {
            // Registration can also return NotFound for its source node. Confirm
            // sandbox absence with the sandbox-only lookup before making this final.
            // Legacy persisted failures have no typed reason, so verify every
            // permanent backup refusal rather than parsing its historical message.
            let backup_lookup = if teardown_failed_backup_artifact(error).is_some() {
                Some(
                    self.resolve_org_snapshot_upload_config(&sandbox_id.to_string())
                        .await,
                )
            } else {
                None
            };
            if let Err(record_error) = self.record_teardown_suspend_driver_failure(
                sandbox_id,
                execution_generation,
                error,
                backup_lookup.as_ref(),
            ) {
                tracing::warn!(%sandbox_id, %execution_generation, %record_error,
                    "failed to persist teardown suspend driver failure; caller polling will retry");
            }
        }
        result
    }

    fn record_teardown_suspend_driver_failure(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        error: &Status,
        backup_lookup: Option<&OrgSnapshotConfigResolution>,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        let local = crate::db::get_sandbox(&db, &sandbox_id.to_string())
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        let reason = match local {
            None => Some(crate::db::TeardownSuspendFailure::Missing),
            Some(row) if row.state == SandboxState::Quarantined => {
                Some(crate::db::TeardownSuspendFailure::Quarantined)
            }
            Some(row) if row.state == SandboxState::Deleting => {
                Some(crate::db::TeardownSuspendFailure::Deleting)
            }
            _ => None,
        };
        if let Some(reason) = reason {
            crate::db::fail_teardown_suspend_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
                reason,
            )
            .map_err(|error| {
                Status::internal(format!("persist terminal teardown suspend: {error}"))
            })?;
        } else if let Some(failed_artifact) = teardown_failed_backup_artifact(error)
            && matches!(
                backup_lookup,
                Some(OrgSnapshotConfigResolution::ClusterRowMissing)
            )
        {
            let failed = crate::db::fail_teardown_suspend_backup_operation(
                &db,
                &sandbox_id.to_string(),
                execution_generation,
                failed_artifact,
            )
            .map_err(|error| {
                Status::internal(format!("persist terminal teardown backup: {error}"))
            })?;
            drop(db);
            if failed {
                mark_sandbox_dirty(&self.db, &sandbox_id.to_string());
            }
        }
        Ok(())
    }

    async fn drive_teardown_suspend_inner(
        &self,
        sandbox_id: SandboxId,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let sandbox_id_string = sandbox_id.to_string();
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id_string);
        let wake_guard = wake_lock.lock_owned().await;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox_id_string);
        let artifact_guard = artifact_lock.lock_owned().await;
        let status = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox_id_string)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?
        };
        if status.is_durable_for(execution_generation) {
            return Ok(());
        }
        if status.is_ready_for(execution_generation) {
            self.complete_teardown_suspend_operation(
                &sandbox_id_string,
                execution_generation,
                event_trigger,
            )?;
            return Ok(());
        }
        self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
        let active_generation = Some(execution_generation);
        let result: Result<(), Status> = match (status.state, status.artifact) {
            (SandboxState::Started | SandboxState::Paused, _) => {
                let context = SuspendRequestContext::for_teardown(
                    sandbox_id_string.clone(),
                    execution_generation,
                );
                let (_, publication) = self
                    .prepare_suspend_snapshot_publication_after_locks(
                        &context,
                        &wake_guard,
                        &artifact_guard,
                    )
                    .await?;
                self.complete_suspend_snapshot_publication(publication)
                    .await
            }
            (
                SandboxState::Suspending
                | SandboxState::CleanupRequired
                | SandboxState::CleanupFailed,
                Some(artifact),
            ) if active_generation == Some(execution_generation) => {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                    },
                )
                .await?;
                self.ensure_teardown_suspend_owner(&sandbox_id_string, execution_generation)?;
                self.finalize_suspended_runtime(
                    &sandbox,
                    artifact.local_artifact_id,
                    &artifact.local_path,
                    None,
                    None,
                )
                .await?;
                mark_sandbox_dirty(&self.db, &sandbox_id_string);
                Ok(())
            }
            (SandboxState::Suspended, Some(artifact))
                if active_generation == Some(execution_generation) =>
            {
                let sandbox = self.lookup_sandbox(&sandbox_id_string)?;
                let format = reupload_format_from_sidecar(&artifact.local_path)?;
                self.ensure_teardown_suspend_backup(
                    &sandbox,
                    TeardownSuspendBackup {
                        execution_generation,
                        artifact: &artifact,
                        format,
                    },
                )
                .await
            }
            (state, _) => Err(Status::failed_precondition(format!(
                "cannot continue teardown suspend from {}",
                state
            ))),
        };
        result?;
        self.complete_teardown_suspend_operation(
            &sandbox_id_string,
            execution_generation,
            event_trigger,
        )?;
        Ok(())
    }

    fn ensure_teardown_suspend_owner(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    ) -> Result<(), Status> {
        let status = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::teardown_suspend_status(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
        };
        let owns_runtime = status
            .and_then(|status| status.operation)
            .is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status == crate::db::TeardownSuspendOperationStatus::Running
            });
        if owns_runtime {
            Ok(())
        } else {
            Err(Status::aborted(
                "execution generation no longer owns sandbox",
            ))
        }
    }

    fn ensure_teardown_suspend_owner_for_durability(
        &self,
        sandbox_id: &str,
        durability: SuspendDurability,
    ) -> Result<(), Status> {
        if let Some(execution_generation) = durability.execution_generation() {
            self.ensure_teardown_suspend_owner(sandbox_id, execution_generation)?;
        }
        Ok(())
    }

    fn complete_teardown_suspend_operation(
        &self,
        sandbox_id: &str,
        execution_generation: pplx_space_common_core::types::ExecutionGeneration,
        event_trigger: SandboxEventTrigger,
    ) -> Result<(), Status> {
        let completed = {
            let db = self.db_conn()?;
            let status = crate::db::teardown_suspend_status(&db, sandbox_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found("sandbox record no longer exists"))?;
            let is_ready = status.is_ready_for(execution_generation);
            let artifact_id = status
                .artifact
                .filter(|_| is_ready)
                .map(|artifact| artifact.local_artifact_id)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "generation suspend artifact is not remotely durable",
                    )
                })?;
            let completed = crate::db::complete_teardown_suspend(
                &db,
                sandbox_id,
                execution_generation,
                Some(artifact_id),
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
            if !completed {
                crate::db::bind_completed_teardown_suspend_artifact(
                    &db,
                    sandbox_id,
                    execution_generation,
                    artifact_id,
                )
                .map_err(|error| Status::internal(format!("db error: {error}")))?;
            }
            completed
        };
        if completed {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::SuspendCompleted,
                SandboxEventSource::SbxManager,
                teardown_suspend_event_details(execution_generation, event_trigger)
                    .result("durable")
                    .with_to_state(SandboxState::Suspended.to_string()),
            );
        }
        Ok(())
    }

    /// Record that a teardown suspend's durable backup is over, on the row
    /// the shim keeps polling.
    ///
    /// A permanent job failure ends the artifact's upload for good:
    /// [`teardown_backup_failure_requires_new_attempt`] refuses a fresh
    /// attempt for it, so every later poll rejoins the same terminal job.
    /// The suspend therefore can never publish, the row can never leave
    /// `suspending`, and nothing on this node distinguishes it from a suspend
    /// still uploading — a distinction the cluster-deleted eviction needs
    /// before it may reclaim anything. The marker supplies exactly that
    /// terminality proof and nothing else; whether the sandbox is *gone*
    /// stays the cluster's call.
    ///
    /// Best-effort: a failed stamp leaves the pre-existing stuck row, which is
    /// the behavior without this marker at all, and the next poll retries.
    fn record_teardown_suspend_backup_permanent_failure(
        &self,
        sandbox_id: &str,
        staged_artifact: LocalArtifactId,
    ) {
        let marked = self.db_conn().and_then(|db| {
            crate::db::mark_teardown_suspend_backup_permanently_failed(
                &db,
                sandbox_id,
                staged_artifact,
            )
            .map_err(|error| Status::internal(format!("db error: {error}")))
        });
        match marked {
            Ok(true) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    "generation suspend backup was permanently refused; marked the suspending \
                     row terminal so a cluster-deleted report can reclaim it"
                );
                mark_sandbox_dirty(&self.db, sandbox_id);
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    %staged_artifact,
                    %error,
                    "failed to mark a permanently refused generation suspend backup; the \
                     suspending row stays unreclaimable until a later attempt marks it"
                );
            }
        }
    }

    async fn ensure_teardown_suspend_backup(
        &self,
        sandbox: &crate::db::SandboxRow,
        backup: TeardownSuspendBackup<'_>,
    ) -> Result<(), Status> {
        let artifact_dedup_key = backup.artifact.local_artifact_id.to_string();
        let persisted_job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &artifact_dedup_key,
        )?;
        let job_id = match persisted_job {
            Some(job)
                if !job.is_terminal()
                    || !teardown_backup_failure_requires_new_attempt(job.failure) =>
            {
                job.job_id
            }
            _ => match self
                .enqueue_suspend_artifact_backup(
                    sandbox,
                    backup.artifact.local_artifact_id,
                    &backup.artifact.local_path,
                    backup.format,
                    Some(&artifact_dedup_key),
                )
                .await
                .map_err(|error| {
                    teardown_artifact_backup_status(error, backup.artifact.local_artifact_id)
                })? {
                SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. } => job_id,
                SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                    return Err(Status::failed_precondition(
                        "generation suspend requires a configured snapshot upload destination",
                    ));
                }
            },
        };
        let job = self
            .snapshot_upload_worker
            .wait_for_job(&job_id, self.snapshot_upload_worker.worst_case_runtime())
            .await?;
        if !job.is_terminal() || job.failure.is_some() {
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) {
                self.record_teardown_suspend_backup_permanent_failure(
                    &sandbox.id,
                    backup.artifact.local_artifact_id,
                );
            }
            let mut error = Status::internal(
                job.error
                    .unwrap_or_else(|| "durable suspend backup failed".to_string()),
            );
            if matches!(
                job.failure,
                Some(pplx_space_common_jobs::JobFailure::Permanent { .. })
            ) {
                error = teardown_backup_failure_status(error, backup.artifact.local_artifact_id);
            }
            return Err(error);
        }
        {
            let db = self.db_conn()?;
            crate::db::mark_local_artifact_backup_completed(
                &db,
                &backup.artifact.local_artifact_id.to_string(),
            )
            .map_err(|error| {
                Status::internal(format!("persist durable suspend completion: {error}"))
            })?;
        }

        let still_owned = {
            let db = self.db_conn()?;
            crate::db::teardown_suspend_status(&db, &sandbox.id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .is_some_and(|status| {
                    status.backup_completed
                        && status.operation.is_some_and(|operation| {
                            operation.status.is_active()
                                && operation.execution_generation == backup.execution_generation
                        })
                        && status.artifact.is_some_and(|artifact| {
                            artifact.local_artifact_id == backup.artifact.local_artifact_id
                        })
                })
        };
        if !still_owned {
            return Err(Status::aborted(
                "generation suspend no longer owns durable suspend artifact",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
