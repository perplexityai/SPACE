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
mod teardown_suspend;

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
#[cfg(test)]
use self::teardown_suspend::teardown_artifact_backup_status;
#[cfg(test)]
pub(super) use self::teardown_suspend::{
    SUSPEND_TRIGGER_METADATA, TerminalTeardownSuspend,
    teardown_backup_failure_requires_new_attempt, teardown_suspend_event_trigger,
};

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
    execution_generation: Option<ExecutionGeneration>,
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
    #[error(transparent)]
    RetainedUpload(#[from] suspend_snapshot::RetainedUploadError),
}

impl From<String> for ArtifactBackupSubmissionError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

pub(super) fn artifact_backup_fence_status(error: String) -> Status {
    let mut status = Status::internal(format!("artifact backup submission failed: {error}"));
    status.metadata_mut().insert(
        AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

struct TeardownSuspendBackup<'a> {
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    artifact: &'a crate::db::LocalArtifactRow,
    format: pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat,
}

/// The persisted retained-source identity of a full artifact, if any.
pub(super) fn sidecar_rootfs_send_source_id(
    snapshot_dir: &str,
) -> Result<Option<uuid::Uuid>, Status> {
    let path =
        std::path::Path::new(snapshot_dir).join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Status::internal(format!(
                "failed to read snapshot metadata at {}: {error}",
                path.display()
            )));
        }
    };
    serde_json::from_slice::<crate::snapshot_upload::SnapshotMeta>(&bytes)
        .map(|meta| meta.rootfs_send_source_id)
        .map_err(|error| {
            Status::internal(format!(
                "snapshot metadata present but unparseable at {}: {error}",
                path.display()
            ))
        })
}

#[cfg(test)]
mod backup_policy_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod teardown_backup_tests;
