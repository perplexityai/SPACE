//! The sandbox-manager wire contract: the generated `space.sandbox.manager.v1`
//! protos and their descriptor set, the metadata keys, reasons and capability
//! names carried beside them, and the conversions between the wire and model
//! types. It is its own crate so a crate that needs only the contract does not
//! link the connection code in `pplx-space-sbx-mgr-client`, which re-exports
//! everything here under its old paths.

use uuid::Uuid;

pub mod operations {
    pub use pplx_space_spaced_guest_protocol::proto::operations::*;
}

#[allow(clippy::large_enum_variant)]
pub mod proto {
    tonic::include_proto!("space.sandbox.manager.v1");
    pub use pplx_space_net_gateway_protocol::proto::{SandboxSubstrateKind, SpaceApiRouteIdentity};

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sandbox_manager_descriptor");

    pub use super::operations::{
        CloseLoopbackBridgeResponse as SpacedCloseLoopbackBridgeResponse,
        CloseStdinResponse as SpacedCloseStdinResponse,
        CreateWatcherResponse as SpacedCreateWatcherResponse,
        DisableFileChangesResponse as SpacedDisableFileChangesResponse,
        EnableFileChangesResponse as SpacedEnableFileChangesResponse, FileChunk as SpacedFileChunk,
        FileEvent as SpacedFileEvent, FileEventKind as SpacedFileEventKind,
        FileInfo as SpacedFileInfo, FileType as SpacedFileType,
        GetModifiedFilesResponse as SpacedGetModifiedFilesResponse,
        GetWatcherEventsResponse as SpacedGetWatcherEventsResponse,
        KillResponse as SpacedKillProcessResponse,
        LoopbackBridgeStatus as SpacedLoopbackBridgeStatus, MkdirResponse as SpacedMkdirResponse,
        ModifiedFile as SpacedModifiedFile,
        NormalizeWatchRootResponse as SpacedNormalizeWatchRootResponse,
        OpenLoopbackBridgeResponse as SpacedOpenLoopbackBridgeResponse,
        ProcessConnected as SpacedProcessConnected, ProcessEvent as SpacedProcessEvent,
        ProcessExit as SpacedProcessExit, ProcessExited as SpacedProcessExited,
        ProcessInfo as SpacedProcessInfo, ProcessOutput as SpacedProcessOutput,
        ProcessSignaled as SpacedProcessSignaled, ProcessStarted as SpacedProcessStarted,
        ProcessTimedOut as SpacedProcessTimedOut, ProcessUnknown as SpacedProcessUnknown,
        ReadRequest as SpacedReadOperation, RemoveResponse as SpacedRemoveResponse,
        RemoveWatcherResponse as SpacedRemoveWatcherResponse,
        RenameResponse as SpacedRenameResponse, ResizePtyResponse as SpacedResizePtyResponse,
        SendInputResponse as SpacedSendInputResponse,
        SendSignalResponse as SpacedSendSignalResponse, StatResponse as SpacedStatResponse,
        WatchRequest as SpacedWatchOperation, WriteBatchEntry as SpacedWriteBatchEntry,
        WriteBatchResponse as SpacedWriteBatchResponse, WriteOpen as SpacedWriteOpen,
        WriteResponse as SpacedWriteResponse, process_event as spaced_process_event,
        process_exit as spaced_process_exit, write_request as spaced_write_request,
    };

    /// The canonical home for the core -> proto artifact-kind mapping.
    ///
    /// Every caller that puts an artifact kind on the wire needs it, and the
    /// crate that owns the generated enum is the only place a single copy
    /// can live: `pplx-space-common-core` cannot depend on the proto crate.
    /// Several sites in `pplx-space-sbx-mgr` still inline the same two-arm
    /// match; they can move onto this without any behavior change.
    impl From<pplx_space_common_core::types::ArtifactKind> for ArtifactKind {
        fn from(artifact_kind: pplx_space_common_core::types::ArtifactKind) -> Self {
            match artifact_kind {
                pplx_space_common_core::types::ArtifactKind::Disk => Self::Disk,
                pplx_space_common_core::types::ArtifactKind::Full => Self::Full,
            }
        }
    }
}

pub mod auto_lifecycle;
pub mod lifecycle;

/// Set only on a request refused before it was served, so a successor may
/// replay it. The shim relies on that to read a marked Stop as refused
/// (`shim/space.go`).
pub const RESTART_RETRY_ON_SUCCESSOR_METADATA: &str = "x-space-retry-on-successor";
const TEMPLATE_POLICY_DEPENDENCY_METADATA: &str = "x-space-template-policy-dependency";

/// Policy-owner readiness is not evidence of worker transport or capacity failure.
/// Preserve the status code so callers can distinguish retryable dependency
/// failures from conflicting hints without parsing error text.
pub fn template_policy_dependency_status(mut status: tonic::Status) -> tonic::Status {
    status.metadata_mut().insert(
        TEMPLATE_POLICY_DEPENDENCY_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

pub fn is_template_policy_dependency(status: &tonic::Status) -> bool {
    status
        .metadata()
        .get(TEMPLATE_POLICY_DEPENDENCY_METADATA)
        .is_some_and(|value| value == "true")
}

/// Set on a Stop error sandbox-manager answered before admitting the stop.
/// `Unavailable` also reports a connection that dropped after admission, so a
/// caller that tracks whether its own Stop owns the sandbox reads only a
/// marked error as a refusal. The shim reads it by name (`shim/space.go`), as
/// it reads `RESTART_RETRY_ON_SUCCESSOR_METADATA`, which the restart drain sets
/// on a Stop it refuses before the handler runs.
pub const STOP_NOT_ADMITTED_METADATA: &str = "x-space-stop-not-admitted";

/// `status`, keeping its code, marked as a Stop refused before admission.
pub fn stop_not_admitted_status(mut status: tonic::Status) -> tonic::Status {
    status.metadata_mut().insert(
        STOP_NOT_ADMITTED_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

pub fn is_stop_not_admitted(status: &tonic::Status) -> bool {
    status
        .metadata()
        .get(STOP_NOT_ADMITTED_METADATA)
        .is_some_and(|value| value == "true")
}

pub const CLEANUP_REFUSAL_REASON_METADATA: &str = "x-space-cleanup-refusal-reason";
pub const AUTHORITATIVE_ROUTE_IDENTITY_REQUIRED_METADATA: &str =
    "x-space-authoritative-route-identity-required";
pub const SUSPEND_PUBLICATION_PENDING_REASON: &str = "suspend_publication_pending";
pub const RETAINED_FILESYSTEM_ONLY_COPY_REASON: &str = "retained_filesystem_only_copy";
/// `failure_reason` sandbox-manager stamps on a `suspending` row whose
/// teardown suspend staged its artifact locally but had the durable backup
/// permanently refused by the cluster. No further upload attempt is made for
/// that artifact, so the suspend can never publish and the row cannot leave
/// `suspending` on its own.
///
/// This lives here, not in sandbox-manager, because node-ctl matches it
/// exactly when choosing which cluster-deleted ids to offer for eviction: it
/// is the only `suspending` shape whose teardown is provably finished
/// failing, and other `suspending` failure reasons describe suspends that are
/// still recoverable. The marker proves terminality, never deletion — the
/// cluster's absent-row report remains the sole authority for that.
pub const TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON: &str =
    "teardown_suspend_backup_permanently_failed";
/// `failure_reason` sandbox-manager stamps on a sandbox it quarantined
/// because a suspend lost its runtime without a usable capture of its own:
/// the retained instance filesystem may be the sandbox's only copy.
pub const SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE: &str =
    "suspend_publish_failed_no_artifact_runtime_gone";
/// `failure_reason` for an admitted capture with no conclusive completion
/// evidence. Its runtime may still be live; deleting its files is not a safe
/// recovery action.
pub const SUSPEND_CAPTURE_UNRESOLVED: &str = "suspend_capture_unresolved";

/// The shared retention policy for suspend evidence: whether a quarantined or
/// failed sandbox with this `failure_reason` keeps its retained filesystem.
/// An ordinary delete refuses such a row, and so does the eviction that
/// follows a cluster-deleted report. Only an operator's discard delete
/// (`DeleteRequest.discard_retained_filesystem_reason`) removes it.
/// Here so the CLI checks a row by the same rule before it sends a discard,
/// which an older sandbox-manager would run as an ordinary delete.
pub fn suspend_evidence_requires_retention(failure_reason: Option<&str>) -> bool {
    matches!(
        failure_reason,
        Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE | SUSPEND_CAPTURE_UNRESOLVED)
    )
}
pub const PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE: &str =
    "space.sandbox-manager.capability.process-output-cursor.v1";
/// Start can install the supplied cluster policy before runtime activation.
pub const ACTIVATION_POLICY_HEALTH_SERVICE: &str =
    "space.sandbox-manager.capability.activation-policy.v1";
pub const OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE: &str =
    "space.sandbox-manager.capability.operation-activation-receipt.v1";

pub fn restart_retry_on_successor_status(message: impl Into<String>) -> tonic::Status {
    let mut status = tonic::Status::unavailable(message);
    status.metadata_mut().insert(
        RESTART_RETRY_ON_SUCCESSOR_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

pub fn is_restart_retry_on_successor(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && status
            .metadata()
            .get(RESTART_RETRY_ON_SUCCESSOR_METADATA)
            .is_some_and(|value| value == "true")
}

pub fn authoritative_route_identity_required_status(message: impl Into<String>) -> tonic::Status {
    let mut status = tonic::Status::failed_precondition(message);
    status.metadata_mut().insert(
        AUTHORITATIVE_ROUTE_IDENTITY_REQUIRED_METADATA,
        tonic::metadata::MetadataValue::from_static("true"),
    );
    status
}

pub fn is_authoritative_route_identity_required(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status
            .metadata()
            .get(AUTHORITATIVE_ROUTE_IDENTITY_REQUIRED_METADATA)
            .is_some_and(|value| value == "true")
}

pub fn suspend_publication_pending_status(message: impl Into<String>) -> tonic::Status {
    let mut status = tonic::Status::aborted(message);
    status.metadata_mut().insert(
        CLEANUP_REFUSAL_REASON_METADATA,
        tonic::metadata::MetadataValue::from_static(SUSPEND_PUBLICATION_PENDING_REASON),
    );
    status
}

pub fn is_suspend_publication_pending_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Aborted
        && status
            .metadata()
            .get(CLEANUP_REFUSAL_REASON_METADATA)
            .is_some_and(|value| value == SUSPEND_PUBLICATION_PENDING_REASON)
}

pub fn retained_filesystem_only_copy_status(message: impl Into<String>) -> tonic::Status {
    let mut status = tonic::Status::failed_precondition(message);
    status.metadata_mut().insert(
        CLEANUP_REFUSAL_REASON_METADATA,
        tonic::metadata::MetadataValue::from_static(RETAINED_FILESYSTEM_ONLY_COPY_REASON),
    );
    status
}

pub fn is_retained_filesystem_only_copy_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status
            .metadata()
            .get(CLEANUP_REFUSAL_REASON_METADATA)
            .is_some_and(|value| value == RETAINED_FILESYSTEM_ONLY_COPY_REASON)
}

pub fn encode_optional_sandbox_id(id: Option<Uuid>) -> String {
    id.map(|id| id.to_string()).unwrap_or_default()
}

pub fn direct_restore_start_request(
    sandbox_id: String,
    blocking: bool,
    ready_timeout_secs: Option<u32>,
    suspend_restore_storage_uri: Option<String>,
    suspend_restore_source_node: Option<String>,
) -> proto::StartRequest {
    proto::StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        restore_artifact_as_new: None,
        expected_local_artifact_id: None,
        sandbox_id,
        vcpu: None,
        memory_mib: None,
        checkpoint: Some("suspend".to_string()),
        from_warm: false,
        blocking,
        netns_path: None,
        ready_timeout_secs,
        target_state: 0,
        suspend_restore_storage_uri,
        suspend_restore_source_node,
        route_identity: None,
    }
}

#[cfg(test)]
mod tests;

/// Proof that the worker processed AccessIdleUpdate, rather than an ordinary touch.
/// Kept in response metadata so deployment checks do not expand the protobuf API.
pub const ACCESS_IDLE_UPDATE_ACK_METADATA: &str = "x-space-access-idle-update";
