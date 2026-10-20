pub mod environment;
use std::path::Path;

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

pub mod operations {
    pub use pplx_space_spaced_common::proto::operations::*;
}

#[allow(clippy::large_enum_variant)]
pub mod proto {
    tonic::include_proto!("space.sandbox.manager.v1");
    pub use pplx_space_net_gateway_client::proto::{SandboxSubstrateKind, SpaceApiRouteIdentity};

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

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Unix socket path for the sbx-manager gRPC service.
pub const SOCKET_PATH: &str = pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_SOCKET;
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

pub type SandboxManagerGrpcClient =
    proto::sandbox_manager_client::SandboxManagerClient<GrpcClientTrace<Channel>>;

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const HTTP2_KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

fn configured_endpoint() -> Result<Endpoint, tonic::transport::Error> {
    Ok(Endpoint::try_from(LOCAL_ENDPOINT_URI)?
        .max_frame_size(pplx_space_common_grpc::BULK_HTTP2_MAX_FRAME_SIZE)
        .timeout(REQUEST_TIMEOUT)
        .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT))
}

fn client_from_channel(channel: Channel) -> SandboxManagerGrpcClient {
    proto::sandbox_manager_client::SandboxManagerClient::new(wrap_grpc_client_channel(channel))
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

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("sandbox-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("sandbox-manager connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
}

pub use pplx_space_common_grpc::GrpcRetryPolicy;

/// The connect budget [`connect`] spends.
///
/// Re-exported so a caller that carries the budget in its own configuration
/// can default to the shared policy without duplicating its value.
pub const DEFAULT_CONNECT_RETRY_POLICY: GrpcRetryPolicy =
    pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY;

/// Connect to the sbx-manager gRPC service over Unix socket.
pub async fn connect() -> Result<SandboxManagerGrpcClient, ConnectError> {
    connect_with_retry_policy(DEFAULT_CONNECT_RETRY_POLICY).await
}

/// Connect to the sbx-manager gRPC service over Unix socket under a caller-
/// chosen connect retry budget.
///
/// [`connect`] spends the shared default budget. This exists for callers
/// that carry the budget in their own configuration, so a test that runs
/// without a sandbox-manager socket can decline the retries.
pub async fn connect_with_retry_policy(
    retry_policy: GrpcRetryPolicy,
) -> Result<SandboxManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint_with_retry(
        "sandbox-manager",
        Path::new(SOCKET_PATH),
        configured_endpoint()?,
        retry_policy,
    )
    .await?;
    Ok(client_from_channel(channel))
}

/// Dial a fresh, uncached connection to the sandbox-manager socket.
///
/// During a sandbox-manager restart handoff, a shared channel's established
/// HTTP/2 connection can stay pinned to the draining predecessor process
/// while the successor already owns the socket path. Callers replaying a
/// request across the handoff must dial fresh so they reach whichever
/// process currently owns the socket. There is deliberately no connect
/// retry here: the caller's handoff loop owns the retry budget, and a dial
/// failure is proof the request was never transmitted.
pub async fn connect_fresh() -> Result<SandboxManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint(
        "sandbox-manager",
        Path::new(SOCKET_PATH),
        configured_endpoint()?,
    )
    .await?;
    Ok(client_from_channel(channel))
}

/// Build an sbx-manager client without opening the Unix socket immediately.
///
/// The lazy channel intentionally uses the same request timeout and HTTP/2
/// keepalive settings as [`connect`]. Individual RPCs surface socket failures.
pub fn lazy() -> SandboxManagerGrpcClient {
    static CLIENT: std::sync::OnceLock<SandboxManagerGrpcClient> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let channel = pplx_space_common_grpc::connect_uds_channel_lazy(
                Path::new(SOCKET_PATH),
                configured_endpoint().expect("static endpoint URI is valid"),
            );
            client_from_channel(channel)
        })
        .clone()
}

/// Sandboxes requested per `List` page: one inventory-client round trip.
const LIST_PAGE_SIZE: u32 = {
    let size = pplx_space_common_core::inventory::READ_BATCH_ITEMS;
    assert!(size <= u32::MAX as usize);
    size as u32
};

/// Read every sandbox through paged `List`. Each sandbox present for the whole
/// scan appears exactly once; see the RPC contract for concurrent changes. An
/// empty `next_page_token` ends the scan.
pub async fn list_all_sandboxes(
    client: &mut SandboxManagerGrpcClient,
) -> Result<Vec<proto::SandboxListElement>, tonic::Status> {
    let mut sandboxes = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = client
            .list(proto::ListRequest {
                page_size: LIST_PAGE_SIZE,
                page_token: page_token.clone(),
            })
            .await?
            .into_inner();
        sandboxes.extend(page.sandboxes);
        match next_list_page_token(&page_token, page.next_page_token)? {
            Some(next) => page_token = next,
            None => return Ok(sandboxes),
        }
    }
}

fn next_list_page_token(previous: &str, next: String) -> Result<Option<String>, tonic::Status> {
    if next.is_empty() {
        return Ok(None);
    }
    if next == previous {
        return Err(tonic::Status::internal(
            "sandbox-manager List page cursor did not advance",
        ));
    }
    Ok(Some(next))
}

#[cfg(test)]
mod tests;

/// Proof that the worker processed AccessIdleUpdate, rather than an ordinary touch.
/// Kept in response metadata so deployment checks do not expand the protobuf API.
pub const ACCESS_IDLE_UPDATE_ACK_METADATA: &str = "x-space-access-idle-update";
