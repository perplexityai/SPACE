pub mod environment;
use std::path::Path;

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};

// The wire contract lives in `pplx-space-sbx-mgr-protocol`; these re-exports
// keep its items at their original paths in this crate.
pub use pplx_space_sbx_mgr_protocol::{
    ACCESS_IDLE_UPDATE_ACK_METADATA, ACTIVATION_POLICY_HEALTH_SERVICE,
    AUTHORITATIVE_ROUTE_IDENTITY_REQUIRED_METADATA, CLEANUP_REFUSAL_REASON_METADATA,
    OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE, PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE,
    RESTART_RETRY_ON_SUCCESSOR_METADATA, RETAINED_FILESYSTEM_ONLY_COPY_REASON,
    SUSPEND_PUBLICATION_PENDING_REASON, TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON,
    authoritative_route_identity_required_status, auto_lifecycle, direct_restore_start_request,
    encode_optional_sandbox_id, is_authoritative_route_identity_required,
    is_restart_retry_on_successor, is_retained_filesystem_only_copy_status,
    is_suspend_publication_pending_status, is_template_policy_dependency, lifecycle, operations,
    proto, restart_retry_on_successor_status, retained_filesystem_only_copy_status,
    suspend_publication_pending_status, template_policy_dependency_status,
};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Unix socket path for the sbx-manager gRPC service.
pub const SOCKET_PATH: &str = pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_SOCKET;

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
