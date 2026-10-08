pub mod proto {
    tonic::include_proto!("space.node.v1");
}

pub mod execution_fence;

/// One sequential inventory walk. An expired/consumed cursor is an error, never EOF.
#[derive(Debug)]
pub struct SnapshotPages {
    request: Option<proto::ListBtrfsSnapshotsRequest>,
}

impl SnapshotPages {
    pub fn new(sandbox: pplx_space_common_core::types::SandboxId, measure: bool) -> Self {
        Self {
            request: Some(proto::ListBtrfsSnapshotsRequest {
                sandbox_id: sandbox.to_string(),
                skip_size_measurement: !measure,
                page_token: String::new(),
            }),
        }
    }

    pub async fn next(
        &mut self,
        client: &mut NodeManagerGrpcClient,
    ) -> Result<Option<Vec<proto::BtrfsSnapshotInfo>>, tonic::Status> {
        self.next_with(|request| {
            let mut client = client.clone();
            async move { client.list_btrfs_snapshots(request).await }
        })
        .await
    }

    async fn next_with<F, Fut>(
        &mut self,
        mut fetch: F,
    ) -> Result<Option<Vec<proto::BtrfsSnapshotInfo>>, tonic::Status>
    where
        F: FnMut(proto::ListBtrfsSnapshotsRequest) -> Fut,
        Fut: std::future::Future<
                Output = Result<tonic::Response<proto::ListBtrfsSnapshotsResponse>, tonic::Status>,
            >,
    {
        let Some(request) = self.request.as_mut() else {
            return Ok(None);
        };
        // ResourceExhausted rejects admission before consuming the cursor.
        // Other failures may follow a consumed page: preserve them for the
        // scan owner rather than replaying an uncertain single-use token.
        let response = pplx_space_common_grpc::retry_grpc_status_with_context_if(
            "node-manager",
            "ListBtrfsSnapshots",
            pplx_space_common_grpc::DEFAULT_LOCAL_GRPC_RETRY_POLICY,
            pplx_space_common_grpc::GrpcRetryContext::none(),
            |status| status.code() == tonic::Code::ResourceExhausted,
            || {
                let response = fetch(request.clone());
                async move {
                    match response.await {
                        Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                            Err(status)
                        }
                        response => Ok(response),
                    }
                }
            },
        )
        .await??
        .into_inner();
        if !response.next_page_token.is_empty() {
            request.page_token = response.next_page_token;
        } else {
            self.request = None;
        }
        Ok(Some(response.snapshots))
    }
}

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

pub const NODE_STATUS_FAILURE_GRACE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskSchedulingEffect {
    Open,
    WarnOnly,
    Full,
    Unavailable,
}

impl proto::CacheDiskStatus {
    pub fn admission_state_enum(&self) -> proto::CacheDiskAdmissionState {
        proto::CacheDiskAdmissionState::try_from(self.admission_state).unwrap_or_default()
    }

    pub fn scheduling_effect(&self) -> CacheDiskSchedulingEffect {
        match self.admission_state_enum() {
            proto::CacheDiskAdmissionState::Ready => CacheDiskSchedulingEffect::Open,
            proto::CacheDiskAdmissionState::Warning => CacheDiskSchedulingEffect::WarnOnly,
            proto::CacheDiskAdmissionState::Full => CacheDiskSchedulingEffect::Full,
            proto::CacheDiskAdmissionState::Unavailable
            | proto::CacheDiskAdmissionState::Unspecified => CacheDiskSchedulingEffect::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeResourceStatusReadError {
    #[error("node-manager resource status failed: {0}")]
    Rpc(String),
    #[error("node-manager response omitted cache disk status")]
    MissingCacheDisk,
}

pub type NodeResourceStatusRead =
    StatusRead<proto::NodeResourceStatusResponse, NodeResourceStatusReadError>;

#[derive(Debug, Clone)]
pub struct NodeResourceStatusReader {
    reader: GracedStatusReader<proto::NodeResourceStatusResponse>,
}

#[derive(Debug)]
struct GracedStatusReaderState<T> {
    last_success: Option<CachedStatus<T>>,
    failure_grace: Duration,
}

#[derive(Debug, Clone)]
struct CachedStatus<T> {
    response: T,
    observed_at: Instant,
}

#[derive(Debug, Clone)]
pub struct StatusRead<T, E> {
    pub response: T,
    fallback_error: Option<E>,
}

#[derive(Debug, Clone)]
struct GracedStatusReader<T> {
    state: Arc<Mutex<GracedStatusReaderState<T>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkPoolStatusReadError {
    #[error("node-manager network pool status failed: {0}")]
    Rpc(String),
}

pub type NetworkPoolStatusRead =
    StatusRead<proto::NetworkPoolStatusResponse, NetworkPoolStatusReadError>;

#[derive(Debug, Clone)]
pub struct NetworkPoolStatusReader {
    reader: GracedStatusReader<proto::NetworkPoolStatusResponse>,
}

impl<T, E> StatusRead<T, E> {
    pub fn fallback_error(&self) -> Option<&E> {
        self.fallback_error.as_ref()
    }
}

impl<T: Clone> GracedStatusReader<T> {
    fn new(failure_grace: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(GracedStatusReaderState {
                last_success: None,
                failure_grace,
            })),
        }
    }

    fn observe_success<E>(&self, response: T, observed_at: Instant) -> StatusRead<T, E> {
        let mut state = self.lock_state();
        state.last_success = Some(CachedStatus {
            response: response.clone(),
            observed_at,
        });
        StatusRead {
            response,
            fallback_error: None,
        }
    }

    fn status_for_failure<E>(&self, now: Instant, error: E) -> Option<StatusRead<T, E>> {
        let state = self.lock_state();
        let cached = state.last_success.as_ref()?;
        let age = now.saturating_duration_since(cached.observed_at);
        (age <= state.failure_grace).then(|| StatusRead {
            response: cached.response.clone(),
            fallback_error: Some(error),
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, GracedStatusReaderState<T>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl NodeResourceStatusReader {
    pub fn new() -> Self {
        Self {
            reader: GracedStatusReader::new(NODE_STATUS_FAILURE_GRACE),
        }
    }

    pub async fn read(
        &self,
        client: NodeManagerGrpcClient,
        request: Request<proto::NodeResourceStatusRequest>,
    ) -> Result<NodeResourceStatusRead, NodeResourceStatusReadError> {
        let now = Instant::now();
        let read = pplx_space_common_grpc::fwd_retry!(
            "node-manager",
            client,
            get_node_resource_status,
            request
        )
        .map_err(|error| NodeResourceStatusReadError::Rpc(error.to_string()))
        .map(|response| response.into_inner())
        .and_then(validate_resource_status_response);
        match read {
            Ok(response) => Ok(self.observe_success(response, now)),
            Err(error) => self.status_for_failure(now, error.clone()).ok_or(error),
        }
    }

    fn observe_success(
        &self,
        response: proto::NodeResourceStatusResponse,
        observed_at: Instant,
    ) -> NodeResourceStatusRead {
        self.reader.observe_success(response, observed_at)
    }

    fn status_for_failure(
        &self,
        now: Instant,
        error: NodeResourceStatusReadError,
    ) -> Option<NodeResourceStatusRead> {
        self.reader.status_for_failure(now, error)
    }

    #[cfg(test)]
    fn with_failure_grace(failure_grace: Duration) -> Self {
        Self {
            reader: GracedStatusReader::new(failure_grace),
        }
    }
}

impl Default for NodeResourceStatusReader {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkPoolStatusReader {
    pub fn new() -> Self {
        Self {
            reader: GracedStatusReader::new(NODE_STATUS_FAILURE_GRACE),
        }
    }

    pub async fn read(
        &self,
        client: NodeManagerGrpcClient,
        request: Request<proto::NetworkPoolStatusRequest>,
    ) -> Result<NetworkPoolStatusRead, NetworkPoolStatusReadError> {
        let now = Instant::now();
        let read = pplx_space_common_grpc::fwd_retry!(
            "node-manager",
            client,
            get_network_pool_status,
            request
        )
        .map_err(|error| NetworkPoolStatusReadError::Rpc(error.to_string()))
        .map(|response| response.into_inner());
        match read {
            Ok(response) => Ok(self.observe_success(response, now)),
            Err(error) => self.status_for_failure(now, error.clone()).ok_or(error),
        }
    }

    fn observe_success(
        &self,
        response: proto::NetworkPoolStatusResponse,
        observed_at: Instant,
    ) -> NetworkPoolStatusRead {
        self.reader.observe_success(response, observed_at)
    }

    fn status_for_failure(
        &self,
        now: Instant,
        error: NetworkPoolStatusReadError,
    ) -> Option<NetworkPoolStatusRead> {
        self.reader.status_for_failure(now, error)
    }

    #[cfg(test)]
    fn with_failure_grace(failure_grace: Duration) -> Self {
        Self {
            reader: GracedStatusReader::new(failure_grace),
        }
    }
}

impl Default for NetworkPoolStatusReader {
    fn default() -> Self {
        Self::new()
    }
}

pub fn cache_disk_status(
    response: &proto::NodeResourceStatusResponse,
) -> Result<&proto::CacheDiskStatus, NodeResourceStatusReadError> {
    response
        .cache_disk
        .as_ref()
        .ok_or(NodeResourceStatusReadError::MissingCacheDisk)
}

fn validate_resource_status_response(
    response: proto::NodeResourceStatusResponse,
) -> Result<proto::NodeResourceStatusResponse, NodeResourceStatusReadError> {
    cache_disk_status(&response)?;
    Ok(response)
}

/// Unix socket path for the node-manager gRPC service.
pub const SOCKET_PATH: &str = pplx_space_common_core::host::linux::paths::NODE_MANAGER_SOCKET;
const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

pub type NodeManagerGrpcClient =
    proto::node_manager_client::NodeManagerClient<GrpcClientTrace<Channel>>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("invalid gRPC endpoint URI: {0}")]
    InvalidUri(#[from] tonic::codegen::http::uri::InvalidUri),
    #[error("node-manager transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("node-manager connect failed: {0}")]
    Connect(#[from] pplx_space_common_grpc::UdsConnectError),
}

fn endpoint() -> Result<Endpoint, tonic::transport::Error> {
    Endpoint::try_from(LOCAL_ENDPOINT_URI)
}

/// Connect to the node-manager gRPC service over Unix socket.
pub async fn connect() -> Result<NodeManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint_with_retry(
        "node-manager",
        std::path::Path::new(SOCKET_PATH),
        endpoint()?,
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await?;
    Ok(proto::node_manager_client::NodeManagerClient::new(
        wrap_grpc_client_channel(channel),
    ))
}

/// Dial the node-manager socket once, for callers that own their retry loop
/// and must classify each failure themselves.
pub async fn connect_once() -> Result<NodeManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint(
        "node-manager",
        std::path::Path::new(SOCKET_PATH),
        endpoint()?,
    )
    .await?;
    Ok(proto::node_manager_client::NodeManagerClient::new(
        wrap_grpc_client_channel(channel),
    ))
}

/// Build a node-manager client without opening the Unix socket immediately.
pub fn lazy() -> NodeManagerGrpcClient {
    static CLIENT: std::sync::OnceLock<NodeManagerGrpcClient> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let channel = pplx_space_common_grpc::connect_uds_channel_lazy(
                std::path::Path::new(SOCKET_PATH),
                endpoint().expect("static endpoint URI is valid"),
            );
            proto::node_manager_client::NodeManagerClient::new(wrap_grpc_client_channel(channel))
        })
        .clone()
}

#[cfg(test)]
mod tests;
