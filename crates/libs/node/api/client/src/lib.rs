pub mod activation;
pub mod cluster;

pub mod proto {
    tonic::include_proto!("space.node.api.v1");

    pub const WORKER_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("worker_descriptor");
    pub const BUILDER_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("builder_descriptor");
    pub const CLUSTER_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("cluster_descriptor");
}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_bearer_token::{SpaceBearerToken, insert_space_bearer_token};
use pplx_space_common_grpc::{
    DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY, GrpcClient, GrpcRetryContext, GrpcRetryPolicy,
    should_reconnect_status,
};
use pplx_space_common_grpc::{
    GrpcClientTrace, trace_grpc_client_transport as wrap_grpc_client_channel,
};
use tonic::transport::{Channel, Endpoint};

pub use pplx_space_common_core::operation_budget::RuntimeOperationBudget as NodeApiOperationBudget;

type NodeApiConnectFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Channel,
                    pplx_space_common_grpc::internal_transport::TransportError,
                >,
            > + Send,
    >,
>;
type NodeApiConnectFn = Box<dyn FnMut() -> NodeApiConnectFuture + Send>;
type NodeCtlConnectFuture =
    Pin<Box<dyn Future<Output = Result<NodeCtlGrpcClient, tonic::transport::Error>> + Send>>;
type NodeCtlConnectFn = Box<dyn FnMut() -> NodeCtlConnectFuture + Send>;

/// Default TCP port for node-api-svc.
pub const DEFAULT_PORT: u16 = 9800;
pub const DEFAULT_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
pub const DEFAULT_TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_NODE_CTL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const NODE_API_DRAINING_MESSAGE: &str = "node-api draining";
/// HTTP/2 connection window for a node-api connection that carries many
/// callers' streams at once: 16 default-sized (2 MiB) stream windows, so
/// concurrent calls share a larger buffer. It bounds the response data one
/// connection can buffer; each stream's share stays within its stream window.
/// Callers opt in per connection; others keep the transport default.
pub const MULTIPLEXED_CONNECTION_WINDOW_BYTES: u32 = 32 * 1024 * 1024;

const LOCAL_ENDPOINT_URI: &str = "http://[::]:50051";

fn health_capability_unsupported(code: tonic::Code) -> bool {
    matches!(code, tonic::Code::NotFound | tonic::Code::Unimplemented)
}

pub type NodeCtlGrpcClient =
    proto::node_ctl_client::NodeCtlClient<GrpcClientTrace<tonic::transport::Channel>>;

/// Lazily connected node-ctl client over its node-local Unix socket.
///
/// Clones share one evictable cached generated client while running independent
/// RPCs concurrently. Transport-like gRPC failures clear the connection they
/// used without evicting a newer connection established by another caller.
#[derive(Clone)]
pub struct NodeCtlLazyClient {
    cache: Arc<tokio::sync::Mutex<NodeCtlClientCache>>,
}

#[derive(Clone)]
struct CachedNodeCtlClient {
    connection_id: Arc<()>,
    client: NodeCtlGrpcClient,
}

struct NodeCtlClientCache {
    client: Option<CachedNodeCtlClient>,
    connect: NodeCtlConnectFn,
}

impl NodeCtlClientCache {
    fn new(connect: NodeCtlConnectFn) -> Self {
        Self {
            client: None,
            connect,
        }
    }

    async fn get_or_connect(&mut self) -> Result<CachedNodeCtlClient, tonic::transport::Error> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }

        let client = CachedNodeCtlClient {
            connection_id: Arc::new(()),
            client: (self.connect)().await?,
        };
        self.client = Some(client.clone());
        Ok(client)
    }

    fn clear_if_same(&mut self, failed: &CachedNodeCtlClient) {
        if self
            .client
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(&cached.connection_id, &failed.connection_id))
        {
            self.client = None;
        }
    }
}

pub fn is_node_api_draining_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unavailable && status.message() == NODE_API_DRAINING_MESSAGE
}

macro_rules! node_api_unary_methods {
    ($(
        $method:ident, $client_new:path, $request:ty, $response:ty;
    )*) => {
        $(
            pub async fn $method(
                &mut self,
                request: $request,
                timeout: Duration,
            ) -> Result<tonic::Response<$response>, tonic::Status> {
                let client = self.client();
                GrpcClient::call(
                    &mut client.inner,
                    stringify!($method),
                    GrpcRetryContext::with_timeout(timeout),
                    move |channel| {
                        let request = request.clone();
                        async move {
                            let mut client = $client_new(wrap_grpc_client_channel(channel));
                            client
                                .$method(node_request_with_metadata(
                                    request,
                                    tonic::metadata::MetadataMap::new(),
                                    SpaceBearerToken::NodeApi,
                                )?)
                                .await
                        }
                    },
                )
                .await
            }
        )*
    };
}

macro_rules! node_api_internal_unary_methods {
    ($(
        $method:ident, $client_new:path, $request:ty, $response:ty;
    )*) => {
        $(
            pub async fn $method(
                &mut self,
                request: $request,
                timeout: Duration,
            ) -> Result<tonic::Response<$response>, tonic::Status> {
                let client = self.client();
                GrpcClient::call(
                    &mut client.inner,
                    stringify!($method),
                    GrpcRetryContext::with_timeout(timeout),
                    move |channel| {
                        let request = request.clone();
                        async move {
                            let mut client = $client_new(wrap_grpc_client_channel(channel));
                            client
                                .$method(node_request_with_metadata(
                                    request,
                                    tonic::metadata::MetadataMap::new(),
                                    SpaceBearerToken::NodeApiInternal,
                                )?)
                                .await
                        }
                    },
                )
                .await
            }
        )*
    };
}

trait RoleNodeApiClient {
    fn client(&mut self) -> &mut NodeApiClient;
}

/// Stateful node-api client that reconnects and retries replay-safe unary RPCs.
pub struct NodeApiClient {
    inner: GrpcClient<Channel, NodeApiConnectFn>,
}

pub struct WorkerApiClient<'a> {
    client: &'a mut NodeApiClient,
}

pub struct BuilderApiClient<'a> {
    client: &'a mut NodeApiClient,
}

pub struct WorkerApiInternalClient<'a> {
    client: &'a mut NodeApiClient,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeApiEndpointConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Option<Duration>,
    pub http2_keep_alive_interval: Duration,
    pub http2_keep_alive_timeout: Duration,
}

impl Default for NodeApiEndpointConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_TCP_CONNECT_TIMEOUT,
            request_timeout: None,
            http2_keep_alive_interval: DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL,
            http2_keep_alive_timeout: DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT,
        }
    }
}

impl NodeApiEndpointConfig {
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    pub fn with_http2_keep_alive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.http2_keep_alive_interval = interval;
        self.http2_keep_alive_timeout = timeout;
        self
    }
}

pub fn configure_node_api_endpoint(endpoint: Endpoint, config: NodeApiEndpointConfig) -> Endpoint {
    let endpoint = endpoint
        .tcp_nodelay(true)
        .max_frame_size(pplx_space_common_grpc::BULK_HTTP2_MAX_FRAME_SIZE)
        .connect_timeout(config.connect_timeout)
        .http2_keep_alive_interval(config.http2_keep_alive_interval)
        .keep_alive_timeout(config.http2_keep_alive_timeout);
    if let Some(request_timeout) = config.request_timeout {
        endpoint.timeout(request_timeout)
    } else {
        endpoint
    }
}

fn configure_node_ctl_endpoint(endpoint: Endpoint) -> Endpoint {
    endpoint
        .connect_timeout(DEFAULT_TCP_CONNECT_TIMEOUT)
        .timeout(DEFAULT_NODE_CTL_REQUEST_TIMEOUT)
        .http2_keep_alive_interval(DEFAULT_HTTP2_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(DEFAULT_HTTP2_KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
}

impl NodeApiClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self::with_service_name("node-api", endpoint)
    }

    pub fn with_service_name(service_name: &'static str, endpoint: Endpoint) -> Self {
        // Reconnects belong to this client and retain its original trust and
        // identity. Construct a new client to pick up rotated TLS configuration.
        let transport = Arc::new(tokio::sync::OnceCell::new());
        let connect: NodeApiConnectFn = Box::new(move || {
            let endpoint = endpoint.clone();
            let transport = transport.clone();
            Box::pin(async move {
                let transport = transport
                    .get_or_try_init(|| {
                        pplx_space_common_grpc::internal_transport::ClientTransport::from_env(
                            pplx_space_common_grpc::internal_transport::Service::NodeApi,
                            endpoint.uri(),
                        )
                    })
                    .await?;
                Ok(transport.connect_endpoint(endpoint)?.connect().await?)
            })
        });
        Self::from_connect_fn(service_name, connect)
    }

    fn from_connect_fn(service_name: &'static str, connect: NodeApiConnectFn) -> Self {
        Self {
            inner: GrpcClient::new(service_name, connect)
                .with_retry_policy(DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY)
                .with_connect_attempt_timeout(DEFAULT_CONNECT_ATTEMPT_TIMEOUT),
        }
    }

    pub fn with_retry_policy(self, retry_policy: GrpcRetryPolicy) -> Self {
        Self {
            inner: self.inner.with_retry_policy(retry_policy),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn channel(&self) -> Option<Channel> {
        self.inner.cached_client()
    }

    /// Check a capability advertised by node-api's authenticated health
    /// service. Unknown services return false; transport failures retain the
    /// retry policy and status of other replay-safe node-api calls.
    pub async fn health_service_serving(
        &mut self,
        service: &'static str,
        timeout: Duration,
    ) -> Result<bool, tonic::Status> {
        let result = GrpcClient::call(
            &mut self.inner,
            "health_check",
            GrpcRetryContext::with_timeout(timeout),
            move |channel| async move {
                let request = node_request_with_metadata(
                    tonic_health::pb::HealthCheckRequest {
                        service: service.to_string(),
                    },
                    tonic::metadata::MetadataMap::new(),
                    SpaceBearerToken::NodeApiInternal,
                )?;
                tonic_health::pb::health_client::HealthClient::new(wrap_grpc_client_channel(
                    channel,
                ))
                .check(request)
                .await
            },
        )
        .await;
        match result {
            Ok(response) => Ok(response.into_inner().status
                == tonic_health::pb::health_check_response::ServingStatus::Serving as i32),
            Err(status) if health_capability_unsupported(status.code()) => Ok(false),
            Err(status) => Err(status),
        }
    }

    node_api_unary_methods! {
        get_node_info, proto::node_info_client::NodeInfoClient::new, proto::GetNodeInfoRequest, proto::GetNodeInfoResponse;
    }
}

impl RoleNodeApiClient for NodeApiClient {
    fn client(&mut self) -> &mut NodeApiClient {
        self
    }
}

impl RoleNodeApiClient for WorkerApiClient<'_> {
    fn client(&mut self) -> &mut NodeApiClient {
        self.client
    }
}

impl<'a> WorkerApiClient<'a> {
    pub fn new(client: &'a mut NodeApiClient) -> Self {
        Self { client }
    }

    pub async fn spaced_open_loopback_bridge_with_metadata(
        &mut self,
        request: tonic::Request<pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest>,
        timeout: Duration,
    ) -> Result<
        tonic::Response<pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeResponse>,
        tonic::Status,
    > {
        let metadata = request.metadata().clone();
        let message = request.into_inner();
        let client = self.client();
        GrpcClient::call(
            &mut client.inner,
            "spaced_open_loopback_bridge",
            GrpcRetryContext::with_timeout(timeout),
            move |channel| {
                let metadata = metadata.clone();
                let message = message.clone();
                async move {
                    let mut client = proto::worker_api_client::WorkerApiClient::new(
                        wrap_grpc_client_channel(channel),
                    );
                    client
                        .spaced_open_loopback_bridge(node_request_with_metadata(
                            message,
                            metadata,
                            SpaceBearerToken::NodeApi,
                        )?)
                        .await
                }
            },
        )
        .await
    }

    pub async fn spaced_wait_ready_with_metadata(
        &mut self,
        request: tonic::Request<pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest>,
        timeout: Duration,
    ) -> Result<
        tonic::Response<pplx_space_sbx_mgr_client::proto::SpacedWaitReadyResponse>,
        tonic::Status,
    > {
        let metadata = request.metadata().clone();
        let message = request.into_inner();
        let client = self.client();
        GrpcClient::call(
            &mut client.inner,
            "spaced_wait_ready",
            GrpcRetryContext::with_timeout(timeout),
            move |channel| {
                let metadata = metadata.clone();
                let message = message.clone();
                async move {
                    let mut client = proto::worker_api_client::WorkerApiClient::new(
                        wrap_grpc_client_channel(channel),
                    );
                    client
                        .spaced_wait_ready(node_request_with_metadata(
                            message,
                            metadata,
                            SpaceBearerToken::NodeApi,
                        )?)
                        .await
                }
            },
        )
        .await
    }

    node_api_unary_methods! {
        create_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::CreateRequest, pplx_space_sbx_mgr_client::proto::CreateResponse;
        start_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::StartRequest, pplx_space_sbx_mgr_client::proto::StartResponse;
        stop_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::StopRequest, pplx_space_sbx_mgr_client::proto::StopResponse;
        kill_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::KillRequest, pplx_space_sbx_mgr_client::proto::KillResponse;
        delete_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::DeleteRequest, pplx_space_sbx_mgr_client::proto::DeleteResponse;
        sandbox_state, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::StateRequest, pplx_space_sbx_mgr_client::proto::StateResponse;
        get_sandbox_health, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::GetSandboxHealthRequest, pplx_space_sbx_mgr_client::proto::GetSandboxHealthResponse;
        list_sandboxes, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::ListRequest, pplx_space_sbx_mgr_client::proto::ListResponse;
        pause_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::PauseRequest, pplx_space_sbx_mgr_client::proto::PauseResponse;
        resume_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::ResumeRequest, pplx_space_sbx_mgr_client::proto::ResumeResponse;
        wake_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::WakeRequest, pplx_space_sbx_mgr_client::proto::WakeResponse;
        create_sandbox_snapshot, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::CreateSnapshotRequest, pplx_space_sbx_mgr_client::proto::CreateSnapshotResponse;
        suspend_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SuspendRequest, pplx_space_sbx_mgr_client::proto::SuspendResponse;
        prepare_teardown_suspend, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendRequest, pplx_space_sbx_mgr_client::proto::PrepareTeardownSuspendResponse;
        enqueue_sandbox_artifact_backup, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest, pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupResponse;
        sandbox_stats, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::StatsRequest, pplx_space_sbx_mgr_client::proto::StatsResponse;
        update_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::UpdateRequest, pplx_space_sbx_mgr_client::proto::UpdateResponse;
        set_sandbox_auto_lifecycle_policy, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyRequest, pplx_space_sbx_mgr_client::proto::SetAutoLifecyclePolicyResponse;
        touch_sandbox_auto_lifecycle, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::TouchAutoLifecycleRequest, pplx_space_sbx_mgr_client::proto::TouchAutoLifecycleResponse;
        set_sandbox_network_policy, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SetNetworkPolicyRequest, pplx_space_sbx_mgr_client::proto::SetNetworkPolicyResponse;
        spaced_ping_guest, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedPingGuestRequest, pplx_space_sbx_mgr_client::proto::SpacedPingGuestResponse;
        spaced_get_status, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedGetStatusRequest, pplx_space_sbx_mgr_client::proto::SpacedGetStatusResponse;
        spaced_wait_ready, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest, pplx_space_sbx_mgr_client::proto::SpacedWaitReadyResponse;
        spaced_stat, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedStatRequest, pplx_space_sbx_mgr_client::operations::StatResponse;
        spaced_write_batch, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedWriteBatchRequest, pplx_space_sbx_mgr_client::operations::WriteBatchResponse;
        spaced_mkdir, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedMkdirRequest, pplx_space_sbx_mgr_client::operations::MkdirResponse;
        spaced_remove, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedRemoveRequest, pplx_space_sbx_mgr_client::operations::RemoveResponse;
        spaced_rename, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedRenameRequest, pplx_space_sbx_mgr_client::operations::RenameResponse;
        spaced_normalize_watch_root, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedNormalizeWatchRootRequest, pplx_space_sbx_mgr_client::operations::NormalizeWatchRootResponse;
        spaced_create_watcher, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedCreateWatcherRequest, pplx_space_sbx_mgr_client::operations::CreateWatcherResponse;
        spaced_get_watcher_events, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedGetWatcherEventsRequest, pplx_space_sbx_mgr_client::operations::GetWatcherEventsResponse;
        spaced_remove_watcher, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedRemoveWatcherRequest, pplx_space_sbx_mgr_client::operations::RemoveWatcherResponse;
        spaced_enable_file_changes, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedEnableFileChangesRequest, pplx_space_sbx_mgr_client::operations::EnableFileChangesResponse;
        spaced_disable_file_changes, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedDisableFileChangesRequest, pplx_space_sbx_mgr_client::operations::DisableFileChangesResponse;
        spaced_get_modified_files, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedGetModifiedFilesRequest, pplx_space_sbx_mgr_client::operations::GetModifiedFilesResponse;
        spaced_list_processes, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedListProcessesRequest, pplx_space_sbx_mgr_client::proto::SpacedListProcessesResponse;
        spaced_send_signal, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedSendSignalRequest, pplx_space_sbx_mgr_client::operations::SendSignalResponse;
        spaced_send_input, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedSendInputRequest, pplx_space_sbx_mgr_client::operations::SendInputResponse;
        spaced_close_stdin, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedCloseStdinRequest, pplx_space_sbx_mgr_client::operations::CloseStdinResponse;
        spaced_resize_pty, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedResizePtyRequest, pplx_space_sbx_mgr_client::operations::ResizePtyResponse;
        spaced_kill_process, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedKillProcessRequest, pplx_space_sbx_mgr_client::operations::KillResponse;
        spaced_open_loopback_bridge, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedOpenLoopbackBridgeRequest, pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeResponse;
        spaced_close_loopback_bridge, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::SpacedCloseLoopbackBridgeRequest, pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeResponse;
        enable_snapshots, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::EnableSnapshotsRequest, pplx_space_sbx_mgr_client::proto::EnableSnapshotsResponse;
        disable_snapshots, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::DisableSnapshotsRequest, pplx_space_sbx_mgr_client::proto::DisableSnapshotsResponse;
        list_snapshots, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::ListSnapshotsRequest, pplx_space_sbx_mgr_client::proto::ListSnapshotsResponse;
        delete_snapshots, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::DeleteSnapshotsRequest, pplx_space_sbx_mgr_client::proto::DeleteSnapshotsResponse;
        rollback_sandbox, proto::worker_api_client::WorkerApiClient::new, pplx_space_sbx_mgr_client::proto::RollbackRequest, pplx_space_sbx_mgr_client::proto::RollbackResponse;
        delete_template, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::DeleteTemplateRequest, pplx_space_template_mgr_client::proto::DeleteTemplateResponse;
        delete_template_build, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::DeleteTemplateBuildRequest, pplx_space_template_mgr_client::proto::DeleteTemplateBuildResponse;
        list_templates, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::ListTemplatesRequest, pplx_space_template_mgr_client::proto::ListTemplatesResponse;
        get_template, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::GetTemplateRequest, pplx_space_template_mgr_client::proto::GetTemplateResponse;
        update_template_settings, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::UpdateTemplateSettingsRequest, pplx_space_template_mgr_client::proto::UpdateTemplateSettingsResponse;
        update_template_snapshot_policy, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyRequest, pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyResponse;
        create_pool, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::CreatePoolRequest, pplx_space_template_mgr_client::proto::CreatePoolResponse;
        delete_pool, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::DeletePoolRequest, pplx_space_template_mgr_client::proto::DeletePoolResponse;
        pool_status, proto::worker_api_client::WorkerApiClient::new, pplx_space_template_mgr_client::proto::PoolStatusRequest, pplx_space_template_mgr_client::proto::PoolStatusResponse;
        expose_port, proto::worker_api_client::WorkerApiClient::new, pplx_space_node_mgr_client::proto::ExposePortRequest, pplx_space_node_mgr_client::proto::ExposePortResponse;
        unexpose_port, proto::worker_api_client::WorkerApiClient::new, pplx_space_node_mgr_client::proto::UnexposePortRequest, pplx_space_node_mgr_client::proto::UnexposePortResponse;
        allow_port, proto::worker_api_client::WorkerApiClient::new, pplx_space_node_mgr_client::proto::AllowPortRequest, pplx_space_node_mgr_client::proto::AllowPortResponse;
        deny_port, proto::worker_api_client::WorkerApiClient::new, pplx_space_node_mgr_client::proto::DenyPortRequest, pplx_space_node_mgr_client::proto::DenyPortResponse;
        pull_template, proto::worker_api_client::WorkerApiClient::new, proto::PullTemplateRequest, proto::PullTemplateResponse;
        list_node_templates, proto::worker_api_client::WorkerApiClient::new, proto::ListNodeTemplatesRequest, proto::ListNodeTemplatesResponse;
        refresh_facet_state, proto::worker_api_client::WorkerApiClient::new, proto::RefreshFacetStateRequest, proto::RefreshFacetStateResponse;
        trigger_state_reconciliation, proto::worker_api_client::WorkerApiClient::new, proto::TriggerStateReconciliationRequest, proto::TriggerStateReconciliationResponse;
    }
}

impl RoleNodeApiClient for BuilderApiClient<'_> {
    fn client(&mut self) -> &mut NodeApiClient {
        self.client
    }
}

impl RoleNodeApiClient for WorkerApiInternalClient<'_> {
    fn client(&mut self) -> &mut NodeApiClient {
        self.client
    }
}

impl<'a> BuilderApiClient<'a> {
    pub fn new(client: &'a mut NodeApiClient) -> Self {
        Self { client }
    }

    node_api_unary_methods! {
        sandbox_state, proto::builder_api_client::BuilderApiClient::new, pplx_space_sbx_mgr_client::proto::StateRequest, pplx_space_sbx_mgr_client::proto::StateResponse;
        list_sandboxes, proto::builder_api_client::BuilderApiClient::new, pplx_space_sbx_mgr_client::proto::ListRequest, pplx_space_sbx_mgr_client::proto::ListResponse;
        stop_sandbox, proto::builder_api_client::BuilderApiClient::new, pplx_space_sbx_mgr_client::proto::StopRequest, pplx_space_sbx_mgr_client::proto::StopResponse;
        delete_sandbox, proto::builder_api_client::BuilderApiClient::new, pplx_space_sbx_mgr_client::proto::DeleteRequest, pplx_space_sbx_mgr_client::proto::DeleteResponse;
        delete_template, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::DeleteTemplateRequest, pplx_space_template_mgr_client::proto::DeleteTemplateResponse;
        delete_template_build, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::DeleteTemplateBuildRequest, pplx_space_template_mgr_client::proto::DeleteTemplateBuildResponse;
        list_templates, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::ListTemplatesRequest, pplx_space_template_mgr_client::proto::ListTemplatesResponse;
        get_template, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::GetTemplateRequest, pplx_space_template_mgr_client::proto::GetTemplateResponse;
        update_template_settings, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::UpdateTemplateSettingsRequest, pplx_space_template_mgr_client::proto::UpdateTemplateSettingsResponse;
        update_template_snapshot_policy, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyRequest, pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyResponse;
        create_pool, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::CreatePoolRequest, pplx_space_template_mgr_client::proto::CreatePoolResponse;
        delete_pool, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::DeletePoolRequest, pplx_space_template_mgr_client::proto::DeletePoolResponse;
        pool_status, proto::builder_api_client::BuilderApiClient::new, pplx_space_template_mgr_client::proto::PoolStatusRequest, pplx_space_template_mgr_client::proto::PoolStatusResponse;
        pull_template, proto::builder_api_client::BuilderApiClient::new, proto::PullTemplateRequest, proto::PullTemplateResponse;
        list_node_templates, proto::builder_api_client::BuilderApiClient::new, proto::ListNodeTemplatesRequest, proto::ListNodeTemplatesResponse;
        refresh_facet_state, proto::builder_api_client::BuilderApiClient::new, proto::RefreshFacetStateRequest, proto::RefreshFacetStateResponse;
        trigger_state_reconciliation, proto::builder_api_client::BuilderApiClient::new, proto::TriggerStateReconciliationRequest, proto::TriggerStateReconciliationResponse;
        build_template, proto::builder_api_client::BuilderApiClient::new, pplx_space_build_mgr_client::proto::StartBuildRequest, proto::BuildTemplateResponse;
        get_build_template_status, proto::builder_api_client::BuilderApiClient::new, pplx_space_build_mgr_client::proto::GetBuildStatusRequest, pplx_space_build_mgr_client::proto::GetBuildStatusResponse;
    }
}

impl<'a> WorkerApiInternalClient<'a> {
    pub fn new(client: &'a mut NodeApiClient) -> Self {
        Self { client }
    }

    node_api_internal_unary_methods! {
        wake_sandbox, proto::worker_api_internal_client::WorkerApiInternalClient::new, pplx_space_sbx_mgr_client::proto::WakeRequest, pplx_space_sbx_mgr_client::proto::WakeResponse;
    }
}

/// Attach the per-surface bearer to a node-api request for the retrying client.
///
/// Reads the projected ServiceAccount token for `kind` (falling back to the
/// static `SPACE_INTERNAL_TOKEN` / `SPACE_INTERNAL_CONTROL_TOKEN` env). A local
/// token-read failure fails loud with a retryable `UNAVAILABLE` rather than
/// sending a header-less request the node would reject as `UNAUTHENTICATED` —
/// mirroring `SpaceBearerInterceptor` on the channel-attached clients so a
/// credential blip is never misread as remote wedge evidence.
fn node_request_with_metadata<T>(
    message: T,
    metadata: tonic::metadata::MetadataMap,
    kind: SpaceBearerToken,
) -> Result<tonic::Request<T>, tonic::Status> {
    let mut request = tonic::Request::new(message);
    *request.metadata_mut() = metadata;
    insert_space_bearer_token(request.metadata_mut(), kind).map_err(|error| {
        tonic::Status::unavailable(format!(
            "failed to attach {} bearer token: {error}",
            kind.as_str()
        ))
    })?;
    Ok(request)
}

macro_rules! node_ctl_lazy_unary_methods {
    ($(
        $method:ident, $request:ty, $response:ty;
    )*) => {
        $(
            pub async fn $method(
                &mut self,
                request: impl tonic::IntoRequest<$request>,
            ) -> Result<tonic::Response<$response>, tonic::Status> {
                self.call_unary(
                    stringify!($method),
                    request,
                    |mut client, request| async move { client.$method(request).await },
                )
                .await
            }
        )*
    };
}

impl NodeCtlLazyClient {
    fn new(connect: NodeCtlConnectFn) -> Self {
        Self {
            cache: Arc::new(tokio::sync::Mutex::new(NodeCtlClientCache::new(connect))),
        }
    }

    async fn call_unary<Request, Response, Op, OpFut>(
        &self,
        command: &'static str,
        request: impl tonic::IntoRequest<Request>,
        mut operation: Op,
    ) -> Result<tonic::Response<Response>, tonic::Status>
    where
        Request: Clone,
        Op: FnMut(NodeCtlGrpcClient, tonic::Request<Request>) -> OpFut,
        OpFut: Future<Output = Result<tonic::Response<Response>, tonic::Status>>,
    {
        let request = request.into_request();
        let metadata = request.metadata().clone();
        let message = request.into_inner();
        let cache = self.cache.clone();
        let connect_cache = cache.clone();
        let last_client = Arc::new(tokio::sync::Mutex::new(None));
        let operation_last_client = last_client.clone();
        let mut client = GrpcClient::new("node-ctl", move || {
            let cache = connect_cache.clone();
            async move { cache.lock().await.get_or_connect().await }
        })
        .with_connect_attempt_timeout(DEFAULT_CONNECT_ATTEMPT_TIMEOUT);
        let result = client
            .call(
                command,
                GrpcRetryContext::with_timeout(DEFAULT_NODE_CTL_REQUEST_TIMEOUT),
                move |client| {
                    let cache = cache.clone();
                    let last_client = operation_last_client.clone();
                    let mut request = tonic::Request::new(message.clone());
                    *request.metadata_mut() = metadata.clone();
                    let response = operation(client.client.clone(), request);
                    async move {
                        *last_client.lock().await = Some(client.clone());
                        let result = response.await;
                        if result.as_ref().is_err_and(should_reconnect_status) {
                            cache.lock().await.clear_if_same(&client);
                        }
                        result
                    }
                },
            )
            .await;

        if result.as_ref().is_err_and(should_reconnect_status)
            && let Some(client) = last_client.lock().await.as_ref()
        {
            self.cache.lock().await.clear_if_same(client);
        }

        result
    }

    node_ctl_lazy_unary_methods! {
        get_reconciliation_status, proto::GetReconciliationStatusRequest, proto::GetReconciliationStatusResponse;
        get_controller_state, proto::GetControllerStateRequest, proto::GetControllerStateResponse;
        refresh_facet_state, proto::RefreshFacetStateRequest, proto::RefreshFacetStateResponse;
        trigger_state_reconciliation, proto::TriggerStateReconciliationRequest, proto::TriggerStateReconciliationResponse;
        mark_facet_dirty, proto::MarkFacetDirtyRequest, proto::MarkFacetDirtyResponse;
        set_facet_sync_disabled, proto::SetFacetSyncDisabledRequest, proto::SetFacetSyncDisabledResponse;
    }
}

/// Lazily connected node-ctl client over its node-local Unix socket.
pub fn node_ctl_lazy() -> NodeCtlLazyClient {
    static CLIENT: std::sync::OnceLock<NodeCtlLazyClient> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            NodeCtlLazyClient::new(Box::new(|| {
                Box::pin(async {
                    connect_node_ctl_channel().await.map(|channel| {
                        proto::node_ctl_client::NodeCtlClient::new(wrap_grpc_client_channel(
                            channel,
                        ))
                    })
                })
            }))
        })
        .clone()
}

async fn connect_node_ctl_channel() -> Result<Channel, tonic::transport::Error> {
    let endpoint = configure_node_ctl_endpoint(
        Endpoint::try_from(LOCAL_ENDPOINT_URI).expect("static endpoint URI is valid"),
    );
    endpoint
        .connect_with_connector(tower::service_fn(|_| async {
            let stream = tokio::net::UnixStream::connect(
                pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET,
            )
            .await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
        }))
        .await
}

/// Connect to the node-ctl gRPC service over its node-local Unix socket.
pub async fn connect_node_ctl() -> Result<NodeCtlGrpcClient, pplx_space_common_grpc::UdsConnectError>
{
    let endpoint = configure_node_ctl_endpoint(
        Endpoint::try_from(LOCAL_ENDPOINT_URI).expect("static endpoint URI is valid"),
    );
    let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint_with_retry(
        "node-ctl",
        std::path::Path::new(pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET),
        endpoint,
        pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
    )
    .await?;
    Ok(proto::node_ctl_client::NodeCtlClient::new(
        wrap_grpc_client_channel(channel),
    ))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod node_request_tests;

#[cfg(test)]
mod pending_local_backup_wire_tests;
