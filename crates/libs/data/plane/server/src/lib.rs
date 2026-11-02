use std::borrow::Cow;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    routing::{MethodFilter, any, get, on, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use pplx_space_common_core::types::cluster::process_cluster_id;
use pplx_space_common_core::types::{NodeId, OrgId, ProcessId, SandboxId, SimpleDuration};
use pplx_space_common_svc::bearer_token::{SpaceBearerToken, insert_space_bearer_token};
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use pplx_space_common_svc::{HttpServerMakeSpan, HttpSpanMetadata};
use pplx_space_feature_flags::{
    Attributes, FeatureFlagClient, SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG,
};
use pplx_space_metrics::data_plane::{
    BackendRpcResult, BackendRpcSurface, ResponseKind, ResponseOrigin, ResponseSurface,
    RouteRepairResult, RouteRepairSurface, ServicePortIngressOpenReason,
    ServicePortIngressOpenResult, ServiceRouteSurface, ServiceRouteTransport,
    SpaceApiRouteTokenDropReason, SpaceApiRouteTokenRouteSource, TooManyRequestsReason,
};
use pplx_space_net_gateway_client::proto::{
    ServicePortIngressFrame, ServicePortIngressStart, service_port_ingress_frame,
};
use pplx_space_node_api_client::cluster::{
    AuthenticatedClusterApiClient, ConnectClusterApiError, EndpointDiscovery,
};
use pplx_space_node_api_client::proto::{
    BindDataPlaneSessionProcessRequest, DataPlaneReconnectReason, DataPlaneRevocationState,
    DataPlaneRevocationStatus, GetDataPlaneOrgRevocationEpochRequest,
    GetDataPlaneSessionRevocationStateRequest, GetSandboxRuntimeAccessRequest,
    RecordDataPlaneSessionReconnectRequest, ResolveDataPlaneSessionRouteRequest,
    ResolveDataPlaneSessionRouteResponse, SandboxRuntimeAccess, SandboxRuntimeAccessDecision,
    SandboxServiceRouteKey, SandboxServiceRouteKind, SandboxServiceRouteTransport,
    UnstableResetSandboxServiceRouteRequest, worker_api_client::WorkerApiClient,
};
use pplx_space_public_api::{
    CreateFileWatchSessionRequest, CreateProcessRequest, CreateTerminalSessionRequest,
    DataPlaneClientFrame, DataPlaneErrorCode, DataPlaneHttpError, DataPlaneHttpErrorCode,
    DataPlaneHttpErrorEnvelope, DataPlaneProcessExitStatus, DataPlaneResyncReason,
    DataPlaneServerFrame, DataPlaneSessionId, FileEventKind, ReconnectReason, SessionKind,
    SessionTargetProtocol, StreamCloseReason, TerminalSize, process_id_for_session,
    routes::{self as native_routes, NativeHttpMethod},
};
use pplx_space_sbx_mgr_client::operations::{
    CloseLoopbackBridgeRequest, CloseStdinRequest, ConnectProcessRequest, PtyConfig, PtySize,
    ResizePtyRequest, SendInputRequest, SendSignalRequest, StartRequest,
    WatchRequest as WatchOperation,
};
use pplx_space_sbx_mgr_client::proto::{
    SpacedCloseLoopbackBridgeRequest, SpacedCloseStdinRequest, SpacedConnectProcessRequest,
    SpacedFileEvent as FileEvent, SpacedFileEventKind, SpacedProcessEvent as ProcessEvent,
    SpacedProcessExit as ProcessExit, SpacedResizePtyRequest, SpacedSendInputRequest,
    SpacedSendSignalRequest, SpacedStartProcessRequest, SpacedWatchRequest, spaced_process_event,
    spaced_process_exit,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::transport::{Channel, Endpoint};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

mod drain;
mod ecw_connect;
mod ecw_files;
mod ecw_process;
mod http_traffic;
mod ingress_lifetime;
mod native;
use ingress_lifetime::{Completion, IngressIo, Progress};
mod runtime_route;
mod state;
mod upload_admission;
use upload_admission::UploadAdmission;
pub use upload_admission::{
    ByteRateConfig, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS_PER_ORG,
    DEFAULT_MAX_CONCURRENT_UPLOADS_PER_SANDBOX, DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES,
    DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG, DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX,
    DEFAULT_ORG_TRANSFER_RATE, DEFAULT_UPLOAD_ADMISSION_WAIT, DEFAULT_UPLOAD_MIN_BODY_RATE,
    LimitOrOff, MinBodyRateConfig, UploadAdmissionArgs, UploadAdmissionConfig, UploadShareConfig,
};

use drain::{
    BridgeEnd, DRAINING_CLOSE_REASON, DrainLifecycle, SHUTDOWN_CLOSE_REASON,
    before_stream_deadline, close_draining_http1_connections, mark_draining_response,
    native_draining_error, native_draining_response, readyz, send_restart_close, serve_and_drain,
};
#[cfg(test)]
use native::{
    CdpProxyBase, HeaderTokenSource, MAX_LAST_EVENT_ID, StreamBridgeError, UpgradedProxyClose,
    attached_json, attached_session_from_route_response, file_event_frame, file_event_kind,
    file_watch_request, last_event_id, native_requested_transport, process_event_frames,
    process_frame_advances_backend_cursor, process_start_request, proto_reconnect_reason,
    proxy_path_and_query, proxy_streams, reconnect_reason_for_route_change,
    refresh_file_watch_attached_if_route_token_stale_with, reject_query_tokens,
    resume_resync_frame, rewrite_cdp_discovery_value, server_frame_event_name, server_frame_json,
    stream_bridge_error_label, strip_data_plane_auth_headers, token_from_headers,
};
use native::{
    RouteResolutionError, attach_session, data_plane_http_error, is_upgrade_request,
    node_api_request_with_route_token_state, node_api_request_with_timeout_and_route_token_state,
    observe_data_plane_http_error, proxy_session, proxy_upgraded_connection,
    revocation_state_is_active, session_events, token_sha256,
};
use runtime_route::{
    EcwRouteTransport, acquire_ecw_stream_permit, ecw_backend_channel,
    refuse_new_ecw_stream_while_draining, resolve_ecw_envd_route, resolve_ecw_route,
    validate_ecw_sandbox_headers,
};
use state::*;

pub const DEFAULT_LISTEN: &str = "0.0.0.0:8090";
pub const DEFAULT_ATTACH_DEADLINE_MS: u64 = 5_000;
pub const DEFAULT_UNAUTHENTICATED_UPGRADE_CAP_PER_IP: usize = 64;
pub const DEFAULT_MAX_ATTACHED_STREAMS: usize = 20_000;
pub const DEFAULT_MAX_ATTACHED_STREAMS_PER_ORG: usize = 5_000;
pub const DEFAULT_MAX_ATTACHED_STREAMS_PER_SANDBOX: usize = 512;
pub const DEFAULT_BACKEND_CHANNEL_POOL_SIZE: usize = 4;
pub const DEFAULT_REVOCATION_CHECK_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_ROUTE_CACHE_TTL_MS: u64 = 1_000;
/// How long after its last confirmed revocation check an attached stream may
/// keep running while later checks fail, before it fails closed.
const REVOCATION_MAX_STALENESS: Duration = Duration::from_secs(60);
/// How long one revocation check may wait on the cluster API, across all of
/// its reads. A check that takes longer counts as unavailable.
const REVOCATION_CHECK_TIMEOUT: Duration = Duration::from_secs(3);
/// The longest revocation check interval the service accepts: a third of
/// what is left of the staleness bound after one check timeout, 19 s at the
/// defaults. A failed check keeps a verdict until it is that bound less one
/// interval and one timeout old, so at this interval or below that is at
/// least two intervals: a stream rides out at least the first failed check
/// after a confirmed one, and a refresh another stream has in flight can
/// still answer for it.
pub const MAX_REVOCATION_CHECK_INTERVAL_MS: u64 =
    ((REVOCATION_MAX_STALENESS.as_millis() - REVOCATION_CHECK_TIMEOUT.as_millis()) / 3) as u64;
/// The shortest drain deadline the service accepts: the 8-second stream-open
/// budget, so a `Process.Start` already opening when SIGTERM arrives finishes
/// opening, and can report whether the command started, before the deadline.
/// A second stop signal forces the exit without that wait.
pub const MIN_SHUTDOWN_GRACE: Duration =
    pplx_space_node_api_client::NodeApiOperationBudget::StreamOpen.timeout();
const ECW_PROXY_FAILURE_BRIDGE_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const ECW_PROXY_FAILURE_ROUTE_REPAIR_COOLDOWN: Duration = Duration::from_secs(30);
const ECW_SERVICE_ROUTE_NOT_READY_RETRY_AFTER: Duration = Duration::from_secs(1);
const ECW_SERVICE_ROUTE_RESOLVE_TIMEOUT: Duration = Duration::from_secs(15);
const ECW_RUNTIME_ACCESS_CLASSIFY_TIMEOUT: Duration = Duration::from_secs(2);
const CLUSTER_API_STARTUP_RETRY_BASE: Duration = Duration::from_millis(250);
const CLUSTER_API_STARTUP_RETRY_MAX: Duration = Duration::from_secs(5);
const DATA_PLANE_TOKEN_PREFIX: &str = "dpt_";
const DEFAULT_TERMINAL_COMMAND: &[&str] = &["/bin/sh"];
const SPACE_ECW_HOSTS_ENV: &str = "SPACE_ECW_HOSTS";
const ECW_ENVD_PORT: u32 = 49_983;
const ECW_TRAFFIC_ACCESS_TOKEN_HEADER: &str = "e2b-traffic-access-token";
const ECW_ENVD_ACCESS_TOKEN_HEADER: &str = "x-access-token";
const ECW_API_KEY_HEADER: &str = "x-api-key";
const ECW_SANDBOX_ID_HEADER: &str = "e2b-sandbox-id";
const ECW_SANDBOX_PORT_HEADER: &str = "e2b-sandbox-port";
const ECW_PROCESS_STREAM_KIND: &str = "ecw_process";
const ECW_PROCESS_INPUT_STREAM_KIND: &str = "ecw_process_input";
const ECW_FILE_WATCH_STREAM_KIND: &str = "ecw_file_watch";
const ECW_SERVICE_STREAM_KIND: &str = "ecw_service";
const FEDERATED_ORG_ID_HEADER: &str = "x-space-federated-org-id";
const FEDERATED_USER_ID_HEADER: &str = "x-space-federated-user-id";
const ECW_SERVICE_PROXY_STRIPPED_SPACE_HEADERS: &[&str] = &[
    ECW_TRAFFIC_ACCESS_TOKEN_HEADER,
    ECW_ENVD_ACCESS_TOKEN_HEADER,
    ECW_API_KEY_HEADER,
    FEDERATED_ORG_ID_HEADER,
    FEDERATED_USER_ID_HEADER,
];
const ECW_MISSING_RETRY_AFTER_FLOOR: Duration = Duration::from_secs(1);

fn record_ecw_backend_rpc_latency(
    operation: &'static str,
    result: BackendRpcResult,
    started_at: std::time::Instant,
) {
    pplx_space_metrics::data_plane::backend_rpc_latency(
        BackendRpcSurface::Ecw,
        operation,
        result,
        started_at,
    );
    if result == BackendRpcResult::Timeout {
        pplx_space_metrics::data_plane::backend_rpc_timeout(BackendRpcSurface::Ecw, operation);
    }
}

#[derive(Debug, Clone)]
pub struct DataPlaneConfig {
    pub listen: String,
    pub attach_deadline: Duration,
    pub unauthenticated_upgrade_cap_per_ip: usize,
    pub max_attached_streams: usize,
    pub max_attached_streams_per_org: usize,
    pub max_attached_streams_per_sandbox: usize,
    pub backend_channel_pool_size: usize,
    /// At most [`MAX_REVOCATION_CHECK_INTERVAL_MS`]; [`run`] refuses a longer
    /// one.
    pub revocation_check_interval: Duration,
    pub route_cache_ttl: Duration,
    /// `None` leaves `POST /files` unbounded.
    pub upload_admission: Option<UploadAdmissionConfig>,
    /// How long live streams may keep running after SIGTERM before they are
    /// cancelled, at least [`MIN_SHUTDOWN_GRACE`]. The pod's termination grace
    /// period, minus its preStop delay, has to cover it plus the 5-second
    /// cancel grace, so the deadline rather than SIGKILL ends a stream that
    /// outlives the drain. `None` sets no deadline: streams run until they end
    /// or the pod's termination grace period kills the process.
    pub shutdown_grace: Option<Duration>,
}

impl Default for DataPlaneConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN.to_string(),
            attach_deadline: Duration::from_millis(DEFAULT_ATTACH_DEADLINE_MS),
            unauthenticated_upgrade_cap_per_ip: DEFAULT_UNAUTHENTICATED_UPGRADE_CAP_PER_IP,
            max_attached_streams: DEFAULT_MAX_ATTACHED_STREAMS,
            max_attached_streams_per_org: DEFAULT_MAX_ATTACHED_STREAMS_PER_ORG,
            max_attached_streams_per_sandbox: DEFAULT_MAX_ATTACHED_STREAMS_PER_SANDBOX,
            backend_channel_pool_size: DEFAULT_BACKEND_CHANNEL_POOL_SIZE,
            revocation_check_interval: Duration::from_millis(DEFAULT_REVOCATION_CHECK_INTERVAL_MS),
            route_cache_ttl: Duration::from_millis(DEFAULT_ROUTE_CACHE_TTL_MS),
            upload_admission: None,
            shutdown_grace: None,
        }
    }
}

#[derive(Clone)]
struct DataPlaneState {
    cluster_api: ClusterApiClients,
    attach_deadline: Duration,
    unauthenticated: Arc<UnauthenticatedUpgradeLimiter>,
    attached_streams: Arc<AttachedStreamLimiter>,
    uploads: Option<Arc<UploadAdmission>>,
    upload_tracker: Arc<http_traffic::UploadTracker>,
    native_sessions: Arc<NativeSessionAttachLimiter>,
    backend_channels: Arc<BackendChannelPool>,
    revocation_cache: Arc<RevocationCache>,
    route_cache: Arc<RouteCache>,
    ecw_route_repairs: Arc<DashMap<String, tokio::time::Instant>>,
    ecw_process_input_locks: Arc<EcwProcessInputLocks>,
    ecw_hosts: Arc<EcwHostConfig>,
    revocation_check_interval: Duration,
    revocation_max_staleness: Duration,
    revocation_check_timeout: Duration,
    route_cache_ttl: Duration,
    /// Cancelled on SIGTERM: a child of the caller's shutdown token. New
    /// streams are refused once it is cancelled, and sockets still
    /// authenticating are told to reconnect; streams already attached keep
    /// running.
    drain_started: CancellationToken,
    /// Cancelled only when the drain deadline passes or the server exits.
    /// Every stream takes a child of this token, never of the SIGTERM token,
    /// so a shutdown signal alone does not end live streams. It is a second
    /// root token on purpose, as `drain_before_shutdown`'s service token is in
    /// the API (`crates/libs/api/src/run.rs`): a child of the SIGTERM token
    /// would be cancelled by the very signal the drain outlives.
    stream_shutdown_token: CancellationToken,
    upgrade_tasks: TaskTracker,
}

#[derive(Clone)]
struct ClusterApiClients<T = AuthenticatedClusterApiClient> {
    service_client: T,
    endpoint_slice_client: Option<T>,
    feature_flags: FeatureFlagClient,
    feature_flag_attributes: Attributes,
}

impl<T: Clone> ClusterApiClients<T> {
    fn new(
        service_client: T,
        endpoint_slice_client: Option<T>,
        feature_flags: FeatureFlagClient,
    ) -> Self {
        let feature_flag_attributes = cluster_api_flag_attributes();
        if endpoint_slice_client.is_none() {
            // `client_for_subject` never reads the flag without an
            // EndpointSlice client, so read it once here. The answer is
            // already decided — the master is off — but the read is what puts
            // this flag in `feature_flags.evaluation`, so a deployment where
            // the rollout cannot apply still shows up instead of looking like
            // one whose routing is never reached.
            let _ = feature_flags.gate_enabled_with_attributes(
                false,
                SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG,
                "startup",
                feature_flag_attributes.clone(),
            );
        }
        Self {
            service_client,
            endpoint_slice_client,
            feature_flags,
            feature_flag_attributes,
        }
    }

    #[cfg(test)]
    fn service_only(client: T) -> Self {
        Self::new(client, None, FeatureFlagClient::disabled())
    }

    /// The cluster-api client this subject routes through.
    ///
    /// Env-master-first, with the EndpointSlice client's *presence* as the
    /// master: `connect_cluster_api_clients_for_startup` builds one only when
    /// `SPACE_CLUSTER_API_ENDPOINT_DISCOVERY` selects Kubernetes and the client
    /// connects, so a missing client is that deploy-time decision already
    /// made. The flag declares `false`, so every reason a read resolves
    /// nothing — no provider, not ready, no assignment, failed evaluation —
    /// keeps this subject on the Service client.
    ///
    /// With no EndpointSlice client this returns before the flag is read. A
    /// per-request flag evaluation, attribute clone, and metric would repeat
    /// an answer fixed at startup. [`Self::new`] reads the flag once instead.
    fn client_for_subject(&self, subject: &str) -> T {
        let Some(endpoint_slice_client) = &self.endpoint_slice_client else {
            return self.service_client.clone();
        };
        let balanced = self.feature_flags.gate_enabled_with_attributes(
            true,
            SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG,
            subject,
            self.feature_flag_attributes.clone(),
        );
        if balanced {
            endpoint_slice_client.clone()
        } else {
            self.service_client.clone()
        }
    }

    fn for_session(&self, session_id: &DataPlaneSessionId) -> T {
        self.client_for_subject(&format!("session:{session_id}"))
    }

    fn for_sandbox(&self, sandbox_id: &impl Display) -> T {
        self.client_for_subject(&format!("sandbox:{sandbox_id}"))
    }

    fn for_org(&self, org_id: &OrgId) -> T {
        self.client_for_subject(&format!("org:{org_id}"))
    }
}

fn cluster_api_flag_attributes() -> Attributes {
    let mut attributes = Attributes::default();
    if let Some(cluster_id) = process_cluster_id() {
        attributes.insert("cluster_id".into(), cluster_id.as_str().to_owned().into());
    }
    attributes
}

impl DataPlaneConfig {
    /// Check the settings [`run`] cannot serve with.
    pub fn validate(&self) -> Result<(), RunDataPlaneError> {
        if self.revocation_check_interval > Duration::from_millis(MAX_REVOCATION_CHECK_INTERVAL_MS)
        {
            return Err(RunDataPlaneError::RevocationCheckIntervalTooLong {
                interval_ms: self.revocation_check_interval.as_millis(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunDataPlaneError {
    #[error(
        "revocation check interval of {interval_ms} ms is above the maximum of {} ms",
        MAX_REVOCATION_CHECK_INTERVAL_MS
    )]
    RevocationCheckIntervalTooLong { interval_ms: u128 },
    #[error("failed to connect to SPACE internal ClusterApi: {0}")]
    ConnectClusterApi(#[source] ConnectClusterApiError),
    #[error("failed to bind data-plane listener at {listen}: {source}")]
    Bind {
        listen: String,
        #[source]
        source: std::io::Error,
    },
    #[error("data-plane HTTP server failed: {0}")]
    Serve(#[source] std::io::Error),
}

/// Serve until `stop.shutdown` is cancelled, then drain. `stop.forced`, a
/// second stop signal, ends the drain the way its deadline does, so a process
/// waiting on a stream without a deadline can still be stopped.
pub async fn run(
    config: DataPlaneConfig,
    stop: pplx_space_common_svc::StopTokens,
) -> Result<(), RunDataPlaneError> {
    config.validate()?;
    let pplx_space_common_svc::StopTokens {
        shutdown: shutdown_token,
        forced: force_token,
    } = stop;
    let upgrade_tasks = TaskTracker::new();
    let drain_started = shutdown_token.child_token();
    let stream_shutdown_token = CancellationToken::new();

    let cluster_api = connect_cluster_api_clients_for_startup(&shutdown_token)
        .await
        .map_err(RunDataPlaneError::ConnectClusterApi)?;
    let attached_streams = Arc::new(AttachedStreamLimiter::new(
        config.max_attached_streams,
        config.max_attached_streams_per_org,
        config.max_attached_streams_per_sandbox,
    ));
    let state = DataPlaneState {
        cluster_api,
        attach_deadline: config.attach_deadline,
        unauthenticated: Arc::new(UnauthenticatedUpgradeLimiter::new(
            config.unauthenticated_upgrade_cap_per_ip,
        )),
        attached_streams: Arc::clone(&attached_streams),
        uploads: config
            .upload_admission
            .as_ref()
            .map(|upload_admission| Arc::new(UploadAdmission::new(upload_admission))),
        upload_tracker: Arc::default(),
        native_sessions: Arc::new(NativeSessionAttachLimiter::default()),
        backend_channels: Arc::new(BackendChannelPool::new(config.backend_channel_pool_size)),
        revocation_cache: Arc::new(RevocationCache::default()),
        route_cache: Arc::new(RouteCache::default()),
        ecw_route_repairs: Arc::new(DashMap::new()),
        ecw_process_input_locks: Arc::new(EcwProcessInputLocks::default()),
        ecw_hosts: Arc::new(EcwHostConfig::from_env()),
        revocation_check_interval: config
            .revocation_check_interval
            .max(Duration::from_millis(1)),
        revocation_max_staleness: REVOCATION_MAX_STALENESS,
        revocation_check_timeout: REVOCATION_CHECK_TIMEOUT,
        route_cache_ttl: config.route_cache_ttl.max(Duration::from_millis(1)),
        drain_started,
        stream_shutdown_token: stream_shutdown_token.clone(),
        upgrade_tasks: upgrade_tasks.clone(),
    };

    tracing::info!(
        revocation_check_interval_ms = state.revocation_check_interval.as_millis(),
        revocation_tolerance_ms = native::revocation_tolerance(&state).as_millis(),
        "attached streams keep a confirmed revocation verdict through failed checks \
         for up to the revocation tolerance"
    );
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|source| RunDataPlaneError::Bind {
            listen: config.listen.clone(),
            source,
        })?;
    tracing::info!(listen = %config.listen, "space-data-plane-svc listening");

    serve_and_drain(
        data_plane_listener(listener),
        app,
        DrainLifecycle {
            shutdown_token,
            stream_shutdown_token,
            tasks: upgrade_tasks,
            attached_streams,
            shutdown_grace: config.shutdown_grace,
            force_token,
        },
    )
    .await
    .map_err(RunDataPlaneError::Serve)
}

/// How long an accepted connection may sit idle before its first keepalive
/// probe.
const DATA_PLANE_TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
/// How long between keepalive probes.
const DATA_PLANE_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// How many unanswered keepalive probes drop the connection.
const DATA_PLANE_TCP_KEEPALIVE_RETRIES: u32 = 3;

type DataPlaneListener = axum::serve::TapIo<
    pplx_space_common_svc::http_listener::NodelayListener,
    fn(&mut tokio::net::TcpStream),
>;

/// Accept connections with `TCP_NODELAY` and the data plane's dead-peer
/// detection set, on every route this listener serves, ECW's included.
///
/// Keepalive probes a connection that carries nothing for 30 seconds and drops
/// it after about a minute of silence, so an idle connection that dies
/// silently releases its stream, and the node's per-sandbox permit behind it.
/// It detects the immediate TCP peer, which is often a proxy rather than the
/// end client. It never closes a connection whose peer still answers, so it
/// sets no limit on how long a client may stop reading. A stream that sends
/// regularly, such as SSE with its 15-second keep-alive comments, is never
/// idle, so a dead peer on it waits out the kernel's retransmission timeout.
/// No `TCP_USER_TIMEOUT` is set: this listener serves ECW routes too, and on
/// Linux that option would also decide when keepalive gives up.
fn data_plane_listener(listener: tokio::net::TcpListener) -> DataPlaneListener {
    fn set_dead_peer_detection(tcp: &mut tokio::net::TcpStream) {
        let socket = socket2::SockRef::from(&*tcp);
        let keepalive = socket2::TcpKeepalive::new()
            .with_time(DATA_PLANE_TCP_KEEPALIVE_TIME)
            .with_interval(DATA_PLANE_TCP_KEEPALIVE_INTERVAL)
            .with_retries(DATA_PLANE_TCP_KEEPALIVE_RETRIES);
        if let Err(error) = socket.set_tcp_keepalive(&keepalive) {
            tracing::debug!(%error, "failed to set TCP keepalive on an accepted connection");
        }
    }
    axum::serve::ListenerExt::tap_io(
        pplx_space_common_svc::http_listener::nodelay(listener),
        set_dead_peer_detection as fn(&mut tokio::net::TcpStream),
    )
}

async fn connect_cluster_api_clients_for_startup(
    shutdown_token: &CancellationToken,
) -> Result<ClusterApiClients, ConnectClusterApiError> {
    let discovery = EndpointDiscovery::from_env();
    let service_client =
        connect_cluster_api_for_startup(shutdown_token, EndpointDiscovery::Off).await?;
    let (endpoint_slice_client, feature_flags) = if discovery == EndpointDiscovery::Kubernetes {
        let endpoint_slice_client = optional_endpoint_slice_client(
            pplx_space_node_api_client::cluster::connect_from_env_with_discovery(discovery).await,
        );
        let feature_flags = if endpoint_slice_client.is_some() {
            FeatureFlagClient::from_env()
        } else {
            FeatureFlagClient::disabled()
        };
        (endpoint_slice_client, feature_flags)
    } else {
        (None, FeatureFlagClient::disabled())
    };
    Ok(ClusterApiClients::new(
        service_client,
        endpoint_slice_client,
        feature_flags,
    ))
}

fn optional_endpoint_slice_client<T>(result: Result<T, ConnectClusterApiError>) -> Option<T> {
    match result {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::warn!(
                %error,
                "EndpointSlice cluster api client unavailable; continuing with the Service client"
            );
            None
        }
    }
}

async fn connect_cluster_api_for_startup(
    shutdown_token: &CancellationToken,
    discovery: EndpointDiscovery,
) -> Result<AuthenticatedClusterApiClient, ConnectClusterApiError> {
    let mut delay = CLUSTER_API_STARTUP_RETRY_BASE;
    let mut warned = false;
    loop {
        match pplx_space_node_api_client::cluster::connect_from_env_with_discovery(discovery).await
        {
            Ok(client) => return Ok(client),
            Err(error) if cluster_api_connect_error_is_retryable(&error) => {
                if warned {
                    tracing::debug!(
                        error = %error,
                        retry_ms = delay.as_millis() as u64,
                        "cluster api unavailable during data-plane startup; retrying"
                    );
                } else {
                    tracing::warn!(
                        error = %error,
                        retry_ms = delay.as_millis() as u64,
                        "cluster api unavailable during data-plane startup; retrying"
                    );
                    warned = true;
                }
                tokio::select! {
                    () = shutdown_token.cancelled() => return Err(error),
                    () = tokio::time::sleep(delay) => {
                        delay = (delay * 2).min(CLUSTER_API_STARTUP_RETRY_MAX);
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn cluster_api_connect_error_is_retryable(error: &ConnectClusterApiError) -> bool {
    matches!(error, ConnectClusterApiError::Transport(_))
}

fn native_method_filter(method: NativeHttpMethod) -> MethodFilter {
    match method {
        NativeHttpMethod::Delete => MethodFilter::DELETE,
        NativeHttpMethod::Get => MethodFilter::GET.or(MethodFilter::HEAD),
        NativeHttpMethod::Patch => MethodFilter::PATCH,
        NativeHttpMethod::Post => MethodFilter::POST,
        NativeHttpMethod::Put => MethodFilter::PUT,
    }
}

fn router(state: DataPlaneState) -> Router {
    let http_span_metadata = DataPlaneHttpSpanMetadata::new(Arc::clone(&state.ecw_hosts));

    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route(
            native_routes::ATTACH_SESSION.runtime_path(),
            on(
                native_method_filter(native_routes::ATTACH_SESSION.method()),
                attach_session,
            ),
        )
        .route(
            native_routes::STREAM_SESSION_EVENTS.runtime_path(),
            on(
                native_method_filter(native_routes::STREAM_SESSION_EVENTS.method()),
                session_events,
            ),
        )
        .route(
            native_routes::PROXY_SESSION.runtime_path(),
            any(proxy_session),
        )
        .route(
            native_routes::PROXY_SESSION_SUBPATH_MOUNT,
            any(proxy_session),
        )
        .route(
            "/health",
            get(|| async { StatusCode::NO_CONTENT }).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/files",
            get(ecw_files::read_file)
                .post(ecw_files::write_file)
                .fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/Stat",
            post(ecw_files::filesystem_stat).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/ListDir",
            post(ecw_files::filesystem_list_dir).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/MakeDir",
            post(ecw_files::filesystem_make_dir).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/Move",
            post(ecw_files::filesystem_move).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/Remove",
            post(ecw_files::filesystem_remove).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/WatchDir",
            post(ecw_files::e2b_watch_dir).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/CreateWatcher",
            post(ecw_files::e2b_create_watcher).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/GetWatcherEvents",
            post(ecw_files::e2b_get_watcher_events).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/filesystem.Filesystem/RemoveWatcher",
            post(ecw_files::e2b_remove_watcher).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/List",
            post(ecw_process::process_list).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/Start",
            post(ecw_process::process_start).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/Connect",
            post(ecw_process::process_connect).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/Update",
            post(ecw_process::process_update).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/StreamInput",
            post(ecw_process::process_stream_input).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/SendInput",
            post(ecw_process::process_send_input).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/SendSignal",
            post(ecw_process::process_send_signal).fallback(unsupported_ecw_envd_method),
        )
        .route(
            "/process.Process/CloseStdin",
            post(ecw_process::process_close_stdin).fallback(unsupported_ecw_envd_method),
        )
        .route("/metrics", any(unsupported_ecw_envd_endpoint))
        .route("/envs", any(unsupported_ecw_envd_endpoint))
        .route("/files/compose", any(unsupported_ecw_envd_endpoint))
        .fallback(any(ecw_service_proxy))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            route_ecw_service_port_requests,
        ))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state.ecw_hosts),
            http_traffic::observe_http_traffic,
        ))
        .with_state(state)
        .layer(middleware::from_fn(close_draining_http1_connections))
        .layer(pplx_space_common_svc::http_trace_layer(
            HttpServerMakeSpan::new(http_span_metadata),
        ))
        .layer(CorsLayer::permissive())
}

async fn unsupported_ecw_envd_endpoint() -> Response {
    EcwProxyError::new(
        StatusCode::NOT_IMPLEMENTED,
        "This E2B envd endpoint is not supported by SPACE ECW",
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
    .into_response()
}

async fn unsupported_ecw_envd_method() -> Response {
    EcwProxyError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "This HTTP method is not supported for the requested E2B envd endpoint",
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
    .into_response()
}

#[derive(Clone, Debug)]
struct DataPlaneHttpSpanMetadata {
    ecw_hosts: Arc<EcwHostConfig>,
}

impl DataPlaneHttpSpanMetadata {
    fn new(ecw_hosts: Arc<EcwHostConfig>) -> Self {
        Self { ecw_hosts }
    }

    fn session_id_from_path<'a>(&self, raw_path: &'a str) -> Option<&'a str> {
        raw_path
            .strip_prefix("/v1/sessions/")
            .and_then(|suffix| suffix.split('/').next())
            .filter(|session_id| !session_id.is_empty())
    }
}

impl HttpSpanMetadata for DataPlaneHttpSpanMetadata {
    fn span_path_and_route<'a>(
        &self,
        _method: &axum::http::Method,
        headers: &HeaderMap,
        raw_path: &'a str,
        matched_route: Option<&'a str>,
    ) -> (Cow<'a, str>, Cow<'a, str>) {
        if self.ecw_hosts.parse_sandbox_host(headers).is_some() {
            let route = matched_route.unwrap_or(pplx_space_common_svc::REDACTED_HTTP_PATH_VALUE);
            return (Cow::Borrowed(route), Cow::Borrowed(route));
        }

        (
            Cow::Borrowed(raw_path),
            Cow::Borrowed(matched_route.unwrap_or(raw_path)),
        )
    }

    fn record_extra_fields(
        &self,
        headers: &HeaderMap,
        raw_path: &str,
        _matched_route: Option<&str>,
        span: &tracing::Span,
    ) {
        if let Some(target) = self.ecw_hosts.parse_sandbox_host(headers) {
            let sandbox_id = target.sandbox_id.to_string();
            span.record("sandbox_id", sandbox_id.as_str());
            span.record("sandbox_port", target.port);
        }

        if let Some(session_id) = self.session_id_from_path(raw_path) {
            span.record("session_id", session_id);
        }
    }

    /// A sandbox-host request is the sandbox's own traffic, proxied to its
    /// service port or envd, so it stays traced whatever its path: a sandbox
    /// app's `/health` can wake, restore or fail like any other request.
    fn is_untraced(
        &self,
        method: &axum::http::Method,
        headers: &HeaderMap,
        raw_path: &str,
    ) -> bool {
        self.ecw_hosts.parse_sandbox_host(headers).is_none()
            && pplx_space_common_svc::is_probe_request(method, raw_path)
    }
}

async fn ecw_service_proxy(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
) -> Result<Response, EcwProxyError> {
    let target = state
        .ecw_hosts
        .parse_sandbox_host(request.headers())
        .ok_or_else(|| EcwProxyError::not_found("Endpoint"))?;
    validate_ecw_sandbox_headers(request.headers(), &target)?;

    if target.port == ECW_ENVD_PORT {
        return Err(EcwProxyError::not_found("Endpoint"));
    }
    if is_upgrade_request(request.headers()) {
        refuse_new_ecw_stream_while_draining(&state, ECW_SERVICE_STREAM_KIND)?;
    }

    pplx_space_metrics::data_plane::service_route_transport(
        ServiceRouteSurface::Ecw,
        ServiceRouteTransport::ServicePortIngress,
    );
    let mut preparation = pplx_space_metrics::data_plane::IngressPreparationObservation::default();
    let _waiter = pplx_space_common_svc::ingress_admission::IngressPreparationPermit::acquire(
        Some(target.sandbox_id),
    )
    .map_err(|_| {
        EcwProxyError::too_many_attached_streams(TooManyRequestsReason::IngressPreparation)
    })?;
    let deadline = tokio::time::Instant::now()
        + pplx_space_common_svc::ingress_admission::preparation_timeout();
    let request_headers = request.headers().clone();
    let prepared = before_stream_deadline(
        &state.stream_shutdown_token,
        tokio::time::timeout_at(deadline, async {
            let mut route = resolve_ecw_route(
                &state,
                &target,
                &request_headers,
                EcwRouteTransport::ServicePortIngress,
            )
            .await?;
            for attempt in 0..2 {
                match service_port_ingress_io(&state, &target, &route).await {
                    Ok(io) => {
                        // Renew authorization after a potentially long implicit wake,
                        // before the first application byte is sent, unless the open
                        // finished while the cache still serves this route.
                        if runtime_route::ecw_route_still_cached(&state, &route) {
                            return Ok((route, io));
                        }
                        let fresh = runtime_route::resolve_ecw_route_uncached(
                            &state,
                            &route.origin,
                            &route.origin.cache_key(),
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .await?;
                        if route.same_ingress_execution(&fresh) {
                            return Ok((fresh, io));
                        }
                        route = fresh;
                    }
                    Err(error) => {
                        let retryable = error
                            .transport_status
                            .as_ref()
                            .is_some_and(crate::runtime_route::retryable_ingress_attachment);
                        if attempt == 1 || !retryable {
                            return Err(error.error);
                        }
                        route = runtime_route::resolve_ecw_route_uncached(
                            &state,
                            &route.origin,
                            &route.origin.cache_key(),
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .await?;
                    }
                }
            }
            Err(EcwProxyError::sandbox_service_route_not_ready())
        }),
    )
    .await;
    let Some(prepared) = prepared else {
        if is_upgrade_request(&request_headers) {
            pplx_space_metrics::data_plane::attach(ECW_SERVICE_STREAM_KIND, "draining");
        }
        return Err(EcwProxyError::draining());
    };
    let prepared = prepared.map_err(|_| EcwProxyError::sandbox_service_route_not_ready())?;
    drop(_waiter);
    let (route, ingress_io) = prepared?;
    preparation.ready();
    drop(preparation);
    ecw_service_proxy_via_service_port_ingress(state, target, route, request, Ok(ingress_io)).await
}

async fn ecw_service_proxy_via_service_port_ingress(
    state: DataPlaneState,
    target: EcwSandboxHost,
    route: EcwResolvedRoute,
    request: Request<Body>,
    ingress_open: Result<IngressIo, ServicePortIngressOpenError>,
) -> Result<Response, EcwProxyError> {
    match ingress_open {
        Ok(ingress_io) => {
            ecw_service_ingress_proxy_with_io(state, target, route, request, ingress_io).await
        }
        Err(ServicePortIngressOpenError {
            error,
            reason,
            transport_status,
        }) => {
            record_service_port_ingress_open(ServicePortIngressOpenResult::Error, reason);
            if let Some(status) = transport_status
                && reset_ecw_backend_channel_on_transport_status(
                    &state,
                    &route,
                    &status,
                    "service_port_ingress_open",
                )
            {
                state
                    .route_cache
                    .forget_ecw_target(target.sandbox_id, target.port);
                schedule_ecw_sandbox_service_route_repair(
                    &state,
                    target.clone(),
                    route.clone(),
                    status.to_string(),
                );
            }
            Err(error)
        }
    }
}

async fn ecw_service_ingress_proxy_with_io(
    state: DataPlaneState,
    target: EcwSandboxHost,
    route: EcwResolvedRoute,
    mut request: Request<Body>,
    mut ingress_io: IngressIo,
) -> Result<Response, EcwProxyError> {
    rewrite_ecw_service_ingress_request(&mut request)?;
    let upgrade_stream_permit = if is_upgrade_request(request.headers()) {
        Some(acquire_ecw_stream_permit(&state, ECW_SERVICE_STREAM_KIND)?)
    } else {
        None
    };
    let downstream_upgrade = if upgrade_stream_permit.is_some() {
        Some(hyper::upgrade::on(&mut request))
    } else {
        None
    };
    let completion = ingress_io.take_completion();
    if upgrade_stream_permit.is_some() {
        ingress_io.track_transport_progress();
    }
    let request = request.map(|body| completion.body(body));
    let (mut sender, connection) =
        http1::handshake(TokioIo::new(ingress_io))
            .await
            .map_err(|error| {
                tracing::debug!(
                    sandbox_id = %target.sandbox_id,
                    port = target.port,
                    route_generation = route.route_generation,
                    %error,
                    "failed to open ECW service port ingress HTTP connection",
                );
                EcwProxyError::sandbox_service_unreachable()
            })?;
    let connection_tasks = state.upgrade_tasks.clone();
    connection_tasks.spawn(async move {
        if let Err(error) = connection.with_upgrades().await {
            tracing::debug!(%error, "ECW service port ingress HTTP connection ended with error");
        }
    });

    let mut response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => {
            state
                .route_cache
                .forget_ecw_target(target.sandbox_id, target.port);
            tracing::debug!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                %error,
                "failed to proxy ECW sandbox service request through ServicePort ingress",
            );
            return Err(EcwProxyError::sandbox_service_unreachable());
        }
    };
    record_ecw_sandbox_proxy_response(response.status());
    mark_sandbox_service_response(&mut response);

    let upstream_upgrade =
        if downstream_upgrade.is_some() && response.status() == StatusCode::SWITCHING_PROTOCOLS {
            Some(hyper::upgrade::on(&mut response))
        } else {
            None
        };
    let response = response.map(|body| completion.body(Body::new(body)));
    if let (Some(downstream_upgrade), Some(upstream_upgrade), Some(stream_permit)) =
        (downstream_upgrade, upstream_upgrade, upgrade_stream_permit)
    {
        let shutdown_token = state.stream_shutdown_token.child_token();
        let upgrade_tasks = state.upgrade_tasks.clone();
        upgrade_tasks.spawn(async move {
            let _stream_permit = stream_permit;
            let _completion = completion;
            match proxy_upgraded_connection(
                ECW_SERVICE_STREAM_KIND,
                downstream_upgrade,
                upstream_upgrade,
                shutdown_token,
            )
            .await
            {
                Ok(close) => pplx_space_metrics::data_plane::stream_closed(
                    ECW_SERVICE_STREAM_KIND,
                    close.result_label(),
                ),
                Err(error) => {
                    pplx_space_metrics::data_plane::stream_closed(
                        ECW_SERVICE_STREAM_KIND,
                        "proxy_error",
                    );
                    tracing::debug!(%error, "ECW sandbox service ingress upgrade proxy ended with error");
                }
            }
        });
    }

    Ok(response.into_response())
}

#[derive(Debug)]
struct ServicePortIngressOpenError {
    error: EcwProxyError,
    reason: ServicePortIngressOpenReason,
    transport_status: Option<tonic::Status>,
}

fn service_port_ingress_missing_token_error() -> ServicePortIngressOpenError {
    ServicePortIngressOpenError {
        error: EcwProxyError::sandbox_service_route_not_ready()
            .without_runtime_access_classification(),
        reason: ServicePortIngressOpenReason::MissingRouteToken,
        transport_status: None,
    }
}

fn record_service_port_ingress_open(
    result: ServicePortIngressOpenResult,
    reason: ServicePortIngressOpenReason,
) {
    pplx_space_metrics::data_plane::service_port_ingress_open(result, reason);
}

fn service_port_ingress_open_reason(error: &tonic::Status) -> ServicePortIngressOpenReason {
    if pplx_space_common_grpc::GuestServiceUnavailable::from_status(error).is_some() {
        ServicePortIngressOpenReason::GuestConnectionRefused
    } else if error.code() == tonic::Code::Unimplemented {
        ServicePortIngressOpenReason::Unsupported
    } else {
        ServicePortIngressOpenReason::OpenError
    }
}

async fn service_port_ingress_io(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    route: &EcwResolvedRoute,
) -> Result<IngressIo, ServicePortIngressOpenError> {
    let route_token = route.service_port_ingress_token.as_ref().ok_or_else(|| {
        tracing::warn!(
            sandbox_id = %target.sandbox_id,
            port = target.port,
            route_generation = route.route_generation,
            "ECW service port ingress route is missing route token"
        );
        service_port_ingress_missing_token_error()
    })?;
    let channel =
        ecw_backend_channel(state, route)
            .await
            .map_err(|error| ServicePortIngressOpenError {
                error,
                reason: ServicePortIngressOpenReason::OpenError,
                transport_status: None,
            })?;
    open_service_port_ingress_io(
        state,
        &target.sandbox_id,
        target.port,
        route.route_generation,
        route.current_node_id,
        channel,
        route_token,
    )
    .await
}

async fn open_service_port_ingress_io(
    state: &DataPlaneState,
    sandbox_id: &SandboxId,
    port: u32,
    route_generation: u64,
    current_node_id: NodeId,
    channel: Channel,
    route_token: &SpaceApiRouteTokenState,
) -> Result<IngressIo, ServicePortIngressOpenError> {
    // Enrichment must not change ingress admission or route-repair behavior.
    let trace_context = u16::try_from(port)
        .ok()
        .and_then(|value| pplx_space_common_core::types::GuestPort::try_from_u16(value).ok())
        .map(|port| pplx_space_common_grpc::SandboxTraceContext {
            sandbox_id: *sandbox_id,
            port,
            route_generation: route_generation.try_into().ok(),
            execution_generation: None,
            current_node_id: Some(current_node_id),
        });
    if let Some(context) = &trace_context {
        context.record(&tracing::Span::current());
    }
    let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    tx.send(ServicePortIngressFrame {
        message: Some(service_port_ingress_frame::Message::Start(
            ServicePortIngressStart {
                sandbox_id: sandbox_id.to_string(),
                port,
            },
        )),
    })
    .await
    .map_err(|_| ServicePortIngressOpenError {
        error: EcwProxyError::sandbox_service_unreachable(),
        reason: ServicePortIngressOpenReason::OpenError,
        transport_status: None,
    })?;
    let mut request = node_api_request_with_route_token_state(
        tokio_stream::wrappers::ReceiverStream::new(rx),
        Some(route_token),
    )
    .map_err(|error| {
        tracing::warn!(
            %sandbox_id,
            port,
            route_generation,
            %error,
            "failed to build service port ingress request"
        );
        ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_route_not_ready()
                .without_runtime_access_classification(),
            reason: ServicePortIngressOpenReason::OpenError,
            transport_status: None,
        }
    })?;
    request.metadata_mut().insert(
        pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA,
        tonic::metadata::MetadataValue::from_static("1"),
    );
    if let Some(context) = trace_context {
        request.extensions_mut().insert(context);
    }
    let response = client
        .service_port_ingress(request)
        .await
        .map_err(|error| {
            pplx_space_common_grpc::record_guest_service_outcome(&error, &tracing::Span::current());
            tracing::debug!(
                %sandbox_id,
                port,
                route_generation,
                %error,
                "failed to open service port ingress"
            );
            let reason = service_port_ingress_open_reason(&error);
            ServicePortIngressOpenError {
                error: EcwProxyError::sandbox_service_unreachable(),
                reason,
                transport_status: Some(error),
            }
        })?;
    record_service_port_ingress_open(
        ServicePortIngressOpenResult::IngressOk,
        ServicePortIngressOpenReason::Ok,
    );
    let downstream_completion = response
        .metadata()
        .get(pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA)
        .is_some_and(|value| value == "1");
    let stream = response.into_inner();
    let (client_io, ingress_io) = tokio::io::duplex(64 * 1024);
    let shutdown_token = state.stream_shutdown_token.child_token();
    let ingress_tasks = state.upgrade_tasks.clone();
    let done = CancellationToken::new();
    let progress = Progress::new();
    let completion = Completion::new(done.clone(), progress.clone());
    ingress_tasks.spawn(async move {
        if let Err(error) = pump_service_port_ingress_io_with_stream(
            ingress_io,
            tx,
            stream,
            shutdown_token,
            done,
            progress,
            downstream_completion,
        )
        .await
        {
            tracing::debug!(%error, "service port ingress IO pump ended with error");
        }
    });
    Ok(IngressIo::new(client_io, completion))
}

async fn native_service_port_ingress_io(
    state: &DataPlaneState,
    attached: &AttachedSession,
) -> Result<IngressIo, ServicePortIngressOpenError> {
    let port = attached
        .target_port
        .ok_or_else(|| ServicePortIngressOpenError {
            error: EcwProxyError::sandbox_service_route_not_ready()
                .without_runtime_access_classification(),
            reason: ServicePortIngressOpenReason::OpenError,
            transport_status: None,
        })?;
    let route_token = attached
        .space_api_route_token
        .as_ref()
        .ok_or_else(service_port_ingress_missing_token_error)?;
    let channel = state
        .backend_channels
        .channel(&attached.backend_endpoint, attached.session_id)
        .await
        .map_err(|error| {
            tracing::warn!(
                session_id = %attached.session_id,
                sandbox_id = %attached.sandbox_id,
                %error,
                "failed to build native service ingress backend channel",
            );
            ServicePortIngressOpenError {
                error: EcwProxyError::sandbox_service_unreachable(),
                reason: ServicePortIngressOpenReason::OpenError,
                transport_status: None,
            }
        })?;
    open_service_port_ingress_io(
        state,
        &attached.sandbox_id,
        port,
        attached.route_generation,
        attached.current_node_id,
        channel,
        route_token,
    )
    .await
}

#[derive(Debug, thiserror::Error)]
enum ServicePortIngressPumpError {
    #[error("service port ingress IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("service port ingress gRPC stream failed: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("service port ingress request channel closed")]
    RequestChannelClosed,
}

async fn pump_service_port_ingress_io_with_stream<S>(
    ingress_io: tokio::io::DuplexStream,
    tx: tokio::sync::mpsc::Sender<ServicePortIngressFrame>,
    mut stream: S,
    shutdown_token: CancellationToken,
    complete: CancellationToken,
    progress: Progress,
    downstream_completion: bool,
) -> Result<(), ServicePortIngressPumpError>
where
    S: futures_util::Stream<Item = Result<ServicePortIngressFrame, tonic::Status>> + Unpin,
{
    let (mut ingress_read, mut ingress_write) = tokio::io::split(ingress_io);
    let completion_tx = downstream_completion.then(|| tx.clone());
    let output = tx;
    let ingress_to_grpc = async move {
        let mut buffer = vec![0_u8; 16 * 1024];
        loop {
            let read = ingress_read.read(&mut buffer).await?;
            if read == 0 && !downstream_completion {
                return Ok::<(), ServicePortIngressPumpError>(());
            }
            let message = if read == 0 {
                service_port_ingress_frame::Message::Eof(())
            } else {
                service_port_ingress_frame::Message::Data(
                    buffer
                        .get(..read)
                        .ok_or_else(|| std::io::Error::other("invalid ingress read length"))?
                        .to_vec(),
                )
            };
            output
                .send(ServicePortIngressFrame {
                    message: Some(message),
                })
                .await
                .map_err(|_| ServicePortIngressPumpError::RequestChannelClosed)?;
            if read == 0 {
                return Ok::<(), ServicePortIngressPumpError>(());
            }
        }
    };
    let grpc_to_ingress = async move {
        let mut eof = false;
        while let Some(frame) = stream.next().await.transpose()? {
            match frame.message {
                Some(service_port_ingress_frame::Message::Data(data)) if !eof => {
                    ingress_write.write_all(&data).await?;
                }
                Some(service_port_ingress_frame::Message::Eof(_)) if !eof => {
                    eof = true;
                    ingress_write.shutdown().await?;
                }
                _ => {
                    return Err(ServicePortIngressPumpError::Grpc(
                        tonic::Status::invalid_argument("invalid ingress response frame"),
                    ));
                }
            }
        }
        if !downstream_completion {
            ingress_write.shutdown().await?;
            return Ok(());
        }
        Err::<(), ServicePortIngressPumpError>(ServicePortIngressPumpError::Grpc(
            tonic::Status::cancelled("ingress ended before downstream completion"),
        ))
    };
    tokio::select! {
        biased;
        _ = shutdown_token.cancelled() => { progress.abort.cancel(); Ok(()) },
        _ = progress.expired(pplx_space_net_gateway_client::service_port_ingress_idle_timeout_from_env()) => {
            pplx_space_metrics::data_plane::ingress_completion_timeout();
            Err(ServicePortIngressPumpError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "ingress completion timed out")))
        },
        _ = complete.cancelled() => {
            // Cancellation still drops the RPC if its request queue is wedged.
            if let Some(tx) = completion_tx
                && !matches!(tokio::time::timeout(Duration::from_secs(1), tx.send(ServicePortIngressFrame {
                message: Some(service_port_ingress_frame::Message::Complete(())),
            })).await, Ok(Ok(()))) {
                tracing::debug!("ingress completion could not be sent; cancelling attachment");
            }
            Ok(())
        },
        result = async { tokio::try_join!(ingress_to_grpc, grpc_to_ingress)?; Ok(()) } => {
            if result.is_err() { progress.abort.cancel(); }
            result
        },
    }
}

fn record_ecw_sandbox_proxy_response(status: StatusCode) {
    pplx_space_metrics::data_plane::response(
        ResponseSurface::Ecw,
        ResponseOrigin::SandboxTraffic,
        ResponseKind::ProxiedStatus,
        status.as_u16(),
    );
    tracing::Span::current().record(
        "space.response_origin",
        ResponseOrigin::SandboxTraffic.as_str(),
    );
    tracing::Span::current().record("space.response_kind", ResponseKind::ProxiedStatus.as_str());
}

pub(crate) async fn ecw_runtime_access_error(
    state: &DataPlaneState,
    sandbox_id: SandboxId,
    error: EcwProxyError,
) -> EcwProxyError {
    if !ecw_error_can_use_runtime_access_decision(&error) {
        return error;
    }
    match get_sandbox_runtime_access(state, sandbox_id).await {
        Ok(Some(access)) => ecw_error_for_runtime_access(access, error),
        Ok(None) => error,
        Err(access_error) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %access_error,
                "failed to classify ECW runtime access failure",
            );
            error
        }
    }
}

fn ecw_error_can_use_runtime_access_decision(error: &EcwProxyError) -> bool {
    error.can_use_runtime_access && error.status == StatusCode::BAD_GATEWAY
}

async fn get_sandbox_runtime_access(
    state: &DataPlaneState,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxRuntimeAccess>, tonic::Status> {
    let started_at = std::time::Instant::now();
    let mut client = state.cluster_api.for_sandbox(&sandbox_id);
    let response = match tokio::time::timeout(
        ECW_RUNTIME_ACCESS_CLASSIFY_TIMEOUT,
        client.get_sandbox_runtime_access(GetSandboxRuntimeAccessRequest {
            sandbox_id: sandbox_id.to_string(),
        }),
    )
    .await
    {
        Ok(Ok(response)) => {
            record_ecw_backend_rpc_latency(
                "get_sandbox_runtime_access",
                BackendRpcResult::Ok,
                started_at,
            );
            response
        }
        Ok(Err(status)) => {
            record_ecw_backend_rpc_latency(
                "get_sandbox_runtime_access",
                BackendRpcResult::Status,
                started_at,
            );
            return Err(status);
        }
        Err(_) => {
            record_ecw_backend_rpc_latency(
                "get_sandbox_runtime_access",
                BackendRpcResult::Timeout,
                started_at,
            );
            return Err(tonic::Status::deadline_exceeded(
                "ECW runtime access classification timed out",
            ));
        }
    };
    Ok(response.into_inner().access)
}

fn ecw_error_for_runtime_access(
    access: SandboxRuntimeAccess,
    original: EcwProxyError,
) -> EcwProxyError {
    match SandboxRuntimeAccessDecision::try_from(access.decision)
        .unwrap_or(SandboxRuntimeAccessDecision::Unspecified)
    {
        SandboxRuntimeAccessDecision::TerminalUnavailable => {
            EcwProxyError::sandbox_data_plane_unavailable()
        }
        SandboxRuntimeAccessDecision::RetryableConverging => {
            let retry_after = if access.retry_after_ms == 0 {
                ECW_MISSING_RETRY_AFTER_FLOOR
            } else {
                Duration::from_millis(access.retry_after_ms)
            };
            EcwProxyError::sandbox_waking(retry_after)
        }
        SandboxRuntimeAccessDecision::Ready
        | SandboxRuntimeAccessDecision::Unknown
        | SandboxRuntimeAccessDecision::Unspecified => original,
    }
}

async fn route_ecw_service_port_requests(
    State(state): State<DataPlaneState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if should_proxy_ecw_service_port_request(&state.ecw_hosts, request.headers()) {
        ecw_service_proxy(State(state), request)
            .await
            .into_response()
    } else {
        next.run(request).await
    }
}

/// Attributes a relayed sandbox response, so a 429 the sandbox's own service
/// returned is counted with `reason:sandbox_service` rather than as an
/// unclassified data-plane rejection. Filter on `reason` to leave it out.
fn mark_sandbox_service_response<B>(response: &mut axum::http::Response<B>) {
    response
        .extensions_mut()
        .insert(TooManyRequestsReason::SandboxService);
}

fn should_proxy_ecw_service_port_request(ecw_hosts: &EcwHostConfig, headers: &HeaderMap) -> bool {
    ecw_hosts
        .parse_sandbox_host(headers)
        .is_some_and(|target| target.port != ECW_ENVD_PORT)
}

#[derive(Debug)]
struct EcwSandboxServiceRouteRepair {
    close_request: SpacedCloseLoopbackBridgeRequest,
    reset_request: UnstableResetSandboxServiceRouteRequest,
}

fn ecw_sandbox_service_route_repair(
    target: &EcwSandboxHost,
    route_generation: u64,
    diagnostic_message: &str,
) -> EcwSandboxServiceRouteRepair {
    EcwSandboxServiceRouteRepair {
        close_request: SpacedCloseLoopbackBridgeRequest {
            sandbox_id: target.sandbox_id.to_string(),
            operation: Some(CloseLoopbackBridgeRequest { port: target.port }),
        },
        reset_request: UnstableResetSandboxServiceRouteRequest {
            route: Some(SandboxServiceRouteKey {
                sandbox_id: target.sandbox_id.to_string(),
                port: target.port,
                kind: SandboxServiceRouteKind::ServicePort as i32,
            }),
            route_generation,
            diagnostic_message: Some(diagnostic_message.to_string()),
        },
    }
}

async fn reset_ecw_sandbox_service_route(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    route: &EcwResolvedRoute,
    diagnostic_message: &str,
) {
    let repair =
        ecw_sandbox_service_route_repair(target, route.route_generation, diagnostic_message);

    apply_ecw_sandbox_service_route_repair(
        repair,
        ECW_PROXY_FAILURE_BRIDGE_CLOSE_TIMEOUT,
        |request| close_ecw_loopback_bridge_after_proxy_failure(state, target, route, request),
        |request| reset_ecw_sandbox_service_route_request(state, target, route, request),
    )
    .await;
}

fn schedule_ecw_sandbox_service_route_repair(
    state: &DataPlaneState,
    target: EcwSandboxHost,
    route: EcwResolvedRoute,
    diagnostic_message: String,
) {
    let repair_key = ecw_sandbox_service_route_repair_key(&target, route.route_generation);
    let now = tokio::time::Instant::now();
    match claim_ecw_sandbox_service_route_repair(
        &state.ecw_route_repairs,
        repair_key,
        now,
        ECW_PROXY_FAILURE_ROUTE_REPAIR_COOLDOWN,
    ) {
        EcwRouteRepairSchedule::Enqueue => {}
        EcwRouteRepairSchedule::Coalesce { cooldown_remaining } => {
            pplx_space_metrics::data_plane::route_repair(
                RouteRepairSurface::Ecw,
                RouteRepairResult::Coalesced,
            );
            tracing::debug!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                cooldown_remaining_ms = cooldown_remaining.as_millis(),
                "coalesced ECW sandbox service route repair after proxy failure",
            );
            return;
        }
    }

    pplx_space_metrics::data_plane::route_repair(
        RouteRepairSurface::Ecw,
        RouteRepairResult::Enqueued,
    );
    let state = state.clone();
    let repair_tasks = state.upgrade_tasks.clone();
    repair_tasks.spawn(async move {
        reset_ecw_sandbox_service_route(&state, &target, &route, diagnostic_message.as_str()).await;
        pplx_space_metrics::data_plane::route_repair(
            RouteRepairSurface::Ecw,
            RouteRepairResult::Completed,
        );
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcwRouteRepairSchedule {
    Enqueue,
    Coalesce { cooldown_remaining: Duration },
}

fn claim_ecw_sandbox_service_route_repair(
    route_repairs: &DashMap<String, tokio::time::Instant>,
    repair_key: String,
    now: tokio::time::Instant,
    cooldown: Duration,
) -> EcwRouteRepairSchedule {
    route_repairs.retain(|_, next_allowed_at| *next_allowed_at > now);
    match route_repairs.entry(repair_key) {
        Entry::Occupied(mut entry) => {
            let next_allowed_at = *entry.get();
            if next_allowed_at > now {
                return EcwRouteRepairSchedule::Coalesce {
                    cooldown_remaining: next_allowed_at.saturating_duration_since(now),
                };
            }
            entry.insert(now + cooldown);
            EcwRouteRepairSchedule::Enqueue
        }
        Entry::Vacant(entry) => {
            entry.insert(now + cooldown);
            EcwRouteRepairSchedule::Enqueue
        }
    }
}

fn ecw_sandbox_service_route_repair_key(target: &EcwSandboxHost, route_generation: u64) -> String {
    format!("{}:{}:{route_generation}", target.sandbox_id, target.port)
}

async fn apply_ecw_sandbox_service_route_repair<Close, CloseFuture, Reset, ResetFuture>(
    repair: EcwSandboxServiceRouteRepair,
    bridge_close_timeout: Duration,
    close_loopback_bridge: Close,
    reset_service_route: Reset,
) where
    Close: FnOnce(SpacedCloseLoopbackBridgeRequest) -> CloseFuture,
    CloseFuture: Future,
    Reset: FnOnce(UnstableResetSandboxServiceRouteRequest) -> ResetFuture,
    ResetFuture: Future<Output = ()>,
{
    let close_request = repair.close_request;
    let sandbox_id = close_request.sandbox_id.clone();
    let port = close_request
        .operation
        .as_ref()
        .map(|operation| operation.port)
        .unwrap_or_default();
    match tokio::time::timeout(bridge_close_timeout, close_loopback_bridge(close_request)).await {
        Ok(_) => {}
        Err(_) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                port,
                route_generation = repair.reset_request.route_generation,
                timeout_ms = bridge_close_timeout.as_millis(),
                "timed out closing ECW loopback bridge after proxy failure"
            );
        }
    }
    reset_service_route(repair.reset_request).await;
}

async fn reset_ecw_sandbox_service_route_request(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    route: &EcwResolvedRoute,
    request: UnstableResetSandboxServiceRouteRequest,
) {
    let mut client = state.cluster_api.for_sandbox(&target.sandbox_id);
    match client.unstable_reset_sandbox_service_route(request).await {
        Ok(response) => {
            let response = response.into_inner();
            tracing::debug!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                result = response.result,
                reconcile_enqueued = response.reconcile_enqueued,
                "reset ECW sandbox service route after proxy failure",
            );
        }
        Err(error) => {
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                %error,
                "failed to reset ECW sandbox service route after proxy failure",
            );
        }
    }
}

async fn close_ecw_loopback_bridge_after_proxy_failure(
    state: &DataPlaneState,
    target: &EcwSandboxHost,
    route: &EcwResolvedRoute,
    request: SpacedCloseLoopbackBridgeRequest,
) {
    let channel = match ecw_backend_channel(state, route).await {
        Ok(channel) => channel,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                ?error,
                "failed to resolve backend for ECW loopback bridge close after proxy failure"
            );
            return;
        }
    };
    let mut client = WorkerApiClient::new(wrap_grpc_client_channel(channel));
    let request = match node_api_request_with_route_token_state(
        request,
        route.space_api_route_token.as_ref(),
    ) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                %error,
                "failed to build ECW loopback bridge close request after proxy failure"
            );
            return;
        }
    };

    match client.spaced_close_loopback_bridge(request).await {
        Ok(_) => {
            tracing::debug!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                "closed ECW loopback bridge after proxy failure"
            );
        }
        Err(error) => {
            tracing::warn!(
                sandbox_id = %target.sandbox_id,
                port = target.port,
                route_generation = route.route_generation,
                %error,
                "failed to close ECW loopback bridge after proxy failure"
            );
        }
    }
}

fn rewrite_ecw_service_ingress_request(request: &mut Request<Body>) -> Result<(), EcwProxyError> {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    *request.uri_mut() = path_and_query
        .parse::<Uri>()
        .map_err(|_| EcwProxyError::internal("Invalid sandbox service target"))?;
    strip_ecw_service_auth_headers(request.headers_mut());
    Ok(())
}

fn strip_ecw_service_auth_headers(headers: &mut HeaderMap) {
    for header in ECW_SERVICE_PROXY_STRIPPED_SPACE_HEADERS {
        headers.remove(*header);
    }
}

fn optional_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests;
