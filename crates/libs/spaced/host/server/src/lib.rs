//! Host-side authenticated routing between callers, control plane, and spaced guests.
//!
//! This service owns channel attestation, generation-aware invalidation, request
//! budgets, and backpressure—not guest execution. Wire messages are validated and
//! forwarded without rebuilding a second domain model. Legacy guest fallbacks
//! are isolated in compatibility modules with versioned removal markers.
//!
//! This crate is the whole service. The `space-spaced-host-svc` binary is a
//! thin `main` that parses flags, installs telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_common_connection_admission::ConnectionAdmissionFailure;
use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use pplx_space_common_svc::{UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal};
use pplx_space_spaced_common::proto::bridge_guest::filesystem_client::FilesystemClient as BridgeGuestFilesystemClient;
use pplx_space_spaced_common::proto::bridge_guest::meta_client::MetaClient as BridgeGuestClient;
use pplx_space_spaced_common::proto::bridge_guest::networking_client::NetworkingClient as BridgeGuestNetworkingClient;
use pplx_space_spaced_common::proto::bridge_guest::process_client::ProcessClient as BridgeGuestProcessClient;
use pplx_space_spaced_common::proto::bridge_guest::{
    ConfigureProcessEnvRequest as BridgeGuestConfigureProcessEnvRequest,
    DisableFileChangesRequest as BridgeGuestDisableFileChangesRequest,
    FlushFilesystemsRequest as BridgeGuestFlushFilesystemsRequest,
    GetModifiedFilesRequest as BridgeGuestGetModifiedFilesRequest,
    GetWatcherEventsRequest as BridgeGuestGetWatcherEventsRequest,
    MkdirRequest as BridgeGuestMkdirRequest, NotifyResumeRequest as BridgeGuestNotifyResumeRequest,
    PingRequest as BridgeGuestPingRequest, ReadRequest as BridgeGuestReadRequest,
    RemoveRequest as BridgeGuestRemoveRequest,
    RemoveWatcherRequest as BridgeGuestRemoveWatcherRequest,
    RenameRequest as BridgeGuestRenameRequest, WaitReadyRequest as BridgeGuestWaitReadyRequest,
    WaitReadyResponse as BridgeGuestWaitReadyResponse,
};
use pplx_space_spaced_common::proto::bridge_host::meta_server::{
    Meta as BridgeHostMeta, MetaServer as BridgeHostMetaServer,
};
use pplx_space_spaced_common::proto::bridge_host::{
    ActivityState as BridgeHostActivityState,
    RequestActiveForRequest as BridgeHostRequestActiveForRequest,
    RequestActiveForResponse as BridgeHostRequestActiveForResponse,
    RequestShutdownRequest as BridgeHostShutdownRequest,
    RequestShutdownResponse as BridgeHostShutdownResponse,
    SetActivityStateRequest as BridgeHostSetActivityStateRequest,
    SetActivityStateResponse as BridgeHostSetActivityStateResponse,
};
pub use pplx_space_spaced_common::proto::host as proto;
use pplx_space_spaced_common::proto::host_control::host_control_client::HostControlClient;
use pplx_space_spaced_common::proto::host_control::{
    RequestActiveForRequest as HostControlRequestActiveForRequest,
    SetActivityStateRequest as HostControlSetActivityStateRequest,
};
use pplx_space_spaced_common::{
    DEFAULT_COMMAND_RETRY_POLICY, LazyClient, RetryContext, RetryPolicy, invalid_argument_status,
    parse_env, parse_globs, proto_duration, proto_env, proto_globs, validate_duration,
    validate_optional_duration, validate_optional_timeout,
};
use proto::filesystem_server::{Filesystem, FilesystemServer};
use proto::meta_server::{Meta, MetaServer};
use proto::networking_server::{Networking, NetworkingServer};
use proto::process_server::{Process, ProcessServer};
use proto::{
    CloseStdinRequest, CloseStdinResponse, ConfigureProcessEnvRequest, ConfigureProcessEnvResponse,
    ConnectProcessRequest, CreateWatcherRequest, CreateWatcherResponse, DisableFileChangesRequest,
    DisableFileChangesResponse, EnableFileChangesRequest, EnableFileChangesResponse, FileChunk,
    FileEvent, FileInfo, FlushGuestFilesystemsRequest, FlushGuestFilesystemsResponse,
    GetModifiedFilesRequest, GetModifiedFilesResponse, GetStatusRequest, GetStatusResponse,
    GetWatcherEventsRequest, GetWatcherEventsResponse, IsConnectedRequest, IsConnectedResponse,
    KillRequest, KillResponse, ListProcessesRequest, ListRequest, MkdirRequest, MkdirResponse,
    NormalizeWatchRootRequest, NormalizeWatchRootResponse, NotifyResumeRequest,
    NotifyResumeResponse, PingGuestRequest, PingGuestResponse, PingRequest, PingResponse,
    PrepareSuspendRequest, PrepareSuspendResponse, ProcessEvent, ProcessInfo, ReadRequest,
    RemoveRequest, RemoveResponse, RemoveWatcherRequest, RemoveWatcherResponse, RenameRequest,
    RenameResponse, ResizePtyRequest, ResizePtyResponse, SendInputRequest, SendInputResponse,
    SendSignalRequest, SendSignalResponse, StartRequest, StatRequest, StatResponse,
    WaitReadyRequest, WaitReadyResponse, WatchRequest, WriteBatchRequest, WriteBatchResponse,
    WriteRequest, WriteResponse,
};
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
mod authorization;
mod bridge;
mod compat;
mod filesystem;
mod meta;
mod networking;
mod process;
mod protocol;
mod run;

pub use run::{RunSpacedHostError, SERVICE_NAME, SpacedHostBootType, SpacedHostRunConfig, run};

const SPACE_SBXMGR_ACCOUNT: &str = user_names::SPACE_SBXMGR;
const GUEST_BRIDGE_MAX_MESSAGE_SIZE: usize = 16 * 1024;
const GUEST_SIGNAL_RATE_LIMIT: usize = 5;
const GUEST_SIGNAL_RATE_WINDOW: Duration = Duration::from_secs(1);
const MAX_GUEST_SHUTDOWN_REASON_BYTES: usize = 1024;
const ALLOW_ROOT_OR_SBXMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
];

#[derive(Clone, Debug)]
pub enum BridgeGuestConnector {
    Uds(PathBuf),
    FirecrackerVsock {
        vsock_uds_path: PathBuf,
        port: u32,
        expected_peer: pplx_space_spaced_common::ExpectedUdsPeer,
    },
}

impl Default for BridgeGuestConnector {
    fn default() -> Self {
        Self::Uds(pplx_space_spaced_common::default_bridge_guest_uds_path())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpacedHostPeerAuthorization {
    Enforce,
    #[cfg(any(test, feature = "test-support"))]
    Disabled,
}

#[derive(Debug, thiserror::Error)]
pub enum RunSpacedHostServerError {
    #[error("failed to bind spaced-host unix socket: {0}")]
    Bind(#[from] pplx_space_spaced_common::BindUdsError),
    #[error("spaced-host gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
    #[error(transparent)]
    Admission(#[from] ConnectionAdmissionFailure),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectBridgeGuestError {
    #[error(transparent)]
    Uds(#[from] pplx_space_spaced_common::UdsConnectError),
    #[error(transparent)]
    FirecrackerVsock(#[from] pplx_space_spaced_common::FirecrackerVsockConnectError),
}

#[derive(Clone, Debug)]
pub struct SpacedHostServiceConfig {
    pub socket_path: PathBuf,
    pub bridge_host_socket_path: PathBuf,
    pub bridge_guest_connector: BridgeGuestConnector,
    pub host_control_socket_path: PathBuf,
    pub host_peer_authorization: SpacedHostPeerAuthorization,
}

impl Default for SpacedHostServiceConfig {
    fn default() -> Self {
        Self {
            socket_path: pplx_space_common_core::host::linux::paths::default_host_uds_path(
                pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE,
            ),
            bridge_host_socket_path: pplx_space_spaced_common::default_bridge_host_uds_path(),
            bridge_guest_connector: BridgeGuestConnector::default(),
            host_control_socket_path: PathBuf::from(
                pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET,
            ),
            host_peer_authorization: SpacedHostPeerAuthorization::Enforce,
        }
    }
}

#[derive(Clone)]
pub struct SpacedHostService {
    inner: Arc<SpacedHostServiceInner>,
}

impl std::fmt::Debug for SpacedHostService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpacedHostService")
            .field("shutting_down", &self.inner.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct GuestReadySnapshot {
    guest_uptime: Option<prost_types::Duration>,
    version: String,
    compatibility_family: String,
    mem_total_bytes: Option<u64>,
    vcpu_count: Option<u32>,
}

#[derive(Debug, Default)]
struct GuestSignalRateState {
    arrivals: VecDeque<Instant>,
    last_warning: Option<Instant>,
}

struct GuestReadinessCoordinator {
    snapshot: tokio::sync::watch::Sender<Option<GuestReadySnapshot>>,
    /// Exactly one request drives bridge-guest readiness polling. Other
    /// requests subscribe to `snapshot`; cancellation drops this permit and
    /// lets another live request take over without leaving background work.
    driver: tokio::sync::Semaphore,
}

impl GuestReadinessCoordinator {
    fn new() -> Self {
        let (snapshot, _) = tokio::sync::watch::channel(None);
        Self {
            snapshot,
            driver: tokio::sync::Semaphore::new(1),
        }
    }
}

struct SpacedHostServiceInner {
    shutdown: CancellationToken,
    socket_path: PathBuf,
    bridge_host_socket_path: PathBuf,
    bridge_host_expected_peer: Option<pplx_space_spaced_common::ExpectedUdsPeer>,
    bridge_guest_connector: BridgeGuestConnector,
    host_control_socket_path: PathBuf,
    bridge_guest_meta_client: LazyClient<BridgeGuestClient<tonic::transport::Channel>>,
    bridge_guest_filesystem_client:
        LazyClient<BridgeGuestFilesystemClient<tonic::transport::Channel>>,
    bridge_guest_process_client: LazyClient<BridgeGuestProcessClient<tonic::transport::Channel>>,
    bridge_guest_networking_client:
        LazyClient<BridgeGuestNetworkingClient<tonic::transport::Channel>>,
    host_control_client: LazyClient<HostControlClient<tonic::transport::Channel>>,
    bridge_retry_policy: RetryPolicy,
    /// Payload budgets of the process and filesystem services, shared by
    /// every listener that serves them.
    process_admission: pplx_space_common_grpc::QueuedSlots,
    filesystem_admission: pplx_space_common_grpc::QueuedSlots,
    guest_readiness: GuestReadinessCoordinator,
    guest_signal_rate: std::sync::Mutex<GuestSignalRateState>,
    #[cfg(any(test, feature = "test-support"))]
    host_peer_authorization: SpacedHostPeerAuthorization,
    host_peer_authorizer: UdsPeerAuthorizer,
}

/// `slots` payload slots with the shared bounded queue in front of them.
fn payload_admission(slots: usize) -> pplx_space_common_grpc::QueuedSlots {
    pplx_space_common_grpc::QueuedSlots::new(Arc::new(tokio::sync::Semaphore::new(slots)))
        .with_queue(
            pplx_space_spaced_common::admission_queue::WAITERS,
            pplx_space_spaced_common::admission_queue::MAX_WAIT,
        )
}
