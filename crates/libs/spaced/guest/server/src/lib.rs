//! Guest execution services for processes, filesystems, watches, and port forwarding.
//!
//! Filesystem retry authority lives in the router; Linux identity workers isolate
//! credentials and failures. Direct execution preserves other-platform boundaries.
//! Caller disconnect does not cancel accepted process starts or owned mutations.
//! Service cancellation closes admission, terminates process groups, and drains
//! owned work for at most ten seconds. OS-blocked cleanup can outlive that budget.
//! Snapshot/resume does not cancel the service. Retry records are not durable
//! across daemon restart. Watch overflow requires rescan, never silent truncation.
//!
//! This crate is the whole service. The `spaced-svc` binary is a thin `main`
//! that parses flags and calls `run` from the `service-entrypoint` feature, so
//! a deployment that needs private wiring can supply its own `main` against
//! this library without copying any of the service.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

use pplx_space_common_connection_admission::{
    ConnectionAdmissionConfig, ConnectionAdmissionFailure, ConnectionAdmissionObservation,
    admit_connections,
};
use pplx_space_spaced_common::lazy_client::FailedGeneration;
use pplx_space_spaced_common::proto::bridge_guest::meta_server::{
    Meta as BridgeGuestMeta, MetaServer as BridgeGuestMetaServer,
};
use pplx_space_spaced_common::proto::bridge_guest::networking_server::NetworkingServer as BridgeGuestNetworkingServer;
use pplx_space_spaced_common::proto::bridge_guest::process_server::{Process, ProcessServer};
use pplx_space_spaced_common::proto::bridge_guest::{
    ClockSyncStatus, CloseStdinRequest, CloseStdinResponse, ConfigureProcessEnvRequest,
    ConfigureProcessEnvResponse, ConnectProcessRequest, FileChunk, FileInfo,
    FlushFilesystemsRequest, FlushFilesystemsResponse, GuestDiagnosticCallsite,
    ListProcessesRequest, NotifyResumeRequest, NotifyResumeResponse,
    PingRequest as BridgeGuestPingRequest, PingResponse as BridgeGuestPingResponse, ProcessEvent,
    ProcessInfo, PtySize, ResizePtyRequest, ResizePtyResponse, SendInputRequest, SendInputResponse,
    SendSignalRequest, SendSignalResponse, StartRequest,
    WaitReadyRequest as BridgeGuestWaitReadyRequest,
    WaitReadyResponse as BridgeGuestWaitReadyResponse,
};
use pplx_space_spaced_common::proto::bridge_host::meta_client::MetaClient as BridgeHostClient;
use pplx_space_spaced_common::proto::bridge_host::{
    ActivityState as BridgeHostActivityState,
    RequestActiveForRequest as BridgeHostRequestActiveForRequest,
    RequestActiveForResponse as BridgeHostRequestActiveForResponse,
    RequestShutdownRequest as BridgeHostShutdownRequest,
    RequestShutdownResponse as BridgeHostShutdownResponse,
    SetActivityStateRequest as BridgeHostSetActivityStateRequest,
    SetActivityStateResponse as BridgeHostSetActivityStateResponse,
};
use pplx_space_spaced_common::proto::guest::meta_server::{
    Meta as LocalMeta, MetaServer as LocalMetaServer,
};
use pplx_space_spaced_common::proto::guest::{
    ActivityState as LocalActivityState, PingRequest, PingResponse,
    RequestActiveForRequest as LocalRequestActiveForRequest,
    RequestActiveForResponse as LocalRequestActiveForResponse,
    RequestShutdownRequest as LocalShutdownRequest,
    RequestShutdownResponse as LocalShutdownResponse,
    SetActivityStateRequest as LocalSetActivityStateRequest,
    SetActivityStateResponse as LocalSetActivityStateResponse,
};
use pplx_space_spaced_common::{
    DEFAULT_COMMAND_RETRY_POLICY, LazyClient, RetryContext, RetryPolicy, invalid_argument_status,
    parse_env, proto_env, validate_duration, validate_optional_timeout,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use pplx_space_common_core::types::{DEFAULT_GUEST_USER, EnvMap, GuestUserName};
type GuestFileInfoStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileInfo, Status>> + Send>>;
type GuestFileChunkStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileChunk, Status>> + Send>>;
const IDEMPOTENCY_RECORD_TTL: Duration = Duration::from_secs(60);
const BRIDGE_HOST_CALL_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONCURRENT_BRIDGE_HOST_CALLS: usize = 32;
const MAX_PROCESS_TAG_BYTES: usize = 4 * 1024;
const LOCAL_GUEST_SOCKET_MODE: u32 = 0o666;
// Physical transports are admitted separately from RPC work. A guest process
// can keep an HTTP/2 connection idle without reaching a tower request layer,
// so these limits must leave descriptor capacity for process pipes, filesystem
// workers, port-forward listeners, and active forwarded connections.
const MAX_LOCAL_GUEST_CONNECTIONS: usize = 256;
const MAX_BRIDGE_GUEST_CONNECTIONS: usize = 64;
const MAX_LOCAL_GUEST_STREAMS: u32 = 64;
// Leave room for identity-worker watches and process/file streams without
// starving control RPCs sharing their HTTP/2 channel.
const MAX_BRIDGE_GUEST_STREAMS: u32 = 2048;
/// How long an accepted connection may hold its admission slot without
/// sending an HTTP/2 preface. A peer that never speaks HTTP/2 would otherwise
/// keep a slot for as long as its socket stays open.
pub const DEFAULT_GUEST_HTTP2_PREFACE_TIMEOUT: Duration = Duration::from_secs(5);
const MIN_NON_GRPC_DESCRIPTOR_RESERVE: usize = 2048;
// Must match spaced.service's LimitNOFILE in
// templates/firecracker/seed/rootfs-builder.sh.
const SPACED_GUEST_NOFILE_LIMIT: usize = 4096;
const _: () = assert!(
    MAX_LOCAL_GUEST_CONNECTIONS + MAX_BRIDGE_GUEST_CONNECTIONS + MIN_NON_GRPC_DESCRIPTOR_RESERVE
        <= SPACED_GUEST_NOFILE_LIMIT
);

// A silent long-running stream is healthy if its transport answers PINGs.
// Only dead peers expire; no request timeout or connection age is imposed.
fn guest_grpc_server_builder(max_streams: u32) -> pplx_space_common_grpc::GrpcServer {
    pplx_space_common_grpc::grpc_server_builder()
        // Bound incomplete requests before method-level admission/decoding.
        .max_concurrent_streams(max_streams)
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(5)))
}

fn observe_connection_admission(
    listener: &'static str,
    observation: ConnectionAdmissionObservation,
) {
    if !observation.is_diagnostic_sample() {
        return;
    }
    let event = observation.event();
    pplx_space_metrics::grpc::server_connection_admission(
        listener,
        event.as_str(),
        event.reason().map_or("none", |reason| reason.as_str()),
    );
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectBridgeHostError {
    #[error(transparent)]
    Uds(#[from] pplx_space_spaced_common::UdsConnectError),
    #[error(transparent)]
    Vsock(#[from] pplx_space_spaced_common::VsockConnectError),
}

#[derive(Debug, thiserror::Error)]
pub enum RunLocalGuestServerError {
    #[error("failed to bind local guest unix socket: {0}")]
    Bind(#[from] pplx_space_spaced_common::BindUdsError),
    #[error("local guest gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
    #[error(transparent)]
    Admission(#[from] ConnectionAdmissionFailure),
}

#[derive(Debug, thiserror::Error)]
pub enum RunBridgeGuestServerError {
    #[error("failed to bind bridge guest vsock: {0}")]
    BindVsock(#[source] std::io::Error),
    #[error(transparent)]
    BindUds(#[from] pplx_space_spaced_common::BindUdsError),
    #[error("bridge guest gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
    #[error(transparent)]
    Admission(#[from] ConnectionAdmissionFailure),
}

#[derive(Debug, thiserror::Error)]
pub enum RunGuestServerError {
    #[error("local guest server failed: {0}")]
    Local(#[source] RunLocalGuestServerError),
    #[error("bridge guest server failed: {0}")]
    Bridge(#[source] RunBridgeGuestServerError),
}

/// Failure while starting the optional filesystem worker subprocess mode.
#[derive(Debug, thiserror::Error)]
#[error("filesystem worker startup failed: {source}")]
pub struct RunFilesystemWorkerError {
    #[source]
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl RunFilesystemWorkerError {
    #[cfg(target_os = "linux")]
    fn new(source: filesystem::FilesystemWorkerError) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

mod cgroup;
mod filesystem;
mod guest_path;
mod idempotency;
mod identity;
mod local_admission;
#[cfg(target_os = "linux")]
mod memlock;
mod meta;
mod networking;
mod port_forward;
#[cfg(test)]
mod transport;

/// Internal diagnostics support; not part of the stable guest service API.
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::port_forward::LoopbackBridgeManager;
}
mod process;
#[cfg(feature = "service-entrypoint")]
mod run;

#[cfg(feature = "service-entrypoint")]
pub use run::{RunSpacedGuestError, SERVICE_NAME, SpacedGuestRunConfig, run, serve};

/// The service name the identity-isolated filesystem worker subprocess reports
/// to telemetry. Deliberately distinct from the daemon's own service name: the
/// worker emits service telemetry but does not own the systemd watchdog
/// contract.
///
/// The worker env builder pins this so an inherited OTel service name cannot
/// rename the child, so it resolves without `service-entrypoint`.
pub const WORKER_SERVICE_NAME: &str = "space-spaced-guest-worker";

use idempotency::IdempotencyStore;
use port_forward::LoopbackBridgeManager;
use process::ProcessRegistry;

const HOST_CONNECTION_PORT: u32 = pplx_space_spaced_common::BRIDGE_HOST_VSOCK_PORT;
const GUEST_BRIDGE_PORT: u32 = pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT;

/// A request refused before any of its work started: an admission limit, or
/// the guest shutting down. The status says so on the wire, with a retry hint,
/// so hops return it to the caller instead of retrying it, and the caller knows
/// this request did nothing.
pub(crate) fn refused(status: Status) -> Status {
    pplx_space_common_grpc::refused_before_start(status, REFUSAL_RETRY_AFTER)
}

// Admission slots free as in-flight requests finish; a second is a fair wait,
// and long enough for a restarting guest to come back.
const REFUSAL_RETRY_AFTER: Duration = Duration::from_secs(1);

pub(crate) fn native_vsock_available() -> bool {
    cfg!(target_os = "linux") && std::path::Path::new("/dev/vsock").exists()
}

#[cfg(target_os = "linux")]
fn read_guest_uptime() -> Option<prost_types::Duration> {
    let time = nix::time::clock_gettime(nix::time::ClockId::CLOCK_BOOTTIME).ok()?;
    if time.tv_sec() < 0 || time.tv_nsec() < 0 || time.tv_nsec() >= 1_000_000_000 {
        return None;
    }

    Some(prost_types::Duration {
        seconds: time.tv_sec(),
        nanos: time.tv_nsec() as i32,
    })
}

#[cfg(not(target_os = "linux"))]
fn read_guest_uptime() -> Option<prost_types::Duration> {
    None
}

#[derive(Clone, Debug, Default)]
pub enum HostConnectorType {
    #[default]
    Auto,
    Uds(PathBuf),
    Vsock,
}

#[derive(Clone, Debug)]
pub struct SpacedGuestServiceConfig {
    pub host_connector_type: HostConnectorType,
    pub local_socket_path: Option<PathBuf>,
    pub bridge_socket_path: Option<PathBuf>,
    pub guest_uptime_source: fn() -> Option<prost_types::Duration>,
    pub default_operation_user: GuestUserName,
    pub filesystem_execution: FilesystemExecutionMode,
    /// See [`filesystem::router::DEFAULT_CANCELLATION_GRACE`]. Configurable so
    /// a test can observe the escalation without waiting out the deployed
    /// deadline; the deployed configuration never moves it.
    pub cancellation_grace: std::time::Duration,
    /// See [`DEFAULT_GUEST_HTTP2_PREFACE_TIMEOUT`]. Configurable so a test can
    /// observe a slot being recycled without waiting out the deployed
    /// deadline; the deployed configuration never moves it.
    pub http2_preface_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemExecutionMode {
    IdentityWorkers,
    Direct,
}

impl Default for SpacedGuestServiceConfig {
    fn default() -> Self {
        Self {
            host_connector_type: HostConnectorType::default(),
            local_socket_path: Some(
                pplx_space_common_core::guest::linux::paths::default_guest_uds_path(
                    pplx_space_common_core::guest::linux::paths::SPACED_GUEST_SOCKET_FILE,
                ),
            ),
            bridge_socket_path: Some(pplx_space_spaced_common::default_bridge_guest_uds_path()),
            guest_uptime_source: read_guest_uptime,
            default_operation_user: GuestUserName::parse(DEFAULT_GUEST_USER)
                .expect("default guest user constant is valid"),
            filesystem_execution: if cfg!(target_os = "linux") {
                FilesystemExecutionMode::IdentityWorkers
            } else {
                FilesystemExecutionMode::Direct
            },
            cancellation_grace: filesystem::router::DEFAULT_CANCELLATION_GRACE,
            http2_preface_timeout: DEFAULT_GUEST_HTTP2_PREFACE_TIMEOUT,
        }
    }
}

#[derive(Clone)]
pub struct SpacedGuestService {
    inner: Arc<SpacedGuestServiceInner>,
}

impl std::fmt::Debug for SpacedGuestService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpacedGuestService")
            .field("shutting_down", &self.inner.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

struct SpacedGuestServiceInner {
    shutdown: CancellationToken,
    process_tasks: tokio_util::task::TaskTracker,
    /// Orders process-start admission with shutdown. A start checks
    /// readiness, claims its process id, begins its idempotent operation and
    /// registers its owner in `process_tasks` under this gate, and shutdown
    /// closes `process_tasks` under it, so no admitted start escapes the
    /// drain. It is never held across a spawn: a spawn that stalls must not
    /// stall every other start.
    start_gate: std::sync::Mutex<()>,
    process_payload_slots: Arc<Semaphore>,
    filesystem_flush_slots: Arc<Semaphore>,
    host_connector_type: HostConnectorType,
    bridge_host_client: LazyClient<BridgeHostClient<tonic::transport::Channel>>,
    bridge_host_calls: Arc<Semaphore>,
    bridge_retry_policy: RetryPolicy,
    local_socket_path: Option<PathBuf>,
    bridge_socket_path: Option<PathBuf>,
    http2_preface_timeout: Duration,
    guest_uptime_source: fn() -> Option<prost_types::Duration>,
    default_operation_user: GuestUserName,
    ready: watch::Sender<bool>,
    ready_events: watch::Sender<u64>,
    process_env: RwLock<EnvMap>,
    processes: ProcessRegistry,
    idempotency: IdempotencyStore,
    filesystem: filesystem::FilesystemRouter,
    loopback_bridge: LoopbackBridgeManager,
    /// Random per-process token regenerated each time spaced-guest-svc
    /// starts. Surfaced in `OpenLoopbackBridgeResponse.service_generation` so
    /// host-side reconcilers can detect guest restarts (which drop the
    /// in-memory authorized port set) and re-apply port bridges.
    service_generation: String,
    /// Guest cgroup split that places spawned workloads in the workloads
    /// unit's leaf, with the rest of user work and away from the supervisor. `None` outside a real
    /// Firecracker guest or when cgroup setup degraded to the shared layout.
    guest_cgroups: Option<Arc<cgroup::GuestCgroups>>,
}

impl SpacedGuestService {
    pub fn new(shutdown: CancellationToken) -> Self {
        Self::with_config(shutdown, SpacedGuestServiceConfig::default())
    }

    pub fn with_config(shutdown: CancellationToken, config: SpacedGuestServiceConfig) -> Self {
        let (ready, _) = watch::channel(false);
        let (ready_events, _) = watch::channel(0);
        let loopback_bridge = LoopbackBridgeManager::new(shutdown.clone());
        let service_generation = uuid::Uuid::now_v7().to_string();
        let filesystem = filesystem::FilesystemRouter::new(
            config.filesystem_execution,
            config.default_operation_user.clone(),
            shutdown.clone(),
            config.cancellation_grace,
        );
        // Only manage cgroups inside the Firecracker microVM, identified by the
        // guest vsock transport device. Setup relocates this process and
        // rewrites cgroup limits, so it must never run against a host's
        // cgroups; with no `/dev/vsock` (any non-guest environment) it no-ops.
        // `run()` has normally done it already, before any large allocation
        // and before telemetry; its report is logged here, where telemetry is
        // up.
        let guest_cgroups = if native_vsock_available() {
            let cgroups = cgroup::GuestCgroups::initialize_for_process();
            cgroup::GuestCgroups::report_process_setup();
            #[cfg(target_os = "linux")]
            memlock::report();
            cgroups
        } else {
            None
        };
        Self {
            inner: Arc::new(SpacedGuestServiceInner {
                shutdown,
                process_tasks: tokio_util::task::TaskTracker::new(),
                start_gate: std::sync::Mutex::new(()),
                filesystem_flush_slots: Arc::new(Semaphore::new(1)),
                process_payload_slots: Arc::new(Semaphore::new(
                    pplx_space_spaced_common::process_limits::PAYLOAD_SLOTS,
                )),
                host_connector_type: config.host_connector_type,
                bridge_host_client: LazyClient::new(),
                bridge_host_calls: Arc::new(Semaphore::new(MAX_CONCURRENT_BRIDGE_HOST_CALLS)),
                bridge_retry_policy: DEFAULT_COMMAND_RETRY_POLICY,
                local_socket_path: config.local_socket_path,
                bridge_socket_path: config.bridge_socket_path,
                http2_preface_timeout: config.http2_preface_timeout,
                guest_uptime_source: config.guest_uptime_source,
                default_operation_user: config.default_operation_user,
                ready,
                ready_events,
                process_env: RwLock::new(EnvMap::empty()),
                processes: ProcessRegistry::new(process::ProcessOutputBudget::for_guest_memory(
                    meta::guest_mem_total_bytes(),
                )),
                idempotency: IdempotencyStore::new("process"),
                filesystem,
                loopback_bridge,
                service_generation,
                guest_cgroups,
            }),
        }
    }

    pub fn ready(&self) {
        self.inner.ready.send_replace(true);
        self.emit_ready_event();
    }

    /// Waits until the bridge-guest listener has bound, or returns `false` if
    /// service shutdown wins the race. The service binary uses this exact edge
    /// for its systemd readiness notification.
    pub async fn wait_until_listening(&self) -> bool {
        if *self.inner.ready.borrow() {
            return true;
        }
        let mut ready = self.inner.ready.subscribe();
        loop {
            if *ready.borrow_and_update() {
                return true;
            }
            tokio::select! {
                result = ready.changed() => {
                    if result.is_err() {
                        return false;
                    }
                }
                _ = self.inner.shutdown.cancelled() => return false,
            }
        }
    }

    fn ready_after_bridge_guest_listening(&self) {
        self.ready();
        tracing::info!("spaced guest bridge is listening; guest userland is ready");
    }

    fn require_ready(&self) -> Result<(), Status> {
        if self.inner.shutdown.is_cancelled() {
            return Err(crate::refused(Status::unavailable("guest shutting down")));
        }
        if *self.inner.ready.borrow() {
            Ok(())
        } else {
            Err(Status::failed_precondition("guest_not_ready"))
        }
    }

    pub async fn request_host_shutdown(
        &self,
        reason: Option<String>,
    ) -> Result<BridgeHostShutdownResponse, Status> {
        self.call_bridge_host("bridge_host_request_shutdown", |mut client| {
            let reason = reason.clone();
            async move {
                client
                    .request_shutdown(BridgeHostShutdownRequest { reason })
                    .await
            }
        })
        .await
    }

    pub async fn set_host_activity_state(
        &self,
        state: BridgeHostActivityState,
    ) -> Result<BridgeHostSetActivityStateResponse, Status> {
        self.call_bridge_host("bridge_host_set_activity_state", |mut client| async move {
            client
                .set_activity_state(BridgeHostSetActivityStateRequest {
                    state: state.into(),
                })
                .await
        })
        .await
    }

    pub async fn request_host_active_for(
        &self,
        hold_for: Duration,
    ) -> Result<BridgeHostRequestActiveForResponse, Status> {
        self.call_bridge_host("bridge_host_request_active_for", |mut client| async move {
            client
                .request_active_for(BridgeHostRequestActiveForRequest {
                    hold_for: Some(pplx_space_spaced_common::duration_to_proto(hold_for)?),
                })
                .await
        })
        .await
    }

    /// Stop admitting process starts. Closure is serialized with start
    /// admission, so every start admitted before it has its owner tracked in
    /// `process_tasks` and the drain waits for it.
    fn close_process_admission(&self) {
        let _start_gate = self
            .inner
            .start_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.process_tasks.close();
    }

    pub async fn run(self) -> Result<(), RunGuestServerError> {
        tracing::info!(
            guest_socket_path = ?self.inner.local_socket_path,
            bridge_guest_socket_path = ?self.inner.bridge_socket_path,
            "spaced guest service started"
        );
        // Resolve the loopback bridge's external bind IP off the request
        // path so the first OpenLoopbackBridge call doesn't pay for the
        // up-to-5 s autodetect retry loop on top of its own wait_timeout.
        self.inner.loopback_bridge.spawn_warm();
        let local_server = self.clone().run_local_server();
        let bridge_server = self.clone().run_bridge_guest_server();
        self.serve_until_shutdown(local_server, bridge_server).await
    }

    async fn serve_until_shutdown(
        &self,
        local_server: impl std::future::Future<Output = Result<(), RunLocalGuestServerError>>,
        bridge_server: impl std::future::Future<Output = Result<(), RunBridgeGuestServerError>>,
    ) -> Result<(), RunGuestServerError> {
        tokio::pin!(local_server);
        tokio::pin!(bridge_server);

        let shutdown = self.inner.shutdown.clone();
        let (mut result, local_done, bridge_done) = tokio::select! {
            result = &mut local_server => (result.map_err(RunGuestServerError::Local), true, false),
            result = &mut bridge_server => (result.map_err(RunGuestServerError::Bridge), false, true),
            _ = shutdown.cancelled() => (Ok(()), false, false),
        };
        shutdown.cancel();
        // Retain completed failures even if another owner exhausts the drain budget.
        let mut local_error = None;
        let mut bridge_error = None;
        let drain = async {
            self.close_process_admission();
            tokio::join!(
                self.inner.filesystem.drain(),
                self.inner.process_tasks.wait(),
                self.inner.loopback_bridge.shutdown(),
                async {
                    if !local_done {
                        local_error = local_server.await.err();
                    }
                },
                async {
                    if !bridge_done {
                        bridge_error = bridge_server.await.err();
                    }
                },
            );
        };
        if tokio::time::timeout(Duration::from_secs(10), drain)
            .await
            .is_err()
        {
            tracing::warn!("guest shutdown drain deadline exceeded; OS cleanup may remain pending");
        }
        for error in [
            local_error.map(RunGuestServerError::Local),
            bridge_error.map(RunGuestServerError::Bridge),
        ]
        .into_iter()
        .flatten()
        {
            tracing::warn!(%error, "guest listener failed while draining");
            if result.is_ok() {
                result = Err(error);
            }
        }

        tracing::info!("spaced guest service stopped");
        result
    }

    async fn connect_bridge_host(
        &self,
    ) -> Result<BridgeHostClient<tonic::transport::Channel>, ConnectBridgeHostError> {
        match &self.inner.host_connector_type {
            HostConnectorType::Auto if native_vsock_available() => {
                let channel = pplx_space_spaced_common::connect_vsock_channel(
                    "spaced bridge host",
                    tokio_vsock::VMADDR_CID_HOST,
                    HOST_CONNECTION_PORT,
                )
                .await?;
                Ok(BridgeHostClient::new(channel))
            }
            HostConnectorType::Auto => {
                let path = pplx_space_spaced_common::default_bridge_host_uds_path();
                let channel =
                    pplx_space_spaced_common::connect_uds_channel("spaced bridge host", &path)
                        .await?;
                Ok(BridgeHostClient::new(channel))
            }
            HostConnectorType::Uds(path) => {
                let channel =
                    pplx_space_spaced_common::connect_uds_channel("spaced bridge host", path)
                        .await?;
                Ok(BridgeHostClient::new(channel))
            }
            HostConnectorType::Vsock => {
                let channel = pplx_space_spaced_common::connect_vsock_channel(
                    "spaced bridge host",
                    tokio_vsock::VMADDR_CID_HOST,
                    HOST_CONNECTION_PORT,
                )
                .await?;
                Ok(BridgeHostClient::new(channel))
            }
        }
    }

    async fn call_bridge_host_response<T, F, Fut>(
        &self,
        command: &'static str,
        call: F,
    ) -> Result<Response<T>, Status>
    where
        F: Fn(BridgeHostClient<tonic::transport::Channel>) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<Response<T>, Status>>,
    {
        let _call_permit = self
            .inner
            .bridge_host_calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                crate::refused(Status::resource_exhausted(
                    "too many concurrent bridge-host calls",
                ))
            })?;
        let failed_generation = FailedGeneration::default();
        pplx_space_spaced_common::retry_status_with_context_and_retry_hook(
            "spaced bridge host",
            command,
            self.inner.bridge_retry_policy,
            RetryContext::with_timeout(BRIDGE_HOST_CALL_TIMEOUT)
                .with_cancellation(self.inner.shutdown.clone()),
            || {
                let call = call.clone();
                let failed_generation = &failed_generation;
                async move {
                    let lease = self
                        .inner
                        .bridge_host_client
                        .get_or_connect_with_generation(|| self.connect_bridge_host())
                        .await
                        .map_err(|error| Status::unavailable(error.to_string()))?;
                    failed_generation.set(lease.generation());
                    let response = call(lease.into_client()).await?;
                    failed_generation.clear();
                    self.inner.bridge_host_client.set_connected();
                    Ok(response)
                }
            },
            || async {
                failed_generation
                    .invalidate(&self.inner.bridge_host_client)
                    .await;
            },
        )
        .await
    }

    async fn call_bridge_host<T, F, Fut>(&self, command: &'static str, call: F) -> Result<T, Status>
    where
        F: Fn(BridgeHostClient<tonic::transport::Channel>) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<Response<T>, Status>>,
    {
        self.call_bridge_host_response(command, call)
            .await
            .map(Response::into_inner)
    }

    fn emit_ready_event(&self) {
        let next = self.inner.ready_events.borrow().wrapping_add(1);
        self.inner.ready_events.send_replace(next);
    }

    fn local_grpc_service(self) -> local_admission::LocalMetaAdmission {
        local_admission::LocalMetaAdmission::new(self)
    }

    async fn run_local_server(self) -> Result<(), RunLocalGuestServerError> {
        let Some(socket_path) = self.inner.local_socket_path.clone() else {
            self.inner.shutdown.cancelled().await;
            return Ok(());
        };
        let incoming = pplx_space_spaced_common::bind_uds_with_mode(
            &socket_path,
            Some(LOCAL_GUEST_SOCKET_MODE),
        )?;
        let preface_timeout = self.inner.http2_preface_timeout;
        let incoming = tokio_stream::StreamExt::map(incoming, move |io| {
            io.map(|io| pplx_space_common_grpc::PrefaceDeadline::new(io, preface_timeout))
        });
        let shutdown = self.inner.shutdown.clone();
        let admission = admit_connections(
            incoming,
            shutdown.clone(),
            ConnectionAdmissionConfig::new("spaced-guest-local", MAX_LOCAL_GUEST_CONNECTIONS),
            observe_connection_admission,
        );
        let result = admission
            .supervise(move |incoming| {
                guest_grpc_server_builder(MAX_LOCAL_GUEST_STREAMS)
                    .add_service(self.local_grpc_service())
                    .serve_with_incoming_shutdown(
                        incoming,
                        async move { shutdown.cancelled().await },
                    )
            })
            .await?;
        result.map_err(RunLocalGuestServerError::Serve)
    }

    async fn run_bridge_guest_server(self) -> Result<(), RunBridgeGuestServerError> {
        match self.inner.host_connector_type {
            HostConnectorType::Vsock | HostConnectorType::Auto if native_vsock_available() => {
                let incoming = pplx_space_spaced_common::bind_vsock(GUEST_BRIDGE_PORT)
                    .map_err(RunBridgeGuestServerError::BindVsock)?
                    .incoming();
                // Guest-local vsock must never reach privileged bridge RPCs,
                // including on kernels that enable VSOCKETS_LOOPBACK.
                let incoming =
                    tokio_stream::StreamExt::filter(incoming, |accepted| match accepted {
                        Ok(stream) => stream
                            .peer_addr()
                            .is_ok_and(|peer| peer.cid() == tokio_vsock::VMADDR_CID_HOST),
                        Err(_) => true,
                    });
                let preface_timeout = self.inner.http2_preface_timeout;
                let incoming = tokio_stream::StreamExt::map(incoming, move |io| {
                    io.map(|io| pplx_space_common_grpc::PrefaceDeadline::new(io, preface_timeout))
                });
                let admission = admit_connections(
                    incoming,
                    self.inner.shutdown.clone(),
                    ConnectionAdmissionConfig::new(
                        "spaced-guest-bridge-vsock",
                        MAX_BRIDGE_GUEST_CONNECTIONS,
                    ),
                    observe_connection_admission,
                );
                self.ready_after_bridge_guest_listening();
                let result = admission
                    .supervise(move |incoming| {
                        guest_grpc_server_builder(MAX_BRIDGE_GUEST_STREAMS)
                            .add_service(BridgeGuestMetaServer::new(self.clone()))
                            .add_service(self.inner.filesystem.grpc_service())
                            .add_service(self.process_grpc_service())
                            .add_service(BridgeGuestNetworkingServer::new(self.clone()))
                            .serve_with_incoming_shutdown(incoming, async move {
                                self.inner.shutdown.cancelled().await
                            })
                    })
                    .await?;
                result.map_err(RunBridgeGuestServerError::Serve)?;
                Ok(())
            }
            _ => {
                let Some(socket_path) = self.inner.bridge_socket_path.clone() else {
                    self.inner.shutdown.cancelled().await;
                    return Ok(());
                };
                let incoming =
                    pplx_space_spaced_common::bind_uds_with_mode(&socket_path, Some(0o600))?;
                let preface_timeout = self.inner.http2_preface_timeout;
                let incoming = tokio_stream::StreamExt::map(incoming, move |io| {
                    io.map(|io| pplx_space_common_grpc::PrefaceDeadline::new(io, preface_timeout))
                });
                let admission = admit_connections(
                    incoming,
                    self.inner.shutdown.clone(),
                    ConnectionAdmissionConfig::new(
                        "spaced-guest-bridge-uds",
                        MAX_BRIDGE_GUEST_CONNECTIONS,
                    ),
                    observe_connection_admission,
                );
                self.ready_after_bridge_guest_listening();
                let result = admission
                    .supervise(move |incoming| {
                        guest_grpc_server_builder(MAX_BRIDGE_GUEST_STREAMS)
                            .add_service(BridgeGuestMetaServer::new(self.clone()))
                            .add_service(self.inner.filesystem.grpc_service())
                            .add_service(self.process_grpc_service())
                            .add_service(BridgeGuestNetworkingServer::new(self.clone()))
                            .serve_with_incoming_shutdown(incoming, async move {
                                self.inner.shutdown.cancelled().await
                            })
                    })
                    .await?;
                result.map_err(RunBridgeGuestServerError::Serve)?;
                Ok(())
            }
        }
    }
}

pub fn run_filesystem_worker_from_environment() -> Result<bool, RunFilesystemWorkerError> {
    #[cfg(target_os = "linux")]
    {
        filesystem::run_worker_from_environment().map_err(RunFilesystemWorkerError::new)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }
}

#[cfg(target_os = "linux")]
const FILESYSTEM_WORKER_MODE_ENV: &str = "SPACE_SPACED_FILESYSTEM_WORKER";

pub fn filesystem_worker_requested() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::env::var_os(FILESYSTEM_WORKER_MODE_ENV).is_some()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}
