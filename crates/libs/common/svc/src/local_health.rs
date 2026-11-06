//! Instance-local gRPC health service for SPACE daemons.
//!
//! Bound on a process-private UDS that is created *before* the service mutex
//! is acquired, so external readiness checks can pass while this instance is
//! still passive waiting on the mutex. The service splits readiness and
//! liveness onto two gRPC service names so the two probes can report
//! independent signals:
//!
//! - `""` (default) and `"readiness"` report `NotServing` while an optional
//!   dependency wait is pending, then follow the ownership lifecycle:
//!   `Passive` → `SERVING` (ready to take over), `Starting` → `NotServing`
//!   (mutex acquired, running migrations/recovery, production socket not yet
//!   bound), `Active(path)` → forwards a fresh `Check` to the production
//!   socket and returns whatever it answers.
//! - `"liveness"` returns `SERVING` while the process can respond to the RPC
//!   and the caller's runtime has run its heartbeat task within
//!   [`MAIN_RUNTIME_STALL_LIMIT`]. Daemons may opt in to forwarding liveness
//!   to the production socket after activation. A slow active-startup keeps
//!   liveness green so the kubelet does not SIGKILL the instance mid-recovery.
//!
//! [`LocalHealthServer`] serves on a dedicated OS thread with its own
//! single-threaded Tokio runtime. Probes therefore get answered while the
//! daemon's main runtime is saturated or its workers are blocked in
//! synchronous calls; only a main-runtime stall longer than
//! [`MAIN_RUNTIME_STALL_LIMIT`] turns liveness to `NOT_SERVING`.
//!
//! Generic across daemons: each service supplies its own logging name via
//! [`local_health_pair`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use pplx_space_common_grpc::connect_uds_channel;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use crate::bootstrap::{
    UnixSocketOptions, bind_unix_socket, grpc_server_builder, grpc_trace_layer,
};

const ACTIVE_PROBE_TIMEOUT: Duration = Duration::from_millis(800);
/// How often the caller's runtime records its heartbeat.
const MAIN_RUNTIME_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Liveness reports `NOT_SERVING` once the caller's runtime has not run its
/// heartbeat task for this long. The kubelet's `failureThreshold` decides how
/// long a stall must continue before a restart, so this only needs to clear
/// the heartbeat interval and brief scheduling delays.
pub const MAIN_RUNTIME_STALL_LIMIT: Duration = Duration::from_secs(10);
const HEALTH_THREAD_NAME: &str = "local-health";
const LIVENESS_SERVICE: &str = "liveness";
const READINESS_SERVICE: &str = "readiness";

pub struct LocalHealth {
    service_name: &'static str,
    liveness: LocalHealthLiveness,
    readiness_withheld: Arc<AtomicBool>,
    production_socket: Arc<OnceLock<PathBuf>>,
    main_runtime: Option<Arc<RuntimeHeartbeat>>,
}

/// The last time the daemon's main runtime ran its heartbeat task, as
/// milliseconds since `origin`.
#[derive(Debug)]
struct RuntimeHeartbeat {
    origin: Instant,
    last_beat_ms: AtomicU64,
}

impl RuntimeHeartbeat {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            last_beat_ms: AtomicU64::new(0),
        }
    }

    fn beat(&self) {
        self.beat_at(Instant::now());
    }

    fn beat_at(&self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.origin);
        let millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.last_beat_ms.fetch_max(millis, Ordering::Relaxed);
    }

    fn stalled_for(&self, now: Instant) -> Duration {
        let last = self.origin + Duration::from_millis(self.last_beat_ms.load(Ordering::Relaxed));
        now.saturating_duration_since(last)
    }

    /// Records a heartbeat every [`MAIN_RUNTIME_HEARTBEAT_INTERVAL`] on the
    /// runtime that awaits it, until `shutdown_token` is cancelled.
    async fn run(self: Arc<Self>, shutdown_token: CancellationToken) {
        let mut interval = tokio::time::interval(MAIN_RUNTIME_HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => return,
                _ = interval.tick() => self.beat(),
            }
        }
    }
}

/// Why [`LocalHealthServer`] could not start.
#[derive(Debug, thiserror::Error)]
pub enum LocalHealthServerError {
    #[error(transparent)]
    Bind(#[from] crate::bootstrap::BindUnixSocketError),
    #[error("failed to start the local health thread: {0}")]
    StartThread(#[source] std::io::Error),
}

/// Selects what the local liveness endpoint reports after activation.
#[derive(Clone, Copy, Debug, Default)]
pub enum LocalHealthLiveness {
    /// Report the local health process only.
    #[default]
    Process,
    /// Forward liveness to the production socket after activation.
    ProductionWhenActive,
}

/// Drives the local health service through its lifecycle:
/// `Waiting` (opt-in) → `Passive` (via [`mark_takeover_ready`](Self::mark_takeover_ready))
/// → `Starting` (via [`mark_starting`](Self::mark_starting))
/// → `Active` (via [`activate`](Self::activate)).
#[derive(Clone)]
pub struct LocalHealthActivator {
    readiness_withheld: Arc<AtomicBool>,
    production_socket: Arc<OnceLock<PathBuf>>,
}

impl LocalHealthActivator {
    /// Allows takeover after dependency checks, before acquiring ownership.
    pub fn mark_takeover_ready(&self) {
        self.readiness_withheld.store(false, Ordering::SeqCst);
    }

    /// Records that this instance has acquired the service mutex and is
    /// running active-startup work (migrations, crash recovery, …) but has
    /// not yet bound its production socket. Readiness probes return
    /// `NotServing` from this point until [`activate`](Self::activate) is
    /// called.
    pub fn mark_starting(&self) {
        if self.readiness_withheld.swap(true, Ordering::SeqCst) {
            tracing::warn!("local health: mark_starting called twice");
        }
    }

    /// Records that this instance has bound its production socket. Readiness
    /// probes from this point forward forward a `Check` to that socket on
    /// every call.
    pub fn activate(&self, production_socket: PathBuf) {
        if self.production_socket.set(production_socket).is_err() {
            tracing::warn!("local health service already activated");
        }
    }
}

/// Builds an instance-local health server and its lifecycle activator.
///
/// `service_name` is the daemon's canonical identifier (e.g. the value passed
/// to [`crate::init_service`]). It flows into log lines and into the gRPC
/// client that probes the production socket so attribution stays correct
/// when several daemons share the binary.
pub fn local_health_pair(service_name: &'static str) -> (LocalHealth, LocalHealthActivator) {
    local_health_pair_with_liveness(service_name, LocalHealthLiveness::Process)
}

fn local_health_pair_with_liveness(
    service_name: &'static str,
    liveness: LocalHealthLiveness,
) -> (LocalHealth, LocalHealthActivator) {
    local_health_pair_with_readiness(service_name, liveness, InitialReadiness::Passive)
}

#[derive(Clone, Copy)]
enum InitialReadiness {
    Passive,
    Waiting,
}

fn local_health_pair_with_readiness(
    service_name: &'static str,
    liveness: LocalHealthLiveness,
    readiness: InitialReadiness,
) -> (LocalHealth, LocalHealthActivator) {
    let readiness_withheld = Arc::new(AtomicBool::new(matches!(
        readiness,
        InitialReadiness::Waiting
    )));
    let cell = Arc::new(OnceLock::new());
    (
        LocalHealth {
            service_name,
            liveness,
            readiness_withheld: Arc::clone(&readiness_withheld),
            production_socket: Arc::clone(&cell),
            main_runtime: None,
        },
        LocalHealthActivator {
            readiness_withheld,
            production_socket: cell,
        },
    )
}

/// One-call setup for the per-instance health socket: binds the UDS, spawns
/// the tonic Health server, and returns the activator + a join handle that
/// resolves once the server has exited.
///
/// The server stops when `shutdown_token` is cancelled. Drop the
/// [`LocalHealthServer`] or call [`join`](Self::join) to wait for in-flight
/// probes to finish.
pub enum LocalHealthServer {
    Enabled {
        activator: LocalHealthActivator,
        handle: JoinHandle<()>,
    },
    Disabled,
}

impl LocalHealthServer {
    /// Returns a disabled local health server. Lifecycle calls become no-ops.
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Binds `socket_path` and spawns the gRPC health server bound to it.
    pub fn spawn(
        socket_path: &Path,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        Self::spawn_with_liveness(
            socket_path,
            socket_options,
            service_name,
            LocalHealthLiveness::Process,
            shutdown_token,
        )
    }

    /// Binds `socket_path` and selects the active liveness behavior.
    pub fn spawn_with_liveness(
        socket_path: &Path,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        liveness: LocalHealthLiveness,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        Self::spawn_with_readiness(
            socket_path,
            socket_options,
            service_name,
            liveness,
            InitialReadiness::Passive,
            shutdown_token,
        )
    }

    fn spawn_with_readiness(
        socket_path: &Path,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        liveness: LocalHealthLiveness,
        readiness: InitialReadiness,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        // The listener is moved to the health thread's runtime, so it leaves
        // the caller's reactor as a std listener.
        let listener = bind_unix_socket(socket_path, socket_options)?
            .into_std()
            .map_err(LocalHealthServerError::StartThread)?;
        let (mut local_health, activator) =
            local_health_pair_with_readiness(service_name, liveness, readiness);
        let heartbeat = Arc::new(RuntimeHeartbeat::new());
        heartbeat.beat();
        local_health.main_runtime = Some(Arc::clone(&heartbeat));
        tokio::spawn(Arc::clone(&heartbeat).run(shutdown_token.clone()));

        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel::<()>();
        std::thread::Builder::new()
            .name(HEALTH_THREAD_NAME.to_owned())
            .spawn(move || {
                serve_on_health_thread(service_name, local_health, listener, shutdown_token);
                let _ = exited_tx.send(());
            })
            .map_err(LocalHealthServerError::StartThread)?;
        let handle = tokio::spawn(async move {
            if exited_rx.await.is_err() {
                tracing::error!(
                    service = service_name,
                    "local health thread ended without reporting its exit"
                );
            }
        });
        Ok(Self::Enabled { activator, handle })
    }

    /// Spawns local health only when `socket_path` is present.
    pub fn spawn_optional(
        socket_path: Option<&Path>,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        Self::spawn_optional_with_liveness(
            socket_path,
            socket_options,
            service_name,
            LocalHealthLiveness::Process,
            shutdown_token,
        )
    }

    /// Spawns local health with selected active liveness when `socket_path` is present.
    pub fn spawn_optional_with_liveness(
        socket_path: Option<&Path>,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        liveness: LocalHealthLiveness,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        match socket_path {
            Some(path) => Self::spawn_with_liveness(
                path,
                socket_options,
                service_name,
                liveness,
                shutdown_token,
            ),
            None => Ok(Self::disabled()),
        }
    }

    /// Starts with liveness available and takeover readiness withheld until
    /// [`mark_takeover_ready`](Self::mark_takeover_ready). The initial state is
    /// installed before serving, so no probe can authorize premature takeover.
    pub fn spawn_optional_waiting(
        socket_path: Option<&Path>,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        shutdown_token: CancellationToken,
    ) -> Result<Self, LocalHealthServerError> {
        match socket_path {
            Some(path) => Self::spawn_with_readiness(
                path,
                socket_options,
                service_name,
                LocalHealthLiveness::Process,
                InitialReadiness::Waiting,
                shutdown_token,
            ),
            None => Ok(Self::disabled()),
        }
    }

    /// See [`LocalHealthActivator::mark_takeover_ready`].
    pub fn mark_takeover_ready(&self) {
        if let Self::Enabled { activator, .. } = self {
            activator.mark_takeover_ready();
        }
    }

    /// See [`LocalHealthActivator::mark_starting`].
    pub fn mark_starting(&self) {
        if let Self::Enabled { activator, .. } = self {
            activator.mark_starting();
        }
    }

    /// See [`LocalHealthActivator::activate`].
    pub fn activate(&self, production_socket: PathBuf) {
        if let Self::Enabled { activator, .. } = self {
            activator.activate(production_socket);
        }
    }

    /// Awaits the health thread's exit after `shutdown_token` is cancelled.
    /// A health thread that ends without reporting (for example a panic) is
    /// logged but not propagated: the caller is mid-shutdown and has no
    /// useful recovery.
    pub async fn join(self) {
        if let Self::Enabled { handle, .. } = self {
            match handle.await {
                Ok(()) => {}
                Err(error) => {
                    tracing::warn!(%error, "local health server task ended unexpectedly");
                }
            }
        }
    }
}

/// Runs the health gRPC server on the calling thread's own single-threaded
/// runtime until `shutdown_token` is cancelled.
fn serve_on_health_thread(
    service_name: &'static str,
    local_health: LocalHealth,
    listener: std::os::unix::net::UnixListener,
    shutdown_token: CancellationToken,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(
                service = service_name,
                %error,
                "local health runtime could not be built"
            );
            return;
        }
    };
    runtime.block_on(async move {
        let listener = match tokio::net::UnixListener::from_std(listener) {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(
                    service = service_name,
                    %error,
                    "local health listener could not be registered"
                );
                return;
            }
        };
        let result = grpc_server_builder()
            .layer(grpc_trace_layer())
            .add_service(HealthServer::new(local_health))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                shutdown_token.cancelled().await
            })
            .await;
        if let Err(error) = result {
            tracing::error!(
                service = service_name,
                %error,
                "local health server exited with error"
            );
        }
    });
}

impl LocalHealth {
    async fn production_status(&self, socket: &Path) -> ServingStatus {
        match probe_production(self.service_name, socket).await {
            Ok(status) => status,
            Err(error) => {
                tracing::warn!(
                    service = self.service_name,
                    socket = %socket.display(),
                    %error,
                    "local health: production socket probe failed"
                );
                ServingStatus::NotServing
            }
        }
    }

    async fn readiness_status(&self) -> ServingStatus {
        if let Some(socket) = self.production_socket.get() {
            return self.production_status(socket).await;
        }
        if self.readiness_withheld.load(Ordering::SeqCst) {
            ServingStatus::NotServing
        } else {
            ServingStatus::Serving
        }
    }

    async fn liveness_status(&self) -> ServingStatus {
        if let Some(heartbeat) = &self.main_runtime {
            let stalled_for = heartbeat.stalled_for(Instant::now());
            if stalled_for > MAIN_RUNTIME_STALL_LIMIT {
                tracing::warn!(
                    service = self.service_name,
                    stalled_ms = u64::try_from(stalled_for.as_millis()).unwrap_or(u64::MAX),
                    "local health: main runtime heartbeat is stale"
                );
                return ServingStatus::NotServing;
            }
        }
        match (self.liveness, self.production_socket.get()) {
            (LocalHealthLiveness::ProductionWhenActive, Some(socket)) => {
                self.production_status(socket).await
            }
            _ => ServingStatus::Serving,
        }
    }
}

#[tonic::async_trait]
impl Health for LocalHealth {
    type WatchStream = ReceiverStream<Result<HealthCheckResponse, Status>>;

    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let probed = request.into_inner().service;
        let status = match probed.as_str() {
            "" | READINESS_SERVICE => self.readiness_status().await,
            LIVENESS_SERVICE => self.liveness_status().await,
            other => {
                return Err(Status::not_found(format!(
                    "local health does not expose service '{other}'"
                )));
            }
        };
        Ok(Response::new(HealthCheckResponse {
            status: status as i32,
        }))
    }

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented(
            "Watch is not implemented on the local health service",
        ))
    }
}

async fn probe_production(
    service_name: &'static str,
    path: &Path,
) -> Result<ServingStatus, ProbeError> {
    let channel = tokio::time::timeout(
        ACTIVE_PROBE_TIMEOUT,
        connect_uds_channel(service_name, path),
    )
    .await
    .map_err(|_| ProbeError::ConnectTimeout)?
    .map_err(|source| ProbeError::Connect {
        source: Box::new(source),
    })?;

    let mut client = HealthClient::new(channel);
    let response = tokio::time::timeout(
        ACTIVE_PROBE_TIMEOUT,
        client.check(HealthCheckRequest {
            service: String::new(),
        }),
    )
    .await
    .map_err(|_| ProbeError::CheckTimeout)?
    .map_err(ProbeError::Status)?;

    Ok(ServingStatus::try_from(response.into_inner().status).unwrap_or(ServingStatus::Unknown))
}

#[derive(Debug, thiserror::Error)]
enum ProbeError {
    #[error("connect to production socket timed out")]
    ConnectTimeout,
    #[error("connect to production socket failed: {source}")]
    Connect {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("production health check timed out")]
    CheckTimeout,
    #[error("production health check returned status: {0}")]
    Status(#[source] tonic::Status),
}

#[cfg(test)]
mod tests;
