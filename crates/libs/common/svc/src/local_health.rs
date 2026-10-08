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
//! - `"liveness"` returns `SERVING` as long as the process can respond to the
//!   RPC. Daemons may opt in to forwarding liveness to the production socket
//!   after activation. A slow active-startup keeps liveness green so the
//!   kubelet does not SIGKILL the instance mid-recovery.
//!
//! Generic across daemons: each service supplies its own logging name via
//! [`local_health_pair`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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
const LIVENESS_SERVICE: &str = "liveness";
const READINESS_SERVICE: &str = "readiness";

pub struct LocalHealth {
    service_name: &'static str,
    liveness: LocalHealthLiveness,
    readiness_withheld: Arc<AtomicBool>,
    production_socket: Arc<OnceLock<PathBuf>>,
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
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
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
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
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
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
        let listener = bind_unix_socket(socket_path, socket_options)?;
        let (local_health, activator) =
            local_health_pair_with_readiness(service_name, liveness, readiness);
        let handle = tokio::spawn(async move {
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
        Ok(Self::Enabled { activator, handle })
    }

    /// Spawns local health only when `socket_path` is present.
    pub fn spawn_optional(
        socket_path: Option<&Path>,
        socket_options: UnixSocketOptions,
        service_name: &'static str,
        shutdown_token: CancellationToken,
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
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
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
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
    ) -> Result<Self, crate::bootstrap::BindUnixSocketError> {
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

    /// Awaits the server's exit. A join error (panic in the spawned task)
    /// is logged but not propagated — the caller is mid-shutdown and has
    /// no useful recovery.
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
