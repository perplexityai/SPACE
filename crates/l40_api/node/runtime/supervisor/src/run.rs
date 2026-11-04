use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pplx_space_node_runtime_core::proto::runtime_supervisor_server::RuntimeSupervisorServer;
use tokio_stream::wrappers::UnixListenerStream;

use crate::service::RuntimeSupervisorService;

/// The service name the runtime supervisor reports to telemetry and names its
/// systemd watchdog contract with.
pub const SERVICE_NAME: &str = "space-runtime-supervisor";

/// Group name that controls who can connect to the supervisor socket.
const SOCKET_GROUP: &str = "root";

/// Everything [`run`] needs from the process that hosts it: the binary's flags,
/// already parsed.
#[derive(Debug, Clone)]
pub struct RuntimeSupervisorRunConfig {
    /// Unix socket the supervisor service listens on.
    pub socket_path: PathBuf,
    /// When set, this process is a self-exec of the supervisor binary that
    /// serves one sidecar launch over this descriptor instead of running the
    /// service. See [`run`] for why the two modes cannot be separated by the
    /// caller.
    pub sidecar_launcher_fd: Option<RawFd>,
    /// How long one Firecracker jailer run may take before the launch fails.
    pub jailer_start_timeout: Duration,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunRuntimeSupervisorError {
    /// Launcher mode failed. The protocol reports the same string back to the
    /// requesting supervisor over the launcher socket before this returns.
    #[error("sidecar launcher failed: {0}")]
    SidecarLauncher(String),
    #[error("failed to configure the runtime-supervisor Tokio runtime: {0}")]
    BuildRuntime(#[source] std::io::Error),
    #[error("failed to initialize runtime-supervisor telemetry: {0}")]
    Telemetry(#[source] pplx_space_common_svc::ObservabilityError),
    #[error("failed to bind the runtime-supervisor socket {path}: {source}")]
    BindSocket {
        path: PathBuf,
        #[source]
        source: pplx_space_common_svc::bootstrap::BindUnixSocketError,
    },
    #[error("failed to start the tracked child reaper: {0}")]
    ChildReaper(#[source] std::io::Error),
    #[error("runtime-supervisor gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
}

/// Runs the runtime supervisor process to completion.
///
/// This owns the mode choice, the Tokio runtime, telemetry, and the shutdown
/// token, rather than leaving them to the caller as the other SPACE service
/// libraries do, because launcher mode has to be reached before any of them
/// exist: `prepare_sidecar_launch` joins the sandbox network namespace, drops
/// privileges, and replaces the process image, all of which are only correct in
/// a dedicated single-threaded process. A caller that built a multi-threaded
/// runtime or initialized telemetry first would corrupt every sidecar launch,
/// so no `main` should be in a position to get that ordering wrong.
pub fn run(config: RuntimeSupervisorRunConfig) -> Result<(), RunRuntimeSupervisorError> {
    if let Some(raw_fd) = config.sidecar_launcher_fd {
        return crate::sidecar_launcher::run(raw_fd)
            .map_err(RunRuntimeSupervisorError::SidecarLauncher);
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(RunRuntimeSupervisorError::BuildRuntime)?
        .block_on(run_service(config.socket_path, config.jailer_start_timeout))
}

#[cfg(test)]
mod tests;

async fn run_service(
    socket: PathBuf,
    jailer_start_timeout: Duration,
) -> Result<(), RunRuntimeSupervisorError> {
    let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)
        .map_err(RunRuntimeSupervisorError::Telemetry)?;
    let listener = pplx_space_common_svc::bind_unix_socket(
        Path::new(&socket),
        pplx_space_common_svc::UnixSocketOptions::new(0o600)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(|source| RunRuntimeSupervisorError::BindSocket {
        path: socket.clone(),
        source,
    })?;
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    let shutdown_token = pplx_space_common_svc::shutdown_token();
    let service = RuntimeSupervisorService::new(shutdown_token.clone())
        .with_jailer_start_timeout(jailer_start_timeout);
    service
        .spawn_tracked_child_reaper()
        .map_err(RunRuntimeSupervisorError::ChildReaper)?;

    tracing::info!(
        socket_path = %socket.display(),
        jailer_start_timeout_ms = jailer_start_timeout.as_millis() as u64,
        "space-runtime-supervisor listening"
    );
    let shutdown = async {
        shutdown_token.cancelled().await;
        tracing::warn!("space-runtime-supervisor shutting down");
    };
    let result = pplx_space_common_svc::grpc_server_builder()
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .add_service(health_service)
        .add_service(RuntimeSupervisorServer::new(service.clone()))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await;
    service.drain().await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    result.map_err(RunRuntimeSupervisorError::Serve)
}
