//! The build manager's process entry point: the queue driver and reaper
//! background tasks, the TCP gRPC surface, and the drain that lets in-flight
//! template builds finish before the process exits.
//!
//! The `space-buildmgr-svc` binary is a thin `main` that parses flags,
//! initializes telemetry, and calls [`run`], so a deployment that needs
//! private wiring can supply its own `main` against this library without
//! copying any of the service.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use pplx_space_build_mgr_client::GRPC_MESSAGE_SIZE_LIMIT_BYTES;
use pplx_space_build_mgr_client::proto::build_manager_server::BuildManagerServer;
use pplx_space_common_svc::auth::build::BuildAuthLayer;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;

use crate::{BuildManagerDaemonService, NewServiceError};

/// The service name build-manager reports to telemetry with.
pub const SERVICE_NAME: &str = "space-build-manager";

/// Where the daemon listens when the host does not say otherwise. Loopback
/// only: node-api is the sole caller and it runs on the same node.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:9900";

/// How long shutdown waits for running builds before exiting with builds
/// still in flight. Template builds are long, so the default is generous.
pub const DEFAULT_DRAIN_TIMEOUT_SECONDS: u64 = 2 * 60 * 60;

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. The internal bearer token stays with the code that
/// reads it from the environment.
#[derive(Debug, Clone)]
pub struct BuildMgrRunConfig {
    /// Address the build-manager gRPC surface listens on.
    pub listen: SocketAddr,
    /// How many builds may run concurrently on this node. Clamped to at least
    /// one, so a zero here still makes progress.
    pub max_concurrent_builds: usize,
    /// Build-manager's SQLite database. A `String` because that is what
    /// [`BuildManagerDaemonService::new`] takes; its parent directory is
    /// created on startup.
    pub db_path: String,
    /// How long shutdown waits for running builds before giving up on them.
    pub drain_timeout: Duration,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunBuildMgrError {
    #[error(transparent)]
    InternalTransport(#[from] pplx_space_common_grpc::internal_transport::TransportError),
    #[error("failed to create the build-manager db directory {path}: {source}")]
    CreateDbDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize the build-manager service: {0}")]
    InitService(#[source] NewServiceError),
    /// The static internal bearer token the gRPC interceptor requires is
    /// missing or malformed. Carries the `tonic::Status` message, which names
    /// the environment variable at fault.
    #[error("failed to resolve the build-manager internal bearer token: {message}")]
    AuthToken { message: String },
    #[error("failed to configure build-manager authentication: {0}")]
    AuthConfig(#[from] pplx_space_common_svc::auth::build::BuildAuthError),
    #[error("failed to bind the build-manager TCP listener {listen}: {source}")]
    BindListener {
        listen: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serve the build-manager TCP gRPC surface: {0}")]
    Serve(#[source] tonic::transport::Error),
}

/// Run the build manager until `shutdown` is cancelled and the drain settles.
///
/// The caller owns the Tokio runtime, telemetry, and the shutdown signal.
pub async fn run(
    config: BuildMgrRunConfig,
    shutdown: CancellationToken,
) -> Result<(), RunBuildMgrError> {
    // Validate listener identity before starting durable work or background tasks.
    let server = pplx_space_common_grpc::internal_transport::server_builder()?
        .timeout(pplx_space_common_svc::bootstrap::GRPC_REQUEST_TIMEOUT)
        .http2_keepalive_interval(Some(
            pplx_space_common_svc::bootstrap::GRPC_HTTP2_KEEPALIVE_INTERVAL,
        ))
        .http2_keepalive_timeout(Some(
            pplx_space_common_svc::bootstrap::GRPC_HTTP2_KEEPALIVE_TIMEOUT,
        ));
    if let Some(parent) = std::path::Path::new(&config.db_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|source| RunBuildMgrError::CreateDbDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    tracing::info!(
        listen = %config.listen,
        db_path = %config.db_path,
        max_concurrent_builds = config.max_concurrent_builds,
        drain_timeout_seconds = config.drain_timeout.as_secs(),
        "space-buildmgr-svc listening"
    );

    let service =
        BuildManagerDaemonService::new(&config.db_path, config.max_concurrent_builds.max(1))
            .map_err(RunBuildMgrError::InitService)?;
    let auth_token = pplx_space_common_svc::auth::required_bearer_token(
        pplx_space_common_svc::auth::SPACE_INTERNAL_TOKEN_ENV,
    )
    .map_err(|status| RunBuildMgrError::AuthToken {
        message: status.message().to_owned(),
    })?;
    let auth_layer = BuildAuthLayer::for_build_manager(auth_token).await?;
    service.recover_jobs();
    let background_token = CancellationToken::new();
    let queue_driver = tokio::spawn(service.clone().run_queue_driver(background_token.clone()));
    let queue_metrics = tokio::spawn(service.clone().run_queue_metrics(background_token.clone()));
    let expired_lease_reaper = tokio::spawn(
        service
            .clone()
            .run_expired_lease_reaper(background_token.clone()),
    );
    let terminal_job_pruner = tokio::spawn(
        service
            .clone()
            .run_terminal_job_pruner(background_token.clone()),
    );
    let blob_reaper = tokio::spawn(service.clone().run_blob_reaper(background_token.clone()));
    let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
    health_reporter
        .set_serving::<BuildManagerServer<BuildManagerDaemonService>>()
        .await;
    let drain_timeout = config.drain_timeout;

    let listener_shutdown = CancellationToken::new();
    let drain = {
        let listener_shutdown = listener_shutdown.clone();
        let service = service.clone();
        let health_reporter = health_reporter.clone();
        async move {
            shutdown.cancelled().await;

            service.start_draining();
            // Drain background jobs first: stop the queue driver and reapers
            // from picking up new work, wait for in-flight builds to settle,
            // then join the background tasks. The gRPC surface stays up the
            // whole time, so this future returns (letting tonic drain
            // in-flight RPCs) only after background work is fully drained.
            background_token.cancel();
            health_reporter
                .set_not_serving::<BuildManagerServer<BuildManagerDaemonService>>()
                .await;

            let running_builds = service.local_running_build_count();
            let drain_started_at = std::time::Instant::now();
            tracing::info!(
                running_builds,
                drain_timeout_seconds = drain_timeout.as_secs(),
                "build-manager draining before shutdown"
            );

            let drained = service.wait_for_running_builds(drain_timeout).await;
            if drained {
                pplx_space_metrics::template::build_drain_completed(
                    pplx_space_metrics::template::TemplateBuildDrainResult::Completed,
                    drain_started_at,
                );
                tracing::info!("build-manager drain completed");
            } else {
                pplx_space_metrics::template::build_drain_completed(
                    pplx_space_metrics::template::TemplateBuildDrainResult::Timeout,
                    drain_started_at,
                );
                tracing::warn!(
                    running_builds = service.local_running_build_count(),
                    drain_timeout_seconds = drain_timeout.as_secs(),
                    "build-manager drain timed out; shutting down with builds still running"
                );
            }

            let _ = queue_driver.await;
            if let Err(error) = queue_metrics.await {
                tracing::warn!(%error, "build-manager queue metrics task failed");
            }
            let _ = expired_lease_reaper.await;
            let _ = terminal_job_pruner.await;
            let _ = blob_reaper.await;
            listener_shutdown.cancel();
        }
    };

    let listener = TcpListener::bind(config.listen).await.map_err(|source| {
        RunBuildMgrError::BindListener {
            listen: config.listen,
            source,
        }
    })?;

    let build_manager_server = BuildManagerServer::new(service)
        .max_decoding_message_size(GRPC_MESSAGE_SIZE_LIMIT_BYTES)
        .max_encoding_message_size(GRPC_MESSAGE_SIZE_LIMIT_BYTES);
    server
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .layer(auth_layer)
        .add_service(health_service)
        .add_service(build_manager_server)
        .serve_with_incoming_shutdown(
            pplx_space_common_grpc::internal_transport::Incoming::new(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                listener_shutdown,
            ),
            drain,
        )
        .await
        .map_err(RunBuildMgrError::Serve)?;

    Ok(())
}
