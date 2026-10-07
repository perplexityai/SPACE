//! The template manager service: the node-local daemon that owns the template
//! catalog, the rootfs cache, warm pools, and the durable background jobs that
//! materialize templates onto the node.
//!
//! This module is the whole service. The `space-tmplmgr-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use std::path::PathBuf;
use std::time::Duration;

use pplx_space_common_svc::core::host::linux::identities::group_names;
use pplx_space_common_svc::{
    AcquireOutcome, DEFAULT_DRAIN_TIMEOUT, LocalHealthServer, ServiceMutex,
};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;

use crate::db::DB_PATH;
use crate::proto::template_manager_server::TemplateManagerServer;
use crate::service::TemplateManagerService;

/// The service name the template manager reports to telemetry and names its
/// health and rate-limit surfaces with.
pub const SERVICE_NAME: &str = "space-template-manager";
const SOCKET_GROUP: &str = group_names::SPACE_TMPLCLIENTS;
const BACKGROUND_WORKER_DRAIN_TIMEOUT: Duration = Duration::from_secs(20);
// Hard-coded so two instances on the same host cannot be configured to use
// different mutex files and both acquire concurrently.
const SERVICE_MUTEX_PATH: &str = "/space/run/template-manager/service.mutex";

static COMMON_SCHEMA_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../../db/common");
static TEMPLATE_MANAGER_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("../../../../db/template-manager");

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Environment-driven tuning (cache budget, prefetch
/// tags, node endpoints) stays with the code that reads it.
#[derive(Debug, Clone)]
pub struct TemplateMgrRunConfig {
    /// Production template-manager Unix socket, bound only after the service
    /// mutex is acquired.
    pub socket_path: PathBuf,
    /// Instance-local health Unix socket, bound from startup so readiness can
    /// pass while this instance is still passive.
    pub health_socket_path: Option<PathBuf>,
    /// How long the passive instance waits for the predecessor to release the
    /// service mutex before exiting non-zero.
    pub service_mutex_timeout: Duration,
}

/// Why [`run`] returned without a clean shutdown. Every variant is a startup
/// failure: once the production socket is bound the service only logs, so
/// there is no type-erased I/O variant here.
#[derive(Debug, thiserror::Error)]
pub enum RunTemplateMgrError {
    #[error("failed to bind the template-manager health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to acquire the template-manager service mutex: {0}")]
    ServiceMutex(#[source] pplx_space_common_svc::service_mutex::AcquireError),
    #[error("failed to migrate the template-manager SQLite database: {0}")]
    Migrate(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("failed to open the template-manager SQLite database: {0}")]
    OpenDatabase(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("failed to initialize template-manager service state: {0}")]
    InitializeService(#[source] rusqlite::Error),
    #[error("failed to bind the template-manager socket: {0}")]
    BindSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
}

struct ServiceRuntime {
    // Fields drop in declaration order: stop runtime-owned work before a
    // successor can acquire ownership, including on errors and unwinding.
    runtime: tokio::runtime::Runtime,
    service_mutex: Option<ServiceMutex>,
}

/// Run from a synchronous host until `shutdown_token` fires or startup fails.
/// Telemetry must already be initialized by the caller. Flushes telemetry before
/// destroying the runtime, then releases service ownership, so detached recovery
/// work cannot outlive ownership on return or unwind.
pub fn run(
    config: TemplateMgrRunConfig,
    shutdown_token: CancellationToken,
    runtime: tokio::runtime::Runtime,
    telemetry: pplx_space_common_svc::TelemetryGuard,
) -> Result<(), RunTemplateMgrError> {
    let mut service = ServiceRuntime {
        runtime,
        service_mutex: None,
    };
    service.runtime.block_on(async {
        let result = run_service(config, shutdown_token, &mut service.service_mutex).await;
        pplx_space_common_svc::shutdown_telemetry(telemetry).await;
        result
    })
}

async fn run_service(
    config: TemplateMgrRunConfig,
    shutdown_token: CancellationToken,
    service_mutex: &mut Option<ServiceMutex>,
) -> Result<(), RunTemplateMgrError> {
    let health_server = LocalHealthServer::spawn_optional(
        config.health_socket_path.as_deref(),
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        shutdown_token.clone(),
    )
    .map_err(RunTemplateMgrError::HealthSocket)?;
    let health_socket = config
        .health_socket_path
        .as_ref()
        .map_or_else(|| "disabled".to_string(), |path| path.display().to_string());

    tracing::info!(
        health_socket = %health_socket,
        service_mutex = SERVICE_MUTEX_PATH,
        "space-tmplmgr passive; waiting for service mutex"
    );

    let acquired_mutex = match ServiceMutex::acquire_with_timeout(
        SERVICE_MUTEX_PATH,
        &shutdown_token,
        config.service_mutex_timeout,
    )
    .await
    .map_err(RunTemplateMgrError::ServiceMutex)?
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => {
            tracing::warn!("terminated while passive; exiting cleanly");
            health_server.join().await;
            return Ok(());
        }
    };

    let acquired_mutex = service_mutex.insert(acquired_mutex);
    tracing::info!(
        service_mutex = %acquired_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );
    health_server.mark_starting();

    pplx_space_common_svc::sql::sqlite::migrate_embedded(
        "template-manager",
        std::path::Path::new(DB_PATH),
        pplx_space_common_svc::sql::sqlite::EmbeddedMigrators {
            service: &TEMPLATE_MANAGER_MIGRATOR,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
    )
    .await
    .map_err(RunTemplateMgrError::Migrate)?;

    let conn = crate::db::open().map_err(RunTemplateMgrError::OpenDatabase)?;

    let service =
        TemplateManagerService::new(conn).map_err(RunTemplateMgrError::InitializeService)?;

    // Re-arm any durable background jobs that were in flight (i.e.
    // `status='running'`) when the previous tmplmgr exited. Without
    // this, a tmplmgr restart mid-job would leave the row stuck in
    // `running` forever (the worker won't pick up its own previous
    // run; only `recover_and_log` flips them back to `pending` for a
    // new attempt).
    let background_worker = service.background_worker();
    background_worker.recover_and_log("template-mgr.background_worker");

    let listener = pplx_space_common_svc::bind_unix_socket(
        &config.socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o770)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(RunTemplateMgrError::BindSocket)?;

    tracing::info!(
        socket_path = %config.socket_path.display(),
        "space-tmplmgr active; listening on production socket"
    );

    health_server.activate(config.socket_path.clone());

    // Background recovery work (prefetch loop, dirty-outbox syncer, startup
    // base-snapshot reconcile) starts only after the instance is active.
    // The eviction loop is wired to the daemon's shutdown token so it
    // exits cooperatively on SIGTERM; the prefetch loop and outbox
    // syncers don't yet accept a token and are dropped at process exit.
    service.spawn_startup_recovery(shutdown_token.clone());

    // Production server runs under its own token so it keeps accepting
    // traffic while the local health server drains on shutdown_token.
    let production_shutdown_token = CancellationToken::new();

    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    let production_shutdown = {
        let token = production_shutdown_token.clone();
        async move { token.cancelled().await }
    };
    let server_handle = tokio::spawn(
        pplx_space_common_svc::grpc_server_builder()
            .timeout(crate::GRPC_REQUEST_TIMEOUT)
            .layer(pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(SERVICE_NAME))
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .add_service(health_service)
            .add_service(TemplateManagerServer::new(service))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), production_shutdown),
    );

    shutdown_token.cancelled().await;

    production_shutdown_token.cancel();
    // Jobs do not need the serving RPC surface to finish. Keep their durable
    // completion writes inside the drain before joining the server.
    let worker_drain = background_worker.shutdown(BACKGROUND_WORKER_DRAIN_TIMEOUT);
    let finish_drain = async {
        let server_result = server_handle.await;
        health_server.join().await;
        if let Err(error) = server_result {
            tracing::warn!(%error, "template-manager gRPC server task ended unexpectedly");
        }
    };

    if !drain_service(worker_drain, finish_drain, DEFAULT_DRAIN_TIMEOUT).await {
        service_mutex
            .take()
            .expect("active template-manager holds service ownership")
            .hard_exit(1, "template-manager drain did not prove quiescence");
    }

    Ok(())
}

async fn drain_service(
    worker_drain: impl Future<Output = bool>,
    finish_drain: impl Future<Output = ()>,
    timeout: Duration,
) -> bool {
    let drain = async {
        if !worker_drain.await {
            tracing::error!("background worker drain did not prove executor quiescence");
            return false;
        }
        finish_drain.await;
        true
    };
    match tokio::time::timeout(timeout, drain).await {
        Ok(quiescent) => quiescent,
        Err(_) => {
            tracing::error!(
                drain_timeout_secs = timeout.as_secs(),
                "drain budget exhausted; exiting while retaining service ownership"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests;
