//! The volume-manager service itself: configuration, startup, and the
//! ownership-safe drain sequence.
//!
//! The public binary (`crates/apps/vol/mgr/svc`) is a thin `main` that parses
//! flags into a [`VolmgrRunConfig`] and calls [`run`]. Everything the service
//! does lives here so a deployment can supply its own `main` without copying
//! any of it.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use pplx_space_common_core::host::linux::identities::group_names;
use pplx_space_common_svc::bootstrap::BindUnixSocketError;
use pplx_space_common_svc::sql::sqlite::SqliteError;
use pplx_space_common_svc::{
    AcquireOutcome, LocalHealthLiveness, LocalHealthServer, SERVICE_MUTEX_POLL_INTERVAL,
    ServiceMutex, ServiceMutexAcquireError, UnixSocketOptions,
};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;

use crate::executor::TransferExecutorConfig;
use crate::proto::volume_manager_server::VolumeManagerServer;
use crate::service::{NewServiceError, VolumeManagerService, VolumeManagerServiceConfig};
use crate::{JobPriority, ObjectCredentials, PriorityBand};

pub const SERVICE_NAME: &str = "space-volume-manager";
const SOCKET_GROUP: &str = group_names::SPACE_SBXCLIENTS;
// Hard-coded so two instances on the same host cannot be configured to use
// different mutex files and both acquire concurrently.
pub const SERVICE_MUTEX_PATH: &str = "/space/run/volume-manager/service.mutex";
const UNEXPECTED_SERVER_EXIT_CODE: i32 = 1;

static VOLUME_MANAGER_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("../../../../../db/volume-manager");
static COMMON_SCHEMA_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../../../db/common");

pub(crate) async fn migrate_volume_manager_db(path: &Path) -> Result<(), SqliteError> {
    pplx_space_common_svc::sql::sqlite::migrate_embedded(
        "volume-manager",
        path,
        pplx_space_common_svc::sql::sqlite::EmbeddedMigrators {
            service: &VOLUME_MANAGER_MIGRATOR,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
    )
    .await
}

// Static S3 credential escape hatch, resolved here at the deployment boundary
// (not inside the platform library). When these are set — e.g. a local kind
// stack with its S3-compatible object store, where credmgr's STS/IRSA exchange isn't available — they are
// injected into the service and used directly instead of the credmgr exchange.
// Production leaves them unset and volume-manager uses credmgr as before.
const STATIC_ACCESS_KEY_ID_ENV: &str = "SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID";
const STATIC_SECRET_ACCESS_KEY_ENV: &str = "SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY";
/// Session token for temporary static credentials (STS, SSO). Without it an
/// `ASIA…` key is rejected by S3 as an unknown access key.
const STATIC_SESSION_TOKEN_ENV: &str = "SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN";
pub const DEFAULT_PUSH_CONCURRENCY: usize = 8;
pub const DEFAULT_PULL_CONCURRENCY: usize = 24;
pub const DEFAULT_CODEC_CONCURRENCY: usize = 0;
pub const DEFAULT_CODEC_PRIORITY_RESERVE: usize = 1;
pub const DEFAULT_TRANSFER_TIMEOUT_SECS: u64 = 1800;
pub const DEFAULT_TRANSFER_MAX_ATTEMPTS: i32 = 5;
pub const DEFAULT_SERVER_DRAIN_TIMEOUT_SECS: u64 = 10;
pub const DEFAULT_TRANSFER_DRAIN_TIMEOUT_SECS: u64 = 100;
pub const DEFAULT_SERVICE_MUTEX_TIMEOUT_SECS: u64 = 150;
const VOLMGR_GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// A `MIN_PRIORITY:CONCURRENCY:ACTIVE_JOBS` pull capacity band.
///
/// The newtype exists so the flag value can be parsed by `FromStr` — the
/// underlying [`PriorityBand`] is foreign to this crate. It lives here rather
/// than in the binary so the parse rules stay testable in the library.
#[derive(Clone, Copy, Debug)]
pub struct PullPriorityBand(pub PriorityBand);

#[derive(Debug, thiserror::Error)]
pub enum ParsePullPriorityBandError {
    #[error("expected MIN_PRIORITY:CONCURRENCY:ACTIVE_JOBS")]
    InvalidFormat,
    #[error("invalid minimum priority")]
    MinPriority(#[source] std::num::ParseIntError),
    #[error("invalid concurrency")]
    Concurrency(#[source] std::num::ParseIntError),
    #[error("invalid active-job limit")]
    ActiveJobs(#[source] std::num::ParseIntError),
    #[error("concurrency must be greater than zero")]
    ZeroConcurrency,
    #[error("active-job limit must be greater than zero")]
    ZeroActiveJobs,
}

#[derive(Debug, thiserror::Error)]
pub enum ValidatePullPriorityBandsError {
    #[error("duplicate pull priority band minimum {0}")]
    DuplicateMinimum(i32),
}

impl FromStr for PullPriorityBand {
    type Err = ParsePullPriorityBandError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let (Some(min_priority), Some(concurrency), Some(active_jobs), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(ParsePullPriorityBandError::InvalidFormat);
        };
        let min_priority = min_priority
            .parse()
            .map_err(ParsePullPriorityBandError::MinPriority)?;
        let concurrency = concurrency
            .parse()
            .map_err(ParsePullPriorityBandError::Concurrency)?;
        let active_jobs = active_jobs
            .parse()
            .map_err(ParsePullPriorityBandError::ActiveJobs)?;
        if concurrency == 0 {
            return Err(ParsePullPriorityBandError::ZeroConcurrency);
        }
        if active_jobs == 0 {
            return Err(ParsePullPriorityBandError::ZeroActiveJobs);
        }
        Ok(Self(PriorityBand::new(
            JobPriority::new(min_priority),
            concurrency,
            active_jobs,
        )))
    }
}

fn validate_pull_priority_bands(
    bands: &[PullPriorityBand],
) -> Result<Vec<PriorityBand>, ValidatePullPriorityBandsError> {
    let mut bands = bands.iter().map(|band| band.0).collect::<Vec<_>>();
    bands.sort_by_key(|band| band.min_priority);
    if let Some(duplicate) = bands.windows(2).find_map(|pair| match pair {
        [lower, upper] if lower.min_priority == upper.min_priority => Some(lower.min_priority),
        _ => None,
    }) {
        return Err(ValidatePullPriorityBandsError::DuplicateMinimum(
            duplicate.get(),
        ));
    }
    Ok(bands)
}

/// Build static object credentials from explicit values, or `None` when the
/// required pair is missing/empty. Pure so it can be unit-tested without
/// mutating process-global environment state.
fn build_static_credentials(
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
) -> Option<ObjectCredentials> {
    let access_key_id = access_key_id.filter(|v| !v.is_empty())?;
    let secret_access_key = secret_access_key.filter(|v| !v.is_empty())?;
    Some(ObjectCredentials {
        access_key_id,
        secret_access_key,
        session_token: session_token.filter(|v| !v.is_empty()),
    })
}

/// Resolve static S3 credentials from the deployment environment, or `None`.
fn static_credentials_from_env() -> Option<ObjectCredentials> {
    static_credentials_from(|name| std::env::var(name).ok())
}

/// Static credentials from `lookup`, which answers for the three documented
/// variables; the session token is optional and carried when present.
fn static_credentials_from(lookup: impl Fn(&str) -> Option<String>) -> Option<ObjectCredentials> {
    build_static_credentials(
        lookup(STATIC_ACCESS_KEY_ID_ENV),
        lookup(STATIC_SECRET_ACCESS_KEY_ENV),
        lookup(STATIC_SESSION_TOKEN_ENV),
    )
}

async fn acquire_service_mutex(
    path: &Path,
    shutdown_token: &CancellationToken,
    passive_timeout: Duration,
    health_server: &LocalHealthServer,
) -> Result<Option<ServiceMutex>, ServiceMutexAcquireError> {
    match ServiceMutex::acquire_with_timeout(path, shutdown_token, passive_timeout).await {
        Ok(AcquireOutcome::Acquired(mutex)) => {
            health_server.mark_starting();
            Ok(Some(mutex))
        }
        Ok(AcquireOutcome::PassiveExit) => Ok(None),
        Err(error @ ServiceMutexAcquireError::Timeout { .. }) => {
            health_server.mark_starting();
            tracing::error!(
                %error,
                "service mutex wait exceeded passive readiness budget; continuing NotReady"
            );
            match ServiceMutex::acquire(
                path,
                shutdown_token.clone(),
                SERVICE_MUTEX_POLL_INTERVAL,
                None,
            )
            .await
            {
                Ok(mutex) => Ok(Some(mutex)),
                Err(ServiceMutexAcquireError::Cancelled) => Ok(None),
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Everything the volume-manager service needs to run, already parsed.
///
/// The public binary builds this from its clap `Cli`; a private `main` can
/// build it directly. Durations are resolved here so the library never has to
/// re-interpret a flag's unit.
#[derive(Clone, Debug)]
pub struct VolmgrRunConfig {
    /// Unix socket path for the gRPC service. Bound only after the service
    /// mutex is acquired.
    pub socket_path: PathBuf,
    /// Instance-local health Unix socket. When supplied, it is bound before
    /// waiting for the service mutex so a surge successor is Ready while
    /// passive. It must live on a pod-private mount.
    pub health_socket_path: Option<PathBuf>,
    /// Maximum time to remain ready while waiting for the predecessor's
    /// service mutex. After this budget, the process becomes NotReady and
    /// continues waiting.
    pub service_mutex_timeout: Duration,
    /// SQLite database path for volume attachment and job state.
    pub db_path: String,
    /// Directory for in-flight push sources.
    pub staging_dir: PathBuf,
    /// Maximum concurrent S3 upload (push) tasks.
    pub push_concurrency: usize,
    /// Maximum concurrent S3 download (pull) tasks.
    pub pull_concurrency: usize,
    /// Maximum concurrent CPU-bound zstd passes. 0 auto-sizes to the CPU quota.
    pub codec_concurrency: usize,
    /// Codec permits reserved for jobs at or above the lowest pull priority band.
    pub codec_priority_reserve: usize,
    /// Process-wide high watermark for streamed output awaiting writeback.
    pub stream_writeback_budget_bytes: u64,
    /// Protected pull capacity bands. Duplicate minimums are rejected by [`run`].
    pub pull_priority_bands: Vec<PullPriorityBand>,
    /// Per-attempt timeout for a single transfer.
    pub transfer_timeout: Duration,
    /// Maximum retry attempts before a transfer job is permanently failed.
    pub transfer_max_attempts: i32,
    /// Push dependency outage retry budget, measured from transfer creation.
    pub dependency_retry_budget: Duration,
    /// How long to wait for in-flight RPCs after SIGTERM.
    pub server_drain_timeout: Duration,
    /// How long to wait for outstanding transfers before the gRPC server stops.
    /// Interrupted transfers remain persisted and are re-queued on startup.
    pub transfer_drain_timeout: Duration,
    /// Compress snapshot files with zstd before uploading to S3.
    pub compress: bool,
    /// Deployment upload policy for ordinary and LUKS snapshots.
    pub snapshot_upload_mode: crate::SnapshotUploadMode,
    /// Decode zstd pulls inline with the ranged download (one pass) instead of
    /// a separate read-back decompress step.
    pub stream_decode_pulls: bool,
}

/// Why the volume-manager service could not start, or could not finish
/// starting. Everything after the service is serving is handled by the drain
/// sequence, which logs and hard-exits rather than returning.
#[derive(Debug, thiserror::Error)]
pub enum RunVolmgrError {
    #[error("invalid pull priority bands")]
    PullPriorityBands(#[from] ValidatePullPriorityBandsError),
    #[error("create service mutex directory {path}")]
    CreateServiceMutexDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("bind local health socket")]
    BindHealthSocket(#[source] BindUnixSocketError),
    #[error("acquire service mutex {}", SERVICE_MUTEX_PATH)]
    AcquireServiceMutex(#[from] ServiceMutexAcquireError),
    #[error("create parent directory {path}")]
    CreateParentDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("apply embedded volume-manager migrations to {path}")]
    Migrate {
        path: PathBuf,
        #[source]
        source: SqliteError,
    },
    #[error("construct the volume-manager service")]
    NewService(#[from] NewServiceError),
    #[error("bind the volume-manager production socket {path}")]
    BindProductionSocket {
        path: PathBuf,
        #[source]
        source: BindUnixSocketError,
    },
}

async fn wait_for_shutdown_or_hard_exit<E>(
    shutdown_token: &CancellationToken,
    server_handle: &mut tokio::task::JoinHandle<Result<(), E>>,
    service_mutex: &mut Option<ServiceMutex>,
) where
    E: std::fmt::Display,
{
    tokio::select! {
        // If shutdown and server completion become ready together, ownership
        // safety takes precedence over the graceful-shutdown path. Otherwise
        // a failed or panicked server could be mistaken for a signal-driven
        // drain and release the flock through normal stack unwinding.
        biased;
        result = server_handle => {
            match result {
                Ok(Ok(())) => tracing::error!("volume-manager production server exited unexpectedly"),
                Ok(Err(error)) => tracing::error!(%error, "volume-manager production server failed unexpectedly"),
                Err(error) => tracing::error!(%error, "volume-manager production server task failed unexpectedly"),
            }
            // Recovery and transfer execution use detached Tokio tasks. A
            // normal return would drop the flock before the runtime cancels
            // those tasks, allowing a successor to recover the same queue
            // while predecessor work still exists. The kernel releases the
            // flock only after this process boundary has stopped every task.
            service_mutex
                .take()
                .expect("service mutex held until process boundary")
                .hard_exit(UNEXPECTED_SERVER_EXIT_CODE, "production server exited unexpectedly");
        }
        _ = shutdown_token.cancelled() => {
            tracing::info!("shutdown signal received; closing mutation admission");
        }
    }
}

async fn stop_production_server_or_hard_exit<E>(
    production_shutdown_token: &CancellationToken,
    server_handle: &mut tokio::task::JoinHandle<Result<(), E>>,
    server_drain_timeout: Duration,
    service_mutex: &mut Option<ServiceMutex>,
) where
    E: std::fmt::Display,
{
    production_shutdown_token.cancel();
    match tokio::time::timeout(server_drain_timeout, &mut *server_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            tracing::error!(%error, "volume-manager production server failed during shutdown");
            service_mutex
                .take()
                .expect("service mutex held until server stops")
                .hard_exit(
                    UNEXPECTED_SERVER_EXIT_CODE,
                    "production server failed during shutdown",
                );
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "volume-manager production server task failed during shutdown");
            service_mutex
                .take()
                .expect("service mutex held until server stops")
                .hard_exit(
                    UNEXPECTED_SERVER_EXIT_CODE,
                    "production server task failed during shutdown",
                );
        }
        Err(_) => {
            tracing::error!(
                server_drain_timeout_secs = server_drain_timeout.as_secs(),
                "gRPC server exceeded drain deadline; aborting before releasing service mutex"
            );
            // Abort and await so no detached server task survives ownership
            // release. A successor may publish the UDS only after this join.
            server_handle.abort();
            match server_handle.await {
                Err(error) if error.is_cancelled() => {}
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(%error, "volume-manager production server failed while being aborted");
                    service_mutex
                        .take()
                        .expect("service mutex held until server stops")
                        .hard_exit(
                            UNEXPECTED_SERVER_EXIT_CODE,
                            "production server failed while being aborted",
                        );
                }
                Err(error) => {
                    tracing::error!(%error, "volume-manager production server task failed while being aborted");
                    service_mutex
                        .take()
                        .expect("service mutex held until server stops")
                        .hard_exit(
                            UNEXPECTED_SERVER_EXIT_CODE,
                            "production server task failed while being aborted",
                        );
                }
            }
        }
    }
}

/// Run the volume-manager service until `shutdown_token` is cancelled and
/// every durable transfer has drained.
///
/// The caller owns telemetry, the Tokio runtime, and the shutdown token. On
/// an ownership-safety violation this never returns: it hard-exits the
/// process so the kernel releases the service mutex only after every detached
/// executor task is gone.
pub async fn run(
    config: VolmgrRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunVolmgrError> {
    let pull_priority_bands = validate_pull_priority_bands(&config.pull_priority_bands)?;
    // Jobs at or above the lowest configured band are "protected"; they may
    // draw on the reserved codec permits. No bands => no protection.
    let codec_priority_threshold = pull_priority_bands
        .iter()
        .map(|band| band.min_priority)
        .min();

    if let Some(parent) = std::path::Path::new(SERVICE_MUTEX_PATH).parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            RunVolmgrError::CreateServiceMutexDir {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }

    let health_server = LocalHealthServer::spawn_optional_with_liveness(
        config.health_socket_path.as_deref(),
        UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        LocalHealthLiveness::ProductionWhenActive,
        shutdown_token.clone(),
    )
    .map_err(RunVolmgrError::BindHealthSocket)?;
    tracing::info!(
        health_socket = %config.health_socket_path.as_ref().map_or_else(
            || "disabled".to_string(),
            |path| path.display().to_string(),
        ),
        service_mutex = SERVICE_MUTEX_PATH,
        "volume manager passive; waiting for service mutex"
    );
    let Some(service_mutex) = acquire_service_mutex(
        Path::new(SERVICE_MUTEX_PATH),
        &shutdown_token,
        config.service_mutex_timeout,
        &health_server,
    )
    .await?
    else {
        tracing::warn!("terminated while passive; exiting cleanly");
        health_server.join().await;
        return Ok(());
    };
    tracing::info!(
        service_mutex = %service_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );

    if let Some(parent) = config.socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| RunVolmgrError::CreateParentDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    if let Some(parent) = std::path::Path::new(&config.db_path).parent() {
        std::fs::create_dir_all(parent).map_err(|source| RunVolmgrError::CreateParentDir {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    migrate_volume_manager_db(std::path::Path::new(&config.db_path))
        .await
        .map_err(|source| RunVolmgrError::Migrate {
            path: PathBuf::from(&config.db_path),
            source,
        })?;

    let static_credentials = static_credentials_from_env();
    if static_credentials.is_some() {
        tracing::info!("using static S3 credentials from environment (credmgr exchange bypassed)");
    }

    let service = VolumeManagerService::new(
        &config.db_path,
        &config.staging_dir,
        VolumeManagerServiceConfig {
            push_concurrency: config.push_concurrency,
            pull_concurrency: config.pull_concurrency,
            pull_priority_bands,
            transfer_timeout: config.transfer_timeout,
            max_attempts: config.transfer_max_attempts,
            executor: TransferExecutorConfig {
                dependency_retry_budget: Some(config.dependency_retry_budget),
                compress: config.compress,
                snapshot_upload_mode: config.snapshot_upload_mode.clone(),
                stream_decode_pulls: config.stream_decode_pulls,
                codec_concurrency: config.codec_concurrency,
                codec_priority_reserve: config.codec_priority_reserve,
                codec_priority_threshold,
                stream_writeback_budget_bytes: config.stream_writeback_budget_bytes,
                static_credentials,
            },
        },
    )?;

    // Drop staging files that belong to jobs no longer in `pending` /
    // `running` (completed, permanently failed, or vanished from the DB
    // entirely). Must run BEFORE `worker.recover_and_log` so the keep-set
    // sees the same active-jobs snapshot the recover step will revive.
    match service.cleanup_staging_orphans() {
        Ok(removed) if removed > 0 => {
            tracing::info!(removed, "vol-mgr: cleaned up orphan staging files")
        }
        Ok(_) => {}
        Err(error) => tracing::warn!(%error, "vol-mgr: staging-orphan cleanup failed"),
    }

    let worker = service.worker();
    let shadow_comparisons = service.shadow_comparison_controller();
    let transfer_admission = service.transfer_admission();
    let service_for_drain = service.clone();
    worker.recover_and_log("vol-mgr.transfer");
    let source_cleanup_token = shutdown_token.child_token();
    let source_cleanup = tokio::spawn(
        service
            .clone()
            .run_btrfs_source_cleanup(source_cleanup_token.clone()),
    );
    let pruner_token = shutdown_token.child_token();
    let pruner_handle = tokio::spawn(
        worker
            .clone()
            .run_terminal_job_pruner("vol-mgr.transfer", pruner_token.clone()),
    );
    let queue_metrics_token = shutdown_token.child_token();
    let queue_metrics_handle = tokio::spawn(
        worker
            .clone()
            .run_queue_metrics(queue_metrics_token.clone()),
    );

    let listener = pplx_space_common_svc::bind_unix_socket(
        &config.socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o770)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(|source| RunVolmgrError::BindProductionSocket {
        path: config.socket_path.clone(),
        source,
    })?;
    tracing::info!(
        socket_path = %config.socket_path.display(),
        db_path = %config.db_path,
        push_concurrency = config.push_concurrency,
        pull_concurrency = config.pull_concurrency,
        codec_concurrency = config.codec_concurrency,
        codec_priority_reserve = config.codec_priority_reserve,
        codec_priority_threshold = ?codec_priority_threshold.map(JobPriority::get),
        stream_writeback_budget_bytes = config.stream_writeback_budget_bytes,
        pull_priority_bands = ?config.pull_priority_bands,
        transfer_timeout_secs = config.transfer_timeout.as_secs(),
        grpc_request_timeout_secs = VOLMGR_GRPC_REQUEST_TIMEOUT.as_secs(),
        server_drain_timeout_secs = config.server_drain_timeout.as_secs(),
        transfer_drain_timeout_secs = config.transfer_drain_timeout.as_secs(),
        compress = config.compress,
        stream_push_buffer_budget_bytes = config.snapshot_upload_mode.buffer_budget_bytes(),
        stream_decode_pulls = config.stream_decode_pulls,
        service_mutex_timeout_secs = config.service_mutex_timeout.as_secs(),
        "volume manager active; listening on production socket"
    );
    health_server.activate(config.socket_path.clone());
    let mut service_mutex = Some(service_mutex);

    // The production server owns a separate token. Process shutdown closes
    // transfer admission and drains durable work while the production UDS
    // remains available for polling/status RPCs. Only then is tonic stopped.
    let production_shutdown_token = CancellationToken::new();
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    let mut server_handle = tokio::spawn(
        pplx_space_common_svc::grpc_server_builder_with_request_timeout(
            VOLMGR_GRPC_REQUEST_TIMEOUT,
        )
        .layer(pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(SERVICE_NAME))
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .add_service(health_service)
        .add_service(VolumeManagerServer::new(service))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), {
            let token = production_shutdown_token.clone();
            async move { token.cancelled().await }
        }),
    );

    wait_for_shutdown_or_hard_exit(&shutdown_token, &mut server_handle, &mut service_mutex).await;

    transfer_admission.close();
    let transfer_drain_timeout = config.transfer_drain_timeout;
    let transfer_drain_deadline = tokio::time::Instant::now() + transfer_drain_timeout;
    let transfer_admission_idle = transfer_admission
        .wait_for_idle(config.server_drain_timeout.min(transfer_drain_timeout))
        .await;
    if !transfer_admission_idle {
        tracing::warn!(
            timeout_secs = config.server_drain_timeout.as_secs(),
            "state-mutation admission exceeded its warning deadline; retaining ownership"
        );
    }
    source_cleanup_token.cancel();
    if let Err(error) = source_cleanup.await {
        tracing::warn!(%error, "send source cleanup task failed");
    }
    pruner_token.cancel();
    queue_metrics_token.cancel();
    let _ = pruner_handle.await;
    let _ = queue_metrics_handle.await;

    if !transfer_admission_idle
        && !transfer_admission
            .wait_for_idle(
                transfer_drain_deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
    {
        // An admitted handler may still be staging a push source before it
        // durably inserts the job. Do not let the active-job query race ahead
        // of that insert and release ownership while old-generation work can
        // still start.
        tracing::error!(
            drain_timeout_secs = config.transfer_drain_timeout.as_secs(),
            "state-mutation admission exceeded the transfer drain deadline; exiting while holding ownership"
        );
        service_mutex
            .take()
            .expect("service mutex held until mutation admission drains")
            .hard_exit(
                1,
                "state-mutation admission exceeded transfer drain deadline",
            );
    }

    tracing::info!("draining outstanding transfer jobs while gRPC remains available");
    let drain_timeout =
        transfer_drain_deadline.saturating_duration_since(tokio::time::Instant::now());
    let (transfer_tasks_drained, shadow_comparisons_drained) = tokio::join!(
        worker.shutdown(drain_timeout),
        shadow_comparisons.shutdown(drain_timeout),
    );
    if !shadow_comparisons_drained {
        tracing::warn!(
            drain_timeout_secs = config.transfer_drain_timeout.as_secs(),
            "sparse shadow comparisons exceeded the service drain deadline"
        );
    }
    let transfer_drain_complete = match service_for_drain.active_transfer_count() {
        Ok(0) if transfer_tasks_drained => true,
        Ok(active_jobs) if !transfer_tasks_drained => {
            tracing::error!(
                active_jobs,
                inflight_transfers = worker.inflight(),
                "transfer drain deadline left executor tasks; process will exit while holding ownership"
            );
            false
        }
        Ok(active_jobs) => {
            tracing::error!(
                active_jobs,
                "transfer drain deadline left active jobs; process will exit while holding ownership"
            );
            false
        }
        Err(error) => {
            tracing::error!(
                %error,
                "could not verify transfer drain; process will exit while holding ownership"
            );
            false
        }
    };

    let server_drain_timeout = config.server_drain_timeout;
    stop_production_server_or_hard_exit(
        &production_shutdown_token,
        &mut server_handle,
        server_drain_timeout,
        &mut service_mutex,
    )
    .await;
    if !transfer_drain_complete {
        // Do not drop the mutex while old executor tasks still exist. A hard
        // process boundary stops those tasks before the kernel releases flock;
        // the successor can then recover every persisted non-terminal row.
        service_mutex
            .take()
            .expect("service mutex held until transfer executors stop")
            .hard_exit(1, "transfer drain did not prove executor quiescence");
    }
    health_server.join().await;
    tracing::info!("drain complete; exiting");

    drop(service_mutex.take());
    Ok(())
}

#[cfg(test)]
mod tests;
