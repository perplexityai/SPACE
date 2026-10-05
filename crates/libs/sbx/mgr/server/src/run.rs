//! The sandbox manager's process entry point: startup recovery, the
//! background workers, the production and host-control gRPC servers, and the
//! restart-handoff drain that hands sandbox ownership to a successor process
//! without abandoning admitted state writes.
//!
//! The `space-sbxmgr-svc` binary is a thin `main` that parses flags,
//! initializes telemetry, and calls [`run`], so a deployment that needs
//! private wiring can supply its own `main` against this library without
//! copying any of the service.

use pplx_space_common_core::types::SandboxId;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, Request as HttpRequest, Response as HttpResponse};
use http_body::{Body, Frame, SizeHint};
use pplx_space_common_svc::core::host::linux::identities::group_names;
use pplx_space_common_svc::core::host::linux::paths::{
    SANDBOX_MANAGER_DEPENDENCY_ADMISSION_GATE, SANDBOX_MANAGER_DEPENDENCY_DRAIN_CAPABILITY,
    SANDBOX_MANAGER_DEPENDENCY_WORK_GATE,
};
use pplx_space_common_svc::{
    AcquireOutcome, DEFAULT_DRAIN_TIMEOUT, DEFAULT_SERVICE_MUTEX_TIMEOUT, LocalHealthServer,
    ServiceMutex,
};
use pplx_space_spaced_common::proto::host_control::host_control_server::HostControlServer;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::WaitForCancellationFutureOwned;
use tokio_util::task::TaskTracker;
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Status, service::Interceptor};
use tower::{BoxError, Layer, Service};

use pplx_space_sbxmgr_migrations::{COMMON_SCHEMA_MIGRATOR, SANDBOX_MANAGER_MIGRATOR};

use crate::proto::sandbox_manager_server::SandboxManagerServer;
use crate::service::{
    EnableDependencyRestartDrainError, ReleaseDependencyRestartDrainError, SandboxManagerService,
    SandboxManagerServiceOptions, StartupRecoveryAdmissionError, StartupRecoveryError,
};

/// The service name sandbox-manager reports to telemetry, names its health
/// and rate-limit surfaces with, and labels its drain log lines with.
pub const SERVICE_NAME: &str = "space-sandbox-manager";
/// The host-control gRPC surface is rate-limited and traced as its own
/// service so its traffic does not dilute the sandbox surface's budget.
const HOST_CONTROL_SERVICE_NAME: &str = "space-sandbox-manager-host-control";
/// Optional override for the node role the service reports; blank or absent
/// leaves the service's own default in place.
const SPACE_NODE_ROLE_ENV: &str = "SPACE_NODE_ROLE";
const SPACE_NODE_MANAGED_BY_ENV: &str = "SPACE_NODE_MANAGED_BY";
const SOCKET_GROUP: &str = group_names::SPACE_SBXCLIENTS;
// Keep this above E2B_FILE_WRITE_CHUNK_BYTES (3 MiB) plus protobuf overhead.
const SANDBOX_MANAGER_MAX_DECODE_BYTES: usize = 4 * 1024 * 1024;
const HOST_CONTROL_MAX_DECODE_BYTES: usize = 64 * 1024;
// Hard-coded so two instances on the same host cannot be configured to use
// different mutex files and both acquire concurrently.
const SERVICE_MUTEX_PATH: &str = "/space/run/sandbox-manager/service.mutex";
// Give local suspend finalization time to publish the paused state during
// deploy shutdown. Remote uploads remain durable and recover on startup.
const SNAPSHOT_WORKER_DRAIN_TIMEOUT: Duration = Duration::from_secs(15);
const PRE_HANDOFF_DRAIN_TIMEOUT: Duration = DEFAULT_DRAIN_TIMEOUT;
const POST_HANDOFF_RPC_DRAIN_TIMEOUT: Duration = DEFAULT_DRAIN_TIMEOUT;
const DRAIN_BLOCKER_WARNING_THRESHOLD: Duration = Duration::from_secs(10);
const STARTUP_RECOVERY_ADMISSION_TIMEOUT: Duration = DEFAULT_SERVICE_MUTEX_TIMEOUT;

async fn migrate_sandbox_manager_db(
    db_path: &std::path::Path,
) -> Result<(), pplx_space_common_svc::sql::sqlite::SqliteError> {
    pplx_space_common_svc::sql::sqlite::migrate_embedded(
        "sandbox-manager",
        db_path,
        pplx_space_common_svc::sql::sqlite::EmbeddedMigrators {
            service: &SANDBOX_MANAGER_MIGRATOR,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
        Some(pplx_space_sbxmgr_migrations::prepare_migrations),
    )
    .await
}

type GrpcServerHandle = JoinHandle<Result<(), tonic::transport::Error>>;
type GrpcServerJoinResult = Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>;

struct ServerExitGuard {
    name: &'static str,
    shutdown: CancellationToken,
    unexpected_exit: CancellationToken,
}

impl Drop for ServerExitGuard {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            tracing::error!(
                server = self.name,
                "production server exited before shutdown; draining manager"
            );
            self.unexpected_exit.cancel();
            self.shutdown.cancel();
        }
    }
}

fn spawn_supervised_server(
    name: &'static str,
    shutdown: &CancellationToken,
    unexpected_exit: &CancellationToken,
    server: impl Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
) -> GrpcServerHandle {
    let guard = ServerExitGuard {
        name,
        shutdown: shutdown.clone(),
        unexpected_exit: unexpected_exit.clone(),
    };
    tokio::spawn(async move {
        // Drop runs on normal completion and panic. Shutdown then follows the
        // ordinary ownership drain; it must not release the mutex immediately.
        let _guard = guard;
        server.await
    })
}

struct HandoffServers<'a> {
    pre_handoff_snapshot: &'a (dyn Fn() -> Vec<(&'static str, usize)> + Sync),
    active_response_streams: &'a ActiveResponseStreams,
    handles: Vec<(&'static str, GrpcServerHandle)>,
}

#[derive(Debug, PartialEq, Eq)]
enum HandoffDrainResult<T> {
    PreHandoffTimedOut(Vec<(&'static str, usize)>),
    PreHandoffNotQuiescent(Vec<(&'static str, usize)>),
    PostHandoffTimedOut(Vec<(&'static str, usize)>),
    Server(T),
}

#[derive(Debug, PartialEq, Eq)]
enum DrainMonitorResult<T, U> {
    Completed(T),
    TimedOut(U),
}

#[derive(Clone)]
struct HandoffDrainInterceptor {
    drain_token: CancellationToken,
}

impl HandoffDrainInterceptor {
    fn new(drain_token: CancellationToken) -> Self {
        Self { drain_token }
    }
}

impl Interceptor for HandoffDrainInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if !self.drain_token.is_cancelled() {
            return Ok(request);
        }

        Err(
            pplx_space_sbx_mgr_client::restart_retry_on_successor_status(
                "sandbox-manager RPC admission is draining; retry on successor",
            ),
        )
    }
}

#[derive(Clone)]
struct HandoffStreamCancellationLayer {
    drain_token: CancellationToken,
    active_response_streams: ActiveResponseStreams,
}

impl HandoffStreamCancellationLayer {
    fn new(drain_token: CancellationToken, active_response_streams: ActiveResponseStreams) -> Self {
        Self {
            drain_token,
            active_response_streams,
        }
    }
}

impl<S> Layer<S> for HandoffStreamCancellationLayer {
    type Service = HandoffStreamCancellationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HandoffStreamCancellationService {
            inner,
            drain_token: self.drain_token.clone(),
            active_response_streams: self.active_response_streams.clone(),
        }
    }
}

fn handoff_interrupted_status() -> Status {
    Status::unavailable("sandbox-manager handoff interrupted an in-flight RPC")
}

#[derive(Clone, Default)]
struct ActiveResponseStreams {
    counts: Arc<Mutex<BTreeMap<&'static str, usize>>>,
}

impl ActiveResponseStreams {
    #[allow(
        clippy::expect_used,
        reason = "Private counter mutex runs only owned map operations and saturating arithmetic, never callbacks"
    )]
    fn register(&self, method: &'static str) -> ActiveResponseStreamLease {
        let mut counts = self.counts.lock().expect("response stream mutex poisoned");
        let active = counts.entry(method).or_default();
        *active = active.saturating_add(1);
        drop(counts);
        ActiveResponseStreamLease {
            streams: self.clone(),
            method,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "Private counter mutex runs only owned map operations, never callbacks"
    )]
    fn snapshot(&self) -> Vec<(&'static str, usize)> {
        self.counts
            .lock()
            .expect("response stream mutex poisoned")
            .iter()
            .map(|(&method, &active)| (method, active))
            .collect()
    }
}

struct ActiveResponseStreamLease {
    streams: ActiveResponseStreams,
    method: &'static str,
}

impl Drop for ActiveResponseStreamLease {
    #[allow(
        clippy::expect_used,
        reason = "Each private lease is registered once and dropped once; only its last release removes the counter"
    )]
    fn drop(&mut self) {
        let mut counts = self
            .streams
            .counts
            .lock()
            .expect("response stream mutex poisoned");
        let active = counts
            .get_mut(&self.method)
            .expect("active response stream method must be registered");
        debug_assert!(*active > 0, "response stream method count underflow");
        *active = active.saturating_sub(1);
        if *active == 0 {
            counts.remove(&self.method);
        }
    }
}

#[derive(Clone)]
struct HandoffStreamCancellationService<S> {
    inner: S,
    drain_token: CancellationToken,
    active_response_streams: ActiveResponseStreams,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for HandoffStreamCancellationService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Body<Data = tonic::codegen::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError>,
{
    type Response = HttpResponse<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: HttpRequest<ReqBody>) -> Self::Future {
        let response_stream_name =
            crate::service::handoff_cancellable_response_stream_name(request.uri().path());
        let future = self.inner.call(request);
        let drain_token = self.drain_token.clone();
        let active_response_streams = self.active_response_streams.clone();
        Box::pin(async move {
            let response = tokio::select! {
                biased;
                response = future => response?,
                _ = drain_token.cancelled() => {
                    return Ok(handoff_interrupted_status().into_http());
                }
            };
            let response_stream_name =
                response_stream_name.filter(|_| !response.headers().contains_key("grpc-status"));
            Ok(response.map(|body| {
                tonic::body::Body::new(HandoffStreamCancellationBody::new(
                    body,
                    response_stream_name.map(|_| drain_token),
                    response_stream_name.map(|method| active_response_streams.register(method)),
                ))
            }))
        })
    }
}

struct HandoffStreamCancellationBody<B> {
    body: Pin<Box<B>>,
    cancelled: Option<Pin<Box<WaitForCancellationFutureOwned>>>,
    _active_stream: Option<ActiveResponseStreamLease>,
    is_cancelled: bool,
}

impl<B> HandoffStreamCancellationBody<B> {
    fn new(
        body: B,
        drain_token: Option<CancellationToken>,
        active_stream: Option<ActiveResponseStreamLease>,
    ) -> Self {
        Self {
            body: Box::pin(body),
            cancelled: drain_token.map(|token| Box::pin(token.cancelled_owned())),
            _active_stream: active_stream,
            is_cancelled: false,
        }
    }
}

fn emit_handoff_unfinished_work(
    drain_phase: &'static str,
    work_kind: &'static str,
    active_work: &[(&'static str, usize)],
    drain_elapsed: Duration,
    possible_uninstrumented_phases: &'static str,
) {
    if active_work.is_empty() {
        tracing::warn!(
            drain_phase,
            work_kind,
            operation = "untracked",
            active_count = 0_u64,
            drain_elapsed_secs = drain_elapsed.as_secs(),
            possible_uninstrumented_phases,
            "sandbox-manager handoff remains active with no tracked unfinished work"
        );
        return;
    }

    for &(operation, active_count) in active_work {
        tracing::warn!(
            drain_phase,
            work_kind,
            operation,
            active_count = active_count as u64,
            drain_elapsed_secs = drain_elapsed.as_secs(),
            "sandbox-manager handoff has unfinished work remaining"
        );
    }
}

async fn monitor_drain<F, T, W, X, U>(
    drain_timeout: Duration,
    warning_threshold: Duration,
    drain: F,
    on_warning: W,
    on_timeout: X,
) -> DrainMonitorResult<T, U>
where
    F: Future<Output = T>,
    W: FnOnce(),
    X: FnOnce() -> U,
{
    tokio::pin!(drain);
    let warn_after = tokio::time::sleep(warning_threshold);
    tokio::pin!(warn_after);
    let monitored_drain = async {
        tokio::select! {
            result = &mut drain => result,
            _ = &mut warn_after => {
                on_warning();
                drain.await
            }
        }
    };
    match tokio::time::timeout(drain_timeout, monitored_drain).await {
        Ok(result) => DrainMonitorResult::Completed(result),
        Err(_) => DrainMonitorResult::TimedOut(on_timeout()),
    }
}

impl<B> Body for HandoffStreamCancellationBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.is_cancelled {
            return Poll::Ready(None);
        }
        if this
            .cancelled
            .as_mut()
            .is_some_and(|cancelled| cancelled.as_mut().poll(context).is_ready())
        {
            this.is_cancelled = true;
            let mut trailers = HeaderMap::new();
            #[allow(
                clippy::expect_used,
                reason = "Handoff status has static ASCII code/message and no fallible metadata"
            )]
            let () = handoff_interrupted_status()
                .add_header(&mut trailers)
                .expect("static handoff status must serialize to gRPC trailers");
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        this.body.as_mut().poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.is_cancelled || self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        if self.is_cancelled {
            SizeHint::with_exact(0)
        } else {
            self.body.size_hint()
        }
    }
}

async fn handoff_production_servers<D, F, E>(
    pre_handoff_timeout: Duration,
    server_drain_timeout: Duration,
    pre_handoff_drain: D,
    rpc_drain_token: &CancellationToken,
    shutdown_token: &CancellationToken,
    release_handoff: F,
    servers: HandoffServers<'_>,
) -> Result<HandoffDrainResult<Vec<(&'static str, GrpcServerJoinResult)>>, E>
where
    D: Future<Output = bool>,
    F: FnOnce() -> Result<(), E>,
{
    let HandoffServers {
        pre_handoff_snapshot,
        active_response_streams,
        handles: server_handles,
    } = servers;
    let pre_handoff_result = monitor_drain(
        pre_handoff_timeout,
        DRAIN_BLOCKER_WARNING_THRESHOLD,
        pre_handoff_drain,
        || {
            emit_handoff_unfinished_work(
                "pre_handoff",
                "state_transition",
                &pre_handoff_snapshot(),
                DRAIN_BLOCKER_WARNING_THRESHOLD,
                "task_tracker,snapshot_worker,other_uninstrumented_work",
            );
        },
        pre_handoff_snapshot,
    )
    .await;
    let pre_handoff_failure = match pre_handoff_result {
        DrainMonitorResult::Completed(true) => None,
        DrainMonitorResult::Completed(false) => Some(HandoffDrainResult::PreHandoffNotQuiescent(
            pre_handoff_snapshot(),
        )),
        DrainMonitorResult::TimedOut(timeout_snapshot) => {
            Some(HandoffDrainResult::PreHandoffTimedOut(timeout_snapshot))
        }
    };
    if let Some(result) = pre_handoff_failure {
        shutdown_token.cancel();
        for (_, handle) in &server_handles {
            handle.abort();
        }
        for (task, handle) in server_handles {
            match handle.await {
                Err(error) if error.is_cancelled() => {}
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, task, "gRPC server ended while aborting timed-out drain")
                }
                Err(error) => {
                    tracing::warn!(%error, task, "gRPC server task failed while aborting timed-out drain")
                }
            }
        }
        return Ok(result);
    }

    rpc_drain_token.cancel();
    shutdown_token.cancel();
    release_handoff()?;

    let server_drain = async move {
        let mut results = Vec::with_capacity(server_handles.len());
        for (name, handle) in server_handles {
            results.push((name, handle.await));
        }
        results
    };
    let server_result = monitor_drain(
        server_drain_timeout,
        DRAIN_BLOCKER_WARNING_THRESHOLD,
        server_drain,
        || {
            emit_handoff_unfinished_work(
                "post_handoff",
                "response_stream",
                &active_response_streams.snapshot(),
                DRAIN_BLOCKER_WARNING_THRESHOLD,
                "response_body_creation,untracked_rpc,other_uninstrumented_work",
            );
        },
        || active_response_streams.snapshot(),
    )
    .await;
    Ok(match server_result {
        DrainMonitorResult::Completed(results) => HandoffDrainResult::Server(results),
        DrainMonitorResult::TimedOut(timeout_snapshot) => {
            HandoffDrainResult::PostHandoffTimedOut(timeout_snapshot)
        }
    })
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Environment-driven configuration stays with the
/// code that reads it, so this carries only what the flags do.
#[derive(Debug, Clone)]
pub struct SbxMgrRunConfig {
    /// Production sandbox-manager Unix socket, bound only after the service
    /// mutex is acquired.
    pub socket_path: PathBuf,
    /// Sandbox-scoped host-control Unix socket, bound only after the service
    /// mutex is acquired.
    pub host_control_socket_path: PathBuf,
    /// Instance-local health Unix socket, bound after receiver verification so
    /// readiness can pass while this instance is still passive.
    pub health_socket_path: Option<PathBuf>,
    /// How long the passive instance waits for the predecessor to release the
    /// service mutex before exiting non-zero.
    pub service_mutex_timeout: Duration,
}

/// Why [`run`] returned without a clean shutdown. Every variant is fatal to
/// the process: the drain paths the service only logs stay inside [`run`].
#[derive(Debug, thiserror::Error)]
pub enum RunSbxMgrError {
    #[error("invalid sandbox-manager configuration: {0}")]
    Config(#[source] crate::config::ConfigLoadError),
    #[error("invalid or missing SPACE API route verification configuration: {0}")]
    RouteVerificationBundle(
        #[source] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error,
    ),
    #[error("failed to bind the sandbox-manager health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to acquire the sandbox-manager service mutex: {0}")]
    ServiceMutex(#[source] pplx_space_common_svc::service_mutex::AcquireError),
    #[error("failed to migrate the sandbox-manager database: {0}")]
    Migrate(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("failed to open the sandbox-manager database: {0}")]
    OpenDatabase(#[source] pplx_space_common_svc::sql::sqlite::SqliteError),
    #[error("snapshot upload cutover preflight failed: {0}")]
    UploadCutover(#[source] crate::snapshot_upload_jobs::UploadCutoverError),
    #[error("snapshot upload cutover preflight task failed: {0}")]
    UploadCutoverTask(#[source] tokio::task::JoinError),
    #[error("invalid {SPACE_NODE_ROLE_ENV} {role:?}: {source}")]
    NodeRole {
        role: String,
        #[source]
        source: pplx_space_common_svc::core::types::NodeRoleParseError,
    },
    #[error("invalid {SPACE_NODE_MANAGED_BY_ENV}: {0}")]
    NodeManagedBy(#[source] pplx_space_common_core::types::NodeManagedByParseError),
    #[error("could not read {SPACE_NODE_MANAGED_BY_ENV}: {0}")]
    NodeManagedByEnvironment(#[source] std::env::VarError),
    #[error("failed to enable dependency restart drain: {0}")]
    EnableDependencyRestartDrain(#[source] EnableDependencyRestartDrainError),
    #[error("failed to admit startup recovery through dependency drain: {0}")]
    StartupRecoveryAdmission(#[source] StartupRecoveryAdmissionError),
    #[error("execution authority is not ready: {0}")]
    ExecutionAuthority(#[source] tonic::Status),
    /// A persistence failure during startup recovery has no in-process retry
    /// owner, so it fails admission and lets process supervision retry.
    #[error("failed to persist sandbox startup recovery: {0}")]
    StartupRecovery(#[source] StartupRecoveryError),
    #[error("failed to recover local_artifacts lease states: {0}")]
    LocalArtifactRecovery(#[source] rusqlite::Error),
    #[error("failed to refund interrupted snapshot upload attempts: {0}")]
    RefundSnapshotUploadAttempts(
        #[source] crate::service::RefundInterruptedSnapshotUploadAttemptsError,
    ),
    #[error("failed to bind the sandbox-manager socket: {0}")]
    BindSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to bind the sandbox-manager host-control socket: {0}")]
    BindHostControlSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to release the dependency restart drain capability: {0}")]
    ReleaseDependencyRestartDrain(#[source] ReleaseDependencyRestartDrainError),
    /// A production gRPC server returned or panicked before shutdown was
    /// requested; the supervised-server guard already drained the manager.
    #[error("production server exited before shutdown")]
    ProductionServerExited,
    /// A background worker the service cannot run without stopped early.
    #[error("critical worker exited before shutdown")]
    CriticalWorkerExited,
}

/// Once service construction can spawn work, returning from startup is not a
/// quiescence proof. Keep the flock until either the normal drain releases it
/// explicitly or process exit stops every constructor/recovery/background task.
struct ActiveServiceOwnership {
    mutex: Option<ServiceMutex>,
}

impl ActiveServiceOwnership {
    fn new(mutex: ServiceMutex) -> Self {
        Self { mutex: Some(mutex) }
    }

    fn release_after_quiescence(&mut self) {
        drop(self.mutex.take());
    }

    fn exit_without_quiescence(mut self, reason: &'static str) -> ! {
        if let Some(mutex) = self.mutex.take() {
            mutex.hard_exit(1, reason);
        }
        // Only the normal handoff may disarm ownership. An explicit fatal
        // exit remains fatal even if called after that boundary.
        std::process::exit(1)
    }
}

impl Drop for ActiveServiceOwnership {
    fn drop(&mut self) {
        if let Some(mutex) = self.mutex.take() {
            // Includes cancellation after active construction: without a drain
            // proof it must exit non-zero, not release ownership as passive exit.
            mutex.hard_exit(
                1,
                "sandbox-manager active task exited before proved quiescence",
            );
        }
    }
}

fn report_active_failure(error: &RunSbxMgrError) {
    // The armed ownership guard exits before the outer main can print this.
    tracing::error!(%error, "sandbox-manager failed while retaining active service ownership");
}

fn parse_node_managed_by(
    value: Result<String, std::env::VarError>,
) -> Result<Option<pplx_space_common_core::types::NodeManagedBy>, RunSbxMgrError> {
    match value {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(RunSbxMgrError::NodeManagedBy),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(RunSbxMgrError::NodeManagedByEnvironment(error)),
    }
}

async fn start_takeover_health(
    config: &SbxMgrRunConfig,
    shutdown_token: &CancellationToken,
    node: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> Result<LocalHealthServer, RunSbxMgrError> {
    // Passive readiness permits Kubernetes to retire the current owner. Check
    // the receiver before exposing it, without opening stores or taking the
    // mutex. Process supervision retries failures while the old owner serves.
    tokio::select! {
        biased;
        _ = shutdown_token.cancelled() => return Ok(LocalHealthServer::disabled()),
        result = tokio::time::timeout(
            Duration::from_secs(10),
            pplx_space_node_mgr_client::execution_fence::inspect_execution(
                node,
                SandboxId::new(),
            ),
        ) => {
            result
                .map_err(|_| RunSbxMgrError::ExecutionAuthority(Status::deadline_exceeded(
                    "node-manager execution authority check timed out before takeover readiness",
                )))?
                .map_err(RunSbxMgrError::ExecutionAuthority)?;
        }
    }
    LocalHealthServer::spawn_optional(
        config.health_socket_path.as_deref(),
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        shutdown_token.clone(),
    )
    .map_err(RunSbxMgrError::HealthSocket)
}

/// Run the sandbox manager until `shutdown_token` fires and the restart
/// handoff completes, or a fatal startup condition ends it. Telemetry must
/// already be initialized by the caller.
pub async fn run(
    config: SbxMgrRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunSbxMgrError> {
    let service_config = crate::config::load().map_err(RunSbxMgrError::Config)?;
    let node_managed_by = parse_node_managed_by(std::env::var(SPACE_NODE_MANAGED_BY_ENV))?;
    let route_ed25519_verification_bundle = Some(std::sync::Arc::new(
        pplx_space_common_svc::space_api_route_ed25519::load_required_space_api_route_ed25519_verification_bundle()
            .map_err(RunSbxMgrError::RouteVerificationBundle)?,
    ));

    let health_server = start_takeover_health(
        &config,
        &shutdown_token,
        &mut pplx_space_node_mgr_client::lazy(),
    )
    .await?;
    let health_socket = config
        .health_socket_path
        .as_ref()
        .map_or_else(|| "disabled".to_string(), |path| path.display().to_string());

    tracing::info!(
        health_socket = %health_socket,
        service_mutex = SERVICE_MUTEX_PATH,
        "space-sbx-manager passive; waiting for service mutex"
    );

    let service_mutex = match ServiceMutex::acquire_with_timeout(
        SERVICE_MUTEX_PATH,
        &shutdown_token,
        config.service_mutex_timeout,
    )
    .await
    .map_err(RunSbxMgrError::ServiceMutex)?
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => {
            tracing::warn!("terminated while passive; exiting cleanly");
            health_server.join().await;
            return Ok(());
        }
    };

    tracing::info!(
        service_mutex = %service_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );
    health_server.mark_starting();

    // The scan is complete but streaming: keep startup ownership off-runtime
    // with the SQLite work so a large queue cannot starve the health watchdog.
    let (service_mutex, upload_cutover) = tokio::task::spawn_blocking(move || {
        let result = crate::snapshot_upload_jobs::validate_upload_cutover_path(
            std::path::Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_DB),
        );
        (service_mutex, result)
    })
    .await
    .map_err(RunSbxMgrError::UploadCutoverTask)?;
    upload_cutover.map_err(RunSbxMgrError::UploadCutover)?;
    migrate_sandbox_manager_db(std::path::Path::new(
        pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_DB,
    ))
    .await
    .map_err(RunSbxMgrError::Migrate)?;

    let conn = crate::db::open().map_err(RunSbxMgrError::OpenDatabase)?;
    let node_role = match std::env::var(SPACE_NODE_ROLE_ENV) {
        Ok(value) if !value.trim().is_empty() => {
            let role = value.trim();
            Some(
                pplx_space_common_svc::core::types::NodeRole::from_str(role).map_err(|source| {
                    RunSbxMgrError::NodeRole {
                        role: role.to_string(),
                        source,
                    }
                })?,
            )
        }
        _ => None,
    };

    let task_tracker = TaskTracker::new();
    let mut active_ownership = ActiveServiceOwnership::new(service_mutex);
    let service = SandboxManagerService::new_with_config(
        conn,
        SandboxManagerServiceOptions {
            host_control_socket_path: config.host_control_socket_path.clone(),
            shutdown_token: shutdown_token.clone(),
            task_tracker: task_tracker.clone(),
            node_role,
            node_managed_by,
            route_ed25519_verification_bundle,
            ..Default::default()
        },
        service_config,
    );
    service.refresh_route_ed25519_verification_bundle_now();
    service
        .enable_dependency_restart_drain(
            SANDBOX_MANAGER_DEPENDENCY_DRAIN_CAPABILITY,
            SANDBOX_MANAGER_DEPENDENCY_ADMISSION_GATE,
            SANDBOX_MANAGER_DEPENDENCY_WORK_GATE,
        )
        .map_err(RunSbxMgrError::EnableDependencyRestartDrain)
        .inspect_err(report_active_failure)?;
    // Capability observation alone is not a stable handoff: the process that
    // init observed can exit while its successor starts. Admit all recovery
    // through the work gate so dependency replacement either waits for these
    // writes or closes admission before any recovery begins.
    let startup_recovery_lease = match service
        .wait_for_startup_recovery_lease(&shutdown_token, STARTUP_RECOVERY_ADMISSION_TIMEOUT)
        .await
    {
        Ok(lease) => lease,
        Err(StartupRecoveryAdmissionError::Cancelled) => {
            tracing::warn!("terminated while waiting for startup recovery admission");
            health_server.join().await;
            active_ownership.exit_without_quiescence(
                "sandbox-manager cancelled after construction without a drain proof",
            );
        }
        Err(error) => {
            let error = RunSbxMgrError::StartupRecoveryAdmission(error);
            report_active_failure(&error);
            return Err(error);
        }
    };
    service
        .prepare_execution_authority()
        .await
        .map_err(RunSbxMgrError::ExecutionAuthority)?;
    let startup_recovery = service
        .recover_interrupted_startup_states()
        .await
        // A persistence failure has no in-process startup retry owner. Fail
        // admission so process supervision retries rather than serving rows
        // stranded in an interrupted operation.
        .map_err(RunSbxMgrError::StartupRecovery)
        .inspect_err(report_active_failure)?;
    if !startup_recovery.is_empty() {
        let changed_count = startup_recovery
            .iter()
            .filter(|record| record.outcome.changed())
            .count();
        let deferred_count = startup_recovery.len().saturating_sub(changed_count);
        let cleanup_required_count = startup_recovery
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    crate::db::StartupRecoveryOutcome::CleanupRequired { .. }
                )
            })
            .count();
        let quarantine_required_count = startup_recovery
            .iter()
            .filter(|record| record.outcome.requires_quarantine())
            .count();
        let created_count = startup_recovery
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    crate::db::StartupRecoveryOutcome::State(crate::state::SandboxState::Created)
                )
            })
            .count();
        let stopped_count = startup_recovery
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    crate::db::StartupRecoveryOutcome::State(crate::state::SandboxState::Stopped)
                )
            })
            .count();
        let suspended_count = startup_recovery
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    crate::db::StartupRecoveryOutcome::State(crate::state::SandboxState::Suspended)
                )
            })
            .count();
        tracing::warn!(
            recovered_count = startup_recovery.len(),
            changed_count,
            deferred_count,
            cleanup_required_count,
            quarantine_required_count,
            created_count,
            stopped_count,
            suspended_count,
            "startup recovery reconciled sandboxes interrupted by prior process exit"
        );
    }
    let local_artifact_recovery = service
        .recover_local_artifact_stuck_states()
        .map_err(RunSbxMgrError::LocalArtifactRecovery)
        .inspect_err(report_active_failure)?;
    if local_artifact_recovery.total() > 0 {
        tracing::warn!(
            restoring_reset = local_artifact_recovery.restoring_reset,
            deleting_rolled_back = local_artifact_recovery.deleting_rolled_back,
            deleting_finished = local_artifact_recovery.deleting_finished,
            deleting_deferred = local_artifact_recovery.deleting_deferred,
            "classified local_artifacts rows stranded by a prior process exit"
        );
    }
    service
        .recover_runtime_registrations()
        .await
        .map_err(RunSbxMgrError::StartupRecovery)
        .inspect_err(report_active_failure)?;

    let snapshot_upload_worker = service.snapshot_upload_worker();
    let refunded_snapshot_attempts = service
        .refund_interrupted_snapshot_upload_attempts()
        .map_err(RunSbxMgrError::RefundSnapshotUploadAttempts)
        .inspect_err(report_active_failure)?;
    if refunded_snapshot_attempts > 0 {
        tracing::warn!(
            refunded_attempts = refunded_snapshot_attempts,
            "refunded interrupted snapshot upload attempts before startup recovery"
        );
    }
    snapshot_upload_worker.recover_and_log("sandbox-mgr.snapshot_upload");
    drop(startup_recovery_lease);

    service.start_auto_lifecycle_scheduler(shutdown_token.child_token(), &task_tracker);
    service.start_route_verification_bundle_refresher(shutdown_token.child_token(), &task_tracker);
    service.start_network_policy_reconciler(shutdown_token.child_token(), &task_tracker);
    service.start_inventory_reconciler(shutdown_token.child_token(), &task_tracker);
    service.start_node_state_dirty_syncer(shutdown_token.child_token(), &task_tracker);
    service.start_snapshot_reaper(shutdown_token.child_token(), &task_tracker);
    service.start_local_artifact_metadata_reaper(shutdown_token.child_token(), &task_tracker);
    service.start_lifecycle_metrics_sampler(shutdown_token.child_token(), &task_tracker);
    service.start_sandbox_eviction(shutdown_token.child_token(), &task_tracker);
    service.start_snapshot_upload_maintenance(shutdown_token.child_token(), &task_tracker);
    task_tracker.spawn(
        snapshot_upload_worker
            .clone()
            .run_queue_metrics(shutdown_token.child_token()),
    );
    task_tracker.close();

    let sandbox_listener = pplx_space_common_svc::bind_unix_socket(
        &config.socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o770)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(RunSbxMgrError::BindSocket)
    .inspect_err(report_active_failure)?;
    let host_control_listener = pplx_space_common_svc::bind_unix_socket(
        &config.host_control_socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o666),
    )
    .map_err(RunSbxMgrError::BindHostControlSocket)
    .inspect_err(report_active_failure)?;

    tracing::info!(
        socket_path = %config.socket_path.display(),
        host_control_socket_path = %config.host_control_socket_path.display(),
        "space-sbx-manager active; listening on production sockets"
    );

    health_server.activate(config.socket_path.clone());

    // Production servers keep serving reads while admitted state writers
    // finish. On SIGTERM, shutdown_token fires first;
    // production_shutdown_token fires only after writers and background work
    // drain.
    let rpc_drain_token = CancellationToken::new();
    let production_shutdown_token = CancellationToken::new();
    let unexpected_server_exit = CancellationToken::new();
    let active_response_streams = ActiveResponseStreams::default();

    let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
    health_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
            tonic_health::ServingStatus::Serving,
        )
        .await;
    health_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE,
            tonic_health::ServingStatus::Serving,
        )
        .await;
    health_reporter
        .set_service_status(
            pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
            tonic_health::ServingStatus::Serving,
        )
        .await;
    let sandbox_shutdown = {
        let token = production_shutdown_token.clone();
        async move { token.cancelled().await }
    };
    let sandbox_server_handle = spawn_supervised_server(
        SERVICE_NAME,
        &shutdown_token,
        &unexpected_server_exit,
        pplx_space_common_svc::grpc_server_builder()
            .layer(HandoffStreamCancellationLayer::new(
                rpc_drain_token.clone(),
                active_response_streams.clone(),
            ))
            .layer(pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(SERVICE_NAME))
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .layer(service.restart_admission_layer())
            .add_service(health_service)
            .add_service(InterceptedService::new(
                SandboxManagerServer::new(service.clone())
                    .max_decoding_message_size(SANDBOX_MANAGER_MAX_DECODE_BYTES),
                HandoffDrainInterceptor::new(rpc_drain_token.clone()),
            ))
            .serve_with_incoming_shutdown(
                UnixListenerStream::new(sandbox_listener),
                sandbox_shutdown,
            ),
    );

    let host_control_shutdown = {
        let token = production_shutdown_token.clone();
        async move { token.cancelled().await }
    };
    let host_control_server_handle = spawn_supervised_server(
        "space-sandbox-manager-host-control",
        &shutdown_token,
        &unexpected_server_exit,
        pplx_space_common_svc::grpc_server_builder()
            .layer(HandoffStreamCancellationLayer::new(
                rpc_drain_token.clone(),
                active_response_streams.clone(),
            ))
            .layer(
                pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(
                    HOST_CONTROL_SERVICE_NAME,
                ),
            )
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .layer(service.restart_admission_layer())
            .add_service(InterceptedService::new(
                HostControlServer::new(service.clone())
                    .max_decoding_message_size(HOST_CONTROL_MAX_DECODE_BYTES),
                HandoffDrainInterceptor::new(rpc_drain_token.clone()),
            ))
            .serve_with_incoming_shutdown(
                UnixListenerStream::new(host_control_listener),
                host_control_shutdown,
            ),
    );

    shutdown_token.cancelled().await;
    service.begin_draining_for_restart();

    let pre_handoff_drain = async {
        // Keep production and host-control serving while state transitions
        // that entered before drain complete. Central admission rejects new
        // state-changing RPCs while reads remain available.
        service.wait_for_state_transitions_drained().await;
        task_tracker.wait().await;
        snapshot_upload_worker
            .yield_for_restart(SNAPSHOT_WORKER_DRAIN_TIMEOUT)
            .await
    };

    // Signal accepted streams at the ownership boundary. Long-lived guest
    // streams reconnect through the successor instead of keeping the
    // predecessor alive for the full post-handoff drain budget.
    let pre_handoff_snapshot = || {
        let mut active = service.active_state_transition_methods();
        let snapshot_uploads = snapshot_upload_worker.inflight();
        if snapshot_uploads > 0 {
            active.push(("snapshot_upload_worker", snapshot_uploads));
        }
        active
    };
    let server_results = handoff_production_servers(
        PRE_HANDOFF_DRAIN_TIMEOUT,
        POST_HANDOFF_RPC_DRAIN_TIMEOUT,
        pre_handoff_drain,
        &rpc_drain_token,
        &production_shutdown_token,
        || {
            service
                .release_dependency_restart_drain_capability()
                .map_err(RunSbxMgrError::ReleaseDependencyRestartDrain)
                .inspect_err(report_active_failure)?;
            active_ownership.release_after_quiescence();
            Ok::<(), RunSbxMgrError>(())
        },
        HandoffServers {
            pre_handoff_snapshot: &pre_handoff_snapshot,
            active_response_streams: &active_response_streams,
            handles: vec![
                (SERVICE_NAME, sandbox_server_handle),
                (HOST_CONTROL_SERVICE_NAME, host_control_server_handle),
            ],
        },
    )
    .await?;
    health_server.join().await;

    let pre_handoff_failure = match &server_results {
        HandoffDrainResult::PreHandoffTimedOut(snapshot) => {
            Some((snapshot, "timeout", PRE_HANDOFF_DRAIN_TIMEOUT))
        }
        HandoffDrainResult::PreHandoffNotQuiescent(snapshot) => {
            Some((snapshot, "snapshot_worker", SNAPSHOT_WORKER_DRAIN_TIMEOUT))
        }
        _ => None,
    };
    if let Some((timeout_snapshot, reason, drain_timeout)) = pre_handoff_failure {
        let work_kind = if reason == "snapshot_worker" {
            "background_job"
        } else {
            "state_transition"
        };
        emit_handoff_unfinished_work(
            "pre_handoff",
            work_kind,
            timeout_snapshot,
            drain_timeout,
            "task_tracker,snapshot_worker,other_uninstrumented_work",
        );
        tracing::warn!(
            reason,
            drain_timeout_secs = drain_timeout.as_secs(),
            "pre-handoff drain did not prove quiescence; hard-exiting while retaining service ownership"
        );
        #[allow(
            clippy::expect_used,
            reason = "Ownership is armed before service creation and no release path precedes this drain decision"
        )]
        let ownership = active_ownership
            .mutex
            .take()
            .expect("service mutex retained when pre-handoff drain times out");
        ownership.hard_exit(
            1,
            "sandbox-manager pre-handoff drain did not prove task quiescence",
        );
    }

    match server_results {
        HandoffDrainResult::Server(server_results) => {
            for (task, result) in server_results {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, task, "gRPC server ended unexpectedly")
                    }
                    Err(error) => {
                        tracing::warn!(%error, task, "gRPC server task ended unexpectedly")
                    }
                }
            }
        }
        HandoffDrainResult::PostHandoffTimedOut(timeout_snapshot) => {
            emit_handoff_unfinished_work(
                "post_handoff",
                "response_stream",
                &timeout_snapshot,
                POST_HANDOFF_RPC_DRAIN_TIMEOUT,
                "response_body_creation,untracked_rpc,other_uninstrumented_work",
            );
            tracing::warn!(
                drain_timeout_secs = POST_HANDOFF_RPC_DRAIN_TIMEOUT.as_secs(),
                "post-handoff RPC drain budget exhausted; exiting and relying on supervisor SIGKILL"
            );
        }
        HandoffDrainResult::PreHandoffTimedOut(_)
        | HandoffDrainResult::PreHandoffNotQuiescent(_) => unreachable!("hard exit above"),
    }

    if unexpected_server_exit.is_cancelled() {
        return Err(RunSbxMgrError::ProductionServerExited);
    }
    if service.critical_worker_failed() {
        return Err(RunSbxMgrError::CriticalWorkerExited);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
