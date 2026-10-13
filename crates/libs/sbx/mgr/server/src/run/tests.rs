use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::*;
use async_trait::async_trait;
use pplx_space_common_jobs::{BackgroundWorker, Job, JobError, JobExecutor, WorkerConfig};
use pplx_space_common_svc::{ServiceMutex, UnixSocketOptions};
use tonic::codegen::Bytes;
use tonic::transport::Channel;
use tonic_health::ServingStatus;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as HealthStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::server::HealthReporter;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber as _;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

const UNFINISHED_WORK_MESSAGE: &str = "sandbox-manager handoff has unfinished work remaining";
const EMPTY_WORK_MESSAGE: &str =
    "sandbox-manager handoff remains active with no tracked unfinished work";
const LOCAL_SUSPEND_FINALIZATION_TIME: Duration = Duration::from_secs(10);

#[tokio::test]
async fn takeover_wait_keeps_liveness_and_withholds_readiness() {
    use super::test_receiver::{Receiver, dial};
    use pplx_space_node_mgr_client::proto::InspectSandboxResponse;

    #[derive(Clone, Copy)]
    enum Outcome {
        Ready,
        Rejected,
        Cancelled,
    }
    for outcome in [Outcome::Ready, Outcome::Rejected, Outcome::Cancelled] {
        let root = tempfile::tempdir().unwrap();
        let receiver_socket = root.path().join("node.sock");
        let health_socket = root.path().join("health.sock");
        let release = Arc::new(AtomicBool::new(false));
        let receiver = Receiver::bind(
            &receiver_socket,
            Arc::new({
                let release = release.clone();
                move |_| {
                    if !release.load(Ordering::SeqCst) {
                        return Err(Status::unavailable("receiver restarting"));
                    }
                    match outcome {
                        Outcome::Rejected => Err(Status::permission_denied("receiver rejected")),
                        _ => Ok(InspectSandboxResponse::default()),
                    }
                }
            }),
        );
        let shutdown = CancellationToken::new();
        let _guard = shutdown.clone().drop_guard();
        let config = SbxMgrRunConfig {
            socket_path: root.path().join("production.sock"),
            host_control_socket_path: root.path().join("control.sock"),
            health_socket_path: Some(health_socket.clone()),
            service_mutex_timeout: Duration::from_secs(1),
        };
        let start = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                start_takeover_health(&config, &shutdown, || dial(receiver_socket.clone())).await
            }
        });
        receiver.entered.notified().await;
        assert!(
            health_socket.exists(),
            "dependency waits must expose liveness"
        );
        let mut health = health_client("readiness-test", &health_socket).await;
        for (name, expected) in [
            ("liveness", HealthStatus::Serving),
            ("readiness", HealthStatus::NotServing),
        ] {
            let response = health
                .check(HealthCheckRequest {
                    service: name.into(),
                })
                .await
                .unwrap();
            assert_eq!(response.into_inner().status, expected as i32);
        }
        assert!(
            !start.is_finished(),
            "transient outage must not exit startup"
        );
        match outcome {
            Outcome::Cancelled => shutdown.cancel(),
            _ => release.store(true, Ordering::SeqCst),
        }
        let result = tokio::time::timeout(Duration::from_secs(5), start)
            .await
            .unwrap()
            .unwrap();
        match outcome {
            Outcome::Ready => {
                let server = result.unwrap();
                let response = health
                    .check(HealthCheckRequest {
                        service: String::new(),
                    })
                    .await
                    .unwrap();
                assert_eq!(response.into_inner().status, HealthStatus::Serving as i32);
                shutdown.cancel();
                server.join().await;
            }
            Outcome::Cancelled => assert!(matches!(result.unwrap(), LocalHealthServer::Disabled)),
            Outcome::Rejected => {
                let RunSbxMgrError::NodeManagerStartup(error) = result.err().unwrap() else {
                    panic!("unexpected error")
                };
                assert_eq!(error.code(), tonic::Code::PermissionDenied);
                assert!(
                    health
                        .check(HealthCheckRequest {
                            service: "liveness".into()
                        })
                        .await
                        .is_err(),
                    "failed startup must stop its health task without cancelling the caller"
                );
                assert!(!shutdown.is_cancelled());
            }
        }
    }
}

#[tokio::test]
async fn recovery_wait_does_not_hold_the_dependency_work_gate() {
    use super::test_receiver::{Receiver, dial};
    use nix::fcntl::{Flock, FlockArg};
    use pplx_space_node_mgr_client::proto::InspectSandboxResponse;

    let root = tempfile::tempdir().unwrap();
    let capability = root.path().join("capability.lock");
    let admission = root.path().join("admission.lock");
    let work = root.path().join("work.lock");
    for path in [&capability, &admission, &work] {
        std::fs::File::create(path).unwrap();
    }
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    service
        .enable_dependency_restart_drain(&capability, &admission, &work)
        .unwrap();
    let socket = root.path().join("node.sock");
    let ready = Arc::new(AtomicBool::new(false));
    let receiver = Receiver::bind(
        &socket,
        Arc::new({
            let ready = ready.clone();
            move |_| {
                if ready.load(Ordering::SeqCst) {
                    Ok(InspectSandboxResponse::default())
                } else {
                    Err(Status::unavailable("receiver restarting"))
                }
            }
        }),
    );
    let shutdown = CancellationToken::new();
    let _guard = shutdown.clone().drop_guard();
    let wait = tokio::spawn({
        let service = service.clone();
        let shutdown = shutdown.clone();
        async move { admit_startup_recovery(&service, &shutdown, || dial(socket.clone())).await }
    });
    receiver.entered.notified().await;
    // This is the lock node-manager activation takes before it can listen.
    let exclusive = Flock::lock(
        std::fs::File::open(&work).unwrap(),
        FlockArg::LockExclusiveNonblock,
    )
    .unwrap();
    assert_eq!(service.active_state_transition_count(), 0);
    drop(exclusive);
    ready.store(true, Ordering::SeqCst);
    let lease = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(service.active_state_transition_count(), 1);
    let (_, error) = Flock::lock(
        std::fs::File::open(&work).unwrap(),
        FlockArg::LockExclusiveNonblock,
    )
    .unwrap_err();
    assert_eq!(error, nix::errno::Errno::EWOULDBLOCK);
    service.prepare_execution_authority().await.unwrap();
    drop(lease);
    let _exclusive = Flock::lock(
        std::fs::File::open(&work).unwrap(),
        FlockArg::LockExclusiveNonblock,
    )
    .unwrap();
}

#[test]
fn host_ownership_configuration_is_explicit_and_fails_closed() {
    use pplx_space_common_core::types::NodeManagedBy;
    assert_eq!(
        parse_node_managed_by(Err(std::env::VarError::NotPresent)).unwrap(),
        None
    );
    for (raw, expected) in [
        ("space", NodeManagedBy::Space),
        ("k8s", NodeManagedBy::Kubernetes),
    ] {
        assert_eq!(
            parse_node_managed_by(Ok(raw.into())).unwrap(),
            Some(expected)
        );
    }
    for raw in ["", " ", " space", "direct", "kubernetes", "unknown"] {
        assert!(matches!(
            parse_node_managed_by(Ok(raw.into())),
            Err(RunSbxMgrError::NodeManagedBy(_))
        ));
    }
    assert!(matches!(
        parse_node_managed_by(Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::new()
        ))),
        Err(RunSbxMgrError::NodeManagedByEnvironment(_))
    ));
}

#[test]
fn active_service_ownership_only_releases_after_quiescence() {
    const MODE: &str = "SPACE_SBXMGR_TEST_ACTIVE_OWNERSHIP_MODE";
    const DIRECTORY: &str = "SPACE_SBXMGR_TEST_ACTIVE_OWNERSHIP_DIRECTORY";
    if let Ok(mode) = std::env::var(MODE) {
        struct DropWitness(PathBuf);
        impl Drop for DropWitness {
            fn drop(&mut self) {
                std::fs::write(&self.0, "unwound").unwrap();
            }
        }
        let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        let _witness = DropWitness(directory.join("destructor"));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mutex = runtime
            .block_on(ServiceMutex::acquire(
                directory.join("ownership.lock"),
                CancellationToken::new(),
                Duration::from_millis(1),
                Some(Duration::from_secs(1)),
            ))
            .unwrap();
        if mode == "cancel" {
            let mut task = Box::pin(async move {
                let _ownership = ActiveServiceOwnership::new(mutex);
                std::future::pending::<()>().await;
            });
            runtime.block_on(std::future::poll_fn(|context| {
                assert!(task.as_mut().poll(context).is_pending());
                Poll::Ready(())
            }));
            drop(task);
            unreachable!("cancelling an armed owner must hard-exit");
        }
        let fail_startup = || -> Result<(), &'static str> {
            let mut ownership = ActiveServiceOwnership::new(mutex);
            match mode.as_str() {
                "error" => return Err("startup failed after tasks were launched"),
                "panic" => panic!("startup panicked after tasks were launched"),
                "quiescent" => ownership.release_after_quiescence(),
                "explicit_cancel" => ownership.exit_without_quiescence("startup cancelled"),
                _ => panic!("unknown ownership probe mode"),
            }
            Ok(())
        };
        fail_startup().unwrap();
        return;
    }

    for mode in ["error", "panic", "cancel", "explicit_cancel", "quiescent"] {
        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "run::tests::active_service_ownership_only_releases_after_quiescence",
                "--nocapture",
            ])
            .env(MODE, mode)
            .env(DIRECTORY, directory.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if mode == "quiescent" { 0 } else { 1 }),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr),
        );
        assert_eq!(
            directory.path().join("destructor").exists(),
            mode == "quiescent"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // The OS releases even the hard-exit case's flock after process death.
        runtime
            .block_on(ServiceMutex::acquire(
                directory.path().join("ownership.lock"),
                CancellationToken::new(),
                Duration::from_millis(1),
                Some(Duration::from_secs(1)),
            ))
            .unwrap();
    }
}

#[tokio::test]
async fn either_production_server_exit_triggers_owned_shutdown() {
    for name in [SERVICE_NAME, "space-sandbox-manager-host-control"] {
        for panic in [false, true] {
            let shutdown = CancellationToken::new();
            let unexpected = CancellationToken::new();
            let handle = spawn_supervised_server(name, &shutdown, &unexpected, async move {
                assert!(!panic, "injected server panic");
                Ok(())
            });
            let result = handle.await;
            assert_eq!(result.is_err(), panic);
            assert!(shutdown.is_cancelled(), "{name}");
            assert!(unexpected.is_cancelled(), "{name}");
        }
    }
}

#[tokio::test]
async fn expected_server_shutdown_does_not_report_failure() {
    let shutdown = CancellationToken::new();
    let unexpected = CancellationToken::new();
    shutdown.cancel();
    spawn_supervised_server(SERVICE_NAME, &shutdown, &unexpected, async { Ok(()) })
        .await
        .unwrap()
        .unwrap();
    assert!(!unexpected.is_cancelled());
}

#[derive(Clone)]
struct SuspendFinalizationExecutor {
    started: Arc<tokio::sync::Notify>,
    completed: Arc<AtomicBool>,
}

#[async_trait]
impl JobExecutor for SuspendFinalizationExecutor {
    async fn execute(&self, _job: &Job, _cancellation: CancellationToken) -> Result<i64, JobError> {
        self.started.notify_one();
        tokio::time::sleep(LOCAL_SUSPEND_FINALIZATION_TIME).await;
        self.completed.store(true, Ordering::SeqCst);
        Ok(1)
    }
}

fn create_jobs_schema(connection: &rusqlite::Connection) {
    for migration in COMMON_SCHEMA_MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        connection.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn snapshot_worker_restart_yield_allows_inflight_suspend_finalization_to_finish() {
    assert!(SNAPSHOT_WORKER_DRAIN_TIMEOUT < PRE_HANDOFF_DRAIN_TIMEOUT);

    let connection = rusqlite::Connection::open_in_memory().expect("open jobs database");
    create_jobs_schema(&connection);
    let started = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let worker = BackgroundWorker::new(
        Arc::new(Mutex::new(connection)),
        SuspendFinalizationExecutor {
            started: Arc::clone(&started),
            completed: Arc::clone(&completed),
        },
        WorkerConfig::new(1, Duration::from_secs(60), 1),
    );
    worker
        .submit("snapshot_upload", "{}", None, None)
        .expect("submit suspend finalization");
    started.notified().await;

    assert!(
        worker
            .yield_for_restart(SNAPSHOT_WORKER_DRAIN_TIMEOUT)
            .await
    );
    assert!(completed.load(Ordering::SeqCst));
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CapturedEvent {
    message: Option<String>,
    drain_phase: Option<String>,
    work_kind: Option<String>,
    operation: Option<String>,
    active_count: Option<u64>,
    drain_elapsed_secs: Option<u64>,
    possible_uninstrumented_phases: Option<String>,
}

#[derive(Clone, Default)]
struct HandoffEventCapture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: Subscriber> tracing_subscriber::Layer<S> for HandoffEventCapture {
    fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, S>) {
        let mut captured = CapturedEvent::default();
        event.record(&mut captured);
        if matches!(
            captured.message.as_deref(),
            Some(UNFINISHED_WORK_MESSAGE | EMPTY_WORK_MESSAGE)
        ) {
            self.events.lock().unwrap().push(captured);
        }
    }
}

impl Visit for CapturedEvent {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_string()),
            "drain_phase" => self.drain_phase = Some(value.to_string()),
            "work_kind" => self.work_kind = Some(value.to_string()),
            "operation" => self.operation = Some(value.to_string()),
            "possible_uninstrumented_phases" => {
                self.possible_uninstrumented_phases = Some(value.to_string())
            }
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "active_count" => self.active_count = Some(value),
            "drain_elapsed_secs" => self.drain_elapsed_secs = Some(value),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, format!("{value:?}").trim_matches('"'));
    }
}

async fn spawn_health_server(
    socket_path: &Path,
    service: &str,
    rpc_drain_token: CancellationToken,
    active_response_streams: ActiveResponseStreams,
) -> (HealthReporter, CancellationToken, GrpcServerHandle) {
    let listener =
        pplx_space_common_svc::bind_unix_socket(socket_path, UnixSocketOptions::new(0o600))
            .unwrap();
    let (reporter, health) = pplx_space_common_svc::grpc_health_service();
    reporter
        .set_service_status(service, ServingStatus::Serving)
        .await;
    let shutdown_token = CancellationToken::new();
    let server_shutdown = shutdown_token.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .layer(HandoffStreamCancellationLayer::new(
                rpc_drain_token.clone(),
                active_response_streams,
            ))
            .add_service(InterceptedService::new(
                health,
                HandoffDrainInterceptor::new(rpc_drain_token),
            ))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                server_shutdown.cancelled().await
            }),
    );
    (reporter, shutdown_token, server)
}

async fn health_client(service: &'static str, socket_path: &Path) -> HealthClient<Channel> {
    let channel = pplx_space_common_grpc::connect_uds_channel(service, socket_path)
        .await
        .unwrap();
    HealthClient::new(channel)
}

fn health_request(service: &str) -> HealthCheckRequest {
    HealthCheckRequest {
        service: service.to_string(),
    }
}

fn temp_socket_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("sbxmgr-")
        .tempdir_in("/tmp")
        .unwrap()
}

#[derive(Clone)]
struct HoldResponseService<S> {
    inner: S,
    response_ready: Arc<tokio::sync::Notify>,
}

impl<S> HoldResponseService<S> {
    fn new(inner: S, response_ready: Arc<tokio::sync::Notify>) -> Self {
        Self {
            inner,
            response_ready,
        }
    }
}

impl<S, ReqBody> Service<HttpRequest<ReqBody>> for HoldResponseService<S>
where
    S: Service<HttpRequest<ReqBody>>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: HttpRequest<ReqBody>) -> Self::Future {
        let future = self.inner.call(request);
        let response_ready = Arc::clone(&self.response_ready);
        Box::pin(async move {
            let _response = future.await?;
            response_ready.notify_one();
            std::future::pending().await
        })
    }
}

impl<S> tonic::server::NamedService for HoldResponseService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

struct OneFrameBody(Option<Bytes>);

impl Body for OneFrameBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|data| Ok(Frame::data(data))))
    }
}

#[tokio::test]
async fn handoff_drain_does_not_replace_completed_unary_response() {
    let drain_token = CancellationToken::new();
    let mut service =
        HandoffStreamCancellationLayer::new(drain_token.clone(), ActiveResponseStreams::default())
            .layer(tower::service_fn(|_request: HttpRequest<()>| async {
                Ok::<_, std::convert::Infallible>(HttpResponse::new(OneFrameBody(Some(
                    Bytes::from_static(b"completed"),
                ))))
            }));
    let response = service
        .call(
            HttpRequest::builder()
                .uri("/space.sandbox.manager.v1.SandboxManager/Create")
                .body(())
                .unwrap(),
        )
        .await
        .unwrap();

    drain_token.cancel();

    let mut body = response.into_body();
    let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frame
            .into_data()
            .expect("unary response data should survive"),
        Bytes::from_static(b"completed")
    );
}

#[tokio::test]
async fn handoff_cancellable_responses_are_registered_until_the_body_drops() {
    let drain_token = CancellationToken::new();
    let active_response_streams = ActiveResponseStreams::default();
    let mut service =
        HandoffStreamCancellationLayer::new(drain_token, active_response_streams.clone()).layer(
            tower::service_fn(|_request: HttpRequest<()>| async {
                Ok::<_, std::convert::Infallible>(HttpResponse::new(OneFrameBody(Some(
                    Bytes::from_static(b"stream"),
                ))))
            }),
        );

    let response = service
        .call(
            HttpRequest::builder()
                .uri("/space.sandbox.manager.v1.SandboxManager/SpacedStartProcess")
                .body(())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        active_response_streams.snapshot(),
        vec![(
            "/space.sandbox.manager.v1.SandboxManager/SpacedStartProcess",
            1,
        )]
    );

    drop(response);
    assert!(active_response_streams.snapshot().is_empty());
}

#[tokio::test]
async fn handoff_drain_preserves_terminal_retry_on_successor_response() {
    // Arrange.
    let drain_token = CancellationToken::new();
    let active_response_streams = ActiveResponseStreams::default();
    let mut service =
        HandoffStreamCancellationLayer::new(drain_token.clone(), active_response_streams.clone())
            .layer(tower::service_fn(|_request: HttpRequest<()>| async {
                Ok::<_, std::convert::Infallible>(
                    HttpResponse::builder()
                        .header("grpc-status", "14")
                        .header(
                            pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA,
                            "true",
                        )
                        .body(OneFrameBody(Some(Bytes::from_static(b"rejected"))))
                        .unwrap(),
                )
            }));

    // Act.
    let response = service
        .call(
            HttpRequest::builder()
                .uri("/space.sandbox.manager.v1.SandboxManager/SpacedStartProcess")
                .body(())
                .unwrap(),
        )
        .await
        .unwrap();
    drain_token.cancel();

    // Assert.
    assert_eq!(
        response
            .headers()
            .get(pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA)
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    assert!(active_response_streams.snapshot().is_empty());
    let mut body = response.into_body();
    let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frame
            .into_data()
            .expect("terminal response body should survive"),
        Bytes::from_static(b"rejected")
    );
}

#[tokio::test]
async fn handoff_cancels_pending_unary_handler_without_replay_proof() {
    let started = Arc::new(tokio::sync::Notify::new());
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let mut service = HandoffStreamCancellationLayer::new(
        rpc_drain_token.clone(),
        ActiveResponseStreams::default(),
    )
    .layer(tower::service_fn({
        let started = Arc::clone(&started);
        move |_request: HttpRequest<()>| {
            started.notify_one();
            std::future::pending::<Result<HttpResponse<OneFrameBody>, std::convert::Infallible>>()
        }
    }));
    let server = tokio::spawn(async move {
        let response = service
            .call(
                HttpRequest::builder()
                    .uri("/space.sandbox.manager.v1.SandboxManager/GetDiskUsage")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|value| value.to_str().ok()),
            Some("14")
        );
        assert!(
            response
                .headers()
                .get(pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA)
                .and_then(|value| value.to_str().ok())
                .is_none(),
            "an already-dispatched handler must not authorize replay"
        );
        Ok(())
    });
    started.notified().await;

    let result = handoff_production_servers(
        Duration::from_millis(50),
        Duration::from_millis(50),
        async { true },
        &rpc_drain_token,
        &shutdown_token,
        || Ok::<(), anyhow::Error>(()),
        HandoffServers {
            pre_handoff_snapshot: &Vec::new,
            active_response_streams: &ActiveResponseStreams::default(),
            handles: vec![("sandbox-manager", server)],
        },
    )
    .await
    .unwrap();

    let HandoffDrainResult::Server(results) = result else {
        panic!("pending unary handler should be cancelled at handoff");
    };
    results.into_iter().next().unwrap().1.unwrap().unwrap();
}

#[tokio::test]
async fn grpc_writer_during_pre_handoff_drain_returns_retry_on_successor() {
    let dir = temp_socket_dir();
    let socket_path = dir.path().join("sandbox-manager.sock");
    let listener =
        pplx_space_common_svc::bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600))
            .unwrap();
    let service = SandboxManagerService::for_test(rusqlite::Connection::open_in_memory().unwrap());
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let server_shutdown = shutdown_token.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .layer(HandoffStreamCancellationLayer::new(
                rpc_drain_token.clone(),
                ActiveResponseStreams::default(),
            ))
            .layer(service.restart_admission_layer())
            .add_service(InterceptedService::new(
                SandboxManagerServer::new(service.clone()),
                HandoffDrainInterceptor::new(rpc_drain_token),
            ))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                server_shutdown.cancelled().await
            }),
    );
    let channel = pplx_space_common_grpc::connect_uds_channel("sandbox-manager", &socket_path)
        .await
        .unwrap();
    let mut client = crate::proto::sandbox_manager_client::SandboxManagerClient::new(channel);
    service.begin_draining_for_restart();

    let result = client
        .stats(crate::proto::StatsRequest {
            sandbox_id: "sandbox".to_string(),
        })
        .await;
    shutdown_token.cancel();
    server.await.unwrap().unwrap();

    let status = result.expect_err("a new writer must retry on the successor during drain");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(pplx_space_sbx_mgr_client::is_restart_retry_on_successor(
        &status
    ));
}

#[tokio::test]
async fn grpc_in_flight_request_interrupted_at_handoff_is_not_marked_replayable() {
    let dir = temp_socket_dir();
    let socket_path = dir.path().join("sandbox-manager.sock");
    let listener =
        pplx_space_common_svc::bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600))
            .unwrap();
    let service = SandboxManagerService::for_test(rusqlite::Connection::open_in_memory().unwrap());
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let server_shutdown = shutdown_token.clone();
    let response_ready = Arc::new(tokio::sync::Notify::new());
    let held_service = HoldResponseService::new(
        InterceptedService::new(
            SandboxManagerServer::new(service.clone()),
            HandoffDrainInterceptor::new(rpc_drain_token.clone()),
        ),
        Arc::clone(&response_ready),
    );
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .layer(HandoffStreamCancellationLayer::new(
                rpc_drain_token.clone(),
                ActiveResponseStreams::default(),
            ))
            .layer(service.restart_admission_layer())
            .add_service(held_service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                server_shutdown.cancelled().await
            }),
    );
    let channel = pplx_space_common_grpc::connect_uds_channel("sandbox-manager", &socket_path)
        .await
        .unwrap();
    let mut client = crate::proto::sandbox_manager_client::SandboxManagerClient::new(channel);
    let request = tokio::spawn(async move {
        client
            .stats(crate::proto::StatsRequest {
                sandbox_id: "sandbox".to_string(),
            })
            .await
    });
    response_ready.notified().await;

    rpc_drain_token.cancel();
    let status = request
        .await
        .unwrap()
        .expect_err("handoff must interrupt the held response");
    shutdown_token.cancel();
    server.await.unwrap().unwrap();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        !pplx_space_sbx_mgr_client::is_restart_retry_on_successor(&status),
        "an already-dispatched request must not authorize replay"
    );
}

#[tokio::test(start_paused = true)]
async fn monitored_drain_logs_multiple_entries_and_refreshes_at_one_total_budget() {
    let active_work = Arc::new(Mutex::new(vec![
        ("/space.sandbox.manager.v1.SandboxManager/Start", 1),
        ("/space.sandbox.manager.v1.SandboxManager/Update", 2),
    ]));
    let capture = HandoffEventCapture::default();
    let events = Arc::clone(&capture.events);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(capture));

    let drain = {
        let warning_work = Arc::clone(&active_work);
        let timeout_work = Arc::clone(&active_work);
        tokio::spawn(
            async move {
                monitor_drain(
                    DEFAULT_DRAIN_TIMEOUT,
                    DRAIN_BLOCKER_WARNING_THRESHOLD,
                    std::future::pending::<()>(),
                    move || {
                        emit_handoff_unfinished_work(
                            "pre_handoff",
                            "state_transition",
                            &warning_work.lock().unwrap(),
                            DRAIN_BLOCKER_WARNING_THRESHOLD,
                            "task_tracker,snapshot_worker,other_uninstrumented_work",
                        );
                    },
                    move || timeout_work.lock().unwrap().clone(),
                )
                .await
            }
            .with_subscriber(dispatch),
        )
    };
    tokio::task::yield_now().await;

    tokio::time::advance(DRAIN_BLOCKER_WARNING_THRESHOLD - Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(events.lock().unwrap().is_empty());
    assert!(!drain.is_finished());

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            CapturedEvent {
                message: Some(UNFINISHED_WORK_MESSAGE.to_string()),
                drain_phase: Some("pre_handoff".to_string()),
                work_kind: Some("state_transition".to_string()),
                operation: Some("/space.sandbox.manager.v1.SandboxManager/Start".to_string()),
                active_count: Some(1),
                drain_elapsed_secs: Some(10),
                possible_uninstrumented_phases: None,
            },
            CapturedEvent {
                message: Some(UNFINISHED_WORK_MESSAGE.to_string()),
                drain_phase: Some("pre_handoff".to_string()),
                work_kind: Some("state_transition".to_string()),
                operation: Some("/space.sandbox.manager.v1.SandboxManager/Update".to_string()),
                active_count: Some(2),
                drain_elapsed_secs: Some(10),
                possible_uninstrumented_phases: None,
            },
        ]
    );
    assert!(!drain.is_finished());

    *active_work.lock().unwrap() = vec![(
        "/space.sandbox.manager.v1.SandboxManager/PrepareTeardownSuspend",
        3,
    )];
    tokio::time::advance(
        DEFAULT_DRAIN_TIMEOUT - DRAIN_BLOCKER_WARNING_THRESHOLD - Duration::from_secs(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(events.lock().unwrap().len(), 2);
    assert!(!drain.is_finished());

    tokio::time::advance(Duration::from_secs(1)).await;
    let result = drain.await.unwrap();

    assert_eq!(
        result,
        DrainMonitorResult::TimedOut(vec![(
            "/space.sandbox.manager.v1.SandboxManager/PrepareTeardownSuspend",
            3,
        )])
    );

    tracing::subscriber::with_default(
        tracing_subscriber::registry().with(HandoffEventCapture {
            events: Arc::clone(&events),
        }),
        || {
            let DrainMonitorResult::TimedOut(timeout_snapshot) = &result else {
                unreachable!("pending drain must time out");
            };
            emit_handoff_unfinished_work(
                "pre_handoff",
                "state_transition",
                timeout_snapshot,
                DEFAULT_DRAIN_TIMEOUT,
                "task_tracker,snapshot_worker,other_uninstrumented_work",
            );
        },
    );
    assert_eq!(events.lock().unwrap().len(), 3);
    assert_eq!(
        events.lock().unwrap().last(),
        Some(&CapturedEvent {
            message: Some(UNFINISHED_WORK_MESSAGE.to_string()),
            drain_phase: Some("pre_handoff".to_string()),
            work_kind: Some("state_transition".to_string()),
            operation: Some(
                "/space.sandbox.manager.v1.SandboxManager/PrepareTeardownSuspend".to_string()
            ),
            active_count: Some(3),
            drain_elapsed_secs: Some(25),
            possible_uninstrumented_phases: None,
        })
    );
}

#[tokio::test]
async fn uds_handoff_cancels_existing_stream_while_successor_serves_new_connections() {
    let dir = temp_socket_dir();
    let socket_path = dir.path().join("sandbox-manager.sock");
    let mutex_path = dir.path().join("service.mutex");
    let active_mutex = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .unwrap();
    let old_rpc_drain_token = CancellationToken::new();
    let active_response_streams = ActiveResponseStreams::default();
    let (_old_reporter, old_shutdown_token, old_server) = spawn_health_server(
        &socket_path,
        "old",
        old_rpc_drain_token.clone(),
        active_response_streams.clone(),
    )
    .await;
    let mut old_client = health_client("old-sandbox-manager", &socket_path).await;
    let mut old_watch = old_client
        .watch(health_request("old"))
        .await
        .unwrap()
        .into_inner();
    let initial = old_watch.message().await.unwrap().unwrap();
    assert_eq!(initial.status, HealthStatus::Serving as i32);

    let handoff_shutdown_token = old_shutdown_token.clone();
    let handoff_rpc_drain_token = old_rpc_drain_token.clone();
    let active_response_streams_for_handoff = active_response_streams.clone();
    let handoff = tokio::spawn(async move {
        let mut active_mutex = Some(active_mutex);
        handoff_production_servers(
            PRE_HANDOFF_DRAIN_TIMEOUT,
            POST_HANDOFF_RPC_DRAIN_TIMEOUT,
            async { true },
            &handoff_rpc_drain_token,
            &handoff_shutdown_token,
            || {
                drop(active_mutex.take());
                Ok::<(), anyhow::Error>(())
            },
            HandoffServers {
                pre_handoff_snapshot: &Vec::new,
                active_response_streams: &active_response_streams_for_handoff,
                handles: vec![("old-sandbox-manager", old_server)],
            },
        )
        .await
        .unwrap()
    });
    let successor_mutex = ServiceMutex::acquire(
        &mutex_path,
        CancellationToken::new(),
        Duration::from_millis(10),
        Some(Duration::from_secs(1)),
    )
    .await
    .expect("successor should acquire the service mutex while the old stream is open");
    let (_new_reporter, new_shutdown_token, new_server) = spawn_health_server(
        &socket_path,
        "new",
        CancellationToken::new(),
        ActiveResponseStreams::default(),
    )
    .await;

    let new_status = pplx_space_common_grpc::retry_grpc_status_with_context(
        "sandbox-manager",
        "HealthCheck",
        pplx_space_common_grpc::DEFAULT_LOCAL_GRPC_RETRY_POLICY,
        pplx_space_common_grpc::GrpcRetryContext::none(),
        || {
            let mut client = old_client.clone();
            async move { client.check(health_request("new")).await }
        },
    )
    .await
    .expect("the existing client channel must retry onto the successor")
    .into_inner();
    assert_eq!(new_status.status, HealthStatus::Serving as i32);

    let old_results = tokio::time::timeout(Duration::from_secs(1), handoff)
        .await
        .expect("handoff should cancel accepted streams instead of waiting for them")
        .unwrap();
    let HandoffDrainResult::Server(old_results) = old_results else {
        panic!("old server should stop at handoff");
    };
    assert_eq!(old_results.len(), 1);
    let old_server_result = old_results.into_iter().next().unwrap().1;
    old_server_result
        .expect("old server task should join cleanly")
        .expect("old server should stop cleanly");
    let old_stream_status = tokio::time::timeout(Duration::from_secs(1), old_watch.message())
        .await
        .expect("old stream should terminate promptly at handoff")
        .expect_err("old stream should identify the retryable handoff");
    assert_eq!(old_stream_status.code(), tonic::Code::Unavailable);
    assert!(
        !pplx_space_sbx_mgr_client::is_restart_retry_on_successor(&old_stream_status),
        "an already-started stream must not authorize replay"
    );
    assert!(
        active_response_streams.snapshot().is_empty(),
        "completed handoff streams must release their blocker registration"
    );
    drop(old_client);

    new_shutdown_token.cancel();
    tokio::time::timeout(Duration::from_secs(1), new_server)
        .await
        .expect("new server should stop cleanly")
        .unwrap()
        .unwrap();
    drop(successor_mutex);
}

#[tokio::test]
async fn pre_handoff_work_does_not_consume_server_drain_budget() {
    let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_flag = released.clone();
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let server_drain_token = rpc_drain_token.clone();
    let server = tokio::spawn(async move {
        server_drain_token.cancelled().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    });

    let result = handoff_production_servers(
        Duration::from_millis(75),
        Duration::from_millis(75),
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            true
        },
        &rpc_drain_token,
        &shutdown_token,
        move || {
            release_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), anyhow::Error>(())
        },
        HandoffServers {
            pre_handoff_snapshot: &Vec::new,
            active_response_streams: &ActiveResponseStreams::default(),
            handles: vec![("sandbox-manager", server)],
        },
    )
    .await
    .unwrap();

    let HandoffDrainResult::Server(results) = result else {
        panic!("both drain phases should stay within their independent budgets");
    };
    results.into_iter().next().unwrap().1.unwrap().unwrap();
    assert!(released.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn nonquiescent_pre_handoff_stops_server_without_releasing_ownership() {
    let server =
        tokio::spawn(async { std::future::pending::<Result<(), tonic::transport::Error>>().await });
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let released = Arc::new(AtomicBool::new(false));
    let release_flag = Arc::clone(&released);

    let result = handoff_production_servers(
        Duration::from_secs(1),
        Duration::from_secs(1),
        async { false },
        &rpc_drain_token,
        &shutdown_token,
        move || {
            release_flag.store(true, Ordering::SeqCst);
            Ok::<(), anyhow::Error>(())
        },
        HandoffServers {
            pre_handoff_snapshot: &Vec::new,
            active_response_streams: &ActiveResponseStreams::default(),
            handles: vec![("sandbox-manager", server)],
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        result,
        HandoffDrainResult::PreHandoffNotQuiescent(ref snapshot) if snapshot.is_empty()
    ));
    assert!(!rpc_drain_token.is_cancelled());
    assert!(!released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pre_handoff_timeout_stops_server_tasks_before_returning() {
    struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let _notify = NotifyOnDrop(Some(stopped_tx));
        started_tx.send(()).unwrap();
        std::future::pending::<Result<(), tonic::transport::Error>>().await
    });
    let abort_handle = server.abort_handle();
    started_rx.await.unwrap();
    let rpc_drain_token = CancellationToken::new();
    let shutdown_token = CancellationToken::new();
    let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_flag = released.clone();

    let result = handoff_production_servers(
        Duration::from_millis(1),
        Duration::from_millis(1),
        std::future::pending(),
        &rpc_drain_token,
        &shutdown_token,
        move || {
            release_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), anyhow::Error>(())
        },
        HandoffServers {
            pre_handoff_snapshot: &Vec::new,
            active_response_streams: &ActiveResponseStreams::default(),
            handles: vec![("sandbox-manager", server)],
        },
    )
    .await
    .unwrap();

    let stopped_before_cleanup = tokio::time::timeout(Duration::from_millis(50), stopped_rx)
        .await
        .is_ok();
    abort_handle.abort();
    assert!(matches!(
        result,
        HandoffDrainResult::PreHandoffTimedOut(ref snapshot) if snapshot.is_empty()
    ));
    assert!(
        !rpc_drain_token.is_cancelled(),
        "a pre-handoff timeout must not authorize replay on the successor"
    );
    assert!(!released.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        stopped_before_cleanup,
        "a pre-handoff timeout must not detach live server tasks"
    );
}

#[tokio::test]
async fn daemon_migrations_converge_jobs_from_each_supported_schema() {
    use pplx_space_common_jobs::{JobPriority, JobStatus, db as jobs};
    for schema in ["fresh", "converged"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sandbox-manager.db");
        if schema != "fresh" {
            migrate_sandbox_manager_db(&path).await.unwrap();
            let conn = rusqlite::Connection::open(&path).unwrap();
            jobs::insert_job(
                &conn,
                jobs::InsertJob {
                    job_id: &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
                    kind: "sandbox_cleanup",
                    payload: "retained intent",
                    subject_id: Some("retained sandbox"),
                    dedup_key: Some("retained sandbox"),
                    max_attempts: 9,
                    traceparent: None,
                    priority: JobPriority::default(),
                },
            )
            .unwrap();
            jobs::mark_job_running(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
            )
            .unwrap();
        }
        migrate_sandbox_manager_db(&path).await.unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.prepare("SELECT activation_attempt_id, operation_activation_attempt_id FROM sandbox_cleanup_episodes")
            .expect("real daemon migrations install independent operation ownership");
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            3,
            "{schema}"
        );
        conn.prepare("SELECT auto_lifecycle_delete_mode FROM sandboxes")
            .unwrap_err();

        if schema != "fresh" {
            let retained = jobs::get_job(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(retained.payload, "retained intent", "{schema}");
            assert_eq!(retained.status, JobStatus::Running.as_str(), "{schema}");
            assert_eq!(retained.attempts, 1, "{schema}");
        }
        jobs::insert_job(
            &conn,
            jobs::InsertJob {
                job_id: &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap(),
                kind: "sandbox_cleanup",
                payload: "{}",
                subject_id: None,
                dedup_key: None,
                max_attempts: 3,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .unwrap();
        jobs::mark_job_running(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap(),
        )
        .unwrap();
        jobs::request_job_cancellation(
            &conn,
            &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap(),
        )
        .unwrap();
        assert!(
            jobs::mark_job_cancelled(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
            )
            .unwrap()
        );
        assert_eq!(
            jobs::get_job(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
            )
            .unwrap()
            .unwrap()
            .status,
            JobStatus::Cancelled.as_str(),
            "{schema}"
        );
        drop(conn);
        migrate_sandbox_manager_db(&path).await.unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name = ?1",
                ["idx_sandbox_cleanup_due"],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1,
            "{schema}: required access path survives schema convergence"
        );
        assert_eq!(
            jobs::get_job(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000002".parse().unwrap()
            )
            .unwrap()
            .unwrap()
            .status,
            JobStatus::Cancelled.as_str(),
            "{schema}: repeat startup preserves terminal state"
        );
    }
}

#[tokio::test]
async fn daemon_migration_failure_preserves_durable_jobs() {
    use pplx_space_common_jobs::{JobPriority, db as jobs};

    for history in ["_sqlx_migrations", "_space_common_schema_migrations"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sandbox-manager.db");
        migrate_sandbox_manager_db(&path).await.unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();

        jobs::insert_job(
            &conn,
            jobs::InsertJob {
                job_id: &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap(),
                kind: "sandbox_cleanup",
                payload: "durable cleanup intent",
                subject_id: None,
                dedup_key: None,
                max_attempts: 3,
                traceparent: None,
                priority: JobPriority::default(),
            },
        )
        .unwrap();
        conn.execute(&format!("UPDATE {history} SET success = FALSE"), [])
            .unwrap();
        drop(conn);

        assert!(
            migrate_sandbox_manager_db(&path).await.is_err(),
            "{history}"
        );
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            jobs::get_job(
                &conn,
                &"019d84bd-280d-7331-99c6-000000000001".parse().unwrap()
            )
            .unwrap()
            .unwrap()
            .payload,
            "durable cleanup intent",
            "{history}: startup must fail without resetting the database"
        );
    }
}
