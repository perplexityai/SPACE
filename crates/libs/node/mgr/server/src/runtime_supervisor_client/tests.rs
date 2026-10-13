use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response};

use pplx_space_node_runtime_core::proto;

#[tokio::test]
async fn retries_rpc_across_supervisor_socket_replacement() {
    let (_temp_dir, socket_path) = test_socket_path();
    let predecessor_calls = Arc::new(AtomicUsize::new(0));
    let successor_calls = Arc::new(AtomicUsize::new(0));
    let connect_count = Arc::new(AtomicUsize::new(0));
    let predecessor = spawn_supervisor_server(
        &socket_path,
        StubRuntimeSupervisor::counting(predecessor_calls.clone()),
    )
    .await;
    let client = supervisor_test_client(socket_path.clone(), connect_count.clone());
    client
        .launch_firecracker(proto::LaunchFirecrackerRequest::default())
        .await
        .unwrap();
    stop_supervisor_server(predecessor).await;
    std::fs::remove_file(&socket_path).unwrap();
    let successor_socket_path = socket_path.clone();
    let successor_calls_for_server = successor_calls.clone();
    let successor = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        spawn_supervisor_server(
            &successor_socket_path,
            StubRuntimeSupervisor::counting(successor_calls_for_server),
        )
        .await
    });

    let response = client
        .launch_firecracker(proto::LaunchFirecrackerRequest::default())
        .await
        .unwrap();

    assert_eq!(response.into_inner().pid, STUB_LAUNCH_PID);
    assert_eq!(predecessor_calls.load(Ordering::SeqCst), 1);
    assert_eq!(successor_calls.load(Ordering::SeqCst), 1);
    assert!(connect_count.load(Ordering::SeqCst) > 1);
    stop_supervisor_server(successor.await.unwrap()).await;
}

/// The graceful-shutdown race: systemctl restart SIGTERMs the supervisor
/// while node-manager holds a live cached HTTP/2 channel. Tonic sends GOAWAY
/// on that connection and keeps draining an in-flight RPC; a NEW rpc issued
/// through the same cached channel in that window is rejected client-side
/// ("operation was canceled: connection closed") without ever reaching a
/// server, and must be replayed against the successor. Awaiting predecessor
/// termination before the next RPC (as the socket-replacement test does)
/// never exercises this window.
#[tokio::test]
async fn replays_rpc_raced_by_graceful_predecessor_shutdown() {
    let (_temp_dir, socket_path) = test_socket_path();
    let predecessor_calls = Arc::new(AtomicUsize::new(0));
    let successor_calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Notify::new());
    let predecessor = spawn_supervisor_server(
        &socket_path,
        StubRuntimeSupervisor::gated(predecessor_calls.clone(), gate.clone()),
    )
    .await;
    let client = supervisor_test_client(socket_path.clone(), Arc::new(AtomicUsize::new(0)));

    let inflight = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .launch_firecracker(proto::LaunchFirecrackerRequest::default())
                .await
        })
    };
    let entered_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while predecessor_calls.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < entered_deadline,
            "in-flight RPC never reached the predecessor"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Graceful shutdown while the RPC is in flight: GOAWAY goes out on the
    // shared connection, the listener closes, the in-flight RPC keeps
    // draining.
    predecessor.shutdown.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let raced = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .launch_firecracker(proto::LaunchFirecrackerRequest::default())
                .await
        })
    };

    // Let the predecessor finish draining and exit, then bring up the
    // successor on the same path — the raced RPC's replay must land there.
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.notify_one();
    predecessor.task.await.unwrap();
    std::fs::remove_file(&socket_path).unwrap();
    let successor = spawn_supervisor_server(
        &socket_path,
        StubRuntimeSupervisor::counting(successor_calls.clone()),
    )
    .await;

    let raced_response = raced.await.unwrap().expect(
        "an RPC raced by graceful predecessor shutdown never reached a server \
         and must be replayed against the successor",
    );
    assert_eq!(raced_response.into_inner().pid, STUB_LAUNCH_PID);
    inflight.await.unwrap().unwrap();
    assert_eq!(predecessor_calls.load(Ordering::SeqCst), 1);
    assert_eq!(successor_calls.load(Ordering::SeqCst), 1);
    stop_supervisor_server(successor).await;
}

#[tokio::test]
async fn does_not_retry_server_returned_error() {
    let (_temp_dir, socket_path) = test_socket_path();
    let calls = Arc::new(AtomicUsize::new(0));
    let server = spawn_supervisor_server(
        &socket_path,
        StubRuntimeSupervisor::rejecting(calls.clone()),
    )
    .await;
    let client = supervisor_test_client(socket_path, Arc::new(AtomicUsize::new(0)));

    let status = client
        .launch_firecracker(proto::LaunchFirecrackerRequest::default())
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop_supervisor_server(server).await;
}

pub(crate) struct StubSupervisorServer {
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn spawn_supervisor_server<T>(socket_path: &Path, service: T) -> StubSupervisorServer
where
    T: proto::runtime_supervisor_server::RuntimeSupervisor,
{
    let listener = tokio::net::UnixListener::bind(socket_path).unwrap();
    let incoming = UnixListenerStream::new(listener);
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (_, health_service) = pplx_space_common_svc::grpc_health_service();
        tonic::transport::Server::builder()
            .add_service(health_service)
            .add_service(proto::runtime_supervisor_server::RuntimeSupervisorServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_receiver.await;
            })
            .await
            .unwrap();
    });
    StubSupervisorServer { shutdown, task }
}

pub(crate) async fn stop_supervisor_server(server: StubSupervisorServer) {
    server.shutdown.send(()).unwrap();
    server.task.await.unwrap();
}

fn supervisor_test_client(
    socket_path: PathBuf,
    connect_count: Arc<AtomicUsize>,
) -> RuntimeSupervisorLazyClient {
    RuntimeSupervisorLazyClient::from_connect_fn(Box::new(move || {
        let socket_path = socket_path.clone();
        let connect_count = connect_count.clone();
        Box::pin(async move {
            connect_count.fetch_add(1, Ordering::SeqCst);
            let channel = tonic::transport::Endpoint::try_from("http://[::1]:50051")
                .map_err(|error| Status::unavailable(format!("test endpoint: {error}")))?
                .connect_with_connector(tower::service_fn(move |_| {
                    let socket_path = socket_path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(socket_path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await
                .map_err(|error| {
                    Status::unavailable(format!("connect to runtime-supervisor failed: {error}"))
                })?;
            prove_live_and_wrap(channel).await
        })
    }))
}

fn test_socket_path() -> (tempfile::TempDir, PathBuf) {
    let temp_dir = tempfile::tempdir().unwrap();
    let socket_path = temp_dir.path().join("runtime-supervisor.sock");
    (temp_dir, socket_path)
}

const STUB_LAUNCH_PID: u32 = 42;

pub(crate) async fn stop_failure_client(
    code: tonic::Code,
) -> (
    tempfile::TempDir,
    RuntimeSupervisorLazyClient,
    StubSupervisorServer,
) {
    let (root, path) = test_socket_path();
    let mut stub = StubRuntimeSupervisor::counting(Arc::new(AtomicUsize::new(0)));
    stub.stop_status = code;
    let server = spawn_supervisor_server(&path, stub).await;
    let client = supervisor_test_client(path, Arc::new(AtomicUsize::new(0)));
    (root, client, server)
}

/// Stop fails with `code` `failures` times, then succeeds.
pub(crate) async fn flaky_stop_client(
    code: tonic::Code,
    failures: usize,
) -> (
    tempfile::TempDir,
    RuntimeSupervisorLazyClient,
    StubSupervisorServer,
) {
    let (root, path) = test_socket_path();
    let mut stub = StubRuntimeSupervisor::counting(Arc::new(AtomicUsize::new(0)));
    stub.stop_status = code;
    stub.stop_failures = Some(Arc::new(AtomicUsize::new(failures)));
    let server = spawn_supervisor_server(&path, stub).await;
    let client = supervisor_test_client(path, Arc::new(AtomicUsize::new(0)));
    (root, client, server)
}

/// Answers the stub's `stop_process` RPC with the request it received.
pub(crate) type StopProcessReply = Arc<
    dyn Fn(proto::StopProcessRequest) -> Result<proto::StopProcessResponse, Status> + Send + Sync,
>;

/// Answers the stub's `update_cgroup` RPC with the request it received.
pub(crate) type UpdateCgroupReply = Arc<
    dyn Fn(proto::UpdateCgroupRequest) -> Result<proto::UpdateCgroupResponse, Status> + Send + Sync,
>;

/// Supervisor replies for node-manager RPC tests; `None` keeps the stub's
/// default reply. Runtime stop always fails with `Unimplemented`, so a refusal
/// that regresses past its check ends at the stop instead of host cleanup.
#[derive(Clone, Default)]
pub(crate) struct SupervisorScript {
    pub(crate) bind_execution_failure: Option<tonic::Code>,
    pub(crate) stop_process: Option<StopProcessReply>,
    pub(crate) update_cgroup: Option<UpdateCgroupReply>,
}

pub(crate) async fn scripted_client(
    script: SupervisorScript,
) -> (
    tempfile::TempDir,
    RuntimeSupervisorLazyClient,
    StubSupervisorServer,
) {
    let (root, path) = test_socket_path();
    let mut stub = StubRuntimeSupervisor::counting(Arc::new(AtomicUsize::new(0)));
    stub.stop_status = tonic::Code::Unimplemented;
    stub.script = script;
    let server = spawn_supervisor_server(&path, stub).await;
    let client = supervisor_test_client(path, Arc::new(AtomicUsize::new(0)));
    (root, client, server)
}

/// [`scripted_client`] served in process, with no socket to own.
pub(crate) fn scripted_in_process_client(script: SupervisorScript) -> RuntimeSupervisorLazyClient {
    let mut stub = StubRuntimeSupervisor::counting(Arc::new(AtomicUsize::new(0)));
    stub.stop_status = tonic::Code::Unimplemented;
    stub.script = script;
    in_process_client(stub)
}

/// The unit-test service's default supervisor: every RPC fails with
/// `Unimplemented` at once. It is not a connect failure, so the client does
/// not spend its restart budget retrying, and no host process is contacted.
pub(crate) fn refusing_client() -> RuntimeSupervisorLazyClient {
    in_process_client(RefusingRuntimeSupervisor)
}

/// Each dial serves `service` over an in-memory stream and passes the
/// production liveness check, as a dial to the supervisor socket does.
fn in_process_client<T>(service: T) -> RuntimeSupervisorLazyClient
where
    T: proto::runtime_supervisor_server::RuntimeSupervisor + Clone,
{
    RuntimeSupervisorLazyClient::from_connect_fn(Box::new(move || {
        let service = service.clone();
        Box::pin(async move {
            let channel = tonic::transport::Endpoint::try_from("http://runtime-supervisor.invalid")
                .map_err(|error| Status::unavailable(format!("test endpoint: {error}")))?
                .connect_with_connector(tower::service_fn(move |_| {
                    let (client, server) = tokio::io::duplex(64 * 1024);
                    let service = service.clone();
                    tokio::spawn(async move {
                        let (_, health_service) = pplx_space_common_svc::grpc_health_service();
                        tonic::transport::Server::builder()
                            .add_service(health_service)
                            .add_service(
                                proto::runtime_supervisor_server::RuntimeSupervisorServer::new(
                                    service,
                                ),
                            )
                            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(
                                server,
                            )))
                            .await
                            .unwrap();
                    });
                    async move { Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client)) }
                }))
                .await
                .map_err(|error| {
                    Status::unavailable(format!("connect to runtime-supervisor failed: {error}"))
                })?;
            prove_live_and_wrap(channel).await
        })
    }))
}

#[derive(Clone)]
struct RefusingRuntimeSupervisor;

fn refused(rpc: &str) -> Status {
    Status::unimplemented(format!(
        "unit-test runtime supervisor refuses {rpc}; inject a scripted supervisor"
    ))
}

#[tonic::async_trait]
impl proto::runtime_supervisor_server::RuntimeSupervisor for RefusingRuntimeSupervisor {
    async fn bind_execution(
        &self,
        _request: Request<proto::BindExecutionRequest>,
    ) -> Result<Response<proto::BindExecutionResponse>, Status> {
        Err(refused("BindExecution"))
    }
    async fn retire_execution(
        &self,
        _request: Request<proto::RetireExecutionRequest>,
    ) -> Result<Response<proto::RetireExecutionResponse>, Status> {
        Err(refused("RetireExecution"))
    }
    async fn launch_firecracker(
        &self,
        _request: Request<proto::LaunchFirecrackerRequest>,
    ) -> Result<Response<proto::LaunchFirecrackerResponse>, Status> {
        Err(refused("LaunchFirecracker"))
    }
    async fn launch_sidecar(
        &self,
        _request: Request<proto::LaunchSidecarRequest>,
    ) -> Result<Response<proto::LaunchSidecarResponse>, Status> {
        Err(refused("LaunchSidecar"))
    }
    async fn stop_process(
        &self,
        _request: Request<proto::StopProcessRequest>,
    ) -> Result<Response<proto::StopProcessResponse>, Status> {
        Err(refused("StopProcess"))
    }
    async fn stop_sandbox_runtime(
        &self,
        _request: Request<proto::StopSandboxRuntimeRequest>,
    ) -> Result<Response<proto::StopSandboxRuntimeResponse>, Status> {
        Err(refused("StopSandboxRuntime"))
    }
    async fn update_cgroup(
        &self,
        _request: Request<proto::UpdateCgroupRequest>,
    ) -> Result<Response<proto::UpdateCgroupResponse>, Status> {
        Err(refused("UpdateCgroup"))
    }
}

#[derive(Clone)]
struct StubRuntimeSupervisor {
    launch_firecracker_calls: Arc<AtomicUsize>,
    reject_launch_firecracker: bool,
    launch_gate: Option<Arc<tokio::sync::Notify>>,
    stop_status: tonic::Code,
    /// When set, stop fails with `stop_status` this many times, then succeeds.
    stop_failures: Option<Arc<AtomicUsize>>,
    script: SupervisorScript,
}

impl StubRuntimeSupervisor {
    fn counting(calls: Arc<AtomicUsize>) -> Self {
        Self {
            launch_firecracker_calls: calls,
            reject_launch_firecracker: false,
            launch_gate: None,
            stop_status: tonic::Code::Unimplemented,
            stop_failures: None,
            script: SupervisorScript::default(),
        }
    }

    fn rejecting(calls: Arc<AtomicUsize>) -> Self {
        Self {
            launch_firecracker_calls: calls,
            reject_launch_firecracker: true,
            launch_gate: None,
            stop_status: tonic::Code::Unimplemented,
            stop_failures: None,
            script: SupervisorScript::default(),
        }
    }

    /// Handler blocks until the gate is notified, holding its RPC in flight
    /// so a graceful shutdown drains around it.
    fn gated(calls: Arc<AtomicUsize>, gate: Arc<tokio::sync::Notify>) -> Self {
        Self {
            launch_firecracker_calls: calls,
            reject_launch_firecracker: false,
            launch_gate: Some(gate),
            stop_status: tonic::Code::Unimplemented,
            stop_failures: None,
            script: SupervisorScript::default(),
        }
    }
}

#[tonic::async_trait]
impl proto::runtime_supervisor_server::RuntimeSupervisor for StubRuntimeSupervisor {
    async fn bind_execution(
        &self,
        _request: Request<proto::BindExecutionRequest>,
    ) -> Result<Response<proto::BindExecutionResponse>, Status> {
        if let Some(code) = self.script.bind_execution_failure {
            return Err(Status::new(code, "bind execution unavailable"));
        }
        Ok(Response::new(proto::BindExecutionResponse {}))
    }
    async fn retire_execution(
        &self,
        _request: Request<proto::RetireExecutionRequest>,
    ) -> Result<Response<proto::RetireExecutionResponse>, Status> {
        Ok(Response::new(proto::RetireExecutionResponse {}))
    }
    async fn launch_firecracker(
        &self,
        _request: Request<proto::LaunchFirecrackerRequest>,
    ) -> Result<Response<proto::LaunchFirecrackerResponse>, Status> {
        self.launch_firecracker_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.launch_gate {
            gate.notified().await;
        }
        if self.reject_launch_firecracker {
            return Err(Status::internal("firecracker launch failed"));
        }
        Ok(Response::new(proto::LaunchFirecrackerResponse {
            pid: STUB_LAUNCH_PID,
        }))
    }

    async fn launch_sidecar(
        &self,
        _request: Request<proto::LaunchSidecarRequest>,
    ) -> Result<Response<proto::LaunchSidecarResponse>, Status> {
        Err(Status::unimplemented("launch_sidecar"))
    }

    async fn stop_process(
        &self,
        request: Request<proto::StopProcessRequest>,
    ) -> Result<Response<proto::StopProcessResponse>, Status> {
        match &self.script.stop_process {
            Some(reply) => reply(request.into_inner()).map(Response::new),
            None => Err(Status::unimplemented("stop_process")),
        }
    }

    async fn stop_sandbox_runtime(
        &self,
        request: Request<proto::StopSandboxRuntimeRequest>,
    ) -> Result<Response<proto::StopSandboxRuntimeResponse>, Status> {
        if request.get_ref().execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "test stop requires explicit identity",
            ));
        }
        if let Some(failures) = &self.stop_failures
            && failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_err()
        {
            return Ok(Response::new(proto::StopSandboxRuntimeResponse::default()));
        }
        Err(Status::new(self.stop_status, "stop completion unavailable"))
    }

    async fn update_cgroup(
        &self,
        request: Request<proto::UpdateCgroupRequest>,
    ) -> Result<Response<proto::UpdateCgroupResponse>, Status> {
        match &self.script.update_cgroup {
            Some(reply) => reply(request.into_inner()).map(Response::new),
            None => Err(Status::unimplemented("update_cgroup")),
        }
    }
}

fn status_with_source(
    code: tonic::Code,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Status {
    let mut status = Status::new(code, "test");
    status.set_source(Arc::new(source));
    status
}

#[test]
fn refused_before_transmission_requires_a_typed_source() {
    // A bare Cancelled — including one decoded from wire trailers — carries
    // no local transport source and can describe a request that executed.
    assert!(!is_request_refused_before_transmission(&Status::cancelled(
        "operation was canceled"
    )));
    assert!(!is_request_refused_before_transmission(&Status::cancelled(
        "connection closed"
    )));
}

#[test]
fn refused_stream_is_refused_before_transmission() {
    // The peer only sends REFUSED_STREAM for streams it did not process.
    let status = status_with_source(
        tonic::Code::Unavailable,
        h2::Error::from(h2::Reason::REFUSED_STREAM),
    );
    assert!(is_request_refused_before_transmission(&status));
}

#[test]
fn other_h2_reasons_are_not_refused_before_transmission() {
    // A mid-stream reset may have executed server-side.
    let status = status_with_source(tonic::Code::Cancelled, h2::Error::from(h2::Reason::CANCEL));
    assert!(!is_request_refused_before_transmission(&status));
}

#[test]
fn server_returned_codes_are_never_refused_before_transmission() {
    let status = status_with_source(
        tonic::Code::Internal,
        h2::Error::from(h2::Reason::REFUSED_STREAM),
    );
    assert!(!is_request_refused_before_transmission(&status));
}
