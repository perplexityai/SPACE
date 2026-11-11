use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::Full;
use pplx_space_common_grpc::GrpcRetryPolicy;
use pplx_space_node_api_client::{NodeApiClient, WorkerApiClient, WorkerApiInternalClient};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    body::Body,
    codegen::http::{Request, Response},
    transport::{Endpoint, Server},
};
use tower::Service;

const UPDATE_SANDBOX: &str = "/space.node.api.v1.WorkerApi/UpdateSandbox";
const SET_AUTO_LIFECYCLE: &str = "/space.node.api.v1.WorkerApi/SetSandboxAutoLifecyclePolicy";
const SET_NETWORK_POLICY: &str = "/space.node.api.v1.WorkerApi/SetSandboxNetworkPolicy";
const SPACED_WAIT_READY: &str = "/space.node.api.v1.WorkerApi/SpacedWaitReady";
const PREPARE_TEARDOWN_SUSPEND: &str = "/space.node.api.v1.WorkerApi/PrepareTeardownSuspend";
const INTERNAL_WAKE_SANDBOX: &str = "/space.node.api.v1.WorkerApiInternal/WakeSandbox";

fn test_retry_policy() -> GrpcRetryPolicy {
    GrpcRetryPolicy {
        max_retries: 2,
        initial_delay: Duration::ZERO,
        backoff_multiplier: 1.0,
        max_delay: Duration::ZERO,
        max_elapsed: Duration::from_secs(1),
        jitter_factor: 0.0,
        max_server_retry_after: Duration::from_millis(10),
    }
}

struct TestNodeApi {
    endpoint: Endpoint,
    state: Arc<Mutex<FaultState>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestNodeApi {
    async fn spawn(routes: impl IntoIterator<Item = (&'static str, Vec<Fault>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test node-api listener");
        let addr = listener.local_addr().expect("read test listener address");
        let state = Arc::new(Mutex::new(FaultState::new(routes)));
        let service = FaultyGrpcService {
            state: Arc::clone(&state),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            Server::builder()
                .serve_with_incoming_shutdown(
                    service,
                    TcpListenerStream::new(listener),
                    async move {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("serve test node-api");
        });

        Self {
            endpoint: Endpoint::from_shared(format!("http://{addr}"))
                .expect("build test node-api endpoint")
                .connect_timeout(Duration::from_millis(500))
                .timeout(Duration::from_secs(1)),
            state,
            shutdown: Some(shutdown_tx),
        }
    }

    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    fn calls(&self, path: &str) -> usize {
        self.state
            .lock()
            .expect("fault state mutex poisoned")
            .calls(path)
    }
}

impl Drop for TestNodeApi {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

struct FaultState {
    routes: BTreeMap<&'static str, RouteState>,
}

impl FaultState {
    fn new(routes: impl IntoIterator<Item = (&'static str, Vec<Fault>)>) -> Self {
        Self {
            routes: routes
                .into_iter()
                .map(|(path, faults)| (path, RouteState::new(faults)))
                .collect(),
        }
    }

    fn calls(&self, path: &str) -> usize {
        self.routes.get(path).map_or(0, |route| route.calls)
    }
}

struct RouteState {
    calls: usize,
    faults: VecDeque<Fault>,
}

impl RouteState {
    fn new(faults: Vec<Fault>) -> Self {
        Self {
            calls: 0,
            faults: VecDeque::from(faults),
        }
    }
}

struct Fault {
    code: Code,
    message: &'static str,
}

impl Fault {
    fn new(code: Code, message: &'static str) -> Self {
        Self { code, message }
    }
}

#[derive(Clone)]
struct FaultyGrpcService {
    state: Arc<Mutex<FaultState>>,
}

impl Service<Request<Body>> for FaultyGrpcService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let path = request.uri().path().to_string();
        let response = {
            let mut state = self.state.lock().expect("fault state mutex poisoned");
            let route = state
                .routes
                .get_mut(path.as_str())
                .expect("unexpected gRPC path");
            route.calls += 1;
            match route.faults.pop_front() {
                Some(fault) => grpc_status_response(fault.code, fault.message),
                None => grpc_ok_response(),
            }
        };
        Box::pin(async move { Ok(response) })
    }
}

fn grpc_status_response(code: Code, message: &str) -> Response<Body> {
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .header("grpc-status", (code as i32).to_string())
        .header("grpc-message", message)
        .body(Body::empty())
        .expect("build gRPC status response")
}

fn grpc_ok_response() -> Response<Body> {
    static EMPTY_GRPC_MESSAGE: &[u8] = &[0, 0, 0, 0, 0];
    Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(Body::new(Full::new(Bytes::from_static(EMPTY_GRPC_MESSAGE))))
        .expect("build gRPC ok response")
}

/// Node-api RPCs now attach a bearer (projected token, or the static
/// `SPACE_INTERNAL_TOKEN` fallback) and fail loud with `UNAVAILABLE` if neither
/// is set. These transport-retry tests don't exercise auth, so give the process
/// a static token once; the mock server ignores its value.

#[tokio::test]
async fn worker_rpcs_retry_transient_status_then_succeed() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-token")),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-control-token")),
        ),
    ]) {
        return;
    }

    let server = TestNodeApi::spawn([
        (
            UPDATE_SANDBOX,
            vec![Fault::new(
                Code::Internal,
                "transport error: connection reset",
            )],
        ),
        (
            SET_AUTO_LIFECYCLE,
            vec![Fault::new(Code::Unavailable, "node-api restarting")],
        ),
        (
            SET_NETWORK_POLICY,
            vec![Fault::new(Code::Unavailable, "node-api restarting")],
        ),
        (
            SPACED_WAIT_READY,
            vec![Fault::new(
                Code::Internal,
                "transport error: connection closed",
            )],
        ),
    ])
    .await;
    let mut node_api = NodeApiClient::new(server.endpoint()).with_retry_policy(test_retry_policy());

    {
        let mut client = WorkerApiClient::new(&mut node_api);
        client
            .update_sandbox(Default::default(), Duration::from_secs(1))
            .await
            .unwrap();
        client
            .set_sandbox_auto_lifecycle_policy(Default::default(), Duration::from_secs(1))
            .await
            .unwrap();
        client
            .set_sandbox_network_policy(Default::default(), Duration::from_secs(1))
            .await
            .unwrap();
        client
            .spaced_wait_ready(Default::default(), Duration::from_secs(1))
            .await
            .unwrap();
    }

    assert_eq!(server.calls(UPDATE_SANDBOX), 2);
    assert_eq!(server.calls(SET_AUTO_LIFECYCLE), 2);
    assert_eq!(server.calls(SET_NETWORK_POLICY), 2);
    assert_eq!(server.calls(SPACED_WAIT_READY), 2);
}

#[tokio::test]
async fn worker_internal_rpcs_use_internal_routes_and_retry_transient_status() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-token")),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-control-token")),
        ),
    ]) {
        return;
    }

    let server = TestNodeApi::spawn([(
        INTERNAL_WAKE_SANDBOX,
        vec![Fault::new(Code::Unavailable, "node-api restarting")],
    )])
    .await;
    let mut node_api = NodeApiClient::new(server.endpoint()).with_retry_policy(test_retry_policy());

    let mut client = WorkerApiInternalClient::new(&mut node_api);
    client
        .wake_sandbox(Default::default(), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(server.calls(INTERNAL_WAKE_SANDBOX), 2);
}

#[tokio::test]
async fn worker_rpc_does_not_retry_application_status() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-token")),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-control-token")),
        ),
    ]) {
        return;
    }

    let server = TestNodeApi::spawn([(
        UPDATE_SANDBOX,
        vec![Fault::new(Code::FailedPrecondition, "wrong owner")],
    )])
    .await;
    let mut node_api = NodeApiClient::new(server.endpoint()).with_retry_policy(test_retry_policy());

    let status = WorkerApiClient::new(&mut node_api)
        .update_sandbox(Default::default(), Duration::from_secs(1))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(server.calls(UPDATE_SANDBOX), 1);
}

#[tokio::test]
async fn worker_facade_exposes_generation_suspend_rpc() {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-token")),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN",
            Some(std::ffi::OsStr::new("retry-test-control-token")),
        ),
    ]) {
        return;
    }

    let server = TestNodeApi::spawn([(PREPARE_TEARDOWN_SUSPEND, vec![])]).await;
    let mut node_api = NodeApiClient::new(server.endpoint()).with_retry_policy(test_retry_policy());

    WorkerApiClient::new(&mut node_api)
        .prepare_teardown_suspend(Default::default(), Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(server.calls(PREPARE_TEARDOWN_SUSPEND), 1);
}
