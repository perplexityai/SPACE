use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::response::Response;
use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_node_api_client::proto::{
    DataPlaneBackendEndpoint, UnstableResolveSandboxServiceRouteResponse,
};
use pplx_space_sandbox_model::SandboxId;
use prost::Message as _;
use tokio::net::TcpSocket;
use tokio_util::sync::CancellationToken;

use crate::native::{RouteResolutionError, resolve_session_route};
use crate::runtime_route::{EcwRouteTransport, resolve_ecw_route};
use crate::tests::{grpc_test_response, route_response_with_token, test_data_plane_state};
use crate::{
    ClusterApiClients, DATA_PLANE_TOKEN_PREFIX, DataPlaneSessionId, EcwSandboxHost, SessionKind,
};

fn enter_insecure_test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_TOKEN", Some(std::ffi::OsStr::new("test"))),
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
    ])
}

/// A ClusterApi replica on a fixed address that can be stopped the way a
/// rollout stops one, gracefully, and brought back on the same address, as
/// the Service's next replica answers there.
struct RestartingClusterApi {
    addr: SocketAddr,
    /// Bound to `addr` for the replica's whole life and never listening, so
    /// that no other socket on the host takes the port while no replica
    /// listens on it, and a dial there is refused.
    _port: TcpSocket,
    resolves: Arc<AtomicUsize>,
    answer: fn() -> Response,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl RestartingClusterApi {
    async fn start(answer: fn() -> Response) -> Self {
        let socket = shared_port_socket();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1024).unwrap();
        let addr = listener.local_addr().unwrap();
        let port = shared_port_socket();
        port.bind(addr).unwrap();
        let resolves = Arc::new(AtomicUsize::new(0));
        let stop = CancellationToken::new();
        let server = serve(listener, resolves.clone(), answer, stop.clone());
        Self {
            addr,
            _port: port,
            resolves,
            answer,
            stop,
            server,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stop the replica, wait until its connections are closed and its
    /// address refuses, then run `during` while the address stays refused for
    /// `down_for` and the next replica comes back there.
    ///
    /// The next replica is brought back on the test's own task, alongside
    /// `during`, not in a spawned one: a replica that cannot listen fails the
    /// test when `down_for` ends, instead of leaving `during` to retry against
    /// a refused address until its retry window ends.
    async fn restart<T>(&mut self, down_for: Duration, during: impl Future<Output = T>) -> T {
        self.stop.cancel();
        (&mut self.server).await.unwrap();
        let refused = tokio::net::TcpStream::connect(self.addr).await;
        assert_eq!(
            refused.err().map(|error| error.kind()),
            Some(std::io::ErrorKind::ConnectionRefused),
            "a stopped replica's address refuses"
        );
        let next = shared_port_socket();
        next.bind(self.addr).unwrap();
        self.stop = CancellationToken::new();
        let come_back = async {
            tokio::time::sleep(down_for).await;
            let listener = next
                .listen(1024)
                .expect("the next replica listens on the stopped replica's address");
            self.server = serve(
                listener,
                self.resolves.clone(),
                self.answer,
                self.stop.clone(),
            );
        };
        let (result, ()) = tokio::join!(during, come_back);
        result
    }
}

/// A socket that can share a replica's port with the socket holding it. A
/// socket that does not set `SO_REUSEPORT` cannot bind that port.
fn shared_port_socket() -> TcpSocket {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_reuseport(true).unwrap();
    socket
}

fn serve(
    listener: tokio::net::TcpListener,
    resolves: Arc<AtomicUsize>,
    answer: fn() -> Response,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let service = Router::new().fallback(move |request: Request<Body>| {
        let resolves = resolves.clone();
        async move {
            if request.uri().path().ends_with("/Check") {
                return grpc_test_response(vec![8, 1]);
            }
            resolves.fetch_add(1, Ordering::SeqCst);
            answer()
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, service)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
    })
}

fn ecw_route_answer() -> Response {
    let node_id = NodeId::new().to_string();
    grpc_test_response(
        UnstableResolveSandboxServiceRouteResponse {
            sandbox_id: SandboxId::new().to_string(),
            org_id: OrgId::new().to_string(),
            port: crate::ECW_ENVD_PORT,
            lifecycle: "started".into(),
            route_generation: 1,
            current_node_id: node_id.clone(),
            backend: Some(DataPlaneBackendEndpoint {
                node_id,
                endpoint: "http://worker:9800".into(),
                transport: None,
            }),
            template_id: None,
            space_api_route_token: None,
            service_port_ingress_token: None,
        }
        .encode_to_vec(),
    )
}

fn session_route_answer() -> Response {
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    grpc_test_response(route_response_with_token(session_id, None).encode_to_vec())
}

fn route_not_ready_answer() -> Response {
    pplx_space_common_grpc::sandbox_service_route_not_ready_status(
        tonic::Code::Unavailable,
        "sandbox service route is not ready",
    )
    .into_http::<Body>()
}

async fn state_for(cluster_api: &RestartingClusterApi) -> crate::DataPlaneState {
    let mut state = test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(cluster_api.endpoint(), "test")
            .await
            .unwrap(),
    );
    state
}

fn envd_target() -> EcwSandboxHost {
    EcwSandboxHost {
        sandbox_id: SandboxId::new(),
        port: crate::ECW_ENVD_PORT,
    }
}

/// A resolve sent while the data plane's ClusterApi replica restarts, so that
/// its connection is gone and the address refuses for a moment, gets the route
/// from the replica that comes back instead of failing.
#[tokio::test]
async fn ecw_route_resolve_rides_out_a_cluster_api_restart() {
    if !enter_insecure_test_process() {
        return;
    }
    let mut cluster_api = RestartingClusterApi::start(ecw_route_answer).await;
    let state = state_for(&cluster_api).await;
    let target = envd_target();

    let route = cluster_api
        .restart(
            Duration::from_millis(300),
            resolve_ecw_route(
                &state,
                &target,
                &HeaderMap::new(),
                EcwRouteTransport::Direct,
            ),
        )
        .await;

    assert!(route.is_ok(), "{:?}", route.err());
    assert_eq!(cluster_api.resolves.load(Ordering::SeqCst), 1);
}

/// The Native session route is resolved the same way.
#[tokio::test]
async fn session_route_resolve_rides_out_a_cluster_api_restart() {
    if !enter_insecure_test_process() {
        return;
    }
    let mut cluster_api = RestartingClusterApi::start(session_route_answer).await;
    let state = state_for(&cluster_api).await;

    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
    let route = cluster_api
        .restart(
            Duration::from_millis(300),
            resolve_session_route(&state, session_id, &token),
        )
        .await;

    assert!(route.is_ok(), "{:?}", route.err());
    assert_eq!(cluster_api.resolves.load(Ordering::SeqCst), 1);
}

/// An `Unavailable` that ClusterApi answered is its verdict on the sandbox, not
/// a transport failure, so it is reported once rather than sent again.
#[tokio::test]
async fn answered_unavailable_is_not_sent_again() {
    if !enter_insecure_test_process() {
        return;
    }
    let cluster_api = RestartingClusterApi::start(route_not_ready_answer).await;
    let state = state_for(&cluster_api).await;

    let ecw = resolve_ecw_route(
        &state,
        &envd_target(),
        &HeaderMap::new(),
        EcwRouteTransport::Direct,
    )
    .await;
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
    let native = resolve_session_route(&state, session_id, &token).await;

    let Err(ecw) = ecw else {
        panic!("a route-not-ready answer resolved a route");
    };
    assert_eq!(
        ecw.kind,
        pplx_space_public_api_http::metrics::ResponseKind::RouteNotReady
    );
    // A session attach reads the marked answer as a route on its way.
    assert!(matches!(native, Err(RouteResolutionError::SessionNotReady)));
    assert_eq!(cluster_api.resolves.load(Ordering::SeqCst), 2);
}
