use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::response::Response;
use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_common_core::types::SandboxId;
use pplx_space_node_api_client::proto::{
    DataPlaneBackendEndpoint, UnstableResolveSandboxServiceRouteResponse,
};
use prost::Message as _;
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
    resolves: Arc<AtomicUsize>,
    answer: fn() -> Response,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl RestartingClusterApi {
    async fn start(answer: fn() -> Response) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let resolves = Arc::new(AtomicUsize::new(0));
        let stop = CancellationToken::new();
        let server = serve(listener, resolves.clone(), answer, stop.clone());
        Self {
            addr,
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
    /// address refuses, and serve there again after `down_for`.
    async fn restart(&mut self, down_for: Duration) {
        self.stop.cancel();
        (&mut self.server).await.unwrap();
        let addr = self.addr;
        let resolves = self.resolves.clone();
        let answer = self.answer;
        let stop = CancellationToken::new();
        self.stop = stop.clone();
        self.server = tokio::spawn(async move {
            tokio::time::sleep(down_for).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            serve(listener, resolves, answer, stop).await.unwrap();
        });
    }
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
    cluster_api.restart(Duration::from_millis(300)).await;

    let route = resolve_ecw_route(
        &state,
        &envd_target(),
        &HeaderMap::new(),
        EcwRouteTransport::Direct,
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
    cluster_api.restart(Duration::from_millis(300)).await;

    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let token = format!("{DATA_PLANE_TOKEN_PREFIX}{}", "a".repeat(64));
    let route = resolve_session_route(&state, session_id, &token).await;

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
    assert!(matches!(
        native,
        Err(RouteResolutionError::ResolutionFailed)
    ));
    assert_eq!(cluster_api.resolves.load(Ordering::SeqCst), 2);
}
