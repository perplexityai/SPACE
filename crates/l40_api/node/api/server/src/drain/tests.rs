use std::convert::Infallible;
use std::future::ready;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;
use pplx_space_node_api_client::proto::worker_api_client::WorkerApiClient;
use pplx_space_sbx_mgr_client::proto::StateRequest;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Clone)]
struct CountingService {
    calls: Arc<AtomicUsize>,
}

impl Service<Request<tonic::body::Body>> for CountingService {
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<tonic::body::Body>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok(Response::new(tonic::body::Body::empty())))
    }
}

impl tonic::server::NamedService for CountingService {
    const NAME: &'static str = "space.node.api.v1.WorkerApi";
}

#[tokio::test]
async fn rejects_space_rpc_while_draining_before_inner_service() {
    let drain_signal = pplx_space_common_svc::DrainSignal::new();
    drain_signal.begin();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut service = NodeApiDrainLayer::new(drain_signal).layer(CountingService {
        calls: Arc::clone(&calls),
    });
    let request = Request::builder()
        .uri("/space.node.api.v1.WorkerApi/SandboxState")
        .body(tonic::body::Body::empty())
        .unwrap();

    let err = service.call(request).await.unwrap_err();

    let status = err.downcast_ref::<Status>().unwrap();
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(status.message(), NODE_API_DRAINING_MESSAGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn allows_health_rpc_while_draining() {
    let drain_signal = pplx_space_common_svc::DrainSignal::new();
    drain_signal.begin();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut service = NodeApiDrainLayer::new(drain_signal).layer(CountingService {
        calls: Arc::clone(&calls),
    });
    let request = Request::builder()
        .uri("/grpc.health.v1.Health/Check")
        .body(tonic::body::Body::empty())
        .unwrap();

    let response = service.call(request).await.unwrap();

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn grpc_client_observes_unavailable_while_draining() {
    let drain_signal = pplx_space_common_svc::DrainSignal::new();
    drain_signal.begin();
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tonic::transport::Server::builder()
        .layer(NodeApiDrainLayer::new(drain_signal))
        .add_service(CountingService {
            calls: Arc::clone(&calls),
        })
        .serve_with_incoming(TcpListenerStream::new(listener));
    let server_handle = tokio::spawn(server);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut client = WorkerApiClient::new(channel);

    let status = client
        .sandbox_state(StateRequest::default())
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(status.message(), NODE_API_DRAINING_MESSAGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server_handle.abort();
    let _ = server_handle.await;
}
