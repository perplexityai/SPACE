#![cfg(test)]

//! Transport-timing contract of the shared gRPC server builders: the
//! observation-only builder every listener starts from must add no request
//! deadline of its own, and a deadline configured on top of it must still
//! expire.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status};

/// Longer than `REQUEST_TIMEOUT`, short enough to keep the test fast.
const HANDLER_DELAY: Duration = Duration::from_millis(400);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(100);
const HANDLER_COMPLETED: &str = "handler-completed";
const SLOW_METHOD: &str = "/space.test.v1.Slow/Wait";

/// A client that sees `HANDLER_COMPLETED` observed no server-side deadline;
/// one that sees `CANCELLED` observed one.
#[derive(Clone)]
struct SlowService;

impl tower::Service<http::Request<tonic::body::Body>> for SlowService {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(async move {
            tokio::time::sleep(HANDLER_DELAY).await;
            Ok(Status::internal(HANDLER_COMPLETED).into_http())
        })
    }
}

impl tonic::server::NamedService for SlowService {
    const NAME: &'static str = "space.test.v1.Slow";
}

async fn call_slow_method(addr: std::net::SocketAddr) -> Status {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let codec = tonic_prost::ProstCodec::<
        tonic_health::pb::HealthCheckRequest,
        tonic_health::pb::HealthCheckResponse,
    >::default();
    client
        .unary(
            Request::new(tonic_health::pb::HealthCheckRequest::default()),
            http::uri::PathAndQuery::from_static(SLOW_METHOD),
            codec,
        )
        .await
        .expect_err("the slow handler never returns a message")
}

#[tokio::test]
async fn observation_only_builder_imposes_no_request_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        pplx_space_common_grpc::grpc_server_builder()
            .add_service(SlowService)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let status = call_slow_method(addr).await;
    assert_eq!(status.code(), Code::Internal, "status: {status:?}");
    assert_eq!(status.message(), HANDLER_COMPLETED);

    server.abort();
}

#[tokio::test]
async fn configured_request_timeout_still_expires_through_the_observation_layers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        pplx_space_grpc_server::grpc_server_builder_with_request_timeout(REQUEST_TIMEOUT)
            .add_service(SlowService)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let status = call_slow_method(addr).await;
    assert_eq!(status.code(), Code::Cancelled, "status: {status:?}");

    server.abort();
}
