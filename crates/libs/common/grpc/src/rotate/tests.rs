use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Context;
use std::time::Duration;

use tokio_stream::StreamExt as _;
use tonic::transport::Endpoint;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse, health_check_response};

use super::{RotatingChannel, max_connection_age_from_value};

/// Only an explicit positive number enables rotation. Everything else is off,
/// so a deployment that never sets this keeps the connection behavior it had.
#[test]
fn only_an_explicit_positive_env_value_enables_rotation() {
    for (value, expected) in [
        (Some("90"), Some(Duration::from_secs(90))),
        (Some(" 90 "), Some(Duration::from_secs(90))),
        (None, None),
        (Some(""), None),
        (Some("0"), None),
        (Some("abc"), None),
        (Some("-5"), None),
        (Some("5s"), None),
    ] {
        assert_eq!(
            max_connection_age_from_value(value),
            expected,
            "value {value:?}"
        );
    }
}

/// Services keep one long-lived clone and clone *it* per request. If a clone
/// carried a channel, that template would hold the process's first connection
/// open for its whole life — one connection permanently pinned to one pod,
/// surviving every rotation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpolled_clone_holds_no_connection() {
    let (addr, _connections) = serve_health().await;
    let template = connect(addr, None).await;
    assert!(
        template.current.is_none(),
        "a freshly built channel should not hold a connection before it is polled"
    );
    assert!(
        template.clone().current.is_none(),
        "cloning must not copy the readied channel"
    );

    let mut polled = template.clone();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let _ = tower::Service::poll_ready(&mut polled, &mut cx);
    assert!(
        polled.current.is_some(),
        "poll_ready must leave the readied channel in place for call"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_without_an_age_never_redials() {
    let (addr, connections) = serve_health().await;
    let mut client = HealthClient::new(connect(addr, None).await);

    for _ in 0..20 {
        check(&mut client).await.expect("request succeeds");
    }

    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "a channel with no age must stay on its first connection"
    );
}

/// The whole point of the type, and the property that makes it safe to enable:
/// it redials on the age, and does so without failing a request, because the
/// replacement is connected before traffic moves to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aged_channel_redials_without_failing_requests() {
    let (addr, connections) = serve_health().await;
    let mut client = HealthClient::new(connect(addr, Some(Duration::from_millis(100))).await);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        check(&mut client).await.expect("rotation failed a request");
    }

    // The initial dial plus at least two rotations; the age is jittered, so the
    // exact count is not fixed.
    assert!(
        connections.load(Ordering::SeqCst) >= 3,
        "expected repeated redials, saw {}",
        connections.load(Ordering::SeqCst)
    );
}

async fn connect(addr: SocketAddr, max_age: Option<Duration>) -> RotatingChannel {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).expect("endpoint");
    RotatingChannel::connect("test", endpoint, None, max_age)
        .await
        .expect("connect")
}

async fn check(client: &mut HealthClient<RotatingChannel>) -> Result<(), tonic::Status> {
    let request = HealthCheckRequest {
        service: String::new(),
    };
    client.check(request).await.map(|_| ())
}

/// A health listener that counts accepted TCP connections, which is how a test
/// observes redialing. The server task ends with the test's runtime.
async fn serve_health() -> (SocketAddr, Arc<AtomicU64>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let connections = Arc::new(AtomicU64::new(0));
    let accepted = connections.clone();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |conn| {
        if conn.is_ok() {
            accepted.fetch_add(1, Ordering::SeqCst);
        }
        conn
    });
    tokio::spawn(async move {
        let _ = crate::grpc_server_builder()
            .add_service(HealthServer::new(AlwaysServing))
            .serve_with_incoming(incoming)
            .await;
    });
    (addr, connections)
}

struct AlwaysServing;

#[tonic::async_trait]
impl Health for AlwaysServing {
    async fn check(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(HealthCheckResponse {
            status: health_check_response::ServingStatus::Serving as i32,
        }))
    }

    type WatchStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<HealthCheckResponse, tonic::Status>> + Send>,
    >;

    async fn watch(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("watch"))
    }
}
