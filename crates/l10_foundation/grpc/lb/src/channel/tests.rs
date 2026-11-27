//! Balancing behaviour against real tonic servers on loopback. Each server names
//! itself in its response, so per-endpoint request counts are exact.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tower::ServiceExt;

use super::*;

const METHOD: &str = "/space.test.v1.Echo/Call";

/// Long enough that no test but the expiry one can outrun it. The bound is real
/// behaviour, so a test slower than it would silently measure expiry instead of
/// what it meant to.
const NO_EXPIRY: Duration = Duration::from_secs(300);
const SHORT_BOUND: Duration = Duration::from_millis(200);

/// Answers with its own name and counts what it served. A raw tower service
/// rather than a generated client/server pair: the test only needs to know which
/// endpoint answered, which a trailers-only response already carries.
#[derive(Clone)]
struct NamedServer {
    name: &'static str,
    hits: Arc<AtomicUsize>,
    delay: Option<Duration>,
}

impl tower::Service<http::Request<tonic::body::Body>> for NamedServer {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<tonic::body::Body>) -> Self::Future {
        self.hits.fetch_add(1, Ordering::SeqCst);
        let name = self.name;
        let delay = self.delay;
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            Ok(tonic::Status::internal(name).into_http())
        })
    }
}

impl tonic::server::NamedService for NamedServer {
    const NAME: &'static str = "space.test.v1.Echo";
}

struct Server {
    name: &'static str,
    addr: SocketAddr,
    hits: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
}

impl Server {
    async fn spawn(name: &'static str, delay: Option<Duration>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let service = NamedServer {
            name,
            hits: hits.clone(),
            delay,
        };
        let connections = Arc::new(AtomicUsize::new(0));
        let incoming = TcpListenerStream::new(listener).inspect({
            let connections = connections.clone();
            move |connection| {
                if connection.is_ok() {
                    connections.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
        });
        Self {
            name,
            addr,
            hits,
            connections,
        }
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn configure(uri: Uri) -> Endpoint {
    Endpoint::from(uri).connect_timeout(Duration::from_secs(1))
}

fn channel_over(fallback: &Server, bound: Duration) -> (Channel, mpsc::Sender<Resolution>) {
    let (tx, rx) = mpsc::channel(8);
    let fallback_uri: Uri = format!("http://{}", fallback.addr).parse().unwrap();
    let channel = spawn_feed(
        "test",
        fallback_uri,
        configure,
        ReceiverStream::new(rx),
        bound,
        None,
    );
    (channel, tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_failure_keeps_the_service_fallback_rotating() {
    let fallback = Server::spawn("fallback", None).await;
    let fallback_uri: Uri = format!("http://{}", fallback.addr).parse().unwrap();
    let (endpoints, resolutions) = mpsc::channel(8);
    let channel = spawn_feed(
        "test",
        fallback_uri,
        configure,
        ReceiverStream::new(resolutions),
        NO_EXPIRY,
        Some(SHORT_BOUND),
    );

    assert_eq!(call(&channel).await, "fallback");
    endpoints.send(Resolution::Failed).await.unwrap();

    for _ in 0..100 {
        assert_eq!(call(&channel).await, "fallback");
        if fallback.connections() >= 2 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the Service fallback connection did not rotate");
}

fn set_of(servers: &[&Server]) -> EndpointSet {
    servers.iter().map(|server| server.addr).collect()
}

/// Issues one request and returns the name of the server that answered.
async fn call(channel: &Channel) -> String {
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri(METHOD)
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::empty())
        .unwrap();
    let response = channel.clone().oneshot(request).await.unwrap();
    tonic::Status::from_header_map(response.headers())
        .expect("trailers-only status")
        .message()
        .to_owned()
}

/// Retries until `expected` answers, so a test never races the feed task's
/// application of a set it just pushed.
async fn wait_until_served(channel: &Channel, expected: &str) {
    for _ in 0..100 {
        if call(channel).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{expected} never served a request");
}

#[tokio::test(flavor = "multi_thread")]
async fn balances_over_discovered_endpoints_and_falls_back_when_there_are_none() {
    let fallback = Server::spawn("fallback", None).await;
    let (a, b, c) = (
        Server::spawn("a", None).await,
        Server::spawn("b", None).await,
        Server::spawn("c", None).await,
    );
    let (channel, endpoints) = channel_over(&fallback, NO_EXPIRY);

    // Before the first resolution the fallback is the only endpoint, which is
    // where these requests would have gone without discovery at all.
    assert_eq!(call(&channel).await, "fallback");

    endpoints
        .send(Resolution::Endpoints(set_of(&[&a])))
        .await
        .unwrap();
    wait_until_served(&channel, "a").await;

    // Endpoints added later take traffic, and the fallback stops taking any.
    endpoints
        .send(Resolution::Endpoints(set_of(&[&a, &b, &c])))
        .await
        .unwrap();
    wait_until_served(&channel, "c").await;
    let fallback_before = fallback.hits();
    for _ in 0..60 {
        assert_ne!(
            call(&channel).await,
            "fallback",
            "fallback served while pods were ready"
        );
    }
    assert_eq!(fallback.hits(), fallback_before);
    for server in [&a, &b, &c] {
        assert!(server.hits() > 0, "{} served nothing", server.name);
    }

    // An empty resolution means "discovery has nothing to say", not "there are no
    // backends", so the fallback returns.
    endpoints
        .send(Resolution::Endpoints(EndpointSet::new()))
        .await
        .unwrap();
    wait_until_served(&channel, "fallback").await;
}

/// tower's `ReadyCache::evict` drops the evicted service. This pins what that
/// does to a request already dispatched to it, which is what decides whether
/// removal needs a drain window.
#[tokio::test(flavor = "multi_thread")]
async fn removal_does_not_cancel_a_request_already_in_flight() {
    let fallback = Server::spawn("fallback", None).await;
    let slow = Server::spawn("slow", Some(Duration::from_millis(500))).await;
    let (channel, endpoints) = channel_over(&fallback, NO_EXPIRY);

    endpoints
        .send(Resolution::Endpoints(set_of(&[&slow])))
        .await
        .unwrap();
    wait_until_served(&channel, "slow").await;

    let in_flight = tokio::spawn({
        let channel = channel.clone();
        async move { call(&channel).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        slow.hits(),
        2,
        "the request under test never reached the endpoint"
    );
    endpoints
        .send(Resolution::Endpoints(EndpointSet::new()))
        .await
        .unwrap();

    assert_eq!(
        in_flight.await.unwrap(),
        "slow",
        "a request already dispatched to a removed endpoint must still complete"
    );
}

/// A resolver that terminates never recovers, so termination itself must remove
/// the last set. The no-expiry bound proves this is the termination path and not
/// expiry.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminated_resolver_reverts_to_the_fallback() {
    let fallback = Server::spawn("fallback", None).await;
    let pod = Server::spawn("pod", None).await;
    let (channel, endpoints) = channel_over(&fallback, NO_EXPIRY);

    endpoints
        .send(Resolution::Endpoints(set_of(&[&pod])))
        .await
        .unwrap();
    wait_until_served(&channel, "pod").await;

    drop(endpoints);
    wait_until_served(&channel, "fallback").await;
}

/// Failure — never silence — starts the staleness clock, anchored to the first
/// failure of the outage. The failures repeat far faster than the bound here,
/// exactly as watch retries do in production: if each one re-anchored the
/// clock, a permanently broken watch would keep its endpoints forever and this
/// test would never see the fallback.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_failure_expires_the_endpoints_to_the_fallback() {
    let fallback = Server::spawn("fallback", None).await;
    let pod = Server::spawn("pod", None).await;
    let (channel, endpoints) = channel_over(&fallback, SHORT_BOUND);

    endpoints
        .send(Resolution::Endpoints(set_of(&[&pod])))
        .await
        .unwrap();
    wait_until_served(&channel, "pod").await;

    let failures = tokio::spawn({
        let endpoints = endpoints.clone();
        async move {
            while endpoints.send(Resolution::Failed).await.is_ok() {
                tokio::time::sleep(SHORT_BOUND / 4).await;
            }
        }
    });
    wait_until_served(&channel, "fallback").await;
    failures.abort();
}

/// The normal state of a stable Service is a silent watch: bookmarks are
/// consumed inside kube-runtime and a routine reconnect emits nothing. Silence
/// must therefore never expire a healthy set — only failure does.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_healthy_watch_keeps_its_endpoints_past_the_staleness_bound() {
    let fallback = Server::spawn("fallback", None).await;
    let pod = Server::spawn("pod", None).await;
    let (channel, endpoints) = channel_over(&fallback, SHORT_BOUND);

    endpoints
        .send(Resolution::Endpoints(set_of(&[&pod])))
        .await
        .unwrap();
    wait_until_served(&channel, "pod").await;

    tokio::time::sleep(3 * SHORT_BOUND).await;
    let fallback_before = fallback.hits();
    for _ in 0..20 {
        assert_eq!(
            call(&channel).await,
            "pod",
            "quiet watch lost its endpoints"
        );
    }
    assert_eq!(fallback.hits(), fallback_before);
}

/// A dropped channel drops the balancer's receiver; the feed must notice and
/// stop rather than keep the resolver — and its Kubernetes connection — alive
/// for a channel nobody holds.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_every_channel_handle_stops_the_feed() {
    let fallback = Server::spawn("fallback", None).await;
    let (channel, endpoints) = channel_over(&fallback, NO_EXPIRY);
    wait_until_served(&channel, "fallback").await;

    drop(channel);
    tokio::time::timeout(Duration::from_secs(5), endpoints.closed())
        .await
        .expect("feed kept running after every channel handle was dropped");
}
