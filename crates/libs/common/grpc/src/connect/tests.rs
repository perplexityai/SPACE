use std::path::Path;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Request;
use tonic_health::pb::health_client::HealthClient;

async fn serve_health(socket_path: &Path) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::UnixListener::bind(socket_path).expect("bind test socket");
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve test health service");
    });
    (shutdown_tx, task)
}

fn health_request() -> Request<tonic_health::pb::HealthCheckRequest> {
    let mut request = Request::new(tonic_health::pb::HealthCheckRequest {
        service: String::new(),
    });
    request.set_timeout(Duration::from_secs(1));
    request
}

#[tokio::test(start_paused = true)]
async fn stalled_uds_attempt_shares_total_budget_with_prior_retries() {
    let budget = Duration::from_secs(1);
    let policy = crate::GrpcRetryPolicy {
        max_elapsed: budget,
        jitter_factor: 0.0,
        ..crate::DEFAULT_GRPC_CONNECT_RETRY_POLICY
    };
    let started = tokio::time::Instant::now();
    let attempts = std::cell::Cell::new(0);
    // A pending connector represents socket establishment or preface-write
    // backpressure, not a peer withholding SETTINGS (which need not block connect).
    let connect = super::retry_uds_connect("stalled", Path::new("/pending.sock"), policy, || {
        let attempt = attempts.get() + 1;
        attempts.set(attempt);
        async move {
            if attempt == 1 {
                Err(super::UdsConnectError::Endpoint(
                    tonic::transport::Endpoint::from_shared("http://[".to_string())
                        .expect_err("invalid URI"),
                ))
            } else {
                std::future::pending().await
            }
        }
    });
    let result = tokio::time::timeout(budget * 2, connect)
        .await
        .expect("an in-flight attempt must not bypass the total connect budget");
    assert!(matches!(
        result,
        Err(super::UdsConnectError::Timeout { .. })
    ));
    assert_eq!(attempts.get(), 2);
    assert_eq!(started.elapsed(), budget);
}

#[tokio::test]
async fn lazy_uds_channel_reconnects_after_socket_replacement() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket_path = dir.path().join("backend.sock");
    let (predecessor_shutdown, predecessor_task) = serve_health(&socket_path).await;
    let endpoint =
        tonic::transport::Endpoint::try_from("http://[::]:50051").expect("static test endpoint");
    let channel = super::connect_uds_channel_lazy(&socket_path, endpoint);
    let mut client = HealthClient::new(channel);

    client
        .check(health_request())
        .await
        .expect("predecessor health check");
    predecessor_shutdown
        .send(())
        .expect("stop predecessor health service");
    predecessor_task.await.expect("join predecessor");
    std::fs::remove_file(&socket_path).expect("remove predecessor socket");

    let successor_path = socket_path.clone();
    let successor = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        serve_health(&successor_path).await
    });
    let response = crate::fwd_retry!("test-backend", client, check, health_request())
        .expect("health check reconnects to successor");

    assert_eq!(
        response.into_inner().status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
    let (successor_shutdown, successor_task) = successor.await.expect("start successor");
    successor_shutdown
        .send(())
        .expect("stop successor health service");
    successor_task.await.expect("join successor");
}

async fn serve_tcp_health(
    listener: tokio::net::TcpListener,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .expect("serve test health service");
    });
    (shutdown_tx, task)
}

// A kept channel whose peer restarts waits for it on the next call: the redial
// retries until the peer listens again, instead of failing the call.
#[tokio::test]
async fn a_kept_tcp_channel_waits_for_its_peer_to_come_back() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    let channel = super::connect_tcp_channel_with_retrying_redials(
        "test-backend",
        endpoint,
        Duration::from_secs(1),
        None,
        crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    client.check(health_request()).await.expect("served");

    stop.send(()).unwrap();
    server.await.unwrap();
    let mut waiting = client.clone();
    let call = tokio::spawn(async move {
        let mut request = health_request();
        request.set_timeout(Duration::from_secs(10));
        waiting.check(request).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!call.is_finished(), "the call waits while the peer is down");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    call.await
        .unwrap()
        .expect("the redial reaches the restarted peer");
    stop.send(()).unwrap();
    server.await.unwrap();
}

fn stalled_tcp_policy(budget: Duration) -> crate::GrpcRetryPolicy {
    crate::GrpcRetryPolicy {
        max_elapsed: budget,
        jitter_factor: 0.0,
        ..crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY
    }
}

// An attempt that never finishes, such as a DNS lookup or TLS handshake no peer
// answers, is abandoned at its cap and redialed while the budget remains.
#[tokio::test(start_paused = true)]
async fn a_stalled_tcp_attempt_is_abandoned_at_its_cap_and_redialed() {
    let cap = Duration::from_secs(1);
    let policy = stalled_tcp_policy(Duration::from_secs(10));
    let started = tokio::time::Instant::now();
    let attempts = std::cell::Cell::new(0);
    let connect = super::retry_tcp_connect("stalled", "stalled:1", policy, cap, || {
        let attempt = attempts.get() + 1;
        attempts.set(attempt);
        async move {
            if attempt == 1 {
                std::future::pending().await
            } else {
                Ok::<_, super::TcpConnectError>(attempt)
            }
        }
    });
    let connected = tokio::time::timeout(policy.max_elapsed, connect)
        .await
        .expect("a stalled attempt must not hold the connect")
        .expect("the redial connects");
    assert_eq!(connected, 2);
    assert_eq!(started.elapsed(), cap + policy.initial_delay);
}

// Attempts that never finish still end the call within the budget plus the one
// attempt the budget let start before it ran out.
#[tokio::test(start_paused = true)]
async fn stalled_tcp_attempts_end_within_the_budget_and_one_attempt_cap() {
    let cap = Duration::from_secs(1);
    let policy = stalled_tcp_policy(Duration::from_secs(3));
    let started = tokio::time::Instant::now();
    let attempts = std::cell::Cell::new(0);
    let connect = super::retry_tcp_connect("stalled", "stalled:1", policy, cap, || {
        attempts.set(attempts.get() + 1);
        std::future::pending::<Result<(), super::TcpConnectError>>()
    });
    let result = tokio::time::timeout(policy.max_elapsed * 2, connect)
        .await
        .expect("stalled attempts must not bypass the attempt cap");
    assert!(
        matches!(result, Err(super::TcpConnectError::AttemptTimeout { cap: fired, .. }) if fired == cap),
        "{result:?}"
    );
    assert!(attempts.get() > 1, "the budget allowed redials");
    let elapsed = started.elapsed();
    assert!(
        elapsed > policy.max_elapsed && elapsed <= policy.max_elapsed + cap,
        "gave up after {elapsed:?}"
    );
}

// An endpoint that sets no dial bound is dialed with the default one the
// attempt cap is sized from: a dial whose SYN is never answered fails with the
// dial bound's `TimedOut` and is redialed, instead of holding its attempt
// until the cap abandons it. Under the paused clock the kernel cannot time the
// dial out first. Linux drops a SYN to a listener whose accept queue is full,
// which stands in for a SYN pinned to a stale conntrack entry.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn an_endpoint_without_a_dial_bound_is_redialed_at_the_default_one() {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(2).unwrap();
    let address = listener.local_addr().unwrap();
    // Linux counts the accept queue full only once it holds more than the
    // backlog, so three unaccepted connections fill it. Dialing them one at a
    // time keeps at most one handshake pending, so the SYN queue never counts
    // as full and the setup does not depend on SYN cookies.
    let mut queued = Vec::new();
    for _ in 0..3 {
        queued.push(tokio::net::TcpStream::connect(address).await.unwrap());
    }
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    assert_eq!(endpoint.get_connect_timeout(), None);

    let retries = pplx_space_metrics::test_capture::capture_counters_guard();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        super::connect_tcp_channel_with_retry(
            "blackhole",
            address.to_string(),
            endpoint,
            None,
            crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        ),
    )
    .await
    .expect("an unanswered dial must be bounded");
    let retries = retries
        .finish()
        .into_iter()
        .filter(|counter| counter.name == "grpc.client.retry")
        .count();
    let error = match result {
        Err(error @ super::TcpConnectError::Transport(_)) => error,
        other => panic!("expected the dial bound to fail the dial: {other:?}"),
    };
    let timed_out = std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .any(|error| {
        error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
    });
    assert!(timed_out, "the dial bound did not fire: {error:?}");
    assert!(retries > 0, "the failed dial was not redialed");
    drop(queued);
}

#[cfg(feature = "internal-tls")]
#[derive(Default)]
struct TestEnvironment(std::collections::HashMap<&'static str, std::ffi::OsString>);

#[cfg(feature = "internal-tls")]
impl pplx_space_common_core::env::Environment for TestEnvironment {
    fn var_os(&self, key: &str) -> Option<std::ffi::OsString> {
        self.0.get(key).cloned()
    }
}

#[cfg(feature = "internal-tls")]
fn install_crypto_provider() {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        rustls::crypto::CryptoProvider::get_default()
            .expect("a rejected provider installation leaves an installed provider");
    }
}

/// A CA certificate PEM file in `dir`, trusted for `127.0.0.1`.
#[cfg(feature = "internal-tls")]
fn write_test_ca(dir: &Path) -> std::path::PathBuf {
    let ca = dir.join("ca.pem");
    let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    std::fs::write(&ca, certificate.cert.pem()).unwrap();
    ca
}

/// A loopback peer that accepts every TCP connection and never answers, and
/// its address.
#[cfg(feature = "internal-tls")]
async fn silent_tls_peer() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            held.push(listener.accept().await.unwrap().0);
        }
    });
    (address, peer)
}

/// Assert that a connect to a silent peer failed with the TLS handshake bound's
/// own cause, not as an abandoned attempt, and not before `handshake_bound`.
#[cfg(feature = "internal-tls")]
fn assert_failed_at_handshake_bound(
    result: Result<tonic::transport::Channel, super::TcpConnectError>,
    elapsed: Duration,
    handshake_bound: Duration,
) {
    let error = match result {
        Err(error @ super::TcpConnectError::Transport(_)) => error,
        other => panic!("expected the TLS handshake bound to fail the dial: {other:?}"),
    };
    let chain = std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    assert!(
        chain
            .iter()
            .any(|message| message.contains("TLS handshake timeout")),
        "{chain:?}"
    );
    assert!(elapsed >= handshake_bound, "failed after {elapsed:?}");
}

// A peer that accepts TCP but never answers the ClientHello fails the connect
// with the TLS handshake bound's own cause, not as an abandoned attempt and not
// at once. The bound outlasts the dial and name-resolution shares of the cap,
// so the cap leaves room for it only by counting it. This waits out the real
// bound: a paused clock could fire the cap before the loopback dial's
// readiness is observed.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn a_tls_peer_that_never_answers_fails_the_connect_at_its_handshake_bound() {
    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = std::fs::read(write_test_ca(dir.path())).unwrap();
    let handshake_bound = Duration::from_secs(4);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500))
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(ca))
                .timeout(handshake_bound),
        )
        .unwrap();

    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        super::connect_tcp_channel_with_retry(
            "silent-tls",
            address.to_string(),
            endpoint,
            Some(handshake_bound),
            crate::GrpcRetryPolicy::none(),
        ),
    )
    .await
    .expect("an unanswered TLS handshake must be bounded");
    assert_failed_at_handshake_bound(result, started.elapsed(), handshake_bound);
    silent_peer.abort();
}

// An internal-RPC connect to a peer that never answers the ClientHello fails
// with the handshake bound the transport's own TLS config applies, which is
// the bound the attempt cap is sized from: here it outlasts the dial and
// name-resolution shares, so the cap must count it for its cause to surface.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn an_internal_rpc_connect_fails_at_its_transports_handshake_bound() {
    use crate::internal_transport::{CA_ENV, ClientTransport, Service};

    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let mut env = TestEnvironment::default();
    env.0
        .insert(CA_ENV, write_test_ca(dir.path()).into_os_string());
    let handshake_bound = Duration::from_secs(4);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500));
    let endpoint = ClientTransport::from_environment_with_tls_handshake_timeout(
        Service::NodeApi,
        endpoint.uri(),
        &env,
        handshake_bound,
    )
    .await
    .unwrap()
    .connect_endpoint(endpoint)
    .unwrap();

    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        endpoint.connect_with_retry(
            "silent-internal-rpc",
            address.to_string(),
            crate::GrpcRetryPolicy::none(),
        ),
    )
    .await
    .expect("an unanswered TLS handshake must be bounded");
    assert_failed_at_handshake_bound(result, started.elapsed(), handshake_bound);
    silent_peer.abort();
}
