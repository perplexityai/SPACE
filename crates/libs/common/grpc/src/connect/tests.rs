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

// A watched channel's watch stays open while its connection serves calls and
// closes once the peer goes away, without any call having to fail first.
#[tokio::test]
async fn a_watched_channel_reports_when_its_peer_closes_the_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));
    let (channel, watch) = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        address.to_string(),
        endpoint,
        None,
        crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    for _ in 0..3 {
        client.check(health_request()).await.expect("served");
    }
    assert!(!watch.is_closed(), "calls reuse the open connection");

    stop.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !watch.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the watch closes with the connection");
}

// The watch counts the connections its channel opens, so an RPC that had to
// redial after the connection ended can be told from one that waited on it.
#[tokio::test]
async fn a_watch_counts_the_redials_its_channel_makes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));
    let (channel, watch) = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        address.to_string(),
        endpoint,
        None,
        crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    client.check(health_request()).await.expect("served");
    client.check(health_request()).await.expect("served");
    assert_eq!(
        watch.connections_opened(),
        1,
        "calls share the dialed connection"
    );

    stop.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !watch.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the watch closes with the connection");
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    client
        .check(health_request())
        .await
        .expect("the channel redials the restarted peer");
    assert_eq!(watch.connections_opened(), 2, "the redial is counted");
    stop.send(()).unwrap();
    server.await.unwrap();
}

// A holder can retire a connection on its own evidence; every clone sees it.
#[tokio::test]
async fn closing_a_watch_is_seen_by_every_clone() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    let (channel, watch) = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        address.to_string(),
        endpoint,
        None,
        crate::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
    )
    .await
    .expect("first dial");
    let kept = watch.clone();
    watch.close();
    assert!(kept.is_closed());
    // Retiring it does not break the channel for a caller already holding it.
    HealthClient::new(channel)
        .check(health_request())
        .await
        .expect("still served");
    stop.send(()).unwrap();
    server.await.unwrap();
}

// A dial that cannot connect fails like the unwatched helper, so a caller
// that keeps channels sees the same error it would without keeping them.
#[tokio::test]
async fn a_watched_dial_to_a_closed_port_fails() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(200));
    let error = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        address.to_string(),
        endpoint,
        None,
        stalled_tcp_policy(Duration::from_millis(500)),
    )
    .await
    .expect_err("nothing listens");
    assert!(
        matches!(error, super::TcpConnectError::Transport(_)),
        "{error:?}"
    );
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

// The observer hears about every failed attempt that is redialed, with its
// count and cause, and not about the last one, whose error the call returns.
#[tokio::test(start_paused = true)]
async fn the_retry_observer_sees_each_redialed_attempt() {
    let cap = Duration::from_secs(1);
    let policy = stalled_tcp_policy(Duration::from_secs(10));
    let attempts = std::cell::Cell::new(0);
    let mut observed = Vec::new();
    let result = super::retry_tcp_connect_observed(
        "refused",
        "refused:1",
        policy,
        cap,
        || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            async move {
                Err::<(), _>(super::TcpConnectError::AttemptTimeout {
                    service_name: "refused",
                    target: format!("attempt {attempt}"),
                    cap,
                })
            }
        },
        |retry| observed.push((retry.attempts, retry.error.to_string())),
    )
    .await;
    assert!(result.is_err());
    let made = attempts.get();
    assert!(made > 1, "the budget allowed redials");
    let expected: Vec<_> = (1..made)
        .map(|attempt| {
            let error = super::TcpConnectError::AttemptTimeout {
                service_name: "refused",
                target: format!("attempt {attempt}"),
                cap,
            };
            (attempt, error.to_string())
        })
        .collect();
    assert_eq!(observed, expected);
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
    let blackhole = crate::test_support::Blackhole::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = blackhole.address();
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    assert_eq!(endpoint.get_connect_timeout(), None);

    let retries = pplx_space_metrics_emit::test_capture::capture_counters_guard();
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
    drop(blackhole);
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
// at once. The bound outlasts the dial and name-resolution shares of the cap
// and its margin, so the cap leaves room for it only by counting it. This
// waits out the real bound: a paused clock could fire the cap before the
// loopback dial's readiness is observed.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn a_tls_peer_that_never_answers_fails_the_connect_at_its_handshake_bound() {
    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = std::fs::read(write_test_ca(dir.path())).unwrap();
    let handshake_bound = Duration::from_secs(5);
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

/// An internal-RPC connect endpoint for `address` whose transport's TLS
/// config bounds the handshake at `handshake_bound`, with the CA it trusts
/// written to `dir`.
#[cfg(feature = "internal-tls")]
async fn internal_rpc_connect_endpoint(
    address: std::net::SocketAddr,
    dir: &Path,
    handshake_bound: Duration,
) -> crate::internal_transport::ConnectEndpoint {
    use crate::internal_transport::{CA_ENV, ClientTransport, Service};

    let mut env = TestEnvironment::default();
    env.0.insert(CA_ENV, write_test_ca(dir).into_os_string());
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500));
    ClientTransport::from_environment_with_tls_handshake_timeout(
        Service::NodeApi,
        endpoint.uri(),
        &env,
        handshake_bound,
    )
    .await
    .unwrap()
    .connect_endpoint(endpoint)
    .unwrap()
}

// An internal-RPC connect to a peer that never answers the ClientHello fails
// with the handshake bound the transport's own TLS config applies, which is
// the bound the attempt cap is sized from: here it outlasts the dial and
// name-resolution shares, so the cap must count it for its cause to surface.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn an_internal_rpc_connect_fails_at_its_transports_handshake_bound() {
    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let handshake_bound = Duration::from_secs(5);
    let endpoint = internal_rpc_connect_endpoint(address, dir.path(), handshake_bound).await;

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

// A watched internal-RPC connect is given the transport's handshake bound the
// same way, so a kept channel's attempt cap leaves the handshake its share.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn a_watched_internal_rpc_connect_fails_at_its_transports_handshake_bound() {
    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let handshake_bound = Duration::from_secs(5);
    let endpoint = internal_rpc_connect_endpoint(address, dir.path(), handshake_bound).await;

    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        endpoint.connect_with_retry_watched(
            "silent-internal-rpc",
            address.to_string(),
            crate::GrpcRetryPolicy::none(),
        ),
    )
    .await
    .expect("an unanswered TLS handshake must be bounded");
    assert_failed_at_handshake_bound(
        result.map(|(channel, _watch)| channel),
        started.elapsed(),
        handshake_bound,
    );
    silent_peer.abort();
}

// A watched dial layers the endpoint's TLS on its own connector, and gives
// the handshake the same bound as an unwatched dial: a peer that never answers
// the ClientHello fails with the handshake bound's cause, not an earlier cut.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn a_watched_dial_applies_tls_and_its_handshake_bound() {
    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let ca = std::fs::read(write_test_ca(dir.path())).unwrap();
    let handshake_bound = Duration::from_secs(5);
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
        super::connect_tcp_channel_with_retry_watched(
            "silent-tls",
            address.to_string(),
            endpoint,
            Some(handshake_bound),
            crate::GrpcRetryPolicy::none(),
        ),
    )
    .await
    .expect("an unanswered TLS handshake must be bounded");
    assert_failed_at_handshake_bound(
        result.map(|(channel, _watch)| channel),
        started.elapsed(),
        handshake_bound,
    );
    silent_peer.abort();
}

fn status_chain(
    status: &tonic::Status,
) -> impl Iterator<Item = &(dyn std::error::Error + 'static)> {
    std::iter::successors(std::error::Error::source(status), |error| error.source())
}

fn is_marked(status: &tonic::Status) -> bool {
    status_chain(status).any(|error| error.is::<crate::ConnectPhaseError>())
}

#[cfg(target_os = "linux")]
fn has_io_error(status: &tonic::Status, kind: std::io::ErrorKind) -> bool {
    status_chain(status).any(|error| {
        error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == kind)
    })
}

/// A health check without a deadline, so only a transport bound can end it.
fn unbounded_health_request() -> Request<tonic_health::pb::HealthCheckRequest> {
    Request::new(tonic_health::pb::HealthCheckRequest::default())
}

async fn health_check_status(channel: tonic::transport::Channel) -> tonic::Status {
    HealthClient::new(channel)
        .check(unbounded_health_request())
        .await
        .expect_err("the health check must fail")
}

// A lazy channel's dial that its bound times out fails the RPC before any
// request bytes exist, and the connector says so: the failure is replay-safe
// although its cause is a bare `TimedOut`. Under the paused clock the dial
// bound fires before the kernel's own SYN retries could end the dial.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn a_lazy_channels_timed_out_dial_is_a_client_connect_failure() {
    let blackhole = crate::test_support::Blackhole::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{}", blackhole.address()))
            .unwrap()
            .connect_timeout(Duration::from_secs(1));

    let status =
        health_check_status(super::connect_tcp_channel_lazy(endpoint, None).unwrap()).await;
    assert!(
        has_io_error(&status, std::io::ErrorKind::TimedOut),
        "{status:?}"
    );
    assert!(is_marked(&status), "{status:?}");
    assert!(crate::is_client_connect_failure(&status), "{status:?}");
}

// The same timed-out dial through tonic's own connector carries only the bare
// `TimedOut`, which an established connection that gives up on a request it
// already sent reports too, so it proves nothing.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn an_unmarked_timed_out_dial_is_not_a_client_connect_failure() {
    let blackhole = crate::test_support::Blackhole::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{}", blackhole.address()))
            .unwrap()
            .connect_timeout(Duration::from_secs(1));

    let status = health_check_status(endpoint.connect_lazy()).await;
    assert!(
        has_io_error(&status, std::io::ErrorKind::TimedOut),
        "{status:?}"
    );
    assert!(!crate::is_client_connect_failure(&status), "{status:?}");
}

// A refused dial is a connect failure through either connector: the refusal
// itself proves the dial failed. SPACE's connector reports it as its own
// connect-phase failure, tonic's as the refusal.
#[tokio::test]
async fn a_refused_dial_is_a_client_connect_failure_through_either_connector() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));

    for (connector, channel, cause) in [
        (
            "space",
            super::connect_tcp_channel_lazy(endpoint.clone(), None).unwrap(),
            crate::ClientConnectFailureCause::ConnectPhase,
        ),
        (
            "tonic",
            endpoint.connect_lazy(),
            crate::ClientConnectFailureCause::Refused,
        ),
    ] {
        let status = health_check_status(channel).await;
        assert!(
            crate::is_client_connect_failure(&status),
            "{connector}: {status:?}"
        );
        assert_eq!(
            crate::client_connect_failure_cause(&status),
            Some(cause),
            "{connector}: {status:?}"
        );
    }
}

// A kept channel whose redial never connects fails the RPC that waited for it
// with the connector's marker, so the redials it retries leave the failure
// replay-safe. The peer's address turns into a blackhole once it stops.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_kept_channels_redial_that_never_connects_is_a_client_connect_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap();
    let channel = super::connect_tcp_channel_with_retrying_redials(
        "test-backend",
        endpoint,
        Duration::from_millis(100),
        None,
        stalled_tcp_policy(Duration::from_millis(500)),
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    client.check(health_request()).await.expect("served");

    stop.send(()).unwrap();
    server.await.unwrap();
    let blackhole = crate::test_support::Blackhole::bind(address).await.unwrap();
    let status = client
        .check(unbounded_health_request())
        .await
        .expect_err("nothing answers the redial");
    assert!(
        has_io_error(&status, std::io::ErrorKind::TimedOut),
        "{status:?}"
    );
    assert!(is_marked(&status), "{status:?}");
    assert!(crate::is_client_connect_failure(&status), "{status:?}");
    drop(blackhole);
}

// A watched channel dials through SPACE's TCP connector too. Its watch still
// closes once the connection it was dialed with ends, and the redial that a
// holder's next RPC makes, which never connects, fails at the dial bound with
// the connector's marker, so the failure is replay-safe. The peer's address
// turns into a blackhole once it stops.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_watched_channels_connect_phase_timeout_is_a_client_connect_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, server) = serve_tcp_health(listener).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(200));
    let (channel, watch) = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        address.to_string(),
        endpoint,
        None,
        crate::GrpcRetryPolicy::none(),
    )
    .await
    .expect("first dial");
    let mut client = HealthClient::new(channel);
    client.check(health_request()).await.expect("served");
    assert!(!watch.is_closed(), "the dialed connection is open");

    stop.send(()).unwrap();
    server.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !watch.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the watch closes with the connection");
    let blackhole = crate::test_support::Blackhole::bind(address).await.unwrap();
    let status = tokio::time::timeout(
        Duration::from_secs(60),
        client.check(unbounded_health_request()),
    )
    .await
    .expect("an unanswered redial must be bounded")
    .expect_err("nothing answers the redial");
    assert!(
        has_io_error(&status, std::io::ErrorKind::TimedOut),
        "{status:?}"
    );
    assert!(is_marked(&status), "{status:?}");
    assert!(crate::is_client_connect_failure(&status), "{status:?}");
    assert!(watch.is_closed(), "a closed watch never reopens");
    drop(blackhole);
}

// A watched first dial that never connects fails with the connector's marked
// dial timeout, not as an attempt abandoned at its cap. Under the paused clock
// the dial bound fires before the kernel's own SYN retries could end the dial.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn a_watched_dial_that_never_connects_fails_in_its_connect_phase() {
    let blackhole = crate::test_support::Blackhole::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{}", blackhole.address()))
            .unwrap()
            .connect_timeout(Duration::from_secs(1));

    let error = super::connect_tcp_channel_with_retry_watched(
        "test-backend",
        blackhole.address().to_string(),
        endpoint,
        None,
        crate::GrpcRetryPolicy::none(),
    )
    .await
    .expect_err("nothing answers the dial");
    let error = match error {
        error @ super::TcpConnectError::Transport(_) => error,
        other => panic!("expected the dial bound to fail the attempt: {other:?}"),
    };
    let marked = std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .any(|error| error.is::<crate::ConnectPhaseError>());
    assert!(marked, "{error:?}");
}

/// What [`request_reading_peer`] does with a connection once it has read a
/// request's HEADERS.
#[derive(Clone, Copy, Debug)]
enum AfterRequest {
    Close,
    Reset,
    /// Keep the connection open and answer nothing, PINGs included.
    Hold,
}

/// A loopback peer that reads the HTTP/2 preface and each frame of every
/// connection until a request's HEADERS, then treats the connection as
/// `after` says. Returns its address and how many requests it read.
async fn request_reading_peer(
    after: AfterRequest,
) -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::AsyncReadExt as _;

    const HTTP2_PREFACE_LEN: usize = 24;
    const HTTP2_FRAME_HEADER_LEN: usize = 9;
    const HTTP2_HEADERS_FRAME: u8 = 0x1;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = requests.clone();
    let peer = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preface = [0; HTTP2_PREFACE_LEN];
            if stream.read_exact(&mut preface).await.is_err() {
                continue;
            }
            loop {
                let mut header = [0; HTTP2_FRAME_HEADER_LEN];
                if stream.read_exact(&mut header).await.is_err() {
                    break;
                }
                let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
                let mut payload = vec![0; length];
                if stream.read_exact(&mut payload).await.is_err() {
                    break;
                }
                if header[3] == HTTP2_HEADERS_FRAME {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match after {
                        AfterRequest::Close => {}
                        AfterRequest::Reset => stream.set_zero_linger().unwrap(),
                        AfterRequest::Hold => held.push(stream),
                    }
                    break;
                }
            }
        }
    });
    (address, requests, peer)
}

/// Assert that `status`, from an RPC `peer` read, failed locally but is no
/// connect failure.
fn assert_sent_and_not_a_connect_failure(
    label: &str,
    status: &tonic::Status,
    requests: &std::sync::atomic::AtomicUsize,
) {
    assert!(
        requests.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "{label}: the peer never read the request: {status:?}"
    );
    assert!(
        std::error::Error::source(status).is_some(),
        "{label}: the failure must be local: {status:?}"
    );
    assert!(
        !crate::is_client_connect_failure(status),
        "{label}: {status:?}"
    );
}

// An RPC whose connection dies after the peer read it was sent and may have
// run: its failure is local, but it is no connect failure, whether the peer
// closes the connection or resets it.
#[tokio::test]
async fn an_rpc_whose_connection_dies_after_it_was_sent_is_not_a_client_connect_failure() {
    for after in [AfterRequest::Close, AfterRequest::Reset] {
        let (address, requests, peer) = request_reading_peer(after).await;
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(1));

        let status =
            health_check_status(super::connect_tcp_channel_lazy(endpoint, None).unwrap()).await;
        assert_sent_and_not_a_connect_failure(&format!("{after:?}"), &status, &requests);
        peer.abort();
    }
}

// A request the peer read, on a connection that then goes silent, fails when
// the client's HTTP/2 keep-alive gives up. Tonic reports that local failure
// as `UNAVAILABLE`, the code a connect failure has too, so only the chain can
// tell that the request was sent. This waits out a real keep-alive: a paused
// clock could expire it before the peer read the request.
#[tokio::test]
async fn a_sent_rpc_whose_connection_goes_silent_is_not_a_client_connect_failure() {
    let (address, requests, peer) = request_reading_peer(AfterRequest::Hold).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1))
        .http2_keep_alive_interval(Duration::from_millis(200))
        .keep_alive_timeout(Duration::from_millis(200));

    let status = tokio::time::timeout(
        Duration::from_secs(60),
        health_check_status(super::connect_tcp_channel_lazy(endpoint, None).unwrap()),
    )
    .await
    .expect("the keep-alive must end the RPC");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert_sent_and_not_a_connect_failure("silent", &status, &requests);
    peer.abort();
}

/// A health service that answers every check with `UNAVAILABLE` and `message`.
struct UnavailableHealth(&'static str);

#[tonic::async_trait]
impl tonic_health::pb::health_server::Health for UnavailableHealth {
    async fn check(
        &self,
        _request: Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<tonic::Response<tonic_health::pb::HealthCheckResponse>, tonic::Status> {
        Err(tonic::Status::unavailable(self.0))
    }

    type WatchStream =
        tokio_stream::Empty<Result<tonic_health::pb::HealthCheckResponse, tonic::Status>>;

    async fn watch(
        &self,
        _request: Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("watch"))
    }
}

// A peer's `UNAVAILABLE` arrives over a working connection, whatever its
// message says about connecting.
#[tokio::test]
async fn a_peers_unavailable_that_reads_like_a_connect_failure_is_not_one() {
    for message in [
        "tcp connect error: Connection refused (os error 111)",
        "tcp connect error: connection timed out",
        "dns error: failed to lookup address information",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(tonic_health::pb::health_server::HealthServer::new(
                    UnavailableHealth(message),
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move {
                        let _ = shutdown_rx.await;
                    },
                ),
        );
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(1));

        let status =
            health_check_status(super::connect_tcp_channel_lazy(endpoint, None).unwrap()).await;
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert_eq!(status.message(), message);
        assert!(!crate::is_client_connect_failure(&status), "{status:?}");
        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}

// A TLS handshake runs on the connection the dial established, after the
// connector returned it, so its timeout carries no marker and stays
// ambiguous. That the handshake's own bound ends it, not a whole-connect
// bound cut to the dial's, shows a lazy internal-RPC channel keeps both.
#[cfg(feature = "internal-tls")]
#[tokio::test]
async fn a_lazy_internal_rpc_channels_tls_handshake_timeout_is_not_a_client_connect_failure() {
    use crate::internal_transport::{CA_ENV, ClientTransport, Service};

    install_crypto_provider();
    let (address, silent_peer) = silent_tls_peer().await;
    let dir = tempfile::tempdir().unwrap();
    let mut env = TestEnvironment::default();
    env.0
        .insert(CA_ENV, write_test_ca(dir.path()).into_os_string());
    let handshake_bound = Duration::from_secs(1);
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(500));
    let channel = ClientTransport::from_environment_with_tls_handshake_timeout(
        Service::NodeApi,
        endpoint.uri(),
        &env,
        handshake_bound,
    )
    .await
    .unwrap()
    .connect_endpoint(endpoint)
    .unwrap()
    .connect_lazy();

    let started = tokio::time::Instant::now();
    let status = tokio::time::timeout(Duration::from_secs(60), health_check_status(channel))
        .await
        .expect("an unanswered TLS handshake must be bounded");
    let elapsed = started.elapsed();
    assert!(
        status_chain(&status).any(|error| error.to_string().contains("TLS handshake timeout")),
        "{status:?}"
    );
    assert!(elapsed >= handshake_bound, "failed after {elapsed:?}");
    assert!(!is_marked(&status), "{status:?}");
    assert!(!crate::is_client_connect_failure(&status), "{status:?}");
    silent_peer.abort();
}

/// A resolver whose lookups never finish, as with a DNS server that does not
/// answer.
#[derive(Clone)]
struct StalledResolver;

impl tower::Service<hyper_util::client::legacy::connect::dns::Name> for StalledResolver {
    type Response = std::vec::IntoIter<std::net::SocketAddr>;
    type Error = std::io::Error;
    type Future = std::future::Pending<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _name: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        std::future::pending()
    }
}

/// An endpoint whose host name only [`StalledResolver`] is asked about, and
/// SPACE's TCP connector for it over that resolver.
fn stalled_lookup_connector(
    dial_timeout: Duration,
) -> (
    tonic::transport::Endpoint,
    super::TcpConnector<hyper_util::client::legacy::connect::HttpConnector<StalledResolver>>,
) {
    let endpoint = tonic::transport::Endpoint::from_static("http://stalled.invalid:50051")
        .connect_timeout(dial_timeout);
    super::tcp_connector(
        endpoint,
        None,
        hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(StalledResolver),
    )
}

// A lookup no DNS server answers fails the RPC when the connector's own bound
// on name resolution and the dial expires, before tonic's whole-connect
// backstop could: the status is a marked `UNAVAILABLE`, not an unmarked
// `UNKNOWN`, so it is replay-safe.
#[tokio::test(start_paused = true)]
async fn a_stalled_name_resolution_is_a_marked_client_connect_failure() {
    let dial_timeout = Duration::from_secs(1);
    let (endpoint, connector) = stalled_lookup_connector(dial_timeout);

    let started = tokio::time::Instant::now();
    let status = health_check_status(endpoint.connect_with_connector_lazy(connector)).await;
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(is_marked(&status), "{status:?}");
    assert!(crate::is_client_connect_failure(&status), "{status:?}");
    assert_eq!(started.elapsed(), super::connect_phase_bound(dial_timeout));
}

// In a retried connect, a stalled lookup fails its attempt with the
// connector's marked cause rather than as an attempt abandoned at its cap.
#[tokio::test(start_paused = true)]
async fn a_retried_connects_stalled_lookup_fails_with_its_own_cause() {
    let dial_timeout = Duration::from_secs(1);
    let (endpoint, connector) = stalled_lookup_connector(dial_timeout);

    let result = super::retry_tcp_connect(
        "stalled-lookup",
        "stalled.invalid:50051",
        crate::GrpcRetryPolicy::none(),
        super::tcp_attempt_cap(dial_timeout, None),
        || {
            let endpoint = endpoint.clone();
            let connector = connector.clone();
            async move {
                endpoint
                    .connect_with_connector(connector)
                    .await
                    .map_err(super::TcpConnectError::from)
            }
        },
    )
    .await;
    let error = match result {
        Err(error @ super::TcpConnectError::Transport(_)) => error,
        other => panic!("expected the connect-phase bound to fail the attempt: {other:?}"),
    };
    let marked = std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .any(|error| error.is::<crate::ConnectPhaseError>());
    assert!(marked, "{error:?}");
}

// A watched dial bounds a stalled lookup the same way: the attempt fails with
// the connector's marked cause, before the attempt cap or tonic's
// whole-connect backstop could end it unmarked.
#[tokio::test(start_paused = true)]
async fn a_watched_dials_stalled_lookup_fails_with_its_own_cause() {
    let dial_timeout = Duration::from_secs(1);
    let endpoint = tonic::transport::Endpoint::from_static("http://stalled.invalid:50051")
        .connect_timeout(dial_timeout);

    let started = tokio::time::Instant::now();
    let result = super::retry_tcp_connect(
        "stalled-lookup",
        "stalled.invalid:50051",
        crate::GrpcRetryPolicy::none(),
        super::tcp_attempt_cap(dial_timeout, None),
        || {
            let endpoint = endpoint.clone();
            async move {
                super::dial_watched_tcp_channel(
                    endpoint,
                    None,
                    hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(
                        StalledResolver,
                    ),
                )
                .await
                .map_err(super::TcpConnectError::from)
            }
        },
    )
    .await;
    let error = match result {
        Err(error @ super::TcpConnectError::Transport(_)) => error,
        other => panic!("expected the connect-phase bound to fail the attempt: {other:?}"),
    };
    let marked = std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .any(|error| error.is::<crate::ConnectPhaseError>());
    assert!(marked, "{error:?}");
    assert_eq!(started.elapsed(), super::connect_phase_bound(dial_timeout));
}

// A lazy channel whose endpoint sets no dial bound is given the default one,
// so a dial no peer answers fails with the dial bound's own marked
// `TimedOut` instead of hanging. Under the paused clock the kernel cannot
// time the dial out first.
#[cfg(target_os = "linux")]
#[tokio::test(start_paused = true)]
async fn a_lazy_channel_without_a_dial_bound_is_given_the_default_one() {
    let blackhole = crate::test_support::Blackhole::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{}", blackhole.address())).unwrap();
    assert_eq!(endpoint.get_connect_timeout(), None);

    let status = tokio::time::timeout(
        Duration::from_secs(60),
        health_check_status(super::connect_tcp_channel_lazy(endpoint, None).unwrap()),
    )
    .await
    .expect("an unanswered dial must be bounded");
    assert!(
        has_io_error(&status, std::io::ErrorKind::TimedOut),
        "{status:?}"
    );
    assert!(crate::is_client_connect_failure(&status), "{status:?}");
}

// Every SPACE TCP connect refuses, before any dial, an https endpoint whose
// handshake bound it was not told and a Unix-socket endpoint, which it would
// otherwise bound wrongly or resolve as the host `tonic`.
#[tokio::test]
async fn a_tcp_connect_refuses_endpoints_it_would_dial_wrongly() {
    use super::TcpEndpointError::{MissingTlsHandshakeBound, UnixSocket};

    for (uri, refusal) in [
        ("https://127.0.0.1:1", MissingTlsHandshakeBound),
        ("unix:///nonexistent/space-test.sock", UnixSocket),
    ] {
        let endpoint = tonic::transport::Endpoint::from_static(uri);
        let label = format!("{uri}: expected {refusal:?}");
        let is_refusal = |error: &super::TcpEndpointError| {
            std::mem::discriminant(error) == std::mem::discriminant(&refusal)
        };
        assert!(
            matches!(super::connect_tcp_channel_lazy(endpoint.clone(), None), Err(ref error) if is_refusal(error)),
            "{label}"
        );
        assert!(
            matches!(super::connect_tcp_channel(endpoint.clone(), None).await, Err(super::TcpChannelError::Endpoint(ref error)) if is_refusal(error)),
            "{label}"
        );
        assert!(
            matches!(
                super::connect_tcp_channel_with_retry("refused", uri, endpoint.clone(), None, crate::GrpcRetryPolicy::none()).await,
                Err(super::TcpConnectError::Endpoint(ref error)) if is_refusal(error)
            ),
            "{label}"
        );
        assert!(
            matches!(
                super::connect_tcp_channel_with_retry_watched("refused", uri, endpoint.clone(), None, crate::GrpcRetryPolicy::none()).await,
                Err(super::TcpConnectError::Endpoint(ref error)) if is_refusal(error)
            ),
            "{label}"
        );
        assert!(
            matches!(
                super::connect_tcp_channel_with_retrying_redials("refused", endpoint.clone(), Duration::from_secs(1), None, crate::GrpcRetryPolicy::none()).await,
                Err(super::TcpChannelError::Endpoint(ref error)) if is_refusal(error)
            ),
            "{label}"
        );
        assert!(
            matches!(
                crate::RotatingChannel::connect("refused", endpoint, None, None).await,
                Err(super::TcpChannelError::Endpoint(ref error)) if is_refusal(error)
            ),
            "{label}"
        );
    }
}
