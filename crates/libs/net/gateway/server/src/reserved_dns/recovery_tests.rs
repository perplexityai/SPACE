use futures_util::future::join_all;
use hickory_resolver::config::{ConnectionConfig, NameServerConfig};
use hickory_resolver::lookup::Lookup;
use hickory_resolver::net::xfer::{DnsHandle, FirstAnswer};
use hickory_resolver::proto::op::{DnsRequest, DnsRequestOptions};
use hickory_resolver::proto::rr::rdata::A;
use hickory_resolver::{ConnectionProvider, PoolContext, TlsConfig};
use parking_lot::Mutex;
use rustls::pki_types::PrivateKeyDer;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::upstream::LoggingConnectionProvider;
use super::*;

const UPSTREAM_NAME: &str = "dns.example.test";

// Real-time measurements are opt-in even when CI includes ignored tests.
// A warm connection makes the cutoff observable; cold startup can fan out
// before a connection has entered Hickory's pool. Every result records both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real-time DNS cutoff characterization"]
async fn cutoff_timing_matrix() {
    if std::env::var("SPACE_DNS_TIMING_EXPERIMENT").as_deref() != Ok("1") {
        return;
    }
    let _tracing =
        tracing::subscriber::set_default(tracing_subscriber::fmt().with_test_writer().finish());
    let output = std::env::var("SPACE_DNS_TIMING_OUTPUT").expect("measurement output JSONL path");
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output)
        .unwrap();
    use std::io::Write;
    for (label, timeout_ms, attempts, max_active) in [
        ("baseline", 4000, 1, 64),
        ("short", 500, 1, 64),
        ("short_retries", 500, 3, 64),
        ("one_second", 1000, 2, 64),
        ("active32", 4000, 1, 32),
        ("combined", 500, 3, 32),
    ] {
        for close in [false, true] {
            for repeated in [false, true] {
                for warm in [true, false] {
                    let mut upstream = TestUpstream::start(true).await;
                    let resolver = upstream.resolver_with(|options| {
                        options.timeout = Duration::from_millis(timeout_ms);
                        options.attempts = attempts;
                        options.max_active_requests = max_active;
                        options.num_concurrent_reqs = 1;
                    });
                    if warm {
                        lookup(&resolver, "warm.example.").await.unwrap();
                        for control in upstream.state.controls.lock().iter() {
                            control.answered.store(0, Ordering::Relaxed);
                        }
                    }
                    upstream.state.answer_limit.store(46, Ordering::Relaxed);
                    upstream
                        .state
                        .close_at_limit
                        .store(close, Ordering::Relaxed);
                    let names: Vec<_> = (0..80)
                        .map(|i| format!("query-{}.example.", if repeated { i % 4 } else { i }))
                        .collect();
                    let started = std::time::Instant::now();
                    let results = join_all(names.iter().enumerate().map(|(id, name)| {
                        let resolver = &resolver;
                        async move {
                            let started = std::time::Instant::now();
                            let result =
                                tokio::time::timeout(TOTAL_LOOKUP_TIMEOUT, lookup(resolver, name))
                                    .await;
                            let (ok, error) = match result {
                                Ok(Ok(_)) => (true, None),
                                Ok(Err(error)) => (false, Some(format!("{error:?}"))),
                                Err(_) => (false, Some("TotalLookupTimeout".to_owned())),
                            };
                            serde_json::json!({
                                "id": id, "name": name, "ok": ok, "error": error,
                                "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                            })
                        }
                    }))
                    .await;
                    let mut latencies: Vec<_> = results
                        .iter()
                        .map(|result| result["latency_ms"].as_f64().unwrap())
                        .collect();
                    latencies.sort_by(f64::total_cmp);
                    let sends: std::collections::BTreeMap<_, _> = names
                        .iter()
                        .map(|name| (name.clone(), upstream.queries(name)))
                        .collect();
                    let buckets: Vec<_> = [(0.0, 50.0), (50.0, 500.0), (500.0, 1500.0),
                        (1500.0, 3900.0), (3900.0, 4500.0), (4500.0, 7900.0),
                        (7900.0, f64::INFINITY)].into_iter()
                        .map(|(lower, upper)| serde_json::json!({
                            "lower_ms": lower, "upper_ms": if upper.is_finite() { Some(upper) } else { None },
                            "count": latencies.iter().filter(|&&value| value >= lower && value < upper).count(),
                        })).collect();
                    let row = serde_json::json!({
                        "label": label, "timeout_ms": timeout_ms, "attempts": attempts,
                        "max_active": max_active, "close": close, "repeated": repeated,
                        "warm": warm, "answer_limit": 46, "n": 80,
                        "ok": results.iter().filter(|result| result["ok"] == true).count(),
                        "wall_ms": started.elapsed().as_secs_f64() * 1000.0,
                        "p50_ms": latencies[39], "p90_ms": latencies[71],
                        "p99_ms": latencies[79], "connections": upstream.connections().accepted,
                        "sends_per_name": sends, "buckets": buckets, "results": results,
                    });
                    writeln!(output, "{row}").unwrap();
                    output.flush().unwrap();
                    tracing::info!(
                        label,
                        close,
                        repeated,
                        warm,
                        p99_ms = latencies[79],
                        connections = upstream.connections().accepted,
                        "DNS cutoff timing"
                    );
                    assert!(
                        results.iter().all(|result| result["ok"] == true),
                        "healthy retry connections must complete every lookup"
                    );
                    if label == "baseline" && warm && !repeated {
                        assert!(
                            latencies.iter().any(|&value| value < 500.0),
                            "initial answers must arrive"
                        );
                        if !close {
                            assert!(
                                latencies.iter().any(|&value| value >= 3900.0),
                                "silent queries must hold their attempt deadline"
                            );
                            assert!(
                                sends.values().any(|&count| count >= 2),
                                "timed-out names must be retried"
                            );
                        } else {
                            assert!(
                                latencies[79] < 2000.0,
                                "a close must wake pending lookups before their four-second deadline"
                            );
                        }
                    }
                    // Connections are scoped to this fixture. Dropping the
                    // resolver then closing them avoids idle tasks across rows.
                    drop(resolver);
                    tokio::time::timeout(Duration::from_secs(2), upstream.close_connections())
                        .await
                        .expect("fixture connections must close");
                    upstream.stop_accepting().await;
                }
            }
        }
    }
}

// Hickory's exchange reports a send on a connection the upstream closed as
// Busy. Concurrent sends through Hickory's clones of the pooled connection
// share one reconnect. TLS also shows that it keeps the pool's TLS trust.
#[tokio::test]
async fn idle_closed_connection_reconnects_once_for_concurrent_sends() {
    for tls in [false, true] {
        let upstream = TestUpstream::start(tls).await;
        let handle = upstream.connection().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            handle
                .send(request("first.example."))
                .first_answer()
                .await
                .unwrap();
            upstream.close_connections().await;
            let answers = join_all((0..4).map(|i| {
                handle
                    .clone()
                    .send(request(&format!("after-{i}.example.")))
                    .first_answer()
            }))
            .await;
            for answer in answers {
                answer.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(upstream.connections().accepted, 2, "tls: {tls}");
    }
}

// A connection that stops answering without closing stays open, so a timeout
// marks it failed for every pooled connection that moved onto it. Failed
// connections Hickory has given up on must close, not pile up.
#[tokio::test]
async fn failed_connection_moves_every_pooled_connection_off_it() {
    let upstream = TestUpstream::start(false).await;
    let first = upstream.connection().await;
    let mut second = upstream.connection().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        for (handle, name) in [(&first, "first.example."), (&second, "second.example.")] {
            handle.send(request(name)).first_answer().await.unwrap();
        }
        upstream.close_connections().await;
        // Both move to one new connection.
        for (handle, name) in [(&first, "moved-1.example."), (&second, "moved-2.example.")] {
            handle.send(request(name)).first_answer().await.unwrap();
        }
        assert_eq!(upstream.connections().accepted, 3);
        upstream.stall_connections();
        assert!(matches!(
            first.send(request("stalled.example.")).first_answer().await,
            Err(NetError::Timeout)
        ));
        second
            .send(request("after.example."))
            .first_answer()
            .await
            .unwrap();
        assert_eq!(upstream.connections().accepted, 4);

        // As Hickory does, drop each pooled connection whose request timed out
        // and dial a new one.
        drop(first);
        for round in 0..2 {
            upstream.stall_connections();
            assert!(matches!(
                second
                    .send(request(&format!("stalled-{round}.example.")))
                    .first_answer()
                    .await,
                Err(NetError::Timeout)
            ));
            second = upstream.connection().await;
            second
                .send(request(&format!("redialed-{round}.example.")))
                .first_answer()
                .await
                .unwrap();
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            upstream.until(|connections| connections.closed == connections.accepted - 1),
        )
        .await
        .expect("superseded failed connections must close");
    })
    .await
    .unwrap();
    assert_eq!(upstream.connections().accepted, 6);
}

// A reconnect whose every waiter was cancelled still completes, so a later
// send uses it instead of resuming a handshake nobody drove.
#[tokio::test]
async fn abandoned_reconnect_completes_for_later_sends() {
    let upstream = TestUpstream::start(true).await;
    let handle = upstream.connection().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        handle
            .send(request("first.example."))
            .first_answer()
            .await
            .unwrap();
        upstream.hold_handshakes(true);
        upstream.close_connections().await;
        tokio::select! {
            _ = handle.send(request("abandoned.example.")).first_answer() => {
                panic!("the reconnect's handshake is held");
            }
            () = upstream.until(|connections| connections.accepted == 2) => {}
        }
        upstream.hold_handshakes(false);
        tokio::time::timeout(
            Duration::from_secs(2),
            upstream.until(|connections| connections.established == 2),
        )
        .await
        .expect("the abandoned reconnect must complete its handshake");
        handle
            .send(request("after.example."))
            .first_answer()
            .await
            .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(upstream.connections().accepted, 2);
}

// The newest connection is not the only candidate: a send on a closed one
// moves to an older open one rather than reconnecting.
#[tokio::test]
async fn closed_connection_moves_to_an_older_open_connection() {
    let mut upstream = TestUpstream::start(false).await;
    let older = upstream.connection().await;
    let newest = upstream.connection().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        for (handle, name) in [(&older, "older.example."), (&newest, "newest.example.")] {
            handle.send(request(name)).first_answer().await.unwrap();
        }
        upstream.close_newest().await;
        upstream.stop_accepting().await;
        newest
            .send(request("after.example."))
            .first_answer()
            .await
            .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(upstream.connections().accepted, 2);
}

// Five closed pooled connections exhaust Hickory's Busy backoff and end in
// NoConnections unless each reconnects. Replacing the resolver drops its cache,
// so upstream query counts show whether recovery replaced it.
#[tokio::test]
async fn idle_closed_connections_keep_the_resolver_and_outages_replace_it_once() {
    let mut upstream = TestUpstream::start(false).await;
    let resolver = upstream.resolver();
    let warm: Vec<_> = (0..5).map(|i| format!("warm-{i}.example.")).collect();

    tokio::time::timeout(Duration::from_secs(5), async {
        // Concurrent lookups each open a pooled connection.
        for result in join_all(warm.iter().map(|name| lookup(&resolver, name))).await {
            result.unwrap();
        }
        assert_eq!(upstream.connections().accepted, 5);
        upstream.close_connections().await;
        let after: Vec<_> = (0..8).map(|i| format!("after-{i}.example.")).collect();
        for result in join_all(after.iter().map(|name| lookup(&resolver, name))).await {
            result.unwrap();
        }
        // Later lookups reach the other closed pooled connections, which move
        // to the one reconnected connection.
        for i in 0..8 {
            lookup(&resolver, &format!("later-{i}.example."))
                .await
                .unwrap();
        }
        assert_eq!(upstream.connections().accepted, 6);
        for name in &warm {
            lookup(&resolver, name).await.unwrap();
            assert_eq!(upstream.queries(name), 1, "{name} must stay cached");
        }

        upstream.refuse().await;
        assert!(matches!(
            lookup(&resolver, "outage-1.example.").await,
            Err(NetError::NoConnections)
        ));
        upstream.restore().await;
        lookup(&resolver, &warm[0]).await.unwrap();
        assert_eq!(
            upstream.queries(&warm[0]),
            2,
            "the outage replaced the resolver"
        );

        // A second outage within the cooldown keeps the replacement and its cache.
        upstream.refuse().await;
        assert!(matches!(
            lookup(&resolver, "outage-2.example.").await,
            Err(NetError::NoConnections)
        ));
        lookup(&resolver, &warm[0]).await.unwrap();
    })
    .await
    .unwrap();
}

/// A DNS-over-TCP or DNS-over-TLS upstream that answers every query. On
/// request it closes connections like an idle timeout, stops answering on them
/// without closing, holds TLS handshakes, or refuses connections.
struct TestUpstream {
    addr: SocketAddr,
    acceptor: Option<TlsAcceptor>,
    client_tls: rustls::ClientConfig,
    // One provider, as a Hickory pool holds. It also outlives the exchanges,
    // which its runtime's last clone would abort.
    provider: LoggingConnectionProvider,
    state: Arc<UpstreamState>,
    listener: Option<JoinHandle<()>>,
}

struct UpstreamState {
    connections: watch::Sender<Connections>,
    queries: Mutex<HashMap<Name, usize>>,
    // One per accepted connection, in accept order.
    controls: Mutex<Vec<Arc<ConnectionControl>>>,
    hold_handshakes: watch::Sender<bool>,
    answer_limit: AtomicUsize,
    close_at_limit: AtomicBool,
}

#[derive(Default)]
struct ConnectionControl {
    close: CancellationToken,
    stall: AtomicBool,
    answered: AtomicUsize,
}

#[derive(Clone, Copy, Default)]
struct Connections {
    accepted: usize,
    // Accepted connections that finished any TLS handshake.
    established: usize,
    closed: usize,
}

impl TestUpstream {
    async fn start(tls: bool) -> Self {
        let certificate = rcgen::generate_simple_self_signed(vec![UPSTREAM_NAME.into()]).unwrap();
        let crypto = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.cert.der().clone()).unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(crypto.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let acceptor = TlsAcceptor::from(Arc::new(
            rustls::ServerConfig::builder_with_provider(crypto)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.cert.der().clone()],
                    PrivateKeyDer::Pkcs8(certificate.signing_key.serialize_der().into()),
                )
                .unwrap(),
        ));
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut upstream = Self {
            addr: listener.local_addr().unwrap(),
            acceptor: tls.then_some(acceptor),
            client_tls,
            provider: LoggingConnectionProvider::new(runtime(), ReservedResolver::Google),
            state: Arc::new(UpstreamState {
                connections: watch::Sender::default(),
                queries: Mutex::default(),
                controls: Mutex::default(),
                hold_handshakes: watch::Sender::new(false),
                answer_limit: AtomicUsize::new(usize::MAX),
                close_at_limit: AtomicBool::new(false),
            }),
            listener: None,
        };
        upstream.listen(listener);
        upstream
    }

    fn connection_config(&self) -> ConnectionConfig {
        let mut config = match self.acceptor {
            Some(_) => ConnectionConfig::tls(Arc::from(UPSTREAM_NAME)),
            None => ConnectionConfig::tcp(),
        };
        config.port = self.addr.port();
        config
    }

    // A new pooled connection, with only the self-signed root trusted.
    async fn connection(&self) -> upstream::LoggingDnsHandle {
        let mut options = ResolverOpts::default();
        options.timeout = Duration::from_secs(1);
        self.provider
            .new_connection(
                self.addr.ip(),
                &self.connection_config(),
                &PoolContext::new(
                    options,
                    TlsConfig {
                        config: self.client_tls.clone(),
                    },
                ),
            )
            .unwrap()
            .await
            .unwrap()
    }

    fn resolver(&self) -> RecoveringResolver {
        self.resolver_with(|_| {})
    }

    fn resolver_with(&self, configure: impl FnOnce(&mut ResolverOpts)) -> RecoveringResolver {
        let mut server = NameServerConfig::tcp(self.addr.ip());
        server.connections = vec![self.connection_config()];
        let mut options = ResolverOpts::default();
        options.attempts = 1;
        options.max_active_requests = MAX_CONCURRENT_LOOKUPS;
        options.use_hosts_file = ResolveHosts::Never;
        configure(&mut options);
        let mut resolver = RecoveringResolver::new(
            ResolverConfig::from_parts(None, Vec::new(), vec![server]),
            options,
            runtime(),
            ReservedResolver::Google,
        )
        .unwrap();
        if self.acceptor.is_some() {
            resolver.set_test_tls(self.client_tls.clone());
        }
        resolver
    }

    fn connections(&self) -> Connections {
        *self.state.connections.borrow()
    }

    fn queries(&self, name: &str) -> usize {
        let name = Name::from_ascii(name).unwrap();
        self.state.queries.lock().get(&name).copied().unwrap_or(0)
    }

    async fn until(&self, condition: impl FnMut(&Connections) -> bool) {
        self.state
            .connections
            .subscribe()
            .wait_for(condition)
            .await
            .unwrap();
    }

    /// Closes every connection and waits until the client closed its end,
    /// which it does only once its exchange has shut down.
    async fn close_connections(&self) {
        for control in self.state.controls.lock().iter() {
            control.close.cancel();
        }
        self.until(|connections| connections.closed == connections.accepted)
            .await;
    }

    /// Closes the most recently accepted connection, which must be open.
    async fn close_newest(&self) {
        let closed = self.connections().closed;
        self.state.controls.lock().last().unwrap().close.cancel();
        self.until(|connections| connections.closed == closed + 1)
            .await;
    }

    /// Keeps every current connection open but stops answering on it.
    fn stall_connections(&self) {
        for control in self.state.controls.lock().iter() {
            control
                .stall
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn hold_handshakes(&self, hold: bool) {
        self.state.hold_handshakes.send_replace(hold);
    }

    async fn stop_accepting(&mut self) {
        let listener = self.listener.take().unwrap();
        listener.abort();
        assert!(listener.await.unwrap_err().is_cancelled());
    }

    async fn refuse(&mut self) {
        self.close_connections().await;
        self.stop_accepting().await;
    }

    async fn restore(&mut self) {
        self.listen(TcpListener::bind(self.addr).await.unwrap());
    }

    fn listen(&mut self, listener: TcpListener) {
        let acceptor = self.acceptor.clone();
        let state = self.state.clone();
        // Connections outlive the listener, so refusing new ones keeps them.
        self.listener = Some(tokio::spawn(async move {
            loop {
                let stream = listener.accept().await.unwrap().0;
                state
                    .connections
                    .send_modify(|connections| connections.accepted += 1);
                let control = Arc::new(ConnectionControl::default());
                state.controls.lock().push(control.clone());
                let acceptor = acceptor.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    match acceptor {
                        Some(acceptor) => {
                            let _ = state
                                .hold_handshakes
                                .subscribe()
                                .wait_for(|hold| !hold)
                                .await;
                            if let Ok(stream) = acceptor.accept(stream).await {
                                serve(stream, &state, &control).await;
                            }
                        }
                        None => serve(stream, &state, &control).await,
                    }
                    state
                        .connections
                        .send_modify(|connections| connections.closed += 1);
                });
            }
        }));
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    state: &UpstreamState,
    control: &ConnectionControl,
) {
    state
        .connections
        .send_modify(|connections| connections.established += 1);
    loop {
        let length = tokio::select! {
            length = stream.read_u16() => match length {
                Ok(length) => length,
                Err(_) => return,
            },
            () = control.close.cancelled() => break,
        };
        let mut wire = vec![0; usize::from(length)];
        stream.read_exact(&mut wire).await.unwrap();
        let mut response = Message::from_vec(&wire).unwrap().into_response();
        let name = response.queries[0].name.clone();
        *state.queries.lock().entry(name.clone()).or_default() += 1;
        if control.stall.load(std::sync::atomic::Ordering::Relaxed) {
            continue;
        }
        if control.answered.fetch_add(1, Ordering::Relaxed)
            >= state.answer_limit.load(Ordering::Relaxed)
        {
            if state.close_at_limit.load(Ordering::Relaxed) {
                // Dropping the TLS stream closes TCP without close_notify.
                return;
            }
            continue;
        }
        response.answers.push(Record::from_rdata(
            name,
            60,
            RData::A(A(Ipv4Addr::new(192, 0, 2, 1))),
        ));
        let wire = response.to_vec().unwrap();
        // One write, so Nagle's algorithm does not hold the message body.
        let mut frame = u16::try_from(wire.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(&wire);
        stream.write_all(&frame).await.unwrap();
    }
    // Close like an upstream idle timeout, then wait for the client's close.
    let _ = stream.shutdown().await;
    let _ = stream.read_to_end(&mut Vec::new()).await;
}

fn runtime() -> GatewayRuntimeProvider {
    GatewayRuntimeProvider {
        handle: TokioHandle::default(),
        outbound_mark: None,
        lookup_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_LOOKUPS)),
    }
}

async fn lookup(resolver: &RecoveringResolver, name: &str) -> Result<Lookup, NetError> {
    resolver
        .lookup(ReservedResolver::Google, &question(name))
        .await
}

fn request(name: &str) -> DnsRequest {
    let mut message = Message::query();
    message.add_query(question(name));
    DnsRequest::new(message, DnsRequestOptions::default())
}

fn question(name: &str) -> Query {
    Query::query(Name::from_ascii(name).unwrap(), RecordType::A)
}
