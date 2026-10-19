use futures_util::future::join_all;
use hickory_resolver::config::{ConnectionConfig, NameServerConfig};
use hickory_resolver::lookup::Lookup;
use hickory_resolver::net::xfer::{DnsHandle, FirstAnswer};
use hickory_resolver::proto::op::{DnsRequest, DnsRequestOptions};
use hickory_resolver::proto::rr::rdata::A;
use hickory_resolver::{ConnectionProvider, PoolContext, TlsConfig};
use parking_lot::Mutex;
use rustls::pki_types::PrivateKeyDer;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

use super::upstream::LoggingConnectionProvider;
use super::*;

const UPSTREAM_NAME: &str = "dns.example.test";

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
}

#[derive(Default)]
struct ConnectionControl {
    close: CancellationToken,
    stall: AtomicBool,
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
        let mut server = NameServerConfig::tcp(self.addr.ip());
        server.connections = vec![self.connection_config()];
        let mut options = ResolverOpts::default();
        options.attempts = 1;
        options.max_active_requests = MAX_CONCURRENT_LOOKUPS;
        options.use_hosts_file = ResolveHosts::Never;
        RecoveringResolver::new(
            ResolverConfig::from_parts(None, Vec::new(), vec![server]),
            options,
            runtime(),
            ReservedResolver::Google,
        )
        .unwrap()
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
