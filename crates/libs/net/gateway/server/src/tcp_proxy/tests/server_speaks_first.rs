//! Server-speaks-first flows under TLS interception. Origins listen on
//! 0.0.0.0 (policies refuse 127/8) and the dialer sets no socket mark, so no
//! test needs privilege or an external address.

use super::*;
use crate::tls_sni::test_fixtures::client_hello;
use futures_util::FutureExt as _;
use pplx_space_metrics::test_capture::{
    CapturedCounter, capture_counters_guard, capture_values_guard,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Past the 50 ms peek, so the guest writes while the probe is running.
const AFTER_PROBE_STARTS: Duration = Duration::from_millis(300);

type CertificateRequests = Arc<std::sync::Mutex<Vec<(DomainName, u16)>>>;

async fn dial(dst: SocketAddr) -> Result<TcpStream, transport::Error> {
    Ok(TcpStream::connect(dst).await?)
}

/// A destination that never speaks.
#[derive(Clone, Copy)]
enum Silent {
    Idle,
    Closes,
    Refuses,
    /// The dial never completes.
    Hangs,
}

impl Silent {
    async fn dial(self) -> Result<tokio::io::DuplexStream, transport::Error> {
        let (upstream, peer) = tokio::io::duplex(64);
        match self {
            // Leaked so the upstream stays open for the rest of the test.
            Self::Idle => {
                Box::leak(Box::new(peer));
            }
            Self::Closes => drop(peer),
            Self::Refuses => return Err(io::Error::from(io::ErrorKind::ConnectionRefused).into()),
            Self::Hangs => return std::future::pending().await,
        }
        Ok(upstream)
    }
}

/// A proxy enforcing `policy`. Its certificate authority records and refuses
/// every request, so an intercepted flow ends at the handshake.
async fn proxy(policy: NetworkPolicyV1) -> (TcpProxy, CertificateRequests) {
    let (network_state, _) = ready_policy(policy).await;
    let requests = CertificateRequests::default();
    let recorded = Arc::clone(&requests);
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        tls_interception: TlsInterception::new(Arc::new(move |host, port| {
            recorded.lock().unwrap().push((host, port));
            Box::pin(async { Err(tonic::Status::unavailable("synthetic signer")) })
        })),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        known_egress_state: KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    (proxy, requests)
}

fn intercepted(policy: NetworkPolicyV1) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        tls_interception: pplx_space_net_common::TlsInterception::On,
        ..policy
    }
}

/// Interception on, with the destination admitted by its address alone.
fn ip_allow(destination: SocketAddr) -> NetworkPolicyV1 {
    intercepted(NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Ip(destination.ip())],
            Vec::new(),
        )
        .unwrap(),
    })
}

async fn guest_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (guest, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    (guest.unwrap(), accepted.unwrap().0)
}

/// A guest with a 4 KiB receive buffer, so a large reply is still queued in
/// the gateway when the relay ends.
async fn slow_guest_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let (guest, accepted) = tokio::join!(
        socket.connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    (guest.unwrap(), accepted.unwrap().0)
}

async fn origin() -> (TcpListener, SocketAddr) {
    let origin = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let destination = origin.local_addr().unwrap();
    (origin, destination)
}

/// Read a probe connection to its end: what arrived, and whether the gateway
/// reset the connection instead of closing it.
async fn drain(stream: &mut TcpStream) -> (Vec<u8>, bool) {
    let mut received = Vec::new();
    match stream.read_to_end(&mut received).await {
        Ok(_) => (received, false),
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => (received, true),
        Err(error) => panic!("reading the probe connection failed: {error}"),
    }
}

fn missing_sni_detail(outcome: &FlowOutcome) -> Option<&'static str> {
    match outcome {
        FlowOutcome::Rejected(Rejection::TlsInterception(error)) => interception_detail(error),
        _ => None,
    }
}

fn admission_decisions(counters: &[CapturedCounter]) -> u64 {
    counters
        .iter()
        .filter(|counter| {
            counter.name == "gateway.policy.decision"
                && counter.tags.iter().any(|tag| tag == "phase:admission")
        })
        .map(|counter| counter.value)
        .sum()
}

/// The greeting reaches a guest that waits for it, and the session relays both
/// ways. A guest that half-closes and reads slowly still gets the whole reply:
/// the relay ends with an orderly close, not a reset that discards what is
/// still queued.
#[tokio::test]
async fn a_server_first_greeting_reaches_a_silent_guest_and_the_session_relays() {
    const REPLY_LEN: usize = 256 << 10;
    for (name, policy, greeting_delay) in [
        ("IP allow", None, Duration::ZERO),
        (
            "allow all, delayed greeting",
            Some(intercepted(NetworkPolicyV1::allow_all())),
            Duration::from_secs(1),
        ),
    ] {
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(policy.unwrap_or_else(|| ip_allow(destination))).await;
        let (mut guest, gateway_guest) = slow_guest_pair().await;

        let server = async {
            let (mut stream, _) = origin.accept().await.unwrap();
            tokio::time::sleep(greeting_delay).await;
            stream
                .write_all(b"220 ftp.example.test ready\r\n")
                .await
                .unwrap();
            let mut command = [0_u8; 12];
            stream.read_exact(&mut command).await.unwrap();
            assert_eq!(&command, b"USER guest\r\n", "{name}");
            stream.write_all(&vec![b'x'; REPLY_LEN]).await.unwrap();
        };
        let client = async {
            let mut greeting = [0_u8; 28];
            guest.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"220 ftp.example.test ready\r\n", "{name}");
            guest.write_all(b"USER guest\r\n").await.unwrap();
            guest.shutdown().await.unwrap();
            let mut chunk = [0_u8; 1024];
            let mut reply = 0;
            loop {
                match guest.read(&mut chunk).await {
                    Ok(0) => return Ok(reply),
                    Ok(read) => {
                        assert!(chunk[..read].iter().all(|byte| *byte == b'x'), "{name}");
                        reply += read;
                    }
                    Err(error) => return Err((reply, error.kind())),
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let shutdown = CancellationToken::new();
        let counters = capture_counters_guard();
        let values = capture_values_guard();
        let ((_, outcome), (), reply) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                server,
                client,
            )
        })
        .await
        .expect(name);
        assert_eq!(
            reply,
            Ok(REPLY_LEN),
            "{name}: the guest lost the end of the reply"
        );
        assert_eq!(admission_decisions(&counters.finish()), 1, "{name}");
        let client_hello_ms = values
            .finish()
            .into_iter()
            .filter(|value| value.name == "gateway.tls.client_hello.duration_ms")
            .count();
        assert_eq!(client_hello_ms, 1, "{name}: the scan is recorded once");

        let FlowOutcome::Relayed(Relay::Tunnelled { wire_tx, wire_rx }) = outcome else {
            panic!("{name}: a server-first flow must relay");
        };
        assert_eq!(
            (wire_tx, wire_rx),
            (12, 28 + REPLY_LEN as u64),
            "{name}: greeting is counted"
        );
        assert!(requests.lock().unwrap().is_empty(), "{name}");
        assert_eq!(proxy.stats.tcp_total.load(Ordering::Relaxed), 1, "{name}");
    }
}

/// A ClientHello sent after the probe starts, even after the destination
/// closed the probe connection, is handled like an on-time one: it reaches
/// interception. Late bytes interception refuses, such as an upload whose
/// first byte is a TLS record type, are refused. Either way the probe
/// connection gets no bytes and is reset; an orderly close would tell a
/// passive FTP data port the upload completed empty.
#[tokio::test]
async fn late_bytes_are_inspected_and_the_probe_connection_sees_nothing() {
    const UPLOAD: &[u8] = b"\x16 binary file content, not a TLS handshake";
    let hello = client_hello(Some("good.example.test"));
    let mut outcomes = Vec::new();
    for (timing, payload) in [
        ("on time", hello.as_slice()),
        ("late", hello.as_slice()),
        ("after the destination closed", hello.as_slice()),
        ("late upload", UPLOAD),
    ] {
        let late = timing != "on time";
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let client = async {
            if late {
                tokio::time::sleep(AFTER_PROBE_STARTS).await;
            }
            guest.write_all(payload).await.unwrap();
            if payload == UPLOAD {
                // Ends the upload; the gateway may already have reset the guest.
                let _ = guest.shutdown().await;
            }
        };
        let probe_connection = async {
            if !late {
                return;
            }
            let (mut stream, _) = origin.accept().await.unwrap();
            if timing == "after the destination closed" {
                stream.shutdown().await.unwrap();
            }
            let (received, reset) = drain(&mut stream).await;
            assert!(
                received.is_empty(),
                "{timing}: the probe connection received guest bytes"
            );
            assert!(reset, "{timing}: the probe connection was not reset");
        };
        let shutdown = CancellationToken::new();
        let counters = capture_counters_guard();
        let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                client,
                probe_connection,
            )
        })
        .await
        .expect(timing);
        assert_eq!(
            admission_decisions(&counters.finish()),
            1,
            "{timing}: one admission decision per flow"
        );
        assert!(
            origin.accept().now_or_never().is_none(),
            "{timing}: nothing may be relayed to the destination"
        );
        if payload == UPLOAD {
            assert_eq!(outcome.metric_tags(), ("rejected", "tls_interception"));
            assert!(requests.lock().unwrap().is_empty(), "{timing}");
            continue;
        }
        assert!(
            matches!(
                outcome,
                FlowOutcome::Rejected(Rejection::TlsInterception(
                    TlsInterceptionError::CertificateRequest(_)
                ))
            ),
            "{timing}: the ClientHello must reach interception"
        );
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [("good.example.test".parse().unwrap(), destination.port())],
            "{timing}"
        );
        outcomes.push(outcome.metric_tags());
    }
    assert!(
        outcomes.windows(2).all(|pair| pair[0] == pair[1]),
        "lateness changed the outcome: {outcomes:?}"
    );
}

/// A late ClientHello that is intercepted never uses the probe connection, so
/// it is reset when interception starts rather than held for the session.
#[tokio::test]
async fn an_intercepted_late_client_hello_releases_the_probe_connection_at_once() {
    let (origin, destination) = origin().await;
    let policy = ip_allow(destination);
    let (mut proxy, _) = proxy(policy.clone()).await;
    let (root, _, provider) = crate::certificates::local_provider_with_policy(
        proxy.network_state.clone(),
        policy,
        Default::default(),
    )
    .await;
    proxy.tls_interception = TlsInterception::new(provider.requester());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root.der().clone()).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let (guest, gateway_guest) = guest_pair().await;
    let (probing, probed) = tokio::sync::oneshot::channel();
    let (reset_tx, reset) = tokio::sync::oneshot::channel();
    let server = async {
        let (mut stream, _) = origin.accept().await.unwrap();
        probing.send(()).unwrap();
        assert_eq!(drain(&mut stream).await, (Vec::new(), true));
        reset_tx.send(()).unwrap();
    };
    let client = async {
        probed.await.unwrap();
        let mut session = connector
            .connect("good.example.test".try_into().unwrap(), guest)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(500), reset)
            .await
            .expect("the probe connection is held while the session is open")
            .unwrap();
        session.shutdown().await.unwrap();
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            server,
            client,
        )
    })
    .await
    .unwrap();
    assert!(
        matches!(outcome, FlowOutcome::InspectedTls),
        "{:?}",
        outcome.metric_tags()
    );
}

/// Late plaintext is relayed over the probe connection itself, as a
/// destination that accepts one connection (an FTP passive data port) needs.
/// The relay ends with an orderly close, so a destination that half-closes
/// first and reads slowly still gets every byte the guest sent.
#[tokio::test]
async fn late_plaintext_is_relayed_over_the_probe_connection_and_closes_in_order() {
    const START: &[u8] = b"upload begins\r\n";
    const BODY_LEN: usize = 2 << 20;
    let (origin, destination) = origin().await;
    let (proxy, _) = proxy(ip_allow(destination)).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let client = async {
        tokio::time::sleep(AFTER_PROBE_STARTS).await;
        guest.write_all(START).await.unwrap();
        let mut rest = Vec::new();
        guest.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        guest.write_all(&vec![b'x'; BODY_LEN]).await.unwrap();
        guest.shutdown().await.unwrap();
    };
    let server = async {
        let (mut probed, _) = origin.accept().await.unwrap();
        // A second dial is refused, as by a passive data port already taken.
        drop(origin);
        let mut start = vec![0_u8; START.len()];
        probed.read_exact(&mut start).await.unwrap();
        assert_eq!(start, START);
        probed.shutdown().await.unwrap();
        // Slow enough that the gateway has data still unsent when its relay ends.
        let mut chunk = vec![0_u8; 8 << 10];
        let mut body = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(2)).await;
            match probed.read(&mut chunk).await {
                Ok(0) => return Ok(body),
                Ok(read) => body += read,
                Err(error) => return Err((body, error.kind())),
            }
        }
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), body) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            client,
            server,
        )
    })
    .await
    .unwrap();
    assert!(
        matches!(outcome, FlowOutcome::Relayed(Relay::Tunnelled { .. })),
        "{:?}",
        outcome.metric_tags()
    );
    assert_eq!(
        body,
        Ok(BODY_LEN),
        "the destination lost the end of the upload"
    );
}

/// The production dialer resets a connection it abandons before returning, as
/// when the flow is cancelled just after the destination accepts, and returns
/// one that closes in order.
#[tokio::test]
async fn the_dialer_resets_an_abandoned_dial_and_returns_an_orderly_connection() {
    fn socket() -> socket2::Socket {
        socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap()
    }
    let (origin, destination) = origin().await;

    let mut abandoned = Box::pin(transport::connect_socket(socket(), destination));
    assert!((&mut abandoned).now_or_never().is_none());
    let (mut accepted, _) = origin.accept().await.unwrap();
    drop(abandoned);
    assert_eq!(drain(&mut accepted).await, (Vec::new(), true));

    let returned = transport::connect_socket(socket(), destination)
        .await
        .unwrap();
    let (mut accepted, _) = origin.accept().await.unwrap();
    drop(returned);
    assert_eq!(drain(&mut accepted).await, (Vec::new(), false));
}

/// A probe cancelled after its dial resets the connection it holds.
#[tokio::test]
async fn a_probe_cancelled_after_its_dial_resets_its_connection() {
    let (origin, destination) = origin().await;
    let (_guest, gateway_guest) = guest_pair().await;
    let token = CancellationToken::new();
    let server = async {
        let (mut stream, _) = origin.accept().await.unwrap();
        // Lets the probe finish its dial first.
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
        drain(&mut stream).await
    };
    let probing = server_first::probe(
        &gateway_guest,
        destination,
        dial,
        server_first::PROBE_WINDOW,
        &token,
    );
    let (probe, drained) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(probing, server)
    })
    .await
    .unwrap();
    assert!(matches!(probe, server_first::Probe::Cancelled));
    assert_eq!(drained, (Vec::new(), true));
}

/// A destination whose greeting arrives when its timer fires.
struct GreetsAfter(std::pin::Pin<Box<tokio::time::Sleep>>);

impl tokio::io::AsyncRead for GreetsAfter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::ready!(std::future::Future::poll(self.0.as_mut(), cx));
        buf.put_slice(b"220 ready\r\n");
        std::task::Poll::Ready(Ok(()))
    }
}

impl transport::Close for GreetsAfter {
    fn set_abortive_close(&self, _: bool) -> io::Result<()> {
        Ok(())
    }
}

/// The probe window is one deadline across the dial and the greeting: a
/// greeting 9 s after a prompt dial is relayed, but a slow dial leaves less
/// time for the greeting.
#[tokio::test(start_paused = true)]
async fn the_probe_window_covers_the_dial_and_the_greeting() {
    for (dial_delay, greeting_delay, greeted) in [
        (Duration::ZERO, Duration::from_secs(9), true),
        (Duration::from_secs(6), Duration::from_secs(5), false),
    ] {
        let (_guest, gateway_guest) = guest_pair().await;
        let connect = move |_| async move {
            tokio::time::sleep(dial_delay).await;
            Ok::<_, transport::Error>(GreetsAfter(Box::pin(tokio::time::sleep(greeting_delay))))
        };
        let probe = server_first::probe(
            &gateway_guest,
            "192.0.2.1:21".parse().unwrap(),
            connect,
            server_first::PROBE_WINDOW,
            &CancellationToken::new(),
        )
        .await;
        let name = format!("dial {dial_delay:?}, greeting {greeting_delay:?}");
        if greeted {
            assert!(
                matches!(probe, server_first::Probe::ServerFirst(_)),
                "{name}"
            );
        } else {
            assert!(matches!(probe, server_first::Probe::TimedOut), "{name}");
        }
    }
}

/// A greeting that cannot reach the guest leaves the probe connection reset.
#[tokio::test]
async fn a_greeting_the_guest_cannot_take_resets_the_probe_connection() {
    let (origin, destination) = origin().await;
    let (proxy, _) = proxy(ip_allow(destination)).await;
    let (guest, mut gateway_guest) = guest_pair().await;
    transport::Close::set_abortive_close(&guest, true).unwrap();
    drop(guest);
    // Wait until the guest's reset has arrived.
    gateway_guest.read(&mut [0_u8; 1]).await.unwrap_err();
    let (upstream, accepted) = tokio::join!(dial(destination), origin.accept());
    let upstream = upstream.unwrap();
    transport::Close::set_abortive_close(&upstream, true).unwrap();

    let probed = server_first::Probed {
        upstream,
        received: b"220 ready\r\n".to_vec(),
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        proxy.relay_probed(gateway_guest, probed),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            outcome,
            FlowOutcome::RelayFailed(RelayFailure::Transfer { .. })
        ),
        "{:?}",
        outcome.metric_tags()
    );
    assert_eq!(outcome.metric_fault(), TcpConnectionFault::GuestWrite);
    let (mut accepted, _) = accepted.unwrap();
    assert_eq!(drain(&mut accepted).await, (Vec::new(), true));
}

/// The guest speaking while the dial is in flight does not abandon it: the
/// connection the destination accepted carries the guest's bytes, and there
/// is no second dial.
#[tokio::test]
async fn a_guest_that_speaks_during_the_dial_keeps_the_dialled_connection() {
    const REQUEST: &[u8] = b"PING example\r\n";
    #[derive(Default)]
    struct Dialer {
        dials: std::sync::atomic::AtomicUsize,
        release: tokio::sync::Notify,
        peer: std::sync::Mutex<Option<tokio::io::DuplexStream>>,
    }
    let dialer: &'static Dialer = Box::leak(Box::default());
    let connect = move |_| async move {
        if dialer.dials.fetch_add(1, Ordering::SeqCst) > 0 {
            return Err(io::Error::from(io::ErrorKind::ConnectionRefused).into());
        }
        dialer.release.notified().await;
        let (upstream, peer) = tokio::io::duplex(1024);
        *dialer.peer.lock().unwrap() = Some(peer);
        Ok::<_, transport::Error>(upstream)
    };
    let destination: SocketAddr = "192.0.2.1:30000".parse().unwrap();
    let (proxy, _) = proxy(ip_allow(destination)).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let peers = async {
        while dialer.dials.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        guest.write_all(REQUEST).await.unwrap();
        guest.shutdown().await.unwrap();
        // Long enough for Tokio to see the guest before the dial completes.
        tokio::time::sleep(Duration::from_millis(100)).await;
        dialer.release.notify_one();
        let mut request = vec![0_u8; REQUEST.len()];
        let mut upstream = loop {
            if let Some(peer) = dialer.peer.lock().unwrap().take() {
                break peer;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        upstream.read_exact(&mut request).await.unwrap();
        assert_eq!(request, REQUEST);
        upstream.write_all(b"PONG\r\n").await.unwrap();
        drop(upstream);
        let mut reply = Vec::new();
        guest.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"PONG\r\n");
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, connect),
            peers,
        )
    })
    .await
    .unwrap();
    assert!(
        matches!(outcome, FlowOutcome::Relayed(Relay::Tunnelled { .. })),
        "{:?}",
        outcome.metric_tags()
    );
    assert_eq!(dialer.dials.load(Ordering::SeqCst), 1);
}

/// A silent guest that never gets a greeting is refused with how the probe
/// ended, after one admission decision.
#[tokio::test(start_paused = true)]
async fn a_silent_guest_without_a_greeting_ends_with_how_the_probe_ended() {
    for (name, destination) in [
        ("neither side speaks", Silent::Idle),
        ("the destination closes first", Silent::Closes),
        ("the dial fails", Silent::Refuses),
        ("the dial outlasts the window", Silent::Hangs),
    ] {
        let (_origin, address) = origin().await;
        let (proxy, requests) = proxy(ip_allow(address)).await;
        let (_guest, gateway_guest) = guest_pair().await;
        let counters = capture_counters_guard();
        let (_, outcome) = tokio::time::timeout(
            server_first::PROBE_WINDOW * 2,
            proxy.handle_with(
                gateway_guest,
                address,
                &CancellationToken::new(),
                move |_| destination.dial(),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("{name}: the probe window does not end the flow"));
        match destination {
            Silent::Idle | Silent::Closes => {
                let detail = if matches!(destination, Silent::Idle) {
                    "server_first_timeout"
                } else {
                    "server_first_upstream_closed"
                };
                assert_eq!(missing_sni_detail(&outcome), Some(detail), "{name}");
                assert_eq!(
                    outcome.metric_tags(),
                    ("rejected", "tls_interception"),
                    "{name}"
                );
            }
            Silent::Refuses => assert!(
                matches!(outcome, FlowOutcome::RelayFailed(RelayFailure::Connect(_))),
                "{name}"
            ),
            Silent::Hangs => assert!(
                matches!(
                    outcome,
                    FlowOutcome::RelayFailed(RelayFailure::Connect(transport::Error::Timeout))
                ),
                "{name}"
            ),
        }
        assert_eq!(admission_decisions(&counters.finish()), 1, "{name}");
        assert_eq!(proxy.stats.tcp_total.load(Ordering::Relaxed), 1, "{name}");
        assert!(requests.lock().unwrap().is_empty(), "{name}");
    }
}

/// An upstream whose greeting is ready at once and that, as the probe reads
/// it, has the guest send `payload` and close, where Tokio has not yet looked.
/// It records what is relayed to it.
struct GreetsAsTheGuestSpeaks {
    guest: Option<std::net::TcpStream>,
    payload: Vec<u8>,
    received: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl GreetsAsTheGuestSpeaks {
    fn new(guest: &TcpStream, payload: Vec<u8>) -> Self {
        let fd = std::os::fd::AsFd::as_fd(guest)
            .try_clone_to_owned()
            .unwrap();
        Self {
            guest: Some(std::net::TcpStream::from(fd)),
            payload,
            received: Arc::default(),
        }
    }
}

impl tokio::io::AsyncRead for GreetsAsTheGuestSpeaks {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        use std::io::Write as _;
        if let Some(mut guest) = self.guest.take() {
            guest.write_all(&self.payload).unwrap();
            guest.shutdown(std::net::Shutdown::Write).unwrap();
            // Loopback delivery completes well within this.
            std::thread::sleep(Duration::from_millis(20));
            buf.put_slice(b"220 ready\r\n");
        }
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for GreetsAsTheGuestSpeaks {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.received.lock().unwrap().extend_from_slice(bytes);
        std::task::Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl transport::Close for GreetsAsTheGuestSpeaks {
    fn set_abortive_close(&self, _: bool) -> io::Result<()> {
        Ok(())
    }
}

/// Tokio sees the destination first, but the guest had already spoken, so the
/// guest wins: plaintext is relayed over the probe connection behind the
/// greeting the probe read, and a ClientHello is inspected with neither side
/// seeing the other's bytes.
#[tokio::test]
async fn a_guest_that_spoke_before_the_greeting_wins_and_keeps_its_bytes() {
    for (name, payload) in [
        ("plaintext", b"PING example\r\n".to_vec()),
        ("ClientHello", client_hello(Some("good.example.test"))),
    ] {
        let destination: SocketAddr = "192.0.2.1:21".parse().unwrap();
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let upstream = GreetsAsTheGuestSpeaks::new(&guest, payload.clone());
        let received = Arc::clone(&upstream.received);
        let dial: &'static std::sync::Mutex<Option<GreetsAsTheGuestSpeaks>> =
            Box::leak(Box::new(std::sync::Mutex::new(Some(upstream))));
        let connect = move |_| async move {
            Ok::<_, transport::Error>(dial.lock().unwrap().take().expect("one dial"))
        };
        let (_, outcome) = tokio::time::timeout(
            Duration::from_secs(5),
            proxy.handle_with(
                gateway_guest,
                destination,
                &CancellationToken::new(),
                connect,
            ),
        )
        .await
        .unwrap();
        let mut reply = Vec::new();
        let _ = guest.read_to_end(&mut reply).await;
        if name == "plaintext" {
            assert!(
                matches!(outcome, FlowOutcome::Relayed(Relay::Tunnelled { .. })),
                "{:?}",
                outcome.metric_tags()
            );
            assert_eq!(reply, b"220 ready\r\n", "{name}");
            assert_eq!(*received.lock().unwrap(), payload, "{name}");
            assert!(requests.lock().unwrap().is_empty(), "{name}");
        } else {
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::TlsInterception(
                        TlsInterceptionError::CertificateRequest(_)
                    ))
                ),
                "{:?}",
                outcome.metric_tags()
            );
            assert!(reply.is_empty(), "{name}");
            assert!(received.lock().unwrap().is_empty(), "{name}");
            assert_eq!(requests.lock().unwrap().len(), 1, "{name}");
        }
    }
}

/// A policy installed during the peek, before the flow is registered for the
/// policy resweep, is caught before the probe: the now-denied destination is
/// never dialled.
#[tokio::test(start_paused = true)]
async fn a_policy_update_during_the_peek_stops_the_flow_before_the_dial() {
    let (origin, destination) = origin().await;
    let (proxy, requests) = proxy(ip_allow(destination)).await;
    let (_guest, gateway_guest) = guest_pair().await;
    let dials: &'static std::sync::atomic::AtomicUsize = Box::leak(Box::default());
    let connect = move |dst| {
        dials.fetch_add(1, Ordering::SeqCst);
        dial(dst)
    };
    let update = async {
        // Inside the 50 ms peek.
        tokio::time::sleep(Duration::from_millis(10)).await;
        install_policy(
            &proxy.network_state,
            intercepted(NetworkPolicyV1::deny_all()),
            "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .await;
    };
    let shutdown = CancellationToken::new();
    let flow = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, connect),
            update,
        )
    })
    .await;
    assert_eq!(
        dials.load(Ordering::SeqCst),
        0,
        "dialled a denied destination"
    );
    let ((_, outcome), ()) = flow.expect("the flow stops without a dial");
    assert!(origin.accept().now_or_never().is_none());
    assert!(matches!(
        outcome,
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::PolicyChanged))
    ));
    assert!(requests.lock().unwrap().is_empty());
}

/// A policy update during the probe never lets the flow through on the old
/// policy: one that denies the destination cancels it through the resweep, and
/// any other refuses the handoff. Neither side receives a byte.
#[tokio::test]
async fn a_policy_update_during_the_probe_stops_the_flow() {
    for name in ["deny", "unrelated allow"] {
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let update = if name == "deny" {
            intercepted(NetworkPolicyV1::deny_all())
        } else {
            let mut policy = ip_allow(destination);
            policy.egress = EgressPolicyV1::new(
                EgressDefaultAction::Deny,
                vec![
                    EgressDestinationV1::Ip(destination.ip()),
                    EgressDestinationV1::Ip("198.51.100.7".parse().unwrap()),
                ],
                Vec::new(),
            )
            .unwrap();
            policy
        };
        let server = async {
            let (mut probe, _) = origin.accept().await.unwrap();
            install_policy(
                &proxy.network_state,
                update,
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .await;
            // A greeting the gateway must not forward; a cancelled probe has
            // already closed, so the write may fail.
            let _ = probe.write_all(b"220 ready\r\n").await;
            let mut received = Vec::new();
            let _ = probe.read_to_end(&mut received).await;
            assert!(
                received.is_empty(),
                "{name}: the destination received guest bytes"
            );
        };
        let client = async {
            // The gateway may reset the refused connection.
            let mut received = Vec::new();
            let _ = guest.read_to_end(&mut received).await;
            assert!(
                received.is_empty(),
                "{name}: the guest received the greeting"
            );
        };
        let shutdown = CancellationToken::new();
        let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                server,
                client,
            )
        })
        .await
        .expect(name);
        if name == "deny" {
            assert!(matches!(outcome, FlowOutcome::Cancelled), "{name}");
        } else {
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::PolicyChanged))
                ),
                "{name}"
            );
        }
        assert!(requests.lock().unwrap().is_empty(), "{name}");
    }
}

/// A guest that speaks after the probe is decided on the policy current when
/// its ClientHello completes, not the snapshot taken before the wait. The
/// probe has handed the flow back by then, so an update that still allows
/// the destination by its domain does not cancel it.
#[tokio::test]
async fn a_late_client_hello_is_decided_on_the_current_policy() {
    let domain_allow = |default| {
        intercepted(NetworkPolicyV1 {
            egress: EgressPolicyV1::new(
                default,
                vec![EgressDestinationV1::Domain(
                    "good.example.test".parse().unwrap(),
                )],
                Vec::new(),
            )
            .unwrap(),
            ..NetworkPolicyV1::allow_all()
        })
    };
    for name in ["unrelated allow", "deny", "domain allow"] {
        let (origin, destination) = origin().await;
        let (initial, update) = match name {
            "unrelated allow" => {
                let mut update = ip_allow(destination);
                update.egress = EgressPolicyV1::new(
                    EgressDefaultAction::Deny,
                    vec![
                        EgressDestinationV1::Ip(destination.ip()),
                        EgressDestinationV1::Ip("198.51.100.7".parse().unwrap()),
                    ],
                    Vec::new(),
                )
                .unwrap();
                (ip_allow(destination), update)
            }
            "deny" => (
                ip_allow(destination),
                intercepted(NetworkPolicyV1::deny_all()),
            ),
            _ => (
                domain_allow(EgressDefaultAction::Allow),
                domain_allow(EgressDefaultAction::Deny),
            ),
        };
        let (proxy, requests) = proxy(initial.clone()).await;
        if name == "domain allow" {
            let dns = proxy.network_state.dns_allow_cache();
            assert!(dns.observe_response(
                &dns_a_response("good.example.test", [0, 0, 0, 0], 60),
                &initial
            ));
            assert!(dns.allows_ip(destination.ip(), &update));
        }
        let (mut guest, gateway_guest) = guest_pair().await;
        let hello = client_hello(Some("good.example.test"));
        let (probing, probed) = tokio::sync::oneshot::channel();
        let server = async {
            let (mut probe, _) = origin.accept().await.unwrap();
            probing.send(()).unwrap();
            let (received, reset) = drain(&mut probe).await;
            assert!(received.is_empty(), "{name}");
            assert!(reset, "{name}: the probe connection was not reset");
        };
        let client = async {
            probed.await.unwrap();
            guest.write_all(&hello[..5]).await.unwrap();
            // Long enough for the probe to see the guest and hand the flow back.
            tokio::time::sleep(Duration::from_millis(150)).await;
            install_policy(
                &proxy.network_state,
                update,
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .await;
            // Lets a resweep cancellation land before the ClientHello completes.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = guest.write_all(&hello[5..]).await;
        };
        let shutdown = CancellationToken::new();
        // As in serving, a resweep cancellation ends the flow.
        let flow = async {
            tokio::select! {
                biased;
                (_, outcome) = proxy.handle_with(gateway_guest, destination, &shutdown, dial) => outcome,
                () = shutdown.cancelled() => FlowOutcome::Cancelled,
            }
        };
        let (outcome, (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(flow, server, client)
        })
        .await
        .expect(name);
        if name == "deny" {
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::Admission(
                        TcpAdmissionDenial::DnsAttribution(_)
                    ))
                ),
                "{name}: {:?}",
                outcome.metric_tags()
            );
            assert!(requests.lock().unwrap().is_empty(), "{name}");
        } else {
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::TlsInterception(
                        TlsInterceptionError::CertificateRequest(_)
                    ))
                ),
                "{name}: {:?}",
                outcome.metric_tags()
            );
            assert_eq!(
                requests.lock().unwrap().as_slice(),
                [("good.example.test".parse().unwrap(), destination.port())],
                "{name}: the ClientHello must reach interception"
            );
        }
        assert!(
            origin.accept().now_or_never().is_none(),
            "{name}: nothing may be relayed to the destination"
        );
    }
}
