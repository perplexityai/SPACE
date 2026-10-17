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

async fn must_not_dial(dst: SocketAddr) -> Result<TcpStream, transport::Error> {
    panic!("must not dial {dst}");
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

#[tokio::test]
async fn a_server_first_greeting_reaches_a_silent_guest_and_the_session_relays() {
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
        let (mut guest, gateway_guest) = guest_pair().await;

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
            stream.write_all(b"331 password\r\n").await.unwrap();
        };
        let client = async {
            let mut greeting = [0_u8; 28];
            guest.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"220 ftp.example.test ready\r\n", "{name}");
            guest.write_all(b"USER guest\r\n").await.unwrap();
            guest.shutdown().await.unwrap();
            let mut reply = Vec::new();
            guest.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"331 password\r\n", "{name}");
        };
        let shutdown = CancellationToken::new();
        let counters = capture_counters_guard();
        let values = capture_values_guard();
        let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                server,
                client,
            )
        })
        .await
        .expect(name);
        assert_eq!(admission_decisions(&counters.finish()), 1, "{name}");
        // The greeting wait is not ClientHello time; only the peek is.
        let client_hello_ms: Vec<f64> = values
            .finish()
            .into_iter()
            .filter(|value| value.name == "gateway.tls.client_hello.duration_ms")
            .map(|value| value.value)
            .collect();
        assert!(
            matches!(client_hello_ms.as_slice(), [ms] if *ms < 500.0),
            "{name}: {client_hello_ms:?}"
        );

        let FlowOutcome::Relayed(Relay::Tunnelled { wire_tx, wire_rx }) = outcome else {
            panic!("{name}: a server-first flow must relay");
        };
        assert_eq!(
            (wire_tx, wire_rx),
            (12, 28 + 14),
            "{name}: greeting is counted"
        );
        assert!(requests.lock().unwrap().is_empty(), "{name}");
        assert_eq!(proxy.stats.tcp_total.load(Ordering::Relaxed), 1, "{name}");
    }
}

/// A ClientHello sent after the probe starts, even after the destination
/// closed the probe connection, is handled like an on-time one: it reaches
/// interception, and the probe connection gets no bytes and is reset.
#[tokio::test]
async fn a_late_client_hello_is_inspected_and_the_probe_connection_sees_nothing() {
    let hello = client_hello(Some("good.example.test"));
    let mut outcomes = Vec::new();
    for timing in ["on time", "late", "after the destination closed"] {
        let late = timing != "on time";
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let client = async {
            if late {
                tokio::time::sleep(AFTER_PROBE_STARTS).await;
            }
            guest.write_all(&hello).await.unwrap();
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
        assert!(
            origin.accept().now_or_never().is_none(),
            "{timing}: nothing may be relayed to the destination"
        );
        outcomes.push(outcome.metric_tags());
    }
    assert!(
        outcomes.windows(2).all(|pair| pair[0] == pair[1]),
        "lateness changed the outcome: {outcomes:?}"
    );
}

/// Late plaintext is relayed over the probe connection itself, as a
/// destination that accepts one connection (an FTP passive data port) needs.
#[tokio::test]
async fn late_plaintext_is_relayed_over_the_probe_connection() {
    const REQUEST: &[u8] = b"PING example\r\n";
    let (origin, destination) = origin().await;
    let (proxy, requests) = proxy(ip_allow(destination)).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let client = async {
        tokio::time::sleep(AFTER_PROBE_STARTS).await;
        guest.write_all(REQUEST).await.unwrap();
        guest.shutdown().await.unwrap();
        let mut reply = Vec::new();
        guest.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"PONG\r\n");
    };
    let server = async {
        let (mut probed, _) = origin.accept().await.unwrap();
        // A second dial is refused, as by a passive data port already taken.
        drop(origin);
        let mut request = Vec::new();
        probed.read_to_end(&mut request).await.unwrap();
        assert_eq!(
            request, REQUEST,
            "the request must arrive on the probe connection"
        );
        probed.write_all(b"PONG\r\n").await.unwrap();
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            proxy.handle_with(gateway_guest, destination, &shutdown, dial),
            client,
            server,
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        FlowOutcome::Relayed(Relay::Tunnelled {
            wire_tx: 14,
            wire_rx: 6
        })
    ));
    assert!(requests.lock().unwrap().is_empty());
}

/// Late bytes interception refuses, such as an upload whose first byte is a
/// TLS record type, reset the probe connection. An orderly close would tell a
/// passive FTP data port the upload completed empty.
#[tokio::test]
async fn a_refused_late_payload_resets_the_probe_connection() {
    const UPLOAD: &[u8] = b"\x16 binary file content, not a TLS handshake";
    let (origin, destination) = origin().await;
    let (proxy, requests) = proxy(ip_allow(destination)).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let client = async {
        tokio::time::sleep(AFTER_PROBE_STARTS).await;
        guest.write_all(UPLOAD).await.unwrap();
        // The gateway may already have reset the guest.
        let _ = guest.shutdown().await;
    };
    let server = async {
        let (mut probed, _) = origin.accept().await.unwrap();
        drain(&mut probed).await
    };
    let shutdown = CancellationToken::new();
    let ((_, outcome), (), (received, reset)) =
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                proxy.handle_with(gateway_guest, destination, &shutdown, dial),
                client,
                server,
            )
        })
        .await
        .unwrap();
    assert_eq!(outcome.metric_tags(), ("rejected", "tls_interception"));
    assert!(
        received.is_empty(),
        "the probe connection received guest bytes"
    );
    assert!(
        reset,
        "a refused flow's probe connection was closed, not reset"
    );
    assert!(requests.lock().unwrap().is_empty());
}

/// A relay over the probe connection ends with an orderly close, so a
/// destination that half-closes first and reads slowly still gets every byte
/// the guest sent.
#[tokio::test]
async fn a_relayed_probe_connection_closes_in_order() {
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

/// A probe that times out, or is cancelled after its dial, resets the
/// connection it holds.
#[tokio::test]
async fn a_timed_out_or_cancelled_probe_resets_its_connection() {
    for cancelled in [false, true] {
        let (origin, destination) = origin().await;
        let (_guest, gateway_guest) = guest_pair().await;
        let token = CancellationToken::new();
        let server = async {
            let (mut stream, _) = origin.accept().await.unwrap();
            if cancelled {
                // Lets the probe finish its dial first.
                tokio::time::sleep(Duration::from_millis(50)).await;
                token.cancel();
            }
            drain(&mut stream).await
        };
        let window = if cancelled {
            server_first::PROBE_WINDOW
        } else {
            Duration::from_millis(200)
        };
        let probing = server_first::probe(&gateway_guest, destination, dial, window, &token);
        let (probe, drained) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(probing, server)
        })
        .await
        .unwrap();
        if cancelled {
            assert!(matches!(probe, server_first::Probe::Cancelled));
        } else {
            assert!(matches!(probe, server_first::Probe::TimedOut));
        }
        assert_eq!(drained, (Vec::new(), true), "cancelled: {cancelled}");
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
        matches!(outcome, FlowOutcome::RelayFailed(RelayFailure::Transfer(_))),
        "{:?}",
        outcome.metric_tags()
    );
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
        let (_, outcome) = proxy
            .handle_with(
                gateway_guest,
                address,
                &CancellationToken::new(),
                move |_| destination.dial(),
            )
            .await;
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

/// A domain allow needs the ClientHello to bind the hostname, so a silent
/// guest is still refused and the destination is never dialled.
#[tokio::test(start_paused = true)]
async fn a_dns_only_allow_still_fails_closed_for_a_silent_guest() {
    let policy = intercepted(NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain("pypi.org".parse().unwrap())],
            Vec::new(),
        )
        .unwrap(),
    });
    let (proxy, _) = proxy(policy.clone()).await;
    proxy
        .network_state
        .dns_allow_cache()
        .observe_response(&dns_a_response("pypi.org", [93, 184, 216, 34], 60), &policy);
    let destination: SocketAddr = "93.184.216.34:21".parse().unwrap();
    let (_guest, gateway_guest) = guest_pair().await;
    let (_, outcome) = proxy
        .handle_with(
            gateway_guest,
            destination,
            &CancellationToken::new(),
            must_not_dial,
        )
        .await;
    assert!(matches!(
        outcome,
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::DnsAttribution(
            DenyReason::Default
        )))
    ));
}

/// Without interception a silent guest is relayed after the short peek over
/// one destination connection; there is no probe.
#[tokio::test]
async fn interception_off_relays_a_silent_guest_without_probing() {
    let (origin, destination) = origin().await;
    let (proxy, _) = proxy(NetworkPolicyV1::allow_all()).await;
    let (mut guest, gateway_guest) = guest_pair().await;
    let server = async {
        let (mut stream, _) = origin.accept().await.unwrap();
        stream.write_all(b"220 ready\r\n").await.unwrap();
    };
    let client = async {
        let mut greeting = Vec::new();
        guest.read_to_end(&mut greeting).await.unwrap();
        assert_eq!(greeting, b"220 ready\r\n");
        guest.shutdown().await.unwrap();
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
    assert!(matches!(
        outcome,
        FlowOutcome::Relayed(Relay::Tunnelled { .. })
    ));
    assert!(origin.accept().now_or_never().is_none());
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
/// its ClientHello completes, not the snapshot taken before the wait.
#[tokio::test]
async fn a_late_client_hello_is_decided_on_the_current_policy() {
    for name in ["unrelated allow", "deny"] {
        let (origin, destination) = origin().await;
        let (proxy, requests) = proxy(ip_allow(destination)).await;
        let (mut guest, gateway_guest) = guest_pair().await;
        let hello = client_hello(Some("good.example.test"));
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
            // The probe has settled once the flow leaves the registry.
            while proxy.network_state.tcp_connections().len() > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            install_policy(
                &proxy.network_state,
                update,
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .await;
            guest.write_all(&hello[5..]).await.unwrap();
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
            assert!(
                matches!(
                    outcome,
                    FlowOutcome::Rejected(Rejection::Admission(
                        TcpAdmissionDenial::DnsAttribution(_)
                    ))
                ),
                "{name}"
            );
            assert!(requests.lock().unwrap().is_empty(), "{name}");
        } else {
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
