use std::net::IpAddr;

use pplx_space_net_common::{
    DenyReason, EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, EgressDefaultAction,
    EgressDestinationV1, EgressPolicyV1, NetworkPolicyV1,
};

use super::*;

const ALLOW: EgressDecision = EgressDecision::Allow;
const RULE_DENY: EgressDecision = EgressDecision::Deny {
    reason: DenyReason::Rule,
};
const DEFAULT_DENY: EgressDecision = EgressDecision::Deny {
    reason: DenyReason::Default,
};

fn destination_rule(value: &str) -> EgressDestinationV1 {
    if let Ok(ip) = value.parse::<IpAddr>() {
        EgressDestinationV1::Ip(ip)
    } else if value.contains('/') {
        EgressDestinationV1::Cidr(value.parse().unwrap())
    } else {
        EgressDestinationV1::Domain(value.parse().unwrap())
    }
}

fn v1_policy(allow: &[&str], deny: &[&str]) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            allow.iter().map(|value| destination_rule(value)).collect(),
            deny.iter().map(|value| destination_rule(value)).collect(),
        )
        .unwrap(),
    }
}

fn dns_a_response(name: &str, ip: [u8; 4]) -> Vec<u8> {
    fn write_name(packet: &mut Vec<u8>, name: &str) {
        for label in name.split('.') {
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
    }

    let mut packet = vec![
        0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    write_name(&mut packet, name);
    packet.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    write_name(&mut packet, name);
    packet.extend_from_slice(&[0x00, 0x01, 0x00, 0x01, 0, 0, 0, 60, 0, 4]);
    packet.extend_from_slice(&ip);
    packet
}

struct UdpProbe {
    name: &'static str,
    policy: NetworkPolicyV1,
    destination: SocketAddr,
    dns_response_name: Option<&'static str>,
    dns_allowed: bool,
    expected: EgressDecision,
}

#[test]
fn udp_permanent_deny_first_covers_ip_and_dns_probe_shapes() {
    const PROXY: &str = "egress-proxy.example.test";
    const S3_BUCKET: &str = "example-user-uploads.s3.amazonaws.com";
    const UNLISTED: &str = "unlisted.example";

    let probes = [
        UdpProbe {
            name: "raw IP overlap",
            policy: v1_policy(&["192.0.2.10"], &["192.0.2.0/24"]),
            destination: "192.0.2.10:443".parse().unwrap(),
            dns_response_name: None,
            dns_allowed: false,
            expected: RULE_DENY,
        },
        UdpProbe {
            name: "raw IP allow without overlap",
            policy: v1_policy(&["192.0.2.11"], &[]),
            destination: "192.0.2.11:443".parse().unwrap(),
            dns_response_name: None,
            dns_allowed: false,
            expected: ALLOW,
        },
        UdpProbe {
            name: "egress proxy DNS attribution plus IP/CIDR overlap",
            policy: v1_policy(&[PROXY, "203.0.113.10"], &["203.0.113.0/24"]),
            destination: "203.0.113.10:443".parse().unwrap(),
            dns_response_name: Some(PROXY),
            dns_allowed: false,
            expected: RULE_DENY,
        },
        UdpProbe {
            name: "egress proxy without DNS attribution",
            policy: v1_policy(&[PROXY], &[]),
            destination: "203.0.113.11:443".parse().unwrap(),
            dns_response_name: None,
            dns_allowed: false,
            expected: DEFAULT_DENY,
        },
        UdpProbe {
            name: "DNS-only S3 bucket has no in-band UDP host",
            policy: v1_policy(&[S3_BUCKET], &[]),
            destination: "198.51.100.20:443".parse().unwrap(),
            dns_response_name: Some(S3_BUCKET),
            dns_allowed: true,
            expected: DEFAULT_DENY,
        },
        UdpProbe {
            name: "observed hostname not attributed by policy",
            policy: v1_policy(&["allowed.example"], &[]),
            destination: "198.51.100.21:443".parse().unwrap(),
            dns_response_name: Some(UNLISTED),
            dns_allowed: false,
            expected: DEFAULT_DENY,
        },
    ];

    for probe in probes {
        let cache = DnsAllowCache::default();
        if let Some(hostname) = probe.dns_response_name {
            let IpAddr::V4(ip) = probe.destination.ip() else {
                unreachable!("test probes use IPv4");
            };
            cache.observe_response(&dns_a_response(hostname, ip.octets()), &probe.policy);
        }
        assert_eq!(
            cache.allows_ip(probe.destination.ip(), &probe.policy),
            probe.dns_allowed,
            "DNS allow: {}",
            probe.name,
        );

        assert_eq!(
            evaluate_udp_egress(&cache, probe.destination, &probe.policy),
            probe.expected,
            "{}",
            probe.name,
        );
    }
}

#[test]
fn reserved_dns_destination_requires_ordinary_ip_admission() {
    let destination = "1.1.1.1:9953".parse().unwrap();
    assert_eq!(
        evaluate_reserved_dns_egress(destination, &v1_policy(&["allowed.example"], &[])),
        DEFAULT_DENY,
    );
    assert_eq!(
        evaluate_reserved_dns_egress(
            destination,
            &v1_policy(&["1.1.1.1", "allowed.example"], &[]),
        ),
        ALLOW,
    );
}

#[test]
fn classifies_reserved_and_generic_dns_udp_datagrams() {
    assert_eq!(
        udp_datagram_route("1.1.1.1:9953".parse().unwrap(), true),
        UdpDatagramRoute::ReservedDns,
    );
    assert_eq!(
        udp_datagram_route("9.9.9.9:53".parse().unwrap(), false),
        UdpDatagramRoute::GenericDns,
    );
    assert_eq!(
        udp_datagram_route("9.9.9.9:443".parse().unwrap(), false),
        UdpDatagramRoute::Generic,
    );
}

#[tokio::test]
async fn generic_udp_53_replies_relay_unchanged() {
    // The network-namespace traffic suite checks that generic DNS replies do
    // not authorize subsequent HTTP/TLS flows through the shared gateway.
    // This lower-level test owns only transparent reply delivery.
    const ALLOWED_HOST: &str = "allowed.example";

    // Loopback stand-ins for the allowed "resolver", the guest, and the
    // gateway's connected upstream and spoofed-source response sockets.
    let resolver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let upstream_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    upstream_socket
        .connect(resolver.local_addr().unwrap())
        .await
        .unwrap();
    let upstream_addr = upstream_socket.local_addr().unwrap();
    let guest = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let response_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let response_addr = response_socket.local_addr().unwrap();
    let flow = FlowKey {
        guest_src: guest.local_addr().unwrap(),
        orig_dst: "198.51.100.53:53".parse().unwrap(),
    };
    let (session_event_tx, _session_event_rx) = mpsc::channel::<SessionEvent>(1);
    let cancel_token = CancellationToken::new();
    let task = ResponseTask {
        sandbox_id: "test",
        flow,
        session_id: 1,
        started_at: Instant::now(),
        upstream_socket,
        response_socket,
        last_active_ms: Arc::new(AtomicU64::new(0)),
        stats: Arc::new(ProxyStats::new()),
        cancel_token: cancel_token.clone(),
        dead: Arc::new(AtomicBool::new(false)),
        completion: SessionCompletion::new(),
        session_event_tx,
        session_permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
    }
    .spawn();

    let forged = dns_a_response(ALLOWED_HOST, [203, 0, 113, 9]);
    resolver.send_to(&forged, upstream_addr).await.unwrap();
    let mut wire = [0_u8; 512];
    let (length, source) = tokio::time::timeout(Duration::from_secs(1), guest.recv_from(&mut wire))
        .await
        .expect("relayed UDP/53 reply timed out")
        .unwrap();
    assert_eq!(source, response_addr);
    assert_eq!(&wire[..length], forged.as_slice());

    cancel_token.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn reserved_udp_datagrams_are_independent_and_replies_preserve_the_destination() {
    let resolver =
        crate::reserved_dns::test_support::RecordedResolver::new("192.0.2.10".parse().unwrap());
    let network_state = ready_network_state(v1_policy(&["1.1.1.1", "allowed.example"], &[])).await;
    let policy = network_state.snapshot().await.policy.unwrap();
    let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    // Transparent source-address routing is covered by the network-namespace
    // traffic suite. Use loopback sockets here to isolate per-datagram policy
    // decisions and replies without requiring IP_TRANSPARENT or host routes.
    let guest = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let response_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let response_addr = response_socket.local_addr().unwrap();
    let dns_tasks = TaskTracker::new();
    let shutdown = CancellationToken::new();
    let dns_allow_cache = DnsAllowCache::default();
    let reserved_dns_cancellation = ReservedDnsCancellation::default();
    let stats = Arc::new(ProxyStats::new());
    let mut proxy = UdpProxy::new(
        UdpProxyConfig {
            sandbox_id: "test",
            listener,
            stats: Arc::clone(&stats),
            network_state,
            dns_allow_cache,
            known_egress_state: crate::known_egress::KnownEgressState::default(),
            reserved_dns: resolver.core(),
            reserved_dns_cancellation: reserved_dns_cancellation.clone(),
            shutdown_token: shutdown,
        },
        dns_tasks.clone(),
    )
    .unwrap();
    let flow = FlowKey {
        guest_src: guest.local_addr().unwrap(),
        orig_dst: response_addr,
    };

    let denied_wire = dns_query("denied.example");
    proxy
        .forward_reserved_dns_datagram(
            flow,
            ReservedResolver::Cloudflare,
            &denied_wire,
            Arc::clone(&policy),
            reserved_dns_cancellation.operation_token(),
            |addr| {
                assert_eq!(addr, response_addr);
                Ok(response_socket)
            },
        )
        .await;
    let (denied, source, _) = recv_dns(&guest).await;
    assert_eq!(source, flow.orig_dst);
    assert_eq!(
        denied.metadata.response_code,
        hickory_resolver::proto::op::ResponseCode::Refused
    );
    assert!(resolver.requests().is_empty());
    assert!(proxy.sessions.is_empty());

    dns_tasks.close();
    dns_tasks.wait().await;
    let response_socket = UdpSocket::bind(response_addr).await.unwrap();
    let allowed_wire = dns_query("ALLOWED.Example.");
    proxy
        .forward_reserved_dns_datagram(
            flow,
            ReservedResolver::Cloudflare,
            &allowed_wire,
            Arc::clone(&policy),
            reserved_dns_cancellation.operation_token(),
            |addr| {
                assert_eq!(addr, response_addr);
                Ok(response_socket)
            },
        )
        .await;
    let (allowed, source, _) = recv_dns(&guest).await;
    assert_eq!(source, flow.orig_dst);
    assert_eq!(
        allowed.metadata.response_code,
        hickory_resolver::proto::op::ResponseCode::NoError
    );
    assert_eq!(resolver.requests().len(), 1);
    assert!(
        proxy
            .dns_allow_cache
            .contains_host_binding("192.0.2.10".parse().unwrap(), "allowed.example",)
    );
    assert!(
        proxy
            .dns_allow_cache
            .allows_ip("192.0.2.10".parse().unwrap(), policy.as_ref())
    );
    assert!(proxy.sessions.is_empty());
    dns_tasks.close();
    dns_tasks.wait().await;
}

async fn ready_network_state(policy: NetworkPolicyV1) -> GatewayNetworkState {
    let state = GatewayNetworkState::pending_policy();
    install_test_policy(&state, policy).await;
    state
}

async fn install_test_policy(state: &GatewayNetworkState, policy: NetworkPolicyV1) {
    const SHA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let policy_ref =
        EffectiveNetworkPolicyRef::local(EffectiveNetworkPolicyRefScope::Sandbox, "sandbox-1", SHA)
            .unwrap();
    let installed_ref = policy_ref.clone();
    let loader: crate::state::PolicyLoader = Arc::new(move || {
        let policy = policy.clone();
        let installed_ref = installed_ref.clone();
        Box::pin(async move {
            Ok(crate::state::PulledNetworkPolicy {
                policy,
                policy_ref: installed_ref,
                network_credentials: Default::default(),
            })
        })
    });
    let snapshot = state
        .refresh(
            loader,
            Some(policy_ref),
            crate::state::NetworkStateReason::ControlRefresh,
        )
        .await;
    assert_eq!(snapshot.mode, crate::proto::NetworkState::Ready);
}

#[tokio::test]
async fn pending_replay_rechecks_ca_readiness_before_each_datagram() {
    use std::sync::atomic::AtomicUsize;

    for permitted_replays in [0, 1, 2] {
        let remaining = Arc::new(AtomicUsize::new(usize::MAX));
        let gate = remaining.clone();
        let state = GatewayNetworkState::pending_policy().with_traffic_gate(Arc::new(move || {
            gate.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }));
        let listener = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut proxy = UdpProxy::new(
            UdpProxyConfig {
                sandbox_id: "test",
                listener,
                stats: Arc::new(ProxyStats::new()),
                network_state: state.clone(),
                dns_allow_cache: DnsAllowCache::default(),
                known_egress_state: KnownEgressState::default(),
                reserved_dns: crate::reserved_dns::test_support::test_core(),
                reserved_dns_cancellation: ReservedDnsCancellation::default(),
                shutdown_token: CancellationToken::new(),
            },
            TaskTracker::new(),
        )
        .unwrap();
        let destination: SocketAddrV4 = "198.18.1.2:9000".parse().unwrap();
        let flow = FlowKey {
            guest_src: "127.0.0.1:10000".parse().unwrap(),
            orig_dst: SocketAddr::V4(destination),
        };
        // Reuse an established session backed by a real local UDP receiver;
        // transparent socket setup is unrelated to replay admission.
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let session = test_session(1).await;
        session
            .upstream_socket
            .connect(receiver.local_addr().unwrap())
            .await
            .unwrap();
        let upstream_socket = Arc::clone(&session.upstream_socket);
        proxy.sessions.put(flow, session);
        for wire in [b"first".as_slice(), b"second"] {
            proxy
                .handle_guest_datagram(flow.guest_src, destination, wire)
                .await;
        }
        assert_eq!(proxy.pending_udp.datagrams.len(), 2);
        let mut policy = NetworkPolicyV1::allow_all();
        policy.tls_interception = pplx_space_net_common::TlsInterception::On;
        install_test_policy(&state, policy).await;
        // Change availability after the install's readiness snapshot; the
        // replay must independently recheck the gate for each datagram.
        remaining.store(permitted_replays, Ordering::SeqCst);
        proxy.replay_pending_udp().await;
        assert!(proxy.pending_udp.is_empty());
        // A marker on the same local socket bounds the observation: any
        // unadmitted replay must be seen before the marker, even when delivery
        // has not completed when send returns.
        upstream_socket.send(b"replay-complete").await.unwrap();
        for wire in [b"first".as_slice(), b"second"]
            .into_iter()
            .take(permitted_replays)
            .chain([b"replay-complete".as_slice()])
        {
            let mut received = [0; 16];
            let length = tokio::time::timeout(Duration::from_secs(1), receiver.recv(&mut received))
                .await
                .expect("admitted replay did not reach the receiver")
                .unwrap();
            assert_eq!(&received[..length], wire);
        }
    }
}

fn dns_query(name: &str) -> Vec<u8> {
    use hickory_resolver::proto::op::{Message, MessageType, OpCode, Query};
    use hickory_resolver::proto::rr::{Name, RecordType};

    let absolute = if name.ends_with('.') {
        name.to_string()
    } else {
        format!("{name}.")
    };
    let mut message = Message::new(0x4567, MessageType::Query, OpCode::Query);
    message.queries.push(Query::query(
        Name::from_ascii(absolute).unwrap(),
        RecordType::A,
    ));
    message.to_vec().unwrap()
}

async fn recv_dns(
    socket: &tokio::net::UdpSocket,
) -> (hickory_resolver::proto::op::Message, SocketAddr, usize) {
    use hickory_resolver::proto::op::Message;

    let mut wire = [0_u8; crate::reserved_dns::MAX_DNS_MESSAGE_BYTES];
    let (length, source) =
        tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut wire))
            .await
            .expect("reserved DNS response timed out")
            .unwrap();
    (Message::from_vec(&wire[..length]).unwrap(), source, length)
}

fn test_flow() -> FlowKey {
    FlowKey {
        guest_src: "127.0.0.1:10000".parse().unwrap(),
        orig_dst: "127.0.0.1:20000".parse().unwrap(),
    }
}

async fn test_udp_socket() -> Arc<UdpSocket> {
    Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap())
}

async fn test_session(session_id: u64) -> Session {
    Session {
        session_id,
        upstream_socket: test_udp_socket().await,
        last_active_ms: Arc::new(AtomicU64::new(0)),
        cancel_token: CancellationToken::new(),
        dead: Arc::new(AtomicBool::new(false)),
        completion: SessionCompletion::new(),
        task: tokio::spawn(std::future::pending()),
    }
}

fn test_sessions(capacity: usize) -> LruCache<FlowKey, Session> {
    LruCache::new(NonZeroUsize::new(capacity).unwrap())
}

#[test]
fn classifies_recoverable_udp_recv_errors() {
    assert!(is_recoverable_udp_recv_error(
        &io::Error::from_raw_os_error(libc::ECONNREFUSED)
    ));
    assert!(is_recoverable_udp_recv_error(&io::Error::new(
        io::ErrorKind::WouldBlock,
        "would block",
    )));
    assert!(is_recoverable_udp_recv_error(&io::Error::new(
        io::ErrorKind::Interrupted,
        "interrupted",
    )));
    assert!(!is_recoverable_udp_recv_error(&io::Error::new(
        io::ErrorKind::PermissionDenied,
        "permission denied",
    )));
}

#[test]
fn active_udp_session_limit_has_an_exact_boundary() {
    assert!(!udp_session_limit_reached(MAX_ACTIVE_UDP_SESSIONS - 1));
    assert!(udp_session_limit_reached(MAX_ACTIVE_UDP_SESSIONS));
    assert!(udp_session_limit_reached(MAX_ACTIVE_UDP_SESSIONS + 1));
}

#[test]
fn classifies_udp_session_create_errors_with_bounded_reasons() {
    assert_eq!(UdpSessionSocketStage::Upstream.as_str(), "upstream");
    assert_eq!(
        UdpSessionSocketStage::Upstream.socket_error_operation(),
        "create_upstream_socket"
    );
    assert_eq!(UdpSessionSocketStage::Response.as_str(), "response");
    assert_eq!(
        UdpSessionSocketStage::Response.socket_error_operation(),
        "create_response_socket"
    );
    assert_eq!(
        udp_session_create_error_reason(&io::Error::from_raw_os_error(libc::EMFILE)),
        "fd_exhausted"
    );
    assert_eq!(
        udp_session_create_error_reason(&io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
        "permission_denied"
    );
    assert_eq!(
        udp_session_create_error_reason(&io::Error::other("other")),
        "other"
    );
    assert_eq!(
        udp_session_create_failure_reason(UdpSessionSocketStage::Upstream, "unreachable"),
        "create_upstream_socket: unreachable"
    );
}

#[tokio::test]
async fn dead_session_is_visible_without_a_terminal_queue_event() {
    let session = test_session(1).await;
    assert!(!session.is_dead());
    session.dead.store(true, Ordering::Release);
    assert!(session.is_dead());
}

#[tokio::test]
async fn dead_session_lookup_removes_the_mapping_and_updates_stats() {
    let flow = test_flow();
    let stats = ProxyStats::new();
    let mut sessions = test_sessions(1);
    let mut response_sockets = HashMap::new();
    let session = test_session(1).await;
    session.dead.store(true, Ordering::Release);
    sessions.put(flow, session);
    response_sockets.insert(flow.orig_dst, test_udp_socket().await);
    stats.udp_active.store(1, Ordering::Relaxed);

    assert!(
        lookup_live_udp_session(&mut sessions, &mut response_sockets, flow, 10, &stats)
            .await
            .is_none()
    );
    assert!(sessions.is_empty());
    assert!(response_sockets.is_empty());
    assert_eq!(stats.udp_active(), 0);
}

#[tokio::test]
async fn capacity_reclaims_the_lru_session_without_waiting_for_idle_expiry() {
    let lru_flow = test_flow();
    let recent_flow = FlowKey {
        guest_src: "127.0.0.1:10001".parse().unwrap(),
        orig_dst: "127.0.0.1:20001".parse().unwrap(),
    };
    let stats = ProxyStats::new();
    let mut sessions = test_sessions(2);
    let mut response_sockets = HashMap::new();
    sessions.put(lru_flow, test_session(1).await);
    sessions.put(recent_flow, test_session(2).await);
    response_sockets.insert(lru_flow.orig_dst, test_udp_socket().await);
    response_sockets.insert(recent_flow.orig_dst, test_udp_socket().await);
    stats.udp_active.store(2, Ordering::Relaxed);
    assert!(sessions.get(&recent_flow).is_some());

    assert!(evict_lru_udp_session(&mut sessions, &mut response_sockets, &stats).await);

    assert!(sessions.peek(&lru_flow).is_none());
    assert!(sessions.peek(&recent_flow).is_some());
    assert!(!response_sockets.contains_key(&lru_flow.orig_dst));
    assert!(response_sockets.contains_key(&recent_flow.orig_dst));
    assert_eq!(stats.udp_active(), 1);
}

#[tokio::test]
async fn capacity_reclaim_releases_the_task_owned_permit_before_returning() {
    let flow = test_flow();
    let stats = ProxyStats::new();
    let slots = Arc::new(Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let dead = Arc::new(AtomicBool::new(false));
    let task_dead = dead.clone();
    let completion = SessionCompletion::new();
    let task_completion = completion.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _guard = SessionTaskGuard {
            sandbox_id: "test-sandbox",
            flow,
            session_id: 1,
            dead: task_dead,
            completion: task_completion,
            _permit: permit,
        };
        let _ = ready_tx.send(());
        std::future::pending::<()>().await;
    });
    ready_rx.await.unwrap();

    let mut sessions = test_sessions(1);
    sessions.put(
        flow,
        Session {
            session_id: 1,
            upstream_socket: test_udp_socket().await,
            last_active_ms: Arc::new(AtomicU64::new(0)),
            cancel_token: CancellationToken::new(),
            dead: dead.clone(),
            completion,
            task,
        },
    );
    let mut response_sockets = HashMap::new();

    assert!(evict_lru_udp_session(&mut sessions, &mut response_sockets, &stats).await);
    assert!(dead.load(Ordering::Acquire));
    let _released_permit = slots
        .try_acquire_owned()
        .expect("evicted task releases its session permit");
}

#[tokio::test]
async fn session_task_guard_holds_permit_and_marks_dead_on_drop() {
    let slots = Arc::new(Semaphore::new(1));
    let permit = slots.clone().try_acquire_owned().unwrap();
    let dead = Arc::new(AtomicBool::new(false));
    let guard = SessionTaskGuard {
        sandbox_id: "test-sandbox",
        flow: test_flow(),
        session_id: 1,
        dead: dead.clone(),
        completion: SessionCompletion::new(),
        _permit: permit,
    };

    if slots.clone().try_acquire_owned().is_ok() {
        panic!("task guard must retain its session permit");
    }
    drop(guard);
    assert!(dead.load(Ordering::Acquire));
    let _released_permit = slots
        .try_acquire_owned()
        .expect("dropping the task guard releases its permit");
}

#[test]
fn session_completion_preserves_the_first_outcome_when_cleanup_and_task_race() {
    let completion = SessionCompletion::new();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let cleanup_completion = completion.clone();
        let cleanup_barrier = barrier.clone();
        let cleanup = scope.spawn(move || {
            let outcome = UdpSessionOutcome::CapacityEviction;
            cleanup_barrier.wait();
            let won = cleanup_completion.mark(outcome.clone());
            (outcome, won)
        });
        let task_completion = completion.clone();
        let task = scope.spawn(move || {
            let outcome = UdpSessionOutcome::ReceiveUpstreamFailed {
                error: "test receive failure".to_string(),
            };
            barrier.wait();
            let won = task_completion.mark(outcome.clone());
            (outcome, won)
        });
        [cleanup.join().unwrap(), task.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|(_, won)| *won).count(), 1);
    let winner = results
        .into_iter()
        .find_map(|(outcome, won)| won.then_some(outcome))
        .unwrap();
    assert_eq!(completion.outcome(), winner);
    assert!(!completion.mark(UdpSessionOutcome::TaskExit));
}

#[test]
fn clear_dropped_reports_queued_datagrams_and_bytes_and_resets_stats() {
    let stats = ProxyStats::new();
    let mut queue = PendingUdpQueue::default();
    let flow = test_flow();
    let now = Instant::now();

    queue.push(flow, b"abc", now, "test-sandbox", &stats);
    queue.push(flow, b"12345", now, "test-sandbox", &stats);

    assert_eq!(stats.udp_pending_datagrams(), 2);
    assert_eq!(stats.udp_pending_bytes(), 8);

    let dropped = queue.clear_dropped(
        PendingUdpClearReason::NonReadyPolicy,
        "test-sandbox",
        &stats,
    );

    assert_eq!(
        dropped,
        PendingUdpDropAccounting {
            datagrams: 2,
            bytes: 8,
        }
    );
    assert!(queue.is_empty());
    assert_eq!(queue.bytes, 0);
    assert!(queue.per_flow.is_empty());
    assert_eq!(stats.udp_pending_datagrams(), 0);
    assert_eq!(stats.udp_pending_bytes(), 0);
}

#[test]
fn pending_datagram_survives_full_pending_policy_grace() {
    // A guest UDP datagram (the motivating case: a DNS query) buffered
    // during a transient PendingPolicy refresh must survive at least a full
    // policy-hydration cycle so it can be replayed once the new policy
    // lands, rather than dropped mid-refresh and surfaced as a resolver
    // timeout. The former 1s grace was shorter than the hydration deadline
    // and dropped DNS during a >1s refresh; this pins the grace to at least
    // a hydration cycle so that regression can't return.
    assert_eq!(
        UDP_PENDING_MAX_AGE,
        crate::state::TCP_PENDING_TIMEOUT,
        "TCP and UDP pending-policy windows must stay aligned"
    );
    assert!(
        UDP_PENDING_MAX_AGE >= crate::state::HYDRATION_DEADLINE,
        "pending traffic must cover a full policy-hydration cycle"
    );

    let stats = ProxyStats::new();
    let mut queue = PendingUdpQueue::default();
    let flow = test_flow();
    let t0 = Instant::now();

    queue.push(flow, b"dns-query", t0, "test-sandbox", &stats);
    assert_eq!(stats.udp_pending_datagrams(), 1);

    // After a full hydration cycle the datagram must still be queued,
    // awaiting a Ready policy to replay it. (Under the former 1s grace it
    // would already have been evicted here.)
    queue.evict_expired(
        t0 + crate::state::HYDRATION_DEADLINE,
        "test-sandbox",
        &stats,
    );
    assert_eq!(
        stats.udp_pending_datagrams(),
        1,
        "datagram evicted before a full policy-hydration cycle elapsed"
    );

    // Past the grace: the datagram is evicted.
    let expired = t0 + UDP_PENDING_MAX_AGE + Duration::from_millis(1);
    queue.evict_expired(expired, "test-sandbox", &stats);
    assert_eq!(
        stats.udp_pending_datagrams(),
        0,
        "datagram not evicted after the pending-policy grace elapsed"
    );
}

#[tokio::test]
async fn dead_event_removes_matching_session() {
    let flow = test_flow();
    let stats = ProxyStats::new();
    let mut sessions = test_sessions(1);
    let mut response_sockets = HashMap::new();
    sessions.put(flow, test_session(7).await);
    response_sockets.insert(flow.orig_dst, test_udp_socket().await);
    stats.udp_active.store(1, Ordering::Relaxed);

    handle_udp_session_event(
        "test-sandbox",
        &mut sessions,
        &mut response_sockets,
        &stats,
        SessionEvent::Dead {
            flow,
            session_id: 7,
            error: "hard failure".to_string(),
        },
    )
    .await;

    assert!(sessions.is_empty());
    assert!(response_sockets.is_empty());
    assert_eq!(stats.udp_active(), 0);
}

#[tokio::test]
async fn dead_event_does_not_remove_newer_session() {
    let flow = test_flow();
    let stats = ProxyStats::new();
    let mut sessions = test_sessions(1);
    let mut response_sockets = HashMap::new();
    sessions.put(flow, test_session(8).await);
    response_sockets.insert(flow.orig_dst, test_udp_socket().await);
    stats.udp_active.store(1, Ordering::Relaxed);

    handle_udp_session_event(
        "test-sandbox",
        &mut sessions,
        &mut response_sockets,
        &stats,
        SessionEvent::Dead {
            flow,
            session_id: 7,
            error: "stale failure".to_string(),
        },
    )
    .await;

    assert_eq!(sessions.len(), 1);
    assert_eq!(response_sockets.len(), 1);
    assert_eq!(stats.udp_active(), 1);
}
