use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::dns::test_fixtures::a_response as dns_a_response;
use crate::tls_sni::ClientHelloScan;
use pplx_space_net_common::{
    DenyReason, EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, EgressDefaultAction,
    EgressDestinationV1, EgressPolicyV1, NetworkPolicyV1,
};
use pplx_space_observability::test_capture::{CapturedEvent, CapturedEventsLayer, field_value};
use tracing::instrument::WithSubscriber as _;
use tracing_subscriber::layer::SubscriberExt as _;

mod tls_interception;

#[tokio::test]
async fn silent_peers_keep_policy_denials_distinct_from_enabled_inspection() {
    use pplx_space_net_common::TlsInterception as Mode;

    for (
        mode,
        allowed,
        expected_outcome,
        expected_reason,
        expected_stage,
        expected_path,
        policy_reason,
    ) in [
        (
            Mode::On,
            true,
            "denied",
            "first_byte_timeout",
            "pre_tls",
            "tls_inspection",
            "allowed",
        ),
        (
            Mode::Off,
            true,
            "skipped",
            "inspection_disabled",
            "handoff",
            "tunnel",
            "allowed",
        ),
        (
            Mode::On,
            false,
            "denied",
            "first_byte_timeout",
            "destination_policy",
            "unknown",
            "default_deny",
        ),
        (
            Mode::Off,
            false,
            "denied",
            "first_byte_timeout",
            "destination_policy",
            "unknown",
            "default_deny",
        ),
    ] {
        let upstream = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let dst = upstream.local_addr().unwrap();
        let mut policy = if allowed {
            NetworkPolicyV1::allow_all()
        } else {
            NetworkPolicyV1::deny_all()
        };
        policy.tls_interception = mode;
        let (state, _) = ready_policy(policy).await;
        let (mut proxy, _) = dns_attributed_pypi_proxy().await;
        proxy.network_state = state;
        proxy.tls_interception = TlsInterception::new(Arc::new(|_, _| {
            Box::pin(async { panic!("a silent peer must not request a certificate") })
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (guest, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let guest = guest.unwrap();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let shutdown = CancellationToken::new();
        let handle = proxy.handle(accepted.unwrap().0, dst, &shutdown);
        let (_, outcome) = if mode == Mode::Off && allowed {
            tokio::pin!(handle);
            let outcome = tokio::select! {
                outcome = &mut handle => outcome,
                accepted = upstream.accept() => {
                    let (origin, _) = accepted.unwrap();
                    drop(guest);
                    drop(origin);
                    handle.await
                }
            };
            assert!(!matches!(outcome.1, FlowOutcome::Rejected(_)));
            outcome
        } else {
            let outcome = handle.await;
            assert!(matches!(outcome.1, FlowOutcome::Rejected(_)));
            assert!(futures_util::FutureExt::now_or_never(upstream.accept()).is_none());
            drop(guest);
            outcome
        };
        assert_eq!(
            matches!(outcome, FlowOutcome::Rejected(_)),
            !(mode == Mode::Off && allowed)
        );
        let counters = capture.finish();
        let admission: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name == "gateway.inspection.admission")
            .collect();
        let [metric] = admission.as_slice() else {
            panic!("expected exactly one admission: {admission:?}")
        };
        assert_eq!(metric.value, 1);
        assert_eq!(
            metric.tags,
            [
                format!("outcome:{expected_outcome}"),
                format!("reason:{expected_reason}"),
                format!("tls_interception:{}", mode.as_str()),
                format!("stage:{expected_stage}"),
                format!("path:{expected_path}"),
                format!("policy_reason:{policy_reason}"),
            ]
        );
        assert!(
            !counters
                .iter()
                .any(|counter| counter.name == "gateway.inspection.session")
        );
    }
}

fn dns_attribution_denied(reason: DenyReason) -> TcpAdmissionDenial {
    TcpAdmissionDenial::DnsAttribution(reason)
}

fn v1_allow_all() -> NetworkPolicyV1 {
    NetworkPolicyV1::allow_all()
}

async fn ready_policy(policy: NetworkPolicyV1) -> (GatewayNetworkState, Arc<NetworkPolicyV1>) {
    let state = GatewayNetworkState::pending_policy();
    let policy = install_policy(
        &state,
        policy,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .await;
    (state, policy)
}

async fn install_policy(
    state: &GatewayNetworkState,
    policy: NetworkPolicyV1,
    content_hash: &str,
) -> Arc<NetworkPolicyV1> {
    let expected_policy_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "tcp-enforcement-test",
        content_hash,
    )
    .unwrap();
    let policy_ref = expected_policy_ref.to_string();
    let loader: crate::state::PolicyLoader = Arc::new(move || {
        let policy = policy.clone();
        let policy_ref = policy_ref.clone();
        Box::pin(async move {
            Ok(crate::state::PulledNetworkPolicy {
                policy,
                policy_ref,
                network_credentials: Default::default(),
            })
        })
    });
    state
        .refresh(
            loader,
            Some(expected_policy_ref),
            crate::state::NetworkStateReason::Startup,
        )
        .await;
    let TrafficPolicy::Ready(policy) = state.wait_for_tcp_policy().await else {
        panic!("test policy must be ready");
    };
    policy
}

struct FailedPlaintextConnect {
    response: Vec<u8>,
    outcome: FlowOutcome,
    event: CapturedEvent,
}

async fn failed_plaintext_connect() -> FailedPlaintextConnect {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};

    const ALLOWED_HOST: &str = "pypi.org";
    const DESTINATION_IP: &str = "93.184.216.34";
    let destination: SocketAddr = format!("{DESTINATION_IP}:80").parse().unwrap();
    let policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain(ALLOWED_HOST.parse().unwrap())],
            Vec::new(),
        )
        .unwrap(),
    };
    let (network_state, policy) = ready_policy(policy).await;
    let dns_allow_cache = DnsAllowCache::default();
    dns_allow_cache.observe_response(
        &dns_a_response(ALLOWED_HOST, [93, 184, 216, 34], 60),
        policy.as_ref(),
    );
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        dns_allow_cache,
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    let guest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (guest, accepted) = tokio::join!(
        TcpStream::connect(guest_listener.local_addr().unwrap()),
        guest_listener.accept(),
    );
    let mut guest = guest.unwrap();
    let (gateway_guest, _) = accepted.unwrap();

    let layer = CapturedEventsLayer::new().with_event_filter(|event| {
        field_value(event, "message") == Some("plaintext HTTP upstream connection failed")
    });
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let relay = tokio::spawn(
        async move {
            proxy
                .host_bound_http(gateway_guest, destination, |destination| async move {
                    Err::<DuplexStream, _>(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("test connect refusal to {destination}"),
                    ))
                })
                .await
        }
        .with_subscriber(subscriber),
    );

    guest
        .write_all(
            format!("GET / HTTP/1.1\r\nHost: {ALLOWED_HOST}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), guest.read_to_end(&mut response))
        .await
        .expect("the 502 response must close the guest connection")
        .unwrap();
    let outcome = relay.await.unwrap();
    let mut events = events.events();
    assert_eq!(
        events.len(),
        1,
        "expected one upstream connection failure event"
    );

    FailedPlaintextConnect {
        response,
        outcome,
        event: events.remove(0),
    }
}

/// A proxy whose policy allows only `pypi.org` by domain, with the guest having
/// resolved it to `93.184.216.34`.
async fn dns_attributed_pypi_proxy() -> (TcpProxy, Arc<NetworkPolicyV1>) {
    let policy = NetworkPolicyV1 {
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
    };
    let (network_state, policy) = ready_policy(policy).await;
    let dns_allow_cache = DnsAllowCache::default();
    dns_allow_cache.observe_response(
        &dns_a_response("pypi.org", [93, 184, 216, 34], 60),
        policy.as_ref(),
    );
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        dns_allow_cache,
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    (proxy, policy)
}

#[tokio::test]
async fn only_a_dns_attributed_fail_closed_destination_waits_for_a_late_client_hello() {
    let (proxy, policy) = dns_attributed_pypi_proxy().await;
    let ready = TrafficPolicy::Ready(policy);
    let attributed: SocketAddr = "93.184.216.34:443".parse().unwrap();
    let unattributed: SocketAddr = "203.0.113.9:443".parse().unwrap();

    assert_eq!(
        proxy.missing_client_hello(attributed, &ready),
        sniff::MissingClientHello::Denied,
        "a resolved allowed domain's address is decided by its ClientHello"
    );
    assert_eq!(
        proxy.missing_client_hello(unattributed, &ready),
        sniff::MissingClientHello::Proceeds,
        "an address with no DNS evidence is refused whatever the ClientHello says"
    );
    assert_eq!(
        proxy.missing_client_hello(
            attributed,
            &TrafficPolicy::NotReady(PolicyNotReady::PendingPolicyTimeout)
        ),
        sniff::MissingClientHello::Proceeds,
        "without a policy nothing is admitted on the ClientHello"
    );
}

#[tokio::test]
async fn an_address_refused_before_its_client_hello_is_read_does_not_wait_for_one() {
    // Both addresses were resolved through the allowed domain, but a guardrail
    // (link-local metadata) and an explicit deny refuse them outright.
    let denied: SocketAddr = "93.184.216.34:443".parse().unwrap();
    let guardrailed: SocketAddr = "169.254.169.254:443".parse().unwrap();
    let policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain("pypi.org".parse().unwrap())],
            vec![EgressDestinationV1::Ip(denied.ip())],
        )
        .unwrap(),
    };
    let (network_state, policy) = ready_policy(policy).await;
    let (mut proxy, _) = dns_attributed_pypi_proxy().await;
    proxy.network_state = network_state;
    for address in [[93, 184, 216, 34], [169, 254, 169, 254]] {
        proxy
            .dns_allow_cache
            .observe_response(&dns_a_response("pypi.org", address, 60), policy.as_ref());
    }
    let ready = TrafficPolicy::Ready(policy);

    for destination in [denied, guardrailed] {
        assert_eq!(
            proxy.missing_client_hello(destination, &ready),
            sniff::MissingClientHello::Proceeds,
            "{destination} is refused whatever its ClientHello says"
        );
    }
}

#[tokio::test]
async fn tls_interception_waits_for_a_late_client_hello_on_an_allowed_destination() {
    // Interception closes any admitted flow it cannot read SNI from, so under
    // an allow-all policy a late first segment must not read as "no SNI".
    let (proxy, _) = dns_attributed_pypi_proxy().await;
    let destination: SocketAddr = "203.0.113.9:443".parse().unwrap();
    let with_interception = |interception| {
        let mut policy = v1_allow_all();
        policy.tls_interception = interception;
        TrafficPolicy::Ready(Arc::new(policy))
    };

    assert_eq!(
        proxy.missing_client_hello(
            destination,
            &with_interception(pplx_space_net_common::TlsInterception::On)
        ),
        sniff::MissingClientHello::Denied,
        "interception refuses a flow without SNI, so it must wait out a retransmit"
    );
    assert_eq!(
        proxy.missing_client_hello(
            destination,
            &with_interception(pplx_space_net_common::TlsInterception::Off)
        ),
        sniff::MissingClientHello::Proceeds,
        "without interception the flow proceeds without SNI, so waiting only delays it"
    );

    let mut explicit_deny = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: pplx_space_net_common::TlsInterception::On,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Allow,
            Vec::new(),
            vec![EgressDestinationV1::Ip(destination.ip())],
        )
        .unwrap(),
    };
    assert_eq!(
        proxy.missing_client_hello(
            destination,
            &TrafficPolicy::Ready(Arc::new(explicit_deny.clone()))
        ),
        sniff::MissingClientHello::Proceeds,
        "an explicitly denied address is refused whatever its ClientHello says"
    );
    explicit_deny.tls_interception = pplx_space_net_common::TlsInterception::Off;
    assert_eq!(
        proxy.missing_client_hello(destination, &TrafficPolicy::Ready(Arc::new(explicit_deny))),
        sniff::MissingClientHello::Proceeds,
    );
}

#[tokio::test]
async fn a_flow_refused_before_its_client_hello_arrives_is_reset_not_ended() {
    use tokio::io::AsyncReadExt as _;

    for mode in [
        pplx_space_net_common::TlsInterception::On,
        pplx_space_net_common::TlsInterception::Off,
    ] {
        let (mut proxy, policy) = dns_attributed_pypi_proxy().await;
        let mut policy = policy.as_ref().clone();
        policy.tls_interception = mode;
        let (state, policy) = ready_policy(policy).await;
        proxy.network_state = state;
        proxy
            .dns_allow_cache
            .observe_response(&dns_a_response("pypi.org", [93, 184, 216, 34], 60), &policy);
        let destination: SocketAddr = "93.184.216.34:443".parse().unwrap();
        let guest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (guest, accepted) = tokio::join!(
            TcpStream::connect(guest_listener.local_addr().unwrap()),
            guest_listener.accept(),
        );
        let mut guest = guest.unwrap();
        let (gateway_guest, _) = accepted.unwrap();

        // The guest never sends its ClientHello, so admission refuses the flow
        // with nothing unread on the socket: a plain close would be a FIN.
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let (_, outcome) = proxy
            .handle(gateway_guest, destination, &CancellationToken::new())
            .await;
        assert!(
            matches!(outcome, FlowOutcome::Rejected(Rejection::Admission(_))),
            "a silent flow to a DNS-attributed address must be refused at admission"
        );
        let counters = capture.finish();
        let inspection: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name.starts_with("gateway.inspection."))
            .collect();
        let [metric] = inspection.as_slice() else {
            panic!("expected one admission: {inspection:?}")
        };
        assert_eq!(
            metric.tags,
            [
                "outcome:denied".to_owned(),
                "reason:first_byte_timeout".to_owned(),
                format!("tls_interception:{}", mode.as_str()),
                "stage:destination_policy".to_owned(),
                "path:unknown".to_owned(),
                "policy_reason:default_deny".to_owned()
            ]
        );

        let mut buf = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), guest.read(&mut buf))
            .await
            .expect("the refusal must close the guest connection");
        assert_eq!(
            read.map_err(|error| error.kind()),
            Err(io::ErrorKind::ConnectionReset),
            "a refusal must reset, not end, the guest connection"
        );
    }
}

#[tokio::test]
async fn readiness_refusals_do_not_claim_a_destination_verdict_or_disabled_inspection() {
    let mut on = NetworkPolicyV1::allow_all();
    on.tls_interception = pplx_space_net_common::TlsInterception::On;
    let (gated, _) = ready_policy(on).await;
    let gated = gated.with_traffic_gate(Arc::new(|| false));
    for (state, mode, reason) in [
        (GatewayNetworkState::no_identity(), "unknown", "no_identity"),
        (gated, "on", "interception_not_ready"),
    ] {
        let (mut proxy, _) = dns_attributed_pypi_proxy().await;
        proxy.network_state = state;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (guest, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let guest = guest.unwrap();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let (_, outcome) = proxy
            .handle(
                accepted.unwrap().0,
                "203.0.113.10:443".parse().unwrap(),
                &CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            outcome,
            FlowOutcome::Rejected(Rejection::PolicyMissing(_))
        ));
        drop(guest);
        let counters = capture.finish();
        let inspection: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name.starts_with("gateway.inspection."))
            .collect();
        let [metric] = inspection.as_slice() else {
            panic!("expected one admission: {inspection:?}")
        };
        assert_eq!(
            metric.tags,
            [
                "outcome:error".to_owned(),
                format!("reason:{reason}"),
                format!("tls_interception:{mode}"),
                "stage:policy_readiness".to_owned(),
                "path:unknown".to_owned(),
                "policy_reason:not_evaluated".to_owned()
            ]
        );
    }
}

#[tokio::test]
async fn lazy_plaintext_connect_failure_returns_502_and_terminal_connect_outcome() {
    let failure = failed_plaintext_connect().await;

    assert!(
        failure.response.starts_with(b"HTTP/1.1 502"),
        "guest response: {}",
        String::from_utf8_lossy(&failure.response),
    );
    assert_eq!(
        failure.outcome.metric_tags(),
        ("relay_failed", "connect"),
        "lazy upstream setup failure must remain a terminal connect failure",
    );
}

#[tokio::test]
async fn lazy_plaintext_connect_failure_log_omits_raw_destination() {
    const DESTINATION_IP: &str = "93.184.216.34";
    let failure = failed_plaintext_connect().await;

    assert_eq!(field_value(&failure.event, "destination"), None);
    assert_eq!(field_value(&failure.event, "dst_port"), Some("80"));
    assert_eq!(field_value(&failure.event, "address_family"), Some("ipv4"));
    assert!(
        failure
            .event
            .iter()
            .all(|(_, value)| !value.contains(DESTINATION_IP)),
        "raw destination leaked in captured event: {:?}",
        failure.event,
    );
    assert_eq!(field_value(&failure.event, "sandbox_id"), Some("test"));
}

#[tokio::test]
async fn failed_policy_refresh_cancels_established_tcp_connections() {
    let dns_allow_cache = DnsAllowCache::default();
    let dns_attributed_connections = DnsAttributedTcpTracker::default();
    let install_hook = {
        let cache = dns_allow_cache.clone();
        let dns_attributed_connections = dns_attributed_connections.clone();
        Arc::new(move |policy: &NetworkPolicyV1| {
            cache.retain_relevant_to_policy(policy);
            dns_attributed_connections.cancel_denied(policy, &cache);
        })
    };
    let policy_invalidation_hook = {
        let dns_attributed_connections = dns_attributed_connections.clone();
        Arc::new(move |invalidation| {
            if invalidation == crate::state::PolicyInvalidation::InstalledPolicy {
                dns_attributed_connections.cancel_all();
            }
        })
    };
    let state = GatewayNetworkState::pending_policy()
        .with_install_hook(install_hook)
        .with_policy_invalidation_hook(policy_invalidation_hook);
    install_policy(
        &state,
        NetworkPolicyV1::allow_all(),
        "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .await;

    // `TcpProxy::serve` watches each tracked token for the full relay lifetime.
    // Leaving one uncancelled leaves that established tunnel open.
    let dns_attributed_shutdown = CancellationToken::new();
    let _dns_attributed_connection = dns_attributed_connections.register(
        "192.0.2.44:443".parse().unwrap(),
        ClientHelloScan::NoServerName,
        &NetworkPolicyV1::allow_all(),
        &DnsAllowCache::default(),
        dns_attributed_shutdown.clone(),
    );
    let failed_loader: crate::state::PolicyLoader =
        Arc::new(|| Box::pin(async { Err("forced policy refresh failure".to_string()) }));
    let expected_policy_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "tcp-enforcement-test",
        "3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap();

    let snapshot = state
        .refresh(
            failed_loader,
            Some(expected_policy_ref),
            crate::state::NetworkStateReason::ControlRefresh,
        )
        .await;

    assert_eq!(snapshot.mode, crate::proto::NetworkState::PolicyError);
    assert!(snapshot.policy.is_none());
    assert!(matches!(
        state.wait_for_tcp_policy().await,
        TrafficPolicy::NotReady(PolicyNotReady::PolicyError)
    ));
    assert!(
        dns_attributed_shutdown.is_cancelled(),
        "failed refresh left the established TCP flow uncancelled",
    );
}

#[tokio::test]
async fn tcp_admission_returns_the_authorized_route_or_deny() {
    const ALLOWED_HOST: &str = "pypi.org";
    const OTHER_ALLOWED_HOST: &str = "files.pythonhosted.org";
    let destination: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let mut domain_policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain(ALLOWED_HOST.parse().unwrap())],
            Vec::new(),
        )
        .unwrap(),
    };
    domain_policy.tls_interception = pplx_space_net_common::TlsInterception::On;
    let mut domain_policy_off = domain_policy.clone();
    domain_policy_off.tls_interception = pplx_space_net_common::TlsInterception::Off;
    let domain_deny_policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![
                EgressDestinationV1::Domain(ALLOWED_HOST.parse().unwrap()),
                EgressDestinationV1::Domain(OTHER_ALLOWED_HOST.parse().unwrap()),
            ],
            vec![EgressDestinationV1::Domain(ALLOWED_HOST.parse().unwrap())],
        )
        .unwrap(),
    };
    let ip_allow_policy = NetworkPolicyV1 {
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
    };
    let ip_deny_policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Allow,
            vec![EgressDestinationV1::Ip(destination.ip())],
            vec![EgressDestinationV1::Ip(destination.ip())],
        )
        .unwrap(),
    };
    let default_deny_policy = NetworkPolicyV1::deny_all();
    let cache = DnsAllowCache::default();
    cache.observe_response(
        &dns_a_response(ALLOWED_HOST, [93, 184, 216, 34], 60),
        &domain_policy,
    );
    assert!(
        !cache.allows_ip(destination.ip(), &domain_deny_policy),
        "a denied hostname must not contribute DNS-derived permission",
    );
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state: GatewayNetworkState::pending_policy(),
        dns_allow_cache: cache,
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };

    struct Case<'a> {
        name: &'static str,
        policy: &'a NetworkPolicyV1,
        scan: ClientHelloScan,
        expected: Result<AdmittedTcp, TcpAdmissionDenial>,
    }

    let cases = [
        Case {
            name: "DNS-derived permission with matching SNI",
            policy: &domain_policy,
            scan: ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            expected: Ok(AdmittedTcp::Tunnel(destination)),
        },
        Case {
            name: "DNS-derived TLS permission without interception",
            policy: &domain_policy_off,
            scan: ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            expected: Ok(AdmittedTcp::Tunnel(destination)),
        },
        Case {
            name: "DNS-derived permission with co-tenant SNI",
            policy: &domain_policy,
            scan: ClientHelloScan::ServerName("fiddle.fastly.dev".parse().unwrap()),
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "DNS-derived permission with co-tenant SNI without interception",
            policy: &domain_policy_off,
            scan: ClientHelloScan::ServerName("fiddle.fastly.dev".parse().unwrap()),
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "DNS-derived permission with ClientHello missing SNI without interception",
            policy: &domain_policy_off,
            scan: ClientHelloScan::NoServerName,
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "explicit domain deny overlapping a cached domain permission",
            policy: &domain_deny_policy,
            scan: ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            expected: Err(dns_attribution_denied(DenyReason::Rule)),
        },
        Case {
            name: "DNS-derived permission with plaintext protocol",
            policy: &domain_policy,
            scan: ClientHelloScan::NotTls,
            expected: Ok(AdmittedTcp::HostBoundHttp(destination)),
        },
        Case {
            name: "DNS-derived permission with incomplete ClientHello",
            policy: &domain_policy,
            scan: ClientHelloScan::Incomplete,
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "DNS-derived permission with ClientHello missing SNI",
            policy: &domain_policy,
            scan: ClientHelloScan::NoServerName,
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "DNS-derived permission with malformed ClientHello",
            policy: &domain_policy,
            scan: ClientHelloScan::Malformed,
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
        Case {
            name: "explicit IP permission without a complete ClientHello",
            policy: &ip_allow_policy,
            scan: ClientHelloScan::Incomplete,
            expected: Ok(AdmittedTcp::Tunnel(destination)),
        },
        Case {
            name: "explicit IP deny overlapping an IP permission",
            policy: &ip_deny_policy,
            scan: ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            expected: Err(dns_attribution_denied(DenyReason::Rule)),
        },
        Case {
            name: "default-denied unattributed plaintext",
            policy: &default_deny_policy,
            scan: ClientHelloScan::NotTls,
            expected: Err(dns_attribution_denied(DenyReason::Default)),
        },
    ];

    for case in cases {
        assert_eq!(
            proxy.admit(destination, &case.scan, case.policy),
            case.expected,
            "{}",
            case.name,
        );
    }
    proxy.dns_allow_cache.observe_response(
        &dns_a_response(OTHER_ALLOWED_HOST, [93, 184, 216, 34], 60),
        &domain_deny_policy,
    );
    assert!(
        proxy
            .dns_allow_cache
            .allows_ip(destination.ip(), &domain_deny_policy),
        "the other allowed hostname must keep the shared IP eligible",
    );
    assert_eq!(
        proxy.admit(
            destination,
            &ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            &domain_deny_policy,
        ),
        Err(dns_attribution_denied(DenyReason::Rule)),
        "the explicit SNI deny must still win on a shared IP",
    );
    let uncached: SocketAddr = "192.0.2.44:443".parse().unwrap();
    assert_eq!(
        proxy.admit(
            uncached,
            &ClientHelloScan::ServerName(ALLOWED_HOST.parse().unwrap()),
            &domain_policy
        ),
        Err(dns_attribution_denied(DenyReason::Default)),
        "an allowed SNI cannot authorize an uncached destination",
    );
    for scan in [
        ClientHelloScan::NotTls,
        ClientHelloScan::Incomplete,
        ClientHelloScan::NoServerName,
    ] {
        assert_eq!(
            proxy.admit(uncached, &scan, &NetworkPolicyV1::allow_all()),
            Ok(AdmittedTcp::Tunnel(uncached)),
            "independent default permission preserves raw and server-first protocols",
        );
    }
}

#[tokio::test]
async fn tcp_handoff_allows_current_and_rejects_replaced_policy() {
    let (network_state, current_policy) = ready_policy(NetworkPolicyV1::allow_all()).await;
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        dns_allow_cache: DnsAllowCache::default(),
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    let destination = "192.0.2.44:443".parse().unwrap();
    let admitted = proxy
        .admit(destination, &ClientHelloScan::NotTls, &current_policy)
        .unwrap();
    let connection = proxy.dns_attributed_connections.register(
        admitted.destination(),
        ClientHelloScan::NotTls,
        &current_policy,
        &proxy.dns_allow_cache,
        CancellationToken::new(),
    );
    assert_eq!(proxy.validate_admission_policy(&current_policy), Ok(()));
    drop(connection);

    install_policy(
        &proxy.network_state,
        NetworkPolicyV1::deny_all(),
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .await;
    let _connection = proxy.dns_attributed_connections.register(
        admitted.destination(),
        ClientHelloScan::NotTls,
        &current_policy,
        &proxy.dns_allow_cache,
        CancellationToken::new(),
    );
    assert_eq!(
        proxy.validate_admission_policy(&current_policy),
        Err(TcpAdmissionDenial::PolicyChanged)
    );
}

#[tokio::test]
async fn record_emits_one_connection_metric_and_reports_every_failure_outcome() {
    use std::collections::HashSet;
    use tokio::io::AsyncWriteExt as _;

    let host: DomainName = "known.example.com".parse().unwrap();
    let proxy = TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state: GatewayNetworkState::no_identity(),
        dns_allow_cache: DnsAllowCache::default(),
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::new(vec![host.clone()]),
        debug_state: GatewayDebugState::default(),
        shutdown_token: CancellationToken::new(),
        refusal_warnings: Arc::default(),
    };
    let destination = "93.184.216.34:443".parse().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (mut client, (server, _)) = (client.unwrap(), accepted.unwrap());
    let hello = crate::tls_sni::test_fixtures::client_hello(Some(host.as_str()));
    client.write_all(&hello).await.unwrap();
    let (observed_host, policy_missing) = proxy
        .handle(server, destination, &CancellationToken::new())
        .await;
    let observed_host = observed_host.expect("known SNI should survive the policy failure");
    assert_eq!(observed_host, host);

    let outcomes = [
        FlowOutcome::Cancelled,
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::ReadyWithoutPolicy)),
        FlowOutcome::Rejected(Rejection::PolicyMissing(
            PolicyNotReady::PendingPolicyTimeout,
        )),
        policy_missing,
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::PolicyError)),
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::Draining)),
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::Stopped)),
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::Unspecified)),
        FlowOutcome::Rejected(Rejection::PolicyMissing(PolicyNotReady::PendingPolicy)),
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::DnsAttribution(
            DenyReason::PlatformGuardrail {
                guardrail_id: "test",
            },
        ))),
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::DnsAttribution(
            DenyReason::Rule,
        ))),
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::DnsAttribution(
            DenyReason::Default,
        ))),
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::PolicyChanged)),
        FlowOutcome::RelayFailed(RelayFailure::Connect(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "test connect failure",
        ))),
        FlowOutcome::RelayFailed(RelayFailure::PlaintextUpstreamHttp),
        FlowOutcome::RelayFailed(RelayFailure::PlaintextGuestHttp),
        FlowOutcome::Rejected(Rejection::TlsInterception(TlsInterceptionError::Timeout)),
        FlowOutcome::RelayFailed(RelayFailure::Transfer),
        FlowOutcome::Relayed(Relay::Tunnelled {
            wire_tx: 1,
            wire_rx: 2,
        }),
        FlowOutcome::Relayed(Relay::HostBoundHttp {
            requests: 1,
            forwarded: 1,
            refused: 0,
            upstream_failed: 0,
            wire_tx: 1,
            wire_rx: 2,
        }),
    ];
    let outcome_count = outcomes.len();
    let mut emitted_tag_sets = HashSet::with_capacity(outcome_count);

    for (case, outcome) in outcomes.into_iter().enumerate() {
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let failure = matches!(
            &outcome,
            FlowOutcome::Rejected(_) | FlowOutcome::RelayFailed(_)
        );
        let reported = proxy.record(destination, Some(&observed_host), outcome);
        assert_eq!(reported, failure);
        let connection_metrics = capture
            .finish()
            .into_iter()
            .filter(|metric| metric.name == "gateway.tcp.connection")
            .collect::<Vec<_>>();

        assert_eq!(connection_metrics.len(), 1, "{case}");
        assert_eq!(connection_metrics[0].value, 1, "{case}");
        assert_eq!(connection_metrics[0].tags.len(), 2, "{case}");
        let mut tags = connection_metrics[0].tags.clone();
        tags.sort_unstable();
        assert!(
            emitted_tag_sets.insert(tags),
            "case {case} reused another terminal outcome's tags"
        );
    }
    assert_eq!(emitted_tag_sets.len(), outcome_count);
}

/// Full-path devbox validation: stand up the real [`run`] proxy loop with a
/// genuine `Ready`/`allow_all` policy, drive a **real** `openssl` ClientHello
/// through it, and assert the gateway's own tracing output shows the SNI
/// extracted by the debug scan log. Exercises the production sniff →
/// egress-evaluation → observability path end to end.
///
/// A direct test connection cannot carry a TPROXY original destination distinct
/// from the proxy listener. The listener therefore binds loopback: sniffing
/// happens first, then the platform guardrail rejects the flow before the proxy
/// can relay back into itself. `#[ignore]` because it shells out to `openssl`;
/// run with `--ignored`.
#[tokio::test]
#[ignore = "devbox validation: full run() loop + real openssl client"]
async fn devbox_full_proxy_run_extracts_sni_from_real_client() {
    use std::io::{Read, Write};
    use std::sync::Mutex as StdMutex;

    #[derive(Clone)]
    struct SharedWriter(Arc<StdMutex<Vec<u8>>>);
    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Ok(mut buffer) = self.0.lock() {
                buffer.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> SharedWriter {
            self.clone()
        }
    }

    let logs = Arc::new(StdMutex::new(Vec::<u8>::new()));
    // Spawned connection tasks log to the global default subscriber.
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(SharedWriter(logs.clone()))
            .finish(),
    )
    .expect("install tracing subscriber for devbox validation");

    // Drive the real network state to Ready with an allow-all policy via the
    // production refresh path.
    let policy_ref = pplx_space_net_common::EffectiveNetworkPolicyRef::local(
        pplx_space_net_common::EffectiveNetworkPolicyRefScope::Sandbox,
        "devbox-validation",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .unwrap()
    .to_string();
    let state = GatewayNetworkState::pending_policy();
    let loader: crate::state::PolicyLoader = {
        let policy_ref = policy_ref.clone();
        Arc::new(move || {
            let policy_ref = policy_ref.clone();
            Box::pin(async move {
                Ok(crate::state::PulledNetworkPolicy {
                    policy: v1_allow_all(),
                    policy_ref,
                    network_credentials: Default::default(),
                })
            })
        })
    };
    let snapshot = state
        .refresh(loader, None, crate::state::NetworkStateReason::Startup)
        .await;
    assert_eq!(snapshot.mode, crate::proto::NetworkState::Ready);

    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    // Open an operator debug window so the per-flow SNI scan is elevated to an
    // `info!` dump (clamped to the gateway max by `set_deadline`).
    let debug_state = GatewayDebugState::default();
    debug_state.set_deadline(u64::MAX);
    let shutdown = CancellationToken::new();
    let proxy = tokio::spawn(run(TcpProxyConfig {
        sandbox_id: "devbox-validation",
        listener,
        stats: Arc::new(ProxyStats::new()),
        network_state: state,
        dns_allow_cache: DnsAllowCache::default(),
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: crate::reserved_dns::test_support::test_core(),
        reserved_dns_cancellation: crate::reserved_dns::ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: debug_state.clone(),
        shutdown_token: shutdown.clone(),
    }));

    let connect_addr = addr.to_string();
    let open_tls = |sni: &str| {
        std::process::Command::new("openssl")
            // Keep the client alive after its null stdin reaches EOF. Otherwise
            // OpenSSL can exit while its ClientHello is still arriving.
            .args([
                "s_client",
                "-ign_eof",
                "-connect",
                &connect_addr,
                "-servername",
                sni,
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("openssl must be installed for the devbox validation")
    };
    let snapshot_logs = || String::from_utf8_lossy(&logs.lock().unwrap()).into_owned();

    // The live gateway extracts the SNI from a real ClientHello.
    let mut client_on = open_tls("on.devbox.example");
    let captured = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let text = snapshot_logs();
            if text.contains("on.devbox.example") {
                return text;
            }
            if let Some(status) = client_on.try_wait().expect("poll openssl client") {
                let mut stderr = String::new();
                client_on
                    .stderr
                    .take()
                    .expect("openssl stderr pipe")
                    .read_to_string(&mut stderr)
                    .expect("read openssl stderr");
                panic!(
                    "openssl exited before the gateway extracted SNI: {status}; stderr: {stderr}; \
                     captured logs: {text}"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let captured = match captured {
        Ok(captured) => captured,
        Err(_) => {
            let _ = client_on.kill();
            let _ = client_on.wait();
            panic!(
                "gateway should log the extracted SNI within 10s; captured logs: {}",
                snapshot_logs()
            );
        }
    };
    let _ = client_on.kill();
    let _ = client_on.wait();
    assert!(
        captured.contains("extracted TLS SNI from ClientHello"),
        "captured logs: {captured}"
    );
    // With the debug window active, the same scan is elevated to an info dump.
    assert!(
        captured.contains("net-gateway debug: scanned TCP flow for TLS SNI"),
        "debug window active: the SNI scan must be elevated to an info dump; logs: {captured}"
    );
    if let Some(line) = captured
        .lines()
        .find(|line| line.contains("net-gateway debug: scanned TCP flow for TLS SNI"))
    {
        eprintln!(
            "devbox validation (debug dump) — gateway emitted: {}",
            line.trim()
        );
    }

    shutdown.cancel();
    let _ = proxy.await;
}

/// A flooding cause must not spend another cause's first warning, and the
/// surviving line must account for what it stands in for.
#[test]
fn refusal_warning_budget_is_per_cause_and_reports_what_it_suppressed() {
    let budget = RefusalWarnings::default();
    let start = Instant::now();
    let flood = ("certificate_error", Some("unavailable"));
    let rare = ("missing_sni", Some("sni_not_found"));

    assert_eq!(budget.sample_at(flood.0, flood.1, start), (true, 0));
    for _ in 0..50 {
        assert_eq!(budget.sample_at(flood.0, flood.1, start), (false, 0));
    }

    // The rare cause has never been logged, so the flood must not silence it.
    assert_eq!(budget.sample_at(rare.0, rare.1, start), (true, 0));

    let later = start + REFUSAL_WARNING_INTERVAL;
    assert_eq!(
        budget.sample_at(flood.0, flood.1, later),
        (true, 50),
        "the next warning must report the refusals it stands for"
    );
    assert_eq!(
        budget.sample_at(flood.0, flood.1, later),
        (false, 0),
        "the suppressed count must not be reported twice"
    );
}

/// The same outcome with different bounded detail is a different cause: a CA
/// outage must not hide a concurrent capacity refusal.
#[test]
fn refusal_warning_budget_separates_details_within_one_outcome() {
    let budget = RefusalWarnings::default();
    let start = Instant::now();
    assert_eq!(
        budget.sample_at("certificate_error", Some("unavailable"), start),
        (true, 0)
    );
    assert_eq!(
        budget.sample_at("certificate_error", Some("resource_exhausted"), start),
        (true, 0)
    );
    assert_eq!(
        budget.sample_at("certificate_error", Some("unavailable"), start),
        (false, 0)
    );
}
