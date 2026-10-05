use super::*;
use futures_util::FutureExt as _;
use tokio::io::AsyncWriteExt as _;
use tracing_subscriber::Layer as _;

#[derive(Clone, Copy)]
enum SigningResult {
    Unavailable,
    Pending,
}

async fn inspected_proxy() -> (
    TcpProxy,
    rcgen::Certificate,
    Arc<crate::certificates::CertificateLease>,
) {
    let (mut proxy, _) = dns_attributed_pypi_proxy().await;
    proxy.network_state = GatewayNetworkState::pending_policy();
    let mut policy = NetworkPolicyV1::allow_all();
    policy.tls_interception = pplx_space_net_common::TlsInterception::On;
    let (root, _, provider) = crate::certificates::local_provider_with_policy(
        proxy.network_state.clone(),
        policy,
        Default::default(),
    )
    .await;
    let lease = provider
        .get("allowed.test".parse().unwrap(), 443)
        .await
        .unwrap();
    proxy.tls_interception = TlsInterception::new(Arc::new(move |host, port| {
        let provider = provider.clone();
        Box::pin(async move { provider.get(host, port).await })
    }));
    (proxy, root, lease)
}

async fn guest_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (guest, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (guest.unwrap(), accepted.unwrap().0)
}

#[tokio::test]
async fn admitted_sessions_count_handshake_failure_forwarding_failure_and_completion_separately() {
    use crate::tls_sni::test_fixtures::client_hello;

    enum Guest {
        IncompleteHandshake,
        InvalidHttp,
        GracefulClose,
    }
    for (guest_action, stage, expected_outcome, reason) in [
        (
            Guest::IncompleteHandshake,
            "guest_handshake",
            "error",
            "tls_downstream_error",
        ),
        (
            Guest::InvalidHttp,
            "session_forwarding",
            "error",
            "http_connection_error",
        ),
        (
            Guest::GracefulClose,
            "session_forwarding",
            "success",
            "completed",
        ),
    ] {
        let (proxy, root, _) = inspected_proxy().await;
        let (mut guest, gateway) = guest_pair().await;
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let shutdown = CancellationToken::new();
        let (handled, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                proxy.handle(gateway, "203.0.113.10:443".parse().unwrap(), &shutdown),
                async {
                    if matches!(guest_action, Guest::IncompleteHandshake) {
                        guest
                            .write_all(&client_hello(Some("allowed.test")))
                            .await
                            .unwrap();
                        guest.shutdown().await.unwrap();
                        return;
                    }
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
                    let mut guest = tokio_rustls::TlsConnector::from(Arc::new(config))
                        .connect("allowed.test".try_into().unwrap(), guest)
                        .await
                        .unwrap();
                    if matches!(guest_action, Guest::InvalidHttp) {
                        guest.write_all(b"invalid HTTP\r\n\r\n").await.unwrap();
                    }
                    guest.shutdown().await.unwrap();
                }
            )
        })
        .await
        .expect("local TLS session must terminate");
        assert_eq!(
            matches!(handled.1, FlowOutcome::InspectedTls),
            expected_outcome == "success"
        );
        let counters = capture.finish();
        let inspection: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name.starts_with("gateway.inspection."))
            .collect();
        assert_eq!(inspection.len(), 2, "{inspection:?}");
        assert_eq!(inspection[0].name, "gateway.inspection.admission");
        assert_eq!(
            inspection[0].tags,
            [
                "outcome:success",
                "reason:admitted",
                "tls_interception:on",
                "stage:handoff",
                "path:tls_inspection",
                "policy_reason:allowed"
            ]
        );
        assert_eq!(inspection[1].name, "gateway.inspection.session");
        assert_eq!(inspection[1].value, 1);
        assert_eq!(
            inspection[1].tags,
            [
                format!("outcome:{expected_outcome}"),
                format!("reason:{reason}"),
                "tls_interception:on".to_owned(),
                format!("stage:{stage}"),
                "path:tls_inspection".to_owned(),
                "policy_reason:allowed".to_owned()
            ]
        );
    }
}

#[tokio::test]
async fn policy_change_at_handoff_keeps_the_admitting_generation_and_never_starts_a_session() {
    use crate::tls_sni::test_fixtures::client_hello;

    let (mut proxy, _, lease) = inspected_proxy().await;
    let state = proxy.network_state.clone();
    proxy.tls_interception = TlsInterception::new(Arc::new(move |_, _| {
        let state = state.clone();
        let lease = lease.clone();
        Box::pin(async move {
            install_policy(
                &state,
                NetworkPolicyV1::allow_all(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )
            .await;
            Ok(lease)
        })
    }));
    let (mut guest, gateway) = guest_pair().await;
    guest
        .write_all(&client_hello(Some("allowed.test")))
        .await
        .unwrap();
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let (_, outcome) = proxy
        .handle(
            gateway,
            "203.0.113.10:443".parse().unwrap(),
            &CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        outcome,
        FlowOutcome::Rejected(Rejection::Admission(TcpAdmissionDenial::PolicyChanged))
    ));
    let counters = capture.finish();
    let inspection: Vec<_> = counters
        .iter()
        .filter(|counter| counter.name.starts_with("gateway.inspection."))
        .collect();
    let [metric] = inspection.as_slice() else {
        panic!("expected only one admission: {inspection:?}")
    };
    assert_eq!(
        metric.tags,
        [
            "outcome:cancelled",
            "reason:policy_changed",
            "tls_interception:on",
            "stage:handoff",
            "path:tls_inspection",
            "policy_reason:allowed"
        ]
    );
}

#[tokio::test]
async fn interception_wiring_preserves_admission_and_never_falls_back_to_relay() {
    use crate::tls_sni::test_fixtures::client_hello;

    // 0.0.0.0 reaches this local listener without using the policy's forbidden
    // 127/8 range. No request in this test reaches an external destination.
    let upstream = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let dst = upstream.local_addr().unwrap();
    let tls = client_hello(Some("good.com"));
    let no_sni = client_hello(None);
    let malformed = client_hello(Some("not a domain"));
    // name, policy allows, bytes, signing result, requests
    for (name, allowed, bytes, signing_result, requests, expected_reason) in [
        (
            "signing error",
            true,
            &tls,
            SigningResult::Unavailable,
            1,
            "certificate_unavailable",
        ),
        (
            "signing timeout",
            true,
            &tls,
            SigningResult::Pending,
            1,
            "certificate_timeout",
        ),
        (
            "policy denial",
            false,
            &tls,
            SigningResult::Unavailable,
            0,
            "default_deny",
        ),
        (
            "no SNI",
            true,
            &no_sni,
            SigningResult::Unavailable,
            0,
            "sni_not_found",
        ),
        (
            "malformed SNI",
            true,
            &malformed,
            SigningResult::Unavailable,
            0,
            "sni_invalid",
        ),
        (
            "incomplete",
            true,
            &vec![0x16],
            SigningResult::Unavailable,
            0,
            "client_hello_completion_timeout",
        ),
    ] {
        let mut policy = if allowed {
            NetworkPolicyV1::allow_all()
        } else {
            NetworkPolicyV1::deny_all()
        };
        policy.tls_interception = pplx_space_net_common::TlsInterception::On;
        let (network_state, _) = ready_policy(policy).await;
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let tls_interception = TlsInterception::new(Arc::new(move |host, port| {
            let send = send.clone();
            Box::pin(async move {
                send.send((host, port)).await.unwrap();
                match signing_result {
                    SigningResult::Unavailable => Err(tonic::Status::unavailable("test failure")),
                    SigningResult::Pending => std::future::pending().await,
                }
            })
        }));
        let proxy = TcpProxy {
            sandbox_id: "test",
            stats: Arc::new(ProxyStats::new()),
            network_state,
            dns_allow_cache: DnsAllowCache::default(),
            dns_attributed_connections: DnsAttributedTcpTracker::default(),
            tls_interception,
            reserved_dns: crate::reserved_dns::test_support::test_core(),
            reserved_dns_cancellation: ReservedDnsCancellation::default(),
            known_egress_state: KnownEgressState::default(),
            debug_state: GatewayDebugState::default(),
            shutdown_token: CancellationToken::new(),
            refusal_warnings: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (guest, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept(),
        );
        let mut guest = guest.unwrap();
        guest.write_all(bytes).await.unwrap();
        guest.shutdown().await.unwrap();
        let connection = accepted.unwrap().0;
        let shutdown = CancellationToken::new();
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let (host, outcome) = tokio::time::timeout(
            Duration::from_secs(3),
            proxy.handle(connection, dst, &shutdown),
        )
        .await
        .expect(name);
        let counters = capture.finish();
        let admission: Vec<_> = counters
            .iter()
            .filter(|counter| counter.name == "gateway.inspection.admission")
            .collect();
        let [metric] = admission.as_slice() else {
            panic!("{name}: expected exactly one admission: {admission:?}")
        };
        assert_eq!(metric.value, 1, "{name}");
        assert_eq!(
            metric.tags,
            [
                format!(
                    "outcome:{}",
                    if !allowed || requests == 0 {
                        "denied"
                    } else {
                        "error"
                    }
                ),
                format!("reason:{expected_reason}"),
                "tls_interception:on".to_owned(),
                format!(
                    "stage:{}",
                    if allowed {
                        "pre_tls"
                    } else {
                        "destination_policy"
                    }
                ),
                format!(
                    "path:{}",
                    if allowed { "tls_inspection" } else { "unknown" }
                ),
                format!(
                    "policy_reason:{}",
                    if allowed { "allowed" } else { "default_deny" }
                ),
            ],
            "{name}"
        );
        assert!(
            !counters
                .iter()
                .any(|counter| counter.name == "gateway.inspection.session"),
            "{name}: no session was admitted"
        );
        assert!(matches!(outcome, FlowOutcome::Rejected(_)), "{name}");
        assert!(
            upstream.accept().now_or_never().is_none(),
            "{name}: must not dial upstream"
        );
        if requests == 1 {
            assert_eq!(
                receive.try_recv().unwrap(),
                ("good.com".parse().unwrap(), dst.port()),
                "{name}"
            );
        }
        assert!(
            receive.try_recv().is_err(),
            "{name}: unexpected certificate request"
        );
        let intercepted = matches!(
            outcome,
            FlowOutcome::Rejected(Rejection::TlsInterception(_))
        );
        assert_eq!(intercepted, allowed, "{name}: unexpected rejection path");
        if intercepted {
            let layer = CapturedEventsLayer::new().with_event_filter(|event| {
                field_value(event, "message")
                    == Some("experimental TLS interception closed the connection")
            });
            let events = layer.events();
            let warning_layer = CapturedEventsLayer::new().with_event_filter(|event| {
                field_value(event, "message")
                    == Some("experimental TLS interception closed the connection")
            });
            let warning_events = warning_layer.events();
            tracing::subscriber::with_default(
                tracing_subscriber::registry()
                    .with(layer)
                    .with(warning_layer.with_filter(tracing::level_filters::LevelFilter::WARN)),
                || {
                    proxy.record(dst, host.as_ref(), outcome);
                },
            );
            let events = events.events();
            let [event] = events.as_slice() else {
                panic!("{name}: expected one TLS rejection event");
            };
            // Every interception rejection is a fail-closed guest breakage, so
            // all of them must survive a production log level, not only the
            // missing-SNI case.
            assert_eq!(warning_events.events().len(), 1, "{name}");
            assert!(
                event
                    .iter()
                    .all(|(_, value)| !value.contains(&dst.ip().to_string())),
                "{name}: raw destination leaked: {event:?}",
            );
            assert_eq!(
                field_value(event, "destination_port"),
                Some(dst.port().to_string().as_str())
            );
        }
    }
}

#[tokio::test]
async fn interception_exemptions_relay_original_tls_only_after_admission() {
    use crate::tls_sni::test_fixtures::client_hello;
    use tokio::io::AsyncReadExt as _;

    for (name, exempt_ip, host, allowed, denied_host, passes) in [
        (
            "exempt host",
            false,
            Some("sensor.example.test"),
            true,
            false,
            true,
        ),
        ("exempt IP without SNI", true, None, true, false, true),
        (
            "other host intercepted",
            false,
            Some("other.example.test"),
            true,
            false,
            false,
        ),
        (
            "exemption grants no access",
            false,
            Some("sensor.example.test"),
            false,
            false,
            false,
        ),
        (
            "explicit host deny",
            true,
            Some("sensor.example.test"),
            true,
            true,
            false,
        ),
        (
            "DNS-bound host",
            false,
            Some("sensor.example.test"),
            false,
            false,
            true,
        ),
        (
            "unattributed host",
            false,
            Some("sensor.example.test"),
            false,
            false,
            false,
        ),
        (
            "explicit IP deny",
            true,
            Some("sensor.example.test"),
            true,
            false,
            false,
        ),
        (
            "unbound host on open policy",
            false,
            Some("sensor.example.test"),
            true,
            false,
            false,
        ),
        (
            "wrong address on open policy",
            false,
            Some("sensor.example.test"),
            true,
            false,
            false,
        ),
        (
            "wrong host on open policy",
            false,
            Some("sensor.example.test"),
            true,
            false,
            false,
        ),
        (
            "unbound host with IP allow",
            false,
            Some("sensor.example.test"),
            false,
            false,
            false,
        ),
        (
            "bound host with IP allow",
            false,
            Some("sensor.example.test"),
            false,
            false,
            true,
        ),
        (
            "exempt IP with other SNI",
            true,
            Some("other.example.test"),
            true,
            false,
            true,
        ),
        (
            "platform deny with host deny",
            true,
            Some("sensor.example.test"),
            true,
            true,
            false,
        ),
    ] {
        let upstream = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let mut dst = upstream.local_addr().unwrap();
        if name == "platform deny with host deny" {
            dst.set_ip("127.0.0.1".parse().unwrap());
        }
        let mut policy = if allowed {
            NetworkPolicyV1::allow_all()
        } else {
            NetworkPolicyV1::deny_all()
        };
        policy.tls_interception = pplx_space_net_common::TlsInterception::On;
        policy.tls_interception_exemptions.push(if exempt_ip {
            EgressDestinationV1::Ip(dst.ip())
        } else {
            EgressDestinationV1::Domain("sensor.example.test".parse().unwrap())
        });
        if denied_host {
            policy.egress.deny.push(EgressDestinationV1::Domain(
                "sensor.example.test".parse().unwrap(),
            ));
        }
        if matches!(
            name,
            "DNS-bound host"
                | "unattributed host"
                | "unbound host with IP allow"
                | "bound host with IP allow"
        ) {
            policy.egress.allow.push(EgressDestinationV1::Domain(
                "sensor.example.test".parse().unwrap(),
            ));
        }
        if matches!(
            name,
            "unbound host with IP allow" | "bound host with IP allow"
        ) {
            policy.egress.allow.push(EgressDestinationV1::Ip(dst.ip()));
        }
        if name == "explicit IP deny" {
            policy.egress.deny.push(EgressDestinationV1::Ip(dst.ip()));
        }
        if name == "wrong host on open policy" {
            policy.egress.allow.push(EgressDestinationV1::Domain(
                "other.example.test".parse().unwrap(),
            ));
        }
        let (network_state, installed) = ready_policy(policy).await;
        let dns_allow_cache = DnsAllowCache::default();
        if matches!(
            name,
            "exempt host" | "DNS-bound host" | "bound host with IP allow"
        ) {
            assert!(dns_allow_cache.observe_response(
                &dns_a_response("sensor.example.test", [0, 0, 0, 0], 60),
                &installed,
            ));
        }
        if name == "wrong address on open policy" {
            assert!(dns_allow_cache.observe_response(
                &dns_a_response("sensor.example.test", [192, 0, 2, 1], 60),
                &installed,
            ));
        }
        if name == "wrong host on open policy" {
            assert!(dns_allow_cache.observe_response(
                &dns_a_response("other.example.test", [0, 0, 0, 0], 60),
                &installed,
            ));
        }
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recorded = Arc::clone(&calls);
        let proxy = TcpProxy {
            sandbox_id: "test",
            stats: Arc::new(ProxyStats::new()),
            network_state,
            dns_allow_cache,
            dns_attributed_connections: DnsAttributedTcpTracker::default(),
            tls_interception: TlsInterception::new(Arc::new(move |_, _| {
                recorded.fetch_add(1, Ordering::Relaxed);
                Box::pin(async { Err(tonic::Status::unavailable("synthetic signer")) })
            })),
            reserved_dns: crate::reserved_dns::test_support::test_core(),
            reserved_dns_cancellation: ReservedDnsCancellation::default(),
            known_egress_state: KnownEgressState::default(),
            debug_state: GatewayDebugState::default(),
            shutdown_token: CancellationToken::new(),
            refusal_warnings: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (guest, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut guest = guest.unwrap();
        let bytes = client_hello(host);
        guest.write_all(&bytes).await.unwrap();
        guest.shutdown().await.unwrap();
        let shutdown = CancellationToken::new();
        let handled = proxy.handle(accepted.unwrap().0, dst, &shutdown);
        if passes {
            let origin = async {
                let (mut stream, _) = upstream.accept().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert_eq!(received, bytes, "{name}: ClientHello must remain unchanged");
                stream.write_all(b"origin TLS bytes").await.unwrap();
            };
            let ((_, outcome), ()) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::join!(handled, origin)
            })
            .await
            .expect(name);
            assert!(matches!(outcome, FlowOutcome::Relayed(_)), "{name}");
            let mut response = Vec::new();
            guest.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"origin TLS bytes", "{name}");
        } else {
            let (_, outcome) = tokio::time::timeout(Duration::from_secs(3), handled)
                .await
                .expect(name);
            if name == "platform deny with host deny" {
                assert!(
                    matches!(
                        outcome,
                        FlowOutcome::Rejected(Rejection::Admission(
                            TcpAdmissionDenial::DnsAttribution(
                                DenyReason::PlatformGuardrail { .. }
                            )
                        ))
                    ),
                    "{name}"
                );
            } else {
                assert!(matches!(outcome, FlowOutcome::Rejected(_)), "{name}");
            }
            assert!(upstream.accept().now_or_never().is_none(), "{name}");
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(matches!(
                name,
                "other host intercepted"
                    | "unbound host on open policy"
                    | "wrong address on open policy"
                    | "wrong host on open policy"
                    | "unbound host with IP allow"
            )),
            "{name}"
        );
    }
}
