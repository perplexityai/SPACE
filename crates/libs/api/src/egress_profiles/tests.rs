use super::*;

#[rstest::rstest]
#[case(false)]
#[case(true)]
fn composition_preserves_policy_and_deny_precedence(#[case] exemptions_enabled: bool) {
    let destinations = serde_json::json!([
        {"ip":"192.0.2.10"}, {"ip":"192.0.2.11"},
        {"cidr":"203.0.113.0/25"}, {"cidr":"2001:db8:1234::/56"},
        {"ip":"169.254.169.254"}, {"domain":"denied.example.test"},
        {"domain":"sensor.example.test"}
    ]);
    let egress = EgressProfiles::from_config(
        &serde_json::json!({
            "sensor": {"allow": destinations, "tls_interception_exemptions":
                if exemptions_enabled { destinations.clone() } else { serde_json::json!([]) }}
        })
        .to_string(),
    )
    .unwrap();
    egress.compose_override(None, None).unwrap_err();
    for default_action in [EgressDefaultAction::Deny, EgressDefaultAction::Allow] {
        let base = NetworkPolicyV1 {
            version: Default::default(),
            tls_interception: pplx_space_net_common::TlsInterception::On,
            tls_interception_exemptions: Vec::new(),
            network_credential_rules: Vec::new(),
            egress: pplx_space_net_common::EgressPolicyV1::new(
                default_action,
                vec![EgressDestinationV1::Domain("user.example".parse().unwrap())],
                vec![
                    EgressDestinationV1::Ip("192.0.2.10".parse().unwrap()),
                    EgressDestinationV1::Ip("203.0.113.10".parse().unwrap()),
                    EgressDestinationV1::Ip("2001:db8:1234::10".parse().unwrap()),
                    EgressDestinationV1::Domain("denied.example.test".parse().unwrap()),
                ],
            )
            .unwrap(),
        };

        let composed = egress.compose(&base);
        assert_eq!(
            serde_json::to_value(&composed)
                .unwrap()
                .get("tls_interception_exemptions")
                .is_some(),
            exemptions_enabled,
            "disabled composition must preserve the legacy serialized policy shape"
        );
        let expected_interception = if exemptions_enabled {
            pplx_space_net_common::TlsInterception::Off
        } else {
            pplx_space_net_common::TlsInterception::On
        };

        for (allowed, denied, outside) in [
            ("203.0.113.11", "203.0.113.10", "203.0.113.128"),
            (
                "2001:db8:1234::11",
                "2001:db8:1234::10",
                "2001:db8:1234:100::1",
            ),
        ] {
            let allowed = allowed.parse().unwrap();
            assert!(composed.egress_decision_for_ip(allowed).is_allowed());
            assert!(
                !composed
                    .egress_decision_for_ip(denied.parse().unwrap())
                    .is_allowed()
            );
            assert_eq!(
                composed.tls_interception_for(std::net::SocketAddr::new(allowed, 443), None),
                expected_interception
            );
            let outside = outside.parse().unwrap();
            assert_eq!(
                composed.egress_decision_for_ip(outside).is_allowed(),
                default_action == EgressDefaultAction::Allow
            );
            assert_eq!(
                composed.tls_interception_for(std::net::SocketAddr::new(outside, 443), None),
                pplx_space_net_common::TlsInterception::On
            );
        }
        assert!(composed.allows_domain("user.example"));
        assert!(composed.allows_domain("sensor.example.test"));
        assert!(
            composed
                .egress_decision_for_ip("192.0.2.11".parse().unwrap())
                .is_allowed()
        );
        assert!(
            !composed
                .egress_decision_for_ip("192.0.2.10".parse().unwrap())
                .is_allowed()
        );
        assert!(
            !composed
                .egress_decision_for_ip("169.254.169.254".parse().unwrap())
                .is_allowed()
        );
        assert!(!composed.allows_domain("denied.example.test"));
        assert_eq!(composed.tls_interception, base.tls_interception);
        assert_eq!(
            composed.tls_interception_for("192.0.2.11:443".parse().unwrap(), None),
            expected_interception
        );
        assert_eq!(
            composed.tls_interception_for(
                "198.51.100.10:443".parse().unwrap(),
                Some(&"sensor.example.test".parse().unwrap())
            ),
            expected_interception
        );
        assert_eq!(
            composed.tls_interception_for(
                "198.51.100.10:443".parse().unwrap(),
                Some(&"user.example".parse().unwrap())
            ),
            pplx_space_net_common::TlsInterception::On
        );
        assert_eq!(
            composed.tls_interception_for("8.8.8.8:443".parse().unwrap(), None),
            pplx_space_net_common::TlsInterception::On
        );
        assert_eq!(composed.default_action(), default_action);
        assert_eq!(
            composed
                .egress_decision_for_ip("198.51.100.10".parse().unwrap())
                .is_allowed(),
            default_action == EgressDefaultAction::Allow
        );
        assert!(["8.8.8.8", "1.1.1.1"].iter().all(|resolver| {
            composed
                .egress_decision_for_ip(resolver.parse().unwrap())
                .is_allowed()
        }));
        assert_eq!(egress.compose(&composed), composed);
        let disabled = EgressProfiles::from_config(
            r#"{"sensor":{"allow":[{"domain":"sensor.example.test"}]}}"#,
        )
        .unwrap();
        assert_eq!(disabled.compose(&composed), composed);
    }
}

#[rstest::rstest]
#[case("")]
#[case("[]")]
#[case("null")]
#[case(r#"{"sensor":{"allow":[]}}"#)]
#[case(r#"{"Sensor":{"allow":[{"ip":"192.0.2.1"}]}}"#)]
#[case(r#"{"":{"allow":[{"ip":"192.0.2.1"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"ip":"not-an-ip"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"domain":"not a domain"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"unknown":"example.test"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"wildcard_domain":"*.example.test"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"cidr":"192.0.2.0/33"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"cidr":"2001:db8::/129"}]}}"#)]
#[case(r#"{"sensor":{"allow":[{"ip":"192.0.2.1"}]},"sensor":{"allow":[{"ip":"192.0.2.2"}]}}"#)]
fn rejects_invalid_or_unsupported_configuration(#[case] raw: &str) {
    EgressProfiles::from_config(raw).unwrap_err();
}

#[test]
fn selection_preserves_defaults_and_rejects_invalid_requests() {
    let configured = EgressProfiles::from_config(
        r#"{
        "baseline":{"default":true,"allow":[{"ip":"192.0.2.1"}]},
        "sensor":{"allow":[{"domain":"sensor.example.test"}]}
    }"#,
    )
    .unwrap();
    let names = |profiles: EgressProfileSelection| {
        profiles.names.into_iter().map(|n| n.0).collect::<Vec<_>>()
    };
    assert_eq!(names(configured.select(None, true).unwrap()), ["baseline"]);
    assert_eq!(
        names(configured.select(Some("+sensor -baseline"), true).unwrap()),
        ["sensor"]
    );
    assert_eq!(
        names(configured.select(Some("+sensor"), true).unwrap()),
        ["baseline", "sensor"]
    );
    assert!(names(configured.select(Some("+sensor"), false).unwrap()).is_empty());
    assert!(names(configured.select(None, false).unwrap()).is_empty());
    assert!(names(EgressProfiles::default().select(None, true).unwrap()).is_empty());
    for header in [
        "",
        "sensor",
        "+",
        "+Sensor",
        "+sensor +sensor",
        "+sensor -sensor",
        "+missing",
        "-missing",
        "+sensor\t-baseline",
    ] {
        assert!(configured.select(Some(header), true).is_err(), "{header}");
    }
}
