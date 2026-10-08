use super::*;

#[rstest::rstest]
#[case(None, false)]
#[case(Some("false"), false)]
#[case(Some("true"), true)]
fn composition_preserves_policy_and_deny_precedence(
    #[case] activation: Option<&str>,
    #[case] exemptions_enabled: bool,
) {
    let egress = FalconUs2Egress::from_config(
        r#"[
            {"ip":"192.0.2.10"},
            {"ip":"192.0.2.11"},
            {"cidr":"203.0.113.0/25"},
            {"cidr":"2001:db8:1234::/56"},
            {"ip":"169.254.169.254"},
            {"domain":"denied.example.test"},
            {"domain":"sensor.example.test"}
        ]"#,
        activation,
    )
    .unwrap();

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
                composed.tls_interception_for(allowed, None),
                expected_interception
            );
            let outside = outside.parse().unwrap();
            assert_eq!(
                composed.egress_decision_for_ip(outside).is_allowed(),
                default_action == EgressDefaultAction::Allow
            );
            assert_eq!(
                composed.tls_interception_for(outside, None),
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
            composed.tls_interception_for("192.0.2.11".parse().unwrap(), None),
            expected_interception
        );
        assert_eq!(
            composed.tls_interception_for(
                "198.51.100.10".parse().unwrap(),
                Some(&"sensor.example.test".parse().unwrap())
            ),
            expected_interception
        );
        assert_eq!(
            composed.tls_interception_for(
                "198.51.100.10".parse().unwrap(),
                Some(&"user.example".parse().unwrap())
            ),
            pplx_space_net_common::TlsInterception::On
        );
        assert_eq!(
            composed.tls_interception_for("8.8.8.8".parse().unwrap(), None),
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
        let disabled =
            FalconUs2Egress::from_config(r#"[{"domain":"sensor.example.test"}]"#, None).unwrap();
        assert_eq!(disabled.compose(&composed), composed);
    }
}

#[rstest::rstest]
#[case("")]
#[case("[]")]
#[case("null")]
#[case(r#"[{"ip":"not-an-ip"}]"#)]
#[case(r#"[{"domain":"not a domain"}]"#)]
#[case(r#"[{"unknown":"example.test"}]"#)]
#[case(r#"[{"wildcard_domain":"*.example.test"}]"#)]
#[case(r#"[{"cidr":"192.0.2.0/33"}]"#)]
#[case(r#"[{"cidr":"2001:db8::/129"}]"#)]
fn rejects_invalid_or_unsupported_configuration(#[case] raw: &str) {
    FalconUs2Egress::from_config(raw, None).unwrap_err();
}

#[test]
fn create_fallback_preserves_the_requested_overlay() {
    let configured =
        FalconUs2Egress::from_config(r#"[{"domain":"sensor.example.test"}]"#, None).unwrap();
    assert!(for_create(false, Some(&configured)).is_none());
    assert!(for_create(true, None).is_none());
    let selected = for_create(true, Some(&configured)).unwrap();
    assert!(std::ptr::eq(selected, &configured));
}

#[test]
fn rejects_invalid_exemption_activation() {
    for activation in ["", "1", "0", "TRUE", " false "] {
        assert!(matches!(
            FalconUs2Egress::from_config(r#"[{"domain":"sensor.example.test"}]"#, Some(activation)),
            Err(FalconEgressConfigError::ExemptionsActivation(_))
        ));
    }
}
