use super::*;
use pplx_space_net_common::{EgressDefaultAction, EgressPolicyV1, TlsInterception};

#[test]
fn unset_is_a_noop() {
    let configured =
        TlsInterceptionExemptions::from_environment(Err(std::env::VarError::NotPresent)).unwrap();
    assert!(configured.is_none());
    let mut policy = NetworkPolicyV1::deny_all();
    policy.tls_interception = TlsInterception::On;
    assert_eq!(compose(configured.as_ref(), None, Some(&policy)), None);
    assert_eq!(
        compose(configured.as_ref(), Some(policy.clone()), None),
        Some(policy)
    );
}

#[rstest::rstest]
#[case(TlsInterception::On, false)]
#[case(TlsInterception::On, true)]
#[case(TlsInterception::Off, false)]
#[case(TlsInterception::Off, true)]
fn composition_only_changes_on_mode_exemptions(
    #[case] interception: TlsInterception,
    #[case] inherited: bool,
) {
    let configured = TlsInterceptionExemptions::from_config(
        r#"[{"domain":"relay.example.test"},{"wildcard_domain":"*.relay.example.test"},
            {"domain":"relay.example.test"}]"#,
    )
    .unwrap();
    let existing: EgressDestinationV1 =
        EgressDestinationV1::Domain("relay.example.test".parse().unwrap());
    let wildcard = EgressDestinationV1::WildcardDomain("*.relay.example.test".parse().unwrap());
    let base = NetworkPolicyV1 {
        tls_interception: interception,
        tls_interception_exemptions: vec![existing.clone().into()],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Domain(
                "allowed.example.test".parse().unwrap(),
            )],
            vec![existing.clone()],
        )
        .unwrap(),
        ..NetworkPolicyV1::deny_all()
    };
    let requested = (!inherited).then(|| base.clone());
    let composed = compose(Some(&configured), requested.clone(), Some(&base));
    if interception.is_off() {
        assert_eq!(composed, requested);
    } else {
        let composed = composed.unwrap();
        let mut expected = base.clone();
        expected.tls_interception_exemptions = vec![existing.into(), wildcard.into()];
        assert_eq!(composed, expected);
        assert_eq!(
            compose(Some(&configured), Some(composed.clone()), Some(&base)),
            Some(composed.clone())
        );
        assert_eq!(compose(Some(&configured), None, Some(&composed)), None);
        assert!(!composed.allows_domain("relay.example.test"));
        assert!(!composed.allows_domain("child.relay.example.test"));
        assert_eq!(
            composed.tls_interception_for("192.0.2.10:443".parse().unwrap(), None),
            TlsInterception::On
        );
        assert_eq!(
            composed.tls_interception_for(
                "192.0.2.10:443".parse().unwrap(),
                Some(&"child.relay.example.test".parse().unwrap()),
            ),
            TlsInterception::Off
        );
    }
}

#[test]
fn override_takes_precedence_over_template() {
    let configured =
        TlsInterceptionExemptions::from_config(r#"[{"wildcard_domain":"*.relay.example.test"}]"#)
            .unwrap();
    let off = NetworkPolicyV1::deny_all();
    let mut on = NetworkPolicyV1::allow_all();
    on.tls_interception = TlsInterception::On;
    assert_eq!(
        compose(Some(&configured), Some(off.clone()), Some(&on)),
        Some(off.clone())
    );
    let composed = compose(Some(&configured), Some(on.clone()), Some(&off)).unwrap();
    on.tls_interception_exemptions = configured.exemptions.clone();
    assert_eq!(composed, on);
    assert_eq!(compose(Some(&configured), None, None), None);
}

#[test]
fn port_scoped_exemptions_compose_alongside_all_port_exemptions() {
    let configured = TlsInterceptionExemptions::from_config(
        r#"[{"domain":"push.example.test","port":5228},{"domain":"relay.example.test"},
            {"domain":"push.example.test","port":5228}]"#,
    )
    .unwrap();
    let push = EgressDestinationV1::Domain("push.example.test".parse().unwrap());
    let mut base = NetworkPolicyV1::allow_all();
    base.tls_interception = TlsInterception::On;
    // An all-port entry for the same host does not satisfy a port-scoped one.
    base.tls_interception_exemptions = vec![push.clone().into()];
    let composed = compose(Some(&configured), None, Some(&base)).unwrap();
    let mut expected = base.clone();
    expected.tls_interception_exemptions = vec![
        push.clone().into(),
        TlsInterceptionExemptionV1::new(push, Some(5228)).unwrap(),
        EgressDestinationV1::Domain("relay.example.test".parse().unwrap()).into(),
    ];
    assert_eq!(composed, expected);
    assert_eq!(compose(Some(&configured), None, Some(&composed)), None);

    base.tls_interception_exemptions.clear();
    let composed = compose(Some(&configured), None, Some(&base)).unwrap();
    let host = "push.example.test".parse().unwrap();
    assert_eq!(
        composed.tls_interception_for("192.0.2.10:5228".parse().unwrap(), Some(&host)),
        TlsInterception::Off
    );
    assert_eq!(
        composed.tls_interception_for("192.0.2.10:443".parse().unwrap(), Some(&host)),
        TlsInterception::On
    );
}

#[rstest::rstest]
#[case("")]
#[case("[]")]
#[case("null")]
#[case("{}")]
#[case(r#"[{"domain":"bad host"}]"#)]
#[case(r#"[{"wildcard_domain":"relay.example.test"}]"#)]
#[case(r#"[{"wildcard_domain":"*.*.example.test"}]"#)]
#[case(r#"[{"ip":"192.0.2.10"}]"#)]
#[case(r#"[{"cidr":"192.0.2.0/24"}]"#)]
#[case(r#"[{"domain":"relay.example.test"},{"ip":"192.0.2.10"}]"#)]
#[case(r#"[{"ip":"192.0.2.10","port":5228}]"#)]
#[case(r#"[{"domain":"relay.example.test","port":0}]"#)]
#[case(r#"[{"domain":"relay.example.test","port":65536}]"#)]
#[case(r#"[{"domain":"relay.example.test","ports":[5228]}]"#)]
fn invalid_config_is_rejected(#[case] raw: &str) {
    TlsInterceptionExemptions::from_environment(Ok(raw.to_owned())).unwrap_err();
}
