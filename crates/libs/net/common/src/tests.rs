use super::*;

#[test]
fn effective_network_policy_ref_round_trips_cluster() {
    let hash = "a".repeat(64);
    let policy_ref = EffectiveNetworkPolicyRef::cluster(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "sbx-1",
        7,
        &hash,
    )
    .unwrap();
    let serialized = policy_ref.to_string();

    assert_eq!(
        serialized,
        format!("cluster:sandbox:sbx-1:generation:7:sha256:{hash}")
    );
    assert_eq!(
        serialized.parse::<EffectiveNetworkPolicyRef>().unwrap(),
        policy_ref
    );
    assert!(effective_network_policy_refs_match(
        &serialized,
        &serialized
    ));
    assert_eq!(
        effective_network_policy_ref_authority_label(&serialized),
        "cluster"
    );
}

#[test]
fn effective_network_policy_ref_round_trips_local_and_no_identity() {
    let hash = "b".repeat(64);
    let local = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Template,
        "app-runtime",
        &hash,
    )
    .unwrap();
    let serialized = local.to_string();

    assert_eq!(
        serialized,
        format!("local:template:app-runtime:sha256:{hash}")
    );
    assert_eq!(
        serialized.parse::<EffectiveNetworkPolicyRef>().unwrap(),
        local
    );
    assert_eq!(
        "no-identity".parse::<EffectiveNetworkPolicyRef>().unwrap(),
        EffectiveNetworkPolicyRef::no_identity()
    );
    assert_eq!(
        effective_network_policy_ref_authority_label("no-identity"),
        "no_identity"
    );
}

#[test]
fn effective_network_policy_ref_rejects_malformed_values() {
    "".parse::<EffectiveNetworkPolicyRef>().unwrap_err();
    "cluster:sandbox:sbx-1:generation:nope:sha256:aaaa"
        .parse::<EffectiveNetworkPolicyRef>()
        .unwrap_err();
    "cluster:sandbox:sbx-1:generation:1:sha256:aaaa"
        .parse::<EffectiveNetworkPolicyRef>()
        .unwrap_err();
    assert!(!effective_network_policy_refs_match(
        "cluster:sandbox:sbx-1:generation:1:sha256:aaaa",
        "cluster:sandbox:sbx-1:generation:1:sha256:aaaa"
    ));
    assert_eq!(effective_network_policy_ref_authority_label(""), "unknown");
}

#[test]
fn egress_destination_counts_classify_by_rule_class() {
    let allow = vec![
        EgressDestinationV1::Ip("192.0.2.10".parse().unwrap()),
        EgressDestinationV1::Cidr("198.51.100.0/24".parse().unwrap()),
        EgressDestinationV1::Domain("example.com".parse().unwrap()),
        EgressDestinationV1::Domain("api.example.com".parse().unwrap()),
        EgressDestinationV1::WildcardDomain("*.example.org".parse().unwrap()),
    ];
    let deny = vec![
        EgressDestinationV1::Ip("203.0.113.1".parse().unwrap()),
        EgressDestinationV1::Cidr("203.0.113.0/24".parse().unwrap()),
        EgressDestinationV1::Domain("blocked.example.com".parse().unwrap()),
        EgressDestinationV1::WildcardDomain("*.blocked.example.org".parse().unwrap()),
    ];
    let policy = EgressPolicyV1::new(EgressDefaultAction::Deny, allow, deny).unwrap();

    assert_eq!(
        policy.allow_counts(),
        EgressDestinationCounts {
            total: 5,
            ips: 1,
            cidrs: 1,
            domains: 2,
            wildcard_domains: 1,
        }
    );
    assert_eq!(
        policy.deny_counts(),
        EgressDestinationCounts {
            total: 4,
            ips: 1,
            cidrs: 1,
            domains: 1,
            wildcard_domains: 1,
        }
    );
    assert_eq!(
        EgressDestinationCounts::of(&[]),
        EgressDestinationCounts::default()
    );
}

#[test]
fn deny_policy_denies_egress() {
    assert_eq!(
        NetworkPolicyV1::deny_all().default_action(),
        EgressDefaultAction::Deny
    );
}

#[test]
fn platform_egress_hard_deny_reserved_ranges_before_open_policy() {
    let policy = NetworkPolicyV1::allow_all();
    for (ip, guardrail_id) in [
        ("169.254.169.254", "ec2-imds"),
        ("::ffff:169.254.169.254", "ec2-imds"),
        ("169.254.1.10", "link-local"),
        ("10.11.3.4", "space-slots-a"),
        ("10.12.3.4", "space-slots-b"),
        ("127.0.0.1", "loopback"),
    ] {
        assert_eq!(
            policy.egress_decision_for_ip(ip.parse().unwrap()),
            EgressDecision::platform_guardrail(guardrail_id)
        );
    }
    for ip in [
        "100.63.255.255",
        "100.64.0.1",
        "100.127.255.254",
        "100.128.0.0",
        "169.254.0.21",
    ] {
        assert_eq!(
            policy.egress_decision_for_ip(ip.parse().unwrap()),
            EgressDecision::Allow,
            "address outside platform guardrails should remain allowed: {ip}"
        );
    }
}

#[test]
fn parsed_platform_egress_hard_deny_cover_public_metadata() {
    let parsed = parsed_platform_egress_hard_deny_cidrs();

    assert_eq!(parsed.len(), PLATFORM_EGRESS_HARD_DENY_CIDRS.len());
    for ((guardrail, cidr), metadata) in parsed.iter().zip(PLATFORM_EGRESS_HARD_DENY_CIDRS) {
        assert_eq!(guardrail.id, metadata.id);
        assert_eq!(cidr.to_string(), metadata.cidr);
    }
}

#[test]
fn egress_decision_for_ip_preserves_precedence_and_dns_evidence() {
    let policy = |default_action, allow, deny| NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::Off,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(default_action, allow, deny).unwrap(),
    };
    let ip_rule =
        |value: &str| EgressDestinationV1::Ip(value.parse::<IpAddr>().expect("test IP must parse"));
    let cidr_rule = |value: &str| {
        EgressDestinationV1::Cidr(value.parse::<IpCidr>().expect("test CIDR must parse"))
    };

    let cases = [
        (
            "platform guardrail precedes every policy input",
            NetworkPolicyV1::allow_all(),
            "169.254.169.254",
            true,
            EgressDecision::platform_guardrail("ec2-imds"),
        ),
        (
            "explicit deny precedes an open default",
            policy(EgressDefaultAction::Allow, vec![], vec![ip_rule("1.1.1.1")]),
            "1.1.1.1",
            false,
            EgressDecision::Deny {
                reason: DenyReason::Rule,
            },
        ),
        (
            "explicit deny precedes DNS-derived allow evidence",
            policy(
                EgressDefaultAction::Deny,
                vec![],
                vec![cidr_rule("93.184.216.0/24")],
            ),
            "93.184.216.34",
            true,
            EgressDecision::Deny {
                reason: DenyReason::Rule,
            },
        ),
        (
            "explicit deny precedes an overlapping explicit allow",
            policy(
                EgressDefaultAction::Deny,
                vec![ip_rule("192.0.2.10")],
                vec![cidr_rule("192.0.2.0/24")],
            ),
            "192.0.2.10",
            true,
            EgressDecision::Deny {
                reason: DenyReason::Rule,
            },
        ),
        (
            "explicit allow precedes the closed default",
            policy(
                EgressDefaultAction::Deny,
                vec![ip_rule("192.0.2.10")],
                vec![],
            ),
            "192.0.2.10",
            false,
            EgressDecision::Allow,
        ),
        (
            "DNS-derived allow evidence precedes the closed default",
            NetworkPolicyV1::deny_all(),
            "203.0.113.10",
            true,
            EgressDecision::Allow,
        ),
        (
            "closed default denies an unmatched address",
            NetworkPolicyV1::deny_all(),
            "1.1.1.1",
            false,
            EgressDecision::Deny {
                reason: DenyReason::Default,
            },
        ),
        (
            "open default allows an unmatched address",
            NetworkPolicyV1::allow_all(),
            "1.1.1.1",
            false,
            EgressDecision::Allow,
        ),
    ];

    for (name, policy, address, dns_allowed_ip, expected) in cases {
        assert_eq!(
            policy.egress_decision_for_ip_with_dns_allow(address.parse().unwrap(), dns_allowed_ip),
            expected,
            "{name}"
        );
    }
}

#[test]
fn explicit_address_match_is_distinct_from_the_default_action() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::Off,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Allow,
            vec!["192.0.2.10".parse::<IpAddr>().unwrap().into()],
            vec!["198.51.100.0/24".parse::<IpCidr>().unwrap().into()],
        )
        .unwrap(),
    };

    for (ip, expected) in [
        (
            "169.254.169.254",
            EgressAddressMatch::PlatformGuardrail {
                guardrail_id: "ec2-imds",
            },
        ),
        ("198.51.100.10", EgressAddressMatch::ExplicitDeny),
        ("192.0.2.10", EgressAddressMatch::ExplicitAllow),
        ("203.0.113.10", EgressAddressMatch::NoMatch),
    ] {
        assert_eq!(
            policy.egress_match_for_ip(ip.parse().unwrap()),
            expected,
            "unexpected explicit match for {ip}"
        );
    }
}

#[test]
fn ipv4_rules_match_ipv4_mapped_ipv6_inputs() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::Off,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![
                "1.2.3.4".parse::<IpAddr>().unwrap().into(),
                "5.6.7.0/24".parse::<IpCidr>().unwrap().into(),
            ],
            vec![],
        )
        .unwrap(),
    };

    assert_eq!(
        policy.egress_decision_for_ip("::ffff:1.2.3.4".parse().unwrap()),
        EgressDecision::Allow
    );
    assert_eq!(
        policy.egress_decision_for_ip("::ffff:5.6.7.8".parse().unwrap()),
        EgressDecision::Allow
    );
}

#[test]
fn ipv4_mapped_ipv6_cidr_normalizes_to_ipv4_cidr() {
    let cidr = "::ffff:1.2.3.0/120".parse::<IpCidr>().unwrap();

    assert_eq!(cidr.to_string(), "1.2.3.0/24");
    assert!(cidr.contains("1.2.3.4".parse().unwrap()));
    assert!(cidr.contains("::ffff:1.2.3.4".parse().unwrap()));
    assert!(!cidr.contains("1.2.4.4".parse().unwrap()));
}

#[test]
fn ipv4_mapped_ipv6_host_cidr_normalizes_to_ipv4_host() {
    let cidr = "::ffff:1.2.3.4/128".parse::<IpCidr>().unwrap();

    assert_eq!(cidr.to_string(), "1.2.3.4/32");
    assert!(cidr.contains("1.2.3.4".parse().unwrap()));
    assert!(!cidr.contains("1.2.3.5".parse().unwrap()));
}

#[test]
fn ipv4_mapped_ipv6_cidr_rejects_prefix_before_mapped_space() {
    assert!(matches!(
        "::ffff:1.2.3.4/95".parse::<IpCidr>(),
        Err(NetworkPolicyValidationError::InvalidPrefixLength { .. })
    ));
}

#[test]
fn wildcard_domain_matches_normalized_subdomains_only() {
    let wildcard = "*.example.com".parse::<WildcardDomainName>().unwrap();
    assert!(wildcard.matches("api.example.com"));
    assert!(wildcard.matches("API.EXAMPLE.COM."));
    assert!(!wildcard.matches("example.com"));
    assert!(!wildcard.matches("foo..example.com"));
    assert!(!wildcard.matches("bad*.example.com"));
}

#[test]
fn domain_allow_can_be_checked_by_dns_response_name() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::Off,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![
                EgressDestinationV1::Domain("example.com".parse().unwrap()),
                EgressDestinationV1::WildcardDomain("*.github.com".parse().unwrap()),
            ],
            vec![],
        )
        .unwrap(),
    };
    assert!(policy.allows_domain("example.com"));
    assert!(policy.allows_domain("api.github.com"));
    assert!(!policy.allows_domain("github.com"));
    assert!(policy.egress.allow[0].matches_domain("Example.COM."));
    assert!(policy.egress.allow[1].matches_domain("API.GITHUB.COM."));
}

#[test]
fn typed_domain_predicates_preserve_rule_matches_and_deny_precedence() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::Off,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Allow,
            vec![
                EgressDestinationV1::Domain("allowed.example.com".parse().unwrap()),
                EgressDestinationV1::WildcardDomain("*.shared.example.com".parse().unwrap()),
            ],
            vec![
                EgressDestinationV1::Domain("blocked.example.com".parse().unwrap()),
                EgressDestinationV1::WildcardDomain("*.blocked.example.org".parse().unwrap()),
                EgressDestinationV1::Domain("api.shared.example.com".parse().unwrap()),
            ],
        )
        .unwrap(),
    };

    for (domain, expected_allow, expected_deny, expected_explicit_allow) in [
        ("BLOCKED.EXAMPLE.COM.", false, true, false),
        ("api.blocked.example.org", false, true, false),
        ("api.shared.example.com", true, true, false),
        ("allowed.example.com", true, false, true),
        ("cdn.shared.example.com", true, false, true),
        ("unmatched.example.net", false, false, false),
    ] {
        let domain = domain.parse::<DomainName>().unwrap();
        assert_eq!(
            policy.matches_domain_allow_rule(&domain),
            expected_allow,
            "unexpected allow match for {domain}"
        );
        assert_eq!(
            policy.denies_domain(&domain),
            expected_deny,
            "unexpected deny match for {domain}"
        );
        assert_eq!(
            policy.explicitly_allows_domain_name(&domain),
            expected_explicit_allow,
            "unexpected explicit permission for {domain}"
        );
    }
}

#[test]
fn proto_round_trips_detailed_policy() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::On,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::WildcardDomain(
                "*.example.com".parse().unwrap(),
            )],
            vec![
                EgressDestinationV1::Cidr("10.0.0.0/8".parse().unwrap()),
                EgressDestinationV1::Domain("blocked.example.com".parse().unwrap()),
                EgressDestinationV1::WildcardDomain("*.blocked.example.org".parse().unwrap()),
            ],
        )
        .unwrap(),
    };
    let proto = proto::NetworkPolicyV1::from(&policy);
    assert_eq!(proto.version, proto::NetworkPolicyVersion::V1 as i32);
    assert_eq!(NetworkPolicyV1::try_from(&proto).unwrap(), policy);
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trips_domain_denies() {
    let policy = NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: TlsInterception::On,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![],
            vec![
                EgressDestinationV1::Domain("blocked.example.com".parse().unwrap()),
                EgressDestinationV1::WildcardDomain("*.blocked.example.org".parse().unwrap()),
            ],
        )
        .unwrap(),
    };
    let encoded = serde_json::to_value(&policy).unwrap();

    assert_eq!(
        serde_json::from_value::<NetworkPolicyV1>(encoded).unwrap(),
        policy
    );
}

#[test]
fn proto_rejects_unspecified_version() {
    let proto = proto::NetworkPolicyV1 {
        egress_default_action: proto::EgressDefaultAction::Deny.into(),
        ..Default::default()
    };
    let err = NetworkPolicyV1::try_from(&proto).unwrap_err();
    assert!(matches!(err, NetworkPolicyProtoError::UnspecifiedVersion));
}

#[cfg(feature = "serde")]
#[test]
fn tls_interception_preserves_legacy_json_and_defaults_off() {
    let legacy = r#"{"version":"v1","egress":{"default_action":"deny","allow":[],"deny":[]}}"#;
    let policy: NetworkPolicyV1 = serde_json::from_str(legacy).unwrap();

    assert_eq!(policy.tls_interception, TlsInterception::Off);
    assert_eq!(serde_json::to_string(&policy).unwrap(), legacy);
    assert_eq!(
        NetworkPolicyV1::allow_all().tls_interception,
        TlsInterception::Off
    );
    assert_eq!(
        NetworkPolicyV1::deny_all().tls_interception,
        TlsInterception::Off
    );
}

#[cfg(feature = "serde")]
#[test]
fn tls_interception_json_round_trips_on() {
    let on = r#"{"version":"v1","egress":{"default_action":"deny","allow":[],"deny":[]},"tls_interception":"on"}"#;
    let policy: NetworkPolicyV1 = serde_json::from_str(on).unwrap();

    assert_eq!(policy.tls_interception, TlsInterception::On);
    assert_eq!(serde_json::to_string(&policy).unwrap(), on);
}

#[cfg(feature = "serde")]
#[test]
fn network_policy_json_ignores_unknown_top_level_fields() {
    let policy: NetworkPolicyV1 = serde_json::from_str(
        r#"{"version":"v1","egress":{"default_action":"deny"},"future_field":true}"#,
    )
    .unwrap();

    assert_eq!(policy, NetworkPolicyV1::deny_all());
    serde_json::from_str::<NetworkPolicyV1>(
        r#"{"version":"v1","egress":{"default_action":"deny","future_field":true}}"#,
    )
    .unwrap_err();
}

#[test]
fn tls_interception_proto_unspecified_defaults_off_and_on_round_trips() {
    use prost::Message;

    let unspecified = proto::NetworkPolicyV1 {
        version: proto::NetworkPolicyVersion::V1.into(),
        egress_default_action: proto::EgressDefaultAction::Deny.into(),
        ..Default::default()
    };
    assert_eq!(
        NetworkPolicyV1::try_from(&unspecified)
            .unwrap()
            .tls_interception,
        TlsInterception::Off
    );
    assert_eq!(
        proto::NetworkPolicyV1::from(&NetworkPolicyV1::deny_all()).encode_to_vec(),
        [8, 1, 16, 2]
    );

    let mut on = NetworkPolicyV1::deny_all();
    on.tls_interception = TlsInterception::On;
    assert_eq!(
        NetworkPolicyV1::try_from(&proto::NetworkPolicyV1::from(&on)).unwrap(),
        on
    );
}

#[cfg(feature = "serde")]
#[test]
fn network_credential_policy_normalizes_optional_port_and_matches_safe_paths() {
    let omitted = r#"{
        "version":"v1",
        "egress":{"default_action":"allow"},
        "tls_interception":"on",
        "network_credential_rules":[{
            "rule_id":"api",
            "destination":{"exact_host":"API.Example.COM.","path_prefix":"/v1"},
            "credential_header_bindings":[{
                "header_name":"X-Api-Key",
                "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000002"
            },{
                "header_name":"Authorization",
                "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000001"
            }],
            "supported_http_methods":["POST","GET"]
        }]
    }"#;
    let explicit = omitted.replace(
        r#""exact_host":"API.Example.COM.","path_prefix""#,
        r#""exact_host":"API.Example.COM.","port":443,"path_prefix""#,
    );
    let omitted: NetworkPolicyV1 = serde_json::from_str(omitted).unwrap();
    let explicit: NetworkPolicyV1 = serde_json::from_str(&explicit).unwrap();

    assert_eq!(omitted, explicit);
    assert_eq!(
        serde_json::to_value(&omitted).unwrap()["network_credential_rules"][0]["destination"],
        serde_json::json!({"exact_host":"api.example.com","path_prefix":"/v1"})
    );
    let rule = &omitted.network_credential_rules[0];
    let destination = &rule.destination;
    assert!(destination.matches_path("/v1"));
    assert!(destination.matches_path("/v1/users"));
    assert!(!destination.matches_path("/v10"));
    assert!(!destination.matches_path("/v1/%2fadmin"));
    assert!(!destination.matches_path("/v1/../admin"));
    assert_eq!(
        rule.credential_header_bindings,
        [
            CredentialHeaderBindingV1 {
                header_name: "authorization".into(),
                credential_reference_id: "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap(),
            },
            CredentialHeaderBindingV1 {
                header_name: "x-api-key".into(),
                credential_reference_id: "018f1e9d-7b7a-7d3a-8000-000000000002".parse().unwrap(),
            },
        ]
    );
    assert_eq!(
        rule.supported_http_methods,
        [
            NetworkCredentialHttpMethodV1::Get,
            NetworkCredentialHttpMethodV1::Post,
        ]
    );
    assert_eq!(
        omitted
            .matching_network_credential_bindings(
                &"api.example.com".parse().unwrap(),
                443,
                "/v1/users",
                NetworkCredentialHttpMethodV1::Get,
            )
            .into_iter()
            .map(|binding| binding.credential_reference_id)
            .collect::<Vec<_>>(),
        [
            "018f1e9d-7b7a-7d3a-8000-000000000001"
                .parse::<uuid::Uuid>()
                .unwrap(),
            "018f1e9d-7b7a-7d3a-8000-000000000002"
                .parse::<uuid::Uuid>()
                .unwrap(),
        ]
    );
    assert_eq!(
        omitted
            .matching_network_credential_bindings(
                &"api.example.com".parse().unwrap(),
                443,
                "/v1/users",
                NetworkCredentialHttpMethodV1::Post,
            )
            .len(),
        2
    );
    assert!(
        omitted
            .matching_network_credential_bindings(
                &"api.example.com".parse().unwrap(),
                443,
                "/v10",
                NetworkCredentialHttpMethodV1::Get,
            )
            .is_empty()
    );
}

#[cfg(feature = "serde")]
#[test]
fn network_credential_policy_uses_rule_order_and_rejects_unsafe_headers_and_tls_off() {
    let base = serde_json::json!({
        "version":"v1",
        "egress":{"default_action":"allow"},
        "tls_interception":"on",
        "network_credential_rules":[{
            "rule_id":"one",
            "destination":{"exact_host":"api.example.com","path_prefix":"/v1"},
            "credential_header_bindings":[{
                "header_name":"authorization",
                "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000001"
            }],
            "supported_http_methods":["GET"]
        }]
    });
    let mut overlap = base.clone();
    let mut second = overlap["network_credential_rules"][0].clone();
    second["rule_id"] = "two".into();
    second["destination"]["path_prefix"] = "/v1/admin".into();
    second["credential_header_bindings"][0]["credential_reference_id"] =
        "018f1e9d-7b7a-7d3a-8000-000000000002".into();
    second["credential_header_bindings"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "header_name":"x-api-key",
            "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000002"
        }));
    overlap["network_credential_rules"]
        .as_array_mut()
        .unwrap()
        .push(second);
    let overlap = serde_json::from_value::<NetworkPolicyV1>(overlap).unwrap();
    assert_eq!(
        overlap
            .matching_network_credential_bindings(
                &"api.example.com".parse().unwrap(),
                443,
                "/v1/admin",
                NetworkCredentialHttpMethodV1::Get,
            )
            .into_iter()
            .map(|binding| {
                (
                    binding.header_name.as_str(),
                    binding.credential_reference_id,
                )
            })
            .collect::<Vec<_>>(),
        [
            (
                "authorization",
                "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap(),
            ),
            (
                "x-api-key",
                "018f1e9d-7b7a-7d3a-8000-000000000002".parse().unwrap(),
            ),
        ]
    );

    let mut reserved = base.clone();
    reserved["network_credential_rules"][0]["credential_header_bindings"] = serde_json::json!([{
        "header_name":"host",
        "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000001"
    }]);
    serde_json::from_value::<NetworkPolicyV1>(reserved).unwrap_err();

    let mut duplicate = base.clone();
    duplicate["network_credential_rules"][0]["credential_header_bindings"] = serde_json::json!([{
        "header_name":"Authorization",
        "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000001"
    },{
        "header_name":"authorization",
        "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000002"
    }]);
    serde_json::from_value::<NetworkPolicyV1>(duplicate).unwrap_err();

    let mut off = base;
    off.as_object_mut().unwrap().remove("tls_interception");
    serde_json::from_value::<NetworkPolicyV1>(off).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn network_credential_policy_allows_disjoint_paths_and_normalizes_method_sets() {
    let mut policy = serde_json::json!({
        "version":"v1",
        "egress":{"default_action":"allow"},
        "tls_interception":"on",
        "network_credential_rules":[{
            "rule_id":"one",
            "destination":{"exact_host":"api.example.com","path_prefix":"/v1"},
            "credential_header_bindings":[{
                "header_name":"authorization",
                "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000001"
            }],
            "supported_http_methods":["GET"]
        },{
            "rule_id":"two",
            "destination":{"exact_host":"api.example.com","path_prefix":"/v2"},
            "credential_header_bindings":[{
                "header_name":"authorization",
                "credential_reference_id":"018f1e9d-7b7a-7d3a-8000-000000000002"
            }],
            "supported_http_methods":["GET"]
        }]
    });
    serde_json::from_value::<NetworkPolicyV1>(policy.clone()).unwrap();

    policy["network_credential_rules"][1]["supported_http_methods"] =
        serde_json::json!(["GET", "GET"]);
    let normalized = serde_json::from_value::<NetworkPolicyV1>(policy.clone()).unwrap();
    assert_eq!(
        normalized.network_credential_rules[1].supported_http_methods,
        [NetworkCredentialHttpMethodV1::Get]
    );

    policy["network_credential_rules"][1]["supported_http_methods"] = serde_json::json!(["TRACE"]);
    serde_json::from_value::<NetworkPolicyV1>(policy).unwrap_err();
}

#[test]
fn network_credential_policy_proto_round_trips_without_changing_legacy_bytes() {
    use prost::Message;

    let legacy = proto::NetworkPolicyV1::from(&NetworkPolicyV1::deny_all()).encode_to_vec();
    assert_eq!(legacy, [8, 1, 16, 2]);

    let mut policy = NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;
    policy.network_credential_rules = vec![NetworkCredentialRuleV1 {
        rule_id: "api".into(),
        destination: NetworkCredentialDestinationV1::new(
            "api.example.com".parse().unwrap(),
            None,
            Some("/v1".into()),
        )
        .unwrap(),
        credential_header_bindings: vec![CredentialHeaderBindingV1 {
            header_name: "authorization".into(),
            credential_reference_id: "018f1e9d-7b7a-7d3a-8000-000000000001".parse().unwrap(),
        }],
        supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
    }];
    let proto = proto::NetworkPolicyV1::from(&policy);
    assert_eq!(
        proto.network_credential_rules[0]
            .destination
            .as_ref()
            .unwrap()
            .port,
        None
    );
    assert_eq!(NetworkPolicyV1::try_from(&proto).unwrap(), policy);

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyNetworkPolicyV1 {
        #[prost(enumeration = "proto::NetworkPolicyVersion", tag = "1")]
        version: i32,
        #[prost(enumeration = "proto::EgressDefaultAction", tag = "2")]
        egress_default_action: i32,
        #[prost(message, repeated, tag = "3")]
        allow: Vec<proto::EgressDestinationV1>,
        #[prost(message, repeated, tag = "4")]
        deny: Vec<proto::EgressDestinationV1>,
        #[prost(enumeration = "proto::TlsInterception", tag = "5")]
        tls_interception: i32,
    }

    let encoded = proto.encode_to_vec();
    let decoded_by_legacy_reader = LegacyNetworkPolicyV1::decode(encoded.as_slice()).unwrap();
    assert_eq!(
        decoded_by_legacy_reader.version,
        proto::NetworkPolicyVersion::V1 as i32
    );
    assert_eq!(
        decoded_by_legacy_reader.egress_default_action,
        proto::EgressDefaultAction::Allow as i32
    );
    assert_eq!(
        decoded_by_legacy_reader.tls_interception,
        proto::TlsInterception::On as i32
    );
}

impl From<IpAddr> for EgressDestinationV1 {
    fn from(value: IpAddr) -> Self {
        Self::Ip(value)
    }
}

impl From<IpCidr> for EgressDestinationV1 {
    fn from(value: IpCidr) -> Self {
        Self::Cidr(value)
    }
}

#[cfg(feature = "serde")]
#[test]
fn interception_exemptions_round_trip_without_granting_egress() {
    let mut policy = NetworkPolicyV1::deny_all();
    policy.tls_interception = TlsInterception::On;
    policy.tls_interception_exemptions = vec![
        EgressDestinationV1::Ip("192.0.2.10".parse().unwrap()),
        EgressDestinationV1::Domain("sensor.example.test".parse().unwrap()),
    ];
    let json = serde_json::to_string(&policy).unwrap();
    assert_eq!(
        serde_json::from_str::<NetworkPolicyV1>(&json).unwrap(),
        policy
    );
    let wire = proto::NetworkPolicyV1::from(&policy);
    assert_eq!(NetworkPolicyV1::try_from(&wire).unwrap(), policy);
    assert!(
        !policy
            .egress_decision_for_ip("192.0.2.10".parse().unwrap())
            .is_allowed()
    );
    assert!(!policy.allows_domain("sensor.example.test"));
    assert_ne!(
        network_policy_hash(&policy),
        network_policy_hash(&NetworkPolicyV1 {
            tls_interception_exemptions: vec![],
            ..policy
        })
    );
}

#[test]
fn tls_exemptions_select_transport_by_destination() {
    let mut policy = NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;
    policy.tls_interception_exemptions = vec![
        EgressDestinationV1::Domain("sensor.example.test".parse().unwrap()),
        EgressDestinationV1::Ip("192.0.2.10".parse().unwrap()),
        EgressDestinationV1::Cidr("198.51.100.0/24".parse().unwrap()),
        EgressDestinationV1::WildcardDomain("*.pinned.example.test".parse().unwrap()),
    ];
    for (ip, host, expected) in [
        (
            "203.0.113.10",
            Some("sensor.example.test"),
            TlsInterception::Off,
        ),
        ("192.0.2.10", None, TlsInterception::Off),
        (
            "192.0.2.10",
            Some("other.example.test"),
            TlsInterception::Off,
        ),
        ("198.51.100.10", None, TlsInterception::Off),
        (
            "203.0.113.10",
            Some("api.pinned.example.test"),
            TlsInterception::Off,
        ),
        (
            "203.0.113.10",
            Some("other.example.test"),
            TlsInterception::On,
        ),
        (
            "203.0.113.10",
            Some("sensor.example.test.attacker.test"),
            TlsInterception::On,
        ),
        ("203.0.113.10", None, TlsInterception::On),
    ] {
        let host = host.map(|host| host.parse().unwrap());
        assert_eq!(
            policy.tls_interception_for(ip.parse().unwrap(), host.as_ref()),
            expected
        );
    }
}
