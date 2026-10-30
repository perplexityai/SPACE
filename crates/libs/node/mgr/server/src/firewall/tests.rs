use super::*;

#[test]
fn exact_comment_handle_extraction_does_not_match_port_prefixes() {
    let output = r#"
table ip ingress {
  chain prerouting {
tcp dport 220 dnat to 169.254.0.21:22 comment "dnat-10.12.0.2-tcp-220" # handle 7
tcp dport 22 dnat to 169.254.0.22:22 comment "dnat-10.12.0.3-tcp-22" # handle 8
tcp dport 2222 dnat to 169.254.0.23:22 comment "dnat-10.12.0.4-tcp-2222" # handle 9
  }
}
"#;

    let handles = extract_rule_handles(output, &quoted_comment_needle("dnat-10.12.0.3-tcp-22"));
    assert_eq!(handles, vec![8]);
}

#[test]
fn ingress_dnat_target_parse_matches_protocol_and_port() {
    let output = r#"
table ip ingress {
  chain prerouting {
tcp dport 219 dnat to 169.254.0.20:22 comment "space-dnat-10.12.0.1-tcp-219" # handle 6
tcp dport 220 dnat to 169.254.0.21:22 comment "dnat-10.12.0.2-tcp-220" # handle 7
udp dport 221 dnat to 169.254.0.22:22 comment "dnat-10.12.0.3-udp-221" # handle 8
tcp dport 8888 dnat to 10.12.0.4:8888 comment "dnat-10.12.0.4-tcp-8888" # handle 9
  }
}
"#;

    assert_eq!(
        parse_ingress_dnat_target(output, "tcp", 219),
        Some(("169.254.0.20".to_string(), 22))
    );
    assert_eq!(
        parse_ingress_dnat_target(output, "tcp", 220),
        Some(("169.254.0.21".to_string(), 22))
    );
    assert_eq!(
        parse_ingress_dnat_target(output, "udp", 221),
        Some(("169.254.0.22".to_string(), 22))
    );
    assert_eq!(
        parse_ingress_dnat_target(output, "tcp", 8888),
        Some(("10.12.0.4".to_string(), 8888))
    );
    assert_eq!(parse_ingress_dnat_target(output, "tcp", 999), None);
    assert_eq!(parse_ingress_dnat_target(output, "udp", 220), None);
}

#[test]
fn root_ingress_sweep_targets_only_space_dnat_comments() {
    let output = r#"
table ip ingress {
  chain prerouting {
iifname "ens1" tcp dport 9012 dnat to 10.12.128.2:9012 comment "space-dnat-10.12.128.2-tcp-9012" # handle 7
iifname "ens1" udp dport 5000 dnat to 10.12.128.4:5000 comment "dnat-10.12.128.4-udp-5000" # handle 8
iifname "ens1" tcp dport 443 dnat to 10.12.128.6:443 comment "not-space-owned" # handle 9
  }
}
"#;

    let handles = extract_space_ingress_dnat_rule_handles(output);
    assert_eq!(handles, vec![7, 8]);
}

#[test]
fn root_ingress_sweep_rejects_unproven_dnat_comments() {
    let output = r#"
table ip ingress {
  chain prerouting {
iifname "ens1" tcp dport 9012 dnat to 10.12.128.2:9012 comment "dnat-10.99.0.2-tcp-9012" # handle 7
iifname "ens1" tcp dport 9012 dnat to 10.12.128.2:9012 comment "dnat-10.12.128.2-sctp-9012" # handle 8
iifname "ens1" tcp dport 9012 dnat to 10.12.128.2:9012 comment "dnat-10.12.128.2-tcp-0" # handle 9
iifname "ens1" tcp dport 9012 dnat to 10.12.128.4:9012 comment "dnat-10.12.128.2-tcp-9012" # handle 10
iifname "ens1" tcp dport 9013 dnat to 10.12.128.2:9012 comment "dnat-10.12.128.2-tcp-9012" # handle 11
iifname "ens1" tcp dport 9012 accept comment "dnat-10.12.128.2-tcp-9012" # handle 12
  }
}
"#;

    let handles = extract_space_ingress_dnat_rule_handles(output);
    assert!(handles.is_empty());
}

#[test]
fn ingress_dnat_selector_matches_new_and_legacy_comments() {
    let output = r#"
table ip ingress {
  chain prerouting {
tcp dport 9012 dnat to 10.12.128.2:9012 comment "space-dnat-10.12.128.2-tcp-9012" # handle 7
udp dport 5000 dnat to 10.12.128.2:5000 comment "dnat-10.12.128.2-udp-5000" # handle 8
tcp dport 9013 dnat to 10.12.128.4:9013 comment "space-dnat-10.12.128.4-tcp-9013" # handle 9
  }
}
"#;

    assert_eq!(
        extract_ingress_dnat_rule_handles_for_selector(
            output,
            "10.12.128.2",
            "tcp",
            PortSelector::All,
        ),
        vec![7, 8]
    );
    assert_eq!(
        extract_ingress_dnat_rule_handles_for_selector(
            output,
            "10.12.128.2",
            "udp",
            PortSelector::Port(5000),
        ),
        vec![8]
    );
}

#[test]
fn guest_allowlist_comment_match_is_exact() {
    let output = r#"
table ip sandbox {
  chain forward {
iifname "vpeer0" tcp dport 220 accept comment "allow-tcp-220" # handle 3
iifname "vpeer0" tcp dport 22 accept comment "allow-tcp-22" # handle 4
  }
}
"#;

    let handles = extract_rule_handles(output, &quoted_comment_needle("allow-tcp-22"));
    assert_eq!(handles, vec![4]);
}

#[test]
fn missing_ingress_chain_list_error_is_idempotent_absence() {
    let missing_chain = r#"Error: Could not process rule: No such file or directory
list chain ip ingress prerouting
          ^^^^^^^"#;
    let missing_table = r#"Error: No such file or directory; did you mean table 'filter' in family ip?
list chain ip ingress prerouting
          ^^^^^^^"#;
    let permission_denied = r#"Error: Could not process rule: Operation not permitted
list chain ip ingress prerouting
          ^^^^^^^"#;
    let wrong_chain = r#"Error: Could not process rule: No such file or directory
list chain ip sandbox forward
          ^^^^^^^"#;

    assert!(is_missing_ingress_chain_list_error(missing_chain));
    assert!(is_missing_ingress_chain_list_error(missing_table));
    assert!(!is_missing_ingress_chain_list_error(permission_denied));
    assert!(!is_missing_ingress_chain_list_error(wrong_chain));
}

#[test]
fn platform_guardrail_rules_include_imds_link_local_and_loopback() {
    let rules = platform_guardrail_drop_rules_json("inet", SPACE_TABLE, "forward", "sveth-*");

    // Snapshot pins the entire generated rule set (guardrail ids, CIDRs,
    // prefix lengths, the wildcard iifname, and the terminal drop) — a
    // superset of the old substring ladder, and order-sensitive besides.
    insta::with_settings!({ snapshot_path => "../fixtures", prepend_module_to_snapshot => false }, {
        insta::assert_snapshot!("platform_guardrail_rules_include_imds_link_local_and_loopback", rules);
    });
}

#[test]
fn platform_guardrail_rules_filter_by_nft_family() {
    let rules = platform_guardrail_drop_rules_json("ip", SANDBOX_TABLE, "guardrails", "tap0");

    // Snapshot pins the filtered (ip-family-only) rule set; the absence of
    // any ip6 guardrail is verified by inspecting the committed snapshot.
    insta::with_settings!({ snapshot_path => "../fixtures", prepend_module_to_snapshot => false }, {
        insta::assert_snapshot!("platform_guardrail_rules_filter_by_nft_family", rules);
    });
}

#[test]
fn namespace_rules_json_matches_expected_shape() {
    let pre_tproxy_guardrail_rules = packaged_pre_tproxy_guardrails_policy();
    let json = namespace_rules_json(
        "vpeer-000000",
        "10.12.128.2",
        "169.254.0.21",
        &pre_tproxy_guardrail_rules,
    );
    let actual: serde_json::Value = serde_json::from_str(&json).unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/namespace_rules.json")).unwrap();

    assert_eq!(actual, expected);
    insta::with_settings!({ snapshot_path => "../fixtures", prepend_module_to_snapshot => false }, {
        insta::assert_json_snapshot!("namespace_rules_json", sorted_json(actual));
    });
}

fn guest_source_guard_exprs(guest_ip: &str) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "match": {
                "left": { "meta": { "key": "iifname" } },
                "right": "tap0",
                "op": "=="
            }
        }),
        serde_json::json!({
            "match": {
                "left": { "payload": { "protocol": "ip", "field": "saddr" } },
                "right": guest_ip,
                "op": "!="
            }
        }),
        serde_json::json!({ "drop": null }),
    ]
}

fn established_bypass_exprs() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "match": {
                "left": { "meta": { "key": "iifname" } },
                "right": "tap0",
                "op": "=="
            }
        }),
        serde_json::json!({
            "match": {
                "left": { "ct": { "key": "state" } },
                "right": { "set": ["established", "related"] },
                "op": "in"
            }
        }),
        serde_json::json!({ "return": null }),
    ]
}

#[test]
fn namespace_source_guard_precedes_established_conntrack_bypass() {
    let guest_ip = "169.254.0.99";
    let json = namespace_rules_json(
        "vpeer-000000",
        "10.12.128.2",
        guest_ip,
        &packaged_pre_tproxy_guardrails_policy(),
    );
    let rendered: serde_json::Value = serde_json::from_str(&json).unwrap();
    let rules = rendered["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get("add")?.get("rule"))
        .filter(|rule| {
            rule.get("table") == Some(&serde_json::json!(SANDBOX_TABLE))
                && rule.get("chain") == Some(&serde_json::json!(PRE_TPROXY_GUARDRAILS_CHAIN))
        })
        .collect::<Vec<_>>();

    let source_guard = guest_source_guard_exprs(guest_ip);
    let established_bypass = established_bypass_exprs();
    let rule_position = |expected: &[serde_json::Value]| {
        rules.iter().position(|rule| {
            rule.get("expr")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|expr| expected.iter().all(|needle| expr.contains(needle)))
        })
    };

    let source_guard_position = rule_position(&source_guard)
        .expect("tap0 must drop packets whose source is not the assigned guest IP");
    let established_bypass_position = rule_position(&established_bypass)
        .expect("fixture must retain the established/related conntrack bypass");
    assert!(
        source_guard_position < established_bypass_position,
        "guest source validation must run before established traffic can bypass filtering"
    );
}

fn sorted_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(sorted_json).collect())
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));

            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sorted_json(value)))
                    .collect(),
            )
        }
        value => value,
    }
}

#[test]
fn namespace_postrouting_snat_matches_gateway_outbound_mark() {
    let pre_tproxy_guardrail_rules = packaged_pre_tproxy_guardrails_policy();
    let json = namespace_rules_json(
        "vpeer-000000",
        "10.12.128.2",
        "169.254.0.21",
        &pre_tproxy_guardrail_rules,
    );
    let rules: serde_json::Value = serde_json::from_str(&json).unwrap();
    let actual = rules["nftables"]
        .as_array()
        .expect("nftables must be an array")
        .iter()
        .filter_map(|entry| entry.get("add")?.get("rule"))
        .find(|rule| {
            rule.get("family") == Some(&serde_json::json!("ip"))
                && rule.get("table") == Some(&serde_json::json!(SANDBOX_TABLE))
                && rule.get("chain") == Some(&serde_json::json!("postrouting"))
        })
        .expect("sandbox postrouting SNAT rule must be present");
    let expr = actual["expr"]
        .as_array()
        .expect("rule expr must be an array");

    assert!(
        expr.iter()
            .any(|expr| expr.get("match").is_some_and(|matcher| matcher
                == &serde_json::json!({
                    "left": { "meta": { "key": "mark" } },
                    "right": OUTBOUND_MARK,
                    "op": "=="
                }))),
        "sandbox postrouting SNAT must only translate gateway-originated packets"
    );
    assert!(
        expr.iter().any(|expr| expr.get("snat").is_some()),
        "sandbox postrouting rule must still SNAT matching gateway packets"
    );
}

fn chain_with_rules(rules: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({
        "nftables": rules
            .into_iter()
            .map(|rule| serde_json::json!({ "rule": rule }))
            .collect::<Vec<_>>()
    })
}

fn rule_with_exprs(exprs: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({ "expr": exprs })
}

#[test]
fn namespace_source_guard_validator_rejects_ineffective_rules() {
    let guest_ip = "169.254.0.21";
    let source_guard = rule_with_exprs(guest_source_guard_exprs(guest_ip));
    let established_bypass = rule_with_exprs(established_bypass_exprs());
    let (valid, packaged) = udp443_validation_fixture();
    let udp443 = valid["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|entry| {
            (entry["rule"]["comment"] == UDP443_COMMENT).then(|| entry["rule"].clone())
        })
        .unwrap();

    validate_pre_tproxy_guardrails_chain(
        &chain_with_rules(vec![
            source_guard.clone(),
            established_bypass.clone(),
            udp443,
        ]),
        guest_ip,
        &packaged,
    )
    .unwrap();

    let cases = [
        (
            "missing",
            chain_with_rules(vec![established_bypass.clone()]),
            "namespace pre-TPROXY guardrails chain is missing tap0 guest source guard",
        ),
        (
            "wrong guest source",
            chain_with_rules(vec![
                rule_with_exprs(guest_source_guard_exprs("169.254.0.99")),
                established_bypass.clone(),
            ]),
            "namespace pre-TPROXY guardrails chain is missing tap0 guest source guard",
        ),
        (
            "after established bypass",
            chain_with_rules(vec![established_bypass, source_guard]),
            "namespace tap0 guest source guard must precede established/related return",
        ),
    ];

    for (case, chain, expected) in cases {
        assert_eq!(
            validate_pre_tproxy_guardrails_chain(&chain, guest_ip, &packaged)
                .unwrap_err()
                .to_string(),
            expected,
            "{case}"
        );
    }
}

// The fixture is verbatim `nft -j list chain ip sandbox pre_tproxy_guardrails`
// output, captured after applying fixtures/namespace_rules.json — the same two
// argvs apply_nft_json and nft_chain_json use. Every other firewall fixture is
// hand-authored in nft's *input* shape, so a validator that reads rules back
// was untestable: nft rewrites `"op": "in"` to `"op": "=="` for set membership.
#[test]
fn namespace_source_guard_validator_accepts_live_nft_chain_output() {
    let chain: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/live_pre_tproxy_guardrails_chain.json"
    ))
    .expect("captured nft chain fixture must parse");

    let (_, packaged) = udp443_validation_fixture();
    validate_pre_tproxy_guardrails_chain(&chain, "169.254.0.21", &packaged)
        .expect("validator must accept the chain shape real nft lists back");
}

#[test]
fn namespace_forward_drop_validator_accepts_expected_rule_shapes() {
    let sandbox_forward = chain_with_rules(vec![
        serde_json::json!({
            "expr": [
                { "match": { "left": { "meta": { "key": "iifname" } }, "right": "tap0", "op": "==" } },
                { "drop": null }
            ]
        }),
        serde_json::json!({
            "expr": [
                { "match": { "left": { "meta": { "key": "iifname" } }, "right": "vpeer-000000", "op": "==" } },
                { "match": { "left": { "meta": { "key": "oifname" } }, "right": "tap0", "op": "==" } },
                { "drop": null }
            ]
        }),
    ]);

    validate_forward_drop_chain(&sandbox_forward, "vpeer-000000").unwrap();
}

#[test]
fn sandbox_forward_drop_validator_rejects_missing_guest_egress_drop_rule() {
    let sandbox_forward = chain_with_rules(vec![serde_json::json!({
        "expr": [
            { "match": { "left": { "meta": { "key": "iifname" } }, "right": "vpeer-000000", "op": "==" } },
            { "match": { "left": { "meta": { "key": "oifname" } }, "right": "tap0", "op": "==" } },
            { "drop": null }
        ]
    })]);

    assert_eq!(
        validate_forward_drop_chain(&sandbox_forward, "vpeer-000000").unwrap_err(),
        "namespace sandbox forward chain is missing iifname tap0 drop"
    );
}

fn packaged_pre_tproxy_guardrails_policy() -> String {
    include_str!("../../../../../../../docker/space/nftables/sandbox-pre-tproxy-guardrails.json")
        .trim_end()
        .to_string()
}

#[test]
fn inbound_reply_disable_removes_owned_rules_and_preserves_unrelated_allowlist() {
    let legacy = serde_json::json!({
        "handle": 42, "comment": "ct-inbound-replies",
        "expr": [{ "accept": null }]
    });
    let allow = serde_json::json!({
        "handle": 7, "comment": "allow-tcp-9000",
        "expr": [{ "accept": null }]
    });
    let existing = chain_with_rules(vec![allow.clone(), legacy.clone()]);
    let updates = inbound_reply_rule_removals(Some(&existing)).unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["delete"]["rule"]["handle"], 42);
    assert!(
        inbound_reply_rule_removals(Some(&chain_with_rules(vec![allow])))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn inbound_reply_disable_fails_closed_without_rule_handle() {
    let existing = chain_with_rules(vec![serde_json::json!({
        "comment": "ct-inbound-replies", "expr": [{ "accept": null }]
    })]);
    let error = inbound_reply_rule_removals(Some(&existing)).unwrap_err();
    assert!(matches!(
        error,
        FirewallError::MissingRuleHandle {
            comment: "ct-inbound-replies"
        }
    ));
    assert_eq!(
        error.to_string(),
        "owned nft rule ct-inbound-replies is missing its handle"
    );
}

#[test]
fn proxy_keeps_only_local_service_reply_exception() {
    let generated: serde_json::Value =
        serde_json::from_str(&netgtw_tproxy_rules_json(12345, false)).unwrap();
    let rules: Vec<_> = generated["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get("add")?.get("rule"))
        .filter(|rule| {
            matches!(
                rule["comment"].as_str(),
                Some("skip-tproxy-inbound-replies" | "skip-tproxy-local-service-replies")
            )
        })
        .collect();
    assert_eq!(rules, vec![&local_service_reply_rule()]);
}

#[test]
fn broad_tproxy_reply_bypass_is_replaced_in_place() {
    let legacy = serde_json::json!({
        "handle": 42, "comment": "skip-tproxy-inbound-replies",
        "expr": [{ "return": null }]
    });
    let unrelated = serde_json::json!({
        "handle": 7, "comment": "unrelated", "expr": [{ "return": null }]
    });
    let existing = chain_with_rules(vec![unrelated, legacy]);
    let updates = local_service_reply_rule_updates(Some(&existing)).unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["replace"]["rule"]["handle"], 42);
    assert_eq!(
        updates[0]["replace"]["rule"]["comment"],
        "skip-tproxy-local-service-replies"
    );
    assert!(
        updates[0]["replace"]["rule"]["expr"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!({
                "match": { "left": { "payload": { "protocol": "ip", "field": "daddr" } },
                           "right": crate::net::GUEST_GATEWAY, "op": "==" }
            }))
    );
}

// Captured with nft 1.1.3 after `nft -j -f` applied the forward rule JSON,
// followed by `nft -j list chain ip sandbox forward` in an isolated netns.
#[test]
fn inbound_reply_disable_removes_live_rule() {
    let chain =
        serde_json::from_str(include_str!("../fixtures/live_inbound_reply_chain.json")).unwrap();
    assert_eq!(inbound_reply_rule_removals(Some(&chain)).unwrap().len(), 1);
}

#[test]
fn guest_udp443_counter_parses_packets_and_action() {
    let mut chain: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/live_pre_tproxy_guardrails_chain.json"
    ))
    .unwrap();
    assert_eq!(
        udp443_counter(Some(&chain)),
        Some((0, GuestUdp443Action::Reject))
    );
    // Preserve the real readback shape while exercising observation and a nonzero count.
    for entry in chain["nftables"].as_array_mut().unwrap() {
        if entry["rule"]["comment"] == "platform-egress-pre-tproxy-guardrail-quic-udp443" {
            entry["rule"]["expr"]
                .as_array_mut()
                .unwrap()
                .retain(|expr| expr.get("reject").is_none());
            for expr in entry["rule"]["expr"].as_array_mut().unwrap() {
                if expr.get("counter").is_some() {
                    expr["counter"]["packets"] = serde_json::json!(42);
                }
            }
        }
    }
    assert_eq!(
        udp443_counter(Some(&chain)),
        Some((42, GuestUdp443Action::Observe))
    );
    assert_eq!(udp443_counter(None), None);
}

#[test]
fn guest_vlan_counter_combines_both_ingress_representations() {
    let chain = chain_with_rules(vec![
        serde_json::json!({
            "comment": "drop-guest-vlan", "expr": [{"counter": {"packets": 2}}]
        }),
        serde_json::json!({
            "comment": "drop-guest-vlan", "expr": [{"counter": {"packets": 3}}]
        }),
        serde_json::json!({
            "comment": "unrelated", "expr": [{"counter": {"packets": 99}}]
        }),
    ]);
    assert_eq!(guest_vlan_counter(Some(&chain)), 5);
    assert_eq!(guest_vlan_counter(None), 0);
}

fn stale_gateway_mac_rule() -> serde_json::Value {
    serde_json::json!({
        "comment": "accept-stale-gateway-mac",
        "expr": [
            {"match": {"left": {"meta": {"key": "protocol"}}, "right": {"set": [2048, 2054]}, "op": "=="}},
            {"match": {"left": {"meta": {"key": "pkttype"}}, "right": "other", "op": "=="}},
            {"counter": {"packets": 3, "bytes": 180}},
            {"mangle": {"key": {"meta": {"key": "pkttype"}}, "value": "host"}}
        ]
    })
}

#[test]
fn guest_l2_ingress_validator_requires_both_drop_forms() {
    let rule = |left: serde_json::Value| {
        serde_json::json!({
            "comment": "drop-guest-vlan",
            "expr": [
                {"match": {"left": left, "right": {"set": [33024, 34984]}, "op": "=="}},
                {"counter": {"packets": 7, "bytes": 512}},
                {"drop": null}
            ]
        })
    };
    let mut entries = vec![serde_json::json!({"chain": {
        "family": "netdev", "table": "sandbox_l2", "name": "ingress",
        "type": "filter", "hook": "ingress", "prio": -500, "dev": "tap0"
    }})];
    entries.extend([
        serde_json::json!({"rule": rule(serde_json::json!({"meta": {"key": "protocol"}}))}),
        serde_json::json!({"rule": rule(serde_json::json!({
            "payload": {"protocol": "ether", "field": "type"}
        }))}),
        serde_json::json!({"rule": stale_gateway_mac_rule()}),
    ]);
    let valid = serde_json::json!({"nftables": entries});
    validate_guest_l2_ingress_chain(&valid).unwrap();

    let mut missing_drop = valid;
    missing_drop["nftables"][2]["rule"]["expr"][2] = serde_json::json!({"accept": null});
    assert!(validate_guest_l2_ingress_chain(&missing_drop).is_err());
}

// The policy is applied in nft's native syntax, the only form that creates
// the TAP-bound chain on nftables 1.0.4 as well as later versions; the
// 1.0.4 fixtures below are that script's result read back from 1.0.4.
#[test]
fn guest_l2_ingress_policy_is_one_native_transaction_bound_to_the_tap() {
    assert_eq!(
        guest_l2_ingress_policy_script(),
        r#"add table netdev sandbox_l2
add chain netdev sandbox_l2 ingress { type filter hook ingress device "tap0" priority -500; }
flush chain netdev sandbox_l2 ingress
add rule netdev sandbox_l2 ingress meta protocol { 0x8100, 0x88a8 } counter drop comment "drop-guest-vlan"
add rule netdev sandbox_l2 ingress ether type { 0x8100, 0x88a8 } counter drop comment "drop-guest-vlan"
add rule netdev sandbox_l2 ingress meta protocol { 0x0800, 0x0806 } meta pkttype other counter meta pkttype set host comment "accept-stale-gateway-mac"
"#
    );
}

#[test]
fn guest_l2_ingress_validator_accepts_live_nft_versions_and_checks_native_binding() {
    for (json, native) in [
        (
            include_str!("../fixtures/guest_l2_ingress_nft_1_0_4.json"),
            include_str!("../fixtures/guest_l2_ingress_nft_1_0_4.nft"),
        ),
        (
            include_str!("../fixtures/guest_l2_ingress_nft_1_0_9.json"),
            include_str!("../fixtures/guest_l2_ingress_nft_1_0_9.nft"),
        ),
        (
            include_str!("../fixtures/guest_l2_ingress_nft_1_1_3.json"),
            include_str!("../fixtures/guest_l2_ingress_nft_1_1_3.nft"),
        ),
    ] {
        let valid: serde_json::Value = serde_json::from_str(json).unwrap();
        validate_guest_l2_ingress_chain(&valid).unwrap();
        validate_guest_l2_ingress_native_binding(native).unwrap();

        for (path, value) in [
            ("/nftables/1/chain/prio", serde_json::json!(0)),
            (
                "/nftables/2/rule/expr/2",
                serde_json::json!({"accept": null}),
            ),
            (
                "/nftables/3/rule/expr/2",
                serde_json::json!({"accept": null}),
            ),
            (
                "/nftables/2/rule/expr/0/match/right",
                serde_json::json!({"set": ["ip", "8021ad"]}),
            ),
            (
                "/nftables/2/rule/expr/1",
                serde_json::json!({"accept": null}),
            ),
            // The stale gateway MAC rule: IPv4 and ARP only, only frames for
            // another host, reclassified as for this one, and no verdict.
            (
                "/nftables/4/rule/expr/0/match/right",
                serde_json::json!({"set": ["ip"]}),
            ),
            (
                "/nftables/4/rule/expr/0/match/right",
                serde_json::json!({"set": ["ip", "arp", "ip6"]}),
            ),
            (
                "/nftables/4/rule/expr/1/match/right",
                serde_json::json!("broadcast"),
            ),
            (
                "/nftables/4/rule/expr/3/mangle/value",
                serde_json::json!("other"),
            ),
            (
                "/nftables/4/rule/expr/3",
                serde_json::json!({"accept": null}),
            ),
            (
                "/nftables/4/rule/comment",
                serde_json::json!("drop-guest-vlan"),
            ),
        ] {
            let mut invalid = valid.clone();
            *invalid.pointer_mut(path).unwrap() = value;
            assert!(validate_guest_l2_ingress_chain(&invalid).is_err(), "{path}");
        }
        for protocols in [
            serde_json::json!([34984, 33024]),
            serde_json::json!(["8021ad", "8021q"]),
        ] {
            let mut equivalent = valid.clone();
            for entry in equivalent["nftables"].as_array_mut().unwrap() {
                if let Some(rule) = entry.get_mut("rule")
                    && rule["comment"] == "drop-guest-vlan"
                {
                    rule["expr"][0]["match"]["right"]["set"] = protocols.clone();
                }
            }
            validate_guest_l2_ingress_chain(&equivalent).unwrap();
        }
        // nft may print the stale gateway MAC rule's values as numbers.
        let mut numeric = valid.clone();
        numeric["nftables"][4]["rule"]["expr"][0]["match"]["right"]["set"] =
            serde_json::json!([2054, 2048]);
        numeric["nftables"][4]["rule"]["expr"][1]["match"]["right"] = serde_json::json!(3);
        numeric["nftables"][4]["rule"]["expr"][3]["mangle"]["value"] = serde_json::json!(0);
        validate_guest_l2_ingress_chain(&numeric).unwrap();
        // The VLAN drops must come first.
        let mut reordered = valid.clone();
        reordered["nftables"].as_array_mut().unwrap().swap(2, 4);
        assert!(validate_guest_l2_ingress_chain(&reordered).is_err());
        let mut missing_rule = valid;
        missing_rule["nftables"].as_array_mut().unwrap().pop();
        assert!(validate_guest_l2_ingress_chain(&missing_rule).is_err());

        for invalid in [
            String::new(),
            "comment \"device tap0\"".to_string(),
            native.replace("tap0", "vpeer"),
            native.replace("-500", "0"),
        ] {
            assert!(validate_guest_l2_ingress_native_binding(&invalid).is_err());
        }
    }
}

fn udp443_validation_fixture() -> (serde_json::Value, Vec<serde_json::Value>) {
    let chain = serde_json::from_str(include_str!(
        "../fixtures/live_pre_tproxy_guardrails_chain.json"
    ))
    .unwrap();
    let packaged =
        serde_json::from_str(&format!("[{}]", packaged_pre_tproxy_guardrails_policy())).unwrap();
    (chain, packaged)
}

#[test]
fn udp443_guardrail_accepts_live_counters_and_equivalent_ct_state() {
    let (mut chain, packaged) = udp443_validation_fixture();
    let rule = chain["nftables"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find_map(|entry| (entry["rule"]["comment"] == UDP443_COMMENT).then(|| &mut entry["rule"]))
        .unwrap();
    for expr in rule["expr"].as_array_mut().unwrap() {
        if expr.get("counter").is_some() {
            expr["counter"] = serde_json::json!({"packets": 7, "bytes": 280});
        }
    }
    validate_pre_tproxy_guardrails_chain(&chain, "169.254.0.21", &packaged).unwrap();

    for state in [
        serde_json::json!("new"),
        serde_json::json!({"set": ["new"]}),
    ] {
        let mut equivalent = chain.clone();
        for entry in equivalent["nftables"].as_array_mut().unwrap() {
            if entry["rule"]["comment"] != UDP443_COMMENT {
                continue;
            }
            for expr in entry["rule"]["expr"].as_array_mut().unwrap() {
                if expr.pointer("/match/left/ct/key") == Some(&serde_json::json!("state")) {
                    expr["match"]["right"] = state.clone();
                }
            }
        }
        validate_pre_tproxy_guardrails_chain(&equivalent, "169.254.0.21", &packaged).unwrap();
    }
}

#[test]
fn udp443_guardrail_rejects_extra_verdict_duplicate_and_missing_rule() {
    let (chain, packaged) = udp443_validation_fixture();
    for defect in ["extra reject", "duplicate", "missing"] {
        let mut stale = chain.clone();
        let entries = stale["nftables"].as_array_mut().unwrap();
        let index = entries
            .iter()
            .position(|entry| entry["rule"]["comment"] == UDP443_COMMENT)
            .unwrap();
        match defect {
            "extra reject" => entries[index]["rule"]["expr"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"reject": {"type": "icmp", "expr": "port-unreachable"}})),
            "duplicate" => entries.push(entries[index].clone()),
            _ => {
                entries.remove(index);
            }
        }
        assert!(
            matches!(
                validate_pre_tproxy_guardrails_chain(&stale, "169.254.0.21", &packaged),
                Err(FirewallValidationError::Missing(_))
            ),
            "{defect}"
        );
    }
}

/// A malformed packaged policy is a host fault: it must keep the parse error as
/// its source so an operator gets the line and column, and it must not be
/// reported as namespace drift, which would rebuild a healthy namespace.
#[test]
fn malformed_packaged_policy_is_a_typed_host_fault_that_keeps_its_source() {
    use std::error::Error;

    let parse = serde_json::from_str::<Vec<serde_json::Value>>("[{\"add\":]")
        .expect_err("a trailing comma must not parse");
    let error = FirewallValidationError::from(PackagedPolicyError(parse));

    assert!(
        matches!(error, FirewallValidationError::PackagedPolicy(_)),
        "a parse failure must not be reported as missing live state: {error:?}"
    );
    let source = error
        .source()
        .expect("the parse error is kept as the source");
    assert!(
        source.is::<serde_json::Error>(),
        "the source must stay a serde_json::Error, not a flattened string"
    );
    assert!(
        error
            .to_string()
            .contains("sandbox-pre-tproxy-guardrails.json"),
        "the message must name the file an operator has to fix: {error}"
    );
}

#[test]
fn udp443_guardrail_requires_source_validation_and_packaged_policy() {
    let (chain, packaged) = udp443_validation_fixture();
    validate_pre_tproxy_guardrails_chain(&chain, "169.254.0.21", &packaged).unwrap();
    for defect in ["observe-only", "before source", "before return"] {
        let mut stale = chain.clone();
        let entries = stale["nftables"].as_array_mut().unwrap();
        let index = entries
            .iter()
            .position(|entry| entry["rule"]["comment"] == UDP443_COMMENT)
            .unwrap();
        let mut rule = entries.remove(index);
        match defect {
            "observe-only" => {
                rule["rule"]["expr"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|expr| expr.get("reject").is_none());
                entries.insert(index, rule);
            }
            "before source" => entries.insert(0, rule),
            "before return" => entries.insert(index - 1, rule),
            _ => {}
        }
        let result = validate_pre_tproxy_guardrails_chain(&stale, "169.254.0.21", &packaged);
        assert_eq!(
            result.is_ok(),
            defect == "before return",
            "{defect}: {result:?}"
        );
    }
}

#[test]
fn dhcp_bypass_is_owned_by_each_atomic_gateway_ruleset() {
    for dhcp in [false, true] {
        let generated: serde_json::Value =
            serde_json::from_str(&netgtw_tproxy_rules_json(12345, dhcp)).unwrap();
        let commands = generated["nftables"].as_array().unwrap();
        let bypass = commands
            .iter()
            .position(|entry| entry["add"]["rule"]["comment"] == "execution-local-dhcp");
        assert_eq!(bypass.is_some(), dhcp);
        if let Some(bypass) = bypass {
            let last_flush = commands
                .iter()
                .rposition(|entry| entry.get("flush").is_some())
                .unwrap();
            let first_tproxy = commands
                .iter()
                .position(|entry| {
                    entry["add"]["rule"]["expr"]
                        .as_array()
                        .is_some_and(|expr| expr.iter().any(|e| e.get("tproxy").is_some()))
                })
                .unwrap();
            assert!(last_flush < bypass && bypass < first_tproxy);
            assert_eq!(commands[bypass]["add"]["rule"], guest_dhcp_proxy_rule());
        }
    }
}

#[test]
fn dhcp_reconcile_accepts_nft_listing_and_repairs_old_or_duplicate_rules() {
    // nft's listed form omits the implied l4proto match and may reorder sets.
    let installed = serde_json::json!({"rule": {
        "family":"ip", "table":"sandbox", "chain":"pre_tproxy_guardrails",
        "handle":42, "comment":"execution-dhcp-bootstrap", "expr":[
            {"match":{"left":{"meta":{"key":"iifname"}},"op":"==","right":"tap0"}},
            {"match":{"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"in","right":{"set":["169.254.0.22","255.255.255.255"]}}},
            {"match":{"left":{"payload":{"protocol":"udp","field":"sport"}},"op":"==","right":68}},
            {"match":{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":67}},
            {"return":null}
        ]
    }});
    let unrelated = serde_json::json!({"rule":{"handle":99,"comment":"drop-spoofed-guest-source","expr":[{"drop":null}]}});
    let valid = serde_json::json!({"nftables":[installed.clone(),unrelated.clone()]});
    assert!(
        guest_dhcp_bootstrap_updates(Some(&valid))
            .unwrap()
            .is_empty()
    );
    for mode in 0..3 {
        let mut chain = valid.clone();
        match mode {
            0 => {
                chain["nftables"][0]["rule"]["expr"][1]["match"]["right"] =
                    serde_json::json!("255.255.255.255");
            }
            1 => {
                let mut duplicate = installed.clone();
                duplicate["rule"]["handle"] = 43.into();
                chain["nftables"].as_array_mut().unwrap().push(duplicate);
            }
            2 => {
                chain["nftables"].as_array_mut().unwrap().swap(0, 1);
            }
            _ => unreachable!(),
        }
        let updates = guest_dhcp_bootstrap_updates(Some(&chain)).unwrap();
        assert_eq!(updates.len(), if mode == 1 { 3 } else { 2 });
        assert!(updates.iter().all(|update| update.pointer("/delete/rule/handle") != Some(&serde_json::json!(99))));
        assert!(updates.last().unwrap().get("insert").is_some());
    }
    assert_eq!(guest_dhcp_bootstrap_updates(None).unwrap().len(), 1);
}
