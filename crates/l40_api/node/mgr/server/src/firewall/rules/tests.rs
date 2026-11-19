use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::decode_revision;
use serde_json::{Value, json};

use super::render;

fn document(rules: &str) -> String {
    format!(r#"{{"schema_version":1,"rules":[{rules}]}}"#)
}

fn decoded(rules: &str) -> FirewallRuleRevision {
    decode_revision(&document(rules)).unwrap()
}

/// One field of every rendered rule, in output order.
fn rule_fields(revision: &FirewallRuleRevision, field: &str) -> Vec<Value> {
    render(revision).commands()[2..]
        .iter()
        .map(|command| command["add"]["rule"][field].clone())
        .collect()
}

fn table_and_chain() -> [Value; 2] {
    [
        json!({"add": {"table": {"family": "ip", "name": "space_firewall_rules"}}}),
        json!({"add": {"chain": {
            "family": "ip", "table": "space_firewall_rules", "name": "guest_drop",
            "type": "filter", "hook": "prerouting", "prio": -176, "policy": "accept",
        }}}),
    ]
}

// Catches a missing, renamed or misplaced table or chain, or a flush or
// delete slipped into construction.
#[test]
fn empty_revision_builds_only_the_owned_table_and_chain() {
    assert_eq!(render(&decoded("")).commands(), table_and_chain());
}

// Catches a lost guard, verdict or rule identity, or a rule placed outside the
// owned table and chain.
#[test]
fn dns_rule_is_a_guarded_drop_named_by_its_id() {
    let revision = decoded(
        r#"{"id":"block-dns","all_of":[
            {"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#,
    );
    let [table, chain] = table_and_chain();
    let rule = json!({"add": {"rule": {
        "family": "ip", "table": "space_firewall_rules", "chain": "guest_drop",
        "comment": "space-firewall-rule:block-dns",
        "expr": [
            {"match": {"left": {"meta": {"key": "iifname"}}, "op": "==", "right": "tap0"}},
            {"match": {"left": {"payload": {"protocol": "udp", "field": "dport"}}, "op": "==", "right": 53}},
            {"drop": null},
        ],
    }}});
    assert_eq!(render(&revision).commands(), [table, chain, rule]);
}

// Catches a read or value rebuilt wrongly, including a value above u32::MAX
// narrowed. Faithful rendering only; Linux acceptance is not checked here.
#[test]
fn supported_reads_and_values_render_as_submitted() {
    let revision = decoded(
        r#"{"id":"shapes","all_of":[
            {"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":"192.0.2.1"},
            {"left":{"tcp option":{"name":"mptcp"}},"op":"==","right":true},
            {"left":{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18]},"op":"==","right":2},
            {"left":{"payload":{"protocol":"ip","field":"saddr"}},"op":"==","right":{"prefix":{"addr":"198.51.100.0","len":24}}},
            {"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":{"range":[1024,65535]}},
            {"left":{"payload":{"protocol":"tcp","field":"sequence"}},"op":"==","right":18446744073709551615}]}"#,
    );
    let expected = json!([
        {"match": {"left": {"meta": {"key": "iifname"}}, "op": "==", "right": "tap0"}},
        {"match": {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": "192.0.2.1"}},
        {"match": {"left": {"tcp option": {"name": "mptcp"}}, "op": "==", "right": true}},
        {"match": {"left": {"&": [{"payload": {"protocol": "tcp", "field": "flags"}}, 18]}, "op": "==", "right": 2}},
        {"match": {"left": {"payload": {"protocol": "ip", "field": "saddr"}}, "op": "==",
                   "right": {"prefix": {"addr": "198.51.100.0", "len": 24}}}},
        {"match": {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==",
                   "right": {"range": [1024, 65535]}}},
        {"match": {"left": {"payload": {"protocol": "tcp", "field": "sequence"}}, "op": "==",
                   "right": u64::MAX}},
        {"drop": null},
    ]);
    assert_eq!(rule_fields(&revision, "expr"), [expected]);
}

// Catches sorting, deduplication or any order other than the revision's.
#[test]
fn rules_and_comparisons_render_in_revision_order() {
    const DNS: &str = r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#;
    const WEB: &str = r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]}"#;
    let comments = |revision: &FirewallRuleRevision| rule_fields(revision, "comment");
    assert_eq!(
        comments(&decoded(&format!("{DNS},{WEB}"))),
        ["space-firewall-rule:dns", "space-firewall-rule:web"]
    );
    assert_eq!(
        comments(&decoded(&format!("{WEB},{DNS}"))),
        ["space-firewall-rule:web", "space-firewall-rule:dns"]
    );

    let daddr =
        r#"{"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":"192.0.2.1"}"#;
    let dport = r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}"#;
    let matched = |rules: &str| -> Vec<Value> {
        let exprs = rule_fields(&decoded(rules), "expr");
        let expr = exprs[0].as_array().unwrap();
        expr[1..expr.len() - 1].to_vec()
    };
    let daddr_first = matched(&format!(r#"{{"id":"r","all_of":[{daddr},{dport}]}}"#));
    let dport_first = matched(&format!(r#"{{"id":"r","all_of":[{dport},{daddr}]}}"#));
    assert_eq!(daddr_first[0]["match"]["left"]["payload"]["field"], "daddr");
    assert_eq!(
        dport_first,
        daddr_first.into_iter().rev().collect::<Vec<_>>()
    );
}

// Catches a decoder that lets a caller add its own verdict or a nested read,
// which the renderer would otherwise wrap and emit ahead of SPACE's drop.
#[test]
fn verdicts_and_nested_reads_are_refused_before_rendering() {
    let dport = r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}"#;
    for all_of in [
        format!(r#"{dport},{{"accept":null}}"#),
        format!(r#"{dport},{{"drop":null}}"#),
        r#"{"left":{"&":[{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18]},2]},"op":"==","right":2}"#.to_string(),
    ] {
        let rules = format!(r#"{{"id":"r","all_of":[{all_of}]}}"#);
        assert!(decode_revision(&document(&rules)).is_err(), "decoded {rules}");
    }
}
