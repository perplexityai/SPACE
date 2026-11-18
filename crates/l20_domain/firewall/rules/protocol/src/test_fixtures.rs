//! Builders and documents shared by the protocol tests.

use pplx_space_firewall_rules::{
    HeaderField, HeaderProtocol, Match, PayloadHeader, Read, Rule, RuleId, Value,
};

use crate::decode_revision;

pub(crate) fn id(value: &str) -> RuleId {
    RuleId::new(value).unwrap()
}

pub(crate) fn header(protocol: &str, field: &str) -> PayloadHeader {
    PayloadHeader::new(
        HeaderProtocol::new(protocol).unwrap(),
        HeaderField::new(field).unwrap(),
    )
}

pub(crate) fn payload_eq(protocol: &str, field: &str, value: Value) -> Match {
    Match::new(Read::Payload(header(protocol, field)), value)
}

/// Decodes one rule object by wrapping it in a single-rule v1 document, so
/// the tests read rules only through the public, versioned entry point.
pub(crate) fn rule_json(rule: &str) -> Rule {
    let document = format!(r#"{{"schema_version":1,"rules":[{rule}]}}"#);
    let mut rules = decode_revision(&document)
        .unwrap_or_else(|error| panic!("{rule}: {error}"))
        .into_rules();
    assert_eq!(rules.len(), 1);
    rules.remove(0)
}

/// Wraps one comparison in a single-rule v1 document.
pub(crate) fn document_with_match(comparison: &str) -> String {
    format!(r#"{{"schema_version":1,"rules":[{{"id":"r","all_of":[{comparison}]}}]}}"#)
}

/// Wraps one left operand in a single-comparison document.
pub(crate) fn document_with_left(left: &str) -> String {
    document_with_match(&format!(r#"{{"left":{left},"op":"==","right":1}}"#))
}

/// Wraps one right operand in a single-comparison document.
pub(crate) fn document_with_right(right: &str) -> String {
    document_with_match(&format!(
        r#"{{"left":{{"payload":{{"protocol":"tcp","field":"dport"}}}},"op":"==","right":{right}}}"#
    ))
}

/// The acceptance base: `[dns, web, metrics]`.
pub(crate) const ACCEPTANCE_BASE: &str = r#"{
  "schema_version": 1,
  "rules": [
    {"id": "dns", "all_of": [
      {"left": {"payload": {"protocol": "udp", "field": "dport"}}, "op": "==", "right": 53}
    ]},
    {"id": "web", "all_of": [
      {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": {"prefix": {"addr": "198.51.100.0", "len": 24}}},
      {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": 80}
    ]},
    {"id": "metrics", "all_of": [
      {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": {"range": [9100, 9199]}}
    ]}
  ]
}"#;

/// The replacement for `web` in the acceptance batch.
pub(crate) const ACCEPTANCE_WEB_PRIME: &str = r#"{"id": "web", "all_of": [
  {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": {"prefix": {"addr": "198.51.100.0", "len": 24}}},
  {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": {"range": [80, 443]}}
]}"#;

pub(crate) const ACCEPTANCE_ADMIN: &str = r#"{"id": "admin", "all_of": [
  {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": "203.0.113.10"},
  {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": 22}
]}"#;

pub(crate) const ACCEPTANCE_PROBE: &str = r#"{"id": "probe", "all_of": [
  {"left": {"tcp option": {"name": "mptcp"}}, "op": "==", "right": true},
  {"left": {"&": [{"payload": {"protocol": "tcp", "field": "flags"}}, 2]}, "op": "==", "right": 2}
]}"#;
