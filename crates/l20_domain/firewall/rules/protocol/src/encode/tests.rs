use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use pplx_space_firewall_rules::{
    FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix, Match,
    PayloadHeader, Read, Rule, RuleId, TcpOptionName, Value,
};

use super::*;
use crate::decode_revision;

/// Names that are valid but need escaping or look like syntax: quotes,
/// backslashes, a slash, and spaces where the rule allows them.
fn adversarial_header() -> PayloadHeader {
    PayloadHeader::new(
        HeaderProtocol::new(r#"say "hi" \ bye"#).unwrap(),
        HeaderField::new(r#" {"field":1} "#).unwrap(),
    )
}

/// One rule per read kind, each compared with every value kind.
fn every_variant() -> FirewallRuleRevision {
    let values = || {
        [
            Value::Integer(u64::MAX),
            Value::Ipv4(Ipv4Addr::new(192, 0, 2, 1)),
            Value::True,
            Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(192, 0, 2, 7), 24).unwrap()),
            Value::Range(IntegerRange::new(0, u64::MAX).unwrap()),
        ]
    };
    let reads = [
        (r#"q"\/x"#, Read::Payload(adversarial_header())),
        (
            r"\\",
            Read::TcpOption(TcpOptionName::new(r#"opt "a" \b"#).unwrap()),
        ),
        (
            "masked",
            Read::MaskedPayload {
                header: adversarial_header(),
                mask: NonZeroU64::MAX,
            },
        ),
    ];
    FirewallRuleRevision::new(
        reads
            .into_iter()
            .map(|(id, read)| {
                let all_of = values()
                    .into_iter()
                    .map(|value| Match::new(read.clone(), value))
                    .collect();
                Rule::new(RuleId::new(id).unwrap(), all_of).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn every_variant_round_trips_through_the_canonical_form() {
    let revision = every_variant();
    let text = encode_revision(&revision);
    let decoded = decode_revision(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
    assert_eq!(decoded, revision, "{text}");
    // Canonical bytes are a fixed point.
    assert_eq!(encode_revision(&decoded), text);
}

#[test]
fn names_are_escaped_as_json_strings() {
    let rule = |id: &str, read: Read| {
        Rule::new(
            RuleId::new(id).unwrap(),
            vec![Match::new(read, Value::True)],
        )
        .unwrap()
    };
    let revision = FirewallRuleRevision::new(vec![
        rule(r#"q"\/x"#, Read::Payload(adversarial_header())),
        rule(
            r"\\",
            Read::TcpOption(TcpOptionName::new(r#"opt "a" \b"#).unwrap()),
        ),
    ])
    .unwrap();
    assert_eq!(
        encode_revision(&revision),
        concat!(
            r#"{"schema_version":1,"rules":["#,
            r#"{"id":"q\"\\/x","all_of":[{"left":{"payload":{"protocol":"say \"hi\" \\ bye","field":" {\"field\":1} "}},"op":"==","right":true}]},"#,
            r#"{"id":"\\\\","all_of":[{"left":{"tcp option":{"name":"opt \"a\" \\b"}},"op":"==","right":true}]}"#,
            r#"]}"#,
        )
    );
}
