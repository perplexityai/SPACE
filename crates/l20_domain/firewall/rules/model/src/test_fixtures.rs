//! Builders shared by the model and diff tests.

use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use crate::{
    FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix, Match,
    PayloadHeader, Read, Rule, RuleId, TcpOptionName, Value,
};

pub(crate) fn id(value: &str) -> RuleId {
    RuleId::new(value).unwrap()
}

pub(crate) fn header(protocol: &str, field: &str) -> PayloadHeader {
    PayloadHeader {
        protocol: HeaderProtocol::new(protocol).unwrap(),
        field: HeaderField::new(field).unwrap(),
    }
}

pub(crate) fn payload_eq(protocol: &str, field: &str, value: Value) -> Match {
    Match {
        left: Read::Payload(header(protocol, field)),
        right: value,
    }
}

pub(crate) fn rule(name: &str, all_of: Vec<Match>) -> Rule {
    Rule::new(id(name), all_of).unwrap()
}

/// A rule dropping traffic to one destination port.
pub(crate) fn port_rule(name: &str, protocol: &str, port: u64) -> Rule {
    rule(
        name,
        vec![payload_eq(protocol, "dport", Value::Integer(port))],
    )
}

pub(crate) fn revision(rules: Vec<Rule>) -> FirewallRuleRevision {
    FirewallRuleRevision::new(rules).unwrap()
}

fn web_prefix() -> Match {
    payload_eq(
        "ip",
        "daddr",
        Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(198, 51, 100, 0), 24).unwrap()),
    )
}

/// The acceptance base: `[dns, web, metrics]`.
pub(crate) fn acceptance_base() -> FirewallRuleRevision {
    revision(vec![
        port_rule("dns", "udp", 53),
        rule(
            "web",
            vec![web_prefix(), payload_eq("tcp", "dport", Value::Integer(80))],
        ),
        rule(
            "metrics",
            vec![payload_eq(
                "tcp",
                "dport",
                Value::Range(IntegerRange::new(9100, 9199).unwrap()),
            )],
        ),
    ])
}

/// `web` as the acceptance candidate changes it.
pub(crate) fn acceptance_web_prime() -> Rule {
    rule(
        "web",
        vec![
            web_prefix(),
            payload_eq(
                "tcp",
                "dport",
                Value::Range(IntegerRange::new(80, 443).unwrap()),
            ),
        ],
    )
}

pub(crate) fn acceptance_admin() -> Rule {
    rule(
        "admin",
        vec![
            payload_eq("ip", "daddr", Value::Ipv4(Ipv4Addr::new(203, 0, 113, 10))),
            payload_eq("tcp", "dport", Value::Integer(22)),
        ],
    )
}

pub(crate) fn acceptance_probe() -> Rule {
    rule(
        "probe",
        vec![
            Match {
                left: Read::TcpOption(TcpOptionName::new("mptcp").unwrap()),
                right: Value::True,
            },
            Match {
                left: Read::MaskedPayload {
                    header: header("tcp", "flags"),
                    mask: NonZeroU64::new(2).unwrap(),
                },
                right: Value::Integer(2),
            },
        ],
    )
}
