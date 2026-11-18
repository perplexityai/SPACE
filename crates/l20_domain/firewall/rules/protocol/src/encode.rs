//! The canonical encoder: the model converted to the private wire types.

use pplx_space_firewall_rules::{FirewallRuleRevision, Match, PayloadHeader, Read, Rule, Value};

use crate::SCHEMA_VERSION;
use crate::wire::{
    HeaderV1, MatchV1, Object, Op, PayloadReadV1, PrefixV1, ReadV1, RevisionV1, RuleV1,
    TaggedValueV1, TcpOptionV1, ValueV1,
};

/// Writes `revision` in its canonical form: fields in grammar order, rules and
/// comparisons in their own order, and no whitespace. Decoding the result
/// gives back an equal revision, and equal revisions encode to equal bytes.
pub fn encode_revision(revision: &FirewallRuleRevision) -> String {
    match serde_json::to_string(&RevisionV1::from(revision)) {
        Ok(text) => text,
        // Every map key is a string and no wire type returns an error, so
        // writing to a String cannot fail.
        Err(error) => unreachable!("revision serialization failed: {error}"),
    }
}

impl From<&FirewallRuleRevision> for RevisionV1 {
    fn from(revision: &FirewallRuleRevision) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            rules: revision
                .rules()
                .iter()
                .map(|rule| Object(rule.into()))
                .collect(),
        }
    }
}

impl From<&Rule> for RuleV1 {
    fn from(rule: &Rule) -> Self {
        Self {
            id: rule.id().as_str().to_owned(),
            all_of: rule
                .all_of()
                .iter()
                .map(|comparison| Object(comparison.into()))
                .collect(),
        }
    }
}

impl From<&Match> for MatchV1 {
    fn from(comparison: &Match) -> Self {
        Self {
            left: (&comparison.left).into(),
            op: Op::Eq,
            right: (&comparison.right).into(),
        }
    }
}

impl From<&Read> for ReadV1 {
    fn from(read: &Read) -> Self {
        match read {
            Read::Payload(header) => Self::Payload(Object(header.into())),
            Read::TcpOption(name) => Self::TcpOption(Object(TcpOptionV1 {
                name: name.as_str().to_owned(),
            })),
            Read::MaskedPayload { header, mask } => Self::Masked(
                Object(PayloadReadV1 {
                    payload: Object(header.into()),
                }),
                *mask,
            ),
        }
    }
}

impl From<&PayloadHeader> for HeaderV1 {
    fn from(header: &PayloadHeader) -> Self {
        Self {
            protocol: header.protocol.as_str().to_owned(),
            field: header.field.as_str().to_owned(),
        }
    }
}

impl From<&Value> for ValueV1 {
    fn from(value: &Value) -> Self {
        match value {
            Value::Integer(value) => Self::Integer(*value),
            Value::Ipv4(addr) => Self::Ipv4(*addr),
            Value::True => Self::Bool(true),
            Value::Ipv4Prefix(prefix) => Self::Tagged(TaggedValueV1::Prefix(Object(PrefixV1 {
                addr: prefix.addr(),
                len: prefix.prefix_len(),
            }))),
            Value::Range(range) => Self::Tagged(TaggedValueV1::Range(range.start(), range.end())),
        }
    }
}
