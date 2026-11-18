//! The canonical JSON encoder.
//!
//! The model types have no `Serialize`: these private wrappers are the only
//! way to write them, so every caller produces the same bytes.

use pplx_space_firewall_rules::{
    FirewallRuleRevision, Ipv4Prefix, Match, PayloadHeader, Read, Rule, Value,
};
use serde::ser::{Serialize, SerializeMap, SerializeStruct, Serializer};

use crate::SCHEMA_VERSION;

/// Writes `revision` in its canonical form: fields in grammar order, rules and
/// comparisons in their own order, and no whitespace. Decoding the result
/// gives back an equal revision, and equal revisions encode to equal bytes.
pub fn encode_revision(revision: &FirewallRuleRevision) -> String {
    match serde_json::to_string(&RevisionJson(revision)) {
        Ok(text) => text,
        // Every map key is a string and no wrapper here returns an error, so
        // writing to a String cannot fail.
        Err(error) => unreachable!("revision serialization failed: {error}"),
    }
}

struct RevisionJson<'a>(&'a FirewallRuleRevision);

impl Serialize for RevisionJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_struct("Revision", 2)?;
        document.serialize_field("schema_version", &SCHEMA_VERSION)?;
        document.serialize_field("rules", &Rules(self.0.rules()))?;
        document.end()
    }
}

struct Rules<'a>(&'a [Rule]);

impl Serialize for Rules<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(RuleJson))
    }
}

struct RuleJson<'a>(&'a Rule);

impl Serialize for RuleJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut rule = serializer.serialize_struct("Rule", 2)?;
        rule.serialize_field("id", self.0.id().as_str())?;
        rule.serialize_field("all_of", &Matches(self.0.all_of()))?;
        rule.end()
    }
}

struct Matches<'a>(&'a [Match]);

impl Serialize for Matches<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(MatchJson))
    }
}

struct MatchJson<'a>(&'a Match);

impl Serialize for MatchJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut comparison = serializer.serialize_struct("Match", 3)?;
        comparison.serialize_field("left", &ReadJson(self.0.left()))?;
        comparison.serialize_field("op", "==")?;
        comparison.serialize_field("right", &ValueJson(self.0.right()))?;
        comparison.end()
    }
}

struct ReadJson<'a>(&'a Read);

impl Serialize for ReadJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Read::Payload(header) => PayloadReadJson(header).serialize(serializer),
            Read::TcpOption(name) => {
                let mut read = serializer.serialize_map(Some(1))?;
                read.serialize_entry("tcp option", &TcpOptionJson(name.as_str()))?;
                read.end()
            }
            Read::MaskedPayload { header, mask } => {
                let mut read = serializer.serialize_map(Some(1))?;
                read.serialize_entry("&", &(PayloadReadJson(header), mask.get()))?;
                read.end()
            }
        }
    }
}

struct PayloadReadJson<'a>(&'a PayloadHeader);

impl Serialize for PayloadReadJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut read = serializer.serialize_map(Some(1))?;
        read.serialize_entry("payload", &HeaderJson(self.0))?;
        read.end()
    }
}

struct HeaderJson<'a>(&'a PayloadHeader);

impl Serialize for HeaderJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut header = serializer.serialize_struct("PayloadHeader", 2)?;
        header.serialize_field("protocol", self.0.protocol().as_str())?;
        header.serialize_field("field", self.0.field().as_str())?;
        header.end()
    }
}

struct TcpOptionJson<'a>(&'a str);

impl Serialize for TcpOptionJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut option = serializer.serialize_struct("TcpOption", 1)?;
        option.serialize_field("name", self.0)?;
        option.end()
    }
}

struct ValueJson<'a>(&'a Value);

impl Serialize for ValueJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Integer(value) => serializer.serialize_u64(*value),
            Value::Ipv4(addr) => serializer.collect_str(addr),
            Value::True => serializer.serialize_bool(true),
            Value::Ipv4Prefix(prefix) => {
                let mut value = serializer.serialize_map(Some(1))?;
                value.serialize_entry("prefix", &PrefixJson(prefix))?;
                value.end()
            }
            Value::Range(range) => {
                let mut value = serializer.serialize_map(Some(1))?;
                value.serialize_entry("range", &[range.start(), range.end()])?;
                value.end()
            }
        }
    }
}

struct PrefixJson<'a>(&'a Ipv4Prefix);

impl Serialize for PrefixJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut prefix = serializer.serialize_struct("Ipv4Prefix", 2)?;
        prefix.serialize_field("addr", &self.0.addr().to_string())?;
        prefix.serialize_field("len", &self.0.prefix_len())?;
        prefix.end()
    }
}

#[cfg(test)]
mod tests;
