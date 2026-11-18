//! The typed firewall rules: revisions, rules, comparisons, reads and values.

use std::collections::HashMap;
use std::fmt;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use serde::ser::{SerializeMap, SerializeStruct};
use serde::{Serialize, Serializer};

use crate::wire::SCHEMA_VERSION;

/// Declares an opaque name kept byte for byte as the caller wrote it. The
/// rule grammar gives these names no syntax, so construction cannot fail;
/// runtime qualification decides whether a name means anything.
macro_rules! opaque_name {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }
    };
}

opaque_name!(
    /// The stable identity of a rule within a revision, which edits name.
    RuleId
);
opaque_name!(
    /// The protocol of a payload header read, such as `ip` or `tcp`.
    HeaderProtocol
);
opaque_name!(
    /// The field of a payload header read, such as `daddr` or `dport`.
    HeaderField
);
opaque_name!(
    /// The name of a TCP option, such as `mptcp`.
    TcpOptionName
);

/// One complete snapshot of the firewall rules: an ordered list of rules with
/// unique IDs.
///
/// An empty rule list is a real snapshot. There is deliberately no `Default`:
/// an absent revision is not an empty one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirewallRuleRevision {
    rules: Vec<Rule>,
}

impl FirewallRuleRevision {
    /// Validates a complete rule list, keeping its order.
    pub fn new(rules: Vec<Rule>) -> Result<Self, RuleRevisionError> {
        validate_unique_ids(&rules)?;
        Ok(Self { rules })
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    pub fn rule(&self, id: &RuleId) -> Option<&Rule> {
        self.rules.iter().find(|rule| rule.id() == id)
    }

    pub fn into_rules(self) -> Vec<Rule> {
        self.rules
    }
}

fn validate_unique_ids(rules: &[Rule]) -> Result<(), RuleRevisionError> {
    let mut first_index = HashMap::with_capacity(rules.len());
    for (index, rule) in rules.iter().enumerate() {
        if let Some(&first) = first_index.get(rule.id()) {
            return Err(RuleRevisionError::DuplicateRuleId {
                id: rule.id().clone(),
                first_index: first,
                duplicate_index: index,
            });
        }
        first_index.insert(rule.id(), index);
    }
    Ok(())
}

impl Serialize for FirewallRuleRevision {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_struct("FirewallRuleRevision", 2)?;
        document.serialize_field("schema_version", &SCHEMA_VERSION)?;
        document.serialize_field("rules", &self.rules)?;
        document.end()
    }
}

/// A rule list that cannot form a revision.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuleRevisionError {
    #[error("rule ID {id:?} at index {duplicate_index} repeats the rule at index {first_index}", id = .id.as_str())]
    DuplicateRuleId {
        id: RuleId,
        first_index: usize,
        duplicate_index: usize,
    },
}

/// A rule that drops a packet when every comparison in `all_of` matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    id: RuleId,
    all_of: Vec<Match>,
}

impl Rule {
    /// Builds a rule, keeping comparison order. `all_of` must be nonempty.
    pub fn new(id: RuleId, all_of: Vec<Match>) -> Result<Self, EmptyAllOfError> {
        if all_of.is_empty() {
            return Err(EmptyAllOfError { id });
        }
        Ok(Self { id, all_of })
    }

    pub fn id(&self) -> &RuleId {
        &self.id
    }

    pub fn all_of(&self) -> &[Match] {
        &self.all_of
    }
}

impl Serialize for Rule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut rule = serializer.serialize_struct("Rule", 2)?;
        rule.serialize_field("id", &self.id)?;
        rule.serialize_field("all_of", &self.all_of)?;
        rule.end()
    }
}

/// A rule was given no comparisons, which would match every packet.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("rule {id:?} has an empty all_of", id = .id.as_str())]
pub struct EmptyAllOfError {
    pub id: RuleId,
}

/// An equality comparison between a packet read and a value. Equality is the
/// only operator the grammar allows.
///
/// Any read may be paired with any value here; whether the pair makes sense for
/// the field is runtime qualification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    left: Read,
    right: Value,
}

impl Match {
    pub fn new(left: Read, right: Value) -> Self {
        Self { left, right }
    }

    pub fn left(&self) -> &Read {
        &self.left
    }

    pub fn right(&self) -> &Value {
        &self.right
    }
}

impl Serialize for Match {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut comparison = serializer.serialize_struct("Match", 3)?;
        comparison.serialize_field("left", &self.left)?;
        comparison.serialize_field("op", "==")?;
        comparison.serialize_field("right", &self.right)?;
        comparison.end()
    }
}

/// The packet data a comparison reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read {
    /// A payload header field.
    Payload(PayloadHeader),
    /// A TCP option.
    TcpOption(TcpOptionName),
    /// A payload header field ANDed with a nonzero mask.
    MaskedPayload {
        header: PayloadHeader,
        mask: NonZeroU64,
    },
}

impl Serialize for Read {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Payload(header) => PayloadReadJson(header).serialize(serializer),
            Self::TcpOption(name) => {
                let mut read = serializer.serialize_map(Some(1))?;
                read.serialize_entry("tcp option", &TcpOptionJson(name))?;
                read.end()
            }
            Self::MaskedPayload { header, mask } => {
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
        read.serialize_entry("payload", self.0)?;
        read.end()
    }
}

struct TcpOptionJson<'a>(&'a TcpOptionName);

impl Serialize for TcpOptionJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut option = serializer.serialize_struct("TcpOption", 1)?;
        option.serialize_field("name", self.0)?;
        option.end()
    }
}

/// A protocol header field, named as the caller wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadHeader {
    protocol: HeaderProtocol,
    field: HeaderField,
}

impl PayloadHeader {
    pub fn new(protocol: HeaderProtocol, field: HeaderField) -> Self {
        Self { protocol, field }
    }

    pub fn protocol(&self) -> &HeaderProtocol {
        &self.protocol
    }

    pub fn field(&self) -> &HeaderField {
        &self.field
    }
}

impl Serialize for PayloadHeader {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut header = serializer.serialize_struct("PayloadHeader", 2)?;
        header.serialize_field("protocol", &self.protocol)?;
        header.serialize_field("field", &self.field)?;
        header.end()
    }
}

/// The value a read is compared with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Integer(u64),
    Ipv4(Ipv4Addr),
    /// The literal `true`, as in "this TCP option is present". There is no
    /// `false`.
    True,
    Ipv4Prefix(Ipv4Prefix),
    Range(IntegerRange),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Integer(value) => serializer.serialize_u64(*value),
            Self::Ipv4(addr) => serializer.collect_str(addr),
            Self::True => serializer.serialize_bool(true),
            Self::Ipv4Prefix(prefix) => {
                let mut value = serializer.serialize_map(Some(1))?;
                value.serialize_entry("prefix", prefix)?;
                value.end()
            }
            Self::Range(range) => {
                let mut value = serializer.serialize_map(Some(1))?;
                value.serialize_entry("range", &[range.start, range.end])?;
                value.end()
            }
        }
    }
}

/// An IPv4 prefix. The address is kept as written, including host bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ipv4Prefix {
    addr: Ipv4Addr,
    len: u8,
}

impl Ipv4Prefix {
    pub const MAX_LEN: u8 = 32;

    pub fn new(addr: Ipv4Addr, len: u8) -> Result<Self, PrefixLengthError> {
        if len > Self::MAX_LEN {
            return Err(PrefixLengthError { len });
        }
        Ok(Self { addr, len })
    }

    pub fn addr(&self) -> Ipv4Addr {
        self.addr
    }

    pub fn prefix_len(&self) -> u8 {
        self.len
    }
}

impl Serialize for Ipv4Prefix {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut prefix = serializer.serialize_struct("Ipv4Prefix", 2)?;
        prefix.serialize_field("addr", &self.addr.to_string())?;
        prefix.serialize_field("len", &self.len)?;
        prefix.end()
    }
}

/// An IPv4 prefix length outside `0..=32`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("IPv4 prefix length {len} is outside 0..=32")]
pub struct PrefixLengthError {
    pub len: u8,
}

/// An inclusive integer range whose start does not exceed its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntegerRange {
    start: u64,
    end: u64,
}

impl IntegerRange {
    pub fn new(start: u64, end: u64) -> Result<Self, ReversedRangeError> {
        if start > end {
            return Err(ReversedRangeError { start, end });
        }
        Ok(Self { start, end })
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }
}

/// A range whose start exceeds its end.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("range start {start} exceeds its end {end}")]
pub struct ReversedRangeError {
    pub start: u64,
    pub end: u64,
}
