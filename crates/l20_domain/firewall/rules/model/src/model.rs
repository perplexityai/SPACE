//! The typed firewall rules: revisions, rules, comparisons, reads and values.

use std::collections::HashSet;
use std::fmt;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};

use crate::shape::{self, Comparison, MapOnly, Op};

/// The longest rule ID or name, in bytes.
const MAX_NAME_LEN: usize = 64;

/// The stable identity of a rule within a revision, by which a diff matches
/// rules: 1 to 64 bytes, an ASCII letter or digit and then ASCII letters,
/// digits, `.`, `_` or `-` (`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`), kept as
/// written. Loosening this later still accepts stored revisions; tightening
/// would reject some.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuleId(String);

impl RuleId {
    pub fn new(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        validate(
            &value,
            |c| c.is_ascii_alphanumeric(),
            |c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'),
        )?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A protocol, header field or TCP option name, such as `tcp`, `dport` or
/// `sack-perm`: 1 to 64 bytes, a lowercase ASCII letter or digit and then
/// lowercase ASCII letters, digits, `_` or `-` (`[a-z0-9][a-z0-9_-]{0,63}`).
/// Runtime qualification decides whether a valid name means anything.
/// Loosening this later still accepts stored revisions; tightening would
/// reject some.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Name(String);

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Name::new(value.as_str())
            .map_err(|error| de::Error::custom(format_args!("name {value:?} {error}")))
    }
}

impl Name {
    pub fn new(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        fn lower_or_digit(c: char) -> bool {
            c.is_ascii_lowercase() || c.is_ascii_digit()
        }
        validate(&value, lower_or_digit, |c| {
            lower_or_digit(c) || matches!(c, '_' | '-')
        })?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// The syntax rules admit no space, quote, backslash, control or non-ASCII
// character, so a rule ID or name written raw prints exactly as it is and two
// different ones never print alike.
impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate(value: &str, first: fn(char) -> bool, rest: fn(char) -> bool) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty);
    }
    if value.len() > MAX_NAME_LEN {
        return Err(NameError::TooLong { len: value.len() });
    }
    let refused = value
        .char_indices()
        .find(|&(index, c)| !if index == 0 { first(c) } else { rest(c) });
    match refused {
        Some((index, found)) => Err(NameError::InvalidCharacter { index, found }),
        None => Ok(()),
    }
}

/// A rule ID or name that breaks its syntax rule. The messages do not say which
/// kind of name it was; the caller adds that.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("is empty")]
    Empty,
    #[error("is {len} bytes, more than {MAX_NAME_LEN}")]
    TooLong { len: usize },
    /// `index` is the byte offset of the first character the rule refuses.
    #[error("holds {found:?} at byte {index}, which its syntax rule refuses")]
    InvalidCharacter { index: usize, found: char },
}

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
}

fn validate_unique_ids(rules: &[Rule]) -> Result<(), RuleRevisionError> {
    let mut seen = HashSet::with_capacity(rules.len());
    match rules.iter().find(|rule| !seen.insert(rule.id())) {
        Some(rule) => Err(RuleRevisionError::DuplicateRuleId {
            id: rule.id().clone(),
        }),
        None => Ok(()),
    }
}

/// A rule list that cannot form a revision.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuleRevisionError {
    #[error("rule ID {id:?} appears more than once", id = .id.as_str())]
    DuplicateRuleId { id: RuleId },
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
            return Err(EmptyAllOfError);
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

/// A rule was given no comparisons, which would match every packet.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("all_of must contain at least one comparison")]
pub struct EmptyAllOfError;

/// An equality comparison between a packet read and a value. Equality is the
/// only operator the grammar allows.
///
/// Any read may be paired with any value here; whether the pair makes sense for
/// the field is runtime qualification. It serializes as the `{left, op, right}`
/// object an nft JSON `match` statement holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    pub left: Read,
    pub right: Value,
}

impl Serialize for Match {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Comparison {
            left: &self.left,
            op: Op::Eq,
            right: &self.right,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Match {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let Comparison {
            left,
            op: Op::Eq,
            right,
        } = Comparison::deserialize(MapOnly(deserializer))?;
        Ok(Match { left, right })
    }
}

/// The packet data a comparison reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Read {
    /// A payload header field.
    #[serde(rename = "payload")]
    Payload(PayloadHeader),
    /// A TCP option.
    #[serde(rename = "tcp option", with = "shape::tcp_option")]
    TcpOption(Name),
    /// A payload header field ANDed with a nonzero mask.
    #[serde(rename = "&", with = "shape::masked")]
    MaskedPayload {
        header: PayloadHeader,
        mask: NonZeroU64,
    },
}

/// A protocol header field, named as the caller wrote it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct PayloadHeader {
    pub protocol: Name,
    pub field: Name,
}

impl Serialize for PayloadHeader {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for PayloadHeader {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::deserialize(MapOnly(deserializer))
    }
}

/// The value a read is compared with.
///
/// Values that match the same packets stay distinct: `Integer(80)` is not
/// `Range` 80 to 80, and an address is not its `/32` prefix. Re-encoding a
/// rule from one form to the other is a replacement in a diff.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(expecting = "expected an unsigned integer, an IPv4 address, true, a prefix or a range")]
pub enum Value {
    #[serde(rename = "prefix")]
    Ipv4Prefix(Ipv4Prefix),
    #[serde(rename = "range")]
    Range(IntegerRange),
    #[serde(untagged)]
    Integer(u64),
    #[serde(untagged)]
    Ipv4(Ipv4Addr),
    /// The literal `true`, as in "this TCP option is present". There is no
    /// `false`.
    #[serde(untagged, with = "shape::literal_true")]
    True,
}

/// An IPv4 prefix. The address is kept as written, including host bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Self", deny_unknown_fields)]
pub struct Ipv4Prefix {
    addr: Ipv4Addr,
    len: u8,
}

impl Serialize for Ipv4Prefix {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for Ipv4Prefix {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let read = Self::deserialize(MapOnly(deserializer))?;
        Ipv4Prefix::new(read.addr, read.len).map_err(de::Error::custom)
    }
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

/// An IPv4 prefix length outside `0..=32`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("IPv4 prefix length {len} is outside 0..=32")]
pub struct PrefixLengthError {
    pub len: u8,
}

/// An inclusive integer range whose start does not exceed its end, written
/// `[start, end]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "(u64, u64)")]
pub struct IntegerRange {
    start: u64,
    end: u64,
}

impl Serialize for IntegerRange {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.start, self.end).serialize(serializer)
    }
}

impl TryFrom<(u64, u64)> for IntegerRange {
    type Error = ReversedRangeError;

    fn try_from((start, end): (u64, u64)) -> Result<Self, Self::Error> {
        IntegerRange::new(start, end)
    }
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

#[cfg(test)]
mod tests;
