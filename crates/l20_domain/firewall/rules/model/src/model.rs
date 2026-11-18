//! The typed firewall rules: revisions, rules, comparisons, reads and values.

use std::collections::HashSet;
use std::fmt;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

/// The longest name, in bytes, that any [`NameKind`] allows.
const MAX_NAME_LEN: usize = 64;

/// Declares a name kept byte for byte as the caller wrote it, once it passes
/// its kind's syntax rule. Runtime qualification decides whether a valid name
/// means anything.
macro_rules! name {
    ($(#[$doc:meta])* $name:ident, $kind:expr) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, NameError> {
                let value = value.into();
                $kind.validate(&value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        // The syntax rule admits printable ASCII only, so writing a name raw
        // cannot emit a control character, an escape sequence or an invisible
        // character.
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = NameError;

            fn try_from(value: String) -> Result<Self, NameError> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = NameError;

            fn try_from(value: &str) -> Result<Self, NameError> {
                Self::new(value)
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
    };
}

name!(
    /// The stable identity of a rule within a revision, by which a diff
    /// matches rules.
    RuleId,
    NameKind::RuleId
);
name!(
    /// The protocol of a payload header read, such as `ip` or `tcp`.
    HeaderProtocol,
    NameKind::HeaderProtocol
);
name!(
    /// The field of a payload header read, such as `daddr` or `dport`.
    HeaderField,
    NameKind::HeaderField
);
name!(
    /// The name of a TCP option, such as `mptcp`.
    TcpOptionName,
    NameKind::TcpOptionName
);

/// The kinds of name, each with its syntax rule: 1 to 64 bytes of printable
/// ASCII, 0x21 to 0x7E for a rule ID and 0x20 to 0x7E, which adds the space,
/// for every other name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NameKind {
    RuleId,
    HeaderProtocol,
    HeaderField,
    TcpOptionName,
}

impl NameKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuleId => "rule ID",
            Self::HeaderProtocol => "protocol name",
            Self::HeaderField => "field name",
            Self::TcpOptionName => "TCP option name",
        }
    }

    const fn allows(self, c: char) -> bool {
        match self {
            Self::RuleId => c.is_ascii_graphic(),
            Self::HeaderProtocol | Self::HeaderField | Self::TcpOptionName => {
                c.is_ascii_graphic() || c == ' '
            }
        }
    }

    const fn allowed(self) -> &'static str {
        match self {
            Self::RuleId => "printable ASCII other than space",
            Self::HeaderProtocol | Self::HeaderField | Self::TcpOptionName => "printable ASCII",
        }
    }

    fn validate(self, value: &str) -> Result<(), NameError> {
        if value.is_empty() {
            return Err(NameError::Empty { kind: self });
        }
        if value.len() > MAX_NAME_LEN {
            return Err(NameError::TooLong {
                kind: self,
                len: value.len(),
            });
        }
        match value.char_indices().find(|&(_, c)| !self.allows(c)) {
            Some((index, found)) => Err(NameError::InvalidCharacter {
                kind: self,
                index,
                found,
            }),
            None => Ok(()),
        }
    }
}

impl fmt::Display for NameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A name that breaks its kind's syntax rule.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("a {kind} must not be empty")]
    Empty { kind: NameKind },
    #[error("a {kind} is at most {MAX_NAME_LEN} bytes, found {len}")]
    TooLong { kind: NameKind, len: usize },
    /// `index` is the byte offset of the first character the rule refuses.
    #[error("a {kind} may hold only {allowed}, found {found:?} at byte {index}", allowed = .kind.allowed())]
    InvalidCharacter {
        kind: NameKind,
        index: usize,
        found: char,
    },
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
/// the field is runtime qualification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    pub left: Read,
    pub right: Value,
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

/// A protocol header field, named as the caller wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadHeader {
    pub protocol: HeaderProtocol,
    pub field: HeaderField,
}

/// The value a read is compared with.
///
/// Values that match the same packets stay distinct: `Integer(80)` is not
/// `Range` 80 to 80, and an address is not its `/32` prefix. Re-encoding a
/// rule from one form to the other is a replacement in a diff.
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

#[cfg(test)]
mod tests;
