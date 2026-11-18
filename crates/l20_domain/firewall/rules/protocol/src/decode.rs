//! The strict, versioned decoder: serde checks the grammar, then the
//! conversion rejects `false` and the model constructors check names,
//! prefixes, ranges and rule IDs.

use pplx_space_firewall_rules_model::{
    FirewallRuleRevision, IntegerRange, Ipv4Prefix, Match, Name, NameError, PayloadHeader,
    PrefixLengthError, Read, ReversedRangeError, Rule, RuleId, Value,
};

use crate::SCHEMA_VERSION;
use crate::wire::{
    HeaderV1, MatchV1, Object, PayloadReadV1, ReadV1, RevisionV1, RuleV1, TaggedValueV1,
    TcpOptionV1, ValueV1, Version,
};

/// Decodes one complete revision document.
pub fn decode_revision(text: &str) -> Result<FirewallRuleRevision, DecodeError> {
    let Object(version) = serde_json::from_str::<Object<Version>>(text)?;
    check_version(version)?;
    let Object(revision) = serde_json::from_str(text)?;
    decode_v1(revision)
}

/// A firewall rules document that does not decode.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Malformed JSON, or JSON that breaks the grammar. serde_json reports
    /// the line and column.
    #[error("invalid firewall rules document: {0}")]
    Json(#[from] serde_json::Error),
    /// A `schema_version` this crate does not read, whatever else the document
    /// holds.
    #[error("unsupported schema version {0}; only {SCHEMA_VERSION} is supported")]
    UnsupportedSchemaVersion(u64),
    /// A rule that fails a model check or compares with `false`, named by its
    /// index and, once it is valid, its ID. A repeated ID is named by the ID.
    #[error("invalid firewall rules: {0}")]
    Invalid(String),
}

/// The version is read before the grammar is checked, so a newer document is
/// reported as an unsupported version, not as unknown fields.
fn check_version(version: Version) -> Result<(), DecodeError> {
    match version.schema_version {
        SCHEMA_VERSION => Ok(()),
        found => Err(DecodeError::UnsupportedSchemaVersion(found)),
    }
}

fn decode_v1(revision: RevisionV1) -> Result<FirewallRuleRevision, DecodeError> {
    let rules = revision
        .rules
        .into_iter()
        .enumerate()
        .map(|(index, Object(rule))| decode_rule(index, rule))
        .collect::<Result<_, _>>()?;
    FirewallRuleRevision::new(rules).map_err(|error| DecodeError::Invalid(error.to_string()))
}

fn decode_rule(index: usize, rule: RuleV1) -> Result<Rule, DecodeError> {
    let id = RuleId::new(rule.id)
        .map_err(|error| DecodeError::Invalid(format!("rules[{index}]: id {error}")))?;
    let at = format!("rules[{index}] ({:?})", id.as_str());
    let invalid = |error: &dyn std::fmt::Display| DecodeError::Invalid(format!("{at}: {error}"));
    let all_of = rule
        .all_of
        .into_iter()
        .map(|Object(comparison)| Match::try_from(comparison))
        .collect::<Result<_, _>>()
        .map_err(|error| invalid(&error))?;
    Rule::new(id, all_of).map_err(|error| invalid(&error))
}

/// Why a comparison does not convert to the model. The rule adds where.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidMatch {
    #[error("{field} {source}")]
    Name {
        field: &'static str,
        source: NameError,
    },
    #[error(transparent)]
    PrefixLength(#[from] PrefixLengthError),
    #[error(transparent)]
    ReversedRange(#[from] ReversedRangeError),
    #[error("false is not a value; only true is")]
    False,
}

impl TryFrom<MatchV1> for Match {
    type Error = InvalidMatch;

    fn try_from(comparison: MatchV1) -> Result<Self, Self::Error> {
        Ok(Match {
            left: comparison.left.try_into()?,
            right: comparison.right.try_into()?,
        })
    }
}

impl TryFrom<ReadV1> for Read {
    type Error = InvalidMatch;

    fn try_from(read: ReadV1) -> Result<Self, Self::Error> {
        Ok(match read {
            ReadV1::Payload(Object(header)) => Read::Payload(header.try_into()?),
            ReadV1::TcpOption(Object(TcpOptionV1 { name })) => {
                Read::TcpOption(name_of("tcp option name", name)?)
            }
            ReadV1::Masked(
                Object(PayloadReadV1 {
                    payload: Object(header),
                }),
                mask,
            ) => Read::MaskedPayload {
                header: header.try_into()?,
                mask,
            },
        })
    }
}

impl TryFrom<HeaderV1> for PayloadHeader {
    type Error = InvalidMatch;

    fn try_from(header: HeaderV1) -> Result<Self, Self::Error> {
        Ok(PayloadHeader {
            protocol: name_of("protocol", header.protocol)?,
            field: name_of("field", header.field)?,
        })
    }
}

/// Checks a name, saying which one it is: the model's error does not.
fn name_of(field: &'static str, value: String) -> Result<Name, InvalidMatch> {
    Name::new(value).map_err(|source| InvalidMatch::Name { field, source })
}

impl TryFrom<ValueV1> for Value {
    type Error = InvalidMatch;

    fn try_from(value: ValueV1) -> Result<Self, Self::Error> {
        Ok(match value {
            ValueV1::Integer(value) => Value::Integer(value),
            ValueV1::Ipv4(addr) => Value::Ipv4(addr),
            ValueV1::Bool(true) => Value::True,
            ValueV1::Bool(false) => return Err(InvalidMatch::False),
            ValueV1::Tagged(TaggedValueV1::Prefix(Object(prefix))) => {
                Value::Ipv4Prefix(Ipv4Prefix::new(prefix.addr, prefix.len)?)
            }
            ValueV1::Tagged(TaggedValueV1::Range(start, end)) => {
                Value::Range(IntegerRange::new(start, end)?)
            }
        })
    }
}
