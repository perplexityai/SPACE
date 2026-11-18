//! The strict, versioned decoder: serde reads the envelope and the model's
//! comparisons, then the model constructors check rule IDs, non-empty `all_of`
//! and unique IDs.

use pplx_space_firewall_rules_model::{FirewallRuleRevision, Match, Rule, RuleId};

use crate::SCHEMA_VERSION;
use crate::wire::{Object, RevisionV1, RuleV1, Version};

/// A version 1 document as read, before the rule checks.
type DocumentV1 = RevisionV1<Vec<Object<RuleV1<String, Vec<Match>>>>>;

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
    /// A rule whose ID breaks its syntax rule or whose `all_of` is empty, named
    /// by its index and, once it is valid, its ID; or a repeated rule ID.
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

fn decode_v1(revision: DocumentV1) -> Result<FirewallRuleRevision, DecodeError> {
    let rules = revision
        .rules
        .into_iter()
        .enumerate()
        .map(|(index, Object(rule))| decode_rule(index, rule))
        .collect::<Result<_, _>>()?;
    FirewallRuleRevision::new(rules).map_err(|error| DecodeError::Invalid(error.to_string()))
}

fn decode_rule(index: usize, rule: RuleV1<String, Vec<Match>>) -> Result<Rule, DecodeError> {
    let id = RuleId::new(rule.id)
        .map_err(|error| DecodeError::Invalid(format!("rules[{index}]: id {error}")))?;
    let at = format!("rules[{index}] ({:?})", id.as_str());
    Rule::new(id, rule.all_of).map_err(|error| DecodeError::Invalid(format!("{at}: {error}")))
}
