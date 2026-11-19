//! The strict, versioned decoder: serde reads the envelope and the model's
//! comparisons, then the model constructors check rule IDs, non-empty `all_of`
//! and unique IDs.

use pplx_space_firewall_rules_model::{
    EmptyAllOfError, FirewallRuleRevision, Match, NameError, Rule, RuleId, RuleRevisionError,
};

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
    /// The rule at `index` has an ID that breaks its syntax rule.
    #[error("invalid firewall rules: rules[{index}]: id {source}")]
    InvalidRuleId { index: usize, source: NameError },
    /// The rule at `index` has an empty `all_of`.
    #[error("invalid firewall rules: rules[{index}] ({id:?}): {source}", id = .id.as_str())]
    EmptyAllOf {
        index: usize,
        id: RuleId,
        source: EmptyAllOfError,
    },
    /// Valid rules that cannot form a revision, such as a repeated rule ID.
    #[error("invalid firewall rules: {0}")]
    Revision(#[from] RuleRevisionError),
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
    Ok(FirewallRuleRevision::new(rules)?)
}

fn decode_rule(index: usize, rule: RuleV1<String, Vec<Match>>) -> Result<Rule, DecodeError> {
    let id = RuleId::new(rule.id).map_err(|source| DecodeError::InvalidRuleId { index, source })?;
    Rule::new(id.clone(), rule.all_of).map_err(|source| DecodeError::EmptyAllOf {
        index,
        id,
        source,
    })
}
