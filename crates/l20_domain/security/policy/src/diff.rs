//! The canonical difference between a base revision and its candidate.

use crate::model::{Rule, RuleId};

/// The rules that differ between a base and its candidate, in canonical order:
/// replaced and removed rules in base order, then added rules in candidate
/// order. Unchanged rules are absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyDiff {
    changes: Vec<RuleChange>,
}

impl PolicyDiff {
    pub(crate) fn new(changes: Vec<RuleChange>) -> Self {
        Self { changes }
    }

    pub fn changes(&self) -> &[RuleChange] {
        &self.changes
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// One rule that differs, with the content on each side that exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleChange {
    Added(Rule),
    Replaced { before: Rule, after: Rule },
    Removed(Rule),
}

impl RuleChange {
    pub fn id(&self) -> &RuleId {
        match self {
            Self::Added(rule) | Self::Removed(rule) => rule.id(),
            Self::Replaced { after, .. } => after.id(),
        }
    }
}
