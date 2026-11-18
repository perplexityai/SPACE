//! The canonical difference between a base revision and its candidate.

use std::collections::{HashMap, HashSet};

use crate::model::{FirewallRuleRevision, Rule, RuleId};

/// The rules that differ between a base and its candidate, in canonical order:
/// replaced and removed rules in base order, then added rules in candidate
/// order. Unchanged rules are absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleRevisionDiff {
    changes: Vec<RuleChange>,
}

impl RuleRevisionDiff {
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

/// Compares two revisions, matching rules by ID. Rule order is not compared,
/// so a pure reorder yields an empty diff.
pub fn diff(base: &FirewallRuleRevision, candidate: &FirewallRuleRevision) -> RuleRevisionDiff {
    let in_base: HashSet<&RuleId> = base.rules().iter().map(Rule::id).collect();
    let in_candidate: HashMap<&RuleId, &Rule> = candidate
        .rules()
        .iter()
        .map(|rule| (rule.id(), rule))
        .collect();

    let mut changes = Vec::new();
    for before in base.rules() {
        match in_candidate.get(before.id()) {
            None => changes.push(RuleChange::Removed(before.clone())),
            Some(&after) if after != before => changes.push(RuleChange::Replaced {
                before: before.clone(),
                after: after.clone(),
            }),
            Some(_) => {}
        }
    }
    for after in candidate.rules() {
        if !in_base.contains(after.id()) {
            changes.push(RuleChange::Added(after.clone()));
        }
    }
    RuleRevisionDiff { changes }
}

#[cfg(test)]
mod tests;
