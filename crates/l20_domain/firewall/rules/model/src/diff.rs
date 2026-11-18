//! The canonical difference between a base revision and its candidate.

use std::collections::{HashMap, HashSet};

use crate::model::{FirewallRuleRevision, Rule, RuleId};

/// The rules that differ between a base and its candidate, in canonical order:
/// replaced and removed rules in base order, then added rules in candidate
/// order. Unchanged rules are absent. An addition carries no position; the
/// candidate revision holds the full order.
///
/// Rule order is part of a revision, so the diff also reports whether the
/// rules both revisions hold changed order: a revision is the exact document
/// pushed, and a push that only reorders must not look like no change. Every
/// rule drops, so today a reorder does not change which packets drop; whether
/// a reorder-only diff needs a reconcile is the caller's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleRevisionDiff {
    changes: Vec<RuleChange>,
    reordered: bool,
}

impl RuleRevisionDiff {
    pub fn changes(&self) -> &[RuleChange] {
        &self.changes
    }

    /// Whether the rule IDs present in both revisions appear in a different
    /// order. Additions and removals alone do not set it.
    pub fn reordered(&self) -> bool {
        self.reordered
    }

    /// Whether the revisions hold the same rules in the same order.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && !self.reordered
    }
}

/// One rule that differs, with the content on each side that exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleChange {
    Added(Rule),
    Replaced { before: Rule, after: Rule },
    Removed(Rule),
}

/// Compares two revisions, matching rules by ID.
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

    let shared_in_base = base
        .rules()
        .iter()
        .map(Rule::id)
        .filter(|id| in_candidate.contains_key(id));
    let shared_in_candidate = candidate
        .rules()
        .iter()
        .map(Rule::id)
        .filter(|id| in_base.contains(id));
    let reordered = !shared_in_base.eq(shared_in_candidate);
    RuleRevisionDiff { changes, reordered }
}

#[cfg(test)]
mod tests;
