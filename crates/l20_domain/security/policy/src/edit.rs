//! One atomic edit batch applied to an explicit base revision.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::diff::{PolicyDiff, RuleChange};
use crate::model::{PolicyError, PolicyRevision, Rule, RuleId};

/// One requested change to a policy, naming a rule by ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edit {
    /// Appends a rule whose ID the base does not have.
    Add(Rule),
    /// Replaces, in place, the base rule with the same ID.
    Replace(Rule),
    /// Removes the base rule with this ID.
    Remove(RuleId),
}

impl Edit {
    pub fn id(&self) -> &RuleId {
        match self {
            Self::Add(rule) | Self::Replace(rule) => rule.id(),
            Self::Remove(id) => id,
        }
    }

    pub fn kind(&self) -> EditKind {
        match self {
            Self::Add(_) => EditKind::Add,
            Self::Replace(_) => EditKind::Replace,
            Self::Remove(_) => EditKind::Remove,
        }
    }
}

/// Which operation an [`Edit`] requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EditKind {
    Add,
    Replace,
    Remove,
}

impl EditKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Replace => "replace",
            Self::Remove => "remove",
        }
    }
}

impl fmt::Display for EditKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A successfully applied batch: the complete candidate and how it differs
/// from the base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyChange {
    candidate: PolicyRevision,
    diff: PolicyDiff,
}

impl PolicyChange {
    pub fn candidate(&self) -> &PolicyRevision {
        &self.candidate
    }

    pub fn diff(&self) -> &PolicyDiff {
        &self.diff
    }

    pub fn into_parts(self) -> (PolicyRevision, PolicyDiff) {
        (self.candidate, self.diff)
    }
}

/// Why an edit batch was rejected. Edit indexes are positions in the batch.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EditError {
    #[error("edit {index} ({kind}) names rule {id:?}, which edit {first_index} ({first_kind}) already names", id = .id.as_str())]
    ConflictingEdits {
        id: RuleId,
        first_index: usize,
        first_kind: EditKind,
        index: usize,
        kind: EditKind,
    },
    #[error("edit {index} adds rule {id:?}, which the base policy already has", id = .id.as_str())]
    AddExisting { id: RuleId, index: usize },
    #[error("edit {index} replaces rule {id:?}, which the base policy does not have", id = .id.as_str())]
    ReplaceMissing { id: RuleId, index: usize },
    #[error("edit {index} removes rule {id:?}, which the base policy does not have", id = .id.as_str())]
    RemoveMissing { id: RuleId, index: usize },
    #[error("the edited policy is invalid")]
    InvalidCandidate(#[source] PolicyError),
}

impl PolicyRevision {
    /// Applies `edits` to this revision as one operation.
    ///
    /// Edits are checked in batch order and the first failure is returned;
    /// on failure there is no candidate and no partial diff. `self` is only
    /// read. The candidate keeps the base order, with each replaced rule in its
    /// original position and added rules appended in batch order; it is
    /// validated as a complete revision before it is returned. An empty batch
    /// yields an unchanged candidate and an empty diff.
    pub fn apply(&self, edits: &[Edit]) -> Result<PolicyChange, EditError> {
        let present: HashSet<&RuleId> = self.rules().iter().map(Rule::id).collect();
        let mut named: HashMap<&RuleId, (usize, EditKind)> = HashMap::with_capacity(edits.len());
        let mut replacements: HashMap<&RuleId, &Rule> = HashMap::new();
        let mut removals: HashSet<&RuleId> = HashSet::new();
        let mut additions: Vec<&Rule> = Vec::new();

        for (index, edit) in edits.iter().enumerate() {
            let id = edit.id();
            let kind = edit.kind();
            if let Some(&(first_index, first_kind)) = named.get(id) {
                return Err(EditError::ConflictingEdits {
                    id: id.clone(),
                    first_index,
                    first_kind,
                    index,
                    kind,
                });
            }
            named.insert(id, (index, kind));

            match edit {
                Edit::Add(rule) if present.contains(id) => {
                    return Err(EditError::AddExisting {
                        id: rule.id().clone(),
                        index,
                    });
                }
                Edit::Add(rule) => additions.push(rule),
                Edit::Replace(rule) if present.contains(id) => {
                    replacements.insert(id, rule);
                }
                Edit::Replace(rule) => {
                    return Err(EditError::ReplaceMissing {
                        id: rule.id().clone(),
                        index,
                    });
                }
                Edit::Remove(id) if present.contains(id) => {
                    removals.insert(id);
                }
                Edit::Remove(id) => {
                    return Err(EditError::RemoveMissing {
                        id: id.clone(),
                        index,
                    });
                }
            }
        }

        let mut changes = Vec::new();
        let mut rules = Vec::with_capacity(self.rules().len() + additions.len());
        for rule in self.rules() {
            if removals.contains(rule.id()) {
                changes.push(RuleChange::Removed(rule.clone()));
                continue;
            }
            match replacements.get(rule.id()) {
                Some(&replacement) if replacement != rule => {
                    changes.push(RuleChange::Replaced {
                        before: rule.clone(),
                        after: replacement.clone(),
                    });
                    rules.push(replacement.clone());
                }
                _ => rules.push(rule.clone()),
            }
        }
        for &rule in &additions {
            changes.push(RuleChange::Added(rule.clone()));
            rules.push(rule.clone());
        }

        let candidate = PolicyRevision::new(rules).map_err(EditError::InvalidCandidate)?;
        Ok(PolicyChange {
            candidate,
            diff: PolicyDiff::new(changes),
        })
    }
}

#[cfg(test)]
mod tests;
