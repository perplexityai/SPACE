//! Bounded homogeneous-clause matching, independent of inventory enumeration.
use super::*;
use std::collections::BTreeMap;

const SEARCH_BUDGET: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeviceSelectionError {
    #[error("device requirements exceed the admission search budget; narrow alternatives")]
    Complexity,
    #[error("distinct feasible device assignments tie under qualified profile preference")]
    Ambiguous,
}
struct Search<'a> {
    clauses: &'a [DeviceRequirement],
    capacity: BTreeMap<DeviceProfile, u32>,
    preferences: Option<&'a BTreeMap<DeviceProfile, u32>>,
    remaining: usize,
    best: Option<(Vec<u32>, Vec<DeviceProfile>)>,
    ambiguous: bool,
}
impl Search<'_> {
    fn visit(
        &mut self,
        selected: &mut Vec<DeviceProfile>,
        scores: &mut Vec<u32>,
    ) -> Result<bool, DeviceSelectionError> {
        if self.remaining == 0 {
            return Err(DeviceSelectionError::Complexity);
        }
        self.remaining -= 1;
        let Some(clause) = self.clauses.get(selected.len()) else {
            if self.preferences.is_none() {
                return Ok(true);
            }
            match &self.best {
                None => {
                    self.best = Some((scores.clone(), selected.clone()));
                    self.ambiguous = false;
                }
                Some((best, _)) if scores.as_slice() > best.as_slice() => {
                    self.best = Some((scores.clone(), selected.clone()));
                    self.ambiguous = false;
                }
                Some((best, chosen)) if scores == best && selected != chosen => {
                    self.ambiguous = true
                }
                Some(_) => {}
            }
            return Ok(false);
        };
        let count = clause.count();
        for profile in clause.profiles() {
            let rank = match self.preferences {
                Some(preferences) => match preferences.get(profile) {
                    Some(rank) => *rank,
                    None => continue,
                },
                None => 0,
            };
            let available = self.capacity.get(profile).copied().unwrap_or(0);
            if available < count {
                continue;
            }
            self.capacity.insert(profile.clone(), available - count);
            selected.push(profile.clone());
            scores.push(rank);
            let found = self.visit(selected, scores)?;
            selected.pop();
            scores.pop();
            self.capacity.insert(profile.clone(), available);
            if found {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
impl DeviceRequirements {
    /// Preferences must come from qualified host profiles, never from request
    /// order. Higher rank wins; the score vector follows canonical clause order.
    /// Search considers complete assignments, so one greedy choice cannot starve
    /// a later clause. Budget exhaustion is not a claim of insufficient capacity.
    pub fn select_profiles(
        &self,
        capacity: &BTreeMap<DeviceProfile, u32>,
        preferences: &BTreeMap<DeviceProfile, u32>,
    ) -> Result<Option<Vec<DeviceProfile>>, DeviceSelectionError> {
        let mut search = Search {
            clauses: self.entries(),
            capacity: capacity.clone(),
            preferences: Some(preferences),
            remaining: SEARCH_BUDGET,
            best: None,
            ambiguous: false,
        };
        search.visit(&mut vec![], &mut vec![])?;
        if search.ambiguous {
            return Err(DeviceSelectionError::Ambiguous);
        }
        Ok(search.best.map(|(_, selected)| selected))
    }
    /// Whether capacity can satisfy every clause without requiring all supplied
    /// capacity to be consumed. Used to distinguish busy from unusable capacity.
    pub fn can_fit(
        &self,
        capacity: &BTreeMap<DeviceProfile, u32>,
    ) -> Result<bool, DeviceSelectionError> {
        Search {
            clauses: self.entries(),
            capacity: capacity.clone(),
            preferences: None,
            remaining: SEARCH_BUDGET,
            best: None,
            ambiguous: false,
        }
        .visit(&mut vec![], &mut vec![])
    }

    /// Validate acknowledgement coverage without interpreting it as a new
    /// selection decision. Every clause must be homogeneous and consume its
    /// count; extra assigned units are refused.
    pub fn is_satisfied_by(
        &self,
        assigned: &BTreeMap<DeviceProfile, u32>,
    ) -> Result<bool, DeviceSelectionError> {
        if assigned.values().map(|n| u64::from(*n)).sum::<u64>()
            != self
                .entries()
                .iter()
                .map(|c| u64::from(c.count()))
                .sum::<u64>()
        {
            return Ok(false);
        }
        self.can_fit(assigned)
    }
}
