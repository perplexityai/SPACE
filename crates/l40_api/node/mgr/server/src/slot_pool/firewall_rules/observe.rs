//! Reads what every slot's sandbox namespace enforces now against the accepted
//! assignment, writing nothing to nft. The census pairs the pool's inventory
//! with the directory the host declares only managed namespaces occupy; a host
//! without one, as `/run/netns` is shared, reports that gap. A census carries
//! at most [`MAX_CENSUS_GAPS`] gaps; beyond that it keeps the first ones in the
//! order below and ends with one [`CensusGap::Truncated`], and is incomplete.
//! Each reading is revalidated after the last one while its namespace is still
//! pinned, since a released namespace's inode is soon reused.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use uuid::Uuid;

use super::{
    Live, NamespaceIdentity, NotRunnable, SlotHost, SlotTarget, eligible, identity, pin, runnable,
    same_lifecycle,
};
use crate::firewall::assignment::FirewallAssignments;
use crate::firewall::rules::verify::{Verification, verify_with};
use crate::firewall::rules::{Nft, nft};
use crate::slot_pool::{NetworkPool, SlotRecord};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirewallObservation {
    pub node_id: NodeId,
    pub generation: u64,
    pub revision_id: Uuid,
    pub census: Census,
    /// One per inventory record, in slot order.
    pub slots: Vec<SlotEvidence>,
}

/// The most gaps a census carries, counting the [`CensusGap::Truncated`] marker.
/// Twice the largest slot pool, which can never overflow it alone.
pub const MAX_CENSUS_GAPS: usize = 32_768;

/// Complete when every eligible record was read with its namespace listed,
/// and every listed entry is some record's. Gaps come in this order: each
/// eligible record's, by slot; the listing's failure; each extra entry, sorted
/// by path. Past [`MAX_CENSUS_GAPS`], the rest are omitted and counted by a
/// final `Truncated`, so the census is never complete then.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Census {
    pub gaps: Vec<CensusGap>,
}

impl Census {
    pub fn complete(&self) -> bool {
        self.gaps.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CensusGap {
    InventoryFailed,
    /// The host names no directory only managed namespaces occupy.
    Unowned,
    ListingFailed,
    /// An eligible slot has no namespace.
    Missing {
        slot_id: u32,
    },
    /// An eligible slot wasn't read at its final lifecycle.
    Unread {
        slot_id: u32,
    },
    /// A managed namespace no record accounts for. Its path stays local.
    Extra {
        entry: PathBuf,
    },
    /// Ends an overflowing census: this many gaps were left out.
    Truncated {
        omitted: u64,
    },
}

/// Gaps up to the bound, counting what falls past it.
#[derive(Default)]
struct Gaps {
    kept: Vec<CensusGap>,
    omitted: u64,
}

impl Gaps {
    fn push(&mut self, gap: CensusGap) {
        if self.kept.len() < MAX_CENSUS_GAPS - 1 {
            self.kept.push(gap);
        } else {
            self.omitted += 1;
        }
    }

    fn finish(mut self) -> Vec<CensusGap> {
        if self.omitted > 0 {
            self.kept.push(CensusGap::Truncated {
                omitted: self.omitted,
            });
        }
        self.kept
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotEvidence {
    pub slot_id: u32,
    /// The namespace is set once it was pinned.
    pub target: SlotTarget,
    pub outcome: SlotOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotOutcome {
    Verified,
    /// Mismatched while apply work for this target and generation was pending.
    Applying,
    Failed(Failure),
    Unknown(Unknown),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    RulesMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unknown {
    /// Not a slot the apply takes, such as a transitional one.
    Ineligible,
    NamespaceMissing,
    NamespaceUnreadable,
    TableUnreadable,
    /// Read, but not shown unchanged at the end.
    Unconfirmed,
}

/// Why no observation describes the requested generation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Refusal {
    #[error(transparent)]
    Assignment(#[from] NotRunnable),
    #[error("node-manager is shutting down")]
    ShuttingDown,
    #[error("the observation job did not finish")]
    Unfinished,
}

/// An eligible slot the sweep reached, with its namespace pinned if it could be.
struct Reached {
    index: usize,
    record: SlotRecord,
    path: PathBuf,
    pinned: Option<(File, NamespaceIdentity)>,
}

impl NetworkPool {
    /// Observes `generation` if it is still the accepted assignment for `node_id`.
    /// On a live host the census is `Unowned` and no slot is `Applying`.
    pub async fn observe_firewall_rules(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        node_id: NodeId,
        generation: u64,
    ) -> Result<FirewallObservation, Refusal> {
        let (host, nft) = (Arc::new(Live), Arc::new(nft));
        self.observe_firewall_rules_with(assignments, node_id, generation, host, nft, |_, _| false)
            .await
    }

    /// `applying` says whether apply work for a target is pending.
    pub(crate) async fn observe_firewall_rules_with(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        node_id: NodeId,
        generation: u64,
        host: Arc<dyn SlotHost>,
        nft: Arc<Nft<'static>>,
        applying: impl Fn(u32, &SlotTarget) -> bool + Send + 'static,
    ) -> Result<FirewallObservation, Refusal> {
        let (pool, assignments) = (self.clone(), assignments.clone());
        let job = move || {
            let request = (node_id, generation);
            observe_blocking(&pool, &assignments, request, &*host, &*nft, &applying)
        };
        // Nothing is written, so a panic is only a missing answer.
        match self.task_tracker.spawn_blocking(job).await {
            Ok(answer) => answer,
            Err(error) => {
                tracing::debug!(%error, "firewall observation job did not finish");
                Err(Refusal::Unfinished)
            }
        }
    }
}

fn observe_blocking(
    pool: &NetworkPool,
    assignments: &FirewallAssignments,
    (node_id, generation): (NodeId, u64),
    host: &dyn SlotHost,
    nft: &Nft<'_>,
    applying: &dyn Fn(u32, &SlotTarget) -> bool,
) -> Result<FirewallObservation, Refusal> {
    let desired = runnable(assignments.current(), node_id, generation)?;
    let mut observation = FirewallObservation {
        node_id,
        generation,
        revision_id: desired.revision_id,
        census: Census::default(),
        slots: Vec::new(),
    };
    // An unread inventory reads nothing; the final one reports the gap.
    let records = pool.all_records().unwrap_or_default();
    let mut reached = Vec::new();
    for record in records {
        // One job reads every slot, so shutdown is checked between them.
        if pool.shutdown_token.is_cancelled() {
            return Err(Refusal::ShuttingDown);
        }
        let mut target = SlotTarget::of(&record.info);
        let outcome = if eligible(record.state, record.health, &record.info.sandbox_id) {
            let pending = applying(record.info.slot, &target);
            let path = host.namespace_path(&record.info);
            let (outcome, pinned) = read(&desired.revision, &path, nft, pending);
            target.namespace = pinned.as_ref().map(|(_, namespace)| *namespace);
            let index = observation.slots.len();
            reached.push(Reached {
                index,
                record: record.clone(),
                path,
                pinned,
            });
            outcome
        } else {
            SlotOutcome::Unknown(Unknown::Ineligible)
        };
        observation.slots.push(SlotEvidence {
            slot_id: record.info.slot,
            target,
            outcome,
        });
    }
    host.observed();
    finalize(pool, host, &reached, &mut observation);
    // Last, so readings of a superseded generation are never returned.
    runnable(assignments.current(), node_id, generation)?;
    Ok(observation)
}

/// One slot's outcome through the read-only verifier, and the pin to keep.
fn read(
    revision: &FirewallRuleRevision,
    path: &Path,
    nft: &Nft<'_>,
    pending: bool,
) -> (SlotOutcome, Option<(File, NamespaceIdentity)>) {
    let (netns, namespace) = match pin(path) {
        Ok(pinned) => pinned,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return (SlotOutcome::Unknown(Unknown::NamespaceMissing), None);
        }
        Err(error) => {
            tracing::debug!(path = %path.display(), %error, "cannot pin a namespace to observe");
            return (SlotOutcome::Unknown(Unknown::NamespaceUnreadable), None);
        }
    };
    let readback = verify_with(revision, &netns, nft);
    let outcome = match &readback {
        Verification::Match => SlotOutcome::Verified,
        Verification::Inconclusive(_) => SlotOutcome::Unknown(Unknown::TableUnreadable),
        Verification::Mismatch(_) if pending => SlotOutcome::Applying,
        Verification::Mismatch(_) => SlotOutcome::Failed(Failure::RulesMismatch),
    };
    if readback != Verification::Match {
        tracing::debug!(path = %path.display(), ?readback, "observed firewall table differs");
    }
    (outcome, Some((netns, namespace)))
}

/// Judges the census on a final inventory and listing, and leaves a reading
/// only if its slot and namespace are still the ones read.
fn finalize(
    pool: &NetworkPool,
    host: &dyn SlotHost,
    reached: &[Reached],
    observation: &mut FirewallObservation,
) {
    let now = pool.all_records().ok();
    let listed = listing(host);
    for slot in reached {
        let same_record = now
            .iter()
            .flatten()
            .any(|now| same_lifecycle(now, &slot.record));
        let same_namespace = slot.pinned.as_ref().is_some_and(|(_, namespace)| {
            identity(std::fs::metadata(&slot.path)).is_ok_and(|now| now == *namespace)
        });
        if slot.pinned.is_some()
            && !(same_record && same_namespace)
            && let Some(evidence) = observation.slots.get_mut(slot.index)
        {
            evidence.outcome = SlotOutcome::Unknown(Unknown::Unconfirmed);
        }
    }
    let mut gaps = Gaps::default();
    let Some(now) = now else {
        observation.census.gaps = vec![CensusGap::InventoryFailed];
        return;
    };
    let paths: Vec<_> = now
        .iter()
        .map(|record| host.namespace_path(&record.info))
        .collect();
    for (record, path) in now.iter().zip(&paths) {
        if !eligible(record.state, record.health, &record.info.sandbox_id) {
            continue;
        }
        let read = reached
            .iter()
            .find(|slot| same_lifecycle(record, &slot.record));
        let slot_id = record.info.slot;
        let Some(read) = read else {
            gaps.push(CensusGap::Unread { slot_id });
            continue;
        };
        // Gone from the final listing, or outside the declared directory.
        let unlisted = listed.as_ref().is_ok_and(|listed| !listed.contains(path));
        let outcome = observation.slots.get(read.index).map(|slot| slot.outcome);
        if unlisted || outcome == Some(SlotOutcome::Unknown(Unknown::NamespaceMissing)) {
            gaps.push(CensusGap::Missing { slot_id });
        }
    }
    match listed {
        Err(gap) => gaps.push(gap),
        Ok(listed) => {
            let extra = listed.into_iter().filter(|entry| !paths.contains(entry));
            for entry in extra {
                gaps.push(CensusGap::Extra { entry });
            }
        }
    }
    observation.census.gaps = gaps.finish();
}

/// Every entry of the host's managed-namespace directory, sorted.
fn listing(host: &dyn SlotHost) -> Result<Vec<PathBuf>, CensusGap> {
    let dir = host.namespace_dir().ok_or(CensusGap::Unowned)?;
    let entries = std::fs::read_dir(dir).map_err(|_| CensusGap::ListingFailed)?;
    let mut listed = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()
        .map_err(|_| CensusGap::ListingFailed)?;
    listed.sort();
    Ok(listed)
}
