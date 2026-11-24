//! Reads what every slot's sandbox namespace enforces now against the accepted
//! assignment, writing nothing to nft. The census pairs the pool's inventory
//! with the directory the host declares only managed namespaces occupy; a host
//! without one, as `/run/netns` is shared, reports that gap. A census carries
//! at most [`MAX_CENSUS_GAPS`] gaps; beyond that it keeps the first ones in the
//! order below and ends with one [`CensusGap::Truncated`], and is incomplete.
//! A sweep starts no read past [`SWEEP_BUDGET`], and one read can take two nft
//! probes, so it ends within about 70 s of its start; a slot left then is
//! unread.
//! Each reading is revalidated after the last one while its namespace is still
//! pinned, since a released namespace's inode is soon reused. A sweep so holds
//! one descriptor per eligible slot until its final checks, at most
//! [`MAX_IPV4_NETWORK_SLOTS`] (16,383) at once, counted against the open-file
//! limit node-manager shares with its other work, which nothing here raises. A
//! pin refused for lack of descriptors leaves its slot unread; a slot pinned
//! at the limit can still fail to run nft, and reads `TableUnreadable`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::FirewallRuleRevision;
use uuid::Uuid;

use super::{
    Live, NamespaceIdentity, NotRunnable, SlotHost, SlotTarget, eligible, pin, runnable,
    same_lifecycle, unchanged,
};
use crate::firewall::assignment::FirewallAssignments;
use crate::firewall::rules::verify::{Verification, verify_with};
use crate::firewall::rules::{Nft, nft};
use crate::net::MAX_IPV4_NETWORK_SLOTS;
use crate::slot_pool::{NetworkPool, SlotRecord};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirewallObservation {
    pub node_id: NodeId,
    pub generation: u64,
    pub revision_id: Uuid,
    pub census: Census,
    /// One per first-inventory record, in slot order, then one for each slot
    /// gap whose slot that inventory missed.
    pub slots: Vec<SlotEvidence>,
}

/// The most gaps a census carries, counting the [`CensusGap::Truncated`] marker.
pub const MAX_CENSUS_GAPS: usize = 32_768;

// One gap per slot and the listing's never need truncating.
const _: () = assert!(MAX_IPV4_NETWORK_SLOTS as usize + 1 < MAX_CENSUS_GAPS);

/// How long a sweep keeps starting reads: a healthy pool of the default 300 slots
/// takes a few seconds, and a sweep, even one queued behind another, ends well
/// within five minutes.
pub(crate) const SWEEP_BUDGET: Duration = Duration::from_secs(60);

/// Complete when every eligible record was read at its final lifecycle, its
/// namespace pinned, still the one its entry names and listed, and every
/// listed entry is some record's. Gaps come in this order: each eligible
/// record's, by slot; the listing's failure; each extra entry, sorted by path.
/// Past [`MAX_CENSUS_GAPS`], the rest are omitted and counted by a final
/// `Truncated`, so the census is never complete then. A `Missing` gap's slot
/// reads `NamespaceMissing`, and an `Unread` one's `Unconfirmed` or
/// `NamespaceUnreadable`.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// No reading of an eligible slot stands, though its namespace isn't
    /// known to be missing: none was taken at its final lifecycle, its
    /// namespace couldn't be pinned, or it changed after the read.
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
struct Gaps {
    bound: usize,
    kept: Vec<CensusGap>,
    omitted: u64,
}

impl Gaps {
    /// At most `bound` gaps, the marker included.
    fn new(bound: usize) -> Self {
        Self {
            bound,
            kept: Vec::new(),
            omitted: 0,
        }
    }

    fn push(&mut self, gap: CensusGap) {
        if self.kept.len() + 1 < self.bound {
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
    /// No reading stands: read but not shown unchanged at the end, or not
    /// read before the sweep's deadline.
    Unconfirmed,
}

/// Why no observation describes the requested generation.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Refusal {
    #[error(transparent)]
    Assignment(#[from] NotRunnable),
    #[error("node-manager is shutting down")]
    ShuttingDown,
    /// The job panicked, or never ran.
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
        let request = (node_id, generation);
        let not_applying = |_: u32, _: &SlotTarget| false;
        self.observe_firewall_rules_with(
            assignments,
            request,
            host,
            nft,
            not_applying,
            SWEEP_BUDGET,
        )
        .await
    }

    /// `applying` says whether apply work for a target is pending; `budget`
    /// is how long the sweep keeps starting reads.
    pub(crate) async fn observe_firewall_rules_with(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        request: (NodeId, u64),
        host: Arc<dyn SlotHost>,
        nft: Arc<Nft<'static>>,
        applying: impl Fn(u32, &SlotTarget) -> bool + Send + 'static,
        budget: Duration,
    ) -> Result<FirewallObservation, Refusal> {
        let (pool, assignments) = (self.clone(), assignments.clone());
        let job = move || {
            let deadline = Instant::now() + budget;
            observe_blocking(
                &pool,
                &assignments,
                request,
                &*host,
                &*nft,
                &applying,
                deadline,
            )
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
    deadline: Instant,
) -> Result<FirewallObservation, Refusal> {
    let desired = runnable(assignments.current(), node_id, generation)?;
    // An unread inventory reads nothing; the final one reports the gap.
    let records = pool.all_records().unwrap_or_default();
    let (mut slots, mut reached) = (Vec::with_capacity(records.len()), Vec::new());
    for record in records {
        // One job reads every slot, so shutdown and the deadline are checked
        // between them.
        if pool.shutdown_token.is_cancelled() {
            return Err(Refusal::ShuttingDown);
        }
        let slot_id = record.info.slot;
        let mut target = SlotTarget::of(&record.info);
        let outcome = if !eligible(record.state, record.health, &record.info.sandbox_id) {
            SlotOutcome::Unknown(Unknown::Ineligible)
        } else if Instant::now() >= deadline {
            // Left unreached, so the census calls it unread.
            SlotOutcome::Unknown(Unknown::Unconfirmed)
        } else {
            let path = host.namespace_path(&record.info);
            let pending = || applying(slot_id, &target);
            let (outcome, pinned) = read(&desired.revision, &path, nft, pending);
            target.namespace = pinned.as_ref().map(|(_, namespace)| *namespace);
            reached.push(Reached {
                index: slots.len(),
                record,
                path,
                pinned,
            });
            outcome
        };
        slots.push(SlotEvidence {
            slot_id,
            target,
            outcome,
        });
    }
    host.observed();
    let census = finalize(pool, host, &reached, &mut slots);
    // Last, so readings of a superseded generation are never returned.
    runnable(assignments.current(), node_id, generation)?;
    Ok(FirewallObservation {
        node_id,
        generation,
        revision_id: desired.revision_id,
        census,
        slots,
    })
}

/// One slot's outcome through the read-only verifier, and the pin to keep.
fn read(
    revision: &FirewallRuleRevision,
    path: &Path,
    nft: &Nft<'_>,
    pending: impl Fn() -> bool,
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
        // Asked after the read, so work that finished during it isn't pending.
        Verification::Mismatch(_) if pending() => SlotOutcome::Applying,
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
    slots: &mut Vec<SlotEvidence>,
) -> Census {
    let now = pool.all_records().ok();
    let listed = listing(host);
    let by_slot: HashMap<_, _> = now
        .iter()
        .flatten()
        .map(|record| (record.info.slot, record))
        .collect();
    // Each reading still at its slot's lifecycle, and whether it stands.
    let mut readings = HashMap::with_capacity(reached.len());
    for slot in reached {
        let slot_id = slot.record.info.slot;
        let same_record = by_slot
            .get(&slot_id)
            .is_some_and(|now| same_lifecycle(now, &slot.record));
        let stands = same_record
            && slot
                .pinned
                .as_ref()
                .is_some_and(|(_, namespace)| unchanged(&slot.path, *namespace).is_ok());
        if slot.pinned.is_some()
            && !stands
            && let Some(evidence) = slots.get_mut(slot.index)
        {
            evidence.outcome = SlotOutcome::Unknown(Unknown::Unconfirmed);
        }
        if same_record {
            readings.insert(slot_id, (slot.index, stands));
        }
    }
    let Some(now) = now else {
        return Census {
            gaps: vec![CensusGap::InventoryFailed],
        };
    };
    let mut gaps = Gaps::new(MAX_CENSUS_GAPS);
    let mut entries: HashMap<_, _> = (0..)
        .zip(slots.iter())
        .map(|(i, s)| (s.slot_id, i))
        .collect();
    let paths: Vec<_> = now
        .iter()
        .map(|record| host.namespace_path(&record.info))
        .collect();
    let listed_paths: Option<HashSet<&PathBuf>> =
        listed.as_ref().ok().map(|listed| listed.iter().collect());
    for (record, path) in now.iter().zip(&paths) {
        if !eligible(record.state, record.health, &record.info.sandbox_id) {
            continue;
        }
        let slot_id = record.info.slot;
        let Some(&(index, stands)) = readings.get(&slot_id) else {
            gaps.push(CensusGap::Unread { slot_id });
            account(slots, &mut entries, record, Unknown::Unconfirmed);
            continue;
        };
        // Gone from the final listing, or outside the declared directory.
        let unlisted = listed_paths
            .as_ref()
            .is_some_and(|listed| !listed.contains(path));
        let outcome = slots.get(index).map(|slot| slot.outcome);
        if unlisted || outcome == Some(SlotOutcome::Unknown(Unknown::NamespaceMissing)) {
            gaps.push(CensusGap::Missing { slot_id });
            account(slots, &mut entries, record, Unknown::NamespaceMissing);
        } else if !stands {
            gaps.push(CensusGap::Unread { slot_id });
            account(slots, &mut entries, record, Unknown::Unconfirmed);
        }
    }
    match listed {
        Err(gap) => gaps.push(gap),
        Ok(listed) => {
            let paths: HashSet<_> = paths.iter().collect();
            let extra = listed.into_iter().filter(|entry| !paths.contains(entry));
            for entry in extra {
                gaps.push(CensusGap::Extra { entry });
            }
        }
    }
    Census {
        gaps: gaps.finish(),
    }
}

/// Makes `record`'s slot say why its gap is unknown, at its final lifecycle.
/// An unreadable namespace at that lifecycle already says why it is unread.
fn account(
    slots: &mut Vec<SlotEvidence>,
    entries: &mut HashMap<u32, usize>,
    record: &SlotRecord,
    why: Unknown,
) {
    let (outcome, target) = (SlotOutcome::Unknown(why), SlotTarget::of(&record.info));
    let unreadable = SlotOutcome::Unknown(Unknown::NamespaceUnreadable);
    let lifecycle = |target: &SlotTarget| (target.slot_generation, target.sandbox_id.clone());
    let slot_id = record.info.slot;
    match entries
        .get(&slot_id)
        .and_then(|&index| slots.get_mut(index))
    {
        Some(slot) if lifecycle(&slot.target) == lifecycle(&target) => {
            if why != Unknown::Unconfirmed || slot.outcome != unreadable {
                slot.outcome = outcome;
            }
        }
        Some(slot) => {
            *slot = SlotEvidence {
                slot_id,
                target,
                outcome,
            }
        }
        None => {
            entries.insert(slot_id, slots.len());
            slots.push(SlotEvidence {
                slot_id,
                target,
                outcome,
            });
        }
    }
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
