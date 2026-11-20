//! Replaces the owned firewall table in one existing slot's sandbox namespace
//! with the accepted assignment, and reports what a fresh readback showed. A
//! local operation: nothing here schedules, retries or admits traffic.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pplx_space_accounts::NodeId;
use uuid::Uuid;

use super::db::query_record_by_slot;
use super::reset::SlotGuard;
use super::{NetworkPool, SlotHealth, SlotRecord, SlotState, SlotStoreError};
use crate::firewall::assignment::{DesiredAssignment, FirewallAssignments, Unavailable};
use crate::firewall::rules::install::{InstallOutcome, install_with};
use crate::firewall::rules::verify::{Verification, verify_with};
use crate::firewall::rules::{Nft, nft, render};
use crate::net::SlotInfo;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug)]
pub struct SlotFirewallRequest {
    pub node_id: NodeId,
    pub generation: u64,
    pub slot_id: u32,
}

#[derive(Debug)]
pub enum SlotFirewallReport {
    /// Nothing was submitted to nft.
    NotRun(NotRun),
    Attempted(Box<SlotFirewallAttempt>),
}

#[derive(Debug, thiserror::Error)]
pub enum NotRun {
    #[error(transparent)]
    Assignment(#[from] NotRunnable),
    #[error(transparent)]
    Store(#[from] SlotStoreError),
    #[error("the network slot does not exist")]
    NoSlot,
    #[error("the network slot is {state}/{health}, not a healthy ready or leased slot")]
    Ineligible {
        state: SlotState,
        health: SlotHealth,
    },
    #[error("the network slot is owned by other work")]
    Busy,
    #[error("the network slot changed before it was owned")]
    Changed,
    #[error("cannot pin the sandbox network namespace: {0}")]
    Namespace(#[source] io::Error),
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum NotRunnable {
    #[error(transparent)]
    Unavailable(#[from] Unavailable),
    #[error("no firewall assignment is accepted")]
    NoAssignment,
    #[error("the accepted firewall assignment is for node {accepted}")]
    WrongNode { accepted: NodeId },
    #[error("firewall assignment generation {accepted} is accepted instead")]
    NotCurrent { accepted: u64 },
}

/// Why a readback no longer describes what is current.
#[derive(Debug)]
pub enum Stale {
    Assignment(NotRunnable),
    Slot,
    SlotStore(SlotStoreError),
    /// What the slot's namespace entry names now.
    Namespace(io::Result<NamespaceIdentity>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NamespaceIdentity {
    pub device: u64,
    pub inode: u64,
}

/// One submission to one pinned namespace and what it then listed, labelled
/// with the assignment and slot it was made for.
#[derive(Debug)]
pub struct SlotFirewallAttempt {
    pub node_id: NodeId,
    pub assignment_generation: u64,
    pub revision_id: Uuid,
    pub slot_id: u32,
    pub slot_generation: u64,
    pub sandbox_id: Option<String>,
    pub namespace: NamespaceIdentity,
    /// Diagnostic only; `readback` is the evidence.
    pub submitted: InstallOutcome,
    pub readback: Verification,
    pub current: Result<(), Stale>,
}

impl SlotFirewallAttempt {
    /// The pinned namespace listed exactly this revision, and the assignment,
    /// slot and namespace entry were still the attempted ones as of the final
    /// check. It admits no traffic; a later reader revalidates.
    pub fn verified(&self) -> bool {
        self.readback == Verification::Match && self.current.is_ok()
    }
}

/// What the operation reaches beyond the pool and nft; tests substitute it.
pub(super) trait SlotHost: Send + Sync + 'static {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf;
    /// Test seam between the lookup and taking ownership.
    fn looked_up(&self) {}
}

struct Live;

impl SlotHost for Live {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf {
        slot.ns_path().into()
    }
}

/// Exclusive ownership of a slot at the lifecycle it was read at.
struct OwnedSlot {
    record: SlotRecord,
    _guard: SlotGuard,
}

impl NetworkPool {
    /// Installs `request`'s generation, if it is still the accepted one, in
    /// the slot's sandbox namespace and reads it back. A busy, transitional or
    /// changed slot is left alone. `Err` means the job panicked; nft may have
    /// run.
    pub async fn apply_firewall_rules(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        request: SlotFirewallRequest,
    ) -> Result<SlotFirewallReport, tokio::task::JoinError> {
        self.apply_firewall_rules_with(assignments, request, Arc::new(Live), Arc::new(nft))
            .await
    }

    pub(super) async fn apply_firewall_rules_with(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        request: SlotFirewallRequest,
        host: Arc<dyn SlotHost>,
        nft: Arc<Nft<'static>>,
    ) -> Result<SlotFirewallReport, tokio::task::JoinError> {
        let (pool, assignments) = (self.clone(), assignments.clone());
        // Ownership is taken and released inside the tracked job, so a busy
        // pool.db cannot block the runtime and dropping this future cannot
        // release the slot while nft can still run.
        self.task_tracker
            .spawn_blocking(move || apply_blocking(&pool, &assignments, request, &*host, &*nft))
            .await
    }

    fn lookup_eligible(&self, slot_id: u32) -> Result<SlotRecord, NotRun> {
        let record = self.record(slot_id)?.ok_or(NotRun::NoSlot)?;
        if !eligible(record.state, record.health, &record.info.sandbox_id) {
            return Err(NotRun::Ineligible {
                state: record.state,
                health: record.health,
            });
        }
        Ok(record)
    }

    /// Never waits: the DB lock is not held while taking the guard, and a
    /// held guard means the slot is busy.
    fn own(self: &Arc<Self>, record: SlotRecord) -> Result<OwnedSlot, NotRun> {
        let guard = SlotGuard::try_acquire(self, record.info.slot).ok_or(NotRun::Busy)?;
        match self.record(record.info.slot)? {
            Some(now) if same_lifecycle(&now, &record) => Ok(OwnedSlot {
                record,
                _guard: guard,
            }),
            _ => Err(NotRun::Changed),
        }
    }

    fn record(&self, slot_id: u32) -> Result<Option<SlotRecord>, SlotStoreError> {
        let conn = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(query_record_by_slot(&conn, slot_id)?)
    }
}

/// A healthy ready slot no sandbox holds, or a healthy leased one a sandbox does.
fn eligible(state: SlotState, health: SlotHealth, sandbox_id: &str) -> bool {
    matches!(
        (state, health, sandbox_id.is_empty()),
        (SlotState::Ready, SlotHealth::Healthy, true)
            | (SlotState::Leased, SlotHealth::Healthy, false)
    )
}

/// The accepted assignment, if it is the one `node` asked for at `generation`.
fn runnable(
    current: Result<Option<Arc<DesiredAssignment>>, Unavailable>,
    node: NodeId,
    generation: u64,
) -> Result<Arc<DesiredAssignment>, NotRunnable> {
    let accepted = current?.ok_or(NotRunnable::NoAssignment)?;
    if accepted.node_id != node {
        return Err(NotRunnable::WrongNode {
            accepted: accepted.node_id,
        });
    }
    if accepted.generation != generation {
        return Err(NotRunnable::NotCurrent {
            accepted: accepted.generation,
        });
    }
    Ok(accepted)
}

/// `pid` is published during a lease without changing its lifecycle.
fn same_lifecycle(now: &SlotRecord, owned: &SlotRecord) -> bool {
    let key = |record: &SlotRecord| {
        (
            record.info.slot,
            record.info.generation,
            record.state,
            record.health,
            record.info.sandbox_id.clone(),
        )
    };
    key(now) == key(owned)
}

fn apply_blocking(
    pool: &Arc<NetworkPool>,
    assignments: &FirewallAssignments,
    request: SlotFirewallRequest,
    host: &dyn SlotHost,
    nft: &Nft<'_>,
) -> SlotFirewallReport {
    let owned = pool.lookup_eligible(request.slot_id).and_then(|record| {
        host.looked_up();
        pool.own(record)
    });
    let owned = match owned {
        Ok(owned) => owned,
        Err(not_run) => return SlotFirewallReport::NotRun(not_run),
    };
    let path = host.namespace_path(&owned.record.info);
    let (netns, namespace) = match pin(&path) {
        Ok(pinned) => pinned,
        Err(error) => return SlotFirewallReport::NotRun(NotRun::Namespace(error)),
    };
    // Taken under ownership, so an invocation queued behind a newer one
    // cannot reinstall an older revision.
    let desired = match runnable(assignments.current(), request.node_id, request.generation) {
        Ok(desired) => desired,
        Err(not_runnable) => return SlotFirewallReport::NotRun(not_runnable.into()),
    };
    let submitted = install_with(&render(&desired.revision), &netns, nft);
    let readback = verify_with(&desired.revision, &netns, nft);
    let current = still_current(pool, assignments, &desired, &owned.record, &path, namespace);
    let info = &owned.record.info;
    SlotFirewallReport::Attempted(Box::new(SlotFirewallAttempt {
        node_id: desired.node_id,
        assignment_generation: desired.generation,
        revision_id: desired.revision_id,
        slot_id: info.slot,
        slot_generation: info.generation,
        sandbox_id: (!info.sandbox_id.is_empty()).then(|| info.sandbox_id.clone()),
        namespace,
        submitted,
        readback,
        current,
    }))
}

fn pin(path: &Path) -> io::Result<(File, NamespaceIdentity)> {
    let netns = File::open(path)?;
    // A deleted namespace's entry is unmounted before it is unlinked; opened
    // in between it is a plain file.
    if nix::sys::statfs::fstatfs(&netns)?.filesystem_type() != nix::sys::statfs::NSFS_MAGIC {
        return Err(io::Error::other(format!(
            "{} is not a mounted namespace",
            path.display()
        )));
    }
    let namespace = identity(netns.metadata())?;
    Ok((netns, namespace))
}

fn identity(metadata: io::Result<std::fs::Metadata>) -> io::Result<NamespaceIdentity> {
    metadata.map(|metadata| NamespaceIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Within one store a generation names one revision, so the generation is
/// enough to tell the assignment apart. It is read last, since the slot read
/// can wait on pool.db.
fn still_current(
    pool: &NetworkPool,
    assignments: &FirewallAssignments,
    desired: &DesiredAssignment,
    owned: &SlotRecord,
    path: &Path,
    namespace: NamespaceIdentity,
) -> Result<(), Stale> {
    match pool.record(owned.info.slot) {
        Ok(Some(now)) if same_lifecycle(&now, owned) => {}
        Ok(_) => return Err(Stale::Slot),
        Err(error) => return Err(Stale::SlotStore(error)),
    }
    match identity(std::fs::metadata(path)) {
        Ok(now) if now == namespace => {}
        now => return Err(Stale::Namespace(now)),
    }
    runnable(assignments.current(), desired.node_id, desired.generation)
        .map_err(Stale::Assignment)?;
    Ok(())
}
