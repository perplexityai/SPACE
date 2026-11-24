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

pub mod observe;
#[cfg(test)]
pub(crate) mod tests;

#[derive(Clone, Debug)]
pub struct SlotFirewallRequest {
    pub node_id: NodeId,
    pub generation: u64,
    pub slot_id: u32,
    /// When set, a slot or namespace that isn't this target is `Changed`.
    pub expected: Option<SlotTarget>,
}

/// The occupant of a slot, as discovered or as an attempt reached it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotTarget {
    pub slot_generation: u64,
    pub sandbox_id: Option<String>,
    /// Unset until an attempt pinned it.
    pub namespace: Option<NamespaceIdentity>,
}

impl SlotTarget {
    fn of(info: &SlotInfo) -> Self {
        Self {
            slot_generation: info.generation,
            sandbox_id: (!info.sandbox_id.is_empty()).then(|| info.sandbox_id.clone()),
            namespace: None,
        }
    }
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
    #[error("the network slot is {state}/{health}, not a healthy ready or a leased slot")]
    Ineligible {
        state: SlotState,
        health: SlotHealth,
    },
    /// The holder may be an older generation's apply, so retry until this
    /// request applies or a newer generation supersedes it.
    #[error("the network slot is owned by other work")]
    Busy,
    #[error("the network slot changed before it was owned")]
    Changed,
    #[error("cannot pin the sandbox network namespace: {0}")]
    Namespace(#[source] io::Error),
    #[error("node-manager is shutting down")]
    ShuttingDown,
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

    pub fn target(&self) -> SlotTarget {
        SlotTarget {
            slot_generation: self.slot_generation,
            sandbox_id: self.sandbox_id.clone(),
            namespace: Some(self.namespace),
        }
    }
}

/// What the operation reaches beyond the pool and nft; tests substitute it.
pub(crate) trait SlotHost: Send + Sync + 'static {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf;
    /// Test seam between the lookup and taking ownership.
    fn looked_up(&self) {}
    /// A directory only managed namespaces occupy, if the host has one.
    fn namespace_dir(&self) -> Option<&Path> {
        None
    }
    /// Test seam between an observation's last read and its final checks.
    fn observed(&self) {}
}

/// Declares no namespace directory: `/run/netns` also holds CNI-named ones.
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
    /// run. While its job runs, a dynamic create skips a Ready slot it holds,
    /// and fails if every free slot is held. A caller should apply to Ready
    /// slots one at a time.
    pub async fn apply_firewall_rules(
        self: &Arc<Self>,
        assignments: &Arc<FirewallAssignments>,
        request: SlotFirewallRequest,
    ) -> Result<SlotFirewallReport, tokio::task::JoinError> {
        self.apply_firewall_rules_with(assignments, request, Arc::new(Live), Arc::new(nft))
            .await
    }

    pub(crate) async fn apply_firewall_rules_with(
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

    /// Every slot an apply would take now, at the incarnation read. The pool
    /// lock is released on return, before any namespace or nft work.
    pub fn firewall_targets(&self) -> Result<Vec<(u32, SlotTarget)>, SlotStoreError> {
        let records = self.all_records()?;
        let eligible = records
            .iter()
            .filter(|record| eligible(record.state, record.health, &record.info.sandbox_id));
        Ok(eligible
            .map(|record| (record.info.slot, SlotTarget::of(&record.info)))
            .collect())
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

    /// Takes the guard without waiting, then waits for the DB lock.
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

/// A healthy ready slot no sandbox holds, or a leased one a sandbox does at
/// any health, since it still carries guest traffic.
fn eligible(state: SlotState, health: SlotHealth, sandbox_id: &str) -> bool {
    matches!(
        (state, health, sandbox_id.is_empty()),
        (SlotState::Ready, SlotHealth::Healthy, true) | (SlotState::Leased, _, false)
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

/// `pid` and health change during a lease without changing its lifecycle.
fn same_lifecycle(now: &SlotRecord, owned: &SlotRecord) -> bool {
    fn key(record: &SlotRecord) -> (u32, u64, SlotState, &str) {
        let info = &record.info;
        (info.slot, info.generation, record.state, &info.sandbox_id)
    }
    key(now) == key(owned)
}

fn apply_blocking(
    pool: &Arc<NetworkPool>,
    assignments: &FirewallAssignments,
    request: SlotFirewallRequest,
    host: &dyn SlotHost,
    nft: &Nft<'_>,
) -> SlotFirewallReport {
    // As reset does, refuse new slot work once node-manager shuts down.
    if pool.shutdown_token.is_cancelled() {
        return SlotFirewallReport::NotRun(NotRun::ShuttingDown);
    }
    let owned = pool.lookup_eligible(request.slot_id).and_then(|record| {
        host.looked_up();
        pool.own(record)
    });
    let owned = match owned {
        Ok(owned) => owned,
        Err(not_run) => return SlotFirewallReport::NotRun(not_run),
    };
    // Taken under ownership, so an invocation queued behind a newer one
    // cannot reinstall an older revision. Before the target checks, so
    // superseded work reads not current rather than changed.
    let desired = match runnable(assignments.current(), request.node_id, request.generation) {
        Ok(desired) => desired,
        Err(not_runnable) => return SlotFirewallReport::NotRun(not_runnable.into()),
    };
    let info = &owned.record.info;
    let SlotTarget {
        slot_generation,
        sandbox_id,
        ..
    } = SlotTarget::of(info);
    // Both checked under ownership, before nft runs.
    let expected = request.expected.as_ref();
    let changed = |target: &SlotTarget| {
        (target.slot_generation, &target.sandbox_id) != (slot_generation, &sandbox_id)
    };
    if expected.is_some_and(changed) {
        return SlotFirewallReport::NotRun(NotRun::Changed);
    }
    let path = host.namespace_path(info);
    let (netns, namespace) = match pin(&path) {
        Ok(pinned) => pinned,
        Err(error) => return SlotFirewallReport::NotRun(NotRun::Namespace(error)),
    };
    if expected.is_some_and(|target| target.namespace.is_some_and(|pinned| pinned != namespace)) {
        return SlotFirewallReport::NotRun(NotRun::Changed);
    }
    let submitted = install_with(&render(&desired.revision), &netns, nft);
    let readback = verify_with(&desired.revision, &netns, nft);
    let current = still_current(pool, assignments, &desired, &owned.record, &path, namespace);
    SlotFirewallReport::Attempted(Box::new(SlotFirewallAttempt {
        node_id: desired.node_id,
        assignment_generation: desired.generation,
        revision_id: desired.revision_id,
        slot_id: info.slot,
        slot_generation,
        sandbox_id,
        namespace,
        submitted,
        readback,
        current,
    }))
}

fn pin(path: &Path) -> io::Result<(File, NamespaceIdentity)> {
    let netns = File::open(path)?;
    // A deleted namespace's entry is unmounted before it is unlinked; opened
    // in between it is a plain file, so the namespace is as good as gone.
    if nix::sys::statfs::fstatfs(&netns)?.filesystem_type() != nix::sys::statfs::NSFS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} is not a mounted namespace", path.display()),
        ));
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
    unchanged(path, namespace).map_err(Stale::Namespace)?;
    runnable(assignments.current(), desired.node_id, desired.generation)
        .map_err(Stale::Assignment)?;
    Ok(())
}

/// What `path` names now, unless it is still `namespace`.
fn unchanged(
    path: &Path,
    namespace: NamespaceIdentity,
) -> Result<(), io::Result<NamespaceIdentity>> {
    match identity(std::fs::metadata(path)) {
        Ok(now) if now == namespace => Ok(()),
        now => Err(now),
    }
}
