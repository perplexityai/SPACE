//! The test binding of `SyncFirewallRules` to one node, assignment record and
//! either one slot or every eligible slot. One tracked worker runs the latest
//! accepted generation's pass; deliveries of a generation share it, and a newer
//! one supersedes older work. Each `All` pass targets the slot incarnations it
//! lists, and never redirects work to another occupant; a `One` pass's first
//! attempt to reach nft fixes its target for the binding. Per-slot results live
//! in memory only; no node-wide receipt.
//!
//! A result that may not describe what nft did ends the whole binding for this
//! process: a slot job that ended without a result, an inconclusive readback,
//! or a slot or assignment store that can no longer confirm the result. So
//! does any changed target under `One`. Under `All`, a changed target whose
//! readback is conclusive is that slot's result alone, and the pass goes on:
//! changed before nft it reads not run, and changed while nft ran it reads
//! stale, since a sandbox delete doesn't wait for the slot's guard. A delivery
//! after a pass settles re-runs the slots that never reached nft, or lists
//! again after a failed inventory; a delivery during the pass joins it. Nothing
//! else is resubmitted, so a stale slot waits for the next generation to list
//! it again.
//!
//! An `All` binding also gates each runtime launch (`start`); a launch whose
//! outcome is unknown ends the binding, though a pass already running finishes.
//!
//! A production binding is expected to build on this worker. First it must
//! revisit the node-wide end rule, re-listing on retry, and per-slot reporting,
//! and it must ship behind a default-off switch. Under `All`, nothing signals a
//! slot left on its earlier ruleset: one the pass never reaches because the
//! binding ended, one not run when the pass reached it (busy, its namespace
//! unpinnable, or a store unreadable) until a redelivery, one whose occupant
//! changed after listing, one replaced while nft ran, one created after
//! listing, or every slot after an empty first listing, which settles the pass
//! so same-generation redeliveries never list again. The node-wide reply is the
//! same in all of these cases.

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;

use super::super::NodeManagerService;
use crate::firewall::assignment::{
    AcceptError, Acceptance, FirewallAssignments, IncomingAssignment, OpenError,
};
use crate::firewall::rules::Nft;
use crate::firewall::rules::verify::Verification;
use crate::net::SlotInfo;
use crate::proto::{
    FirewallAssignmentAcceptance, FirewallSlotDispatch, FirewallSlotDispatchOutcome as Outcome,
    SyncFirewallRulesRequest, SyncFirewallRulesResponse,
};
use crate::slot_pool::firewall_rules::SlotHost;
use crate::slot_pool::{
    NamespaceIdentity, NetworkPool, NotRun, NotRunnable, SlotFirewallReport, SlotFirewallRequest,
    SlotTarget, Stale,
};

mod rounds;
#[cfg(test)]
mod tests;

/// What the binding trusts instead of the wire.
pub(crate) struct FirewallSyncFixture {
    pub node_id: NodeId,
    pub slots: BoundSlots,
    pub assignment_path: PathBuf,
    /// `NetworkPool::apply_firewall_rules_with`'s namespace and nft seams.
    pub host: Arc<dyn SlotHost>,
    pub nft: Arc<Nft<'static>>,
    /// How long a `One` delivery waits for its dispatch, and a gated `All`
    /// start for the pass to settle. No `All` delivery waits, as its reply
    /// carries no dispatch.
    pub wait: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BoundSlots {
    One(u32),
    /// Every slot the slot apply would take when a generation's pass starts. A
    /// ready slot rebuilt in place keeps its generation, so it is still the target.
    All,
}

/// What a generation's pass has left to run.
enum Left {
    /// Not listed yet, or the inventory failed.
    Unlisted,
    /// Each slot without a result: its index, ID and listed target.
    Slots(Vec<(usize, u32, Option<SlotTarget>)>),
    /// A newer generation replaced it.
    Superseded,
}

/// What a slot's result ends besides its own attempt.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Ends {
    Nothing,
    /// The target changed, before nft or while it ran: a `One` binding ends,
    /// and an `All` pass goes on.
    Target,
    /// What nft did to the target is uncertain.
    Binding,
}

impl Ends {
    /// Whether the result ends a binding of `slots`.
    fn binding(self, slots: BoundSlots) -> bool {
        match self {
            Ends::Binding => true,
            Ends::Target => matches!(slots, BoundSlots::One(_)),
            Ends::Nothing => false,
        }
    }
}

pub(crate) struct FirewallSync {
    fixture: FirewallSyncFixture,
    assignments: Arc<FirewallAssignments>,
    wanted: watch::Sender<Option<Wanted>>,
    /// Cancelled once the worker ends, which ends the binding.
    ended: CancellationToken,
    /// Recent verification rounds, for replay.
    rounds: std::sync::Mutex<rounds::Rounds>,
    /// Held by each round's sweep, so sweeps read one at a time.
    sweeps: tokio::sync::Mutex<()>,
    /// A gated start reads and an acceptance writes, so none lands between a
    /// start's readback and its launch's outcome.
    order: Arc<RwLock<()>>,
}

/// The latest generation armed, and how far its pass got.
struct Wanted {
    generation: u64,
    claimed: bool,
    /// Unset until listed; `Err` if the pool's inventory could not be read.
    pass: Option<Result<Vec<Item>, String>>,
}

struct Item {
    slot_id: u32,
    expected: Option<SlotTarget>,
    /// Unset until this run of the pass reaches the slot.
    dispatch: Option<FirewallSlotDispatch>,
}

impl Wanted {
    /// Every slot has a result, or the inventory failed.
    fn settled(&self) -> bool {
        match &self.pass {
            Some(Ok(items)) => items.iter().all(|item| item.dispatch.is_some()),
            Some(Err(_)) => true,
            None => false,
        }
    }

    /// Clears what never reached nft so it runs again, if anything didn't. The
    /// listed targets stay, so a slot taken since reads not run again.
    fn retry(&mut self) -> bool {
        let retry = match &mut self.pass {
            Some(Ok(items)) => {
                let not_run = |done: &FirewallSlotDispatch| done.outcome() == Outcome::NotRun;
                let mut retry = false;
                for item in items {
                    if item.dispatch.as_ref().is_some_and(not_run) {
                        item.dispatch = None;
                        retry = true;
                    }
                }
                retry
            }
            _ => {
                self.pass = None;
                true
            }
        };
        self.claimed &= !retry;
        retry
    }
}

impl NodeManagerService {
    /// Opens the binding's one assignment store and starts its worker. Tests
    /// hold no service mutex; the store's path is theirs alone.
    pub(crate) fn with_firewall_sync(
        mut self,
        fixture: FirewallSyncFixture,
    ) -> Result<Self, OpenError> {
        let assignments = Arc::new(FirewallAssignments::open(&fixture.assignment_path)?);
        let sync = Arc::new(FirewallSync {
            fixture,
            assignments,
            wanted: watch::Sender::new(None),
            ended: CancellationToken::new(),
            rounds: std::sync::Mutex::default(),
            sweeps: tokio::sync::Mutex::new(()),
            order: Arc::default(),
        });
        self.device_op_tasks
            .spawn(sync.clone().work(self.pool.clone()));
        self.firewall_sync = Some(sync);
        Ok(self)
    }
}

impl FirewallSync {
    pub(super) async fn deliver(
        self: Arc<Self>,
        service: &NodeManagerService,
        request: SyncFirewallRulesRequest,
    ) -> Result<SyncFirewallRulesResponse, Status> {
        // A delivery that races the worker's end can still commit and arm, then replies
        // NOT_RUN "the binding ended" for one slot or no dispatch for all; nothing reaches
        // nft. One that races a start's end is refused under the order.
        if self.ended.is_cancelled() {
            return Err(Status::failed_precondition(
                "the firewall rule sync binding ended",
            ));
        }
        let node_id = NodeId::from_uuid(canonical_uuid("node_id", &request.node_id)?);
        let assignment = request
            .assignment
            .ok_or_else(|| Status::invalid_argument("assignment is required"))?;
        let revision_id = canonical_uuid("revision_id", &assignment.revision_id)?;
        let generation = assignment.generation;
        let sync = self.clone();
        let order = self.order.clone().write_owned().await;
        let acceptance = service
            .off_runtime("accept firewall assignment", move || {
                let _order = order;
                // A start whose outcome is unknown ended the binding meanwhile.
                if sync.ended.is_cancelled() {
                    return Err(Status::failed_precondition(
                        "the firewall rule sync binding ended",
                    ));
                }
                let incoming = IncomingAssignment {
                    node_id,
                    generation,
                    revision_id,
                    document: &assignment.revision,
                };
                let acceptance = sync
                    .assignments
                    .accept(sync.fixture.node_id, incoming)
                    .map_err(accept_status)?;
                // In the same job, so a cancelled caller can't strand a durable acceptance.
                sync.wanted
                    .send_if_modified(|wanted| arm(wanted, generation));
                Ok(acceptance)
            })
            .await?;
        let acceptance = match acceptance {
            Acceptance::Committed => FirewallAssignmentAcceptance::Committed,
            Acceptance::Replayed => FirewallAssignmentAcceptance::Replayed,
        };
        Ok(SyncFirewallRulesResponse {
            accepted_generation: generation,
            acceptance: acceptance.into(),
            // A reply describes one slot at most, never the node.
            dispatch: match self.fixture.slots {
                BoundSlots::One(_) => Some(self.settled(generation).await),
                BoundSlots::All => None,
            },
        })
    }

    /// Under a node-wide binding, runs `launch` only after the accepted
    /// assignment is applied to the lease's slot and read back in the
    /// namespace the launcher opens; every refusal comes before dispatch. A
    /// launch that fails or is dropped may have started its guest, so it ends
    /// the binding.
    pub(in crate::service) async fn start<T>(
        &self,
        pool: &Arc<NetworkPool>,
        lease: &SlotInfo,
        namespace: &Path,
        launch: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        if !matches!(self.fixture.slots, BoundSlots::All) {
            return launch.await;
        }
        let _order = self.order.read().await;
        let refused = |why: String| Status::failed_precondition(format!("firewall check: {why}"));
        if self.ended.is_cancelled() {
            return Err(refused("the binding ended".into()));
        }
        // Only an acceptance arms a pass, so once this one settles the worker
        // stays off the slot until `_order` drops.
        let mut wanted = self.wanted.subscribe();
        let idle = wanted.wait_for(|wanted| {
            wanted
                .as_ref()
                .is_none_or(|wanted| wanted.claimed && wanted.settled())
        });
        tokio::select! {
            idle = tokio::time::timeout(self.fixture.wait, idle) => {
                if idle.is_err() {
                    return Err(refused("the node-wide pass is still running".into()));
                }
            }
            () = self.ended.cancelled() => return Err(refused("the binding ended".into())),
        }
        let accepted = match self.assignments.current() {
            Ok(Some(accepted)) => accepted,
            Ok(None) => return Err(refused("no accepted assignment".into())),
            Err(error) => return Err(refused(error.to_string())),
        };
        let request = SlotFirewallRequest {
            node_id: self.fixture.node_id,
            generation: accepted.generation,
            slot_id: lease.slot,
            expected: Some(SlotTarget {
                slot_generation: lease.generation,
                sandbox_id: Some(lease.sandbox_id.clone()),
                namespace: None,
            }),
        };
        let (host, nft) = (self.fixture.host.clone(), self.fixture.nft.clone());
        let attempt = match pool
            .apply_firewall_rules_with(&self.assignments, request, host, nft)
            .await
        {
            Ok(SlotFirewallReport::Attempted(attempt)) if attempt.verified() => attempt,
            // Even an uncertain result only refuses: no guest runs, and the next start reinstalls.
            report => return Err(refused(format!("{report:?}"))),
        };
        // The launcher opens the namespace by this path.
        let entry = std::fs::metadata(namespace).map(|entry| NamespaceIdentity {
            device: entry.dev(),
            inode: entry.ino(),
        });
        if entry.ok() != Some(attempt.namespace) {
            return Err(refused("the launch names another namespace".into()));
        }
        // Dropped before `_order`, so no acceptance follows an unknown outcome.
        let unknown = self.ended.clone().drop_guard();
        let launched = launch.await?;
        unknown.disarm();
        Ok(launched)
    }

    /// The one slot's dispatch for `generation` once settled, or as of the end
    /// of the wait.
    async fn settled(&self, generation: u64) -> FirewallSlotDispatch {
        let mut wanted = self.wanted.subscribe();
        let settled = wanted.wait_for(|wanted| {
            wanted
                .as_ref()
                .is_some_and(|wanted| wanted.generation != generation || wanted.settled())
        });
        tokio::select! {
            _ = tokio::time::timeout(self.fixture.wait, settled) => {}
            () = self.ended.cancelled() => {}
        }
        let reply = |outcome: Outcome, detail: &str| FirewallSlotDispatch {
            outcome: outcome.into(),
            detail: detail.into(),
        };
        match self.wanted.borrow().as_ref() {
            Some(wanted) if wanted.generation > generation => reply(Outcome::Superseded, ""),
            Some(Wanted {
                pass: Some(Ok(items)),
                ..
            }) if let Some(done) = items.first().and_then(|item| item.dispatch.as_ref()) => {
                done.clone()
            }
            // The worker ended before claiming it.
            _ if self.ended.is_cancelled() => reply(Outcome::NotRun, "the binding ended"),
            _ => reply(Outcome::Pending, ""),
        }
    }

    async fn work(self: Arc<Self>, pool: Arc<NetworkPool>) {
        let _ended = self.ended.clone().drop_guard();
        let mut changed = self.wanted.subscribe();
        let mut target = None;
        loop {
            let Some(generation) = self.claim() else {
                tokio::select! {
                    biased;
                    () = pool.shutdown_token.cancelled() => return,
                    // The sender lives in `self`, so this never fails.
                    _ = changed.changed() => continue,
                }
            };
            let work = match self.left(generation) {
                Left::Slots(work) => work,
                Left::Superseded => continue,
                Left::Unlisted => {
                    let pass = self.list(&pool, &target).await;
                    if let Err(error) = &pass {
                        tracing::warn!(generation, %error, "fixture firewall slot inventory failed");
                    }
                    self.update(generation, |wanted| wanted.pass = Some(pass));
                    match self.left(generation) {
                        Left::Slots(work) => work,
                        Left::Unlisted | Left::Superseded => continue,
                    }
                }
            };
            // Once superseded, the slot apply refuses the rest before nft, and their
            // results are dropped.
            let listed = work.len();
            for (ran, (index, slot_id, expected)) in (1..).zip(work) {
                let request = SlotFirewallRequest {
                    node_id: self.fixture.node_id,
                    generation,
                    slot_id,
                    expected,
                };
                let (host, nft) = (self.fixture.host.clone(), self.fixture.nft.clone());
                let report = pool
                    .apply_firewall_rules_with(&self.assignments, request, host, nft)
                    .await;
                if let (BoundSlots::One(_), Ok(SlotFirewallReport::Attempted(attempt))) =
                    (self.fixture.slots, &report)
                {
                    target.get_or_insert_with(|| attempt.target());
                }
                let (done, ends) = dispatch(&report);
                let ends = ends.binding(self.fixture.slots);
                tracing::info!(
                    generation,
                    slot_id,
                    outcome = ?done.outcome(),
                    detail = done.detail,
                    "fixture firewall slot dispatch finished"
                );
                if ends {
                    tracing::warn!(
                        generation,
                        slot_id,
                        outcome = ?done.outcome(),
                        not_reached = listed - ran,
                        "fixture firewall sync binding ended"
                    );
                }
                self.wanted.send_if_modified(|wanted| {
                    // Under the watch lock, so a waiter the end wakes reads this result,
                    // and no delivery that sees the result is admitted.
                    if ends {
                        self.ended.cancel();
                    }
                    // A newer generation replaced this one, so its result is dropped,
                    // though an end still ends the binding.
                    match wanted {
                        Some(wanted) if wanted.generation == generation => {
                            if let Some(Ok(items)) = &mut wanted.pass
                                && let Some(item) = items.get_mut(index)
                            {
                                item.dispatch = Some(done);
                            }
                            true
                        }
                        _ => false,
                    }
                });
                if ends {
                    return;
                }
            }
        }
    }

    /// The pass's slots: the bound one, or every eligible one at the
    /// incarnation read.
    async fn list(
        &self,
        pool: &Arc<NetworkPool>,
        target: &Option<SlotTarget>,
    ) -> Result<Vec<Item>, String> {
        let found = match self.fixture.slots {
            BoundSlots::One(slot_id) => vec![(slot_id, target.clone())],
            BoundSlots::All => {
                let reader = pool.clone();
                // Off the runtime, as pool.db's lock blocks.
                let found = pool
                    .task_tracker
                    .spawn_blocking(move || reader.firewall_targets())
                    .await;
                match found {
                    Ok(Ok(found)) => found
                        .into_iter()
                        .map(|(slot_id, found)| (slot_id, Some(found)))
                        .collect(),
                    Ok(Err(error)) => return Err(error.to_string()),
                    Err(error) => return Err(error.to_string()),
                }
            }
        };
        let item = |(slot_id, expected)| Item {
            slot_id,
            expected,
            dispatch: None,
        };
        Ok(found.into_iter().map(item).collect())
    }

    fn left(&self, generation: u64) -> Left {
        match self.wanted.borrow().as_ref() {
            Some(wanted) if wanted.generation == generation => {
                let Some(Ok(items)) = &wanted.pass else {
                    return Left::Unlisted;
                };
                let left = items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| item.dispatch.is_none());
                Left::Slots(
                    left.map(|(index, item)| (index, item.slot_id, item.expected.clone()))
                        .collect(),
                )
            }
            _ => Left::Superseded,
        }
    }

    fn update(&self, generation: u64, edit: impl FnOnce(&mut Wanted)) {
        self.wanted.send_if_modified(|wanted| match wanted {
            Some(wanted) if wanted.generation == generation => {
                edit(wanted);
                true
            }
            _ => false,
        });
    }

    /// Marks the pending generation claimed and returns it.
    fn claim(&self) -> Option<u64> {
        let mut claimed = None;
        self.wanted.send_if_modified(|wanted| match wanted {
            Some(wanted) if !wanted.claimed => {
                wanted.claimed = true;
                claimed = Some(wanted.generation);
                true
            }
            _ => false,
        });
        claimed
    }
}

/// Arms `generation` unless a newer one is armed. A delivery during a pass
/// joins it; one after the pass settles re-arms it only for the slots that
/// never reached nft, or to list again after a failed inventory.
fn arm(wanted: &mut Option<Wanted>, generation: u64) -> bool {
    match wanted {
        Some(armed) if armed.generation > generation => false,
        Some(armed) if armed.generation == generation => {
            // A mismatch waits for a newer generation: nft lists some valid revisions differently.
            armed.claimed && armed.settled() && armed.retry()
        }
        _ => {
            *wanted = Some(Wanted {
                generation,
                claimed: false,
                pass: None,
            });
            true
        }
    }
}

/// The reply for `report`, and what it ends.
fn dispatch(report: &Result<SlotFirewallReport, JoinError>) -> (FirewallSlotDispatch, Ends) {
    let (outcome, detail, ends) = match report {
        Err(error) => (Outcome::Unknown, error.to_string(), Ends::Binding),
        Ok(SlotFirewallReport::NotRun(not_run)) => {
            let drifted = matches!(
                not_run,
                NotRun::NoSlot | NotRun::Ineligible { .. } | NotRun::Changed | NotRun::Namespace(_)
            );
            let ends = if drifted { Ends::Target } else { Ends::Nothing };
            (Outcome::NotRun, not_run.to_string(), ends)
        }
        Ok(SlotFirewallReport::Attempted(attempt)) => match (&attempt.readback, &attempt.current) {
            (Verification::Inconclusive(why), _) => {
                (Outcome::Inconclusive, why.clone(), Ends::Binding)
            }
            (_, Err(stale)) => {
                let ends = match stale {
                    Stale::Assignment(
                        NotRunnable::NoAssignment
                        | NotRunnable::WrongNode { .. }
                        | NotRunnable::NotCurrent { .. },
                    ) => Ends::Nothing,
                    Stale::Slot | Stale::Namespace(_) => Ends::Target,
                    // Nothing can confirm the result.
                    Stale::SlotStore(_) | Stale::Assignment(NotRunnable::Unavailable(_)) => {
                        Ends::Binding
                    }
                };
                (Outcome::Stale, format!("{stale:?}"), ends)
            }
            (Verification::Match, Ok(())) => (Outcome::Matched, String::new(), Ends::Nothing),
            (Verification::Mismatch(why), Ok(())) => {
                (Outcome::Mismatched, why.clone(), Ends::Nothing)
            }
        },
    };
    let dispatch = FirewallSlotDispatch {
        outcome: outcome.into(),
        detail,
    };
    (dispatch, ends)
}

/// Only the hyphenated lowercase form, so an ID has one spelling.
fn canonical_uuid(field: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::try_parse(value)
        .ok()
        .filter(|id| id.to_string() == value)
        .ok_or_else(|| Status::invalid_argument(format!("{field} must be a canonical UUID")))
}

fn accept_status(error: AcceptError) -> Status {
    let message = error.to_string();
    match error {
        AcceptError::InvalidRevision(_) | AcceptError::TooLarge => {
            Status::invalid_argument(message)
        }
        AcceptError::WrongNode { .. }
        | AcceptError::BoundToOtherNode { .. }
        | AcceptError::Conflict { .. } => Status::failed_precondition(message),
        AcceptError::Stale { .. } => Status::aborted(message),
        AcceptError::Write(_) => Status::unavailable(message),
        AcceptError::Unsynced(_) | AcceptError::Unavailable(_) => Status::internal(message),
    }
}
