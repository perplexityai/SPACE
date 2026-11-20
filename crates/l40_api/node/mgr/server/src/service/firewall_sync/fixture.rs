//! The test binding of `SyncFirewallRules` to one node, slot and assignment
//! record. One tracked worker applies the latest accepted generation to that
//! slot: deliveries of a generation share its one attempt, and a newer
//! generation supersedes older work. The attempt state lives in memory only;
//! it is no receipt and does not survive a restart. A production binding is
//! meant to promote this worker as is; only where the binding comes from changes.
//!
//! Each result is one of three cases. Retry: nothing was applied and the
//! target isn't known to have changed, so redelivery runs it again. Target
//! changed: the slot number no longer proves it's the same sandbox. Unknown:
//! what the attempt did can't be told. The last two end the binding for this
//! process, so waiting work never runs against a new occupant and a later
//! success can't hide the uncertainty. A completed attempt is never resubmitted.
//! The first attempt to reach nft fixes the target, so a slot or namespace
//! replaced between attempts is a changed target.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use tokio::sync::watch;
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
use crate::proto::{
    FirewallAssignmentAcceptance, FirewallSlotDispatch, FirewallSlotDispatchOutcome as Outcome,
    SyncFirewallRulesRequest, SyncFirewallRulesResponse,
};
use crate::slot_pool::firewall_rules::SlotHost;
use crate::slot_pool::{NetworkPool, NotRun, SlotFirewallReport, SlotFirewallRequest, Stale};

#[cfg(test)]
mod tests;

/// What the binding trusts instead of the wire.
pub(crate) struct FirewallSyncFixture {
    pub node_id: NodeId,
    pub slot_id: u32,
    pub assignment_path: PathBuf,
    /// `NetworkPool::apply_firewall_rules_with`'s namespace and nft seams.
    pub host: Arc<dyn SlotHost>,
    pub nft: Arc<Nft<'static>>,
    /// How long a delivery waits for its dispatch.
    pub wait: Duration,
}

pub(crate) struct FirewallSync {
    fixture: FirewallSyncFixture,
    assignments: Arc<FirewallAssignments>,
    wanted: watch::Sender<Option<Wanted>>,
    /// Cancelled once the worker ends, which ends the binding.
    ended: CancellationToken,
}

/// The latest generation armed for the slot, and how far it got.
struct Wanted {
    generation: u64,
    run: Run,
}

enum Run {
    Pending,
    Running,
    Done(FirewallSlotDispatch),
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
        let acceptance = service
            .off_runtime("accept firewall assignment", move || {
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
            dispatch: Some(self.settled(generation).await),
        })
    }

    /// `generation`'s dispatch once settled, or as of the end of the wait.
    async fn settled(&self, generation: u64) -> FirewallSlotDispatch {
        let mut wanted = self.wanted.subscribe();
        let settled = wanted.wait_for(|wanted| {
            wanted.as_ref().is_some_and(|wanted| {
                wanted.generation != generation || matches!(wanted.run, Run::Done(_))
            })
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
                run: Run::Done(done),
                ..
            }) => done.clone(),
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
            let request = SlotFirewallRequest {
                node_id: self.fixture.node_id,
                generation,
                slot_id: self.fixture.slot_id,
                expected: target.clone(),
            };
            let (host, nft) = (self.fixture.host.clone(), self.fixture.nft.clone());
            let report = pool
                .apply_firewall_rules_with(&self.assignments, request, host, nft)
                .await;
            if let Ok(SlotFirewallReport::Attempted(attempt)) = &report {
                target.get_or_insert_with(|| attempt.target());
            }
            let (done, ends) = dispatch(&report);
            tracing::info!(
                generation,
                slot_id = self.fixture.slot_id,
                outcome = ?done.outcome(),
                detail = done.detail,
                "fixture firewall slot dispatch finished"
            );
            self.wanted.send_if_modified(|wanted| {
                // Under the watch lock, so a waiter the end wakes reads this result,
                // and no delivery that sees the result is admitted.
                if ends {
                    self.ended.cancel();
                }
                // A newer generation replaced this one, so its result is dropped.
                match wanted {
                    Some(wanted) if wanted.generation == generation => {
                        wanted.run = Run::Done(done);
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

    /// Marks the pending generation running and returns it.
    fn claim(&self) -> Option<u64> {
        let mut claimed = None;
        self.wanted.send_if_modified(|wanted| match wanted {
            Some(wanted) if matches!(wanted.run, Run::Pending) => {
                wanted.run = Run::Running;
                claimed = Some(wanted.generation);
                true
            }
            _ => false,
        });
        claimed
    }
}

/// Arms `generation` unless a newer one is armed. Redelivery re-arms only a
/// generation that never reached nft.
fn arm(wanted: &mut Option<Wanted>, generation: u64) -> bool {
    match wanted {
        Some(armed) if armed.generation > generation => false,
        Some(armed) if armed.generation == generation => {
            // A mismatch waits for a newer generation: nft lists some valid revisions differently.
            let retry = matches!(&armed.run, Run::Done(done) if done.outcome() == Outcome::NotRun);
            if retry {
                armed.run = Run::Pending;
            }
            retry
        }
        _ => {
            *wanted = Some(Wanted {
                generation,
                run: Run::Pending,
            });
            true
        }
    }
}

/// The reply for `report`, and whether it ends the binding.
fn dispatch(report: &Result<SlotFirewallReport, JoinError>) -> (FirewallSlotDispatch, bool) {
    let (outcome, detail, ends) = match report {
        Err(error) => (Outcome::Unknown, error.to_string(), true),
        Ok(SlotFirewallReport::NotRun(not_run)) => {
            let drifted = matches!(
                not_run,
                NotRun::NoSlot | NotRun::Ineligible { .. } | NotRun::Changed | NotRun::Namespace(_)
            );
            (Outcome::NotRun, not_run.to_string(), drifted)
        }
        Ok(SlotFirewallReport::Attempted(attempt)) => match (&attempt.readback, &attempt.current) {
            (Verification::Inconclusive(why), _) => (Outcome::Inconclusive, why.clone(), true),
            // Only an assignment change leaves the target trusted.
            (_, Err(stale)) => (
                Outcome::Stale,
                format!("{stale:?}"),
                !matches!(stale, Stale::Assignment(_)),
            ),
            (Verification::Match, Ok(())) => (Outcome::Matched, String::new(), false),
            (Verification::Mismatch(why), Ok(())) => (Outcome::Mismatched, why.clone(), false),
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
