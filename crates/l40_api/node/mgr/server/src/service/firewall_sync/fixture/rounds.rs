//! Verification rounds against the binding's accepted assignment. A new round
//! takes a fresh observation; callers of one round share it, and once it is
//! final it is replayed as is. The last few rounds live in this process's
//! memory only; an older or out-of-order one is refused as stale, never
//! answered from current state.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared};
use pplx_space_accounts::NodeId;
use tokio::sync::oneshot;

use super::{BoundSlots, FirewallSync};
use crate::slot_pool::firewall_rules::observe::{Refusal, SWEEP_BUDGET};
use crate::slot_pool::{FirewallObservation, NetworkPool, SlotTarget};

/// Rounds kept for replay.
const RETAINED: usize = 8;

/// One round's answer.
#[derive(Debug)]
pub(crate) struct Observed {
    pub round: u64,
    pub answer: Result<FirewallObservation, Refusal>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RoundRefused {
    #[error("the verification names another node")]
    WrongNode,
    #[error("the round is no longer retained, or older than the newest")]
    Stale,
    #[error("the round was asked for generation {generation}")]
    Conflict { generation: u64 },
}

#[derive(Default)]
pub(super) struct Rounds {
    newest: Option<u64>,
    retained: VecDeque<Round>,
}

struct Round {
    round: u64,
    generation: u64,
    observed: Answer,
}

/// Resolves once the round's observation is final; a clone shares it.
pub(crate) type Answer = Shared<BoxFuture<'static, Arc<Observed>>>;

impl FirewallSync {
    /// `round`'s observation of `generation`, taken once for this process.
    /// Admitted before it is awaited.
    pub(crate) fn observe(
        self: &Arc<Self>,
        pool: &Arc<NetworkPool>,
        node_id: NodeId,
        generation: u64,
        round: u64,
    ) -> Result<Answer, RoundRefused> {
        if node_id != self.fixture.node_id {
            return Err(RoundRefused::WrongNode);
        }
        let (send, observed) = {
            let mut rounds = lock(&self.rounds);
            match rounds.retained.iter().find(|kept| kept.round == round) {
                Some(kept) if kept.generation != generation => {
                    return Err(RoundRefused::Conflict {
                        generation: kept.generation,
                    });
                }
                Some(kept) => return Ok(kept.observed.clone()),
                None if rounds.newest.is_some_and(|newest| round <= newest) => {
                    return Err(RoundRefused::Stale);
                }
                None => {}
            }
            let (send, receive) = oneshot::channel();
            let observed = answer(round, receive);
            rounds.newest = Some(round);
            rounds.retained.push_back(Round {
                round,
                generation,
                observed: observed.clone(),
            });
            if rounds.retained.len() > RETAINED {
                rounds.retained.pop_front();
            }
            (send, observed)
        };
        self.sweep(pool, generation, round, send);
        Ok(observed)
    }

    /// A tracked task, so a cancelled caller can't strand the round; sweeps
    /// run one at a time, and one whose round was evicted while it waited
    /// never runs, since nobody can ask for that round again.
    fn sweep(
        self: &Arc<Self>,
        pool: &Arc<NetworkPool>,
        generation: u64,
        round: u64,
        send: oneshot::Sender<Arc<Observed>>,
    ) {
        let (sync, reader) = (self.clone(), pool.clone());
        pool.task_tracker.spawn(async move {
            let _serial = sync.sweeps.lock().await;
            if !lock(&sync.rounds)
                .retained
                .iter()
                .any(|kept| kept.round == round)
            {
                return;
            }
            let (host, nft) = (sync.fixture.host.clone(), sync.fixture.nft.clone());
            let pending = sync.clone();
            let applying = move |slot_id: u32, target: &SlotTarget| {
                pending.applying(generation, slot_id, target)
            };
            let request = (sync.fixture.node_id, generation);
            let answer = reader
                .observe_firewall_rules_with(
                    &sync.assignments,
                    request,
                    host,
                    nft,
                    applying,
                    SWEEP_BUDGET,
                )
                .await;
            if send.send(Arc::new(Observed { round, answer })).is_err() {
                tracing::debug!(round, "firewall observation round was evicted while it ran");
            }
        });
    }

    /// The live binding has this generation armed and its pass still to run
    /// on `target`.
    pub(super) fn applying(&self, generation: u64, slot_id: u32, target: &SlotTarget) -> bool {
        if self.ended.is_cancelled() {
            return false;
        }
        let wanted = self.wanted.borrow();
        let Some(wanted) = wanted.as_ref().filter(|w| w.generation == generation) else {
            return false;
        };
        let same = |listed: &SlotTarget| {
            (listed.slot_generation, &listed.sandbox_id)
                == (target.slot_generation, &target.sandbox_id)
        };
        match &wanted.pass {
            // Not listed yet: an `All` pass takes every eligible slot.
            None => match self.fixture.slots {
                BoundSlots::One(bound) => bound == slot_id,
                BoundSlots::All => true,
            },
            Some(Ok(items)) => items.iter().any(|item| {
                item.slot_id == slot_id
                    && item.dispatch.is_none()
                    && item.expected.as_ref().is_none_or(same)
            }),
            Some(Err(_)) => false,
        }
    }
}

/// Resolves to what the round's sweep sends, or `Unfinished` once it can't.
fn answer(round: u64, receive: oneshot::Receiver<Arc<Observed>>) -> Answer {
    let observed = async move {
        receive.await.unwrap_or_else(|_| {
            tracing::debug!(round, "firewall observation round did not finish");
            let answer = Err(Refusal::Unfinished);
            Arc::new(Observed { round, answer })
        })
    };
    observed.boxed().shared()
}

fn lock(rounds: &Mutex<Rounds>) -> std::sync::MutexGuard<'_, Rounds> {
    // Nothing panics while holding it.
    rounds
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
