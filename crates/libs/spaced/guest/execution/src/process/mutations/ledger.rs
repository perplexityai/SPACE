//! One owner and a bounded terminal history for each process incarnation.
use pplx_space_spaced_guest_protocol::recovery::RecoveryReason;
use std::{
    collections::VecDeque,
    num::NonZeroU64,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;
use tonic::{Code, Status};

const RECEIPTS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Binding {
    Input([u8; 32]),
    Close,
    Signal(i32),
}

/// Only bounded, server-generated outcomes survive the request payload.
#[derive(Clone, Debug)]
pub(super) enum Outcome {
    Applied,
    Failed { code: Code, message: String },
    Interrupted { written: u64 },
    Unknown,
    NotBegun,
}
impl Outcome {
    pub(super) fn result(self) -> Result<(), Status> {
        match self {
            Self::Applied => Ok(()),
            Self::Failed { code, message } => Err(Status::new(code, message)),
            Self::Interrupted { written } => {
                let mut status = RecoveryReason::InputOutcomeUncertain.into_status();
                status.metadata_mut().insert(
                    "space-process-input-written",
                    written
                        .to_string()
                        .parse()
                        .map_err(|_| Status::internal("invalid written-byte metadata"))?,
                );
                Err(status)
            }
            Self::Unknown => Err(RecoveryReason::MutationOutcomeUnknown.into_status()),
            Self::NotBegun => Err(crate::refused(Status::unavailable(
                "process mutation cancelled before start",
            ))),
        }
    }
    pub(super) fn from_result(result: Result<(), Status>) -> Self {
        match result {
            Ok(()) => Self::Applied,
            Err(status) => Self::Failed {
                code: status.code(),
                message: status.message().chars().take(256).collect(),
            },
        }
    }
}

struct Receipt {
    sequence: NonZeroU64,
    binding: Binding,
    outcome: Outcome,
}
struct Pending {
    sequence: NonZeroU64,
    binding: Binding,
    result: watch::Sender<Option<Outcome>>,
}
#[derive(Default)]
struct State {
    high_water: u64,
    pending: Option<Pending>,
    receipts: VecDeque<Receipt>,
}
#[derive(Default)]
pub(in crate::process) struct Ledger(Mutex<State>);

pub(super) enum Reservation {
    Owner(Owner),
    Wait(watch::Receiver<Option<Outcome>>),
    Replay(Outcome),
}
impl Ledger {
    #[cfg(test)]
    pub(in crate::process) fn has_pending(&self) -> bool {
        self.0.lock().unwrap().pending.is_some()
    }

    #[cfg(test)]
    pub(in crate::process) fn has_waiter(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .is_some_and(|pending| pending.result.receiver_count() > 0)
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        sequence: NonZeroU64,
        binding: Binding,
    ) -> Result<Reservation, Status> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(receipt) = state
            .receipts
            .iter()
            .find(|receipt| receipt.sequence == sequence)
        {
            require_match(receipt.binding, binding)?;
            return Ok(Reservation::Replay(receipt.outcome.clone()));
        }
        if sequence.get() <= state.high_water {
            return Err(RecoveryReason::MutationReceiptExpired.into_status());
        }
        if let Some(pending) = &state.pending {
            if pending.sequence == sequence {
                require_match(pending.binding, binding)?;
                return Ok(Reservation::Wait(pending.result.subscribe()));
            }
            return Err(RecoveryReason::MutationOutOfOrder.into_status());
        }
        if state.high_water.checked_add(1) != Some(sequence.get()) {
            return Err(RecoveryReason::MutationOutOfOrder.into_status());
        }
        let (result, _) = watch::channel(None);
        state.pending = Some(Pending {
            sequence,
            binding,
            result,
        });
        Ok(Reservation::Owner(Owner {
            ledger: Arc::clone(self),
            phase: OwnerPhase::Reserved,
        }))
    }
}
fn require_match(expected: Binding, supplied: Binding) -> Result<(), Status> {
    if expected != supplied {
        return Err(Status::invalid_argument(
            "mutation sequence is bound to a different operation or payload",
        ));
    }
    Ok(())
}

enum OwnerPhase {
    Reserved,
    Applying,
    Settled,
}
pub(super) struct Owner {
    ledger: Arc<Ledger>,
    phase: OwnerPhase,
}
impl Owner {
    pub(super) fn begin(&mut self) {
        self.phase = OwnerPhase::Applying;
    }
    pub(super) fn complete(mut self, outcome: Outcome) {
        self.settle(outcome);
    }
    fn settle(&mut self, outcome: Outcome) {
        let mut state = self
            .ledger
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state
            .pending
            .take()
            .expect("owner holds the only reservation");
        if matches!(self.phase, OwnerPhase::Applying) {
            state.high_water = pending.sequence.get();
            if state.receipts.len() == RECEIPTS {
                state.receipts.pop_front();
            }
            state.receipts.push_back(Receipt {
                sequence: pending.sequence,
                binding: pending.binding,
                outcome: outcome.clone(),
            });
        }
        pending.result.send_replace(Some(outcome));
        self.phase = OwnerPhase::Settled;
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        match self.phase {
            OwnerPhase::Reserved => self.settle(Outcome::NotBegun),
            OwnerPhase::Applying => self.settle(Outcome::Unknown),
            OwnerPhase::Settled => {}
        }
    }
}

pub(super) async fn wait(mut receiver: watch::Receiver<Option<Outcome>>) -> Outcome {
    loop {
        if let Some(outcome) = receiver.borrow_and_update().clone() {
            return outcome;
        }
        if receiver.changed().await.is_err() {
            return Outcome::Unknown;
        }
    }
}

#[cfg(test)]
mod tests;
