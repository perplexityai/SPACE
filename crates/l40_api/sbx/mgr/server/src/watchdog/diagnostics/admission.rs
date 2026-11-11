//! Admission of evidence reports for degrading health transitions.
//!
//! The queue holds at most one waiting report per sandbox and at most
//! [`EVIDENCE_QUEUE_CAPACITY`] in total, so correlated failures and flapping
//! cost a fixed amount of memory however long collectors fall behind. A
//! repeat transition for a sandbox that is already waiting merges into its
//! report; a transition that finds the queue full is dropped. Either way the
//! transition itself was already logged.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use super::{ConsoleTails, TransitionId};

/// Reports gathered at once, by that many long-lived collector tasks.
pub(in crate::watchdog) const EVIDENCE_COLLECTORS: usize = 4;
/// Reports waiting for a collector. Collectors drain about four per second,
/// so a full queue is about 16 s of backlog; evidence older than that says
/// little about the transition that asked for it.
pub(in crate::watchdog) const EVIDENCE_QUEUE_CAPACITY: usize = 64;

/// A waiting evidence report: only what a collector needs, since the
/// transition line already carries the probe state.
#[derive(Debug)]
pub(in crate::watchdog) struct EvidenceRequest {
    pub(super) sandbox_id: String,
    /// The newest transition this report covers.
    pub(super) transition_id: TransitionId,
    pub(super) console_tails: ConsoleTails,
    /// When the first transition this report covers was admitted.
    pub(super) queued_at: Instant,
    /// Later transitions folded into this report while it waited.
    pub(super) merged_transitions: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(in crate::watchdog) enum Admission {
    Queued,
    /// Folded into the sandbox's waiting report.
    Merged,
    /// The queue was full.
    Dropped,
}

impl Admission {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Merged => "merged",
            Self::Dropped => "dropped",
        }
    }
}

pub(in crate::watchdog) struct EvidenceQueue {
    waiting: Mutex<VecDeque<EvidenceRequest>>,
    capacity: usize,
    ready: tokio::sync::Notify,
}

impl EvidenceQueue {
    pub(in crate::watchdog) fn new(capacity: usize) -> Self {
        Self {
            waiting: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            ready: tokio::sync::Notify::new(),
        }
    }

    /// Admit `request` without waiting. A scan of the bounded queue finds
    /// the sandbox's waiting report, if any.
    pub(in crate::watchdog) fn admit(&self, request: EvidenceRequest) -> Admission {
        let mut waiting = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(report) = waiting
            .iter_mut()
            .find(|report| report.sandbox_id == request.sandbox_id)
        {
            report.transition_id = request.transition_id;
            report.console_tails = report.console_tails.merge(request.console_tails);
            report.merged_transitions = report.merged_transitions.saturating_add(1);
            return Admission::Merged;
        }
        if waiting.len() >= self.capacity {
            return Admission::Dropped;
        }
        waiting.push_back(request);
        drop(waiting);
        self.ready.notify_one();
        Admission::Queued
    }

    /// The oldest waiting report, once there is one. Cancellation safe: a
    /// report is removed only when this returns it.
    pub(in crate::watchdog) async fn next(&self) -> EvidenceRequest {
        loop {
            if let Some(request) = self.pop() {
                return request;
            }
            // `notify_one` keeps a permit when no collector is waiting, so a
            // report admitted since `pop` wakes this at once.
            self.ready.notified().await;
        }
    }

    fn pop(&self) -> Option<EvidenceRequest> {
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests;
