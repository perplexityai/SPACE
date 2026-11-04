//! The typed outcomes [`crate::PlacementExecutor::place`] answers with.

use jiff::Timestamp;
use pplx_space_common_core::types::NodeId;
use pplx_space_placement::{BlockedReason, LossClassification, PodRef};

/// Every `place()` call ends in exactly one of these. Race losses are not
/// errors: exactly one concurrent caller wins the machine's claim, and every
/// other caller is told, precisely, what to wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceOutcome {
    /// The sandbox is Placed; the binding is what routing needs.
    Placed(PlacedBinding),
    /// Another attempt owns the row — or this call's own attempt outlived
    /// the budget and is still moving. Re-call `place()`; before `deadline`
    /// that is a cheap read, after it the re-claim path opens.
    Wait { deadline: Timestamp },
    /// Retry throttled by the failure-backoff schedule.
    Backoff { next_attempt_at: Timestamp },
    /// Nothing may place this sandbox (teardown, terminal lifecycle,
    /// superseded evacuation). Surface it; do not retry.
    Blocked(BlockedReason),
    /// This call claimed, failed to materialize, and abandoned with a
    /// classified loss. A subsequent `place()` may claim again.
    Abandoned { loss: LossClassification },
}

/// What routing needs — and nothing else. The fencing vocabulary
/// (generation, lease) does not appear on the public surface; it stays
/// between the executor and the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedBinding {
    pub node: NodeId,
    pub pod: PodRef,
}
