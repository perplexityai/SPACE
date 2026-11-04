//! The one request shape [`crate::PlacementExecutor::place`] takes.

use std::time::Duration;

use pplx_space_common_core::types::SandboxId;

/// Self-contained: the request carries its sandbox, so one value is the whole
/// call — loggable, queueable, retryable as one unit — consistent with the
/// machine's own request shapes (`CreateSandbox` carries its sandbox) and
/// with the proof-carries-identity precedent on `complete_placement`.
#[derive(Debug, Clone)]
pub struct PlaceRequest {
    /// The sandbox to place.
    pub sandbox: SandboxId,
    /// Which claim protocol the placement runs under.
    pub intent: PlaceIntent,
    /// The caller's patience. `place()` never blocks past it; api handlers
    /// pass a budget inside their own latency bound (the <10s rule).
    pub budget: Duration,
}

impl PlaceRequest {
    /// A user-initiated wake.
    pub fn user_wake(sandbox: SandboxId, budget: Duration) -> Self {
        Self {
            sandbox,
            intent: PlaceIntent::UserWake,
            budget,
        }
    }
}

/// Which claim protocol the placement runs under. Wake-only at first;
/// `Create` lands with its rung. The reconciler-side verbs (suspend,
/// evacuate, delete drivers) are future work and join as methods on the
/// executor in their own rungs, not as new intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceIntent {
    /// A user-initiated wake of a suspended sandbox.
    UserWake,
}
