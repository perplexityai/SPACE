use std::{fmt, str::FromStr};

use crate::PoolError;

/// Lifecycle of a warm-pool slot row. Transitions are verb methods that
/// consume the current state and fail on illegal edges:
///
/// ```text
/// warming ─mark_ready→ ready ─claim→ claimed ─bind→ bound ─cleanup_pending→ cleanup_pending
///                        ▲                 │                     ▲
///                        └────release──────┼─────cleanup_pending─┘
/// ```
///
/// `claim` is the claim transaction of the claim-bind handoff; `bind` is the
/// bind transaction committing sandbox ownership of the pod, stamped in the
/// same transaction so a `claimed` row older than the claim grace period is a
/// handoff that died; `release` abandons a claim before any sandbox rows
/// reference the pod; `cleanup_pending` parks a post-claim boot failure as a
/// durable work item for the warm-pool reconciler (legal from `claimed` while
/// pre-bind rows can still exist, and from `bound` thereafter). Row deletion
/// is legal from any state and is guarded by the store's state-scoped DELETE
/// statements, not by a transition here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolSlotState {
    Warming,
    Ready,
    Claimed,
    Bound,
    CleanupPending,
}

impl PoolSlotState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warming => "warming",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Bound => "bound",
            Self::CleanupPending => "cleanup_pending",
        }
    }

    /// The warming pod was observed running and ready: the slot joins the
    /// claimable inventory.
    pub fn mark_ready(self) -> Result<Self, PoolError> {
        match self {
            Self::Warming => Ok(Self::Ready),
            from => Err(from.invalid_transition(Self::Ready)),
        }
    }

    /// A create request takes the slot out of the inventory.
    pub fn claim(self) -> Result<Self, PoolError> {
        match self {
            Self::Ready => Ok(Self::Claimed),
            from => Err(from.invalid_transition(Self::Claimed)),
        }
    }

    /// The bind transaction committed: sandbox rows now own the pod and the
    /// slot row is only a retirement receipt. Stamped inside that same
    /// transaction, so its absence on an old `claimed` row proves the
    /// handoff died.
    pub fn bind(self) -> Result<Self, PoolError> {
        match self {
            Self::Claimed => Ok(Self::Bound),
            from => Err(from.invalid_transition(Self::Bound)),
        }
    }

    /// The claim is abandoned before the sandbox bind (the claimed pod
    /// failed validation), returning the still-warm slot to the inventory.
    pub fn release(self) -> Result<Self, PoolError> {
        match self {
            Self::Claimed => Ok(Self::Ready),
            from => Err(from.invalid_transition(Self::Ready)),
        }
    }

    /// The sandbox boot failed after the claim: the slot becomes a durable
    /// work item owing a pod delete and sandbox finalize, drained by the
    /// warm-pool reconciler.
    pub fn cleanup_pending(self) -> Result<Self, PoolError> {
        match self {
            Self::Claimed | Self::Bound => Ok(Self::CleanupPending),
            from => Err(from.invalid_transition(Self::CleanupPending)),
        }
    }

    fn invalid_transition(self, to: Self) -> PoolError {
        PoolError::InvalidSlotTransition {
            from: self.as_str(),
            to: to.as_str(),
        }
    }
}

impl fmt::Display for PoolSlotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PoolSlotState {
    type Err = PoolError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "warming" => Ok(Self::Warming),
            "ready" => Ok(Self::Ready),
            "claimed" => Ok(Self::Claimed),
            "bound" => Ok(Self::Bound),
            "cleanup_pending" => Ok(Self::CleanupPending),
            _ => Err(PoolError::InvalidSlotState(value.to_string())),
        }
    }
}
