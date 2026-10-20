//! Typed outcomes, one small closed enum per operation, plus the claim-kind
//! and loss vocabularies the operations take as input.

use jiff::Timestamp;
use pplx_space_common_core::types::SandboxId;

use crate::lifecycle::StoredSandboxLifecycle;
use crate::model::{ArtifactRef, Attempt};

/// Which protocol a placement claim runs under. All kinds mint the same
/// fence; the kind selects the guard set the claim re-checks inside its
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimKind {
    /// First placement of a freshly created sandbox.
    OnDemandCreate,
    /// A user-initiated wake of a suspended sandbox.
    UserRestore,
    /// The node-drain cohort restore, gated on the evacuation marker.
    EvacuationRestore,
    /// Adoption of a pre-warmed pool slot.
    PooledClaim,
}

impl ClaimKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OnDemandCreate => "on_demand_create",
            Self::UserRestore => "user_restore",
            Self::EvacuationRestore => "evacuation_restore",
            Self::PooledClaim => "pooled_claim",
        }
    }
}

impl std::fmt::Display for ClaimKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome of [`crate::PlacementStore::claim_placement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The caller now owns the row; the fence is its proof.
    Claimed(Attempt),
    /// An unexpired lease owns the row; its holder will complete or abandon.
    AlreadyOwned { lease_expires_at: Timestamp },
    /// Backoff from a prior failed attempt has not elapsed.
    Backoff { next_attempt_at: Timestamp },
    /// Teardown or terminal lifecycle; nothing may place this sandbox.
    Blocked(BlockedReason),
}

/// Why a claim may not place a sandbox at all (as opposed to losing a race
/// with another claimant, which is [`ClaimOutcome::AlreadyOwned`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedReason {
    /// The stored lifecycle is outside the claimable window for the
    /// requested claim kind.
    Lifecycle(StoredSandboxLifecycle),
    /// The evacuation marker is gone — a user wake took the row over or the
    /// restore finalized. Drop it, don't retry: re-claiming can never
    /// succeed and a deadline would be a hot loop.
    EvacuationSuperseded,
    /// A stopped evacuation whose teardown hasn't released it: the row is
    /// still on its node, and placing it would give it two homes. It stops
    /// blocking once `active_node_id` clears (the lease and backoff gates
    /// still apply), so a caller should retry it rather than drop it.
    StoppedEvacuationNotReleased,
}

/// Outcome of [`crate::PlacementStore::complete_placement`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementOutcome {
    /// The row flipped to Placed.
    Completed,
    /// A newer suspend claim owns the row; nothing was written. The
    /// node-side work succeeded, so the caller continues — a handoff, not a
    /// loss.
    SupersededBySuspend,
    /// A teardown won the race or the fence is stale. Nothing was written;
    /// the caller must surface the failure, not retry with this fence.
    CasLost,
}

/// Every abandonment classifies its loss; there is no disposition that
/// silently discards a durable artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDisposition {
    /// A durable artifact exists; project the sandbox back to it.
    Preserve { artifact: ArtifactRef },
    /// Evidence unsettled (an upload may still finalize); retry later.
    Defer { until: Timestamp },
    /// A stale attempt superseded by a new claim.
    Replace,
    /// Nothing durable; rebuild from the template.
    Recreate,
}

/// What an abandonment costs, computed from the durable-artifact predicates
/// before anything is written. There is no unclassified variant: an abandon
/// that cannot say what it loses may not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossClassification {
    /// A durable artifact bounds the loss to work since its checkpoint —
    /// the checkpoint cadence is the RPO.
    BoundedByArtifact { artifact: ArtifactRef },
    /// Nothing durable backs the row; the whole execution state is lost.
    Everything,
}

// The outcomes below belong to operations declared ahead of their ports —
// minimal shapes, `#[non_exhaustive]` until each port PR refines them
// against the real guards.

/// Outcome of [`crate::PlacementStore::create`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Created {
    pub sandbox: SandboxId,
}

/// Outcome of [`crate::PlacementEvidence::bind_pod`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BindOutcome {
    /// The pod evidence was recorded against the fenced attempt.
    Bound,
    /// The fence is stale; nothing was written.
    CasLost,
}

/// Outcome of [`crate::PlacementStore::begin_suspend`] and
/// [`crate::PlacementStore::complete_suspend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SuspendOutcome {
    /// The guarded step applied: begin opened the suspend window, complete
    /// projected the row to `Unplaced` + artifact.
    Advanced,
    /// The generation is stale or a racing writer moved the row; nothing
    /// was written.
    CasLost,
}

/// Outcome of [`crate::PlacementStore::abandon`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AbandonOutcome {
    /// The attempt was abandoned; the disposition says how the sandbox
    /// recovers.
    Abandoned { disposition: RecoveryDisposition },
    /// The fence is stale or an unexpired lease still owns the row; nothing
    /// was written.
    CasLost,
}

/// Outcome of one [`crate::PlacementStore::evacuate`] leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvacuationOutcome {
    /// The requested drain leg applied.
    Advanced,
    /// The guard matched zero rows — a racing writer moved the sandbox;
    /// nothing was written.
    CasLost,
}

/// One resident's fate from [`crate::PlacementStore::node_loss`]: bounded
/// by a durable artifact, or lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeLossOutcome {
    pub sandbox: SandboxId,
    pub loss: LossClassification,
}

/// Outcome of [`crate::PlacementStore::quarantine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuarantineOutcome {
    /// The row was unplaced and the hold record written in the same
    /// transaction.
    Held,
    /// The guard matched zero rows; nothing was written.
    CasLost,
}

/// Outcome of [`crate::PlacementStore::finalize_delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeleteOutcome {
    /// The tombstone landed; unplacement preceded the row delete.
    Finalized,
    /// No delete intent on the row, or a racing writer moved it; nothing
    /// was written.
    CasLost,
}

/// Outcome of [`crate::PlacementStore::admin_override`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverrideOutcome {
    /// The override applied and was audited.
    Applied,
}

#[cfg(test)]
mod tests;
