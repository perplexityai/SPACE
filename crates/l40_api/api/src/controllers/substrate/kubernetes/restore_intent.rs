//! What a restore through the shared executor decides differently for the
//! user restore and wake paths and for the node drain's evacuation, built
//! once when the restore is asked for.

use super::RestoreSnapshotChoice;
use super::execution::WakeTarget;
use crate::controllers::substrate::{ActivationAttribution, CapPolicy, RecoveryCharge};
use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::types::SandboxLifecycle;

/// How a restore claims the sandbox row, and what it does when the claim is
/// taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RestoreClaimMode {
    /// The user restore lease (`begin_restore_lifecycle`). It takes a
    /// `suspended` row whether or not the evacuation marker is set, and clears
    /// the marker, but never a `restoring` row that still carries it: that is
    /// a drain restore in flight.
    UserLease {
        /// Whether losing the lease to a live owner joins that owner's
        /// attempt inside the readiness budget instead of failing fast.
        joins_in_flight: bool,
        /// Whether a `Conflict` claim hands back the binding of a user restore
        /// that already completed. A wake that has yet to claim may also retry
        /// the claim, within its readiness budget, while the row stays
        /// `suspended` or `restoring`. Without it a `Conflict` fails at once.
        joins_completed_on_conflict: bool,
    },
    /// The drain's claim on the `evacuation_restore_target` marker
    /// (`begin_evacuation_restore_lifecycle`), the only claim a `stopped`
    /// target may take. It joins nothing and fails fast: the drain owns its
    /// retry loop.
    EvacuationMarker,
}

impl RestoreClaimMode {
    pub(super) fn joins_in_flight(self) -> bool {
        matches!(
            self,
            Self::UserLease {
                joins_in_flight: true,
                ..
            }
        )
    }

    pub(super) fn joins_completed_on_conflict(self) -> bool {
        matches!(
            self,
            Self::UserLease {
                joins_completed_on_conflict: true,
                ..
            }
        )
    }
}

/// How the restore pod is placed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RestorePodPlacement {
    /// The restore plan's source node pins or preferentially places the pod
    /// (`parse_pod_create_context`'s user restore shapes).
    SourceAffine,
    /// No source-node pin. With a `cohort`, its toleration and required node
    /// affinity confine the pod to the active target node group; without one
    /// the pod carries no extra placement. A `stopped` re-placement keeps off
    /// the nodes earlier executions ran on.
    Evacuation { cohort: Option<EvacuationPlacement> },
}

/// Every decision the shared restore executor
/// ([`super::KubernetesSubstrate::restore_execution_inner`] and the wake it
/// drives) makes differently for a user restore or wake and for the drain's
/// evacuation restore. Owned: the executor moves a clone into the detached
/// task that drives node work.
#[derive(Clone)]
pub(super) struct RestoreIntent {
    /// The lifecycle the restore lands the sandbox in, and for an implicit
    /// wake the activity that woke it.
    pub(super) target: WakeTarget,
    pub(super) claim: RestoreClaimMode,
    pub(super) snapshot: RestoreSnapshotChoice,
    pub(super) pod_placement: RestorePodPlacement,
    /// Whether the restore may move the sandbox to a newer runtime
    /// environment. The drain's keeps the sandbox's lifecycle and
    /// environment.
    pub(super) allows_runtime_upgrade: bool,
    /// What the claim winner pays, and what a re-driven started recovery
    /// pays. A preclaimed wake's drive runs as already paid: its caller paid
    /// for the claim.
    pub(super) charge: RecoveryCharge,
    /// Whether a wake that has yet to claim a `suspended` or `restoring` row
    /// goes through the pod-local wake coalescer, so pod-local contenders
    /// share one run. An implicit wake (one that carries its activity) never
    /// does.
    pub(super) coalesces: bool,
    /// Whether the claim's lease owner commits `restoring -> started` itself
    /// before handing out the binding. Otherwise the caller commits the
    /// target (the drain moves the row out of `restoring`).
    pub(super) owner_commits_started: bool,
    /// Whether a started recovery this wake owns is re-driven as a restore of
    /// the suspended sandbox when a suspend supersedes it during or after the
    /// wait. A supersession found while claiming or joining the started
    /// recovery is re-driven either way.
    pub(super) redrives_started_recovery: bool,
}

impl RestoreIntent {
    /// A user restore or wake toward `target`, paying `charge` when it wins
    /// the claim.
    pub(super) fn user(charge: RecoveryCharge, target: WakeTarget) -> Self {
        Self {
            target,
            claim: RestoreClaimMode::UserLease {
                joins_in_flight: true,
                joins_completed_on_conflict: true,
            },
            snapshot: RestoreSnapshotChoice::SourceNodeOrFresh,
            pod_placement: RestorePodPlacement::SourceAffine,
            allows_runtime_upgrade: true,
            charge,
            coalesces: true,
            owner_commits_started: true,
            redrives_started_recovery: true,
        }
    }

    /// The node drain's evacuation restore toward `target`, placed in
    /// `cohort` when a node-group rollout is active. Exempt from the charge;
    /// the restored pod is labeled from the sandbox's record.
    pub(super) fn drain(cohort: Option<EvacuationPlacement>, target: SandboxLifecycle) -> Self {
        Self {
            target: WakeTarget::Explicit(target),
            claim: RestoreClaimMode::EvacuationMarker,
            snapshot: RestoreSnapshotChoice::NewestRemote,
            pod_placement: RestorePodPlacement::Evacuation { cohort },
            allows_runtime_upgrade: false,
            charge: RecoveryCharge::exempt(
                CapPolicy::Skip,
                ActivationAttribution::inherit_unresolved(),
            ),
            coalesces: false,
            owner_commits_started: false,
            redrives_started_recovery: false,
        }
    }

    /// `restore`'s snapshot choice; a materialization without a restore
    /// intent selects as a user restore does.
    pub(super) fn snapshot_or_default(restore: Option<&Self>) -> RestoreSnapshotChoice {
        restore.map_or(RestoreSnapshotChoice::SourceNodeOrFresh, |intent| {
            intent.snapshot
        })
    }
}
