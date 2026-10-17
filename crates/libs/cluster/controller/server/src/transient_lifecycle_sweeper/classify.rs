//! Verdict classification and action selection for the verify-before-destroy
//! sweep.
//!
//! The sweep used to CAS rows to `failed` first and clean up after. That flip
//! is lossy — the `failed` transition's DB triggers erase the evacuation
//! marker and restore lease — and it destroyed sandboxes whose VM was alive
//! and serving while only the Postgres row was stale. The probe therefore
//! runs *before* any flip, and this module turns each probe result into the
//! single action the sweep is allowed to take on that row.

use pplx_space_api::controllers::substrate::ObservedSandboxState;
use pplx_space_common_core::types::SandboxLifecycle;

use super::probe::SweepCandidate;

/// Closed classification of one sweep candidate after its node probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeVerdict {
    /// The node reports the Firecracker VM alive. The row is stale, not the
    /// sandbox — destruction is forbidden, except for a `creating` row whose
    /// create was abandoned, which [`decide`] fails and retires.
    AliveOnNode,
    /// The node knows the sandbox, the VM is not running, and a
    /// current-instance local artifact pointer exists. The node may hold the
    /// only copy of the sandbox's state.
    DormantWithLocalArtifact,
    /// The node knows the sandbox but the VM is not running and no local
    /// artifact pointer exists.
    DeadOnNode,
    /// The node has no record of the sandbox, or the row has no active
    /// execution to locate a node with.
    MissingOnNode,
    /// The probe failed, timed out, or did not cover this sandbox. Could be a
    /// dead node or a transient blip — indistinguishable on one sample, so
    /// the row is left alone.
    Inconclusive,
}

impl ProbeVerdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AliveOnNode => "alive_on_node",
            Self::DormantWithLocalArtifact => "dormant_with_local_artifact",
            Self::DeadOnNode => "dead_on_node",
            Self::MissingOnNode => "missing_on_node",
            Self::Inconclusive => "inconclusive",
        }
    }

    /// Whether the probe permits retiring a `failed` row's active execution:
    /// the node answered and reported no live VM. `DormantWithLocalArtifact`
    /// qualifies because the retire path preserves node-local state by
    /// design (it never sends the sbx-mgr delete that removes the
    /// filesystem and local-artifact rows, and it enqueues a stopped-disk
    /// backup before forgetting tracking when a runtime is reachable) — a
    /// salvageable artifact is no reason to keep the dead pod behind, and
    /// that pod is exactly what feeds kubelet's CreatePodSandbox bounce
    /// loop against a node-local `failed` state. Listed positively so a
    /// future verdict defaults to defer, not retire.
    pub const fn permits_failed_retire(self) -> bool {
        matches!(
            self,
            Self::DeadOnNode | Self::MissingOnNode | Self::DormantWithLocalArtifact
        )
    }
}

impl std::fmt::Display for ProbeVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<ObservedSandboxState> for ProbeVerdict {
    fn from(state: ObservedSandboxState) -> Self {
        match state {
            ObservedSandboxState::VmAlive { .. } => Self::AliveOnNode,
            ObservedSandboxState::DormantWithArtifact { .. } => Self::DormantWithLocalArtifact,
            ObservedSandboxState::DormantNoArtifact { .. } => Self::DeadOnNode,
            ObservedSandboxState::Missing => Self::MissingOnNode,
        }
    }
}

/// Full result from one node probe: the existing destroy-safety verdict plus
/// node-local lifecycle status when the node returned one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeObservation {
    pub verdict: ProbeVerdict,
    pub node_status: Option<SandboxLifecycle>,
}

impl ProbeObservation {
    pub const fn from_verdict(verdict: ProbeVerdict) -> Self {
        Self {
            verdict,
            node_status: None,
        }
    }

    pub const fn inconclusive() -> Self {
        Self::from_verdict(ProbeVerdict::Inconclusive)
    }
}

impl From<ObservedSandboxState> for ProbeObservation {
    fn from(state: ObservedSandboxState) -> Self {
        Self {
            verdict: ProbeVerdict::from(state),
            node_status: state.node_status(),
        }
    }
}

/// Outcome of the act-phase routability check for an alive `restoring` row
/// whose execution the DB reported unroutable ([`SweepAction::HydrateThenConverge`]).
/// Produced by `SweepSink::ensure_routable_execution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRoutability {
    /// The active execution is routable — either it already was (a concurrent
    /// hydration won the race), or the sink verified a live matching pod
    /// against the kube apiserver and hydrated the row from it. Converging to
    /// `started` is safe.
    Routable,
    /// No live Running+IP pod backs the active execution. `started` would be
    /// a promise the substrate can't keep — defer instead.
    NotRoutable,
}

/// The one action the sweep takes on a candidate this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepAction {
    /// Lease-guarded CAS of an alive `restoring` row to the wake target its
    /// VM already reached. Only chosen when the DB already shows a routable
    /// active execution — in the K8s substrate the wake target is a promise
    /// the substrate must be able to keep.
    Converge { target: SandboxLifecycle },
    /// An alive `restoring` row whose active execution the DB reports
    /// unroutable (no `running` + `pod_ip` execution). The act phase must
    /// verify routability against the substrate — hydrating the execution
    /// row from a live matching pod when one exists — and converge only on
    /// proof; otherwise the row defers ([`DeferReason::AliveNotRoutable`]).
    /// Converging blindly manufactures a `started` row without restore
    /// context, which the wake path's gen≥2 fresh-pod refusal turns into a
    /// user-visible wedge.
    HydrateThenConverge { target: SandboxLifecycle },
    /// Guarded CAS to `suspended`: the VM is gone but a ready durable
    /// snapshot recovers the sandbox on the next restore. Only `restoring`
    /// rows are eligible — see [`decide`] for why every other transient
    /// lifecycle's "ready snapshot" is a stale prior-cycle copy.
    RevertToSuspended,
    /// The old age-out behavior — CAS to `failed` and retire the execution.
    /// Reached when the probe proved the sandbox gone from its node and no
    /// durable snapshot exists, or for an alive `creating` row whose create
    /// was abandoned (see [`decide`]).
    FailAndRetire,
    /// No provably-safe action this tick; the row rolls to the next tick.
    Defer { reason: DeferReason },
}

/// Why a candidate was deferred, for the `sweep_deferred` event's `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferReason {
    /// The VM is alive in a lifecycle the sweep does not converge yet, or
    /// the row is `creating` and nothing shows its create was abandoned.
    AliveNotConvergeable,
    /// The VM is alive and the row is `restoring`, but no routable backing
    /// pod exists (DB unroutable and the substrate found no live matching
    /// pod to hydrate from). `restoring` stays — the wake path can recover
    /// it, and once node-side auto-suspend retires the VM the dormant/dead
    /// verdicts open the existing revert-to-suspended path. Emitted by the
    /// act phase, not by [`decide`].
    AliveNotRoutable,
    /// The node holds the only current copy of the sandbox's state: a local
    /// artifact with no ready durable snapshot, or one whose only matching
    /// snapshot is a stale prior-cycle copy (any lifecycle other than
    /// `restoring`). Destroying or reverting would lose data.
    OnlyCopyOnNode,
    /// The probe was inconclusive — unreachable-means-defer.
    ProbeInconclusive,
}

impl DeferReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AliveNotConvergeable => "alive_not_convergeable",
            Self::AliveNotRoutable => "alive_not_routable",
            Self::OnlyCopyOnNode => "only_copy_on_node",
            Self::ProbeInconclusive => "probe_inconclusive",
        }
    }
}

impl std::fmt::Display for DeferReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Pure decision table: probe verdict + row facts -> action. Exhaustive over
/// [`ProbeVerdict`] so adding a verdict forces a decision here.
///
/// ## Why `RevertToSuspended` is `restoring`-only
///
/// The `ready_snapshot` fact matches a ready durable backup against the row's
/// `current_local_artifact_id`. In the K8s substrate, that pointer is written
/// when suspend completion persists the published artifact identity; retire's
/// current-instance backup intentionally does not rewrite the stable sandbox
/// row. Nothing clears or updates the pointer on wake/restore. So for every
/// lifecycle whose VM ran after the last completed suspend (`suspending`,
/// `pausing`, `resuming`, `stopping`, `checkpointing`, and the create/start
/// family), a matching ready backup is by construction the *previous* cycle's
/// bytes: reverting
/// would CAS the row to `suspended`, the next restore would select that stale
/// snapshot by the same stale pointer, and the user would silently get
/// pre-wake state while the genuinely newest copy (a node-local artifact the
/// wedged transition published, or the crashed VM's rootfs) is bypassed and
/// eventually garbage-collected. No snapshot-table predicate can prove
/// otherwise — the writes made since the wake exist only on the node.
///
/// Only `restoring` is different: the pointer was set by the suspend that
/// preceded this restore and the VM has not run since (a live VM is handled
/// by the converge arm), so the snapshot is canonical by construction.
///
/// For the other lifecycles the safe actions are the loud ones: a dormant
/// local artifact defers (`OnlyCopyOnNode` — the node holds the only current
/// copy), and a dead/missing row takes the old fail-and-retire path, which is
/// visible and preserves node-local state for salvage, rather than a silent
/// rollback to stale bytes.
pub fn decide(candidate: &SweepCandidate, verdict: ProbeVerdict) -> SweepAction {
    let snapshot_is_canonical =
        candidate.lifecycle == SandboxLifecycle::Restoring && candidate.ready_snapshot;
    match verdict {
        ProbeVerdict::AliveOnNode if candidate.lifecycle == SandboxLifecycle::Restoring => {
            // The wake target the live VM already reached. Candidates carry
            // the marker only on the `suspending` exemption path, so for
            // `restoring` this is `started` in practice; the marker is read
            // anyway so an evacuation target is never downgraded.
            let target = candidate
                .evacuation_restore_target
                .unwrap_or(SandboxLifecycle::Started);
            if candidate.routable_execution {
                SweepAction::Converge { target }
            } else {
                // The VM is real but the wake target is a promise the
                // substrate can't currently keep: no routable execution
                // backs the row. Verify (and repair) against the substrate
                // before asserting it.
                SweepAction::HydrateThenConverge { target }
            }
        }
        // A create whose owner is gone after its VM came up: the candidate
        // select never lists a `creating` row whose activation lease is live
        // or recently expired, and `activation_abandoned` says this one's
        // expired a full window ago. It fails rather than converges. The
        // request that would have returned the sandbox died: a synchronous
        // create's client never got the id and usually retries into a second
        // sandbox, and nothing records what else the request asked for
        // (file-change tracking, say) for a converge to honor. There is no earlier
        // snapshot to protect, and the retire keeps node-local state.
        ProbeVerdict::AliveOnNode
            if candidate.lifecycle == SandboxLifecycle::Creating
                && candidate.activation_abandoned =>
        {
            SweepAction::FailAndRetire
        }
        ProbeVerdict::AliveOnNode => SweepAction::Defer {
            reason: DeferReason::AliveNotConvergeable,
        },
        ProbeVerdict::DormantWithLocalArtifact if snapshot_is_canonical => {
            SweepAction::RevertToSuspended
        }
        ProbeVerdict::DormantWithLocalArtifact if candidate.fork_restore => SweepAction::Defer {
            reason: DeferReason::OnlyCopyOnNode,
        },
        ProbeVerdict::DormantWithLocalArtifact
            if candidate.lifecycle == SandboxLifecycle::Restoring =>
        {
            SweepAction::FailAndRetire
        }
        ProbeVerdict::DormantWithLocalArtifact => SweepAction::Defer {
            reason: DeferReason::OnlyCopyOnNode,
        },
        ProbeVerdict::DeadOnNode | ProbeVerdict::MissingOnNode if snapshot_is_canonical => {
            SweepAction::RevertToSuspended
        }
        ProbeVerdict::DeadOnNode | ProbeVerdict::MissingOnNode => SweepAction::FailAndRetire,
        ProbeVerdict::Inconclusive => SweepAction::Defer {
            reason: DeferReason::ProbeInconclusive,
        },
    }
}

#[cfg(test)]
mod tests;
