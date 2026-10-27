//! Unified metric + structured-log emission for the cluster-controller's
//! sandbox repair/sweep loops.
//!
//! Names live in the `cluster_reconciler.*` namespace. Nine loops share the family, distinguished by the
//! `loop` tag: `started_sandbox_repair`, `failed_restorable_repair`,
//! `transient_lifecycle_sweeper`, `suspended_execution_reaper`,
//! `stuck_snapshot_reaper`, `stuck_container_creating_gc`,
//! `warm_pool_orphan_gc`, `orphaned_restore_pod_gc`, and
//! `lost_started_sandboxes`. Loops with
//! multiple sub-sweeps further split on the `sweep` tag; single-sweep
//! loops carry `sweep:main`.
//!
//! ## Outcome vocabulary
//!
//! `outcome` values are self-describing phrases with globally consistent
//! semantics. Two tiers:
//!
//! - **No-ops** — `deferred`, `skipped`, `cas_lost`, `error` — mean the same
//!   thing in every sweep (left alone / guard refused / lost the write race /
//!   the reconciler's own operation broke). The per-case cause lives in the
//!   `reason` tag.
//! - **State changes** — every mutation gets an explicit phrase naming what
//!   was changed (`reverted_to_durable_snapshot`, `gave_up_guest_wedged`,
//!   `retired_stale_execution`, ...). Lifecycle transitions additionally
//!   carry `from_lifecycle` / `to_lifecycle` tags; `to_lifecycle` is fixed
//!   per outcome by the constructors so the outcome phrase and the tag can
//!   never disagree. `outcome:gave_up_*` and `to_lifecycle:failed` are two
//!   equivalent ways to ask "what did the platform give up on?".
//!
//! Every [`OutcomeEvent`] emission produces BOTH one
//! `cluster_reconciler.outcome` counter increment and exactly one tracing
//! event with `target: "cluster_reconciler"` carrying the same tag values
//! plus the high-cardinality identifiers (`sandbox_id`, `pod_name`,
//! `snapshot_id`, error strings) that must never become metric tags. The
//! 1:1 pairing means a counter bump on a dashboard filters down to the exact
//! sandbox rows via `target:cluster_reconciler loop:X sweep:Y outcome:Z` in
//! the log index. No-op outcomes log at `debug`, so they need
//! `cluster_reconciler=debug` in the log filter to surface; the metric side
//! is emitted regardless of level.
//!
//! Illegal `(loop, sweep, outcome)` combinations are unrepresentable: each
//! sweep has its own outcome enum, and the [`OutcomeEvent`] constructors pin
//! the triple together.

use pplx_space_common_core::types::{SandboxId, SnapshotId};

use std::fmt;
use std::time::Instant;

use crate::emit::{counter, elapsed_ms, tag, timing_ms};

/// The reconciler loop emitting the metric; the `loop` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loop {
    FailedRestorableRepair,
    StartedSandboxRepair,
    TransientLifecycleSweeper,
    SuspendedExecutionReaper,
    StuckSnapshotReaper,
    StuckContainerCreatingGc,
    WarmPoolOrphanGc,
    OrphanedRestorePodGc,
    LostStartedSandboxes,
}

impl Loop {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FailedRestorableRepair => "failed_restorable_repair",
            Self::StartedSandboxRepair => "started_sandbox_repair",
            Self::TransientLifecycleSweeper => "transient_lifecycle_sweeper",
            Self::SuspendedExecutionReaper => "suspended_execution_reaper",
            Self::StuckSnapshotReaper => "stuck_snapshot_reaper",
            Self::StuckContainerCreatingGc => "stuck_container_creating_gc",
            Self::WarmPoolOrphanGc => "warm_pool_orphan_gc",
            Self::OrphanedRestorePodGc => "orphaned_restore_pod_gc",
            Self::LostStartedSandboxes => "lost_started_sandboxes",
        }
    }
}

impl fmt::Display for Loop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The sub-sweep within a loop; the `sweep` tag. Loops with a single sweep
/// carry [`Sweep::Main`] so the `outcome` metric's tag set is uniform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sweep {
    Main,
    AgedTransient,
    ExecutionHydration,
    FailedCleanup,
    SubstrateCleanup,
    CommandReadyPending,
    UnrestorableRestoring,
}

impl Sweep {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::AgedTransient => "aged_transient",
            Self::ExecutionHydration => "execution_hydration",
            Self::FailedCleanup => "failed_cleanup",
            Self::SubstrateCleanup => "substrate_cleanup",
            Self::CommandReadyPending => "command_ready_pending",
            Self::UnrestorableRestoring => "unrestorable_restoring",
        }
    }
}

impl fmt::Display for Sweep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether the emitting pass ran with writes enabled or in dry-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    DryRun,
    Live,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Live => "live",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The wake state a converge landed the row in; becomes `to_lifecycle`.
/// `started` in the normal case; `paused` when an evacuation-restore marker
/// says the VM was restored into a paused state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeLifecycle {
    Started,
    Paused,
}

impl WakeLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Paused => "paused",
        }
    }
}

const LIFECYCLE_SUSPENDED: &str = "suspended";
const LIFECYCLE_STARTED: &str = "started";
const LIFECYCLE_FAILED: &str = "failed";

const OUTCOME_DEFERRED: &str = "deferred";
const OUTCOME_SKIPPED: &str = "skipped";
const OUTCOME_CAS_LOST: &str = "cas_lost";
const OUTCOME_ERROR: &str = "error";

/// Per-candidate result of the snapshot-revert repair loops
/// (`loop:started_sandbox_repair` and `loop:failed_restorable_repair`, both
/// `sweep:main`). The constructors pin `from_lifecycle` per loop (`started`
/// / `failed`); reverts always land in `to_lifecycle:suspended`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairOutcome {
    /// Row moved to `suspended`, marker preserved (its own snapshot is
    /// current).
    RevertedToDurableSnapshot,
    /// Row moved to `suspended` and the stale marker cleared so restore
    /// selects the newest durable snapshot.
    RevertedToNewestSnapshotMarkerCleared,
    /// Left alone: the tracked pod still exists (`reason:pod_present`).
    Skipped,
    Deferred,
    CasLost,
    Error,
}

impl RepairOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RevertedToDurableSnapshot => "reverted_to_durable_snapshot",
            Self::RevertedToNewestSnapshotMarkerCleared => {
                "reverted_to_newest_snapshot_marker_cleared"
            }
            Self::Skipped => OUTCOME_SKIPPED,
            Self::Deferred => OUTCOME_DEFERRED,
            Self::CasLost => OUTCOME_CAS_LOST,
            Self::Error => OUTCOME_ERROR,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::RevertedToDurableSnapshot | Self::RevertedToNewestSnapshotMarkerCleared => {
                Some(LIFECYCLE_SUSPENDED)
            }
            Self::Skipped | Self::Deferred | Self::CasLost | Self::Error => None,
        }
    }
}

/// Per-candidate resolution of the transient-lifecycle sweeper's
/// verify-before-destroy sweep (`sweep:aged_transient`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgedTransientOutcome {
    /// Live VM, row was stale: CAS'd to the wake state the VM already
    /// reached (`reason:vm_alive_row_stale`).
    ConvergedStaleRowToLiveVm(WakeLifecycle),
    /// Same converge, but the DB-unroutable execution was first hydrated
    /// from its live pod (`reason:vm_alive_hydrated_execution`).
    ConvergedAfterHydratingExecution(WakeLifecycle),
    /// No live VM but a ready durable snapshot: CAS'd to `suspended`.
    RevertedToDurableSnapshot,
    /// Gone from the node with nothing provably recoverable: CAS'd to
    /// `failed` (`reason:transient_lifecycle_age_out`).
    GaveUpNothingRecoverable,
    Deferred,
    CasLost,
}

impl AgedTransientOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConvergedStaleRowToLiveVm(_) => "converged_stale_row_to_live_vm",
            Self::ConvergedAfterHydratingExecution(_) => "converged_after_hydrating_execution",
            Self::RevertedToDurableSnapshot => "reverted_to_durable_snapshot",
            Self::GaveUpNothingRecoverable => "gave_up_nothing_recoverable",
            Self::Deferred => OUTCOME_DEFERRED,
            Self::CasLost => OUTCOME_CAS_LOST,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::ConvergedStaleRowToLiveVm(wake)
            | Self::ConvergedAfterHydratingExecution(wake) => Some(wake.as_str()),
            Self::RevertedToDurableSnapshot => Some(LIFECYCLE_SUSPENDED),
            Self::GaveUpNothingRecoverable => Some(LIFECYCLE_FAILED),
            Self::Deferred | Self::CasLost => None,
        }
    }
}

/// Kube-verified execution hydration attempted before an alive `restoring`
/// row may converge (`sweep:execution_hydration`). No lifecycle transition:
/// this sweep mutates the execution row, not the sandbox lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationOutcome {
    /// `pod_uid`/`pod_ip` persisted onto the execution row from a live
    /// matching pod.
    HydratedExecutionFromLivePod,
    /// A concurrent hydration won the race (`reason:already_routable`).
    Skipped,
    /// No live matching pod; the row stays `restoring`
    /// (`reason:no_routable_pod`).
    Deferred,
    Error,
}

impl HydrationOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HydratedExecutionFromLivePod => "hydrated_execution_from_live_pod",
            Self::Skipped => OUTCOME_SKIPPED,
            Self::Deferred => OUTCOME_DEFERRED,
            Self::Error => OUTCOME_ERROR,
        }
    }
}

/// Probe-gate decision for the failed-row active-execution pass
/// (`sweep:failed_cleanup`). `from_lifecycle` is always `failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedCleanupOutcome {
    /// The node reported a live started sandbox, so the row was CAS-repaired
    /// from `failed` back to `started`.
    RepairedFailedRowToLiveVm,
    /// The probe verdict permits a retire and the cleanup ran (its result is
    /// metered as `sweep:substrate_cleanup` with `from_lifecycle:failed`).
    PermittedRetireAfterProbe,
    /// The probe verdict forbids a retire (`reason:vm_alive_status_failed`,
    /// `reason:vm_alive_status_unknown`, or
    /// `reason:probe_verdict_forbids_retire`).
    Deferred,
    CasLost,
    Error,
}

impl FailedCleanupOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RepairedFailedRowToLiveVm => "repaired_failed_row_to_live_vm",
            Self::PermittedRetireAfterProbe => "permitted_retire_after_probe",
            Self::Deferred => OUTCOME_DEFERRED,
            Self::CasLost => OUTCOME_CAS_LOST,
            Self::Error => OUTCOME_ERROR,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::RepairedFailedRowToLiveVm => Some(LIFECYCLE_STARTED),
            Self::PermittedRetireAfterProbe | Self::Deferred | Self::CasLost | Self::Error => None,
        }
    }
}

/// Best-effort substrate cleanup outcome (`sweep:substrate_cleanup`). No
/// lifecycle transition: the flip already happened; this meters the teardown
/// that follows it. `from_lifecycle` carries whose cleanup it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupOutcome {
    Completed,
    /// The substrate refused the teardown because the caller's pod-identity
    /// fence no longer matched: the sandbox changed between the sweep's
    /// observation and its retire, so nothing was torn down. Distinct from
    /// [`Self::Error`] — no work failed, and the retry belongs to whoever owns
    /// the sandbox now.
    PodIdentityChanged,
    Error,
}

impl CleanupOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed_substrate_cleanup",
            Self::PodIdentityChanged => "deferred_pod_identity_changed",
            Self::Error => OUTCOME_ERROR,
        }
    }
}

/// Outcome of the command-ready-pending reaper for one preserved `started`
/// row (`sweep:command_ready_pending`). `from_lifecycle` is always
/// `started`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandReadyOutcome {
    /// The guest probe confirmed readiness; the stale marker was cleared and
    /// the sandbox stays `started`.
    ClearedStaleReadyMarker,
    /// The runtime was reachable but not ready (positive wedge evidence):
    /// CAS-flipped to `failed`.
    GaveUpGuestWedged,
    /// Unreachable past the terminal age (a dead pod the orphaned-sandbox
    /// reconciler won't collect): CAS-flipped to `failed`.
    GaveUpGuestUnreachable,
    /// Unreachable but not yet terminal-aged (`reason:probe_unreachable`).
    Deferred,
}

impl CommandReadyOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClearedStaleReadyMarker => "cleared_stale_ready_marker",
            Self::GaveUpGuestWedged => "gave_up_guest_wedged",
            Self::GaveUpGuestUnreachable => "gave_up_guest_unreachable",
            Self::Deferred => OUTCOME_DEFERRED,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::GaveUpGuestWedged | Self::GaveUpGuestUnreachable => Some(LIFECYCLE_FAILED),
            Self::ClearedStaleReadyMarker | Self::Deferred => None,
        }
    }
}

/// Per-candidate outcome of the unrestorable-`restoring` sweep
/// (`sweep:unrestorable_restoring`). `from_lifecycle` is always `restoring`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnrestorableOutcome {
    /// Provably restorable from nowhere: CAS'd to `failed`
    /// (`reason:restore_source_unavailable`).
    GaveUpNoRestoreSource,
    CasLost,
}

impl UnrestorableOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GaveUpNoRestoreSource => "gave_up_no_restore_source",
            Self::CasLost => OUTCOME_CAS_LOST,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::GaveUpNoRestoreSource => Some(LIFECYCLE_FAILED),
            Self::CasLost => None,
        }
    }
}

/// Per-candidate result of the lost-started-sandbox give-up
/// (`loop:lost_started_sandboxes, sweep:main`). `from_lifecycle` is always
/// `started`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostStartedOutcome {
    /// The source node is confirmed gone and no snapshot can restore the
    /// sandbox without rolling it back: CAS'd to `failed`.
    GaveUpSourceNodeGone,
    /// Left alone: the source node's instance is still live
    /// (`reason:source_node_live`).
    Skipped,
    /// Left alone this tick: the source node can't be checked
    /// (`reason:source_node_unverifiable`, `reason:kube_membership_unknown`,
    /// `reason:kube_absence_settling`)
    /// or a lookup failed (`reason:instance_lookup_failed`,
    /// `reason:kube_node_lookup_failed`).
    Deferred,
    CasLost,
    Error,
}

impl LostStartedOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GaveUpSourceNodeGone => "gave_up_source_node_gone",
            Self::Skipped => OUTCOME_SKIPPED,
            Self::Deferred => OUTCOME_DEFERRED,
            Self::CasLost => OUTCOME_CAS_LOST,
            Self::Error => OUTCOME_ERROR,
        }
    }

    fn to_lifecycle(self) -> Option<&'static str> {
        match self {
            Self::GaveUpSourceNodeGone => Some(LIFECYCLE_FAILED),
            Self::Skipped | Self::Deferred | Self::CasLost | Self::Error => None,
        }
    }
}

/// Per-candidate result of the suspended-execution reaper
/// (`loop:suspended_execution_reaper, sweep:main`). No lifecycle transition:
/// the sandbox stays `suspended`; this loop retires the stale execution row
/// and placement bookkeeping. `from_lifecycle` is always `suspended`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapOutcome {
    /// The stale execution row was retired and placement cleared.
    RetiredStaleExecution,
    /// A terminal `Succeeded` carrier pod was deleted and confirmed absent
    /// before the retire (emitted alongside the same row's
    /// `retired_stale_execution`).
    DeletedTerminalPod,
    /// Left alone this tick; `reason` carries the guard that refused.
    Skipped,
}

impl ReapOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetiredStaleExecution => "retired_stale_execution",
            Self::DeletedTerminalPod => "deleted_terminal_pod",
            Self::Skipped => OUTCOME_SKIPPED,
        }
    }
}

/// Per-candidate resolution of the stuck-ContainerCreating pod GC
/// (`loop:stuck_container_creating_gc, sweep:main`). The pod-cache-driven GC
/// deletes sandbox pods wedged in `ContainerCreating`; every stuck candidate
/// resolves to exactly one of these.
///
/// Follows the two-tier `cluster_reconciler.outcome` vocabulary: the only
/// state-change is `deleted_stuck_container_creating_pod` (paired with `mode` —
/// `live` = deleted, `dry_run` = would-delete). Every other variant is a no-op
/// that maps to one of the shared no-op outcomes (`skipped` / `deferred` /
/// `cas_lost` / `error`) via [`Self::as_str`], with the case-specific cause
/// carried in the `reason` tag via [`Self::reason`]. This GC never changes
/// sandbox lifecycle (DB convergence is delegated), so no `from`/`to_lifecycle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StuckPodGcOutcome {
    /// The pod was deleted (`mode:live`) or would have been (`mode:dry_run`).
    Deleted,
    // No-ops: `skipped` — the pod isn't a valid target this tick.
    AlreadyGone,
    ProgressingOnRecheck,
    RestorePod,
    WarmPool,
    MissingSandboxId,
    MissingNode,
    // No-ops: `deferred` — retry next tick (node / budget not ready).
    NodeNotReady,
    NodeCordoned,
    NodeUnknown,
    RateLimited,
    /// The tick's wall-clock budget elapsed before this candidate was reached.
    Deferred,
    // No-op: `cas_lost` — the object changed under us.
    StaleIdentity,
    // No-ops: `error` — an apiserver call failed.
    ReadError,
    DeleteFailed,
    TimedOut,
}

impl StuckPodGcOutcome {
    /// The `outcome` tag: the single state-change phrase, or a shared no-op
    /// outcome. The specific cause of a no-op is in [`Self::reason`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "deleted_stuck_container_creating_pod",
            Self::AlreadyGone
            | Self::ProgressingOnRecheck
            | Self::RestorePod
            | Self::WarmPool
            | Self::MissingSandboxId
            | Self::MissingNode => OUTCOME_SKIPPED,
            Self::NodeNotReady
            | Self::NodeCordoned
            | Self::NodeUnknown
            | Self::RateLimited
            | Self::Deferred => OUTCOME_DEFERRED,
            Self::StaleIdentity => OUTCOME_CAS_LOST,
            Self::ReadError | Self::DeleteFailed | Self::TimedOut => OUTCOME_ERROR,
        }
    }

    /// The `reason` tag: the case-specific cause of a no-op outcome. `None` for
    /// the state-change (`Deleted`), which stands alone.
    pub fn reason(self) -> Option<&'static str> {
        let reason = match self {
            Self::Deleted => return None,
            Self::AlreadyGone => "already_gone",
            Self::ProgressingOnRecheck => "progressing_on_recheck",
            Self::RestorePod => "restore_pod",
            Self::WarmPool => "warm_pool",
            Self::MissingSandboxId => "missing_sandbox_id",
            Self::MissingNode => "missing_node",
            Self::NodeNotReady => "node_not_ready",
            Self::NodeCordoned => "node_cordoned",
            Self::NodeUnknown => "node_unknown",
            Self::RateLimited => "rate_limited",
            Self::Deferred => "tick_budget",
            Self::StaleIdentity => "stale_identity",
            Self::ReadError => "read_error",
            Self::DeleteFailed => "delete_failed",
            Self::TimedOut => "timed_out",
        };
        Some(reason)
    }
}

impl fmt::Display for StuckPodGcOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-candidate result of the warm-pool orphan GC (`loop:warm_pool_orphan_gc`,
/// `sweep:main`), which deletes terminal-phase (`Succeeded`/`Failed`)
/// `pool-role=warm` pods that the pool no longer tracks and that back no live
/// sandbox; every candidate resolves to exactly one of these.
///
/// Follows the two-tier `cluster_reconciler.outcome` vocabulary: the only
/// state-change is `deleted_orphaned_warm_pool_pod` (paired with `mode` —
/// `live` = deleted, `dry_run` = would-delete). Every other variant is a no-op
/// that maps to one of the shared no-op outcomes (`skipped` / `deferred` /
/// `cas_lost` / `error`) via [`Self::as_str`], with the case-specific cause
/// carried in the `reason` tag via [`Self::reason`]. This GC never changes
/// sandbox lifecycle (it deletes only pods with no logical sandbox row), so no
/// `from`/`to_lifecycle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmPoolOrphanGcOutcome {
    /// The orphan pod was deleted (`mode:live`) or would have been
    /// (`mode:dry_run`).
    Deleted,
    // No-ops: `skipped` — the pod isn't a valid orphan target this tick.
    AlreadyGone,
    /// The authoritative re-read shows the pod is no longer an orphan candidate
    /// (recovered from terminal, became terminating, gained a restore
    /// annotation, or lost the warm-pool label).
    NoLongerEligible,
    /// The pod's slot still has a `kubernetes_pool_slots` row — the pool's own
    /// desired→actual reconcile / claimed-slot reapers own it, not the sweep.
    SlotRowLive,
    /// The pod is bound to a live sandbox execution (an `active`
    /// `sandbox_k8s_executions` row for its reserved sandbox id) — an adopted
    /// user sandbox, never an orphan.
    SandboxBound,
    /// The authoritative pre-delete recheck found the candidate is no longer
    /// unowned: a `kubernetes_pool_slots` row or an active execution binding
    /// appeared for it after the batch scan (the ownership-TOCTOU fence fired).
    OwnershipReappeared,
    // No-op: `cas_lost` — the object changed under us.
    StaleIdentity,
    // No-ops: `error` — an apiserver call or the ownership recheck failed.
    ReadError,
    DeleteFailed,
    /// The authoritative pre-delete ownership recheck itself errored/timed out;
    /// the candidate is left untouched (fail-closed).
    RecheckFailed,
}

impl WarmPoolOrphanGcOutcome {
    /// The `outcome` tag: the single state-change phrase, or a shared no-op
    /// outcome. The specific cause of a no-op is in [`Self::reason`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "deleted_orphaned_warm_pool_pod",
            Self::AlreadyGone
            | Self::NoLongerEligible
            | Self::SlotRowLive
            | Self::SandboxBound
            | Self::OwnershipReappeared => OUTCOME_SKIPPED,
            Self::StaleIdentity => OUTCOME_CAS_LOST,
            Self::ReadError | Self::DeleteFailed | Self::RecheckFailed => OUTCOME_ERROR,
        }
    }

    /// The `reason` tag: the case-specific cause of a no-op outcome. `None` for
    /// the state-change (`Deleted`), which stands alone.
    pub fn reason(self) -> Option<&'static str> {
        let reason = match self {
            Self::Deleted => return None,
            Self::AlreadyGone => "already_gone",
            Self::NoLongerEligible => "no_longer_eligible",
            Self::SlotRowLive => "slot_row_live",
            Self::SandboxBound => "sandbox_bound",
            Self::OwnershipReappeared => "ownership_reappeared",
            Self::StaleIdentity => "stale_identity",
            Self::ReadError => "read_error",
            Self::DeleteFailed => "delete_failed",
            Self::RecheckFailed => "recheck_failed",
        };
        Some(reason)
    }
}

impl fmt::Display for WarmPoolOrphanGcOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-candidate resolution of the orphaned-restore-pod GC
/// (`loop:orphaned_restore_pod_gc, sweep:main`). The pod-cache-driven reaper
/// deletes restore pods their owning `sandboxes` row has provably superseded
/// (row deleted, no active execution, or a strictly-higher active generation)
/// — the unrouted VM an interrupted source-pinned restore leaves behind when
/// its pod binds after the row moved on.
///
/// Follows the two-tier `cluster_reconciler.outcome` vocabulary. A live reap
/// has two state changes: `requested_orphaned_restore_pod_delete` when the
/// apiserver accepts the DELETE, then `reaped_orphaned_restore_pod` once the
/// pod is observed gone (a pod still terminating past the stuck threshold
/// reports `error` / `teardown_stuck` instead). In dry-run,
/// `reaped_orphaned_restore_pod` (`mode:dry_run`) is the would-delete. Every
/// other variant is a no-op that maps
/// to one of the shared no-op outcomes (`skipped` / `deferred` / `cas_lost` /
/// `error`) via [`Self::as_str`], with the case-specific cause in the `reason`
/// tag via [`Self::reason`]. The reaper only deletes a pod the row already
/// superseded, so the deleted pod strands no lifecycle to converge — this
/// loop needs no DB-convergence companion and changes no sandbox lifecycle, so
/// no `from`/`to_lifecycle` tags. It emits one outcome per *orphan* candidate
/// (the actionable set) per tick, plus the teardown follow-ups, not one per
/// scanned restore pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanedRestorePodOutcome {
    /// The apiserver accepted the DELETE of an orphaned restore pod
    /// (`mode:live`). The pod is now terminating and its VM is in node-side
    /// teardown; completion is reported separately as `Reaped` once the pod
    /// is observed gone, or as `TeardownStuck` if it lingers.
    DeleteRequested,
    /// The orphaned restore pod is confirmed gone after this loop's DELETE
    /// (`mode:live`, observed absent from the pod cache), or would have been
    /// deleted (`mode:dry_run`).
    Reaped,
    // No-ops: `skipped` — the pod is no longer a valid target on the fresh
    // recheck.
    /// The pod vanished between the cached scan and the verify GET / delete.
    AlreadyGone,
    /// The authoritative DB recheck no longer classifies the pod as an orphan:
    /// the row re-entered a transient lifecycle (a new restore or other
    /// operation re-claimed it) or its active execution is at or above the
    /// pod's generation. The specific `SkipClass` is in the log `detail`.
    NotOrphanOnRecheck,
    // No-ops: `deferred` — retry next tick (budget not ready / tick elapsed).
    RateLimited,
    Deferred,
    // No-op: `cas_lost` — the pod object changed identity under us.
    StaleIdentity,
    // No-ops: `error` — an apiserver call failed.
    ReadError,
    DeleteFailed,
    TimedOut,
    /// A restore pod this loop deleted has stayed terminating past the
    /// stuck-teardown threshold — its VM may still be running. Reported once
    /// per pod. (A stuck pod this loop didn't delete is logged, not counted.)
    TeardownStuck,
}

impl OrphanedRestorePodOutcome {
    /// The `outcome` tag: a state-change phrase (delete requested, reap
    /// confirmed), or a shared no-op outcome. The specific cause of a no-op is
    /// in [`Self::reason`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeleteRequested => "requested_orphaned_restore_pod_delete",
            Self::Reaped => "reaped_orphaned_restore_pod",
            Self::AlreadyGone | Self::NotOrphanOnRecheck => OUTCOME_SKIPPED,
            Self::RateLimited | Self::Deferred => OUTCOME_DEFERRED,
            Self::StaleIdentity => OUTCOME_CAS_LOST,
            Self::ReadError | Self::DeleteFailed | Self::TimedOut | Self::TeardownStuck => {
                OUTCOME_ERROR
            }
        }
    }

    /// The `reason` tag: the case-specific cause of a no-op outcome. `None` for
    /// the state changes (`DeleteRequested`, `Reaped`), which stand alone.
    pub fn reason(self) -> Option<&'static str> {
        let reason = match self {
            Self::DeleteRequested | Self::Reaped => return None,
            Self::AlreadyGone => "already_gone",
            Self::NotOrphanOnRecheck => "not_orphan_on_recheck",
            Self::RateLimited => "rate_limited",
            Self::Deferred => "tick_budget",
            Self::StaleIdentity => "stale_identity",
            Self::ReadError => "read_error",
            Self::DeleteFailed => "delete_failed",
            Self::TimedOut => "timed_out",
            Self::TeardownStuck => "teardown_stuck",
        };
        Some(reason)
    }
}

impl fmt::Display for OrphanedRestorePodOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The severity the paired log event is emitted at. The metric increment is
/// identical across levels; the level only controls log visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogLevel {
    Debug,
    Info,
    Warn,
}

/// One reconciler outcome: an increment of `cluster_reconciler.outcome` plus
/// exactly one paired tracing event. Enum-typed fields become metric tags AND
/// log fields; identifier fields (`sandbox_id`, `pod_name`, ...) become log
/// fields only. Construct via the per-sweep constructors, attach identifiers
/// with the builder setters, then finish with [`Self::info`], [`Self::warn`],
/// or [`Self::debug`].
#[must_use = "call .info()/.warn()/.debug() to emit the metric and log"]
#[derive(Debug)]
pub struct OutcomeEvent {
    loop_: Loop,
    sweep: Sweep,
    outcome: &'static str,
    // Metric tags and log fields (low-cardinality):
    mode: Option<Mode>,
    from_lifecycle: Option<String>,
    to_lifecycle: Option<&'static str>,
    verdict: Option<String>,
    reason: Option<String>,
    // Log-only fields (high-cardinality; never metric tags):
    sandbox_id: Option<SandboxId>,
    pod_name: Option<String>,
    snapshot_id: Option<SnapshotId>,
    generation: Option<i64>,
    node: Option<String>,
    age_secs: Option<u64>,
    error: Option<String>,
    detail: Option<String>,
}

impl OutcomeEvent {
    fn new(loop_: Loop, sweep: Sweep, outcome: &'static str) -> Self {
        Self {
            loop_,
            sweep,
            outcome,
            mode: None,
            from_lifecycle: None,
            to_lifecycle: None,
            verdict: None,
            reason: None,
            sandbox_id: None,
            pod_name: None,
            snapshot_id: None,
            generation: None,
            node: None,
            age_secs: None,
            error: None,
            detail: None,
        }
    }

    pub fn started_sandbox_repair(outcome: RepairOutcome, mode: Mode) -> Self {
        let mut event = Self::new(Loop::StartedSandboxRepair, Sweep::Main, outcome.as_str());
        event.mode = Some(mode);
        event.from_lifecycle = Some(LIFECYCLE_STARTED.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event
    }

    pub fn failed_restorable_repair(outcome: RepairOutcome, mode: Mode) -> Self {
        let mut event = Self::new(Loop::FailedRestorableRepair, Sweep::Main, outcome.as_str());
        event.mode = Some(mode);
        event.from_lifecycle = Some(LIFECYCLE_FAILED.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event
    }

    pub fn aged_transient(
        outcome: AgedTransientOutcome,
        from_lifecycle: &str,
        verdict: &str,
    ) -> Self {
        let mut event = Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::AgedTransient,
            outcome.as_str(),
        );
        event.from_lifecycle = Some(from_lifecycle.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event.verdict = Some(verdict.to_string());
        event
    }

    pub fn execution_hydration(outcome: HydrationOutcome) -> Self {
        Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::ExecutionHydration,
            outcome.as_str(),
        )
    }

    pub fn failed_cleanup(outcome: FailedCleanupOutcome, verdict: &str) -> Self {
        let mut event = Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::FailedCleanup,
            outcome.as_str(),
        );
        event.from_lifecycle = Some(LIFECYCLE_FAILED.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event.verdict = Some(verdict.to_string());
        event
    }

    pub fn substrate_cleanup(outcome: CleanupOutcome, from_lifecycle: &str) -> Self {
        let mut event = Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::SubstrateCleanup,
            outcome.as_str(),
        );
        event.from_lifecycle = Some(from_lifecycle.to_string());
        event
    }

    pub fn command_ready_pending(outcome: CommandReadyOutcome) -> Self {
        let mut event = Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::CommandReadyPending,
            outcome.as_str(),
        );
        event.from_lifecycle = Some(LIFECYCLE_STARTED.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event
    }

    pub fn unrestorable_restoring(outcome: UnrestorableOutcome) -> Self {
        let mut event = Self::new(
            Loop::TransientLifecycleSweeper,
            Sweep::UnrestorableRestoring,
            outcome.as_str(),
        );
        event.from_lifecycle = Some("restoring".to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event
    }

    pub fn lost_started_sandbox(outcome: LostStartedOutcome) -> Self {
        let mut event = Self::new(Loop::LostStartedSandboxes, Sweep::Main, outcome.as_str());
        event.mode = Some(Mode::Live);
        event.from_lifecycle = Some(LIFECYCLE_STARTED.to_string());
        event.to_lifecycle = outcome.to_lifecycle();
        event
    }

    pub fn suspended_execution_reap(outcome: ReapOutcome, mode: Mode) -> Self {
        let mut event = Self::new(
            Loop::SuspendedExecutionReaper,
            Sweep::Main,
            outcome.as_str(),
        );
        event.mode = Some(mode);
        event.from_lifecycle = Some(LIFECYCLE_SUSPENDED.to_string());
        event
    }

    /// The stuck-snapshot reaper has exactly one outcome: a suspend-snapshot
    /// row flipped from `state = 'creating'` to `state = 'failed'`. No
    /// lifecycle tags: it moves snapshot state, not sandbox lifecycle.
    pub fn stuck_snapshot_upload_failed() -> Self {
        Self::new(
            Loop::StuckSnapshotReaper,
            Sweep::Main,
            "failed_stuck_snapshot_upload",
        )
    }

    pub fn stuck_container_creating_gc(outcome: StuckPodGcOutcome, mode: Mode) -> Self {
        let mut event = Self::new(
            Loop::StuckContainerCreatingGc,
            Sweep::Main,
            outcome.as_str(),
        );
        event.mode = Some(mode);
        // Two-tier vocabulary: no-op outcomes carry the case-specific cause in
        // `reason`; the state-change (`deleted_stuck_container_creating_pod`)
        // stands alone.
        event.reason = outcome.reason().map(str::to_string);
        event
    }

    pub fn orphaned_restore_pod_gc(outcome: OrphanedRestorePodOutcome, mode: Mode) -> Self {
        let mut event = Self::new(Loop::OrphanedRestorePodGc, Sweep::Main, outcome.as_str());
        event.mode = Some(mode);
        // Two-tier vocabulary: no-op outcomes carry the case-specific cause in
        // `reason`; the state-change (`reaped_orphaned_restore_pod`) stands
        // alone. This reaper changes no sandbox lifecycle, so no lifecycle tags.
        event.reason = outcome.reason().map(str::to_string);
        event
    }

    pub fn warm_pool_orphan_gc(outcome: WarmPoolOrphanGcOutcome, mode: Mode) -> Self {
        let mut event = Self::new(Loop::WarmPoolOrphanGc, Sweep::Main, outcome.as_str());
        event.mode = Some(mode);
        // Two-tier vocabulary: no-op outcomes carry the case-specific cause in
        // `reason`; the state-change (`deleted_orphaned_warm_pool_pod`) stands
        // alone.
        event.reason = outcome.reason().map(str::to_string);
        event
    }

    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn sandbox_id(mut self, id: SandboxId) -> Self {
        self.sandbox_id = Some(id);
        self
    }

    pub fn pod_name(mut self, name: impl Into<String>) -> Self {
        self.pod_name = Some(name.into());
        self
    }

    pub fn pod_name_opt(mut self, name: Option<&str>) -> Self {
        self.pod_name = name.map(str::to_string);
        self
    }

    pub fn snapshot_id(mut self, id: SnapshotId) -> Self {
        self.snapshot_id = Some(id);
        self
    }

    pub fn generation(mut self, generation: i64) -> Self {
        self.generation = Some(generation);
        self
    }

    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    pub fn age_secs(mut self, secs: u64) -> Self {
        self.age_secs = Some(secs);
        self
    }

    pub fn error(mut self, err: impl fmt::Display) -> Self {
        self.error = Some(err.to_string());
        self
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Emit at `info`: benign state changes (converges, heals, retires,
    /// dry-run would-act).
    pub fn info(self) {
        self.emit(LogLevel::Info);
    }

    /// Emit at `warn`: destructive or exceptional outcomes (give-ups,
    /// reverts, repairs, errors).
    pub fn warn(self) {
        self.emit(LogLevel::Warn);
    }

    /// Emit at `debug`: no-op outcomes (CAS losses, routine skips/defers).
    /// The metric still increments; the log needs `cluster_reconciler=debug`.
    pub fn debug(self) {
        self.emit(LogLevel::Debug);
    }

    fn metric_tags(&self) -> Vec<String> {
        let mut tags = vec![
            tag("loop", self.loop_.as_str()),
            tag("sweep", self.sweep.as_str()),
            tag("outcome", self.outcome),
        ];
        if let Some(mode) = self.mode {
            tags.push(tag("mode", mode.as_str()));
        }
        if let Some(from_lifecycle) = &self.from_lifecycle {
            tags.push(tag("from_lifecycle", from_lifecycle));
        }
        if let Some(to_lifecycle) = self.to_lifecycle {
            tags.push(tag("to_lifecycle", to_lifecycle));
        }
        if let Some(verdict) = &self.verdict {
            tags.push(tag("verdict", verdict));
        }
        if let Some(reason) = &self.reason {
            tags.push(tag("reason", reason));
        }
        tags
    }

    fn emit(self, level: LogLevel) {
        counter("cluster_reconciler.outcome", 1, &self.metric_tags());
        self.log(level);
    }

    fn log(&self, level: LogLevel) {
        let sandbox_id = self.sandbox_id.map(|id| id.to_string()).unwrap_or_default();
        let snapshot_id = self
            .snapshot_id
            .map(|id| id.to_string())
            .unwrap_or_default();
        // tracing events need a static field set and a const level, hence the
        // macro-per-level expansion over one shared field list.
        macro_rules! log_at {
            ($macro:ident) => {
                tracing::$macro!(
                    target: "cluster_reconciler",
                    r#loop = self.loop_.as_str(),
                    sweep = self.sweep.as_str(),
                    outcome = self.outcome,
                    mode = self.mode.map(Mode::as_str).unwrap_or(""),
                    from_lifecycle = self.from_lifecycle.as_deref().unwrap_or(""),
                    to_lifecycle = self.to_lifecycle.unwrap_or(""),
                    verdict = self.verdict.as_deref().unwrap_or(""),
                    reason = self.reason.as_deref().unwrap_or(""),
                    sandbox_id = sandbox_id.as_str(),
                    pod_name = self.pod_name.as_deref().unwrap_or(""),
                    snapshot_id = snapshot_id.as_str(),
                    generation = self.generation.unwrap_or(-1),
                    node = self.node.as_deref().unwrap_or(""),
                    age_secs = self.age_secs.unwrap_or(0),
                    error = self.error.as_deref().unwrap_or(""),
                    detail = self.detail.as_deref().unwrap_or(""),
                    "reconciler outcome"
                )
            };
        }
        match level {
            LogLevel::Debug => log_at!(debug),
            LogLevel::Info => log_at!(info),
            LogLevel::Warn => log_at!(warn),
        }
    }
}

/// Candidate rows surfaced by one tick's scan. Also logs one
/// `message = "reconciler scanned"` event with `loop` + `count`.
pub fn scanned(loop_: Loop, count: u64) {
    counter(
        "cluster_reconciler.scanned",
        count,
        &[tag("loop", loop_.as_str())],
    );
    tracing::debug!(
        target: "cluster_reconciler",
        r#loop = loop_.as_str(),
        count,
        "reconciler scanned"
    );
}

/// Wall-clock time for one reconciler tick. Also logs one
/// `message = "reconciler tick"` event with `loop` + `latency_ms`.
pub fn loop_latency(loop_: Loop, started_at: Instant) {
    let latency_ms = elapsed_ms(started_at);
    timing_ms(
        "cluster_reconciler.loop_latency_ms",
        latency_ms,
        &[tag("loop", loop_.as_str())],
    );
    tracing::debug!(
        target: "cluster_reconciler",
        r#loop = loop_.as_str(),
        latency_ms,
        "reconciler tick"
    );
}
