//! Periodic reconciler that drives `nodes.drain_phase` through the drain lifecycle.
//!
//! Lifecycle: `registered -> draining -> drained -> terminating -> deleted`.
//! The admin API flips `registered -> draining` (and cordons the k8s Node).
//! This loop owns every transition after that, **except** `drained ->
//! terminating`, which is intentionally left to operators or a companion
//! reconciler (see "Drained-to-terminating handoff" below):
//!
//! - For each `draining` node: list resident sandboxes and, **in forced mode**,
//!   subject to per-node and per-cluster parallelism caps, run the evacuation
//!   pipeline (suspend -> restore) for each. Flip the node to `drained` when all
//!   four gate terms are zero: no resident sandbox, no in-flight evacuation, no
//!   snapshot blocker, and nothing the `drained_node_terminator`'s resident
//!   guard would refuse the node for. Both modes advance on this same gate. That
//!   fourth term counts four lifecycles the resident include-list treats as
//!   non-occupying, so "zero non-terminal sandboxes" is not the whole condition
//!   — see [`NodeDrainSink::count_termination_blocking_residents_on_node`].
//! - For each `terminating` node: call
//!   `TerminateInstanceInAutoScalingGroup decrement=false`. On success the
//!   row moves to `deleted`; on error the row stays in `terminating` with
//!   the error recorded for the next tick.
//! - KubeNodeGone recovery: a `draining` node whose kube Node object is
//!   gone **and** whose EC2 instance is confirmed terminated (cross-checked
//!   through the same `InstanceStateLookup` seam as
//!   `drained_node_terminator`) switches its per-sandbox work from
//!   node-bound RPCs that can never succeed — suspend evacuations, backup
//!   enqueues against a node-api pod that no longer exists — to guarded
//!   give-ups: dead resident sandboxes are bulk-failed, and pinned
//!   local-only suspended sandboxes either adopt their newest ready remote
//!   snapshot or are marked lost. The drain then converges through the
//!   normal `draining -> drained` gate, where `drained_node_terminator`'s
//!   own KubeNodeGone path reaps the row — **unless the node still holds a
//!   `created` / `stopped` / `deleting` / `quarantined` sandbox**, which the
//!   bulk give-up's include-list cannot clear and the drain gate now counts,
//!   so such a node stays `draining` until an operator clears the sandbox.
//!   See [`Reconciler::assess_gone_draining_node`]. Without the EC2
//!   confirmation (no cross-check configured, instance still live, lookup
//!   error) the node parks with its pre-recovery retry behavior, fail-closed.
//!
//! ## The fourth term guards convergence, not data
//!
//! It is tempting to read the fourth term as the thing that keeps a `stopped`
//! sandbox from being destroyed along with its node. It is not: the drained-node
//! terminator's flip and its KubeNodeGone reap, the admin `:terminate` 409, and
//! the pre-terminate recheck all read
//! `sandbox_residency::active_resident_predicate` without consulting
//! [`ReconcilerConfig::termination_blocking_gate`], so both settings park the
//! node and neither terminates it. The term decides only *where* it parks, and
//! therefore — on a gentle drain, not a forced one — whether its owner can still
//! start it rather than only delete it. See that field's docs for the
//! decision and `sandbox_residency::active_resident_predicate` for the
//! mechanism and the per-lifecycle exits.
//!
//! ## Drain modes
//!
//! `nodes.drain_mode` (see [`NodeDrainMode`]) selects how a `draining` node is
//! emptied. It is orthogonal to the phase: both modes cordon, both are
//! excluded from placement, both advance on the same zero-blockers gate, and
//! both terminate through the same path.
//!
//! - **Gentle** — what a node gets whenever a mode was not explicitly chosen: a
//!   `:drain` body without one, a NULL column, or a value this build does not
//!   recognize (see [`NodeDrainMode::from_column_str`]). Does not touch
//!   residents.
//!   They keep serving traffic and leave on their own — auto-suspend on
//!   inactivity, a user suspend, or a delete — and the user-API drain gates
//!   stay open for them. Local-snapshot promotion still runs, which is how the
//!   node makes progress without disturbing anyone: an S3-backed artifact both
//!   releases the node and sends that sandbox's next restore elsewhere. The
//!   cost is latency, and the failure mode is a node that never converges
//!   because a resident never suspends — see
//!   [`ReconcilerConfig::gentle_escalate`].
//! - **Forced** is the original behavior: evacuate every resident
//!   `started`/`paused` sandbox immediately, with the 409 gates closed for the
//!   whole drain window so users cannot race the pipeline. Per-node disruption
//!   is proportional to resident live-sandbox count.
//!
//! Escalation is a single `UPDATE nodes SET drain_mode = 'forced'`: this
//! reconciler and the user-API gates both read the column, so one committed
//! write moves every consumer at once and there is no in-memory mode state to
//! keep consistent across the two processes.
//!
//! ## Drained-to-terminating handoff
//!
//! `drained` is a stable resting state. **This reconciler** does not
//! advance `drained -> terminating` — that transition is owned by a
//! separate periodic reconciler, `drained_node_terminator` (its own
//! sub-issue), which honors the operator override annotation
//! `space.pplx.net/drain-keep` on the k8s Node. Operators can also
//! flip the transition immediately via the admin
//! `POST /admin/v1/nodes/{id}:terminate` endpoint.
//!
//! Splitting the responsibilities keeps this reconciler's tick tight
//! (5 s by default, busy evacuating sandboxes) and the termination
//! policy loose (60 s, annotation-aware). Once the companion lands,
//! this reconciler still picks up the `terminating` state and calls
//! the ASG.
//!
//! Per the repo-wide rule against background work in API servers (see
//! AGENTS.md), this loop runs in the cluster-singleton controller binary
//! (`space-cluster-controller`, in `crates/apps/cluster-controller/`).
//!
//! The reconciler is intentionally testable through two seams: a
//! [`NodeDrainSink`] for DB + suspend/restore + sandbox-listing and an
//! [`InstanceTerminator`] for ASG termination. Production implementations
//! are exported as [`SubstrateSink`] and [`Ec2InstanceTerminator`].
//!
//! ## Design note: reusing [`SandboxLifecycle`] for the evacuable subset
//!
//! The "lifecycles the pipeline can service while preserving user-visible
//! semantics" set has two members today (`Started` and `Paused`) and is
//! a proper subset of the canonical [`SandboxLifecycle`] enum. An earlier
//! revision of this module modeled that subset as its own enum (with a
//! `TryFrom<SandboxLifecycle>` bridge) so consumer `match` arms were
//! exhaustive at the type level.
//!
//! That was reverted in favor of reusing [`SandboxLifecycle`] everywhere
//! and validating membership at runtime via [`is_evacuable_lifecycle`].
//! The explicit tradeoffs accepted by this design:
//!
//! - **No new vocabulary.** The control plane already has
//!   [`SandboxLifecycle`] (canonical), [`SandboxState`] (sbx-mgr FSM),
//!   and [`SandboxLifecycleStatus`] (API resting states). Adding a
//!   4th "evacuable lifecycle" enum and conversion shims for each
//!   layer (cluster-controller, substrate, sbx-mgr) inflates the
//!   surface for marginal type-system gain.
//! - **Validity is a runtime contract.** [`NodeDrainSink::evacuate_sandbox`]
//!   accepts any [`SandboxLifecycle`] and checks
//!   [`is_evacuable_lifecycle`] at entry. A non-evacuable value returns
//!   [`EvacuateError::Lookup`] rather than failing to type-check. The
//!   SQL filter on [`NodeDrainSink::list_evacuatable_sandboxes_on_node`]
//!   makes this unreachable in production; the predicate is defense in
//!   depth, not the load-bearing check.
//! - **Lose compile-time exhaustiveness at consumer match sites.**
//!   Branches on `pre_lifecycle` use the two valid variants
//!   (`SandboxLifecycle::Started` / `SandboxLifecycle::Paused`) and
//!   either fall through to a defensive default or return an error
//!   for the other 13 variants. Adding a new evacuable variant
//!   requires updating both [`EVACUABLE_LIFECYCLES`] and each match
//!   arm; the lockstep test
//!   `evacuable_lifecycle_set_contains_started_and_paused` is the
//!   tripwire.
//!
//! The same shape applies to the wake/restore `target_state` plumbed
//! through the substrate and sbx-mgr: typed as [`SandboxLifecycle`] /
//! [`SandboxState`] respectively, validated at the proto boundary, no
//! new 2-variant enums introduced.

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SandboxId;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::NODE_RPC_BLOCKING_BUDGET;
use pplx_space_common_core::kubernetes::EvacuationPlacement;
use pplx_space_common_core::types::{NodeDrainMode, PositiveDuration, SandboxLifecycle};
use pplx_space_metrics::node_drain as node_drain_metrics;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::warn_pacer::PerNodeWarnPacer;

pub use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;

// Reuse the drained-node-terminator's EC2 cross-check vocabulary verbatim
// (which itself reuses `orphaned_nodes`' fail-closed verdict types) so the
// three reconcilers don't fork the "is this instance gone?" decision. The
// production impl (`drained_node_terminator::Ec2InstanceStateLookup`) is
// shared too — the wiring constructs one per reconciler.
pub use crate::drained_node_terminator::{
    InstanceLookupOutcome, InstanceStateLookup, NodeInstanceState,
};

#[cfg(feature = "kubernetes")]
pub mod aws;
#[cfg(feature = "kubernetes")]
pub mod kube_delete;
pub mod noop;
#[cfg(feature = "kubernetes")]
pub mod store;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use aws::{Ec2InstanceTerminator, TerminateError};
#[cfg(feature = "kubernetes")]
pub use kube_delete::KubeNodeDeleteTerminator;
pub use noop::NoopInstanceTerminator;
#[cfg(feature = "kubernetes")]
pub use store::{CordonError, CordonOutcome, EvacuateError, RestoreOutcome, SubstrateSink};

pub(crate) const PHASE_DRAINING: &str = "draining";
pub(crate) const PHASE_DRAINED: &str = "drained";
pub(crate) const PHASE_TERMINATING: &str = "terminating";
// No `'deleted'` phase: per the corrected drain design, terminate
// success DELETEs the `nodes` row outright (no tombstone state).
// The CHECK on `drain_phase` only allows `draining`/`drained`/
// `terminating`/`NULL` — see `db/postgres/20261002070625_baseline.up.sql`.

/// Lifecycle values that count as "still resident on the node" for the
/// purposes of declaring a drain complete.
///
/// The evacuation pipeline is suspend → restore, which sandbox-manager
/// only accepts from a small set of lifecycles where the Firecracker VM
/// is actually running (started/paused/resuming) or transitioning toward
/// running (creating/starting/restoring) or actively being persisted
/// (pausing/suspending/checkpointing). The reconciler waits on transient
/// states to settle into a suspendable state on subsequent ticks.
///
/// Defined in `pplx-space-common-svc` alongside the residency predicate, so
/// anything that predicts this reconciler's behaviour counts exactly the rows
/// it will wait on — a predictor scoped to a different set names the wrong
/// nodes. That module's docs carry the per-lifecycle exclusion reasons,
/// including why `created` and `stopped` are absent.
#[cfg(test)]
pub(crate) use pplx_space_common_svc::sql::sandbox_residency::RESIDENT_LIFECYCLES;

/// [`SandboxLifecycle`] values the suspend → restore pipeline can service
/// while preserving user-visible semantics. Single source of truth for
/// the SQL `lifecycle = ANY($1)` filter used by
/// [`NodeDrainSink::list_evacuatable_sandboxes_on_node`] and the
/// runtime validity check on [`NodeDrainSink::evacuate_sandbox`]'s
/// `pre_lifecycle` parameter.
///
/// - `Started` round-trips through suspend → restore and stays `started`.
/// - `Paused` round-trips through suspend → restore with the substrate's
///   restore RPC carrying `target_state = paused`, so the VM never
///   leaves the paused state — preserving the user-visible lifecycle
///   without an additional post-restore RPC.
///
/// Other lifecycles in `RESIDENT_LIFECYCLES` continue to
/// count against the "drain complete?" check but are intentionally
/// excluded here: evacuating an in-flight `pausing`/`stopping` would
/// undo a user request, and transient states (`creating`, `starting`,
/// `resuming`, `suspending`, `restoring`, `checkpointing`) settle on
/// subsequent ticks into one of the evacuable lifecycles.
pub const EVACUABLE_LIFECYCLES: &[SandboxLifecycle] =
    &[SandboxLifecycle::Started, SandboxLifecycle::Paused];

/// Runtime check: is this lifecycle one the drain pipeline can evacuate
/// while preserving semantics? Used at the entry of
/// [`NodeDrainSink::evacuate_sandbox`] to defend against a caller
/// passing a non-evacuable lifecycle. The SQL filter on
/// [`NodeDrainSink::list_evacuatable_sandboxes_on_node`] makes this
/// unreachable in production, but the predicate is the price for
/// reusing [`SandboxLifecycle`] instead of introducing a new subset
/// enum.
pub fn is_evacuable_lifecycle(lifecycle: SandboxLifecycle) -> bool {
    EVACUABLE_LIFECYCLES.contains(&lifecycle)
}

/// Resident lifecycles the suspend → restore pipeline cannot service —
/// i.e. `RESIDENT_LIFECYCLES` minus
/// [`EVACUATABLE_LIFECYCLES`]. Two roles:
///
/// 1. Surfaced in the "stuck draining" warning so operators see every
///    lifecycle that can block a drain.
/// 2. The exact set swept by the stale-abandon policy
///    ([`NodeDrainSink::abandon_stale_resident_sandboxes`]): any row that
///    has sat in one of these on a draining node past
///    [`ReconcilerConfig::stale_abandon`] is flipped to `failed` so the
///    drain converges. None of these settle into an evacuable lifecycle on
///    their own when wedged — `creating`/`starting` never finished starting,
///    a non-evacuation `restoring`/`suspending` (the user wake/suspend path,
///    distinguished from the drain's own evacuation by a NULL
///    `evacuation_restore_target`) hung mid-RPC, etc. — so on a draining
///    node they hold it open indefinitely until aged out.
///
/// `Paused` is intentionally **absent**: the pipeline now evacuates
/// paused sandboxes by re-pausing them after restore (see
/// [`EVACUATABLE_LIFECYCLES`]), so `paused` no longer blocks a drain.
pub(crate) const NON_EVACUATABLE_RESIDENT_LIFECYCLES: &[SandboxLifecycle] = &[
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Stopping,
    SandboxLifecycle::Suspending,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Checkpointing,
];

/// Result of one resident scan on a draining node.
///
/// Reporting `discounted` alongside `occupying` is what lets an operator tell
/// "this drain is nearly done" apart from "this node stopped answering, so
/// every phantom row is counting again" — the two look identical if only the
/// blocking count is published.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentSandboxCounts {
    /// Rows that still block `draining → drained`.
    pub occupying: u64,
    /// Rows dropped because the node's own census proves them absent. Always
    /// zero under [`ResidencyEvidence::LifecycleOnly`].
    pub discounted: u64,
}

/// Rows the drained-node terminator's resident guard would refuse a draining
/// node for, grouped by lifecycle so the WARN can name what is holding it —
/// this is the one drain blocker with no other signal behind it.
///
/// `lifecycle` stays a `String` rather than a `SandboxLifecycle`: the guard is
/// an exclusion list, so a value the enum does not model (a `deleted`
/// tombstone, anything added to the column later) still blocks and still has to
/// be reportable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TerminationBlockingResidents {
    /// `(lifecycle, count)`, one entry per lifecycle actually present,
    /// lifecycle-ordered so the warning text is stable across ticks.
    pub by_lifecycle: Vec<(String, u64)>,
}

impl TerminationBlockingResidents {
    /// Derived rather than stored alongside the breakdown, so the number in
    /// the gate and the numbers in the warning cannot disagree.
    pub fn total(&self) -> u64 {
        self.by_lifecycle.iter().map(|(_, count)| count).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_lifecycle.is_empty()
    }
}

/// How often a per-node "this node is parked and will stay parked" WARN may fire
/// for the same node — the terminator-resident hold and each KubeNodeGone
/// verdict. One interval for all of them, because they share a tick cadence and
/// an operator reads them together; `drained_node_terminator` keeps its own
/// (`REFUSAL_WARN_INTERVAL`) because it ticks at a different rate. The pacer
/// keys on `(node, reason)`, so sharing the interval does not make the
/// conditions share a window.
///
/// Unlike every other drain blocker this one has no give-up: a `quarantined`
/// sandbox holds its node for as long as the investigation lasts, so at the
/// [`DEFAULT_TICK`] cadence an unpaced WARN emits hundreds of lines an hour per
/// node forever. Matches `drained_node_terminator`'s `REFUSAL_WARN_INTERVAL`,
/// which paces the mirror-image message about the same rows.
pub(crate) const HELD_WARN_INTERVAL: Duration = Duration::from_secs(3600);

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(5);

/// Wall-clock at which a per-node store query that still succeeded is reported
/// as slow by [`Reconciler::observe_store_query`], which carries the rationale
/// for the threshold and for why only the succeeding case warns.
const SLOW_STORE_QUERY: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_MAX_PER_NODE: usize = 16;
pub(crate) const DEFAULT_MAX_PER_CLUSTER: usize = 64;
/// Per-attempt restore wake wait, passed to the substrate as
/// `ActivationOptions::ready_timeout`. The Kubernetes substrate clamps every
/// blocking wake wait to its API-side [`NODE_RPC_BLOCKING_BUDGET`] (~135s,
/// held below the node-RPC channel timeout), so values at or above that
/// budget do not extend the wait: each attempt is cut at the budget and
/// resolves to [`RestoreOutcome::Converging`], and a restore slower than one
/// budget converges across sweep re-drives. Values *below* the budget do
/// bind and shorten the per-attempt wait (the kind chart sets 60s for faster
/// local feedback). The 600s default therefore means "defer to the substrate
/// budget": it bounds one task's slot occupancy, not the end-to-end restore.
pub(crate) const DEFAULT_EVACUATE_TIMEOUT_SECS: u64 = 600;
/// How long a sandbox is allowed to sit in *any* non-evacuable resident
/// transient lifecycle (see [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]) on a
/// draining node before the reconciler abandons it (flips the row to
/// `lifecycle = 'failed'` so drain can proceed). The default matches
/// `DEFAULT_EVACUATE_TIMEOUT_SECS` — once a sandbox has been wedged in a
/// transient state longer than one evacuation budget, treating it as
/// failed is the right call: the user already understands a draining node
/// may discard work in flight, and a row that hasn't left a transient
/// lifecycle in that long is one the start/wake path has effectively given
/// up on. The drain's own in-flight evacuation suspend/restore is excluded
/// by the `evacuation_restore_target IS NOT NULL` marker, so this never
/// aborts the pipeline's own work. Wrapped in `Option<PositiveDuration>`
/// at the config layer so tests can disable the policy; the reconciler always
/// uses this default.
pub(crate) const DEFAULT_STALE_ABANDON_SECS: u64 = 600;
/// How long a `suspended` sandbox whose local-snapshot reupload is permanently
/// refused (not `Suspended` on the node — a stuck on-node `restoring`) must
/// have been settled before the node-drain give-up adopts an existing ready
/// remote snapshot to unblock the drain. Measured by
/// `sandboxes.lifecycle_changed_at`, so the window spares a row whose on-node
/// state is merely a quick transient: a freshly-completed suspend whose node is
/// still finishing publishes within seconds, and a `suspending` row resolves
/// well inside this budget, so the reupload succeeds (or the artifact becomes
/// durable) before the window elapses. A restore wedged longer than the budget
/// is eligible immediately. The default matches `DEFAULT_STALE_ABANDON_SECS`.
/// Wrapped in `Option<PositiveDuration>` at the config layer so tests can
/// disable the give-up entirely (`None` — the reupload is retried forever, the
/// prior behavior); the reconciler always uses this default.
pub(crate) const DEFAULT_REUPLOAD_GIVEUP_SETTLE_SECS: u64 = 600;
/// First-failure delay for the cohort-restore retry backoff. A sandbox whose
/// cohort restore fails is not re-selected by the restore sweep until
/// `base * 2^(failures - 1)` has elapsed (capped at
/// [`DEFAULT_RESTORE_BACKOFF_CAP_SECS`]), instead of being retried on every
/// 5s tick forever. One minute keeps the first retry prompt for transient
/// failures while a deterministic failer quickly backs off out of the shared
/// restore budget. Wrapped in `Option<PositiveDuration>` at the config layer;
/// tests can disable the backoff entirely (`None` — every failure retried on
/// the next sweep, the prior behavior); the reconciler always uses this
/// default.
pub(crate) const DEFAULT_RESTORE_BACKOFF_BASE_SECS: u64 = 60;
/// Upper bound on the per-sandbox cohort-restore retry delay. 30 minutes
/// bounds how long a recovered sandbox (e.g. its wedged target node finally
/// cleared) waits for its next automatic retry, while still keeping a
/// permanently-failing sandbox down to ~2 attempts/hr of budget use.
pub(crate) const DEFAULT_RESTORE_BACKOFF_CAP_SECS: u64 = 1800;
const DRAIN_EVACUATION_COHORT_KEY: &str = "space.pplx.net/rollout-cohort";

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Defaults to `false` so the reconciler is inert
    /// in every environment until the operator explicitly opts in via
    /// `SPACE_NODE_DRAIN_ENABLED=true`. The opt-in is per-environment and
    /// is the gate behind which the local-disk-suspend trade-off (see
    /// `RESIDENT_LIFECYCLES`) and the IRSA dependency
    /// for `TerminateInstanceInAutoScalingGroup` live: do not flip this
    /// on until both durable S3 suspend has landed and the
    /// `clusterController.iamRoleArn` IRSA role is wired.
    ///
    /// Opt-in because the default terminator backend is `aws`: enabling this
    /// on a non-AWS cluster would initialize the AWS SDK and attempt EC2
    /// instance termination. Non-AWS deployments must pick `kube-delete` or
    /// `noop` via `SPACE_NODE_DRAIN_TERMINATOR_KIND` when enabling it.
    pub enabled: bool,
    /// Gate on the fourth `draining → drained` term — the one that holds a node
    /// the drained-node terminator's resident guard would refuse. Defaults to
    /// `false`, and the default is the point.
    ///
    /// The term is correct and the trade it makes is deliberate, but it is a
    /// behavior change with a rollout-halting failure mode. `stopped` is an ordinary
    /// long-lived user state that keeps its `active_node_id` (the pod
    /// materialization is retained), the evacuation pipeline cannot service it —
    /// `RESIDENT_LIFECYCLES`' own docs say counting it "makes a drain wait on
    /// something the pipeline cannot service" — and no sweeper clears it while
    /// the node is alive. So one user sandbox left `stopped` on a worker holds
    /// that node until someone clears the sandbox.
    ///
    /// **That hold is not what this flag buys or avoids.** `space-rollout-mng.py`
    /// counts a source node as in-flight while `drain_phase IN {draining,
    /// drained, terminating}`, so a node parked in `drained` occupies a
    /// concurrency slot exactly as one parked in `draining` does. An MNG rollout
    /// stalls on such a sandbox either way; the flag decides only which phase it
    /// stalls in, and therefore what the owner can still do about it.
    ///
    /// `node_drain.termination_blocking_residents` is emitted **regardless of
    /// this flag**, so every environment measures the prevalence before anything
    /// changes behaviour. Flip per environment once those numbers are known.
    /// Off, the gate behaves exactly as it did before this term existed: such a
    /// node reaches `drained` and parks at the terminator, which is the bug —
    /// visible now in the gauge rather than silent.
    ///
    /// **What the flip is not a trade against: termination safety.** Both
    /// settings park the node; neither terminates it. The drained-node
    /// terminator's `drained → terminating` flip, its KubeNodeGone reap, the
    /// admin `:terminate` 409, and
    /// [`Reconciler::handle_terminating_node`]'s pre-terminate recheck each read
    /// `sandbox_residency::active_resident_predicate` **ungated**, and each
    /// refuses a node holding such a resident. This term is the only *decision*
    /// the flag reaches — both halves of it, this pre-check and the completion
    /// write's guard scope, which is why two call sites read the field. The
    /// other half of the decision is therefore *reachability*, not safety, and
    /// it is **conditional on the drain mode**. `NodeDrainState::blocks_placement`
    /// 409s a user `start` on a `drained` node in either mode;
    /// `blocks_user_calls_while_draining` is `matches!(self, Self::Forced)`, so a
    /// `draining` node blocks that `start` under a forced drain and leaves it
    /// open under a gentle one. Turning this flag on therefore restores the
    /// owner's `start` only on a **gentle** drain — and
    /// `space-rollout-mng.py` posts `mode: "forced"` for every MNG rollout, so
    /// on the drain that rollout issues the flip buys nothing on reachability.
    /// Weigh it for the gentle drains an operator runs by hand.
    ///
    /// A parked node is not a trapped sandbox in either mode. Its owner can
    /// always **delete** it — `delete` is not a `SandboxAction` variant, so the
    /// gates that dispatch on one cannot refuse it — and an operator can
    /// `:undrain` the node from `drained`, which clears the park at the cost of
    /// abandoning the drain. An armed auto-lifecycle `Delete` stage also clears
    /// such a resident unattended and does so from *any* state, because that arm
    /// carries no `SandboxState` guard while the `Stop`/`Suspend`/`Pause` arms
    /// beside it do; it applies only where a policy is configured and due. What
    /// gate-on adds is the one exit that keeps the sandbox: starting it. That
    /// predicate's docs carry the per-lifecycle detail.
    pub termination_blocking_gate: bool,
    pub tick: Duration,
    pub max_per_node: usize,
    /// Cluster-wide budget on concurrent drain-driven substrate operations.
    /// **Shared** across both phases: the suspend evacuations (`spawn_bounded`)
    /// and the decoupled cohort restores (`spawn_restores`) draw from this one
    /// budget, so total drain-driven pressure on the substrate (the Kubernetes
    /// API and the suspend/restore RPCs) can't exceed it regardless of how the
    /// work splits between suspending and restoring at any instant.
    pub max_per_cluster: usize,
    /// Per-attempt wake wait for the cohort restore
    /// (see [`DEFAULT_EVACUATE_TIMEOUT_SECS`]), clamped by the substrate's
    /// [`NODE_RPC_BLOCKING_BUDGET`] — see [`DEFAULT_EVACUATE_TIMEOUT_SECS`]
    /// for the clamp story. NOT an end-to-end restore deadline: an attempt
    /// that hits the budget resolves to [`RestoreOutcome::Converging`] and is
    /// re-driven by the next sweep.
    pub evacuate_timeout: PositiveDuration,
    /// Age threshold for the abandon-stuck-transient policy. A sandbox
    /// whose lifecycle has been one of [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]
    /// (and whose `active_node_id` is pinned to a draining node) for longer
    /// than this is flipped to `lifecycle = 'failed'` so the node can finish
    /// draining. `None` disables the policy entirely — the sweep is not even
    /// called.
    ///
    /// `Option<PositiveDuration>` carries two type-level invariants:
    /// (1) the disabled case is `None`, not an in-band magic value
    /// like `Duration::ZERO`, so downstream consumers can't forget the
    /// off-check and accidentally feed zero to the SQL (which would
    /// match every transient row and abandon every in-flight transition
    /// on every draining node); (2) when present, the value is guaranteed
    /// non-zero by `PositiveDuration`'s smart constructor, so the
    /// sink-side SQL never receives `INTERVAL '0s'` even via a
    /// programmer-error path.
    pub stale_abandon: Option<PositiveDuration>,
    /// Settle window for the local-snapshot-promotion give-up. A `suspended`
    /// sandbox whose reupload is permanently refused (not `Suspended` on the
    /// node — a stuck on-node `restoring`) is, after it has been settled this
    /// long (by `lifecycle_changed_at`) and **if the current suspend cycle's
    /// ready remote snapshot exists**, made durable from that snapshot so the
    /// node finishes draining instead of retrying the refused reupload forever.
    /// `None` disables the give-up — the reupload is retried every tick (prior
    /// behavior).
    ///
    /// `Option<PositiveDuration>` for the same two reasons as
    /// [`Self::stale_abandon`]: the disabled case is `None` (not a magic
    /// zero a downstream consumer could forget to special-case), and when
    /// present the value is guaranteed non-zero so the give-up SQL never sees a
    /// zero settle interval that would fire on a just-suspended row.
    pub reupload_giveup_settle: Option<PositiveDuration>,
    /// First-failure delay for the cohort-restore retry backoff. A failed
    /// cohort restore schedules its next attempt
    /// `base * 2^(failures - 1)` out (capped at
    /// [`Self::restore_backoff_cap`]) instead of being re-selected on the
    /// next tick, so a deterministic failer can't monopolize the shared
    /// `max_per_cluster` restore budget. `None` disables the backoff — every
    /// failure is retried on the next sweep (the prior behavior).
    ///
    /// `Option<PositiveDuration>` for the same two reasons as
    /// [`Self::stale_abandon`]: the disabled case is `None` (not a magic zero
    /// a downstream consumer could forget to special-case), and when present
    /// the value is guaranteed non-zero so the backoff SQL never computes a
    /// zero delay that would re-arm the failer immediately.
    pub restore_backoff_base: Option<PositiveDuration>,
    /// Upper bound on the per-sandbox cohort-restore retry delay. Only
    /// consulted when [`Self::restore_backoff_base`] is `Some`.
    pub restore_backoff_cap: PositiveDuration,
    /// Target-cohort placement applied to cluster-controller-built
    /// evacuation pods so that, during a two-MNG blue/green node rollout,
    /// they tolerate the rollout MNG's cohort taint *and* a required
    /// nodeAffinity term pins them to the matching cohort label.
    /// User-facing restore pods do not carry this — only
    /// cluster-controller-built evacuation pods do.
    ///
    /// `None` by default — the rollout pattern is opt-in per environment.
    pub drain_evacuation_placement: Option<EvacuationPlacement>,
    /// How long a **gentle** drain may sit in `draining` before the
    /// reconciler rewrites its `drain_mode` to `forced` and starts evacuating.
    /// Measured from `nodes.drain_started_at` in SQL (`NOW() - INTERVAL`), so
    /// it needs no wall clock in this process.
    ///
    /// `None` (the default) disables escalation: a gentle drain waits
    /// indefinitely. That is the right default because escalation is
    /// destructive — it is the disruption the operator declined by choosing
    /// gentle — and because a parked gentle drain is visible rather than
    /// silent (the per-tick stuck-drain warning plus
    /// `node_drain.sandboxes_remaining`). Set it for a shrink campaign, where
    /// a node hosting a sandbox that never auto-suspends would otherwise never
    /// converge; the operator's alternative is `space.pplx.net/drain-keep`
    /// plus a manual decision per node.
    ///
    /// `Option<PositiveDuration>` for the same two reasons as
    /// [`Self::stale_abandon`]: the disabled case is `None` rather than an
    /// in-band zero a consumer could forget to check (a zero interval here
    /// would escalate **every** gentle drain on the tick it starts, converting
    /// the default mode into the forced one), and when present the value is
    /// guaranteed non-zero.
    pub gentle_escalate: Option<PositiveDuration>,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            termination_blocking_gate: false,
            tick: DEFAULT_TICK,
            max_per_node: DEFAULT_MAX_PER_NODE,
            max_per_cluster: DEFAULT_MAX_PER_CLUSTER,
            evacuate_timeout: PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_EVACUATE_TIMEOUT_SECS,
            )),
            stale_abandon: PositiveDuration::new(Duration::from_secs(DEFAULT_STALE_ABANDON_SECS)),
            reupload_giveup_settle: PositiveDuration::new(Duration::from_secs(
                DEFAULT_REUPLOAD_GIVEUP_SETTLE_SECS,
            )),
            restore_backoff_base: PositiveDuration::new(Duration::from_secs(
                DEFAULT_RESTORE_BACKOFF_BASE_SECS,
            )),
            restore_backoff_cap: PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_RESTORE_BACKOFF_CAP_SECS,
            )),
            drain_evacuation_placement: None,
            gentle_escalate: None,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.node_drain.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config.truthy_default("SPACE_NODE_DRAIN_ENABLED", default.enabled),
            termination_blocking_gate: config.truthy_default(
                "SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE",
                default.termination_blocking_gate,
            ),
            tick: default.tick,
            max_per_node: default.max_per_node,
            max_per_cluster: default.max_per_cluster,
            evacuate_timeout: default.evacuate_timeout,
            stale_abandon: default.stale_abandon,
            reupload_giveup_settle: default.reupload_giveup_settle,
            restore_backoff_base: default.restore_backoff_base,
            restore_backoff_cap: default.restore_backoff_cap,
            drain_evacuation_placement: env_evacuation_placement(
                env,
                "SPACE_CLUSTER_CONTROLLER_DRAIN_EVACUATION_COHORT_VALUE",
            ),
            gentle_escalate: None,
        }
    }
}

/// Parse a target-cohort placement from a `(cohort_key, cohort_value)` pair
/// of env vars. Returns `None` when either var is unset, empty, or
/// whitespace-only — the two-MNG rollout is opt-in and never required for
/// the reconciler to make forward progress.
fn env_evacuation_placement(
    env: &dyn pplx_space_common_core::env::Environment,
    value_var: &str,
) -> Option<EvacuationPlacement> {
    let value = env.var(value_var).ok().unwrap_or_default();
    EvacuationPlacement::from_parts(DRAIN_EVACUATION_COHORT_KEY, value)
}

/// Errors from a per-sandbox promotion attempt. Mapped per-variant to
/// the `node_drain.local_snapshot_promotion{outcome}` counter:
///
/// - [`Self::ArtifactMissing`] → `failed_artifact_missing` (terminal —
///   triggers the mark-sandbox-failed policy so drain can proceed).
/// - [`Self::SandboxRestoringOnNode`] → `gave_up_remote_snapshot` once the
///   give-up flips the row durable, or `restoring_retry` while it is still
///   settling / has no ready newest remote snapshot yet.
/// - Every other variant → `failed` (retryable; retried implicitly on
///   the next tick).
#[derive(Debug, thiserror::Error)]
pub enum PromoteError {
    /// SQL lookup for the source-node endpoint failed.
    #[error("db lookup failed: {0}")]
    Db(String),
    /// On-node sbx-mgr returned an error (transport blip,
    /// FailedPrecondition for transient/config reasons, etc.).
    #[error("on-node backup enqueue failed: {0}")]
    NodeApi(String),
    /// On-node sbx-mgr reported the suspended artifact bytes are
    /// gone from disk. Combined with the sandbox not having a matching
    /// ready backup (otherwise the reconciler would not have tried to
    /// promote it), the sandbox's data is unrecoverable from
    /// anywhere. Terminal — the reconciler marks the sandbox row as
    /// `lifecycle = 'failed'` so it drops out of the local-only
    /// counter and drain can proceed; the customer's next
    /// `wake`/`restore` call gets a clean lifecycle error rather than
    /// a confusing snapshot-not-found.
    #[error("local suspended artifact gone, sandbox unrecoverable: {0}")]
    ArtifactMissing(String),
    /// The on-node sbx-mgr refused the suspend-snapshot reupload because the
    /// sandbox is wedged in `restoring` on the node (a stuck on-node restore)
    /// while the cluster SSOT reads it `suspended`. Classified off
    /// `REUPLOAD_SANDBOX_RESTORING_MARKER`.
    ///
    /// Unlike [`Self::ArtifactMissing`], the local artifacts are not gone — the
    /// reupload simply can't proceed from a stuck restore, and the divergence
    /// need not self-heal before the node must drain. The reconciler routes
    /// this to the give-up path
    /// ([`NodeDrainSink::make_sandbox_durable_from_remote_snapshot`]): after a
    /// settle window, **if the newest ready remote snapshot already exists**,
    /// it adopts that snapshot (makes the row durable) so the
    /// node finishes draining and the sandbox stays recoverable, instead of
    /// retrying the permanently refused reupload forever.
    ///
    /// Scoped to the `restoring` state by design: the VM is loading **from**
    /// the snapshot, so it holds no user state newer than the snapshot, which
    /// makes the snapshot canonical. A refusal from a running state
    /// (`started`/`paused`) or a modified-disk `stopped` carries no marker and
    /// never reaches this variant — abandoning their potentially-newer state
    /// for an older snapshot would lose work.
    #[error("sandbox restoring on node (reupload refused): {0}")]
    SandboxRestoringOnNode(String),
    /// The substrate could not resolve the node-api endpoint (deleted
    /// node, missing inventory row, …).
    #[error("node-api endpoint unresolved: {0}")]
    EndpointUnresolved(String),
}

/// Surface the reconciler depends on. Implementations: [`SubstrateSink`]
/// for production (Postgres + KubernetesSubstrate); [`tests::FakeSink`] for
/// unit tests.
///
/// `evacuate_sandbox` returns an explicit `+ Send` future because it is
/// awaited inside `tokio::spawn` (long-running tasks bounded by the
/// per-node and per-cluster semaphores). The remaining methods are awaited
/// from the tick task itself, where Send is already implied by the
/// surrounding context.
pub trait NodeDrainSink: Send + Sync + 'static {
    /// Nodes currently in `nodes.drain_phase = 'draining'`, each with the
    /// drain mode the operator chose for it.
    fn list_draining_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<DrainingNode>, sqlx::Error>> + Send;
    /// Rewrite `drain_mode` to `forced` on each of `candidates` whose drain
    /// started more than `age` ago, returning the ids actually escalated.
    ///
    /// `candidates` are the nodes the **caller** has already resolved as gentle
    /// via [`NodeDrainMode::from_column_str`]. Keeping the classification here,
    /// in Rust, is what stops the NULL-and-unrecognized-read-as-gentle policy
    /// from being restated in SQL where it could drift from the reader.
    ///
    /// The age comparison stays in SQL against `drain_started_at`, so the
    /// deadline is measured on the database's clock rather than this process's —
    /// a restarted or clock-skewed controller cannot escalate early.
    ///
    /// The UPDATE is the escalation: everything downstream (this reconciler's
    /// evacuation branch, the user-API 409 gates) reads the column, so a single
    /// committed write flips every consumer at once with no in-memory state to
    /// keep consistent.
    fn escalate_stale_gentle_drains(
        &self,
        candidates: &[NodeId],
        age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<NodeId>, sqlx::Error>> + Send;
    /// Node IDs currently in `nodes.drain_phase = 'terminating'`.
    fn list_terminating_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<NodeId>, sqlx::Error>> + Send;
    /// How many sandboxes on `node_id` carry a lifecycle that means they still
    /// occupy it (see `RESIDENT_LIFECYCLES`), split by whether
    /// they count. `occupying` is what gates `draining -> drained`, regardless
    /// of whether the reconciler can actively evacuate them.
    ///
    /// Counts rather than ids because the reconciler only asks "how many" and
    /// "any left?" — and under [`ResidencyEvidence::NodeCensusAware`] the
    /// store discounts rows the node's own census proves absent. The discount
    /// is one-directional: a node the control plane cannot see keeps every row
    /// counting, because a
    /// drain that completes on a live node terminates it.
    fn count_resident_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<ResidentSandboxCounts, sqlx::Error>> + Send;
    /// `(sandbox_id, pre_lifecycle)` pairs for sandboxes resident on
    /// `node_id` whose lifecycle is in [`EVACUATABLE_LIFECYCLES`] — the
    /// subset the reconciler will actually attempt to suspend + restore
    /// this tick. The IDs in the returned slice are always a subset of
    /// `list_sandboxes_on_node`. Splitting the two queries keeps the
    /// "still resident" predicate (which blocks drain completion) from
    /// being entangled with the "safe to evacuate" predicate (which
    /// drives the spawn loop).
    ///
    /// `pre_lifecycle` is forwarded into
    /// [`NodeDrainSink::evacuate_sandbox`] so the restore RPC can be
    /// issued with the matching `target_state` (`Started` resumes the
    /// VM after snapshot load; `Paused` skips the resume and keeps the
    /// VM in the firecracker-paused state, preserving the user-visible
    /// lifecycle without any post-restore RPC).
    fn list_evacuatable_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error>> + Send;
    /// Suspended sandboxes still pinned to this node whose current local
    /// artifact has no ready backup yet. For new rows, portability requires a
    /// ready backup of the exact `current_local_artifact_id`; nullable
    /// rollout-skew rows use the legacy snapshot test. Returned IDs are fed
    /// into [`NodeDrainSink::promote_local_snapshot`].
    ///
    /// `exclude_legacy_rows` drops rows with no `current_local_artifact_id`,
    /// whose only backup route is the legacy sandbox-id-only reupload. That
    /// route reads the live on-disk snapshot directory and checks the sandbox is
    /// `Suspended` only when the job is enqueued, so it is safe only while
    /// nothing can wake the sandbox mid-upload. A **gentle** drain leaves the
    /// user-API gate open by design, so it passes `true` here and lets those
    /// rows keep blocking rather than risk registering a torn snapshot.
    fn list_pinned_local_only_sandboxes_on_node(
        &self,
        node_id: NodeId,
        exclude_legacy_rows: bool,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, sqlx::Error>> + Send;
    /// Count sandboxes that **block** `draining → drained` for this
    /// node — the union of the promotable set above plus mid-state
    /// lifecycles (`suspending`, `restoring`) that pin the node by
    /// virtue of having local-disk artifacts the reconciler cannot
    /// promote (no `state='ready'` row exists yet).
    ///
    /// Routes the portability check through the shared
    /// `fresh_ready_snapshot_exists` SQL fragment so this count
    /// agrees with `drained_node_terminator::count_local_snapshots`
    /// and the admin `:terminate` endpoint's
    /// `sandbox_occupancy_for_node_in_tx` guard: all three observe
    /// the same "is this sandbox portable?" answer, so a node whose
    /// only remaining sandbox is `suspended` + S3-backed advances
    /// uniformly through `draining → drained → terminating` without
    /// wedging at the terminator gate.
    fn count_drain_blocking_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<i64, sqlx::Error>> + Send;
    /// Count sandboxes that would make the drained-node terminator's resident
    /// guard **refuse** this node, so the reconciler never advances a node past
    /// a gate the next one would reject. Routes through
    /// `sandbox_residency::active_resident_predicate`, whose docs carry which
    /// lifecycles that guard counts and why — including the four that
    /// [`NodeDrainSink::count_resident_sandboxes_on_node`]'s include-list treats
    /// as non-occupying.
    ///
    /// Two things are local to this method rather than to the predicate:
    ///
    /// * **A separate count, not a widening of the resident scan.** Adding those
    ///   lifecycles to the resident include-list would enroll them in the
    ///   stale-abandon sweep, which flips wedged resident rows to `failed` —
    ///   auto-destroying a quarantine that exists to be investigated.
    /// * **An error must not read as zero.** An unanswered residency question
    ///   defers the node to the next tick; treating it as "nothing blocking"
    ///   would release a node over a transient DB blip.
    fn count_termination_blocking_residents_on_node(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<TerminationBlockingResidents, sqlx::Error>> + Send;
    /// Enqueue backup for one local-only suspended sandbox through its source
    /// node. Use exact local artifact identity when known, or CurrentInstance
    /// to validate and adopt an unpinned suspended instance. Once the
    /// durable worker registers a matching ready backup, the sandbox drops out
    /// of [`NodeDrainSink::list_pinned_local_only_sandboxes_on_node`].
    fn promote_local_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<(), PromoteError>> + Send;
    /// Mark every sandbox wedged in a non-evacuable resident transient
    /// lifecycle (see [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]) on `node_id`
    /// for longer than `age` as `lifecycle = 'failed'`. Returns the number
    /// of rows actually flipped, so the reconciler can log + emit a metric
    /// only when there was work to do. Implementations must guard the
    /// UPDATE on:
    ///
    /// (a) `lifecycle = ANY(NON_EVACUATABLE_RESIDENT_LIFECYCLES)` — only
    ///     the transient states that can wedge a drain;
    /// (b) `active_node_id = $node_id OR target_node_id = $node_id` — the row
    ///     is attached to this node. Both columns are matched so the sweep
    ///     covers exactly what the drain-complete gate counts: the resident
    ///     set keys on `active_node_id`, but the drain-blocking count keys a
    ///     non-evacuation `restoring`/`suspending` on `target_node_id` (a
    ///     restore that began pre-drain has `active_node_id` cleared but
    ///     `target_node_id` still pinned here);
    /// (c) `(evacuation_restore_target IS NULL OR lifecycle = 'suspending')`
    ///     — never abort the drain's **own** evacuation *restore* (a marked
    ///     `restoring` row, re-driven by the cluster-wide restore sweep), but
    ///     a marked `suspending` row stays eligible because nothing else
    ///     recovers a crashed/hung evacuation suspend. Non-evacuation
    ///     transients (a user wake/suspend/pause that hung, a
    ///     `creating`/`starting` that never finished) are always eligible.
    ///     Mirrors the cluster-wide transient sweeper's guard;
    /// (d) `lifecycle_changed_at < NOW() - age` — wedged at least `age`;
    /// (e) the node still being in `drain_phase = 'draining'` at the moment
    ///     of the UPDATE — so an admin `:undrain` landing between
    ///     `list_draining_nodes()` returning the node id and this UPDATE
    ///     landing can't flip legitimate in-flight rows on a node that is
    ///     no longer being drained.
    ///
    /// The "disabled" case is filtered out at the call site
    /// (`Option<PositiveDuration>` on `ReconcilerConfig`), so this trait
    /// method only ever sees a positive `age` — `PositiveDuration` carries
    /// the non-zero invariant at the type level, so no `is_zero` guard is
    /// needed in either the dispatch or the impl.
    fn abandon_stale_resident_sandboxes(
        &self,
        node_id: NodeId,
        age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// Mark a sandbox `lifecycle = 'failed'` after the reconciler
    /// determined its current local artifact is gone and no ready
    /// S3 snapshot exists. Guarded on `lifecycle = 'suspended' AND
    /// target_node_id = $source_node_id` so a sandbox the user wakes
    /// or that drifts to another node between the promote attempt and
    /// this UPDATE is left alone (the new node's drain will observe
    /// its own state). Returns the number of rows affected — `0` means
    /// the sandbox drifted and the next tick re-evaluates against
    /// whatever's current.
    fn mark_sandbox_lost(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// KubeNodeGone variant of [`Self::mark_sandbox_lost`]: terminal flip for
    /// a pinned local-only `suspended` sandbox whose node's kube Node is gone
    /// and whose EC2 instance is confirmed terminated — the local artifact
    /// bytes died with the instance, so no on-node RPC is needed (or
    /// possible) to establish that the artifact is gone. Guarded like
    /// `mark_sandbox_lost` (`lifecycle = 'suspended' AND target_node_id =
    /// $source_node_id`, no in-flight upload), but with a **stricter**
    /// snapshot guard: it refuses while *any* ready full snapshot exists,
    /// regardless of placement freshness or artifact identity. This path
    /// chains after [`Self::make_sandbox_durable_on_gone_node`], and the
    /// adopt CAS can return 0 transiently (settle window) for a row it
    /// would adopt on a later tick — the stricter guard keeps that row
    /// parked instead of terminally failing it.
    ///
    /// `expected_uid` is the `node_k8s.k8s_node_uid` the EC2 cross-check
    /// observed; implementations must refuse (atomically with the flip) if
    /// the uid has since changed or the node left `drain_phase = 'draining'`
    /// — a same-name kube Node replacement's sandboxes must not be
    /// terminally failed under the dead predecessor's verdict. Returns the
    /// number of rows flipped; `0` means parked, drifted, or
    /// identity-refused, re-evaluated next tick.
    fn mark_sandbox_lost_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// KubeNodeGone variant of
    /// [`Self::make_sandbox_durable_from_remote_snapshot`]: the same
    /// adopt-the-remote-snapshot CAS, plus two guards the gone-node trigger
    /// requires (see `adopt_remote_snapshot_on_gone_node_query` in `store`):
    ///
    /// - the node-identity guards shared with
    ///   [`Self::mark_sandbox_lost_on_gone_node`] (`expected_uid` +
    ///   `drain_phase = 'draining'`), refusing once a same-name kube Node
    ///   replacement registered;
    /// - a current-suspend-cycle anchor on the adopted snapshot. The reupload
    ///   give-up only adopts with a `SandboxRestoringOnNode` proof (the VM is
    ///   loading *from* the snapshot, making it canonical); this path has no
    ///   such proof, so for a legacy nullable row whose last re-suspend never
    ///   registered a snapshot record, the newest-ready test alone would
    ///   adopt the prior cycle's snapshot and silently roll the sandbox back.
    ///   The anchor refuses that; the row parks or is marked lost instead.
    ///
    /// Returns the number of rows flipped; `0` means not eligible this tick.
    fn make_sandbox_durable_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
        expected_uid: Option<&str>,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// Terminal give-up for a sandbox whose evacuation suspend was rejected
    /// because it is already `failed` on the node while the DB still reads it
    /// `started`/`paused` (see [`EvacuateError::SandboxFailedOnNode`]). Flip
    /// the row to `lifecycle = 'failed'`, clearing any evacuation marker, so
    /// the sandbox drops out of the resident set and the node can finish
    /// draining; the implementation also best-effort tears down the backing
    /// pod (the dead Firecracker process is still resident). Guarded on
    /// `active_node_id = $source_node_id AND lifecycle = ANY(EVACUABLE_LIFECYCLES)`
    /// so a sandbox that drifted (woken elsewhere) between the failed suspend
    /// and this flip is left alone. Returns the number of rows flipped — `0`
    /// means the sandbox drifted and the next tick re-evaluates against
    /// whatever's current.
    fn mark_sandbox_failed_on_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// Give-up for a `suspended` sandbox whose local-snapshot promotion is
    /// permanently refused because it is wedged in `restoring` on the node
    /// while the DB reads `suspended` (see
    /// [`PromoteError::SandboxRestoringOnNode`]). When the newest ready remote
    /// snapshot already exists, **adopt it**: make the row durably restorable
    /// from that snapshot so it drops out of the pinned-local / drain-blocking
    /// set and the node can finish draining, instead of retrying the refused
    /// reupload forever. The sandbox stays `suspended` and recoverable — its
    /// next user `restore`/`wake` loads the adopted snapshot — at the cost of
    /// the (un-uploadable) local state the stuck restore left behind.
    ///
    /// Implementations must run this as one atomic CAS gated on:
    /// - `lifecycle = 'suspended' AND target_node_id = $source_node_id` — still
    ///   the pinned suspended row we observed (drifted/woken rows are left
    ///   alone);
    /// - the **newest** full snapshot is durably `ready` — the recoverability
    ///   precondition **and** the data-loss guard: it adopts only the most-recent
    ///   capture, never a stale prior-cycle one a `wake → modify → re-suspend`
    ///   left behind (a newer failed re-suspend snapshot makes the newest unready,
    ///   so the give-up doesn't fire). This is the same newest-full-snapshot
    ///   condition the cohort restore selects on. With none, the row keeps
    ///   blocking (a visible, alertable stuck drain) rather than losing the
    ///   sandbox or restoring stale bytes;
    /// - no suspend upload is in flight from **any** node — never race a
    ///   finalizing upload that would make the current artifact durable on its
    ///   own;
    /// - `lifecycle_changed_at < NOW() - settle` — the settle window that
    ///   spares a sandbox whose on-node state is merely a quick transient (a
    ///   freshly-completed suspend whose node is still finishing, a `suspending`
    ///   that will resolve). `settle` is always positive (`PositiveDuration`),
    ///   so the SQL never sees a zero interval that would fire immediately.
    ///
    /// On the flip, clears the row's current-artifact pointer and evacuation
    /// marker and neutralizes the placement-freshness anchor so the existing
    /// ready snapshot satisfies the portability gate, and emits the
    /// `LocalSnapshotPromotionAbandoned` sandbox event. Returns the
    /// number of rows flipped — `0` means not yet eligible (still settling, no
    /// ready remote snapshot, an in-flight upload, or drifted), so the next
    /// tick re-evaluates against whatever is current.
    fn make_sandbox_durable_from_remote_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// Suspend + restore one sandbox. The implementation owns the
    /// timeouts and the underlying substrate calls.
    ///
    /// `source_node_id` is the node the sandbox was observed on at
    /// listing time; the implementation must guard the lifecycle claim
    /// against `active_node_id` to avoid evacuating a sandbox that has
    /// since been moved to a different node (by a user wake on a
    /// previously-suspended sandbox, an admin force-evacuate, etc.).
    ///
    /// `pre_lifecycle` is the user-visible lifecycle the sandbox was in
    /// at listing time (one of [`SandboxLifecycle::Started`] or
    /// [`SandboxLifecycle::Paused`]). The implementation CAS-claims
    /// against this exact value (`lifecycle = $pre_lifecycle`) so a
    /// concurrent user-driven `pause` / `resume` between listing and
    /// claim is detected and the evacuation backs off. The restore RPC
    /// is issued with `target_state = pre_lifecycle`, so sbx-mgr lands
    /// the VM in the matching state in one shot — a `paused` sandbox
    /// is restored with the post-load resume skipped, and the row
    /// flips to `lifecycle = 'paused'` directly.
    ///
    /// `placement`, when `Some`, pins the rebuilt restore pod to the
    /// active target cohort during a two-MNG blue/green rollout: the
    /// substrate renders a matching `Equal`-operator toleration and a
    /// required nodeAffinity term derived from the placement's
    /// `(cohort_key, cohort_value)` pair. `None` (the steady-state case)
    /// drops the source-node pin but adds no cohort placement.
    /// Suspend one sandbox off its source node, persisting the
    /// `evacuation_restore_target` marker so the cohort restore can be
    /// driven later. This is only the **suspend** half of the evacuation:
    /// it claims `pre_lifecycle → suspending` (recording the marker), runs
    /// the blocking local suspend RPC, commits `suspended`, and enqueues
    /// backup of the published artifact. The cohort restore is decoupled and
    /// driven independently once that backup is ready — see
    /// [`NodeDrainSink::restore_evacuated_sandbox`].
    fn evacuate_sandbox(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        pre_lifecycle: SandboxLifecycle,
    ) -> impl std::future::Future<Output = Result<(), EvacuateError>> + Send;
    /// `(sandbox_id, restore_target_lifecycle)` for sandboxes anywhere in
    /// the cluster that were suspended mid-drain-evacuation and are now
    /// ready to be restored onto a target-cohort node: `lifecycle =
    /// 'suspended'`, a non-NULL `evacuation_restore_target` marker, **and**
    /// a ready backup for its current suspended artifact.
    ///
    /// The cohort restore is **decoupled from the source node's drain**:
    /// the source may already have drained and terminated once the snapshot
    /// was uploaded (the restore reads from S3, not the source). So this
    /// scan is cluster-wide and keys only on the immutable marker +
    /// portability — never on the mutable `target_node_id`, which the
    /// restore's own pod placement moves to the cohort node. It is the
    /// correctness fallback for backup-readiness notifications; both feed
    /// [`NodeDrainSink::restore_evacuated_sandbox`].
    ///
    /// `restore_target_lifecycle` is the user-visible lifecycle the sandbox
    /// must be returned to (`Started` or `Paused`), persisted at suspend
    /// time so the restore preserves it even across a controller restart. A
    /// plain user-suspended sandbox carries a NULL marker and is never
    /// selected.
    ///
    /// The list is **ordered by ascending failed-attempt count, then
    /// most-recently-active first** (see
    /// [`store::restore_pending_evacuations_query`] for why attempt count
    /// must outrank recency). [`Reconciler::spawn_restores`] fills the
    /// bounded cluster restore budget in list order, so under slot pressure
    /// never-failed sandboxes win the slots, hottest first within a tier.
    ///
    /// `honor_backoff_windows` mirrors whether the retry backoff is enabled
    /// ([`ReconcilerConfig::restore_backoff_base`] is `Some`): when true, a
    /// row whose `next_restore_attempt_at` is still in the future is
    /// excluded; when false the backoff state is ignored outright — the
    /// window gate is skipped and the attempt-tier ordering key is dropped
    /// (pure recency order) — so disabling the backoff immediately restores
    /// the pre-backoff behavior even for rows carrying stale windows or
    /// frozen attempt counts written before the disable.
    fn list_restore_pending_evacuations(
        &self,
        honor_backoff_windows: bool,
    ) -> impl std::future::Future<Output = Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error>> + Send;
    /// Record one failed cohort-restore attempt for `sandbox_id`: bump its
    /// `restore_attempt_count` and schedule the earliest next attempt
    /// `min(backoff_base * 2^(previous count), backoff_cap)` from now, so
    /// [`NodeDrainSink::list_restore_pending_evacuations`] keeps the sandbox
    /// out of the sweep until then. Implementations must apply both writes in
    /// one atomic statement guarded on the `evacuation_restore_target` marker
    /// still being set — a user takeover that raced the failed attempt owns
    /// the row now, and backoff state must not be planted on it.
    ///
    /// Returns the post-increment attempt count, or `None` when the marker
    /// guard didn't match (nothing written). The backoff state is erased
    /// (DB trigger) the moment the marker clears — restore success or user
    /// takeover — so each evacuation starts from a clean slate.
    fn schedule_restore_retry(
        &self,
        sandbox_id: SandboxId,
        backoff_base: PositiveDuration,
        backoff_cap: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Option<i32>, sqlx::Error>> + Send;
    /// Recover `suspending` rows stranded after local publication. For rows
    /// with a current local artifact identity, the retired local publication
    /// permits advancement before backup so the pinned-local pass can enqueue
    /// it. Nullable rollout-skew rows advance only after the legacy durable
    /// snapshot is ready. Returns the sandbox ids advanced.
    ///
    /// Run before [`NodeDrainSink::list_restore_pending_evacuations`] on each
    /// restore sweep so a freshly-recovered row is restored in the same pass.
    ///
    /// Advances only rows whose suspend cleanup already completed — gated on
    /// `active_node_id IS NULL`, which `finish_suspended_execution` sets together
    /// with retiring the active execution and clearing placement. That same gate
    /// rules out a live in-flight suspend (which holds `active_node_id` set until
    /// just before its own `suspended` commit), so no in-memory coordination is
    /// needed. See [`store::redrive_stranded_suspending_query`] for the full
    /// rationale, including the narrow crash-before-finish case it deliberately
    /// leaves to wedge (visibly) rather than restore unsafely.
    fn redrive_stranded_suspending_evacuations(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, sqlx::Error>> + Send;
    /// Restore one mid-evacuation suspended sandbox onto a target-cohort
    /// node and finalize it back to `target_lifecycle`, clearing the
    /// `evacuation_restore_target` marker. `restore_evacuated_execution`
    /// with the configured cohort `placement` and
    /// `target_state = target_lifecycle`, then a CAS-guarded finalize to
    /// `target_lifecycle` + `placement = 'on_node'`. The suspended →
    /// restoring claim is atomic on the marker, so it loses to a concurrent
    /// user restore rather than restoring a sandbox the user took over.
    ///
    /// `evacuate_timeout` bounds one attempt's blocking wake wait, clamped by
    /// the substrate's [`NODE_RPC_BLOCKING_BUDGET`] (see
    /// [`DEFAULT_EVACUATE_TIMEOUT_SECS`]). A wait that exhausts the budget
    /// resolves to [`RestoreOutcome::Converging`], not an error.
    ///
    /// Returns an explicit `+ Send` future because it is awaited inside
    /// `tokio::spawn` (the restore polls until the rebuilt pod is
    /// command-ready).
    fn restore_evacuated_sandbox(
        &self,
        sandbox_id: SandboxId,
        target_lifecycle: SandboxLifecycle,
        evacuate_timeout: PositiveDuration,
        placement: Option<EvacuationPlacement>,
    ) -> impl std::future::Future<Output = Result<RestoreOutcome, EvacuateError>> + Send;
    /// Transition the node row to `new_state`, guarding on
    /// `expected_state`. If `last_error` is `Some`, the implementation
    /// writes it to `nodes.drain_last_error` in the same UPDATE;
    /// otherwise it clears the column.
    ///
    /// `expected_state` is the only correctness gate against a replacement
    /// node racing inventory upsert: between the reconciler observing a
    /// node in `draining`/`terminating` and writing its next state, the
    /// kube reflector can have reset `nodes.drain_phase` to `ready`/`registered`
    /// for a fresh `node_k8s.k8s_node_uid` (the iter-4 UID-mismatch path).
    /// Without this guard, the reconciler would stomp the fresh state with
    /// `drained`/`deleted` and mask the replacement node as drained.
    ///
    /// Returns `true` if the UPDATE matched the expected state and the
    /// row was rewritten; `false` if the row was missing or the state had
    /// drifted (treat as a stale tick — log and let the next tick re-
    /// evaluate against current state).
    fn set_drain_phase(
        &self,
        node_id: NodeId,
        expected_phase: &str,
        new_phase: &str,
        last_error: Option<&str>,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
    /// Delete the `nodes` row for a node that successfully terminated.
    /// Per the corrected drain design, there is no `'deleted'` drain
    /// phase — terminate success removes the row outright. Guarded on
    /// `drain_phase = 'terminating'` so a row drifted under us (admin
    /// undrain after the ASG call already succeeded, etc.) is left
    /// alone; returns `true` if the row was deleted, `false` if the
    /// guard didn't match.
    fn delete_terminating_node(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
    /// Per-`drain_phase` node counts across the cluster. Used to emit
    /// `node_drain.nodes_by_state{state=<phase>}` once per tick. Returns
    /// counts only for non-NULL `drain_phase` values; the reconciler
    /// emits zeros for known phases the query doesn't return so
    /// dashboards don't drop tags.
    fn node_phase_counts(
        &self,
    ) -> impl std::future::Future<Output = Result<HashMap<String, u64>, sqlx::Error>> + Send;
    /// Re-assert `spec.unschedulable=true` on the kube Node backing
    /// `node_id`. The admin `:drain` endpoint's substrate patch is
    /// best-effort; this seam is how the reconciler converges if Phase 2
    /// of `POST :drain` failed. Idempotent — patching an already-cordoned
    /// Node is a no-op. Called on every tick for `draining` and
    /// `terminating` nodes, before any work that depends on no fresh
    /// pods landing on the node.
    ///
    /// Returns [`CordonOutcome::KubeNodeGone`] (a success — there is
    /// nothing left to cordon, so drain progress is not blocked) when the
    /// kube Node object is gone, so the caller can route `draining` rows
    /// to the KubeNodeGone recovery instead of retrying node-bound work
    /// that can never reach a deleted node.
    fn ensure_node_unschedulable(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<CordonOutcome, CordonError>> + Send;
    /// KubeNodeGone bulk give-up: flip every sandbox still resident on the
    /// draining node (`active_node_id = node_id`, lifecycle in
    /// `RESIDENT_LIFECYCLES`) to `lifecycle = 'failed'`.
    /// Only called after the kube Node was observed gone **and** EC2
    /// confirmed the backing instance terminated — every resident VM,
    /// in-flight transition, and unbacked local artifact died with the
    /// instance, so no evacuation or settling can ever service these rows.
    ///
    /// Implementations must guard the flip atomically on the node still
    /// being `drain_phase = 'draining'` and on `node_k8s.k8s_node_uid`
    /// matching `expected_uid` (the uid observed by the EC2 cross-check),
    /// and must spare rows recoverable without the dead node: a marked
    /// `restoring` row (re-driven from S3 by the cluster-wide restore
    /// sweep) and a marked `suspending` row whose current cycle still has
    /// recoverable snapshot evidence. See
    /// [`store::fail_resident_sandboxes_on_gone_node_query`]. Returns the
    /// number of rows flipped.
    fn fail_resident_sandboxes_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// KubeNodeGone companion to the bulk fail: terminalize every in-flight
    /// (`state='creating'`) full suspend-snapshot upload sourced from the
    /// gone node. The upload worker ran on the terminated instance, so no
    /// finalize can ever arrive — and the drain blocker count's
    /// in-flight-upload term counts these rows regardless of the owning
    /// sandbox's lifecycle, so leaving them wedges the node in `draining`
    /// until the stuck-snapshot reaper's age window (or forever where that
    /// reaper is disabled). Same flip the reaper applies, triggered by the
    /// EC2-terminated verdict instead of age, under the same identity
    /// guards as the other gone-node writes (see
    /// `fail_inflight_snapshots_on_gone_node_query` in `store`). Returns
    /// the number of snapshot rows flipped.
    fn fail_inflight_snapshots_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    /// Resolve when an evacuated sandbox's backup has just been registered
    /// `ready` (the cluster-api `NOTIFY`s
    /// `EVACUATION_RESTORE_READY_CHANNEL` after the durable S3 upload
    /// finalizes), so the run loop can drive the cohort restore immediately
    /// rather than waiting for the next periodic tick. The production impl
    /// backs this with a Postgres `LISTEN`; the periodic sweep remains the
    /// correctness fallback, so the signal is advisory — the caller always
    /// re-derives the restore-pending set. On any listener error the impl
    /// backs off and returns (the caller harmlessly runs a sweep anyway), so
    /// this never returns an error and never tight-loops.
    fn wait_for_restore_signal(&self) -> impl std::future::Future<Output = ()> + Send;
}

/// Pluggable ASG-terminate seam. Production: [`Ec2InstanceTerminator`].
pub trait InstanceTerminator: Send + Sync + 'static {
    fn terminate(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<(), TerminateError>> + Send;
}

/// In-flight sandbox evacuations indexed by source node. Used to enforce
/// per-node and per-cluster parallelism across ticks: the tick loop checks
/// this map before spawning new evacuation tasks, and each task removes
/// its entry on completion.
type InFlight = Arc<Mutex<HashMap<NodeId, HashSet<SandboxId>>>>;

/// EC2-confirmed recovery context for a draining node whose kube Node is
/// gone: produced by [`Reconciler::assess_gone_draining_node`] only when the
/// backing instance is definitively terminated.
struct GoneNodeRecovery {
    /// `node_k8s.k8s_node_uid` observed by the EC2 cross-check, carried into
    /// the guarded give-up writes so a same-name kube Node replacement that
    /// registered after the cross-check refuses them (see
    /// [`NodeDrainSink::fail_resident_sandboxes_on_gone_node`]).
    expected_uid: Option<String>,
}

pub struct Reconciler<S: NodeDrainSink, T: InstanceTerminator> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    terminator: Arc<T>,
    in_flight: InFlight,
    /// Evacuation tasks spawned by `spawn_evacuations`. Kept tracked here
    /// (instead of detached via `tokio::spawn`) so [`run`] can drain
    /// them with a bounded timeout on shutdown — otherwise a tokio
    /// runtime drop can abort a task between the CAS-to-`'suspending'`
    /// and the suspend RPC, leaving the sandbox stuck in an
    /// intermediate lifecycle that future ticks won't select (they
    /// filter on `'started'`).
    evacuations: Arc<Mutex<tokio::task::JoinSet<()>>>,
    /// Sandboxes with a cohort-restore task currently in flight, driven by
    /// the cluster-wide restore sweep (and the backup-ready notification).
    /// Decoupled from [`Self::in_flight`] (which is keyed by source node and
    /// gates the per-node drain) because the restore is decoupled from any
    /// node's drain: the source may already have terminated. Used only to
    /// avoid double-driving the same sandbox and to bound restore
    /// parallelism by `max_per_cluster`.
    restore_in_flight: Arc<Mutex<HashSet<SandboxId>>>,
    /// EC2 cross-check for the KubeNodeGone recovery on `draining` rows.
    /// `None` on substrates without a real EC2 to query (`kube-delete` /
    /// `noop`); when absent, a draining node whose kube Node is gone keeps
    /// its pre-recovery behavior (parked, per-sandbox work retried every
    /// tick) — the fail-safe. Same seam as `drained_node_terminator`'s.
    instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
    /// Paces this reconciler's per-node "parked and staying parked" WARNs at
    /// [`HELD_WARN_INTERVAL`], keyed by `(node, reason)` so each condition keeps
    /// its own window. Pruned every tick down to the nodes still listed as
    /// `draining`, so a converged or undrained node can't leak its slots.
    warn_pacer: PerNodeWarnPacer,
}

/// One `draining` node as the tick sees it: its id plus the mode that decides
/// whether this tick evacuates its residents.
///
/// The mode travels with the id (rather than being re-read per node) so one
/// query answers "what is draining, and how" and every branch in
/// [`Reconciler::handle_draining_node`] reads the same value for the whole
/// tick. A mode that changes mid-tick — an escalation, or an
/// `:undrain`+`:drain` — is picked up by the next tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainingNode {
    pub id: NodeId,
    pub mode: NodeDrainMode,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub draining_nodes: usize,
    pub terminating_nodes: usize,
    pub spawned_evacuations: usize,
    pub spawned_restores: usize,
    pub drained_transitions: usize,
    pub terminated: usize,
    pub terminate_failures: usize,
    /// Draining nodes whose kube Node is gone but which could not be
    /// safely auto-recovered this tick: no EC2 cross-check configured, the
    /// instance is still live-but-deregistered, or the EC2 lookup errored.
    /// Surfaced via the `node_drain.kube_node_gone_parked{phase:draining}`
    /// gauge; the per-sandbox work keeps its pre-recovery retry behavior.
    pub kube_node_gone_parked: usize,
    /// Resident sandboxes bulk-flipped to `failed` by the KubeNodeGone
    /// recovery (kube Node gone + EC2 instance confirmed terminated).
    pub kube_node_gone_failed_sandboxes: u64,
    /// Pinned local-only suspended sandboxes the KubeNodeGone recovery
    /// made durable by adopting their newest ready remote snapshot.
    pub kube_node_gone_adopted_sandboxes: usize,
    /// Pinned local-only suspended sandboxes the KubeNodeGone recovery
    /// marked `failed` because no recoverable copy exists anywhere (the
    /// local artifact died with the instance, no ready remote snapshot).
    pub kube_node_gone_lost_sandboxes: usize,
    /// Draining nodes handled in gentle mode this tick: cordoned, promoted,
    /// and counted, but with no evacuation spawned.
    pub gentle_nodes: usize,
    /// Gentle drains rewritten to `forced` this tick because they passed
    /// [`ReconcilerConfig::gentle_escalate`].
    pub gentle_escalations: usize,
    /// Draining nodes the drain gate held this tick, i.e. `blockers > 0`.
    ///
    /// Counted separately from the residents because the two do not overlap
    /// completely: the gate counts rows the per-node gauges cannot show. A
    /// suspended rollout-skew row with a NULL `current_local_artifact_id` is
    /// excluded from the pinned list under gentle mode and is not a resident
    /// lifecycle, so `sandboxes_remaining` and `pinned_suspended` both read
    /// zero while the node stays in `draining`. Without this the park is
    /// invisible.
    pub blocked_nodes: usize,
}

impl<S: NodeDrainSink, T: InstanceTerminator> Reconciler<S, T> {
    pub fn new(config: ReconcilerConfig, sink: S, terminator: T) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            terminator: Arc::new(terminator),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            evacuations: Arc::new(Mutex::new(tokio::task::JoinSet::new())),
            restore_in_flight: Arc::new(Mutex::new(HashSet::new())),
            instance_lookup: None,
            warn_pacer: PerNodeWarnPacer::new(HELD_WARN_INTERVAL),
        }
    }

    /// Attach the EC2 cross-check used by the KubeNodeGone recovery for
    /// `draining` rows. Pass `None` (or skip the call — `new` defaults to
    /// `None`) on substrates without a real EC2 (`kube-delete` / `noop`),
    /// where a missing kube Node keeps the fail-safe parked behavior.
    pub fn with_instance_lookup(
        mut self,
        instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
    ) -> Self {
        self.instance_lookup = instance_lookup;
        self
    }

    /// Await all evacuation tasks currently in the JoinSet, bounded by
    /// `timeout`. Called from [`run`] on shutdown so a graceful exit
    /// gives in-flight evacuations a chance to land their post-suspend
    /// / post-restore DB UPDATE rather than getting runtime-aborted
    /// mid-step. Tasks still running after the timeout are dropped on
    /// runtime shutdown.
    pub async fn drain_evacuations(&self, timeout: Duration) {
        let started = Instant::now();
        let drain = async {
            loop {
                let next = self.evacuations.lock().await.join_next().await;
                if next.is_none() {
                    return;
                }
            }
        };
        match tokio::time::timeout(timeout, drain).await {
            Ok(()) => {
                tracing::info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "node_drain: all evacuation tasks drained on shutdown"
                );
            }
            Err(_) => {
                let still_in_flight: usize = {
                    let guard = self.in_flight.lock().await;
                    guard.values().map(HashSet::len).sum()
                };
                tracing::warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    still_in_flight,
                    "node_drain: evacuation tasks did not drain within timeout; \
                     sandboxes may be left in intermediate lifecycle states"
                );
            }
        }
    }

    pub async fn tick(&self) -> TickReport {
        let mut report = TickReport::default();

        // Reap completed evacuation tasks from prior ticks. Without this,
        // `JoinSet` retains every finished task's record for the lifetime
        // of the process (unbounded growth in a long-lived controller
        // draining many sandboxes), and the shutdown drain in [`run`]
        // would iterate over historical tasks instead of just the
        // current in-flight ones. `try_join_next` is non-blocking.
        {
            let mut evacuations = self.evacuations.lock().await;
            while evacuations.try_join_next().is_some() {}
        }

        // Emit fleet-wide state gauges every tick so a state that
        // momentarily has zero rows still re-emits zero rather than
        // dropping off dashboards.
        match self.sink.node_phase_counts().await {
            Ok(counts) => {
                // Emit one series per known `drain_phase` value, including
                // zero counts so dashboards don't drop tags. `NULL`
                // (non-draining nodes) is implicit; the orphaned-sandbox
                // sweeper and other reconcilers own observability for
                // those rows.
                for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
                    let count = counts.get(phase).copied().unwrap_or(0);
                    node_drain_metrics::nodes_by_state(phase, count);
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "node_drain: node_phase_counts query failed");
            }
        }

        match self.sink.list_draining_nodes().await {
            Ok(mut nodes) => {
                report.draining_nodes = nodes.len();
                // List, then escalate, then handle — all within this tick, so a
                // node that just passed the deadline is evacuated now rather
                // than idling one more tick. The classification is done here
                // rather than in the UPDATE's `WHERE`: `list_draining_nodes`
                // already resolved every row through
                // `NodeDrainMode::from_column_str`, so this is the only place
                // that decides what "gentle" means.
                if let Some(age) = self.config.gentle_escalate {
                    let candidates: Vec<NodeId> = nodes
                        .iter()
                        .filter(|node| !node.mode.evacuates_residents())
                        .map(|node| node.id)
                        .collect();
                    self.escalate_gentle_drains(&candidates, age, &mut nodes, &mut report)
                        .await;
                }
                // Prune pacer slots for nodes that left the draining set, after
                // escalation so it prunes against the set actually handled. The
                // pacer keys on `Uuid`, so project the ids rather than widening
                // its signature to a node type it has no other use for.
                let live: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
                self.warn_pacer.retain_live(&live);
                for node in nodes {
                    self.handle_draining_node(node, &mut report).await;
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "node_drain: list_draining_nodes failed");
            }
        }
        node_drain_metrics::gentle_nodes(report.gentle_nodes as u64);

        // Emit every tick, including zero, so an alert can evaluate
        // `kube_node_gone_parked{phase:draining} > 0` directly without the
        // series going stale once the parked rows finally converge. The
        // `drained` phase's series is emitted by `drained_node_terminator`.
        node_drain_metrics::kube_node_gone_parked(
            PHASE_DRAINING,
            report.kube_node_gone_parked as u64,
        );

        match self.sink.list_terminating_nodes().await {
            Ok(nodes) => {
                report.terminating_nodes = nodes.len();
                for node_id in nodes {
                    if self.handle_terminating_node(node_id).await {
                        report.terminated += 1;
                    } else {
                        report.terminate_failures += 1;
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "node_drain: list_terminating_nodes failed");
            }
        }

        // Cluster-wide cohort-restore sweep. It restores sandboxes suspended
        // mid-evacuation only after their published artifact has a ready
        // backup, regardless of whether the source node is still draining.
        // The periodic run catches controller restarts and lost notifications.
        report.spawned_restores = self.run_restore_sweep().await;

        report
    }

    /// List restore-pending evacuations cluster-wide and spawn bounded cohort
    /// restores for any not already in flight. Returns the number spawned.
    async fn run_restore_sweep(&self) -> usize {
        // Recover crash-stranded `suspending` evacuations first. An
        // identity-backed local publication advances to `suspended` so backup
        // can be enqueued; legacy nullable rows wait for their durable snapshot.
        // SQL also requires `active_node_id IS NULL`, proving source execution
        // retirement completed before either path advances.
        match self.sink.redrive_stranded_suspending_evacuations().await {
            Ok(advanced) if !advanced.is_empty() => {
                node_drain_metrics::redrive_stranded_suspended(advanced.len() as u64);
                tracing::info!(
                    advanced = advanced.len(),
                    "node_drain: re-drove crash-stranded suspending evacuations to suspended; \
                     backup or restore reconciliation will continue their evacuation"
                );
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "node_drain: redrive_stranded_suspending_evacuations failed; \
                     will retry on next sweep"
                );
            }
        }

        // With the backoff disabled, ignore any backoff windows already
        // written by failures before the disable — the operator's escape
        // hatch must restore retry-next-sweep behavior immediately, not
        // after stale windows drain.
        let honor_backoff_windows = self.config.restore_backoff_base.is_some();
        let pending = match self
            .sink
            .list_restore_pending_evacuations(honor_backoff_windows)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "node_drain: list_restore_pending_evacuations failed"
                );
                return 0;
            }
        };
        self.spawn_restores(&pending).await
    }

    /// Spawn cohort-restore tasks for `sandboxes`, bounded by the shared
    /// `max_per_cluster` cluster budget and deduped via
    /// [`Self::restore_in_flight`] so a slow restore spanning ticks (or a
    /// concurrent snapshot-register trigger) drives each sandbox exactly once.
    /// Tasks go into the shared `evacuations` `JoinSet` so they get the bounded
    /// shutdown drain. The restore is decoupled from any node's drain *gate*,
    /// but it shares the one `max_per_cluster` substrate-concurrency budget
    /// with the suspend phase (see the count read below) so total drain-driven
    /// pressure on the substrate can't exceed the operator's single knob.
    ///
    /// Slots are filled **in list order**: when `sandboxes` outnumbers the
    /// free budget, the front of the list wins and the tail waits for a later
    /// sweep — with the sink's restore-pending list ordered by attempt tiers
    /// and then recency, never-failed hot sandboxes restore first and serial
    /// failers queue behind every fresher tier.
    async fn spawn_restores(&self, sandboxes: &[(SandboxId, SandboxLifecycle)]) -> usize {
        if sandboxes.is_empty() {
            return 0;
        }
        // Count suspend-phase work against the same cluster budget. Read the
        // suspend in-flight count in a released scope so this never holds both
        // `in_flight` and `restore_in_flight` at once — there is no lock-order
        // coupling between the two phases and so no deadlock. A momentary stale
        // snapshot is fine: the cap is a best-effort substrate throttle, not a
        // hard invariant.
        let suspend_in_flight: usize = {
            let in_flight = self.in_flight.lock().await;
            in_flight.values().map(HashSet::len).sum()
        };
        let mut guard = self.restore_in_flight.lock().await;
        let slots = self
            .config
            .max_per_cluster
            .saturating_sub(suspend_in_flight + guard.len());
        if slots == 0 {
            return 0;
        }

        let mut spawned = 0;
        let mut evacuations = self.evacuations.lock().await;
        // Failed restores back off exponentially (base doubling up to cap) so
        // a deterministic failer is re-armed on a schedule instead of on every
        // tick. `None` disables the backoff: every failure retries on the next
        // sweep (the prior behavior). Both halves are `Copy`, captured per
        // task below.
        let restore_backoff = self
            .config
            .restore_backoff_base
            .map(|base| (base, self.config.restore_backoff_cap));
        for (sandbox_id, target_lifecycle) in sandboxes {
            if spawned == slots {
                break;
            }
            if !guard.insert(*sandbox_id) {
                // Already being restored (prior tick still running, or a
                // snapshot-register trigger beat the sweep to it).
                continue;
            }
            spawned += 1;

            let sink = self.sink.clone();
            let restore_in_flight = self.restore_in_flight.clone();
            let evacuate_timeout = self.config.evacuate_timeout;
            let placement = self.config.drain_evacuation_placement.clone();
            let sandbox = *sandbox_id;
            let target_lifecycle = *target_lifecycle;
            evacuations.spawn(async move {
                let started = Instant::now();
                let result = sink
                    .restore_evacuated_sandbox(
                        sandbox,
                        target_lifecycle,
                        evacuate_timeout,
                        placement,
                    )
                    .await;
                let target_lifecycle_tag = target_lifecycle.as_str();
                match &result {
                    // Only an actual restore counts toward the restore-latency
                    // histogram. A `Deferred` outcome (a user takeover cleared
                    // the marker mid-flight) restored nothing, and a
                    // `Converging` one hasn't finished restoring, so recording
                    // either would pollute the metric with non-restore samples.
                    Ok(RestoreOutcome::Restored) => {
                        node_drain_metrics::sandbox_restore_duration(started, target_lifecycle_tag);
                    }
                    Ok(RestoreOutcome::Deferred) => {}
                    // Converge-by-retry (see [`RestoreOutcome::Converging`]):
                    // expected for restores slower than one wait budget —
                    // info, not warn, and its own counter rather than
                    // `node_drain.failures`, so dashboards and alerts only
                    // see genuine failures. The phase tag names the readiness
                    // step the wait stalled in so a converging storm stays
                    // step-attributable.
                    Ok(RestoreOutcome::Converging { detail, wait_phase }) => {
                        let phase_tag = RestoreOutcome::converging_phase_tag(*wait_phase);
                        node_drain_metrics::restore_converging(phase_tag, target_lifecycle_tag);
                        tracing::info!(
                            sandbox_id = %sandbox,
                            phase = phase_tag,
                            target_lifecycle = target_lifecycle_tag,
                            detail = %detail,
                            "node_drain: cohort restore still converging on the node; \
                             will re-drive on next sweep"
                        );
                    }
                    Err(err) => {
                        node_drain_metrics::failure("cluster", err.phase(), target_lifecycle_tag);
                        tracing::warn!(
                            sandbox_id = %sandbox,
                            phase = err.phase(),
                            target_lifecycle = target_lifecycle_tag,
                            error = %err,
                            "node_drain: cohort restore failed"
                        );
                        // A failure (and only a failure — a `Deferred` user
                        // takeover restored nothing, a `Converging` wake is
                        // still making progress and must be re-driven by the
                        // very next sweep rather than delayed, and a success
                        // resets via the marker-clear trigger) schedules the
                        // next attempt on the exponential backoff, keeping the
                        // sandbox out of the sweep until the window passes.
                        if let Some((base, cap)) = restore_backoff {
                            match sink.schedule_restore_retry(sandbox, base, cap).await {
                                Ok(Some(attempts)) => {
                                    tracing::info!(
                                        sandbox_id = %sandbox,
                                        attempts,
                                        "node_drain: cohort restore retry backed off"
                                    );
                                }
                                Ok(None) => {
                                    // Marker cleared mid-attempt: a user
                                    // restore took the sandbox over, so the
                                    // sweep no longer owns it and there is
                                    // nothing to back off.
                                    tracing::info!(
                                        sandbox_id = %sandbox,
                                        "node_drain: skipping restore retry backoff; \
                                         evacuation marker cleared by a user takeover"
                                    );
                                }
                                Err(db_err) => {
                                    tracing::warn!(
                                        sandbox_id = %sandbox,
                                        error = %db_err,
                                        "node_drain: failed to record restore retry backoff; \
                                         the next sweep may retry immediately"
                                    );
                                }
                            }
                        }
                    }
                }
                restore_in_flight.lock().await.remove(&sandbox);
            });
        }

        spawned
    }

    /// Escalate the gentle drains in `candidates` that have passed `age`, and
    /// promote the escalated ones to `Forced` in `nodes` so this same tick
    /// evacuates them.
    ///
    /// Rewriting the in-memory listing is what makes the escalation take effect
    /// immediately instead of one tick later. It is safe because the DB write
    /// has already committed by the time we get the ids back — the listing and
    /// the column now agree, and any other reader (the user-API drain gates)
    /// sees the same `forced` on its next read.
    ///
    /// A failed escalation is logged and dropped: the nodes stay gentle for this
    /// tick and the next one retries. That is the fail-soft direction — the
    /// alternative, treating an unconfirmed escalation as forced, would evacuate
    /// sandboxes on the strength of a write that may not have landed.
    async fn escalate_gentle_drains(
        &self,
        candidates: &[NodeId],
        age: PositiveDuration,
        nodes: &mut [DrainingNode],
        report: &mut TickReport,
    ) {
        if candidates.is_empty() {
            return;
        }
        let escalated = match self
            .sink
            .escalate_stale_gentle_drains(candidates, age)
            .await
        {
            Ok(escalated) => escalated,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    candidates = candidates.len(),
                    "node_drain: escalate_stale_gentle_drains failed; the nodes stay gentle \
                     this tick and the next one retries"
                );
                return;
            }
        };
        report.gentle_escalations += escalated.len();
        for node_id in escalated {
            node_drain_metrics::gentle_escalation(&node_id.to_string());
            tracing::warn!(
                node_id = %node_id,
                age_secs = age.get().as_secs(),
                reason = "drain_gentle_escalated",
                "node_drain: gentle drain exceeded its escalation deadline; switched \
                 drain_mode to forced — resident sandboxes will now be evacuated and user \
                 calls against them will 409"
            );
            if let Some(node) = nodes.iter_mut().find(|node| node.id == node_id) {
                node.mode = NodeDrainMode::Forced;
            }
        }
    }

    /// Publish one per-node store query's latency and outcome, and warn — paced
    /// — when a query that still succeeded took long enough to be worth
    /// investigating.
    ///
    /// Neither scan scales with the node's *remaining* work — both scale with
    /// its accumulated `target_node_id` history, and that only grows.
    /// `target_node_id` is a placement diagnostic suspend never clears, so a
    /// long-lived node carries every sandbox ever suspended from it, and each
    /// of those rows costs a per-row index probe even when no work remains.
    /// Accumulated history can eventually exhaust the statement timeout and
    /// prevent reconciliation from converging.
    /// Two seconds is the reporting threshold, providing a warning before
    /// these queries hit the statement timeout.
    ///
    /// **Only the succeeding-but-slow case warns here.** A cancelled query
    /// already produces the caller's own failure WARN naming the same query, so
    /// a second line would double every tick of the outage without adding a
    /// reading. And the warn is paced through the shared `(node, reason)`
    /// pacer, keyed on `query` itself — already the `&'static str` the pacer
    /// wants, so there is no second string to keep in step with it and no way
    /// to pace one query inside another's window. A query that sits above the
    /// threshold does so on every tick, and
    /// at the 5 s default that is ~17k lines per node per day — the volume that
    /// pacer exists to keep out of this reconciler's log. The continuous signal
    /// is `node_drain.store_query_duration_ms`, which is emitted every call.
    fn observe_store_query<Row, Err>(
        &self,
        query: &'static str,
        node_id: NodeId,
        started: Instant,
        result: &Result<Row, Err>,
    ) {
        let outcome = if result.is_ok() { "ok" } else { "error" };
        node_drain_metrics::store_query_duration(&node_id.to_string(), query, outcome, started);
        // The metric above is unconditional — a cancellation is the reading that
        // matters most. The WARN below is not: a cancelled query is the caller's
        // to log, and it already does.
        if result.is_err() {
            return;
        }
        let elapsed = started.elapsed();
        if elapsed < SLOW_STORE_QUERY {
            return;
        }
        if self.warn_pacer.due(node_id, query) {
            tracing::warn!(
                node_id = %node_id,
                query = query,
                elapsed_ms = elapsed.as_millis() as u64,
                "node_drain: store query is approaching the statement timeout; its cost \
                 tracks accumulated state rather than this node's remaining work, so it \
                 will not improve as the drain converges. Further slow runs of this query \
                 on this node log at debug for the next hour; \
                 `node_drain.store_query_duration_ms` carries every call"
            );
        } else {
            tracing::debug!(
                node_id = %node_id,
                query = query,
                elapsed_ms = elapsed.as_millis() as u64,
                "node_drain: store query still slow (warn paced)"
            );
        }
    }

    /// Drives one `draining` node for one tick, updating `report` with the
    /// spawned-evacuation / drained-transition / KubeNodeGone-recovery counts.
    ///
    /// Everything except the evacuation spawn and the evacuatable listing runs
    /// in both drain modes, and deliberately so:
    ///
    /// - **Cordon** is what makes either mode a drain at all.
    /// - **KubeNodeGone recovery** applies because the node is gone; there is
    ///   nothing gentle to preserve about sandboxes whose VMs died with the
    ///   instance, and the give-ups are the only path by which such a node
    ///   converges.
    /// - **Local-snapshot promotion** applies because it is what lets a gentle
    ///   drain make progress without disturbing anyone: backing a pinned
    ///   local-only artifact with S3 both releases the node and redirects that
    ///   sandbox's next restore elsewhere.
    ///
    /// The **abandon-stuck-transient sweep** is the exception that is *not*
    /// shared: it is forced-only. It selects on age rather than liveness, so in
    /// gentle mode it could flip a merely-slow create or restore to `failed`,
    /// which is exactly the disruption the mode exists to avoid. See its call
    /// site for the trade.
    ///
    /// Only the evacuation is mode-gated. In gentle mode the node cordons and
    /// waits for residents to leave on their own (auto-suspend on inactivity,
    /// a user suspend, a delete), and reaches `drained` through the same
    /// zero-blockers gate.
    async fn handle_draining_node(&self, node: DrainingNode, report: &mut TickReport) {
        let DrainingNode { id: node_id, mode } = node;
        if !mode.evacuates_residents() {
            report.gentle_nodes += 1;
        }
        // Cordon convergence: admin `POST :drain` commits
        // `drain_phase='draining'` in Phase 1 and patches
        // `spec.unschedulable` in Phase 2 best-effort. If Phase 2
        // fails it persists `drain_last_error` and returns 502,
        // delegating recovery to this reconciler. Re-assert every
        // tick (idempotent on an already-cordoned Node). On failure,
        // record drain_last_error and bail for this node — advancing
        // to drained while the kube scheduler can still place new
        // pods is the exact bug we are preventing.
        //
        // A gone kube Node is a cordon *success* (nothing left to
        // schedule onto), but routes through the KubeNodeGone
        // assessment: with EC2 confirming the backing instance
        // terminated, the per-sandbox work below switches from
        // node-bound RPCs that can never succeed (suspend, backup
        // enqueue) to the guarded give-ups; in every other case
        // (`Park`) the node keeps its pre-recovery behavior.
        let gone_recovery = match self.ensure_cordon(node_id, PHASE_DRAINING).await {
            None => return,
            Some(CordonOutcome::Cordoned) => None,
            Some(CordonOutcome::KubeNodeGone) => {
                self.assess_gone_draining_node(node_id, report).await
            }
        };

        // KubeNodeGone bulk give-up, before the resident/evacuatable
        // listings so this tick already observes the post-flip set (and
        // spawns no futile evacuations for rows that just died with the
        // instance).
        if let Some(recovery) = &gone_recovery {
            match self
                .sink
                .fail_resident_sandboxes_on_gone_node(node_id, recovery.expected_uid.as_deref())
                .await
            {
                Ok(0) => {}
                Ok(failed) => {
                    node_drain_metrics::kube_node_gone_failed_sandboxes(
                        &node_id.to_string(),
                        failed,
                    );
                    report.kube_node_gone_failed_sandboxes += failed;
                    tracing::warn!(
                        node_id = %node_id,
                        failed,
                        reason = "drain_kube_node_gone",
                        "node_drain: kube Node gone and EC2 instance terminated; flipped \
                         resident sandboxes to lifecycle=failed (their VMs died with the \
                         instance) so drain can proceed"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: fail_resident_sandboxes_on_gone_node failed; \
                         will retry on next tick"
                    );
                }
            }

            // Terminalize the dead node's in-flight snapshot uploads. Their
            // worker died with the instance, so they can never finalize, yet
            // the drain blocker count's in-flight-upload term counts them
            // regardless of the owning sandbox's lifecycle — left alone they
            // wedge the drain until the stuck-snapshot reaper's age window
            // (or forever where that reaper is disabled). Same flip the
            // reaper applies, brought forward by the EC2-terminated verdict.
            match self
                .sink
                .fail_inflight_snapshots_on_gone_node(node_id, recovery.expected_uid.as_deref())
                .await
            {
                Ok(0) => {}
                Ok(snapshots) => {
                    tracing::warn!(
                        node_id = %node_id,
                        snapshots,
                        reason = "drain_kube_node_gone_upload_lost",
                        "node_drain: kube Node gone and EC2 instance terminated; \
                         terminalized in-flight snapshot uploads sourced from the dead \
                         node so the drain blocker count can converge"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: fail_inflight_snapshots_on_gone_node failed; \
                         will retry on next tick"
                    );
                }
            }
        }

        // Abandon-stuck-transient sweep: a row wedged in any non-evacuable
        // resident transient lifecycle (creating/starting/pausing/resuming/
        // stopping, plus a *non-evacuation* suspending/restoring) counts as
        // resident — it isn't evacuatable, so it blocks `draining → drained`
        // indefinitely just like a paused sandbox would, except that unlike
        // a paused sandbox it never settles on its own. A row that's sat
        // past `stale_abandon` is one the start/wake path has effectively
        // given up on; flipping it to `failed` lets drain proceed and
        // matches the user-facing contract that a drained node may discard
        // work in flight. The drain's own evacuation *restore* is excluded by
        // the `evacuation_restore_target` marker (a wedged evacuation
        // *suspend* stays eligible — nothing else recovers it), so this never
        // aborts a live restore. Done before the resident scan so
        // the same tick sees the post-abandon set. `stale_abandon = None`
        // disables the policy entirely — the sink call doesn't happen.
        //
        // **Forced mode only.** The sweep selects on age, not on liveness:
        // nothing here proves the transition is actually wedged rather than
        // slow. A cold create pulling a large template, or a restore of a large
        // memfile, legitimately runs past the 600s default — the substrate
        // clamps each blocking wake wait to `NODE_RPC_BLOCKING_BUDGET` (~135s)
        // and relies on sweep re-drives, so a live restore routinely spans
        // several budgets. Under a forced drain, discarding it is the deal the
        // caller already accepted: every resident is being suspended and
        // migrated anyway. Under a gentle drain it would break the one promise
        // the mode makes, which is to cordon and wait without touching
        // residents.
        //
        // The cost is that a genuinely wedged transient now parks a gentle node
        // in `draining` instead of being cleared. That is the right trade — a
        // slow drain beats a destroyed user transition — and the park is not
        // silent: it is counted by `node_drain.drain_blockers` and it converges
        // if the operator sets the escalation deadline, which rewrites the mode
        // to `forced` and re-enables this sweep along with the evacuation.
        if let Some(age) = self
            .config
            .stale_abandon
            .filter(|_| mode.evacuates_residents())
        {
            match self
                .sink
                .abandon_stale_resident_sandboxes(node_id, age)
                .await
            {
                Ok(0) => {}
                Ok(abandoned) => {
                    node_drain_metrics::stale_abandoned(&node_id.to_string(), abandoned);
                    tracing::warn!(
                        node_id = %node_id,
                        abandoned,
                        age_secs = age.get().as_secs(),
                        reason = "drain_abandon_stale",
                        "node_drain: abandoned sandboxes wedged in a non-evacuable transient \
                         lifecycle on draining node; flipped to lifecycle=failed so drain can \
                         proceed"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: abandon_stale_resident_sandboxes failed; \
                         will retry on next tick"
                    );
                }
            }
        }

        // Two-set model:
        //   `resident` blocks `draining -> drained` (everything still
        //   tied to the node).
        //   `evacuatable` is the subset the pipeline can suspend +
        //   restore while preserving user-visible semantics (`started`
        //   stays `started`; `paused` is restored with
        //   `target_state = Paused` so the VM never resumes and the
        //   row flips to `paused` in one shot). Sandboxes that are
        //   resident but not evacuatable — `pausing`, `stopping`,
        //   `creating`, the other transient states — hold the node in
        //   `draining` until they settle or the abandon-stuck-transient
        //   sweep above clears them, rather than getting silently
        //   resumed/aborted by the post-restore lifecycle write.
        let resident = match self.sink.count_resident_sandboxes_on_node(node_id).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "node_drain: count_resident_sandboxes_on_node failed"
                );
                return;
            }
        };
        // Gentle mode never evacuates, so it never needs the evacuatable set.
        // Skipping the query is not just an optimization: the query enumerates
        // every `started`/`paused` row whose `active_node_id` points here, a
        // set that on a large cluster includes stale rows whose VMs are long
        // gone, and a gentle drain has no use for that list at all.
        let evacuatable = if mode.evacuates_residents() {
            match self.sink.list_evacuatable_sandboxes_on_node(node_id).await {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: list_evacuatable_sandboxes_on_node failed"
                    );
                    return;
                }
            }
        } else {
            Vec::new()
        };

        node_drain_metrics::sandboxes_remaining(&node_id.to_string(), resident.occupying);
        // Emitted every scan, zero included, so the discount is legible next to
        // the blocking count rather than inferred from its absence.
        node_drain_metrics::residents_discounted(&node_id.to_string(), resident.discounted);

        let in_flight_for_node = {
            let guard = self.in_flight.lock().await;
            guard.get(&node_id).map(HashSet::len).unwrap_or(0)
        };
        node_drain_metrics::sandboxes_in_flight(&node_id.to_string(), in_flight_for_node as u64);

        // Drive evacuation here, ahead of the node-release path, rather than at
        // the end of the tick. Everything between this point and the end — the
        // pinned local-only enumeration, the promotion RPCs, the drain-blocker
        // count, the termination-blocking scan, the `draining → drained`
        // transition — is about *releasing* the node, and every one of those
        // steps returns from the tick when its query fails. Ordering the
        // evacuation drive behind them let a single failing read stop the only
        // step that reduces the node's resident set. A repeated enumeration
        // timeout can therefore leave all live residents unevacuated. The two
        // paths are independent — evacuation acts on `started`/`paused` rows,
        // the release path on `suspended`/transient ones — so either side must
        // not silence the other.
        //
        // Moving it ahead of the release path cannot let a tick both evacuate and
        // complete the drain, because `spawned_this_tick` is a term in
        // `otherwise_ready` below — see there for why the resident count alone
        // is not enough to establish that. The suspends this spawns are
        // detached, so one that lands mid-tick can put its sandbox into the
        // pinned list this same tick; that is harmless — the row is then
        // promotable, which is what the list is for — and it is the only
        // interaction between the two halves.
        //
        // Never spawn node-bound evacuations for a gone node. With the EC2
        // instance confirmed terminated the suspend RPC has no node-api pod
        // to reach, and — the sharper hazard — if the bulk fail's identity
        // guards refused (a same-name replacement registered, or an admin
        // undrain landed mid-tick), the listed `started`/`paused` rows may
        // belong to the replacement: the evacuate claim guards only on
        // `active_node_id` + lifecycle, and the replacement reuses the same
        // `nodes.id`, so an unguarded spawn would suspend fresh sandboxes
        // off a healthy node. The next tick re-evaluates against whatever
        // state is then current.
        //
        // Gentle mode never reaches the spawn: `evacuatable` is empty by
        // construction above, and the mode check here states the intent rather
        // than relying on that emptiness.
        let spawned_this_tick = if gone_recovery.is_none() && mode.evacuates_residents() {
            self.spawn_evacuations(node_id, &evacuatable).await
        } else {
            0
        };
        report.spawned_evacuations += spawned_this_tick;

        // Local-only suspended sandboxes still pinned to this node. New rows
        // remain here until their exact current artifact has a ready backup;
        // nullable rollout-skew rows retain legacy snapshot portability.
        // Gentle drains exclude legacy (no-artifact-id) rows from **live-node
        // promotion**: their only backup route reads the live snapshot directory,
        // which is safe only while the drain gate blocks wakes — and gentle
        // deliberately doesn't. Those rows stay counted as blockers, so such a
        // node parks instead of publishing a snapshot a concurrent wake could
        // have torn.
        //
        // The exclusion is scoped to that path only. On a node whose kube Node is
        // gone and whose EC2 instance is confirmed terminated there are no live
        // bytes left to protect and no wake can reach it, and the give-ups below
        // (adopt a ready remote snapshot, else mark lost) are the *only* way such
        // a node ever converges. Excluding legacy rows there would wedge the
        // drain forever while protecting nothing.
        let exclude_legacy_rows = gone_recovery.is_none() && !mode.evacuates_residents();
        let pinned_started = Instant::now();
        let pinned_result = self
            .sink
            .list_pinned_local_only_sandboxes_on_node(node_id, exclude_legacy_rows)
            .await;
        self.observe_store_query(
            "list_pinned_local_only_sandboxes_on_node",
            node_id,
            pinned_started,
            &pinned_result,
        );
        let mut local_only = match pinned_result {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "node_drain: list_pinned_local_only_sandboxes_on_node failed"
                );
                return;
            }
        };
        // The store asks for `cap + 1` rows so this comparison is unambiguous: a
        // `cap`-sized page is an exact count, and only a `cap + 1`-sized one
        // means there is more behind it.
        //
        // The reported length and the consumed list are deliberately *not* the
        // same set. This gauge and the gentle-drain debug line below use the
        // untrimmed `pinned_total`, so `cap + 1` is observable and an exact
        // cap-sized backlog is distinguishable from an overflowing one;
        // publishing the trimmed length would collapse those two states into
        // one point, recoverable only from the paced WARN, which is silent for
        // up to an hour at a time by design. Everything that *acts* consumes
        // the trimmed list.
        //
        // Not a universal rule: `give_up_local_only_on_gone_node` logs the
        // length of the slice it is handed, which is the trimmed one. That is
        // right for it — it is reporting what it is about to work through, not
        // the size of the backlog — so on a truncated node its line reads the
        // cap while this gauge reads one above it.
        let pinned_total = local_only.len();
        let truncated = pinned_total > store::PINNED_LOCAL_ONLY_ENUMERATION_CAP.get();
        node_drain_metrics::pinned_suspended(&node_id.to_string(), pinned_total as u64);
        local_only.truncate(store::PINNED_LOCAL_ONLY_ENUMERATION_CAP.get());
        // A truncated page means "at least this many", not "this many". Say so
        // rather than leaving the gauge pinned at the cap and readable as a
        // converged count. Paced: a node deep enough in backlog to hit the cap
        // hits it on every tick.
        if truncated && self.warn_pacer.due(node_id, "pinned_local_only_truncated") {
            tracing::warn!(
                node_id = %node_id,
                cap = store::PINNED_LOCAL_ONLY_ENUMERATION_CAP.get(),
                "node_drain: pinned local-only enumeration filled its cap, so \
                 `node_drain.pinned_suspended` is a floor and not the true backlog. \
                 The page is unordered, so a later tick may return the same rows \
                 rather than the rest; what converges is that a promoted sandbox \
                 becomes durable and leaves the set for good. A backlog that stops \
                 falling here alongside repeated promotion failures is a head that is \
                 not clearing, not a queue being worked through"
            );
        }
        if let Some(recovery) = &gone_recovery {
            // The backup enqueue can never succeed on a node whose EC2
            // instance is confirmed terminated — there is no node-api pod to
            // receive the RPC, and the local artifact bytes died with the
            // instance — so skip the promote RPCs in favor of the guarded
            // adopt / mark-lost give-ups.
            self.give_up_local_only_on_gone_node(node_id, recovery, &local_only, report)
                .await;
        } else {
            self.promote_local_only_sandboxes(node_id, &local_only)
                .await;
        }

        // The cohort restore of evacuated sandboxes is NOT driven here — it
        // is decoupled from this node's drain and runs as a cluster-wide
        // sweep (`run_restore_sweep`) plus readiness notification, so it
        // completes even after this node terminates. This node finishes
        // draining once current artifacts have matching ready backups; restore
        // onto the target cohort happens independently.

        // The drain-complete gate uses the broader "any blocker" count
        // rather than just the promotable list. `suspending`/`restoring`
        // sandboxes are not promotable (no snapshot exists yet) but
        // still block: terminating a node mid-suspend or mid-restore
        // would strand the local-disk artifacts. The count routes
        // through the shared `fresh_ready_snapshot_exists` predicate,
        // so it agrees atomically with
        // `drained_node_terminator::count_local_snapshots` and the
        // admin `:terminate` guard — a portable suspended sandbox
        // advances `draining → drained → terminating` uniformly
        // without wedging at the terminator gate.
        let blocking_started = Instant::now();
        let blocking_result = self
            .sink
            .count_drain_blocking_sandboxes_on_node(node_id)
            .await;
        self.observe_store_query(
            "count_drain_blocking_sandboxes_on_node",
            node_id,
            blocking_started,
            &blocking_result,
        );
        let blocking_count = match blocking_result {
            Ok(count) => count,
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "node_drain: count_drain_blocking_sandboxes_on_node failed"
                );
                return;
            }
        };

        // Publish the gate's third input. `sandboxes_remaining` and
        // `pinned_suspended` between them do not cover it: the gate counts rows
        // that are neither a resident lifecycle nor a pinned local-only row, so
        // a node can be held with both of those gauges at zero. Emitted every
        // tick including zero so an alert can read it directly.
        node_drain_metrics::drain_blockers(
            &node_id.to_string(),
            blocking_count.max(0).cast_unsigned(),
        );
        if blocking_count > 0 {
            report.blocked_nodes += 1;
        }
        // The terminator's *other* guard, which `blocking_count` does not speak
        // for. See `NodeDrainSink::count_termination_blocking_residents_on_node`
        // for why a node this guard would refuse must not reach `drained`.
        //
        // Scoped to the ticks where the answer can change a decision: both
        // consumers below require the other three terms to be zero, so on a tick
        // where the drain is still working the scan would cost a round trip and
        // a second pass over the node's `sandboxes` rows — thousands of them, on
        // the very node where that is most expensive — to move a gauge nobody
        // reads then. Zero when skipped is also the honest reading of the
        // series: something else is holding the node.
        // `spawned_this_tick` is the fourth term because the other three are all
        // reads taken *before* the spawn, and the first two can disagree with
        // each other. `resident.occupying` and `evacuatable` are separate round
        // trips through the same `occupies_node_predicate`, which under
        // `NodeCensusAware` is `NOT node_proves_sandbox_absent(..)` — a function
        // of the node's latest census report, so it is time-varying. A sandbox
        // the first read discounted as census-absent can be named by a census
        // that lands between the two reads and come back in `evacuatable`; the
        // spawn then registers it in `self.in_flight`, which
        // `in_flight_for_node` was snapshotted too early to see. Without this
        // term the gate could commit `drained` on the very tick that launched an
        // evacuation off the node. `GuardScope::Resident` in `set_drain_phase`
        // catches the common case atomically, but not the residue where the
        // spawned suspend clears `active_node_id` before the write lands — that
        // node reaches `drained` with an upload still in flight from it, which
        // the terminator then refuses on every tick.
        let otherwise_ready = resident.occupying == 0
            && in_flight_for_node + spawned_this_tick == 0
            && blocking_count == 0;
        let termination_blocking = if otherwise_ready {
            match self
                .sink
                .count_termination_blocking_residents_on_node(node_id)
                .await
            {
                Ok(count) => count,
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: count_termination_blocking_residents_on_node failed"
                    );
                    return;
                }
            }
        } else {
            TerminationBlockingResidents::default()
        };
        node_drain_metrics::termination_blocking_residents(
            &node_id.to_string(),
            termination_blocking.total(),
        );

        // Empty + nothing in flight + no blockers = drained. The UPDATE re-checks
        // the residency half of this atomically (see `set_drain_phase_query`), so
        // these reads are the cheap pre-check and the statement is authoritative.
        //
        // The fourth term is behind [`ReconcilerConfig::termination_blocking_gate`]
        // — see that field for why it ships off. The *scan* and the *gauge* above
        // are not gated: every environment measures how often this would bite
        // before it does. With the flag off this is the pre-term behaviour, so
        // such a node reaches `drained` and parks at the terminator exactly as it
        // did before — the difference is that the gauge now says so.
        let hold_for_terminator =
            self.config.termination_blocking_gate && !termination_blocking.is_empty();
        if otherwise_ready && !hold_for_terminator {
            match self
                .sink
                .set_drain_phase(node_id, PHASE_DRAINING, PHASE_DRAINED, None)
                .await
            {
                Ok(true) => {
                    node_drain_metrics::drained(PHASE_DRAINING, PHASE_DRAINED, mode);
                    tracing::info!(node_id = %node_id, "node_drain: drained");
                    report.drained_transitions += 1;
                    return;
                }
                Ok(false) => {
                    // Either the row left `draining` (admin undrain, or a
                    // replacement instance UID reset by the inventory upsert),
                    // or the UPDATE's own residency guard refused it because a
                    // sandbox attached between the pre-check above and the
                    // statement. Both mean "don't stomp the fresh state"; the
                    // next tick re-evaluates against whatever's current.
                    tracing::info!(
                        node_id = %node_id,
                        "node_drain: skipping drained transition; node or sandbox \
                         state changed under us"
                    );
                    return;
                }
                Err(err) if store::is_lock_timeout(&err) => {
                    // The bounded lock wait fired, not a database problem: some
                    // other transaction holds this node's row. Debug, not warn —
                    // a node whose row is held by a long-lived writer would
                    // otherwise emit one WARN per tick, in the same channel the
                    // pacer above exists to keep readable, and indistinguishable
                    // from a real outage. The next tick re-asks.
                    tracing::debug!(
                        node_id = %node_id,
                        timeout_ms = store::DRAIN_COMPLETION_LOCK_TIMEOUT.as_millis() as u64,
                        "node_drain: drained transition deferred; nodes row contended"
                    );
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: set_drain_phase(drained) failed"
                    );
                    return;
                }
            }
        }

        // Every arm of the gate above returns, so reaching here means it was not
        // entered — either `otherwise_ready` is false, or the fourth term held the
        // node. `hold_for_terminator` establishes the second case, and it is
        // false when the flag is off, so an ungated environment never emits this
        // WARN for a node it did not actually hold.
        //
        // The one hold this does *not* report is the atomic guard firing —
        // `Ok(false)` returns above with only the ambiguous info line. That path
        // means a sandbox attached between the pre-check and the write, which is
        // rare by construction and self-correcting on the next tick, but it is
        // the least-instrumented outcome here.
        if hold_for_terminator {
            // The hold no other signal explains: the resident warning below
            // counts a different set and stays silent, and the evacuation
            // pipeline has no work for these rows. There is deliberately no
            // give-up — the stale-abandon sweep's give-up is
            // `lifecycle = 'failed'`, which would destroy the quarantine an
            // investigation is holding — so the node stays here until an
            // operator or quarantine resolution clears the sandbox. Hence the
            // pacing; the `lifecycles` field names the actual blockers, so the
            // message must not restate the set (that is what drifted last time).
            if self.warn_pacer.due(node_id, "termination_blocking_hold") {
                tracing::warn!(
                    node_id = %node_id,
                    termination_blocking = termination_blocking.total(),
                    lifecycles = ?termination_blocking.by_lifecycle,
                    "node_drain: node is held in `draining` only by sandboxes the \
                     drained-node terminator counts as resident; advancing it \
                     would park it in `drained`, where the terminator refuses it \
                     on every tick and this reconciler no longer looks at it. \
                     Clearing the listed sandboxes releases the node; further \
                     holds for this node log at debug for the next hour"
                );
            } else {
                // Debug, not info: the gauge above carries this every scan, so
                // an info line per tick would be ~17k lines/day/node of pure
                // duplicate — the volume the pacing exists to avoid.
                tracing::debug!(
                    node_id = %node_id,
                    termination_blocking = termination_blocking.total(),
                    lifecycles = ?termination_blocking.by_lifecycle,
                    "node_drain: node still held in `draining` by sandboxes the \
                     drained-node terminator counts as resident (warn paced)"
                );
            }
        }

        if !mode.evacuates_residents() && (resident.occupying > 0 || blocking_count > 0) {
            // The steady state of a gentle drain: residents are still there and
            // nothing is being done to them. Deliberately **not** logged per
            // tick — a gentle drain is expected to last hours or days, so one
            // line per node per 5s tick is ~17k lines/node/day of "still
            // waiting" that would bury this reconciler's real warnings. The
            // continuous signal is carried by metrics instead:
            // `node_drain.gentle_nodes` (how many are parked) and the per-node
            // `node_drain.sandboxes_remaining` (how far from converging).
            // `debug` keeps the per-node detail available when an operator is
            // actively debugging one drain.
            tracing::debug!(
                node_id = %node_id,
                resident = resident.occupying,
                discounted = resident.discounted,
                blockers = blocking_count,
                pinned = pinned_total,
                escalate_secs = self.config.gentle_escalate.map(|age| age.get().as_secs()),
                "node_drain: gentle drain waiting for resident sandboxes to leave on their \
                 own (auto-suspend on inactivity, user suspend, or delete); no evacuation \
                 will be attempted"
            );
        } else if evacuatable.is_empty() && resident.occupying > 0 && in_flight_for_node == 0 {
            // Resident-but-not-evacuatable sandboxes. Surface a single
            // warning per tick so operators know why the drain is stuck
            // instead of seeing silence. Enumerates the full set (see
            // [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`]) so operators
            // hitting this during a rollout see every lifecycle that can
            // block a drain. Most entries are transient and settle on
            // subsequent ticks (`pausing` → `paused`, `resuming` →
            // `started`, etc., both of which the pipeline then
            // evacuates). The wedged cases — a `creating`/`starting` that
            // never finished, or a non-evacuation `restoring`/`suspending`
            // (a user wake/suspend) hung mid-RPC — never settle on their
            // own; the abandon-stuck-transient sweep above clears them
            // automatically once `stale_abandon` has elapsed.
            let lifecycles: Vec<String> = NON_EVACUATABLE_RESIDENT_LIFECYCLES
                .iter()
                .map(SandboxLifecycle::to_string)
                .collect();
            tracing::warn!(
                node_id = %node_id,
                resident = resident.occupying,
                discounted = resident.discounted,
                lifecycles = ?lifecycles,
                "node_drain: node has resident sandboxes in non-evacuatable lifecycles; \
                 drain is blocked until they settle into `started`/`paused` (then the \
                 pipeline evacuates them) or the abandon-stuck-transient sweep flips a \
                 wedged row to `failed`"
            );
        }
    }

    /// Enqueue backups for up to one tick's worth of pinned local-only
    /// suspended sandboxes through their source node's node-api, applying
    /// the per-error give-up policies (artifact-missing -> mark lost,
    /// restoring-on-node -> adopt remote snapshot). Extracted from
    /// [`Self::handle_draining_node`] so the KubeNodeGone recovery can
    /// replace it wholesale with the no-RPC give-up path
    /// ([`Self::give_up_local_only_on_gone_node`]).
    async fn promote_local_only_sandboxes(&self, node_id: NodeId, local_only: &[SandboxId]) {
        // Per-promotion ceiling AND bounded concurrency so a slow or
        // wedged node-api on one node can't stall the reconciler's
        // tick. The substrate's backup call tolerates up to 30 s by
        // default; tighter here, and parallelized across
        // sandboxes on this node so the per-node worst-case latency
        // is bounded by one timeout window (not N x timeout). The
        // outer tick still processes draining nodes sequentially in
        // a for-loop, so with K draining nodes carrying backlog the
        // whole-tick worst case is K x this per-node window; sized
        // to keep K x 5 s under the tick interval for realistic K.
        // Timed-out promotions are reported as `failed`; the next
        // tick retries.
        const PROMOTION_CALL_TIMEOUT: Duration = Duration::from_secs(5);
        const PROMOTION_CONCURRENCY: usize = 8;
        // Cap per-tick work so a node with a large backlog can't
        // monopolize the reconciler. The remainder stays in the
        // pinned list and gets picked up on subsequent ticks. With
        // 5 s timeout x 8-wide x 1 batch = <= 5 s spent on this node
        // per tick, the tick interval (default 5 s) stays meaningful
        // for other draining/terminating nodes in the same loop.
        const PROMOTION_MAX_PER_TICK: usize = PROMOTION_CONCURRENCY;
        let node_label = node_id.to_string();
        let sink_ref = self.sink.as_ref();
        type PromotionOutcome = Result<Result<(), PromoteError>, tokio::time::error::Elapsed>;
        use futures_util::stream::{self, StreamExt};
        let outcomes: Vec<(SandboxId, PromotionOutcome)> =
            stream::iter(local_only.iter().copied().take(PROMOTION_MAX_PER_TICK))
                .map(|sandbox_id| async move {
                    let fut = sink_ref.promote_local_snapshot(sandbox_id, node_id);
                    let outcome = tokio::time::timeout(PROMOTION_CALL_TIMEOUT, fut).await;
                    (sandbox_id, outcome)
                })
                .buffer_unordered(PROMOTION_CONCURRENCY)
                .collect()
                .await;
        for (sandbox_id, outcome) in outcomes {
            match outcome {
                Ok(Ok(())) => {
                    node_drain_metrics::local_snapshot_promotion(&node_label, "backup_enqueued");
                    tracing::info!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        "node_drain: queued backup for local suspended artifact"
                    );
                }
                Ok(Err(PromoteError::ArtifactMissing(detail))) => {
                    // Terminal: no recoverable copy of the sandbox state
                    // exists anywhere. Mark the row `lifecycle =
                    // 'failed'` so the sandbox drops out of the
                    // local-only counter and drain proceeds. The
                    // customer's next wake/restore call gets a clean
                    // lifecycle error rather than a confusing
                    // snapshot-not-found.
                    node_drain_metrics::local_snapshot_promotion(
                        &node_label,
                        "failed_artifact_missing",
                    );
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        detail = %detail,
                        "node_drain: local artifact gone; marking sandbox as failed \
                         (no recoverable copy exists, drain proceeds)"
                    );
                    match self.sink.mark_sandbox_lost(sandbox_id, node_id).await {
                        Ok(0) => {
                            tracing::info!(
                                node_id = %node_id,
                                sandbox_id = %sandbox_id,
                                "node_drain: sandbox drifted before mark-failed; skipping"
                            );
                        }
                        Ok(_) => {}
                        Err(db_err) => {
                            tracing::warn!(
                                node_id = %node_id,
                                sandbox_id = %sandbox_id,
                                error = %db_err,
                                "node_drain: failed to mark sandbox as failed after artifact loss; \
                                 will retry on next tick"
                            );
                        }
                    }
                }
                Ok(Err(PromoteError::SandboxRestoringOnNode(detail))) => {
                    // Current-instance backup was permanently refused because the
                    // sandbox is wedged in `restoring` on the node while the DB
                    // reads `suspended`. Retrying it every tick wedges the drain
                    // forever. If the give-up is enabled, hand the sandbox to
                    // the adopt-remote-snapshot path: after the settle window,
                    // and only if the newest ready remote snapshot already
                    // exists, it makes the row durable from that
                    // snapshot so the node can finish draining (the sandbox
                    // stays recoverable). `None` keeps the old retry-forever
                    // behavior.
                    // `gave_up_remote_snapshot` is emitted only after a row was
                    // actually made durable, so the counter reflects give-ups
                    // (not detections) and a retried refusal can't inflate it;
                    // every other path (disabled, still settling, no ready
                    // snapshot, in-flight upload, drifted, or a DB error) leaves
                    // the sandbox blocking and is counted as a retry.
                    let outcome = match self.config.reupload_giveup_settle {
                        None => {
                            tracing::warn!(
                                node_id = %node_id,
                                sandbox_id = %sandbox_id,
                                detail = %detail,
                                "node_drain: reupload refused (sandbox restoring on node); \
                                 give-up disabled, will retry on next tick"
                            );
                            "failed"
                        }
                        Some(settle) => {
                            match self
                                .sink
                                .make_sandbox_durable_from_remote_snapshot(
                                    sandbox_id, node_id, settle,
                                )
                                .await
                            {
                                Ok(0) => {
                                    tracing::info!(
                                        node_id = %node_id,
                                        sandbox_id = %sandbox_id,
                                        detail = %detail,
                                        "node_drain: reupload refused (sandbox restoring on \
                                         node); awaiting settle window / newest ready \
                                         remote snapshot"
                                    );
                                    "restoring_retry"
                                }
                                Ok(_) => {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        sandbox_id = %sandbox_id,
                                        detail = %detail,
                                        "node_drain: reupload permanently refused (sandbox \
                                         restoring on node); adopted newest ready remote \
                                         snapshot and abandoned local promotion so drain proceeds"
                                    );
                                    "gave_up_remote_snapshot"
                                }
                                Err(db_err) => {
                                    tracing::warn!(
                                        node_id = %node_id,
                                        sandbox_id = %sandbox_id,
                                        error = %db_err,
                                        "node_drain: failed to adopt remote snapshot for \
                                         restoring-on-node sandbox; will retry on next tick"
                                    );
                                    "restoring_retry"
                                }
                            }
                        }
                    };
                    node_drain_metrics::local_snapshot_promotion(&node_label, outcome);
                }
                Ok(Err(err)) => {
                    node_drain_metrics::local_snapshot_promotion(&node_label, "failed");
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "node_drain: local artifact backup enqueue failed; will retry on next tick"
                    );
                }
                Err(_) => {
                    node_drain_metrics::local_snapshot_promotion(&node_label, "failed");
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        timeout_secs = PROMOTION_CALL_TIMEOUT.as_secs(),
                        "node_drain: local artifact backup enqueue timed out; will retry on next tick"
                    );
                }
            }
        }
    }

    /// KubeNodeGone give-up for the pinned local-only suspended sandboxes of
    /// a draining node whose kube Node is gone and whose EC2 instance is
    /// confirmed terminated. The backup enqueue can never succeed — there is
    /// no node-api pod to receive it, and the local artifact bytes died with
    /// the instance — so instead of retrying `promote_local_snapshot` every
    /// tick forever ("no ready node-api pod for role worker", every tick,
    /// indefinitely), each sandbox gets one of:
    ///
    /// 1. **Adopt** — if the newest ready remote snapshot already exists and
    ///    is from the current suspend cycle, make the row durable from it
    ///    (the gone-node variant of the reupload give-up's CAS; the sandbox
    ///    stays `suspended` and recoverable).
    /// 2. **Mark lost** — if no ready remote snapshot exists at all and no
    ///    upload is in flight, flip the row to `failed` (terminal; the data
    ///    was already lost when the instance terminated).
    /// 3. **Park** — neither guard fired (an in-flight `creating` upload row
    ///    the stuck-snapshot reaper hasn't terminalized yet, a ready-but-
    ///    stale snapshot awaiting the adopt CAS's settle window, or a
    ///    drift); the row keeps blocking and is re-evaluated next tick.
    ///
    /// Both writes carry `recovery.expected_uid` plus the `drain_phase =
    /// 'draining'` guard (the same identity defense as the bulk resident
    /// fail), so a same-name kube Node replacement that registered after the
    /// EC2 cross-check refuses them rather than having its fresh sandboxes
    /// adopted from old snapshots or terminally failed.
    ///
    /// Gated on the same `reupload_giveup_settle` knob as the reupload
    /// give-up: `None` disables every automatic give-up, so operators who
    /// opted out keep park-forever semantics. The mark-lost arm is withheld
    /// together with the adopt arm — running it alone could terminally fail
    /// a row whose ready-but-placement-stale snapshot the adopt CAS would
    /// have recovered.
    async fn give_up_local_only_on_gone_node(
        &self,
        node_id: NodeId,
        recovery: &GoneNodeRecovery,
        local_only: &[SandboxId],
        report: &mut TickReport,
    ) {
        if local_only.is_empty() {
            return;
        }
        let Some(settle) = self.config.reupload_giveup_settle else {
            tracing::warn!(
                node_id = %node_id,
                pinned = local_only.len(),
                "node_drain: kube Node gone and EC2 instance terminated, but the \
                 give-up is disabled (the reupload give-up settle window=0); \
                 pinned local-only sandboxes keep blocking the drain"
            );
            return;
        };
        // Pure SQL per sandbox (no node RPC can exist for a gone node), so a
        // wider per-tick cap than the RPC-bound promote path is safe; still
        // bounded so one dead node with a deep backlog can't monopolize the
        // tick for other draining nodes.
        const GONE_NODE_GIVEUP_MAX_PER_TICK: usize = 64;
        let node_label = node_id.to_string();
        for &sandbox_id in local_only.iter().take(GONE_NODE_GIVEUP_MAX_PER_TICK) {
            // Adopt before mark-lost: prefer recovery over declared loss.
            // The mark-lost guard refuses while any ready snapshot exists,
            // so this chain can never terminally fail a row the adopt CAS
            // would recover on a later tick.
            let outcome = match self
                .sink
                .make_sandbox_durable_on_gone_node(
                    sandbox_id,
                    node_id,
                    settle,
                    recovery.expected_uid.as_deref(),
                )
                .await
            {
                Ok(n) if n > 0 => {
                    report.kube_node_gone_adopted_sandboxes += 1;
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        "node_drain: node gone (EC2 instance terminated); adopted newest \
                         ready remote snapshot and abandoned local promotion so drain proceeds"
                    );
                    "node_gone_gave_up_remote_snapshot"
                }
                Ok(_) => match self
                    .sink
                    .mark_sandbox_lost_on_gone_node(
                        sandbox_id,
                        node_id,
                        recovery.expected_uid.as_deref(),
                    )
                    .await
                {
                    Ok(n) if n > 0 => {
                        report.kube_node_gone_lost_sandboxes += 1;
                        tracing::warn!(
                            node_id = %node_id,
                            sandbox_id = %sandbox_id,
                            "node_drain: node gone (EC2 instance terminated) and no ready \
                             remote snapshot exists; marked sandbox as failed (no \
                             recoverable copy exists, drain proceeds)"
                        );
                        "node_gone_lost"
                    }
                    Ok(_) => {
                        tracing::info!(
                            node_id = %node_id,
                            sandbox_id = %sandbox_id,
                            "node_drain: node gone; sandbox not yet eligible for adopt or \
                             mark-lost (in-flight upload row, settle window, or drift); \
                             will retry on next tick"
                        );
                        "node_gone_retry"
                    }
                    Err(err) => {
                        tracing::warn!(
                            node_id = %node_id,
                            sandbox_id = %sandbox_id,
                            error = %err,
                            "node_drain: mark_sandbox_lost_on_gone_node failed; \
                             will retry on next tick"
                        );
                        "node_gone_retry"
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "node_drain: adopt remote snapshot for gone-node sandbox failed; \
                         will retry on next tick"
                    );
                    "node_gone_retry"
                }
            };
            node_drain_metrics::local_snapshot_promotion(&node_label, outcome);
        }
    }

    /// Reserve up to `min(per-node, per-cluster)` in-flight slots on
    /// `node_id` and spawn `make_task` for each newly-claimed sandbox into
    /// the shared `evacuations` `JoinSet`, returning the number spawned.
    ///
    /// Backs [`Self::spawn_evacuations`] (the suspend phase): the per-node /
    /// per-cluster cap math, the already-in-flight suppression guard, and the
    /// per-node in-flight insert/cleanup bookkeeping live here. Routing
    /// through the `evacuations` `JoinSet` gives the bounded shutdown drain in
    /// [`run`] — a task past its first DB CAS lands its UPDATE rather than
    /// getting runtime-aborted mid-step. The slot is released when
    /// `make_task`'s future completes. (The cohort restore uses the separate
    /// node-agnostic [`Self::spawn_restores`] path.)
    async fn spawn_bounded<F, Fut>(
        &self,
        node_id: NodeId,
        sandboxes: &[(SandboxId, SandboxLifecycle)],
        make_task: F,
    ) -> usize
    where
        F: Fn(Arc<S>, SandboxId, SandboxLifecycle) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // Count restore-phase work against the same cluster budget so suspend +
        // restore together can't exceed the operator's single `max_per_cluster`
        // knob. Read the restore count in a released scope so this never holds
        // both `in_flight` and `restore_in_flight` at once (no lock-order
        // coupling, no deadlock); a momentary stale snapshot is fine for a
        // best-effort throttle.
        let restore_in_flight: usize = { self.restore_in_flight.lock().await.len() };
        let mut guard = self.in_flight.lock().await;

        let cluster_in_flight: usize =
            guard.values().map(HashSet::len).sum::<usize>() + restore_in_flight;
        let cluster_slots = self
            .config
            .max_per_cluster
            .saturating_sub(cluster_in_flight);
        if cluster_slots == 0 {
            return 0;
        }

        let entry = guard.entry(node_id).or_default();
        let node_slots = self.config.max_per_node.saturating_sub(entry.len());
        let slots = node_slots.min(cluster_slots);
        if slots == 0 {
            return 0;
        }

        let mut spawned = 0;
        let mut evacuations = self.evacuations.lock().await;
        for (sandbox_id, lifecycle) in sandboxes {
            if spawned == slots {
                break;
            }
            if !entry.insert(*sandbox_id) {
                // Already in flight on this node (a prior tick's task is
                // still running, or an earlier phase hasn't dropped its
                // slot). Drive each sandbox exactly once.
                continue;
            }
            spawned += 1;

            let in_flight = self.in_flight.clone();
            let task = make_task(self.sink.clone(), *sandbox_id, *lifecycle);
            let sandbox = *sandbox_id;
            let node = node_id;
            evacuations.spawn(async move {
                task.await;
                let mut guard = in_flight.lock().await;
                if let Some(set) = guard.get_mut(&node) {
                    set.remove(&sandbox);
                    if set.is_empty() {
                        guard.remove(&node);
                    }
                }
            });
        }

        spawned
    }

    /// Spawn evacuation (suspend) tasks for sandboxes not already in flight,
    /// bounded by per-node and per-cluster caps. This is the suspend half of
    /// the evacuation; the cohort restore is driven separately by the
    /// cluster-wide restore sweep ([`Self::run_restore_sweep`]).
    async fn spawn_evacuations(
        &self,
        node_id: NodeId,
        sandboxes: &[(SandboxId, SandboxLifecycle)],
    ) -> usize {
        self.spawn_bounded(node_id, sandboxes, move |sink, sandbox, pre_lifecycle| async move {
            let started = Instant::now();
            let result = sink.evacuate_sandbox(sandbox, node_id, pre_lifecycle).await;
            let pre_lifecycle_tag = pre_lifecycle.as_str();
            match result {
                Ok(()) => {
                    // The `pre_lifecycle` tag separates paused
                    // lifecycle-preserving evacuations from started
                    // evacuations on dashboards. Restore latency is tracked
                    // separately by the restore sweep.
                    node_drain_metrics::sandbox_suspend_duration(
                        started,
                        &node_id.to_string(),
                        pre_lifecycle_tag,
                    );
                }
                // Terminal divergence: the sandbox is `failed` on the node but
                // `started`/`paused` in the DB, so suspend will never succeed.
                // Retrying every tick is exactly the wedge this guards against
                // — instead give up and flip the DB row to `failed` so the
                // node can finish draining. The claim was already reverted to
                // `pre_lifecycle` inside `evacuate_sandbox`, so the row is in a
                // clean evacuable state for the mark-failed CAS (and, if this
                // mark-failed call is lost to a crash, the next tick re-lists
                // and re-detects the divergence).
                Err(EvacuateError::SandboxFailedOnNode(detail)) => {
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox,
                        pre_lifecycle = pre_lifecycle_tag,
                        detail = %detail,
                        "node_drain: sandbox is failed on the node but \
                         started/paused in the DB; marking the DB row failed so \
                         drain can proceed (suspend can never succeed)"
                    );
                    match sink.mark_sandbox_failed_on_node(sandbox, node_id).await {
                        Ok(0) => {
                            // CAS missed: the sandbox drifted (woken elsewhere)
                            // between the failed suspend and this flip. Nothing
                            // was flipped, so the next tick re-evaluates — don't
                            // count it as a flip.
                            tracing::info!(
                                node_id = %node_id,
                                sandbox_id = %sandbox,
                                "node_drain: sandbox drifted before mark-failed-on-node; skipping"
                            );
                        }
                        Ok(_) => {
                            // Emit only after the row was actually flipped to
                            // `failed`, so the counter reflects flips (not
                            // detections) and a retried DB error can't inflate it.
                            node_drain_metrics::sandbox_failed_on_node(
                                &node_id.to_string(),
                                pre_lifecycle_tag,
                            );
                        }
                        Err(db_err) => {
                            tracing::warn!(
                                node_id = %node_id,
                                sandbox_id = %sandbox,
                                error = %db_err,
                                "node_drain: failed to mark node-failed sandbox as failed; \
                                 will retry on next tick"
                            );
                        }
                    }
                }
                Err(err) => {
                    node_drain_metrics::failure(&node_id.to_string(), err.phase(), pre_lifecycle_tag);
                    tracing::warn!(
                        node_id = %node_id,
                        sandbox_id = %sandbox,
                        phase = err.phase(),
                        pre_lifecycle = pre_lifecycle_tag,
                        error = %err,
                        "node_drain: sandbox evacuation (suspend) failed; will retry on next tick"
                    );
                }
            }
        })
        .await
    }

    /// Returns `true` if the node row reached `deleted` this tick;
    /// `false` if the call failed or was a no-op (stale identity).
    async fn handle_terminating_node(&self, node_id: NodeId) -> bool {
        // Same cordon convergence as draining (see handle_draining_node).
        // The cordon must hold from :drain through :terminate; if the
        // Node became schedulable again between phases (manual
        // un-cordon, controller race) re-assert before letting the
        // ASG terminate the instance — otherwise fresh pods could be
        // placed in the window between cordon-loss and instance death.
        // A gone kube Node does not block this phase: the terminator
        // has its own KubeNodeGone handling below (park with an
        // explicit `drain_last_error` for manual recovery).
        if self
            .ensure_cordon(node_id, PHASE_TERMINATING)
            .await
            .is_none()
        {
            return false;
        }
        // Last look before the instance dies. Everything above only proves the
        // node *was* empty when something wrote `terminating`; the ASG call is
        // irreversible, so ask once more here.
        //
        // **This narrows the window; it does not close it.** Between this count
        // and the terminate call a placement writer can still attach, because
        // the writers that set `sandboxes.active_node_id` on the pooled
        // post-wake, post-create, pooled-create, and direct-restore paths take
        // no lock on the `nodes` row and so cannot be ordered against this
        // read. Closing it needs a fence every placement writer and this path
        // both honour, which is its own change (see the RFC's reverse-lock-order
        // section). What this does buy: the window shrinks
        // from "any time after the phase write" (which for an operator-driven
        // `:terminate` can be minutes, since the next tick is up to 5 s away and
        // the phase write is not the terminate) to the microseconds between this
        // statement and the ASG call, and a sandbox that attached in that longer
        // window now parks the node instead of dying with it.
        //
        // Counted with the terminator's own predicate, so a node this refuses is
        // exactly a node the `drained → terminating` gate would have refused.
        // An error defers rather than proceeding: an unanswered residency
        // question must never authorize a destroy.
        match self
            .sink
            .count_termination_blocking_residents_on_node(node_id)
            .await
        {
            Ok(blocking) if !blocking.is_empty() => {
                let mut lifecycles = blocking.by_lifecycle.clone();
                lifecycles.sort();
                let rendered = lifecycles
                    .iter()
                    .map(|(lifecycle, count)| format!("{lifecycle}={count}"))
                    .collect::<Vec<_>>()
                    .join(",");
                // Revert to `drained` rather than parking in `terminating`:
                // `drained` is the phase whose reconciler re-evaluates the
                // resident guard every tick, so the node converges on its own
                // once the sandbox clears. Parking in `terminating` would need
                // an operator to notice. The revert is not the completion edge,
                // so it carries no residency guard of its own and cannot be
                // refused here.
                if let Err(err) = self
                    .sink
                    .set_drain_phase(
                        node_id,
                        PHASE_TERMINATING,
                        PHASE_DRAINED,
                        Some("termination aborted: sandbox attached after the phase write"),
                    )
                    .await
                {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: reverting terminating -> drained failed;                          instance NOT terminated"
                    );
                    return false;
                }
                tracing::warn!(
                    node_id = %node_id,
                    lifecycles = %rendered,
                    "node_drain: aborting terminate — a sandbox attached after the                      phase write; node returned to drained rather than destroyed"
                );
                return false;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "node_drain: pre-terminate residency recheck failed; deferring                      the ASG call to the next tick"
                );
                return false;
            }
        }
        match self.terminator.terminate(node_id).await {
            Ok(()) => {
                self.advance_terminating_to_deleted(node_id, "terminated")
                    .await
            }
            Err(TerminateError::KubeNodeGone) => {
                // The kube Node is gone, but a deleted kube Node does
                // NOT imply the underlying EC2 instance was terminated
                // — an operator could have removed the Node object
                // manually, leaving a live ASG member. Without an EC2
                // DescribeInstances roundtrip we can't tell, and
                // auto-advancing to `deleted` here would silently
                // strand a running instance. Stay in `terminating`
                // with an explicit `drain_last_error` so an operator
                // can investigate (look up the instance in AWS, then
                // either terminate it manually and flip the row, or
                // re-attach a kube Node and let the reconciler retry).
                node_drain_metrics::terminate_failure(&node_id.to_string());
                let msg = "kube Node missing; EC2 instance state unverified — \
                           manual intervention required";
                if let Err(db_err) = self
                    .sink
                    .set_drain_phase(node_id, PHASE_TERMINATING, PHASE_TERMINATING, Some(msg))
                    .await
                {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %db_err,
                        "node_drain: set_drain_phase(terminating, kube_node_gone) failed"
                    );
                }
                tracing::warn!(
                    node_id = %node_id,
                    "node_drain: kube Node missing; row left in terminating for manual recovery"
                );
                false
            }
            Err(TerminateError::StaleIdentity) => {
                // The state was reset under us (replacement instance UID
                // mismatch, or admin undrain). Leave the row alone — the
                // next tick will pick up whatever lifecycle is now
                // current. No `drain_last_error` write: stale identity
                // is not an error condition.
                tracing::info!(
                    node_id = %node_id,
                    "node_drain: skipping termination; node identity changed under us"
                );
                false
            }
            Err(err) => {
                node_drain_metrics::terminate_failure(&node_id.to_string());
                let msg = err.to_string();
                match self
                    .sink
                    .set_drain_phase(node_id, PHASE_TERMINATING, PHASE_TERMINATING, Some(&msg))
                    .await
                {
                    Ok(false) => {
                        tracing::info!(
                            node_id = %node_id,
                            "node_drain: skipping last_error record; node no longer terminating"
                        );
                    }
                    Err(db_err) => {
                        tracing::warn!(
                            node_id = %node_id,
                            error = %db_err,
                            "node_drain: set_drain_phase(terminating, error) failed"
                        );
                    }
                    Ok(true) => {}
                }
                tracing::warn!(
                    node_id = %node_id,
                    error = %msg,
                    "node_drain: terminate failed; will retry on next tick"
                );
                false
            }
        }
    }

    /// Idempotently ensure the kube Node is cordoned. Called at the
    /// top of `handle_draining_node` and `handle_terminating_node` to
    /// converge any divergence left by a Phase-2 failure of `POST
    /// :drain` (or manual uncordon). On failure: record
    /// `drain_last_error` (best-effort) and return `None` (deferred) so
    /// the caller short-circuits — we MUST NOT advance drain progress
    /// while the kube scheduler may still place new pods on the node.
    /// Idempotent: a successful patch on an already-cordoned Node is a
    /// no-op at the kube level. A gone kube Node is a cordon *success*
    /// (nothing left to schedule onto), passed through as
    /// [`CordonOutcome::KubeNodeGone`] so the draining handler can route
    /// it to the KubeNodeGone recovery (the terminating handler proceeds
    /// either way — its terminator has its own KubeNodeGone handling).
    async fn ensure_cordon(&self, node_id: NodeId, expected_phase: &str) -> Option<CordonOutcome> {
        match self.sink.ensure_node_unschedulable(node_id).await {
            Ok(outcome) => Some(outcome),
            Err(CordonError::StaleIdentity) => {
                // Same-name kube Node replacement detected. The
                // inventory reflector will reset `drain_phase` on its
                // next tick (see `upsert_k8s_node_role` in
                // `crates/libs/api/src/controllers/substrate/kubernetes/store/node_inventory.rs`).
                // Skip this tick silently — no `drain_last_error`
                // write, because the row is about to lose its drain
                // state anyway and a stale error would survive past
                // the reset.
                tracing::info!(
                    node_id = %node_id,
                    phase = expected_phase,
                    "node_drain: cordon deferred — kube Node UID changed, awaiting inventory reconcile"
                );
                None
            }
            Err(err) => {
                let msg = format!("cordon convergence failed: {err}");
                if let Err(db_err) = self
                    .sink
                    .set_drain_phase(node_id, expected_phase, expected_phase, Some(&msg))
                    .await
                {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %db_err,
                        "node_drain: set_drain_phase(cordon error) failed"
                    );
                }
                tracing::warn!(
                    node_id = %node_id,
                    error = %msg,
                    phase = expected_phase,
                    "node_drain: cordon convergence failed; deferring drain progress for this node"
                );
                None
            }
        }
    }

    /// EC2 cross-check for a `draining` node whose kube Node is gone.
    /// Returns the recovery context (the `node_k8s.k8s_node_uid` observed by
    /// the cross-check, carried into every guarded give-up write) only when
    /// EC2 definitively reports the backing instance terminated. Every other
    /// case — no cross-check configured, instance still live (a manually
    /// deregistered Node on a running box — the genuinely interesting case),
    /// or a lookup error — parks the node: counted in
    /// [`TickReport::kube_node_gone_parked`], surfaced via the
    /// `node_drain.kube_node_gone_parked{phase:draining}` gauge, and the
    /// per-sandbox work keeps its pre-recovery retry behavior. Mirrors
    /// `drained_node_terminator::handle_kube_node_gone`'s fail-closed
    /// contract; the verdict itself (`Gone` only on an explicit `terminated`
    /// EC2 state) is defined once in `orphaned_nodes::aws`.
    ///
    /// Runs once per node per tick while the node stays in this state — one
    /// `DescribeInstances` per [`DEFAULT_TICK`].
    ///
    /// **A `Gone` verdict no longer converges the node in a few ticks.** The
    /// bulk give-up (`fail_resident_sandboxes_on_gone_node`) binds the resident
    /// include-list, so it cannot clear the four lifecycles the drain gate now
    /// holds on (`created`, `stopped`, `deleting`, `quarantined`) — and on a
    /// terminated instance a `quarantined` row can never resolve either, because
    /// quarantine recovery needs the owning node to republish state. Such a node
    /// stays `draining` until an operator deletes the sandbox, so its WARN is
    /// paced ([`HELD_WARN_INTERVAL`]) rather than emitted every tick; without
    /// that it is ~720 identical lines an hour, per node, indefinitely.
    ///
    /// The `DescribeInstances` call itself is still per-tick. Converging such a
    /// node — the give-up's premise (local bytes died with the instance) does
    /// hold for these lifecycles too — is a change to a destructive sweep's
    /// scope and wants its own review.
    async fn assess_gone_draining_node(
        &self,
        node_id: NodeId,
        report: &mut TickReport,
    ) -> Option<GoneNodeRecovery> {
        let Some(lookup) = self.instance_lookup.as_ref() else {
            tracing::warn!(
                node_id = %node_id,
                "node_drain: kube Node missing for draining node and no EC2 \
                 cross-check configured; node-bound work will keep retrying"
            );
            report.kube_node_gone_parked += 1;
            return None;
        };

        match lookup.lookup_instance_state(node_id).await {
            Ok(NodeInstanceState {
                outcome: InstanceLookupOutcome::Gone,
                expected_uid,
            }) => {
                // Paced: a node held by a divergent-lifecycle sandbox stays in
                // this state indefinitely (see this method's docs), and the
                // recovery below still runs on every tick regardless.
                if self.warn_pacer.due(node_id, "gone_instance_terminated") {
                    tracing::warn!(
                        node_id = %node_id,
                        "node_drain: kube Node missing and EC2 instance terminated; \
                         running KubeNodeGone recovery for draining node (further \
                         ticks for this node log at debug for the next hour)"
                    );
                } else {
                    tracing::debug!(
                        node_id = %node_id,
                        "node_drain: KubeNodeGone recovery still running for draining \
                         node (warn paced)"
                    );
                }
                // Deliberately **not** counted in `kube_node_gone_parked`, even
                // though this branch can now leave such a node in `draining`
                // indefinitely (the bulk give-up binds the resident include-list,
                // so a `created`/`stopped`/`deleting`/`quarantined` row survives
                // it). That gauge means "could not be auto-recovered this tick",
                // and recovery *is* running here — `g4` covers the case where it
                // converges. A node whose recovery converges nothing is already
                // reported by `termination_blocking_residents`, which is nonzero
                // for exactly that node and names the blocking lifecycles;
                // overloading this gauge would make both harder to read.
                Some(GoneNodeRecovery { expected_uid })
            }
            Ok(NodeInstanceState {
                outcome: InstanceLookupOutcome::Live { instance_id, state },
                ..
            }) => {
                // Paced for the same reason as the `Gone` arm: this parks the
                // node too, and a deregistered-but-live box stays that way until
                // someone looks at it.
                if self.warn_pacer.due(node_id, "gone_instance_live") {
                    tracing::warn!(
                        node_id = %node_id,
                        %instance_id,
                        %state,
                        "node_drain: kube Node missing but EC2 instance still live \
                         (deregistered node); leaving draining node for manual \
                         review (further ticks log at debug for the next hour)"
                    );
                } else {
                    tracing::debug!(
                        node_id = %node_id,
                        %instance_id,
                        %state,
                        "node_drain: draining node still parked on a live \
                         deregistered instance (warn paced)"
                    );
                }
                report.kube_node_gone_parked += 1;
                None
            }
            Err(err) => {
                // Paced like the other two arms. A persistently failing
                // cross-check (throttling, a missing IAM permission) repeats
                // every tick for as long as the cause lasts, which is not
                // bounded by anything this reconciler controls.
                if self.warn_pacer.due(node_id, "gone_cross_check_failed") {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: kube Node missing and EC2 cross-check failed; \
                         will retry on next tick (further failures for this node \
                         log at debug for the next hour)"
                    );
                } else {
                    tracing::debug!(
                        node_id = %node_id,
                        error = %err,
                        "node_drain: EC2 cross-check still failing for draining \
                         node (warn paced)"
                    );
                }
                report.kube_node_gone_parked += 1;
                None
            }
        }
    }

    async fn advance_terminating_to_deleted(&self, node_id: NodeId, reason: &str) -> bool {
        // Per the corrected drain design: terminate success DELETEs the
        // `nodes` row (no `'deleted'` tombstone phase). The
        // `drain_phase = 'terminating'` guard inside
        // `delete_terminating_node` protects against admin drift.
        match self.sink.delete_terminating_node(node_id).await {
            Ok(true) => {
                node_drain_metrics::transition(PHASE_TERMINATING, "deleted");
                node_drain_metrics::clear_node_gauges(&node_id.to_string());
                tracing::info!(node_id = %node_id, reason, "node_drain: terminated");
                true
            }
            Ok(false) => {
                tracing::info!(
                    node_id = %node_id,
                    reason,
                    "node_drain: skipping delete; node no longer in terminating phase"
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    reason,
                    "node_drain: delete_terminating_node failed after terminate"
                );
                false
            }
        }
    }
}

/// Drive the reconciler loop. Returns when `shutdown_token` fires.
///
/// When `config.enabled = false` (the default), the function logs the
/// inert state once and then waits on the shutdown token without ever
/// running a tick. The reconciler is therefore present-but-dormant in
/// every environment that does not explicitly opt in.
pub async fn run<S: NodeDrainSink, T: InstanceTerminator>(
    config: ReconcilerConfig,
    sink: S,
    terminator: T,
    instance_lookup: Option<Arc<dyn InstanceStateLookup>>,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "node-drain reconciler is disabled (SPACE_NODE_DRAIN_ENABLED unset); \
             waiting on shutdown without running any ticks"
        );
        shutdown_token.cancelled().await;
        tracing::info!("node-drain reconciler stopping");
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        max_per_node = config.max_per_node,
        max_per_cluster = config.max_per_cluster,
        evacuate_timeout_secs = config.evacuate_timeout.get().as_secs(),
        // The substrate clamps each blocking wake wait to its node-RPC
        // blocking budget, so the configured value only binds below it —
        // log the effective per-attempt wait so config and behavior can
        // be reconciled from the logs alone.
        evacuate_timeout_effective_secs = config
            .evacuate_timeout
            .get()
            .min(NODE_RPC_BLOCKING_BUDGET)
            .as_secs(),
        ec2_cross_check = instance_lookup.is_some(),
        "starting node-drain reconciler"
    );
    let reconciler =
        Reconciler::new(config.clone(), sink, terminator).with_instance_lookup(instance_lookup);

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let _ = reconciler.tick().await;
                node_drain_metrics::loop_latency(started);
            }
            // Event-driven cohort restore: an artifact backup was just
            // registered `ready` (the cluster-api `NOTIFY`d), so drive the
            // restore sweep immediately instead of waiting for the next tick.
            // The periodic tick above remains the correctness fallback, so a
            // missed signal only costs latency, not correctness.
            _ = reconciler.sink.wait_for_restore_signal() => {
                let spawned = reconciler.run_restore_sweep().await;
                if spawned > 0 {
                    tracing::info!(
                        spawned,
                        "node_drain: backup-ready signal drove cohort restore sweep"
                    );
                }
            }
            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    // Give in-flight evacuations a bounded chance to land their final
    // DB UPDATE before the runtime is dropped. Tighter than the outer
    // SHUTDOWN_DRAIN_TIMEOUT in space-cluster-controller's
    // `run_reconcilers` (10 s) so this reconciler always returns
    // within the parent's budget.
    reconciler
        .drain_evacuations(EVACUATION_SHUTDOWN_DRAIN_TIMEOUT)
        .await;
    tracing::info!("node-drain reconciler stopping");
}

/// Bounded wait for in-flight evacuation tasks on shutdown. Sized
/// smaller than `space-cluster-controller`'s outer SHUTDOWN_DRAIN_TIMEOUT
/// (10 s) so [`run`] always returns within the parent's budget. Any
/// evacuation still running past this point will be aborted when the
/// tokio runtime is dropped; sandboxes left in `'suspending'` or
/// `'restoring'` need manual operator recovery.
const EVACUATION_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
