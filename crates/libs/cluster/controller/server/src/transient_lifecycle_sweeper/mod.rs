//! Cluster-singleton sweeper that bounds the time a sandbox row can sit in a
//! transient lifecycle.
//!
//! On *draining* nodes the drain reconciler's per-node stale-abandon sweep
//! (`node_drain::abandon_stale_resident_sandboxes`) already bounds the full
//! non-evacuable transient set, but only there — on a non-draining node the
//! transient lifecycles `creating`, `starting`, `pausing`, `resuming`,
//! `suspending`, `restoring`, `stopping`, `checkpointing` are otherwise
//! unbounded. If the underlying sbx-mgr RPC silently hangs, the client
//! crashes mid-flight, or a stage between DB writes is lost, the row stays
//! in the transient state until an operator intervenes. This loop is the
//! cluster-wide backstop.
//!
//! ## Scope
//!
//! Cluster-wide. The drain reconciler's per-node stale-abandon sweep
//! is preserved as a faster path on
//! draining nodes; this loop catches stuck rows on any node, including
//! non-draining ones, so a wedged `pausing` outside any drain context
//! still ages out.
//!
//! ## Per-lifecycle knobs
//!
//! A single default age threshold applies to every transient lifecycle,
//! with optional per-lifecycle overrides. `creating` may legitimately take
//! a minute or two during a cold start; `pausing` is an in-memory
//! transition that should complete in seconds. Operators override either
//! direction without recompiling. Setting a per-lifecycle override to `0`
//! disables the sweep for that lifecycle only; setting the master
//! `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED=false` disables the entire
//! loop.
//!
//! ## Verify before destroy
//!
//! Aging out is **select -> probe -> classify -> act**. The CAS to `failed`
//! is lossy — the DB triggers that fire on that transition erase the
//! evacuation marker and restore lease — and a row stuck past the age
//! threshold can still have a live, serving VM behind it (a node-side
//! restore that merely missed the API's blocking budget). So candidates are
//! *listed* without flipping, their nodes are asked what they actually know
//! (`WorkerApi/SandboxState`, batched per node), and each row gets the one
//! provably-safe action for its probe verdict:
//!
//! - **alive + `restoring`, routable execution** — converge the row to the
//!   wake target the VM already reached (lease-guarded CAS), instead of
//!   failing it.
//! - **alive + `restoring`, unroutable execution** — the DB shows no active
//!   `running` + `pod_ip` execution, so the wake target is a promise the
//!   Kubernetes substrate can't currently keep (a `started` row without
//!   restore context wedges behind the wake path's gen≥2 fresh-pod refusal).
//!   Verify against the apiserver first: a live pod matching the active
//!   execution's generation hydrates the execution row (the wake path's own
//!   `persist_pod_metadata` write) and the row then converges; no such pod
//!   defers the row, still `restoring`, until something hydrates it or the
//!   VM retires into the revert path below.
//! - **alive + `creating`, owner gone, no receipt can finish it** — a create
//!   whose API process died after its pod was placed, before its execution
//!   became routable (`running` with a pod IP), leaves the VM running and the
//!   row `creating`. So does a start that adopted such a row and gave its
//!   activation up before its wake, routable or not: it clears the attempt a
//!   completion receipt would match on. The candidate select never lists a `creating` row whose
//!   active execution's activation-attempt lease (or the expiry an owner left
//!   when it gave the activation up) hasn't been expired for a full age
//!   window: until then the request may still be running and finalize the
//!   row itself. Once it has, the row fails and retires like a dead one
//!   rather than converging: the client that asked for it usually never got
//!   its id, and nothing records what else the request asked for. The fail
//!   CAS re-checks that no receipt can finish it yet.
//! - **alive + `creating`, otherwise** — defer. A crashed create whose
//!   execution became routable still holds its attempt, so the node's
//!   activation-completion receipt moves it to `started`. A `creating` row
//!   whose execution has no lease at all has nothing to show its owner is
//!   gone: a non-blocking warm-pool adoption never takes one, and an
//!   activation that finished releases its lease entirely even if its
//!   lifecycle write then failed.
//! - **alive + anything else** — defer; a later slice may converge those.
//! - **`restoring` (dormant / dead / missing) with a ready durable
//!   snapshot** — revert to `suspended` and retire the leftover execution;
//!   the snapshot is canonical mid-restore and the next restore recovers it.
//!   Revert never applies to any other lifecycle: the artifact pointer is
//!   never cleared on wake, so a matching ready backup there is the
//!   *previous* suspend cycle's bytes and reverting would silently restore
//!   pre-wake state (see `classify::decide`).
//! - **dormant local artifact, any other lifecycle** — defer; the node holds
//!   the only current copy and the sweep never destroys it.
//! - **dead / missing, any other lifecycle** — the old loud behavior: CAS to
//!   `failed` and retire the execution (node-local state preserved for
//!   salvage).
//! - **inconclusive probe** — defer. Unreachable means defer, never destroy.
//!
//! The probe fan-out is budgeted to half the tick (same pattern as the
//! command-ready-pending phase): each unreachable node costs up to ~2x the
//! probe timeout, and a mass-unreachability incident — exactly when the
//! candidate set is large — must not stretch one tick to tens of minutes or
//! act on verdicts gathered long before. Candidates on unprobed nodes defer
//! to the next tick.
//!
//! ## Owner check
//!
//! A row in an activation-owned lifecycle (`creating`, `starting`,
//! `resuming`, `restoring`, `pausing`, `checkpointing`;
//! [`is_activation_owned`]) can have an owner, the request bringing the
//! sandbox up, which holds an activation-attempt lease on the active
//! execution. For `pausing` and `checkpointing` that owner is a start that
//! runs on the row before it settles; the pause or checkpoint itself takes no
//! attempt, so a row without one is swept as before. The candidate select never lists such a row, and
//! the fail, converge and revert CASes refuse it, until that lease has ended
//! a full age window ago: until then the owner may still be running, and
//! acting on its row makes its own transition lose. Age alone can't show
//! that, because the lease's TTL outlives any one API-to-node call but not a
//! whole activation.
//!
//! An owner that gives up an activation it never dispatched clears its
//! attempt but keeps the time it gave up as the lease's expiry, so a row it
//! leaves behind is covered too. For a `creating` row that's a start which
//! adopted a crashed create's row and failed: once the window has passed,
//! the row is failed and retired whether its VM is alive, dead or missing,
//! even though whoever called the start knows its id. That holds even when
//! the start had recorded the pod routable: unlike a crashed create that
//! became routable, the row holds no attempt for the node's
//! activation-completion receipt to finish. Failed cleanup reads such an
//! execution as finished, since it reached `running`, so a retire that
//! doesn't complete can leave it to repair the row to `started`.
//!
//! For `restoring` the restore lease usually says the same thing, since
//! materialization sets both to the same window, but a `restoring` row whose
//! restore lease was cleared still has its wake's attempt. The
//! unrestorable-`restoring` fail doesn't apply this check; it matches only a
//! row with no active execution.
//!
//! A client that keeps retrying a failed restore renews both on every retry,
//! so the check alone would spare its row, and the pending pod holding a node
//! slot, for as long as the client keeps going. The restoring-episode
//! deadline ([`ReconcilerConfig::restoring_episode_deadline`]) ends that: past
//! it, a Kubernetes `restoring` row whose active execution has no pod IP, and
//! whose pre-restore snapshot proves a revert safe, is listed whatever its
//! leases say. Its probe still decides the action, and only the revert to
//! `suspended` honors the deadline; the fail and converge CASes still apply
//! the check. See `restoring_episode::restoring_episode_overdue`.
//!
//! Of these lifecycles only `creating` and `restoring` are ever written to
//! the row today: the start and resume paths keep the row's old lifecycle
//! while they run, and nothing writes `pausing` or `checkpointing`. The other
//! arms keep the check in place if a writer starts using them.
//!
//! ## Drift safety
//!
//! Every action is its own guarded CAS re-checking `lifecycle = $expected
//! AND lifecycle_changed_at < NOW() - INTERVAL`. A row that exited the
//! transient lifecycle between the select and the act — a concurrent
//! completion CAS, a user-issued lifecycle action, an admin force-flip —
//! drops out of the predicate and is left alone. Substrate cleanup runs only
//! on rows the fail CAS actually claimed.
//!
//! ## Substrate cleanup
//!
//! After flipping a row to `'failed'` — or reverting a `restoring` row to
//! `'suspended'` — the sweeper calls
//! [`SubstrateAdapter::retire_execution`] to tear down any backing pod that
//! the wedged transition would have left behind while preserving node-local
//! sandbox state, and (via the retire's pod-tracking cleanup) to finish the
//! active execution row and unplace the sandbox. The call is best-effort: if
//! it fails, the row keeps its resting lifecycle.
//!
//! Every retire carries the pod identity its candidate select observed. The
//! sequence is select -> probe -> CAS -> retire, and each step is
//! point-in-time: the CAS proves the *row* did not change, nothing proves the
//! *pod* did not. Between them a create can converge, a wake can replace the
//! execution, or the pod can simply be rescheduled — and an unfenced retire
//! would stop that recovered VM and delete its pod on the strength of a probe
//! taken before any of it happened. So the substrate re-checks the identity
//! and answers `PodIdentityChanged` instead, tearing nothing down. That is a
//! defer, counted apart from failures: the sandbox belongs to whatever changed
//! it, and this loop's next tick re-reads it from scratch. When
//! Unless `SPACE_FAILED_SANDBOX_CLEANUP_ENABLED=false`, the same sweeper also
//! retries active Kubernetes executions for authoritative
//! `failed` rows — gated behind the same node probe, so a failed row whose
//! VM is still alive (or whose node can't be reached) is deferred rather
//! than cleaned; dormant rows are retired even when the node holds a
//! salvageable local artifact (see
//! [`ProbeVerdict::permits_failed_retire`]). Attempts rotate by
//! `sandbox_k8s_executions.updated_at` so a persistently bad row cannot
//! starve later failed sandboxes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::{
    AbandonedRestoreRetirement, CommandReadyProbe, PodFenceMismatch, RetireExecutionOutcome,
    RetireFence, Substrate,
};
use pplx_space_common_core::types::{PositiveDuration, SandboxId, SandboxLifecycle};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::cluster_reconciler::{
    self as metrics, AgedTransientOutcome, CleanupOutcome, CommandReadyOutcome,
    FailedCleanupOutcome, Loop, OutcomeEvent, UnrestorableOutcome, WakeLifecycle,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env_config;

pub mod classify;
pub mod probe;
mod restoring_episode;
pub mod store;

#[cfg(test)]
mod lifecycle_writer_tests;
#[cfg(test)]
mod tests;

pub use classify::{ExecutionRoutability, ProbeObservation, ProbeVerdict, SweepAction};
pub use probe::{ProbeNodeError, SweepCandidate};
pub use store::SubstrateSweepSink;

/// Lifecycles the sweeper bounds. The 8 transient states a sandbox can
/// legitimately pass through during normal lifecycle operations; each
/// represents an in-flight request that should complete in seconds-to-
/// minutes. `Deleting` is intentionally excluded: rows in `deleting` are
/// already on an explicit delete path, and flipping a `deleting` row to
/// `failed` would be a regression rather than an unblock.
///
/// Independently of this set, the production [`SubstrateSweepSink`] exempts
/// evacuation-marked **`restoring`** rows (non-NULL
/// `evacuation_restore_target`) from age-out — node-drain owns re-driving
/// those, so the backstop must not race them to `failed`. Marked
/// `suspending` rows are deliberately **not** exempt: nothing else recovers a
/// crashed/hung evacuation suspend, so the backstop is its only age-out path.
pub(crate) const SWEEPABLE_LIFECYCLES: &[SandboxLifecycle] = &[
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Suspending,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Stopping,
    SandboxLifecycle::Checkpointing,
];

/// Whether a row in `lifecycle` can be owned by a request holding an
/// activation-attempt lease on the active execution: the create, start,
/// resume, or wake or restore bringing the sandbox up, or a start that runs
/// while the row is still `pausing` or `checkpointing` (start materializes an
/// execution from either). The sweep doesn't act on such a row while that
/// lease may still be its owner's; see "Owner check" in the module docs.
pub(crate) const fn is_activation_owned(lifecycle: SandboxLifecycle) -> bool {
    matches!(
        lifecycle,
        SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Checkpointing
    )
}

fn emit_transient_sweep_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    lifecycle: &str,
    age: Duration,
    error: Option<String>,
) {
    let mut details = transient_sweep_details(event_type, lifecycle, age);
    if let Some(error) = error {
        details = details.error(error);
    }
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    // Every failed-phase event emitted here follows the caller's WARN outcome
    // log for the same failure.
    .emit_with(details.logged_by_caller());
}

/// The state fields of a transient-sweep event, split out so a test can pin
/// the direction each event type records. A rollback runs *out of* `failed`
/// and everything else runs into it, and an event that reports the wrong
/// direction is precisely the misreading these events exist to prevent.
fn transient_sweep_details(
    event_type: SandboxEventType,
    lifecycle: &str,
    age: Duration,
) -> SandboxEventDetails {
    let details = SandboxEventDetails::default()
        .reason("transient_lifecycle_age_out")
        .operation("transient_lifecycle_sweep")
        .trigger(SandboxEventTrigger::System)
        // The row's *actual* time in the transient lifecycle (not the config
        // threshold), so a reconstructed timeline shows how long it sat.
        .age_secs(age.as_secs().to_string());
    let mut details = details;
    if event_type == SandboxEventType::LifecycleTransitionRolledBack {
        // The one event here that runs *out of* `failed`, so every state field
        // is the reverse of the age-out's. Reusing the failure shape would
        // record the rollback as still failed, which is exactly the misreading
        // this event exists to prevent.
        details = details
            .with_from_state("failed")
            .with_to_state(lifecycle)
            .lifecycle_state(lifecycle)
            .target_state(lifecycle);
    } else {
        details = details.with_from_state(lifecycle).target_state("failed");
        if event_type == SandboxEventType::LifecycleTransitionTimedOut {
            details = details.with_to_state("failed");
        } else {
            details = details.lifecycle_state("failed");
        }
    }
    details
}

fn emit_sandbox_failed_event(
    sandbox_id: SandboxId,
    from_state: &str,
    reason: &'static str,
    operation: &'static str,
    age: Duration,
    result: Option<&str>,
) {
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation(operation)
        .trigger(SandboxEventTrigger::System)
        .with_from_state(from_state)
        .with_to_state("failed")
        .age_secs(age.as_secs().to_string());
    if let Some(result) = result {
        details = details.result(result);
    }
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxFailed,
        SandboxEventSource::ClusterController,
    )
    // Every failed-phase event emitted here follows the caller's WARN outcome
    // log for the same failure.
    .emit_with(details.logged_by_caller());
}

/// Emit a non-destructive verify-before-destroy outcome: a converge
/// ([`SandboxEventType::LifecycleConverged`]), a revert
/// ([`SandboxEventType::LifecycleRevertedToSuspended`]), or a defer
/// ([`SandboxEventType::SweepDeferred`]). `result` carries the probe verdict
/// that drove the decision; `reason` names the decision rule.
fn emit_sweep_verify_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    lifecycle: SandboxLifecycle,
    to_state: Option<SandboxLifecycle>,
    verdict: ProbeVerdict,
    age: Duration,
    reason: &str,
) {
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation("transient_lifecycle_sweep")
        .trigger(SandboxEventTrigger::System)
        .with_from_state(lifecycle.as_str())
        .result(verdict.as_str())
        .age_secs(age.as_secs().to_string());
    if let Some(to_state) = to_state {
        details = details
            .with_to_state(to_state.as_str())
            .target_state(to_state.as_str());
    }
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// Which pass asked for a retire. Each has its own operation label, its own
/// consequence when the retire is refused, and its own counter; the enum keeps
/// those three together instead of spreading them across the call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupPass {
    /// Retire after a claimed revert of a `restoring` row to `suspended`.
    RevertToSuspended,
    /// Retire after a claimed age-out CAS to `failed`.
    TransientAgeOut,
    /// Retire after the command-ready-pending reaper flipped a wedged
    /// `started` row to `failed`.
    CommandReadyReap,
    /// Retire retried against an authoritative `failed` row that still has an
    /// active execution.
    FailedCleanup,
}

impl CleanupPass {
    fn operation(self) -> &'static str {
        match self {
            Self::RevertToSuspended | Self::TransientAgeOut => "transient_lifecycle_sweep",
            Self::CommandReadyReap => "command_ready_pending_sweep",
            Self::FailedCleanup => "failed_sandbox_active_execution_cleanup",
        }
    }

    /// What the sandbox is left holding when this pass's retire is refused.
    fn deferral_detail(self) -> &'static str {
        match self {
            Self::RevertToSuspended => {
                "reverted row keeps its execution, placement, and pod; whatever replaced the \
                 observed pod owns them now"
            }
            Self::TransientAgeOut | Self::CommandReadyReap => {
                "row stays failed with its execution and pod intact; the successor that \
                 replaced the observed pod is left running"
            }
            Self::FailedCleanup => {
                "cleanup deferred; the next pass re-reads the execution and fences on \
                 whatever is current"
            }
        }
    }

    fn count_deferral(self, report: &mut TickReport) {
        match self {
            Self::RevertToSuspended | Self::TransientAgeOut | Self::CommandReadyReap => {
                report.cleanup_pod_identity_deferred += 1;
            }
            Self::FailedCleanup => report.failed_cleanup_pod_identity_deferred += 1,
        }
    }
}

/// Record a retire the substrate refused on pod identity: meter it, count it,
/// and emit the sandbox event. Shared by every cleanup path so all of them
/// report the defer the same way — and so none of them can quietly book it as
/// a completed cleanup.
fn note_cleanup_pod_identity_deferral(
    sandbox_id: SandboxId,
    pass: CleanupPass,
    from_state: &str,
    fence: &RetireFence,
    mismatch: PodFenceMismatch,
    report: &mut TickReport,
) {
    OutcomeEvent::substrate_cleanup(CleanupOutcome::PodIdentityChanged, from_state)
        .sandbox_id(sandbox_id)
        .reason(mismatch.as_str())
        .detail(pass.deferral_detail())
        .warn();
    pass.count_deferral(report);

    let mut details = SandboxEventDetails::default()
        .reason("pod_identity_changed")
        .operation(pass.operation())
        .trigger(SandboxEventTrigger::System)
        .with_from_state(from_state)
        .result(mismatch.as_str());
    if let RetireFence::Pod(fence) = fence {
        details = details.pod_name(fence.pod_name.as_str());
        if let Some(pod_uid) = fence.pod_uid.as_ref() {
            details = details.pod_uid(pod_uid.as_str());
        }
    }
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SweepDeferred,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// Emit one step of the execution retire that follows a claimed revert to
/// `suspended` ([`SandboxEventType::CleanupStarted`] / `CleanupCompleted` /
/// `CleanupFailed`). Unlike the fail-path cleanup events, the row's resting
/// lifecycle is `suspended`, not `failed`.
fn emit_revert_cleanup_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    lifecycle: SandboxLifecycle,
    age: Duration,
    error: Option<String>,
) {
    let mut details = SandboxEventDetails::default()
        .reason("transient_lifecycle_revert")
        .operation("transient_lifecycle_sweep")
        .trigger(SandboxEventTrigger::System)
        .with_from_state(lifecycle.as_str())
        .lifecycle_state(SandboxLifecycle::Suspended.as_str())
        .age_secs(age.as_secs().to_string());
    if let Some(error) = error {
        details = details.error(error);
    }
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    // Every failed-phase event emitted here follows the caller's WARN outcome
    // log for the same failure.
    .emit_with(details.logged_by_caller());
}

fn emit_failed_cleanup_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    result: Option<&str>,
    error: Option<String>,
) {
    let mut details = SandboxEventDetails::default()
        .reason("failed_sandbox_active_execution_cleanup")
        .operation("failed_sandbox_active_execution_cleanup")
        .trigger(SandboxEventTrigger::System)
        .lifecycle_state("failed");
    if let Some(result) = result {
        details = details.result(result);
    }
    if let Some(error) = error {
        details = details.error(error);
    }

    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    // Every failed-phase event emitted here follows the caller's WARN outcome
    // log for the same failure.
    .emit_with(details.logged_by_caller());
}

fn emit_failed_active_repair_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    result: &str,
    node_status: Option<SandboxLifecycle>,
    error: Option<String>,
) {
    let mut details = SandboxEventDetails::default()
        .reason("failed_sandbox_active_execution_repair")
        .operation("failed_sandbox_active_execution_repair")
        .trigger(SandboxEventTrigger::System)
        .with_from_state("failed")
        .target_state("started")
        .lifecycle_state("started")
        .result(result);
    if event_type == SandboxEventType::LifecycleConverged {
        details = details.with_to_state("started");
    }
    if let Some(node_status) = node_status {
        details = details.condition_set(format!("node_status={}", node_status.as_str()));
    }
    if let Some(error) = error {
        // The caller's outcome event already logs this failure at WARN.
        details = details.error(error).logged_by_caller();
    }

    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// The `to_lifecycle` label for a converge. Converge targets come from the
/// wake path: `started` normally, `paused` when an evacuation-restore marker
/// pinned a paused restore — `classify::decide` produces exactly those two.
fn wake_lifecycle(target: SandboxLifecycle) -> WakeLifecycle {
    match target {
        SandboxLifecycle::Paused => WakeLifecycle::Paused,
        _ => WakeLifecycle::Started,
    }
}

/// Log-only `detail` string carrying the probe's node-reported lifecycle for
/// the paired `cluster_reconciler` emit.
fn node_status_label(observation: ProbeObservation) -> String {
    format!(
        "node_status={}",
        observation
            .node_status
            .map(|status| status.as_str())
            .unwrap_or("unknown")
    )
}

/// `reason` for a failed-cleanup `deferred` outcome: why the probe verdict
/// forbids retiring this candidate's execution.
fn failed_cleanup_defer_reason(observation: ProbeObservation) -> &'static str {
    match (observation.verdict, observation.node_status) {
        (ProbeVerdict::AliveOnNode, Some(SandboxLifecycle::Failed)) => "vm_alive_status_failed",
        (ProbeVerdict::AliveOnNode, _) => "vm_alive_status_unknown",
        _ => "probe_verdict_forbids_retire",
    }
}

/// Emit a sandbox event for the command-ready-pending reaper.
///
/// A *reap* is a real lifecycle transition (`started` -> `failed`), so it
/// carries the `LifecycleTransitionTimedOut` + `Cleanup*` shape, with
/// `from_state = "started"` and `to_state = "failed"`. A *heal*
/// ([`SandboxEventType::GuestReady`]) only clears the readiness marker — the
/// sandbox stays `started` — so it records `lifecycle_state = "started"` with
/// `result = "healed"` and never claims a `failed` transition.
fn emit_command_ready_pending_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    pending_age: Duration,
    result: Option<&str>,
    error: Option<String>,
) {
    let mut details = SandboxEventDetails::default()
        .reason("command_ready_pending_reap")
        .operation("command_ready_pending_sweep")
        .trigger(SandboxEventTrigger::System)
        .with_from_state("started")
        // The row's *actual* time-since-pending, not the config threshold, so a
        // reconstructed timeline shows how long each row really sat pending.
        .age_secs(pending_age.as_secs().to_string());
    match event_type {
        SandboxEventType::GuestReady => details = details.lifecycle_state("started"),
        SandboxEventType::LifecycleTransitionTimedOut => {
            details = details.target_state("failed").with_to_state("failed");
        }
        // The reap cleanup sequence. Listed explicitly (rather than a `_`
        // catch-all) so this helper's legal event set stays visible: adding
        // a new event type to a call site forces a decision here.
        SandboxEventType::CleanupStarted
        | SandboxEventType::CleanupCompleted
        | SandboxEventType::CleanupFailed => details = details.lifecycle_state("failed"),
        // The reap was rolled back: this one runs `failed` -> `started`, the
        // opposite direction from every other event here, so it has to
        // override the helper's `from_state` rather than inherit it.
        SandboxEventType::LifecycleTransitionRolledBack => {
            details = details
                .with_from_state("failed")
                .with_to_state("started")
                .lifecycle_state("started")
                .target_state("started");
        }
        other => {
            debug_assert!(
                false,
                "unexpected command-ready-pending event type: {other:?}"
            );
            details = details.lifecycle_state("failed");
        }
    }
    if let Some(result) = result {
        details = details.result(result);
    }
    if let Some(error) = error {
        details = details.error(error);
    }
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    // Every failed-phase event emitted here follows the caller's WARN outcome
    // log for the same failure.
    .emit_with(details.logged_by_caller());
}

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);
/// Default age threshold applied to every sweepable lifecycle in the
/// absence of a per-lifecycle override. 10 minutes covers warm-pool cold
/// starts and an end-to-end suspend/restore round-trip with comfortable
/// headroom while still bounding wedges below the 1-hour mark that drives
/// the worst on-call escalations.
pub(crate) const DEFAULT_AGE_SECS: u64 = 600;
pub(crate) const DEFAULT_FAILED_CLEANUP_BATCH_SIZE: i64 = 100;
/// Bounded per-node `SandboxState` probe deadline. The probe is a read-only
/// metadata RPC against the node-api (which answers from the on-node sbx-mgr
/// without touching the guest), so this only needs to exceed connect jitter.
/// Worst case per node is ~2x this value (bounded connect + bounded RPC).
pub(crate) const DEFAULT_SWEEP_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Default age before a preserved `started` + `command_ready_pending` row is
/// probed. Generous (15 min) on purpose: a legitimately slow guest boot and
/// any client retry have long since resolved by then, so only a sandbox that
/// has sat unaccessed this long with its readiness marker still set becomes a
/// heal/reap candidate. The probe (not the age) decides heal-vs-reap; the age
/// just keeps a transient node-mgr blip on a fresh row from ever reaching it.
pub(crate) const DEFAULT_COMMAND_READY_PENDING_AGE_SECS: u64 = 900;
/// Bounded per-row guest readiness probe deadline. A healthy guest answers
/// `SpacedWaitReady` near-instantly once `started`, so this only needs to
/// exceed node-mgr RPC connect jitter — not a cold boot.
pub(crate) const DEFAULT_COMMAND_READY_PENDING_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Terminal age past which a still-pending row that probes `Unreachable` is
/// reaped rather than deferred forever. A `started` + `command_ready_pending`
/// row that the reaper has been unable to confirm ready for this long — far
/// longer than the probe age, ~360 sampling ticks at the default tick — is a
/// dead pod the orphaned-sandbox reconciler won't collect (it treats a
/// Running+IP pod as present), not a transient blip: a healthy reachable guest
/// would have been *healed* (marker cleared) on one of those ticks. Reaping
/// requires *both* this age **and** a current `Unreachable` probe, so a
/// reachable-but-idle sandbox is never reaped on age alone.
pub(crate) const DEFAULT_COMMAND_READY_PENDING_TERMINAL_AGE_SECS: u64 = 3600;
/// Minimum time since the last placement change before a provably-
/// unrestorable `restoring` row (no source node, no eligible snapshot, no
/// in-flight upload — see `store::unrestorable_restoring_guards`) is failed.
/// The predicate itself is the primary guard; this window is belt-and-braces
/// against enqueue latency and unmodeled transients, so it only needs to
/// comfortably exceed suspend→backup-enqueue time (seconds).
pub(crate) const DEFAULT_UNRESTORABLE_RESTORING_AGE_SECS: u64 = 300;
/// Default restoring-episode deadline: how long a `restoring` row may go,
/// from the claim that started its episode, while its active execution has no
/// pod IP, before the sweep may revert it even under a live retry.
///
/// Priced against both failure modes. A restore pod has no IP until its node
/// has loaded the snapshot and resumed the VM: the shim makes that start
/// inside the pod sandbox's creation, and kubelet publishes the IP only once
/// that returns. So the deadline has to clear the slowest healthy load,
/// including one that first downloads the snapshot from durable storage, and
/// reverting one wastes that load and starts a fresh restore from the same
/// snapshot. Every minute past it holds a node slot for a restore that won't
/// finish. Half an hour is three default `restoring` ages and six
/// restore-ownership windows, far past any load a healthy node takes, so a
/// slow episode gets several full retries first. It's a budget for the whole
/// episode, not for each attempt: time a failed attempt spent counts against
/// the attempt after it.
pub(crate) const DEFAULT_RESTORING_EPISODE_DEADLINE_SECS: u64 = 1800;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Default `false`: every environment opts in via
    /// `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED=true`.
    pub enabled: bool,
    pub tick: Duration,
    /// Default age threshold used for any sweepable lifecycle without an
    /// explicit override. Always positive — `PositiveDuration` carries the
    /// non-zero invariant at the type level, and the env parser falls
    /// back to [`DEFAULT_AGE_SECS`] for unparseable / zero values.
    pub default_age: PositiveDuration,
    /// Per-lifecycle override. `Some(age)` overrides the default for that
    /// lifecycle. `None` falls back to [`Self::default_age`]. The env
    /// parser maps `0` to `Some(None)`-style disabled (we model that
    /// directly via [`LifecycleAge::Disabled`]) so `None` here is
    /// strictly "use the default".
    pub creating_age: LifecycleAge,
    pub starting_age: LifecycleAge,
    pub pausing_age: LifecycleAge,
    pub resuming_age: LifecycleAge,
    pub suspending_age: LifecycleAge,
    pub restoring_age: LifecycleAge,
    pub stopping_age: LifecycleAge,
    pub checkpointing_age: LifecycleAge,
    /// Per-node `SandboxState` probe deadline for the verify-before-destroy
    /// sweep and the failed-cleanup probe gate.
    pub probe_timeout: Duration,
    /// When true, retry cleanup for authoritative `failed` k8s sandboxes
    /// that still have an active `sandbox_k8s_executions` row. This is
    /// intentionally independent from `enabled`: operators can run only
    /// failed cleanup without enabling transient lifecycle age-out.
    pub failed_cleanup_enabled: bool,
    pub failed_cleanup_batch_size: i64,
    /// When true, probe-and-resolve preserved `started` +
    /// `command_ready_pending` rows older than
    /// [`Self::command_ready_pending_age`]. Independent of `enabled` so an
    /// operator can run only this reaper. Default `false`.
    pub command_ready_pending_enabled: bool,
    /// Minimum age (since the row entered `started` with the marker set) before
    /// a `command_ready_pending` row is probed. Generous by design — see
    /// [`DEFAULT_COMMAND_READY_PENDING_AGE_SECS`].
    pub command_ready_pending_age: PositiveDuration,
    /// Per-row bounded guest readiness probe deadline.
    pub command_ready_pending_probe_timeout: Duration,
    /// Age past which a still-pending row that probes `Unreachable` is reaped
    /// instead of deferred forever — the terminal backstop for a dead pod the
    /// orphaned-sandbox reconciler won't collect. See
    /// [`DEFAULT_COMMAND_READY_PENDING_TERMINAL_AGE_SECS`].
    pub command_ready_pending_terminal_age: PositiveDuration,
    /// When true (the default), fail markerless `restoring` rows the restore
    /// planner provably cannot restore from anywhere — no source node, no
    /// eligible snapshot, no in-flight upload. An active or recently expired
    /// restore lease defers the ordinary aged-transient sweep, while legacy
    /// retries may also refresh `lifecycle_changed_at`, so this gets its own
    /// probe-free phase.
    /// Gated behind [`Self::enabled`] like the aged-transient sweep; this
    /// knob is the phase-specific kill switch.
    pub unrestorable_restoring_enabled: bool,
    /// Minimum time since the last placement change before an unrestorable
    /// `restoring` row is failed. See
    /// [`DEFAULT_UNRESTORABLE_RESTORING_AGE_SECS`].
    pub unrestorable_restoring_min_placement_age: PositiveDuration,
    /// Whether the revert to `suspended` retires the abandoned restore's
    /// execution (`SPACE_ABANDONED_RESTORE_RETIREMENT_ENABLED`, default off;
    /// see [`AbandonedRestoreRetirement`]). Disabled, the revert only moves
    /// the lifecycle and the cleanup retires the execution afterwards,
    /// refusing a pod that came up since the candidate was observed.
    pub abandoned_restore_retirement: AbandonedRestoreRetirement,
    /// How long a Kubernetes `restoring` episode may run while its active
    /// execution has no pod IP before the sweep may revert it to `suspended`,
    /// however recently a client retry renewed its restore lease and
    /// activation attempt (`SPACE_TRANSIENT_LIFECYCLE_SWEEPER_RESTORING_EPISODE_DEADLINE_SECS`,
    /// default [`DEFAULT_RESTORING_EPISODE_DEADLINE_SECS`], `0` turns it off).
    /// Measured from `lifecycle_changed_at`, which re-drives don't move, and
    /// effective only past the `restoring` age too. See
    /// `restoring_episode::restoring_episode_overdue`.
    pub restoring_episode_deadline: Option<PositiveDuration>,
}

/// Per-lifecycle override resolution.
///
/// We can't reuse `Option<PositiveDuration>` because we already use `None`
/// here to mean "use the default". Modeling the closed set explicitly
/// keeps the disabled case distinct from the "no override" case and
/// satisfies the codebase's enums-over-strings convention for closed-set
/// vocabularies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAge {
    /// Use [`ReconcilerConfig::default_age`].
    UseDefault,
    /// Override the default for this lifecycle.
    Override(PositiveDuration),
    /// Skip this lifecycle entirely.
    Disabled,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            default_age: PositiveDuration::new_panic(Duration::from_secs(DEFAULT_AGE_SECS)),
            creating_age: LifecycleAge::UseDefault,
            starting_age: LifecycleAge::UseDefault,
            pausing_age: LifecycleAge::UseDefault,
            resuming_age: LifecycleAge::UseDefault,
            suspending_age: LifecycleAge::UseDefault,
            restoring_age: LifecycleAge::UseDefault,
            stopping_age: LifecycleAge::UseDefault,
            checkpointing_age: LifecycleAge::UseDefault,
            probe_timeout: DEFAULT_SWEEP_PROBE_TIMEOUT,
            failed_cleanup_enabled: true,
            failed_cleanup_batch_size: DEFAULT_FAILED_CLEANUP_BATCH_SIZE,
            command_ready_pending_enabled: false,
            command_ready_pending_age: PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_COMMAND_READY_PENDING_AGE_SECS,
            )),
            command_ready_pending_probe_timeout: DEFAULT_COMMAND_READY_PENDING_PROBE_TIMEOUT,
            command_ready_pending_terminal_age: PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_COMMAND_READY_PENDING_TERMINAL_AGE_SECS,
            )),
            unrestorable_restoring_enabled: true,
            unrestorable_restoring_min_placement_age: PositiveDuration::new_panic(
                Duration::from_secs(DEFAULT_UNRESTORABLE_RESTORING_AGE_SECS),
            ),
            abandoned_restore_retirement: AbandonedRestoreRetirement::Disabled,
            restoring_episode_deadline: Some(PositiveDuration::new_panic(Duration::from_secs(
                DEFAULT_RESTORING_EPISODE_DEADLINE_SECS,
            ))),
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.transientLifecycleSweeper.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config
                .truthy_default("SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED", default.enabled),
            tick: config.duration_secs("SPACE_TRANSIENT_LIFECYCLE_SWEEPER_TICK_SECS", default.tick),
            default_age: env
                .var("SPACE_TRANSIENT_LIFECYCLE_SWEEPER_DEFAULT_AGE_SECS")
                .ok()
                .and_then(|raw| raw.parse::<u64>().ok())
                .and_then(|n| PositiveDuration::new(Duration::from_secs(n)))
                .unwrap_or(default.default_age),
            // Unset keeps the default age, as before this knob existed, so a
            // deployment that ages `creating` out on the default keeps doing
            // so. A deployment whose default is effectively "never" opts in
            // here.
            creating_age: env_lifecycle_age(
                env,
                "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CREATING_AGE_SECS",
            ),
            starting_age: default.starting_age,
            pausing_age: default.pausing_age,
            resuming_age: default.resuming_age,
            suspending_age: env_lifecycle_age(
                env,
                "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_SUSPENDING_AGE_SECS",
            ),
            restoring_age: env_lifecycle_age(
                env,
                "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_RESTORING_AGE_SECS",
            ),
            stopping_age: default.stopping_age,
            checkpointing_age: env_lifecycle_age(
                env,
                "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CHECKPOINTING_AGE_SECS",
            ),
            probe_timeout: default.probe_timeout,
            // Ships enabled and retires failed rows, so a malformed value must
            // disable it rather than leave it running.
            failed_cleanup_enabled: config.destructive_gate("SPACE_FAILED_SANDBOX_CLEANUP_ENABLED"),
            failed_cleanup_batch_size: default.failed_cleanup_batch_size,
            command_ready_pending_enabled: false,
            command_ready_pending_age: default.command_ready_pending_age,
            command_ready_pending_probe_timeout: default.command_ready_pending_probe_timeout,
            command_ready_pending_terminal_age: default.command_ready_pending_terminal_age,
            unrestorable_restoring_enabled: true,
            unrestorable_restoring_min_placement_age: default
                .unrestorable_restoring_min_placement_age,
            abandoned_restore_retirement: AbandonedRestoreRetirement::from_env(),
            restoring_episode_deadline: config.optional_positive_duration_secs(
                "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_RESTORING_EPISODE_DEADLINE_SECS",
                default.restoring_episode_deadline,
            ),
        }
    }

    /// Resolve the effective age threshold for `lifecycle`. Returns `None`
    /// if the lifecycle is disabled, otherwise the override or the
    /// default.
    pub(crate) fn resolve_age(&self, lifecycle: SandboxLifecycle) -> Option<PositiveDuration> {
        // Exhaustive match on the closed `SandboxLifecycle` set: adding a
        // new transient state to the enum breaks compilation here, forcing
        // an explicit decision about whether the sweeper should bound it.
        // The non-sweepable arm groups every terminal / stable / mid-
        // delete state and returns `None` so callers can't accidentally
        // age out a `Started` or `Paused` row.
        let entry = match lifecycle {
            SandboxLifecycle::Creating => self.creating_age,
            SandboxLifecycle::Starting => self.starting_age,
            SandboxLifecycle::Pausing => self.pausing_age,
            SandboxLifecycle::Resuming => self.resuming_age,
            SandboxLifecycle::Suspending => self.suspending_age,
            SandboxLifecycle::Restoring => self.restoring_age,
            SandboxLifecycle::Stopping => self.stopping_age,
            SandboxLifecycle::Checkpointing => self.checkpointing_age,
            SandboxLifecycle::Created
            | SandboxLifecycle::Started
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Suspended
            | SandboxLifecycle::Stopped
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Failed => return None,
        };
        match entry {
            LifecycleAge::Disabled => None,
            LifecycleAge::UseDefault => Some(self.default_age),
            LifecycleAge::Override(age) => Some(age),
        }
    }
}

/// Parse a per-lifecycle override. `0` -> [`LifecycleAge::Disabled`];
/// positive integer -> [`LifecycleAge::Override`]; unset or unparseable ->
/// [`LifecycleAge::UseDefault`] (so a typo doesn't silently disable a
/// lifecycle).
fn env_lifecycle_age(
    env: &dyn pplx_space_common_core::env::Environment,
    var: &str,
) -> LifecycleAge {
    match env.var(var).ok().and_then(|raw| raw.parse::<u64>().ok()) {
        Some(0) => LifecycleAge::Disabled,
        Some(n) => PositiveDuration::new(Duration::from_secs(n))
            .map(LifecycleAge::Override)
            .unwrap_or(LifecycleAge::UseDefault),
        None => LifecycleAge::UseDefault,
    }
}

/// A revert to `suspended` the CAS claimed, with what undoing it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevertedToSuspended {
    /// Names the exact lifecycle write the revert made.
    pub write_token: String,
    /// The row's `placement_changed_at` before the revert cleared it.
    pub prior_placement_changed_at: Option<time::OffsetDateTime>,
}

/// A `failed` row with an active Kubernetes execution that the
/// failed-cleanup pass wants to retire, plus the node to probe first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedCleanupCandidate {
    pub sandbox_id: SandboxId,
    /// `k8s_node_name` from the active execution row; `None` when the pod
    /// never reached a node.
    pub node_name: Option<String>,
    /// Whether the execution's first activation (for a cold create, the
    /// create) finished, and if not whether its owner is gone.
    pub first_activation: FirstActivation,
    /// What the listing observed of that execution, so the retire can refuse
    /// if the sandbox moved on before the probe and the retire ran.
    pub pod_fence: RetireFence,
}

/// What the failed-cleanup listing saw of an execution's first activation
/// (for a cold create, the create itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstActivation {
    /// It finished (the execution reached `running`), or the execution holds no
    /// lease at all: no attempt and no expiry. The row may be repaired to
    /// `started` when its node reports the VM started.
    Finished,
    /// It never reached `running` and still holds an activation attempt (the
    /// route's or a later one) whose lease hasn't expired: its owner may still be
    /// working. Never repaired to `started` (nothing routable to reach it at), and
    /// not retired while the VM is alive; deferred until the lease expires.
    /// An attempt with no expiry counts as live, as in the sweep's owner
    /// check; no writer produces one.
    InFlight,
    /// As `InFlight`, but the lease has expired. The row is `failed`, so the
    /// create can't finish even if its owner is still working. Retired even
    /// with its VM alive, never repaired to `started`.
    ///
    /// It also covers an activation its last owner gave up without
    /// dispatching it (`give_up_activation_attempt_best_effort`): no attempt,
    /// but the expiry the owner cut to when it gave up, now passed.
    Abandoned,
}

impl FirstActivation {
    /// From the listing's `first_activation_unfinished` and
    /// `first_activation_abandoned` columns.
    pub(crate) fn from_columns(unfinished: bool, abandoned: bool) -> Self {
        match (unfinished, abandoned) {
            (_, true) => Self::Abandoned,
            (true, false) => Self::InFlight,
            (false, false) => Self::Finished,
        }
    }
}

/// A preserved `started` row whose `command_ready_pending` marker has stuck,
/// as of the reaper's candidate select.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandReadyPendingCandidate {
    pub sandbox_id: SandboxId,
    /// How long the marker has been set (from `lifecycle_changed_at`).
    pub pending_age: Duration,
    /// What the listing observed of the active execution. The reaper flips a
    /// row that still reads `started` to `failed`, so its retire is the most
    /// exposed of the three: the guest probe it acts on says nothing about the
    /// pod, and a create that finally converged between probe and retire looks
    /// identical to one that never will.
    pub pod_fence: RetireFence,
}

/// What the fail CAS re-checks beyond the candidate select's guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailGuard {
    /// Only the candidate select's guards.
    Aged,
    /// Also that no activation-completion receipt can still finish the row:
    /// its active execution isn't both routable and holding an attempt. The
    /// sweep fails an alive abandoned create unless a receipt can still move
    /// it to `started`, which takes a routable execution and the attempt the
    /// receipt matches on. So a pod IP committed between the select and the
    /// CAS (a read of the pod records one) makes the CAS claim nothing while
    /// an attempt is held, and an attempt claimed in between does too while
    /// the execution is routable. An owner that gave its activation up holds
    /// no attempt, so a routable row it left behind still fails. The pod-IP
    /// write doesn't take the sandbox row lock, so one that lands after the
    /// CAS, before the retire, isn't caught: the retire's pod fence checks the
    /// pod's identity, not its IP.
    NoPendingReceipt,
}

/// DB-side surface the sweeper depends on. Implementations:
/// [`SubstrateSweepSink`] for production (Postgres + Substrate);
/// `tests::FakeSink` for unit tests.
pub trait SweepSink: Send + Sync + 'static {
    /// List rows stuck in `lifecycle` past `age` — **without** flipping
    /// anything. Implementations must exempt evacuation-marked `restoring`
    /// rows (node-drain re-drives those; marked `suspending` rows are
    /// intentionally not exempt — see the production impl) and resolve each
    /// candidate's node from its active `sandbox_k8s_executions` row, not
    /// `sandboxes.active_node_id` (NULL for this population).
    fn list_aged_transient(
        &self,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<SweepCandidate>, sqlx::Error>> + Send;

    /// Batch-probe `sandbox_ids` on one node, bounded by `probe_timeout`.
    /// Read-only. Ids the node did not answer for may be absent from the
    /// map; the caller treats them as [`ProbeVerdict::Inconclusive`].
    fn probe_node(
        &self,
        node_name: &str,
        sandbox_ids: &[SandboxId],
        probe_timeout: Duration,
    ) -> impl std::future::Future<
        Output = Result<HashMap<SandboxId, ProbeObservation>, ProbeNodeError>,
    > + Send;

    /// Lease-guarded CAS: converge an aged `restoring` row whose VM the
    /// probe found alive to `target` (its wake target). The CAS re-checks
    /// `lifecycle = 'restoring'`, the age window, that no unexpired restore
    /// lease exists (an active restore attempt owns the row and will
    /// finalize it itself), **and** that the active execution is routable at
    /// flip time (running + pod IP, or a non-k8s substrate) — the act-phase
    /// routability proof is stale the moment a concurrent wake replaces the
    /// active execution, so the wake-target contract must be enforced inside
    /// the CAS itself. It also applies the activation-owner check (see
    /// "Owner check" in the module docs) under the sandbox row lock. Returns
    /// whether this call claimed the row.
    fn converge_restoring(
        &self,
        sandbox_id: SandboxId,
        target: SandboxLifecycle,
        age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Verify — and repair, when a live matching pod exists — routability of
    /// the row's active execution, for an alive `restoring` row the DB
    /// reported unroutable. Called by the act phase before such a row may
    /// converge to `started` (a `started` row without a routable execution
    /// wedges behind the wake path's gen≥2 fresh-pod refusal).
    ///
    /// Contract: a read-only authoritative pod lookup plus, at most, the
    /// idempotent generation-guarded pod-metadata hydration the wake path
    /// itself uses. Never flips `sandboxes.lifecycle`, never writes
    /// placement, never creates or deletes pods. `Routable` means the
    /// converge CAS may proceed; `NotRoutable` means defer. Errors (kube
    /// apiserver unreachable, DB error) roll the row to the next tick.
    /// `probe_timeout` bounds the substrate lookup.
    fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> impl std::future::Future<Output = Result<ExecutionRoutability, String>> + Send;

    /// Guarded CAS: revert an aged **`restoring`** row with no live VM to
    /// `suspended`, re-checking `(lifecycle, age window)`, that no active or
    /// recently expired restore lease owns the row, **and** that a ready
    /// durable snapshot still exists under the suspended-revert-safe restore
    /// predicate. A row must never read `suspended` without a restorable
    /// snapshot behind it. The
    /// implementation hard-pins `lifecycle = 'restoring'`: every other
    /// transient lifecycle's matching backup is a stale prior-cycle copy
    /// (see `classify::decide`), so the CAS must claim nothing even if
    /// classification drifts.
    ///
    /// With abandoned-restore retirement enabled, the same transaction
    /// retires the restore's execution as `replaced`, matching `fence`'s
    /// recorded identity (or, for a candidate observed with no execution,
    /// requires that none exists). That retirement is what records the
    /// restore as abandoned, so the revert claims nothing unless it lands
    /// too. Disabled (the default, see
    /// [`ReconcilerConfig::abandoned_restore_retirement`]), the revert moves
    /// only the lifecycle, and the cleanup retires the execution afterwards
    /// under `fence`.
    ///
    /// A retiring revert also clears `placement_changed_at`, as the API
    /// store's markerless restoring recovery does. A restore whose pod bound
    /// moved it past the pre-restore snapshot, and both the suspended restore
    /// selector and `durably_restorable` take a markerless snapshot only when
    /// it is newer than the last placement: left in place, the proven source isn't
    /// selected, and a fenced release of the abandoned pod's runtime is held
    /// until the next suspend.
    ///
    /// Like the converge CAS, it applies the activation-owner check under the
    /// sandbox row lock.
    ///
    /// Returns what the write needs to be undone, or `None` if it claimed
    /// nothing.
    fn revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        fence: &RetireFence,
    ) -> impl std::future::Future<Output = Result<Option<RevertedToSuspended>, sqlx::Error>> + Send;

    /// CAS one provably-unrecoverable row from `lifecycle` to `'failed'`,
    /// with the same guards as the candidate select, including the
    /// activation-owner check for an activation-owned lifecycle, which it
    /// applies under the sandbox row lock, plus whatever `guard` adds.
    /// Returns whether this call claimed the row; cleanup runs only on
    /// claimed rows.
    fn fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        guard: FailGuard,
    ) -> impl std::future::Future<Output = Result<Option<String>, sqlx::Error>> + Send;

    /// Undo a fail this sweep just made, when the retire it was supposed to
    /// enable turned out to be refused.
    ///
    /// Third of the same shape, and the least recoverable without it: nothing
    /// walks a `failed` row back. Pause completion refuses to overwrite
    /// `failed`, and the failed-cleanup pass repairs only node-reported
    /// `started` sandboxes, so an aged `pausing` row whose pause finished
    /// during the sweep would be represented as failed forever while its
    /// execution runs on. Bound to the fail CAS's own write.
    fn undo_fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        write_token: &str,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// List markerless `restoring` rows the restore planner provably cannot
    /// restore from anywhere — unplaced, no execution history naming a node,
    /// no snapshot in the restore-retry window, no in-flight suspend upload —
    /// paired with time since their last placement change. Aged on
    /// `placement_changed_at`, **not** `lifecycle_changed_at`: restore claims
    /// preserve the latter as the source-selection boundary, so it is not an
    /// activity clock; legacy rows may also carry prior lifecycle-clock
    /// refreshes. See `store::unrestorable_restoring_guards` for the full
    /// predicate and the deliberate absence of a lease guard.
    fn list_unrestorable_restoring(
        &self,
        min_placement_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<(SandboxId, Duration)>, sqlx::Error>> + Send;

    /// CAS one provably-unrestorable `restoring` row to `'failed'`,
    /// re-checking the full source-absence predicate atomically in the
    /// UPDATE — a source that appeared between select and act makes this
    /// claim nothing. Returns whether this call claimed the row.
    fn fail_unrestorable_restoring(
        &self,
        sandbox_id: SandboxId,
        min_placement_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Best-effort substrate cleanup for a sandbox the sweep just
    /// flipped to `'failed'` or reverted to `'suspended'`. Tears down any
    /// backing pod the wedged transition left behind while preserving
    /// node-local sandbox state (filesystem and local-artifact rows): a row
    /// that failed mid-suspend may hold the only up-to-date copy of the
    /// sandbox on the node, so the sweep retires the execution rather than
    /// deleting the sandbox. On the revert path this also restores the
    /// `suspended` row shape (execution finished and sandbox unplaced) that
    /// the normal suspend completion produces.
    /// Errors are surfaced for metrics/logging but the sweep does not
    /// retry — the row has already reached its resting lifecycle.
    ///
    /// `pod_fence` is the pod identity the candidate select observed. The
    /// probe and the CAS in between are both point-in-time, so the substrate
    /// re-checks that identity and answers
    /// [`RetireExecutionOutcome::PodIdentityChanged`] — nothing torn down —
    /// when the sandbox moved on. That is a defer, not a failure.
    fn cleanup_after_sweep(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
    ) -> impl std::future::Future<Output = Result<RetireExecutionOutcome, String>> + Send;

    /// Undo a revert this sweep just made, when the cleanup it was supposed
    /// to enable turned out to be refused.
    ///
    /// With abandoned-restore retirement disabled (the default), the revert
    /// moved only the lifecycle and the cleanup retires the execution under
    /// the candidate's fence. A pod that came up after the pre-check, which a
    /// Pending pod going Running is enough for, fails that fence and leaves a
    /// `suspended` row owning a live execution, which no reconciler revisits;
    /// the restore the sweep judged dead is running after all, so putting the
    /// lifecycle back is simply correct.
    ///
    /// With retirement enabled, the revert retired the restore's execution in
    /// its own transaction, so the cleanup refuses only when an execution it
    /// never observed is active by the time it runs. Putting the lifecycle
    /// back lets the next tick judge the row afresh; the retired execution
    /// stays `replaced`, since a restore never re-adopts a retired
    /// generation. A pod that merely came up after the pre-check isn't a
    /// refusal then: the cleanup deletes it as the abandoned restore's
    /// leftover (see `candidate_pod_fence_with_observation`).
    ///
    /// CAS-guarded on the row still being `suspended`, so a genuine suspend
    /// that landed since is not clobbered. `reverted` is what the revert CAS
    /// returned. Its write token names that
    /// exact lifecycle write: undoing on the lifecycle value alone would reach
    /// past this sweep's own transition and drag a later, legitimate suspend
    /// back to `restoring`. Its prior `placement_changed_at` is put back, so
    /// the restored `restoring` row keeps the placement clock its restore
    /// retry window reads.
    ///
    /// `preserve_local_artifact` is the candidate's `fork_restore`, restored
    /// alongside the lifecycle because leaving `restoring` cleared it. Without
    /// it a fork restore comes back missing the contract that keeps its local
    /// artifact, which is data loss rather than a stale flag.
    fn undo_revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        reverted: &RevertedToSuspended,
        preserve_local_artifact: bool,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Undo a reap this sweep just made, when the retire it was supposed to
    /// enable turned out to be refused.
    ///
    /// Symmetric to [`Self::undo_revert_to_suspended`], and needed for a
    /// sharper reason: the flip to `failed` fires a trigger that clears
    /// `command_ready_pending`, and nothing puts it back. A still-wedged guest
    /// left without it never enters this reaper again, whether the row stays
    /// `failed` or the failed-cleanup pass repairs it to `started`. Restores
    /// both, bound to the reap's own write.
    fn undo_reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
        write_token: &str,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Whether a [`Self::cleanup_after_sweep`] carrying this fence would be
    /// permitted right now, without tearing anything down.
    ///
    /// Not a pure read: the substrate may record the UID of a pod it found but
    /// could not fence against, so the next attempt has an enforceable fence.
    /// That write cannot land on a successor and changes nothing this sweep
    /// would have to undo.
    ///
    /// The revert path needs this because its lifecycle CAS is destructive and
    /// irreversible in practice: `suspended` is a resting lifecycle that no
    /// reconciler revisits, so committing it and only then discovering the
    /// fence was lost leaves a dormant row owning a live execution. Asking
    /// first means a candidate whose pod recovered is deferred with its
    /// lifecycle untouched, and the next tick re-evaluates it from scratch.
    fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> impl std::future::Future<Output = Result<Option<PodFenceMismatch>, String>> + Send;

    /// List authoritative failed k8s sandboxes that still have active
    /// execution rows and need substrate cleanup retried, each with the
    /// node its execution points at (for the probe gate).
    fn list_failed_active_executions(
        &self,
        batch_size: i64,
    ) -> impl std::future::Future<Output = Result<Vec<FailedCleanupCandidate>, sqlx::Error>> + Send;

    /// Re-read a failed-cleanup candidate's first activation, as the listing
    /// classifies it. `None` when the sandbox is no longer `failed` with an
    /// active execution.
    fn first_activation(
        &self,
        sandbox_id: SandboxId,
    ) -> impl std::future::Future<Output = Result<Option<FirstActivation>, sqlx::Error>> + Send;

    /// Rotate the cleanup candidate before attempting deletion so repeated
    /// failures do not monopolize every tick.
    ///
    /// The column it bumps, `updated_at`, is also where the age of a
    /// materialization attempt comes from. That is safe only because a
    /// `failed` sandbox cannot have a create in flight, so nothing reads that
    /// age here; were it read, every rotation would refresh it and an
    /// abandoned create would defer forever.
    fn note_failed_cleanup_attempt(
        &self,
        sandbox_id: SandboxId,
    ) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send;

    /// Same contract as [`Self::cleanup_after_sweep`], for the failed-cleanup
    /// pass's own candidates.
    fn cleanup_failed_execution(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
    ) -> impl std::future::Future<Output = Result<RetireExecutionOutcome, String>> + Send;

    /// CAS-repair a `failed` k8s sandbox that still has an active execution
    /// and whose owning node reports the sandbox as live and `started`.
    /// Returns whether the row was claimed; a concurrent lifecycle transition
    /// or execution cleanup makes this a no-op.
    fn repair_failed_active_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;

    /// List preserved `started` rows whose `command_ready_pending` marker has
    /// stayed set longer than `age` (a slow create returned `started` and no
    /// client retry ever cleared it), each paired with how long it has been
    /// pending. The age lets the caller apply the terminal-unreachable
    /// backstop. Each is a heal-or-reap candidate.
    fn list_command_ready_pending(
        &self,
        age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<CommandReadyPendingCandidate>, sqlx::Error>> + Send;

    /// Probe the guest's command-readiness without recreating its execution.
    /// `Ready` means heal (clear the marker); `Wedged` means reap.
    fn probe_command_ready(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> impl std::future::Future<Output = CommandReadyProbe> + Send;

    /// Clear the `command_ready_pending` marker on a row the probe confirmed
    /// ready. Best-effort: the row stays `started` either way.
    fn heal_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send;

    /// CAS-flip a wedged `started` + `command_ready_pending` row to `failed`,
    /// returning whether this call actually claimed it. The CAS re-checks both
    /// `lifecycle = 'started'` and `command_ready_pending = true`, so a row a
    /// concurrent access just healed (marker cleared) or transitioned out of
    /// `started` drops out and is left alone — substrate cleanup then runs only
    /// on rows we truly claimed. Returns the token naming the lifecycle write
    /// this call made, or `None` if it claimed nothing.
    fn reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> impl std::future::Future<Output = Result<Option<String>, sqlx::Error>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// Rows the fail CAS claimed (flipped to `failed` + retired).
    pub aged_out: usize,
    /// Alive `restoring` rows converged to their wake target.
    pub converged: usize,
    /// Alive `restoring` rows whose execution the DB reported unroutable,
    /// repaired by kube-verified hydration and then converged to their wake
    /// target.
    pub converged_hydrated: usize,
    /// Dead/missing rows with a ready durable snapshot reverted to
    /// `suspended`.
    pub reverted_to_suspended: usize,
    /// Candidates with no provably-safe action this tick.
    pub deferred: usize,
    /// Unrestorable `restoring` rows (no source anywhere) flipped to
    /// `failed` by the probe-free phase.
    pub unrestorable_failed: usize,
    pub cleanup_failures: usize,
    /// Retires the substrate refused because the pod-identity fence no longer
    /// matched (revert, age-out, and command-ready-reap paths). Not failures:
    /// the sandbox changed under the sweep and nothing was torn down.
    pub cleanup_pod_identity_deferred: usize,
    pub failed_cleanup_attempts: usize,
    pub failed_cleanup_failures: usize,
    /// Same, for the failed-cleanup pass. Counted apart from
    /// [`Self::failed_cleanup_deferred`], which is the probe gate refusing to
    /// retire a sandbox whose VM may still be alive — a different decision,
    /// made before the retire is even attempted.
    pub failed_cleanup_pod_identity_deferred: usize,
    /// Failed-row cleanups skipped because the probe could not prove the
    /// sandbox is gone from its node.
    pub failed_cleanup_deferred: usize,
    /// Failed rows repaired back to `started` because the node already
    /// reported a live started sandbox.
    pub failed_cleanup_repaired: usize,
    /// Failed-row repair CASes that lost a race to another transition or
    /// cleanup.
    pub failed_cleanup_repair_cas_lost: usize,
    pub failed_cleanup_repair_failures: usize,
    pub command_ready_healed: usize,
    pub command_ready_reaped: usize,
    pub command_ready_deferred: usize,
}

pub struct Reconciler<S: SweepSink> {
    config: ReconcilerConfig,
    sink: Arc<S>,
}

impl<S: SweepSink> Reconciler<S> {
    pub fn new(config: ReconcilerConfig, sink: S) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
        }
    }

    pub async fn tick(&self) -> TickReport {
        let mut report = TickReport::default();
        if self.config.enabled {
            self.sweep_transient_lifecycles(&mut report).await;
        }
        if self.config.enabled && self.config.unrestorable_restoring_enabled {
            self.sweep_unrestorable_restoring(&mut report).await;
        }
        if self.config.failed_cleanup_enabled {
            self.cleanup_failed_active_executions(&mut report).await;
        }
        if self.config.command_ready_pending_enabled {
            self.sweep_command_ready_pending(&mut report).await;
        }
        report
    }

    /// The verify-before-destroy sweep: select aged candidates without
    /// flipping, probe their nodes (one `SandboxState` call per node,
    /// sequential), classify each row, and apply the one provably-safe
    /// action per row. See the module docs for the decision table.
    async fn sweep_transient_lifecycles(&self, report: &mut TickReport) {
        // SELECT. Track each lifecycle's resolved threshold so the act-phase
        // CASes re-check the same age window the select used.
        let mut ages: HashMap<SandboxLifecycle, PositiveDuration> = HashMap::new();
        let mut candidates: Vec<SweepCandidate> = Vec::new();
        for lifecycle in SWEEPABLE_LIFECYCLES.iter().copied() {
            let Some(age) = self.config.resolve_age(lifecycle) else {
                continue;
            };
            match self.sink.list_aged_transient(lifecycle, age).await {
                Ok(mut rows) => {
                    ages.insert(lifecycle, age);
                    candidates.append(&mut rows);
                }
                Err(err) => {
                    tracing::warn!(
                        lifecycle = %lifecycle,
                        error = %err,
                        "transient_lifecycle_sweeper: list_aged_transient failed; \
                         will retry on next tick"
                    );
                }
            }
        }
        if candidates.is_empty() {
            return;
        }

        // PROBE. The flip to `failed` is lossy (DB triggers erase the
        // evacuation marker and restore lease on that transition), so the
        // probe MUST run before any flip. The fan-out is budgeted to half
        // the tick so a mass-unreachability incident can't stretch the tick
        // unboundedly or let verdicts go stale before the act phase.
        let targets: Vec<(SandboxId, Option<String>)> = candidates
            .iter()
            .map(|candidate| (candidate.sandbox_id, candidate.node_name.clone()))
            .collect();
        let observations = probe::probe_targets(
            self.sink.as_ref(),
            &targets,
            self.config.probe_timeout,
            self.config.tick / 2,
        )
        .await;

        // CLASSIFY + ACT.
        for candidate in candidates {
            let Some(age) = ages.get(&candidate.lifecycle).copied() else {
                // A candidate only exists because its lifecycle resolved an
                // age above; this arm is unreachable but cheap to keep total.
                continue;
            };
            let verdict = observations
                .get(&candidate.sandbox_id)
                .copied()
                .unwrap_or_else(ProbeObservation::inconclusive)
                .verdict;
            match classify::decide(&candidate, verdict) {
                SweepAction::Converge { target } => {
                    self.converge_candidate(&candidate, verdict, target, age, report)
                        .await;
                }
                SweepAction::HydrateThenConverge { target } => {
                    self.hydrate_and_converge_candidate(&candidate, verdict, target, age, report)
                        .await;
                }
                SweepAction::RevertToSuspended => {
                    self.revert_candidate(&candidate, verdict, age, report)
                        .await;
                }
                SweepAction::FailAndRetire => {
                    self.fail_candidate(&candidate, verdict, age, report).await;
                }
                SweepAction::Defer { reason } => {
                    OutcomeEvent::aged_transient(
                        AgedTransientOutcome::Deferred,
                        candidate.lifecycle.as_str(),
                        verdict.as_str(),
                    )
                    .reason(reason.as_str())
                    .sandbox_id(candidate.sandbox_id)
                    .age_secs(candidate.age.as_secs())
                    .info();
                    report.deferred += 1;
                    emit_sweep_verify_event(
                        candidate.sandbox_id,
                        SandboxEventType::SweepDeferred,
                        candidate.lifecycle,
                        None,
                        verdict,
                        candidate.age,
                        reason.as_str(),
                    );
                }
            }
        }
    }

    /// Lease-guarded converge of an alive `restoring` row to its wake
    /// target. The Postgres row was stale, not the sandbox — nothing is
    /// destroyed.
    async fn converge_candidate(
        &self,
        candidate: &SweepCandidate,
        verdict: ProbeVerdict,
        target: SandboxLifecycle,
        age: PositiveDuration,
        report: &mut TickReport,
    ) {
        let claimed = match self
            .sink
            .converge_restoring(candidate.sandbox_id, target, age)
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: converge CAS failed; will retry on next tick"
                );
                return;
            }
        };
        if !claimed {
            // The row moved on, or its restore lease is not quiet long enough.
            OutcomeEvent::aged_transient(
                AgedTransientOutcome::CasLost,
                candidate.lifecycle.as_str(),
                verdict.as_str(),
            )
            .sandbox_id(candidate.sandbox_id)
            .debug();
            return;
        }
        OutcomeEvent::aged_transient(
            AgedTransientOutcome::ConvergedStaleRowToLiveVm(wake_lifecycle(target)),
            candidate.lifecycle.as_str(),
            verdict.as_str(),
        )
        .reason("vm_alive_row_stale")
        .sandbox_id(candidate.sandbox_id)
        .age_secs(candidate.age.as_secs())
        .warn();
        report.converged += 1;
        emit_sweep_verify_event(
            candidate.sandbox_id,
            SandboxEventType::LifecycleConverged,
            candidate.lifecycle,
            Some(target),
            verdict,
            candidate.age,
            "vm_alive_row_stale",
        );
    }

    /// Routability-verified converge of an alive `restoring` row whose
    /// execution the DB reported unroutable. The sink consults the substrate
    /// (an authoritative pod lookup, hydrating the execution row from a live
    /// matching pod when one exists); only a `Routable` answer may run the
    /// converge CAS. `NotRoutable` defers — converging blindly would assert
    /// a `started` the substrate can't back and wedge every subsequent wake
    /// behind the gen≥2 fresh-pod refusal.
    async fn hydrate_and_converge_candidate(
        &self,
        candidate: &SweepCandidate,
        verdict: ProbeVerdict,
        target: SandboxLifecycle,
        age: PositiveDuration,
        report: &mut TickReport,
    ) {
        match self
            .sink
            .ensure_routable_execution(candidate.sandbox_id, self.config.probe_timeout)
            .await
        {
            Ok(ExecutionRoutability::Routable) => {}
            Ok(ExecutionRoutability::NotRoutable) => {
                // The wake-wedge signature: node-side restore completed (the
                // VM is alive) but nothing hydrated the execution row, so the
                // row must stay `restoring` (wake-recoverable) until a pod
                // exists or the VM retires into the revert path.
                OutcomeEvent::aged_transient(
                    AgedTransientOutcome::Deferred,
                    candidate.lifecycle.as_str(),
                    verdict.as_str(),
                )
                .reason(classify::DeferReason::AliveNotRoutable.as_str())
                .sandbox_id(candidate.sandbox_id)
                .age_secs(candidate.age.as_secs())
                .warn();
                report.deferred += 1;
                emit_sweep_verify_event(
                    candidate.sandbox_id,
                    SandboxEventType::SweepDeferred,
                    candidate.lifecycle,
                    None,
                    verdict,
                    candidate.age,
                    classify::DeferReason::AliveNotRoutable.as_str(),
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: routability check failed; will retry \
                     on next tick"
                );
                return;
            }
        }
        let claimed = match self
            .sink
            .converge_restoring(candidate.sandbox_id, target, age)
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: converge CAS failed after hydration; \
                     will retry on next tick"
                );
                return;
            }
        };
        if !claimed {
            OutcomeEvent::aged_transient(
                AgedTransientOutcome::CasLost,
                candidate.lifecycle.as_str(),
                verdict.as_str(),
            )
            .sandbox_id(candidate.sandbox_id)
            .debug();
            return;
        }
        OutcomeEvent::aged_transient(
            AgedTransientOutcome::ConvergedAfterHydratingExecution(wake_lifecycle(target)),
            candidate.lifecycle.as_str(),
            verdict.as_str(),
        )
        .reason("vm_alive_hydrated_execution")
        .sandbox_id(candidate.sandbox_id)
        .age_secs(candidate.age.as_secs())
        .warn();
        report.converged_hydrated += 1;
        emit_sweep_verify_event(
            candidate.sandbox_id,
            SandboxEventType::LifecycleConverged,
            candidate.lifecycle,
            Some(target),
            verdict,
            candidate.age,
            "vm_alive_hydrated_execution",
        );
    }

    /// Guarded revert of a dead/missing row with a ready durable snapshot to
    /// `suspended`, so the next restore recovers it instead of losing it.
    async fn revert_candidate(
        &self,
        candidate: &SweepCandidate,
        verdict: ProbeVerdict,
        age: PositiveDuration,
        report: &mut TickReport,
    ) {
        // Ask before committing (a UID backfill aside, this changes nothing).
        // The revert below moves the sandbox to `suspended`, which no
        // reconciler revisits. Its transaction re-checks the execution row's
        // recorded identity, but only this check can see the live pod: a pod
        // replaced or moved on since the candidate select must stop the
        // revert here.
        match self
            .sink
            .retire_fence_mismatch(candidate.sandbox_id, &candidate.pod_fence)
            .await
        {
            Ok(None) => {}
            Ok(Some(mismatch)) => {
                note_cleanup_pod_identity_deferral(
                    candidate.sandbox_id,
                    CleanupPass::RevertToSuspended,
                    candidate.lifecycle.as_str(),
                    &candidate.pod_fence,
                    mismatch,
                    report,
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: could not re-check the pod fence before \
                     reverting; deferring to the next tick rather than committing a revert \
                     that may not be followed by a retire"
                );
                return;
            }
        }

        let reverted = match self
            .sink
            .revert_to_suspended(
                candidate.sandbox_id,
                candidate.lifecycle,
                age,
                &candidate.pod_fence,
            )
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: revert CAS failed; will retry on next tick"
                );
                return;
            }
        };
        let Some(reverted) = reverted else {
            OutcomeEvent::aged_transient(
                AgedTransientOutcome::CasLost,
                candidate.lifecycle.as_str(),
                verdict.as_str(),
            )
            .sandbox_id(candidate.sandbox_id)
            .debug();
            return;
        };
        let reason = restoring_episode::revert_reason(candidate);
        OutcomeEvent::aged_transient(
            AgedTransientOutcome::RevertedToDurableSnapshot,
            candidate.lifecycle.as_str(),
            verdict.as_str(),
        )
        .reason(reason)
        .sandbox_id(candidate.sandbox_id)
        .age_secs(candidate.age.as_secs())
        .warn();
        report.reverted_to_suspended += 1;
        emit_sweep_verify_event(
            candidate.sandbox_id,
            SandboxEventType::LifecycleRevertedToSuspended,
            candidate.lifecycle,
            Some(SandboxLifecycle::Suspended),
            verdict,
            candidate.age,
            reason,
        );

        // The revert already retired the restore's execution, so what's left
        // of the wedged restore is its pod and the row's placement:
        // `suspended` is a contract that neither remains, and no reconciler
        // collects them on a dormant row (the orphaned-sandbox reconciler
        // excludes dormant lifecycles; the failed-cleanup pass only lists
        // `failed`). The retire finds no active execution, deletes the pods
        // recorded on inactive rows and unplaces the sandbox. Best-effort: a
        // pod it misses belongs to no active execution, which the
        // orphaned-restore-pod GC and the next restore's lingering-pod sweep
        // both reap.
        emit_revert_cleanup_event(
            candidate.sandbox_id,
            SandboxEventType::CleanupStarted,
            candidate.lifecycle,
            candidate.age,
            None,
        );
        match self
            .sink
            .cleanup_after_sweep(candidate.sandbox_id, candidate.pod_fence.clone())
            .await
        {
            Ok(RetireExecutionOutcome::Retired) => {
                OutcomeEvent::substrate_cleanup(
                    CleanupOutcome::Completed,
                    candidate.lifecycle.as_str(),
                )
                .sandbox_id(candidate.sandbox_id)
                .debug();
                emit_revert_cleanup_event(
                    candidate.sandbox_id,
                    SandboxEventType::CleanupCompleted,
                    candidate.lifecycle,
                    candidate.age,
                    None,
                );
            }
            Ok(outcome @ RetireExecutionOutcome::PodIdentityChanged { mismatch, .. }) => {
                note_cleanup_pod_identity_deferral(
                    candidate.sandbox_id,
                    CleanupPass::RevertToSuspended,
                    candidate.lifecycle.as_str(),
                    &candidate.pod_fence,
                    mismatch,
                    report,
                );
                if !outcome.permits_undo() {
                    // The refusal came after the in-VM stop, so the sandbox is
                    // stopped whatever the row says. Putting `restoring` back
                    // would claim a running VM there isn't one of; `suspended`
                    // is the closer description, and its execution row is the
                    // leak the deferral metric surfaces.
                    tracing::warn!(
                        sandbox_id = %candidate.sandbox_id,
                        "the retire was refused only after stopping the execution that still owns \
                         this sandbox; leaving the revert in place rather than restoring a \
                         lifecycle for a stopped VM"
                    );
                    return;
                }
                // The revert retired the restore's execution, so the cleanup
                // refused because an execution it never observed is active
                // now. Leaving the row `suspended` would strand that one where
                // nothing looks again. Put the lifecycle back, if the revert is
                // still the row's last write, and let the next tick judge it.
                match self
                    .sink
                    .undo_revert_to_suspended(
                        candidate.sandbox_id,
                        candidate.lifecycle,
                        &reverted,
                        candidate.fork_restore,
                    )
                    .await
                {
                    Ok(true) => {
                        report.reverted_to_suspended -= 1;
                        emit_sweep_verify_event(
                            candidate.sandbox_id,
                            SandboxEventType::LifecycleTransitionRolledBack,
                            SandboxLifecycle::Suspended,
                            Some(candidate.lifecycle),
                            verdict,
                            candidate.age,
                            "retire_refused_after_revert",
                        );
                    }
                    Ok(false) => tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        "the reverted row moved on before the refused retire could undo it"
                    ),
                    Err(err) => tracing::error!(
                        sandbox_id = %candidate.sandbox_id,
                        error = %err,
                        lifecycle = %candidate.lifecycle,
                        "could not undo a revert whose cleanup was refused; the row is \
                         suspended with an active execution and no reconciler revisits it"
                    ),
                }
            }
            Err(err) => {
                OutcomeEvent::substrate_cleanup(
                    CleanupOutcome::Error,
                    candidate.lifecycle.as_str(),
                )
                .sandbox_id(candidate.sandbox_id)
                .error(&err)
                .detail(if self.config.abandoned_restore_retirement.is_enabled() {
                    "pod cleanup failed after revert to suspended; the execution is \
                     retired, so orphan GC or the next restore removes the pod, but the \
                     row keeps its placement, which nothing revisits on a suspended row"
                } else {
                    "execution retire failed after revert to suspended; stale execution, \
                     placement, and pod persist until the node terminates (no reconciler \
                     retries them on a suspended row)"
                })
                .warn();
                report.cleanup_failures += 1;
                emit_revert_cleanup_event(
                    candidate.sandbox_id,
                    SandboxEventType::CleanupFailed,
                    candidate.lifecycle,
                    candidate.age,
                    Some(err.clone()),
                );
            }
        }
    }

    /// The old age-out behavior, now reached only for rows the probe proved
    /// gone from their node with nothing recoverable, or for an alive
    /// abandoned create: CAS to `failed` and retire the execution.
    async fn fail_candidate(
        &self,
        candidate: &SweepCandidate,
        verdict: ProbeVerdict,
        age: PositiveDuration,
        report: &mut TickReport,
    ) {
        let sandbox_id = candidate.sandbox_id;
        let lifecycle_label = candidate.lifecycle.to_string();
        // `decide` fails an alive create only if no completion receipt can
        // still finish it; the CAS re-checks that, as it re-checks the owner.
        let guard = if candidate.lifecycle == SandboxLifecycle::Creating
            && verdict == ProbeVerdict::AliveOnNode
        {
            FailGuard::NoPendingReceipt
        } else {
            FailGuard::Aged
        };
        // Ask before committing, as the revert and reap paths do. `failed` is
        // the hardest of the three to walk back — nothing does — so learning
        // the fence was lost afterwards is how a sandbox that finished pausing
        // mid-sweep ends up represented as failed with its execution still
        // running.
        match self
            .sink
            .retire_fence_mismatch(sandbox_id, &candidate.pod_fence)
            .await
        {
            Ok(None) => {}
            Ok(Some(mismatch)) => {
                note_cleanup_pod_identity_deferral(
                    sandbox_id,
                    CleanupPass::TransientAgeOut,
                    &lifecycle_label,
                    &candidate.pod_fence,
                    mismatch,
                    report,
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: could not re-check the pod fence before \
                     failing the candidate; deferring to the next tick rather than committing \
                     a lifecycle nothing walks back"
                );
                return;
            }
        }

        let write_token = match self
            .sink
            .fail_aged_candidate(sandbox_id, candidate.lifecycle, age, guard)
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    lifecycle = %lifecycle_label,
                    error = %err,
                    "transient_lifecycle_sweeper: fail CAS failed; will retry on next tick"
                );
                return;
            }
        };
        let Some(write_token) = write_token else {
            OutcomeEvent::aged_transient(
                AgedTransientOutcome::CasLost,
                &lifecycle_label,
                verdict.as_str(),
            )
            .sandbox_id(sandbox_id)
            .debug();
            return;
        };
        OutcomeEvent::aged_transient(
            AgedTransientOutcome::GaveUpNothingRecoverable,
            &lifecycle_label,
            verdict.as_str(),
        )
        .reason("transient_lifecycle_age_out")
        .sandbox_id(sandbox_id)
        .age_secs(candidate.age.as_secs())
        .warn();
        report.aged_out += 1;

        // Best-effort substrate cleanup. The row is already `'failed'`
        // in postgres, so a failure here doesn't open a new race — it
        // just risks a leaked kube pod that the orphaned-sandbox
        // reconciler / node-cascade picks up.
        emit_transient_sweep_event(
            sandbox_id,
            SandboxEventType::LifecycleTransitionTimedOut,
            &lifecycle_label,
            candidate.age,
            None,
        );
        emit_sandbox_failed_event(
            sandbox_id,
            &lifecycle_label,
            "transient_lifecycle_age_out",
            "transient_lifecycle_sweep",
            candidate.age,
            Some(verdict.as_str()),
        );
        emit_transient_sweep_event(
            sandbox_id,
            SandboxEventType::CleanupStarted,
            &lifecycle_label,
            candidate.age,
            None,
        );
        match self
            .sink
            .cleanup_after_sweep(sandbox_id, candidate.pod_fence.clone())
            .await
        {
            Ok(RetireExecutionOutcome::Retired) => {
                OutcomeEvent::substrate_cleanup(CleanupOutcome::Completed, &lifecycle_label)
                    .sandbox_id(sandbox_id)
                    .debug();
                emit_transient_sweep_event(
                    sandbox_id,
                    SandboxEventType::CleanupCompleted,
                    &lifecycle_label,
                    candidate.age,
                    None,
                );
            }
            Ok(outcome @ RetireExecutionOutcome::PodIdentityChanged { mismatch, .. }) => {
                note_cleanup_pod_identity_deferral(
                    sandbox_id,
                    CleanupPass::TransientAgeOut,
                    &lifecycle_label,
                    &candidate.pod_fence,
                    mismatch,
                    report,
                );
                if !outcome.permits_undo() {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        "the retire was refused only after stopping the execution that still owns \
                         this sandbox; leaving the age-out in place rather than restoring a \
                         lifecycle for a stopped VM"
                    );
                    return;
                }
                // An undo may only run where it can restore the *whole*
                // pre-transition state. Entering `failed` from `suspending` or
                // `restoring` fires triggers that clear the evacuation marker
                // and its backoff, the restore lease, and the only-copy
                // protection flag — and the lease must not come back, since
                // another actor may hold one by now. Returning such a row to
                // its old lifecycle stripped of its drain-redrive marker and
                // only-copy protection changes behavior silently, which is
                // worse than a `failed` row this counter makes visible. From
                // every other transient lifecycle the round trip is lossless:
                // those triggers had already cleared that state on the way in.
                if matches!(
                    candidate.lifecycle,
                    SandboxLifecycle::Suspending | SandboxLifecycle::Restoring
                ) {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        lifecycle = %lifecycle_label,
                        "not undoing an age-out whose original lifecycle carried \
                         trigger-owned state the transition cleared; the row stays failed \
                         rather than returning without its evacuation and only-copy markers"
                    );
                    return;
                }
                match self
                    .sink
                    .undo_fail_aged_candidate(sandbox_id, candidate.lifecycle, &write_token)
                    .await
                {
                    Ok(true) => {
                        report.aged_out -= 1;
                        emit_transient_sweep_event(
                            sandbox_id,
                            SandboxEventType::LifecycleTransitionRolledBack,
                            &lifecycle_label,
                            candidate.age,
                            Some("retire_refused_after_age_out".to_string()),
                        );
                    }
                    Ok(false) => tracing::info!(
                        sandbox_id = %sandbox_id,
                        "the failed row moved on before the refused retire could undo it"
                    ),
                    Err(err) => tracing::error!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        lifecycle = %lifecycle_label,
                        "could not undo an age-out whose retire was refused; the row is failed \
                         with a live execution and nothing walks a failed row back"
                    ),
                }
            }
            Err(err) => {
                OutcomeEvent::substrate_cleanup(CleanupOutcome::Error, &lifecycle_label)
                    .sandbox_id(sandbox_id)
                    .error(&err)
                    .detail(
                        "substrate cleanup failed after lifecycle flip; backing pod \
                         persists until the node terminates (the orphaned-sandbox \
                         reconciler skips lifecycle=failed rows, so no automatic retry)",
                    )
                    .warn();
                report.cleanup_failures += 1;
                emit_transient_sweep_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    &lifecycle_label,
                    candidate.age,
                    Some(err.clone()),
                );
            }
        }
    }

    /// Probe-free sweep of markerless `restoring` rows the restore planner
    /// provably cannot restore from anywhere: unplaced, no execution history
    /// naming a node, no snapshot in the restore-retry window, no in-flight
    /// suspend upload. This is the population created when a suspended
    /// sandbox's only state copy is lost (local snapshot pruned before any
    /// upload) and a client keeps calling wake: each doomed attempt re-claims
    /// the restore lease, and concurrent attempts 502 on "restore already in
    /// flight". This phase diagnoses that missing-source shape directly rather
    /// than waiting for the lease to expire and ordinary age recovery to run.
    ///
    /// No probe phase: the aged sweep probes because a wedged-looking row can
    /// hide a live VM, but this predicate requires that no node reference
    /// exists anywhere — there is nothing to probe, and the absence of any
    /// probe target is itself the verdict. No cleanup phase either: the
    /// predicate requires no active execution, so there is no pod or
    /// execution row to retire. Drift between select and act is handled by
    /// the fail CAS re-checking the full predicate atomically.
    async fn sweep_unrestorable_restoring(&self, report: &mut TickReport) {
        let age = self.config.unrestorable_restoring_min_placement_age;
        let candidates = match self.sink.list_unrestorable_restoring(age).await {
            Ok(candidates) => candidates,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "transient_lifecycle_sweeper: list_unrestorable_restoring failed; \
                     will retry on next tick"
                );
                return;
            }
        };
        for (sandbox_id, placement_age) in candidates {
            let claimed = match self.sink.fail_unrestorable_restoring(sandbox_id, age).await {
                Ok(claimed) => claimed,
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "transient_lifecycle_sweeper: unrestorable-restoring fail CAS \
                         errored; will retry on next tick"
                    );
                    continue;
                }
            };
            if !claimed {
                // A restore source appeared between select and act.
                OutcomeEvent::unrestorable_restoring(UnrestorableOutcome::CasLost)
                    .sandbox_id(sandbox_id)
                    .debug();
                continue;
            }
            OutcomeEvent::unrestorable_restoring(UnrestorableOutcome::GaveUpNoRestoreSource)
                .reason("restore_source_unavailable")
                .sandbox_id(sandbox_id)
                .age_secs(placement_age.as_secs())
                .detail(
                    "no source node, no eligible snapshot, no in-flight upload — \
                     nothing can ever restore it",
                )
                .warn();
            report.unrestorable_failed += 1;
            emit_sandbox_failed_event(
                sandbox_id,
                "restoring",
                "restore_source_unavailable",
                "unrestorable_restoring_sweep",
                placement_age,
                None,
            );
        }
    }

    /// Heal-or-reap preserved `started` + `command_ready_pending` rows whose
    /// marker has stuck past the configured age.
    ///
    /// For each aged candidate we probe the guest's command-readiness without
    /// recreating its execution:
    /// - `Ready` -> the guest is live; the marker was a stale leftover from a
    ///   slow create no client retried. Clear it (heal) and leave the sandbox
    ///   `started`. This is what spares a healthy fire-and-forget sandbox from
    ///   being reaped just because nobody touched it.
    /// - `Wedged` -> the runtime is reachable but not reporting ready. CAS-flip
    ///   the row to `failed` and tear down the backing pod. The CAS re-checks
    ///   `started` + marker, so a row a concurrent access just healed drops out.
    /// - `Unreachable` -> inconclusive on one sample (dead pod or transient
    ///   blip), so defer — *unless* the row has been pending past the terminal
    ///   age, in which case the dead pod the orphaned-sandbox reconciler won't
    ///   collect is reaped (a healthy reachable guest would have been healed on
    ///   an earlier tick).
    ///
    /// The phase is bounded to half the tick so it can never monopolize the
    /// shared sweeper loop (the transient age-out and failed-cleanup run in the
    /// same task); unprobed candidates roll to the next tick, and the
    /// `random()` candidate ordering keeps that fair.
    async fn sweep_command_ready_pending(&self, report: &mut TickReport) {
        let age = self.config.command_ready_pending_age;
        let terminal_age = self.config.command_ready_pending_terminal_age.get();
        let candidates = match self.sink.list_command_ready_pending(age).await {
            Ok(ids) => ids,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "transient_lifecycle_sweeper: list_command_ready_pending failed; \
                     will retry on next tick"
                );
                return;
            }
        };
        if candidates.is_empty() {
            return;
        }

        let started = Instant::now();
        let budget = self.config.tick / 2;
        let total = candidates.len();
        for (probed, candidate) in candidates.into_iter().enumerate() {
            let CommandReadyPendingCandidate {
                sandbox_id,
                pending_age,
                pod_fence,
            } = candidate;
            if started.elapsed() >= budget {
                tracing::info!(
                    probed,
                    deferred_to_next_tick = total - probed,
                    budget_secs = budget.as_secs(),
                    "transient_lifecycle_sweeper: command_ready_pending sweep hit its tick \
                     budget; remaining candidates roll to the next tick"
                );
                break;
            }
            match self
                .sink
                .probe_command_ready(sandbox_id, self.config.command_ready_pending_probe_timeout)
                .await
            {
                CommandReadyProbe::Ready => {
                    if let Err(err) = self.sink.heal_command_ready_pending(sandbox_id).await {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            error = %err,
                            "transient_lifecycle_sweeper: failed to clear command_ready_pending \
                             marker after a ready probe; will retry on next tick"
                        );
                        continue;
                    }
                    OutcomeEvent::command_ready_pending(
                        CommandReadyOutcome::ClearedStaleReadyMarker,
                    )
                    .sandbox_id(sandbox_id)
                    .age_secs(pending_age.as_secs())
                    .info();
                    report.command_ready_healed += 1;
                    emit_command_ready_pending_event(
                        sandbox_id,
                        SandboxEventType::GuestReady,
                        pending_age,
                        Some("healed"),
                        None,
                    );
                }
                CommandReadyProbe::Wedged => {
                    self.reap_pending_row(
                        sandbox_id,
                        pending_age,
                        pod_fence,
                        CommandReadyOutcome::GaveUpGuestWedged,
                        report,
                    )
                    .await;
                }
                CommandReadyProbe::Unreachable if pending_age >= terminal_age => {
                    // Terminal backstop: a row we still can't reach this long
                    // after it went pending is a dead pod the orphaned-sandbox
                    // reconciler won't collect, not a transient blip — a healthy
                    // reachable guest would have been healed on an earlier tick.
                    self.reap_pending_row(
                        sandbox_id,
                        pending_age,
                        pod_fence,
                        CommandReadyOutcome::GaveUpGuestUnreachable,
                        report,
                    )
                    .await;
                }
                CommandReadyProbe::Unreachable => {
                    // Inconclusive on one sample (dead pod or transient blip),
                    // and not yet past the terminal age. Defer rather than reap
                    // a possibly-healthy abandoned sandbox; a wedged guest whose
                    // proxy is up surfaces as `Wedged` and is reaped now, and a
                    // truly-gone pod is reaped once it crosses the terminal age.
                    OutcomeEvent::command_ready_pending(CommandReadyOutcome::Deferred)
                        .reason("probe_unreachable")
                        .sandbox_id(sandbox_id)
                        .age_secs(pending_age.as_secs())
                        .debug();
                    report.command_ready_deferred += 1;
                }
            }
        }
    }

    /// CAS-flip a `started` + `command_ready_pending` row to `failed` and tear
    /// down its pod. Shared by the `Wedged` and terminal-`Unreachable` reap
    /// paths; `outcome` distinguishes them (`reaped` vs `reaped_unreachable`).
    /// The CAS re-checks `started` + marker, so a row a concurrent access just
    /// healed drops out and is left alone.
    async fn reap_pending_row(
        &self,
        sandbox_id: SandboxId,
        pending_age: Duration,
        pod_fence: RetireFence,
        outcome: CommandReadyOutcome,
        report: &mut TickReport,
    ) {
        // Ask before committing, for the same reason the revert path does. The
        // CAS below flips the row to `failed`, and that transition's trigger
        // clears `command_ready_pending` — so a fence lost between the probe
        // and here would strip the marker while the teardown is refused,
        // dropping a still-wedged sandbox out of this sweep for good once the
        // failed-cleanup pass repairs it back to `started`.
        match self
            .sink
            .retire_fence_mismatch(sandbox_id, &pod_fence)
            .await
        {
            Ok(None) => {}
            Ok(Some(mismatch)) => {
                note_cleanup_pod_identity_deferral(
                    sandbox_id,
                    CleanupPass::CommandReadyReap,
                    "started",
                    &pod_fence,
                    mismatch,
                    report,
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: could not re-check the pod fence before \
                     reaping; deferring to the next tick rather than clearing the \
                     command-ready marker for a teardown that may be refused"
                );
                return;
            }
        }

        let write_token = match self.sink.reap_command_ready_pending(sandbox_id).await {
            Ok(claimed) => claimed,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %err,
                    "transient_lifecycle_sweeper: reap CAS failed; will retry on next tick"
                );
                return;
            }
        };
        let Some(write_token) = write_token else {
            // Healed or transitioned out of `started` between probe and CAS.
            return;
        };
        OutcomeEvent::command_ready_pending(outcome)
            .sandbox_id(sandbox_id)
            .age_secs(pending_age.as_secs())
            .warn();
        report.command_ready_reaped += 1;
        self.cleanup_after_reap(sandbox_id, pending_age, pod_fence, &write_token, report)
            .await;
    }

    /// Tear down the backing execution of a row the reaper just flipped to
    /// `failed`, emitting the `LifecycleTransitionTimedOut` -> `Cleanup*`
    /// event sequence around it. Best-effort: the row is already `failed`, so
    /// a cleanup failure just leaves a pod for the node-cascade to collect.
    async fn cleanup_after_reap(
        &self,
        sandbox_id: SandboxId,
        pending_age: Duration,
        pod_fence: RetireFence,
        write_token: &str,
        report: &mut TickReport,
    ) {
        emit_command_ready_pending_event(
            sandbox_id,
            SandboxEventType::LifecycleTransitionTimedOut,
            pending_age,
            None,
            None,
        );
        emit_sandbox_failed_event(
            sandbox_id,
            "started",
            "command_ready_pending_reap",
            "command_ready_pending_sweep",
            pending_age,
            None,
        );
        emit_command_ready_pending_event(
            sandbox_id,
            SandboxEventType::CleanupStarted,
            pending_age,
            None,
            None,
        );
        match self
            .sink
            .cleanup_after_sweep(sandbox_id, pod_fence.clone())
            .await
        {
            Ok(RetireExecutionOutcome::Retired) => {
                OutcomeEvent::substrate_cleanup(CleanupOutcome::Completed, "started")
                    .sandbox_id(sandbox_id)
                    .debug();
                emit_command_ready_pending_event(
                    sandbox_id,
                    SandboxEventType::CleanupCompleted,
                    pending_age,
                    None,
                    None,
                );
            }
            Ok(outcome @ RetireExecutionOutcome::PodIdentityChanged { mismatch, .. }) => {
                note_cleanup_pod_identity_deferral(
                    sandbox_id,
                    CleanupPass::CommandReadyReap,
                    "started",
                    &pod_fence,
                    mismatch,
                    report,
                );
                if !outcome.permits_undo() {
                    // Refused only after the stop: the guest is down, so
                    // restoring `started` and its readiness marker would put
                    // the row back into a state that reads as a live sandbox.
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        "the retire was refused only after stopping the execution that still owns \
                         this sandbox; leaving the reap in place rather than restoring a \
                         started row for a stopped VM"
                    );
                    return;
                }
                // The flip to `failed` already cleared the marker, and nothing
                // puts it back. Without both restored this guest never enters
                // the reaper again, however wedged it still is.
                match self
                    .sink
                    .undo_reap_command_ready_pending(sandbox_id, write_token)
                    .await
                {
                    Ok(true) => {
                        report.command_ready_reaped -= 1;
                        emit_command_ready_pending_event(
                            sandbox_id,
                            SandboxEventType::LifecycleTransitionRolledBack,
                            pending_age,
                            Some("retire_refused_after_reap"),
                            None,
                        );
                    }
                    Ok(false) => tracing::info!(
                        sandbox_id = %sandbox_id,
                        "the reaped row moved on before the refused retire could undo it"
                    ),
                    Err(err) => tracing::error!(
                        sandbox_id = %sandbox_id,
                        error = %err,
                        "could not undo a reap whose retire was refused; the row is failed \
                         without its command-ready marker and will not be reaped again"
                    ),
                }
            }
            Err(err) => {
                OutcomeEvent::substrate_cleanup(CleanupOutcome::Error, "started")
                    .sandbox_id(sandbox_id)
                    .error(&err)
                    .detail(
                        "substrate cleanup failed after command_ready_pending reap; \
                         backing pod persists until the node terminates",
                    )
                    .warn();
                report.cleanup_failures += 1;
                emit_command_ready_pending_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    pending_age,
                    None,
                    Some(err.clone()),
                );
            }
        }
    }

    async fn repair_failed_active_candidate(
        &self,
        sandbox_id: SandboxId,
        observation: ProbeObservation,
        report: &mut TickReport,
    ) {
        match self.sink.repair_failed_active_execution(sandbox_id).await {
            Ok(true) => {
                OutcomeEvent::failed_cleanup(
                    FailedCleanupOutcome::RepairedFailedRowToLiveVm,
                    observation.verdict.as_str(),
                )
                .reason("node_reported_live_started_sandbox")
                .sandbox_id(sandbox_id)
                .detail(node_status_label(observation))
                .warn();
                report.failed_cleanup_repaired += 1;
                emit_failed_active_repair_event(
                    sandbox_id,
                    SandboxEventType::LifecycleConverged,
                    "repaired",
                    observation.node_status,
                    None,
                );
            }
            Ok(false) => {
                OutcomeEvent::failed_cleanup(
                    FailedCleanupOutcome::CasLost,
                    observation.verdict.as_str(),
                )
                .sandbox_id(sandbox_id)
                .detail(node_status_label(observation))
                .debug();
                report.failed_cleanup_repair_cas_lost += 1;
                emit_failed_active_repair_event(
                    sandbox_id,
                    SandboxEventType::LifecycleTransitionCasLost,
                    "cas_lost",
                    observation.node_status,
                    None,
                );
            }
            Err(error) => {
                OutcomeEvent::failed_cleanup(
                    FailedCleanupOutcome::Error,
                    observation.verdict.as_str(),
                )
                .sandbox_id(sandbox_id)
                .detail(node_status_label(observation))
                .error(&error)
                .warn();
                report.failed_cleanup_repair_failures += 1;
                emit_failed_active_repair_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    "error",
                    observation.node_status,
                    Some(error.to_string()),
                );
            }
        }
    }

    async fn cleanup_failed_active_executions(&self, report: &mut TickReport) {
        let candidates = match self
            .sink
            .list_failed_active_executions(self.config.failed_cleanup_batch_size)
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "transient_lifecycle_sweeper: list failed active executions failed"
                );
                return;
            }
        };

        if candidates.is_empty() {
            return;
        }

        tracing::warn!(
            failed_active_executions = candidates.len(),
            "transient_lifecycle_sweeper: cleaning active executions for failed sandboxes"
        );

        // Probe gate: only a live VM (version-skew suspend failures during a
        // deploy produced `failed` rows whose VM was in fact alive and
        // serving) or an unreachable node defers. See
        // [`ProbeVerdict::permits_failed_retire`] for why a dormant row with
        // a salvageable local artifact proceeds.
        let targets: Vec<(SandboxId, Option<String>)> = candidates
            .iter()
            .map(|candidate| (candidate.sandbox_id, candidate.node_name.clone()))
            .collect();
        let observations = probe::probe_targets(
            self.sink.as_ref(),
            &targets,
            self.config.probe_timeout,
            self.config.tick / 2,
        )
        .await;

        for candidate in candidates {
            let sandbox_id = candidate.sandbox_id;
            let observation = observations
                .get(&sandbox_id)
                .copied()
                .unwrap_or_else(ProbeObservation::inconclusive);
            let verdict = observation.verdict;

            // Rotate before acting either way, so a persistently deferred
            // candidate cannot monopolize the front of the batch.
            if let Err(error) = self.sink.note_failed_cleanup_attempt(sandbox_id).await {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    error = %error,
                    "transient_lifecycle_sweeper: failed to rotate failed cleanup candidate; skipping attempt"
                );
                continue;
            }

            // A first activation that never finished is retired even with its
            // VM alive once its owner is gone: the sweep fails such a create
            // on purpose, and repairing it to `started` would hand out a
            // sandbox its create request never returned, with no pod IP to
            // reach it at. One whose owner may still be working is never
            // repaired either; with its VM alive it defers below.
            let retire_alive_create = candidate.first_activation == FirstActivation::Abandoned
                && verdict == ProbeVerdict::AliveOnNode;
            // The listing's facts predate the probe, and an execution that
            // reached `running` since (the pod-metadata write records the pod IP
            // and `running` together) is routable, so re-read them before
            // retiring a live VM. Anything that moved waits for the next tick's
            // listing.
            if retire_alive_create {
                match self.sink.first_activation(sandbox_id).await {
                    Ok(Some(FirstActivation::Abandoned)) => {}
                    Ok(now) => {
                        tracing::debug!(
                            sandbox_id = %sandbox_id,
                            first_activation = ?now,
                            "transient_lifecycle_sweeper: failed create moved since the listing; \
                             not retiring its live VM this tick"
                        );
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            error = %error,
                            "transient_lifecycle_sweeper: could not re-read a failed create's \
                             first activation; not retiring its live VM this tick"
                        );
                        continue;
                    }
                }
            }
            if candidate.first_activation == FirstActivation::Finished
                && verdict == ProbeVerdict::AliveOnNode
                && observation.node_status == Some(SandboxLifecycle::Started)
            {
                self.repair_failed_active_candidate(sandbox_id, observation, report)
                    .await;
                continue;
            }

            if !retire_alive_create && !verdict.permits_failed_retire() {
                let defer_reason = failed_cleanup_defer_reason(observation);
                OutcomeEvent::failed_cleanup(FailedCleanupOutcome::Deferred, verdict.as_str())
                    .reason(defer_reason)
                    .sandbox_id(sandbox_id)
                    .detail(node_status_label(observation))
                    .warn();
                report.failed_cleanup_deferred += 1;
                emit_failed_cleanup_event(
                    sandbox_id,
                    SandboxEventType::SweepDeferred,
                    Some(defer_reason),
                    None,
                );
                continue;
            }
            OutcomeEvent::failed_cleanup(
                FailedCleanupOutcome::PermittedRetireAfterProbe,
                verdict.as_str(),
            )
            .sandbox_id(sandbox_id)
            .debug();
            report.failed_cleanup_attempts += 1;

            emit_failed_cleanup_event(sandbox_id, SandboxEventType::CleanupStarted, None, None);
            match self
                .sink
                .cleanup_failed_execution(sandbox_id, candidate.pod_fence.clone())
                .await
            {
                Ok(RetireExecutionOutcome::Retired) => {
                    OutcomeEvent::substrate_cleanup(CleanupOutcome::Completed, "failed")
                        .sandbox_id(sandbox_id)
                        .info();
                    emit_failed_cleanup_event(
                        sandbox_id,
                        SandboxEventType::CleanupCompleted,
                        Some("ok"),
                        None,
                    );
                }
                Ok(RetireExecutionOutcome::PodIdentityChanged { mismatch, .. }) => {
                    note_cleanup_pod_identity_deferral(
                        sandbox_id,
                        CleanupPass::FailedCleanup,
                        "failed",
                        &candidate.pod_fence,
                        mismatch,
                        report,
                    );
                }
                Err(error) => {
                    OutcomeEvent::substrate_cleanup(CleanupOutcome::Error, "failed")
                        .sandbox_id(sandbox_id)
                        .error(&error)
                        .detail("cleanup failed; rotated retry behind older work")
                        .warn();
                    report.failed_cleanup_failures += 1;
                    emit_failed_cleanup_event(
                        sandbox_id,
                        SandboxEventType::CleanupFailed,
                        Some("failed"),
                        Some(error.clone()),
                    );
                }
            }
        }
    }
}

/// Run the sweeper on the current task until the shutdown token is
/// cancelled. Spawned by `space-cluster-controller` alongside the other
/// cluster-singleton reconcilers.
pub async fn run<S: SweepSink>(
    config: ReconcilerConfig,
    sink: S,
    shutdown_token: CancellationToken,
) {
    if !config.enabled && !config.failed_cleanup_enabled && !config.command_ready_pending_enabled {
        tracing::info!(
            "transient_lifecycle_sweeper disabled \
             (SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED, \
              SPACE_FAILED_SANDBOX_CLEANUP_ENABLED, and \
              the fixed-disabled command-ready-pending reaper are all false); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        default_age_secs = config.default_age.get().as_secs(),
        transient_age_out_enabled = config.enabled,
        probe_timeout_secs = config.probe_timeout.as_secs(),
        failed_cleanup_enabled = config.failed_cleanup_enabled,
        failed_cleanup_batch_size = config.failed_cleanup_batch_size,
        command_ready_pending_enabled = config.command_ready_pending_enabled,
        command_ready_pending_age_secs = config.command_ready_pending_age.get().as_secs(),
        command_ready_pending_probe_timeout_secs =
            config.command_ready_pending_probe_timeout.as_secs(),
        command_ready_pending_terminal_age_secs =
            config.command_ready_pending_terminal_age.get().as_secs(),
        "starting transient_lifecycle_sweeper"
    );

    let reconciler = Reconciler::new(config.clone(), sink);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race with the rest
    // of the controller coming online.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let _ = reconciler.tick().await;
                metrics::loop_latency(Loop::TransientLifecycleSweeper, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("transient_lifecycle_sweeper stopping");
                return;
            }
        }
    }
}

/// Convenience entry point for `reconcilers.rs`: builds the production
/// sink from `(pool, substrate)` and runs the loop.
pub async fn run_with_substrate(
    config: ReconcilerConfig,
    pool: PgPool,
    substrate: Arc<Substrate>,
    shutdown_token: CancellationToken,
) {
    let sink = SubstrateSweepSink::new(pool, substrate)
        .with_abandoned_restore_retirement(config.abandoned_restore_retirement)
        .with_restoring_episode_deadline(config.restoring_episode_deadline);
    run(config, sink, shutdown_token).await;
}
