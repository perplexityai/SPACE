//! Periodic reconciler for sandbox rows whose backing kube pod has disappeared.
//!
//! Edge-triggered cleanup (auto-lifecycle timer, ECW DELETE, controller
//! cancellation) leaks rows whenever a link in that chain breaks: a pod
//! force-deleted by an admin, a cleanup RPC dropped under APF saturation,
//! sbx-mgr crashing mid-cleanup. This loop scans non-terminal rows on a
//! cadence, classifies them against the kube reflector cache and durable
//! artifact and policy evidence, then finalizes intended deletion, projects a
//! recoverable row back to `suspended`, or moves genuinely ambiguous state to
//! the inert `quarantined` hold state.
//!
//! Per the repo-wide rule against background work in API servers (see
//! AGENTS.md), this loop runs in the cluster-singleton controller binary
//! (`space-cluster-controller`, in `crates/libs/cluster/controller/server/`), not
//! as a `tokio::spawn` inside the API server. Dry-run defaults to true so
//! the operator opts into writes; the deployment itself is the master
//! enable/disable.

use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use pplx_space_sandbox_teardown::{
    delete_reconcile_state_for_elements, finalize_sandbox_db_delete_tx,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{
    PodUid, SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction,
    SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    SubstrateKind,
};
use pplx_space_common_svc::sql::sandbox_quarantine::{
    quarantine_history_insert_columns, quarantine_state_conflict_assignments,
    quarantine_state_insert_columns,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::sandbox::api::reconciler::{
    self as reconciler_metrics, ClassificationOutcome, ClassificationReason, PodEvidence,
};
use pplx_space_sandbox_portability::{
    SuspendedTargetNodeAssignment, clear_current_local_artifact_marker_assignments,
    quarantined_k8s_finalization_sql, started_repair_revert_eligibility,
    suspended_k8s_finalization_sql,
};
use sqlx::PgPool;
use sqlx::types::Json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use pplx_space_api::reconcile_queue::ReconcileElementRef;

use pplx_space_api::controllers::substrate::kubernetes::{
    K8sBlockingDeleteError, K8sBlockingDeleteOutcome, KubernetesSubstrate, SandboxPodView,
};

use crate::env_config;

mod quarantine_recovery;

const DEFAULT_INTERVAL: Duration = Duration::from_mins(1);
const DEFAULT_GRACE_PERIOD: Duration = Duration::from_mins(1);
const DEFAULT_BATCH_SIZE: u32 = 500;
const DEFAULT_MAX_CLASSIFICATIONS_PER_MINUTE: u32 = 60;
const DEFAULT_MAX_ACTIVE_QUARANTINES: u32 = 50;
const DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP: u32 = 20;
const DEFAULT_MAX_QUARANTINE_PERCENTAGE: u32 = 25;
// Ceilings on the reconciler's safety limits. They exist so a typo can't
// switch a limit off: an over-ceiling value clamps, so each ceiling has to be
// a value at which its limit still bites. Raising a limit for a known cleanup
// stays possible within the ceiling; the percentage ceiling deliberately caps
// a cleanup that is almost all quarantines.

/// 10× the default.
const MAX_ACTIVE_QUARANTINES_CEILING: u32 = 10 * DEFAULT_MAX_ACTIVE_QUARANTINES;
/// 10× the default, and below the rate ceiling: a small table's sweep fits in
/// one tick, where the rate limit would stop it first.
const MAX_NEW_QUARANTINES_PER_SWEEP_CEILING: u32 = 10 * DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP;
const _: () =
    assert!(MAX_NEW_QUARANTINES_PER_SWEEP_CEILING < MAX_CLASSIFICATIONS_PER_MINUTE_CEILING);
/// 5× the default, and below [`DEFAULT_BATCH_SIZE`]. The limit counts live
/// actions, and one tick a minute acts on at most one batch of candidates, so
/// a limit at or above the batch size can't bind within a tick. It could bind
/// only on the previous tick's actions, when they're still inside the window,
/// which depends on how late each tick wakes.
const MAX_CLASSIFICATIONS_PER_MINUTE_CEILING: u32 = 5 * DEFAULT_MAX_CLASSIFICATIONS_PER_MINUTE;
const _: () = assert!(MAX_CLASSIFICATIONS_PER_MINUTE_CEILING < DEFAULT_BATCH_SIZE);
/// 3× the default. A sweep's planned quarantines never exceed its planned
/// dispositions, so at 100 the percentage breaker could never trip.
const MAX_QUARANTINE_PERCENTAGE_CEILING: u32 = 3 * DEFAULT_MAX_QUARANTINE_PERCENTAGE;
const _: () = assert!(MAX_QUARANTINE_PERCENTAGE_CEILING < 100);
const QUARANTINE_PERCENTAGE_MIN_PLANS: u32 = 20;

const RATE_LIMIT_WINDOW: Duration = Duration::from_mins(1);

fn emit_orphan_reconcile_event(
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    reason: &'static str,
    details: SandboxEventDetails,
) {
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.reason(reason).trigger(SandboxEventTrigger::System));
}

const ORPHAN_RECONCILER_STOP_LIFECYCLES: &[&str] =
    &["failed", "quarantined", "cleanup_required", "deleted"];

/// Lifecycle set the `Forgotten` path operates on. A row's *latest*
/// execution being `state='forgotten'` is the orphan signal, but only when
/// the lifecycle is one of these — states where a live pod is expected.
/// Layering both checks fences against:
/// - dormant lifecycles where deletion is wrong: `stopped` (`stop→start`
///   is a valid recovery), `created` (registered but never started),
///   `suspended` (snapshot on disk).
/// - transient states whose successful completion produces on-disk state:
///   `restoring`, `suspending`, `checkpointing` — would leave ambiguous
///   durable-state ownership if forced to terminal mid-transition.
///
/// `creating` is included: a failed pod creation calls `forget_pod_tracking`
/// while the row is still `lifecycle='creating'`, and a crash before the outer
/// create handler can finalize the failure leaves a non-recoverable shell. The
/// latest-execution-is-forgotten check distinguishes this from a normal create
/// in flight (which has no execution row yet, so the forgotten-execution
/// predicate fails).
///
/// `deleting` is handled by a dedicated finalize path: the lifecycle claim is
/// durable delete intent, and a missing execution means no runtime remains to
/// finish the database teardown.
const ACTIVE_EXECUTION_REQUIRED_LIFECYCLES: &[&str] = &[
    "creating", "starting", "started", "paused", "resuming", "pausing", "stopping",
];

/// Lifecycles where the **kube pod is already supposed to be alive** when
/// the reconciler observes it. The `Normal` orphan path always fires for
/// these — a missing pod is unambiguously an orphan.
const LIVE_POD_REQUIRED_LIFECYCLES: &[&str] =
    &["started", "paused", "resuming", "pausing", "stopping"];

/// Lifecycles where pod creation may legitimately still be in flight.
/// `ensure_active_execution_row` inserts the execution row with `pod_name`
/// set *before* awaiting `pods.create`, so an active execution + missing
/// pod can mean either:
///
/// - a healthy slow create progressing (busy scheduler, image pull, etc.)
///   — must NOT be classified as missing runtime;
/// - a crashed create that left a stuck row no one will clean up — must
///   be classified after the grace period.
///
/// The Normal SELECT/UPDATE for these lifecycles requires the active
/// execution row to be older than `CREATE_AGE_THRESHOLD_SECS` and any
/// materialization attempt lease to have expired: young or currently-owned
/// rows are presumed in-flight and skipped. An expired `creating` row is
/// cleanup work even when its pod is present; classification first commits
/// `lifecycle='deleting'`, which prevents a successor activation, and only
/// then deletes the pod. The Forgotten path catches the variant where the
/// failed-create handler got to run `forget_pod_tracking` (different
/// signal, same intent).
const PENDING_POD_LIFECYCLES: &[&str] = &["creating", "starting"];

/// Minimum execution age before the Normal path acts on a
/// `creating`/`starting` candidate. A non-NULL materialization lease must also
/// be expired. 5 minutes is comfortably longer than any healthy
/// `pods.create` round-trip including scheduling and image pull, and short
/// enough that genuinely-stuck rows don't linger.
const CREATE_AGE_THRESHOLD_SECS: i64 = 300;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub grace_period: Duration,
    pub batch_size: u32,
    pub max_classifications_per_minute: u32,
    pub max_active_quarantines: u32,
    pub max_new_quarantines_per_sweep: u32,
    pub max_quarantine_percentage: u32,
    pub dry_run: bool,
}

impl ReconcilerConfig {
    /// Returns config populated from env vars. Live reconciliation is gated
    /// explicitly so deployments can keep the loop observable without giving
    /// it write authority.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        Self {
            enabled: config.destructive_gate("SPACE_ORPHANED_SANDBOX_RECONCILER_ENABLED"),
            interval: DEFAULT_INTERVAL,
            grace_period: DEFAULT_GRACE_PERIOD,
            batch_size: DEFAULT_BATCH_SIZE,
            // A safety rate limit that non-production clusters deliberately
            // lower, so it stays configurable.
            max_classifications_per_minute: config.bounded_u32(
                "SPACE_API_RECONCILER_MAX_CLASSIFICATIONS_PER_MINUTE",
                DEFAULT_MAX_CLASSIFICATIONS_PER_MINUTE,
                MAX_CLASSIFICATIONS_PER_MINUTE_CEILING,
            ),
            // Safety ceilings: `0` stops new quarantines.
            max_active_quarantines: config.bounded_u32_allow_zero(
                "SPACE_API_RECONCILER_MAX_ACTIVE_QUARANTINES",
                DEFAULT_MAX_ACTIVE_QUARANTINES,
                MAX_ACTIVE_QUARANTINES_CEILING,
            ),
            max_new_quarantines_per_sweep: config.bounded_u32_allow_zero(
                "SPACE_API_RECONCILER_MAX_NEW_QUARANTINES_PER_SWEEP",
                DEFAULT_MAX_NEW_QUARANTINES_PER_SWEEP,
                MAX_NEW_QUARANTINES_PER_SWEEP_CEILING,
            ),
            max_quarantine_percentage: config.bounded_u32_allow_zero(
                "SPACE_API_RECONCILER_MAX_QUARANTINE_PERCENTAGE",
                DEFAULT_MAX_QUARANTINE_PERCENTAGE,
                MAX_QUARANTINE_PERCENTAGE_CEILING,
            ),
            // Dry-run defaults to true so a fresh deployment gets
            // observability before any writes happen.
            dry_run: config.truthy_default("SPACE_API_RECONCILER_DRY_RUN", true),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCandidate {
    pub sandbox_id: SandboxId,
    pub pod_name: String,
    /// Set only for `OrphanKind::Normal`. Used to fence classification
    /// against same-generation pod recreation (see `ClassifyGuard::PodName`).
    pub pod_uid: Option<PodUid>,
    pub kind: OrphanKind,
    /// Lifecycle observed by the candidate scan. Live classification still
    /// re-verifies it under the guarded transaction; this copy is only for
    /// bounded dry-run/live telemetry.
    pub lifecycle: SandboxLifecycle,
}

#[derive(sqlx::FromRow)]
struct OrphanCandidateRow {
    sandbox_id: SandboxId,
    pod_name: Option<String>,
    pod_uid: Option<PodUid>,
    lifecycle: SandboxLifecycle,
}

/// How the candidate was identified, which determines the action path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanKind {
    /// Active execution still tracked and `pod_name` is set. Run the full
    /// presence check and grace clock; on action, guard the UPDATE on the
    /// active execution still having this pod_name (catches a concurrent
    /// wake that swapped in a new execution).
    Normal,
    /// The **most recent** execution for this sandbox is in
    /// `state='forgotten'` — set by `finish_active_execution_in(...,
    /// "forgotten")` inside `forget_pod_tracking` when an API path
    /// observed the missing pod and gave up. "Most recent" matters: a
    /// historical `forgotten` execution followed by a later successful
    /// suspend (`state='suspended'`) means there's recoverable on-disk
    /// state and the row must NOT be quarantined. Apply grace using the
    /// sandbox_id alone (no pod_name) and re-check the same latest-
    /// execution-is-forgotten condition in the UPDATE so a concurrent
    /// recovery (newer active execution) or a concurrent
    /// suspend-completion (newer inactive `state='suspended'`) aborts.
    Forgotten,
    /// Rows left in `lifecycle='deleting'` by the old orphan cleanup path.
    /// The lifecycle claim is durable delete intent, so the reconciler
    /// completes their guarded database teardown.
    LegacyDeleting,
}

impl OrphanKind {
    /// Stable metric/log tag for this orphan kind. The values correspond
    /// 1:1 to the explicit signals the reconciler
    /// classifies (see the `OrphanKind` variant docs above) so dashboards
    /// can split `rows_classified{reason}` by what the loop is
    /// actually doing rather than treating every action as the same.
    fn classification_reason(self) -> ClassificationReason {
        match self {
            OrphanKind::Normal => ClassificationReason::Normal,
            OrphanKind::Forgotten => ClassificationReason::Forgotten,
            OrphanKind::LegacyDeleting => ClassificationReason::LegacyDeleting,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodPresenceStatus {
    Present,
    TerminalSucceeded,
    TerminalFailed,
    Unready,
    Missing,
    Unknown,
}

/// Dependency injection seam for pod presence so tests don't need a real
/// reflector or kube client.
pub trait PodPresence: Send + Sync {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus;
}

/// Dependency injection seam for the SQL + substrate side so tests don't need
/// a real Postgres or kube cluster.
pub trait OrphanActionSink: Send + Sync {
    /// Lists candidates whose `sandbox_id` is in `(after_sandbox_id,
    /// upper_bound]`. The upper bound is captured once per sweep so
    /// continuous sandbox creation can't keep extending the cursor's right
    /// edge — without it, v7-UUID-time-ordered IDs starve older orphans on
    /// busy clusters.
    async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error>;
    /// The `guard` selects how to fence classification against concurrent
    /// state changes. See [`ClassifyGuard`] for the variants.
    async fn classify_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<ClassifyOutcome, OrphanClassifyError>;
    /// Evaluates the same guarded evidence as live classification without
    /// changing sandbox lifecycle.
    async fn plan_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<Option<OrphanPlan>, OrphanClassifyError>;
    async fn active_quarantine_count(&self) -> Result<u32, OrphanClassifyError>;
    /// Returns the largest `sandboxes.id` (any lifecycle, any substrate) at
    /// call time, or `None` if the table is empty. Used as the per-sweep
    /// high-water mark.
    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifyOutcome {
    Deleted {
        from_lifecycle: SandboxLifecycle,
        reason: OrphanDeleteReason,
    },
    RecoveredSuspended {
        from_lifecycle: SandboxLifecycle,
    },
    Quarantined {
        from_lifecycle: SandboxLifecycle,
    },
    Deferred {
        from_lifecycle: SandboxLifecycle,
        reason: OrphanDeferReason,
    },
    SkippedTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanPlan {
    pub from_lifecycle: SandboxLifecycle,
    pub disposition: OrphanDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanDisposition {
    FinalizeDelete(OrphanDeleteReason),
    RecoverSuspended,
    QuarantineAmbiguous,
    Defer(OrphanDeferReason),
}

impl OrphanDisposition {
    fn metric_outcome(self) -> ClassificationOutcome {
        match self {
            Self::FinalizeDelete(_) => ClassificationOutcome::Deleted,
            Self::RecoverSuspended => ClassificationOutcome::Suspended,
            Self::QuarantineAmbiguous => ClassificationOutcome::Quarantined,
            Self::Defer(_) => ClassificationOutcome::Deferred,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanDeleteReason {
    ExplicitDeleting,
    AbandonedCreate,
}

impl OrphanDeleteReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitDeleting => "explicit_deleting",
            Self::AbandonedCreate => "abandoned_create",
        }
    }
}

impl std::fmt::Display for OrphanDeleteReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanDeferReason {
    InFlightSnapshot,
    PodCleanupUnconfirmed,
}

impl OrphanDeferReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InFlightSnapshot => "in_flight_snapshot",
            Self::PodCleanupUnconfirmed => "pod_cleanup_unconfirmed",
        }
    }
}

impl std::fmt::Display for OrphanDeferReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Selects how the classification UPDATE re-verifies the world before
/// changing lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassifyGuard {
    /// Pod with this name+uid was observed missing. UPDATE proceeds only
    /// if the active execution still has the same pod_name AND its
    /// pod_uid is unchanged from what we observed (NULL-safe).
    /// pod_name catches a concurrent wake that bumped the generation;
    /// pod_uid catches same-generation recreation by `ensure_pod_exists`.
    /// That path publishes a short materialization lease before it creates
    /// or loads the pod, so classification defers the otherwise-dangerous
    /// "pod exists, DB UID is stale" interval. `uid: None` means we
    /// observed NULL, so any materialized recreation with a non-null UID
    /// aborts classification.
    PodName { name: String, uid: Option<PodUid> },
    /// Active execution was cleared by an API path while the lifecycle is
    /// still active-required. UPDATE proceeds only if the row is still
    /// active-required and still has no active execution — catches a
    /// transition-to-dormant or a concurrent recovery between scan and
    /// UPDATE.
    Forgotten,
    /// Legacy row already sitting in `lifecycle='deleting'`. This does not
    /// need artifact recovery: the lifecycle claim is durable delete intent,
    /// and classification completes the existing teardown.
    LegacyDeleting { pod_name: Option<String> },
}

impl ClassifyGuard {
    fn observed_pod_cleanup_name(&self) -> Option<String> {
        match self {
            Self::PodName { name, .. } => Some(name.clone()),
            Self::LegacyDeleting {
                pod_name: Some(name),
            } => Some(name.clone()),
            Self::LegacyDeleting { pod_name: None } => None,
            Self::Forgotten => None,
        }
    }
}

fn non_empty_string(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

#[derive(Debug, thiserror::Error)]
pub enum OrphanClassifyError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub pod_present: u32,
    pub pod_unready: u32,
    pub pod_unknown: u32,
    pub within_grace: u32,
    pub rate_limited: u32,
    pub dry_run_classified: u32,
    pub deleted: u32,
    pub recovered_suspended: u32,
    pub quarantined: u32,
    pub deferred: u32,
    pub circuit_breaker: u32,
    pub skipped_terminal: u32,
}

/// Stateful per-tick reconciler core. Pure with respect to time and
/// dependencies — tests drive it directly with a fake clock and fake sink.
pub struct Reconciler<P: PodPresence, S: OrphanActionSink> {
    config: ReconcilerConfig,
    presence: P,
    sink: S,
    /// Per-sandbox grace clock. Stores the (pod_name, pod_uid) we observed
    /// missing alongside the timestamp so that any change to either —
    /// new execution generation (different pod_name) OR same-generation
    /// recreation by `ensure_pod_exists` (same pod_name, new pod_uid via
    /// `persist_pod_metadata`) — restarts the clock for the new pod
    /// instead of reusing a stale timestamp.
    first_missing_at: HashMap<SandboxId, (String, Option<PodUid>, Instant)>,
    action_history: VecDeque<Instant>,
    scan_after: Option<SandboxId>,
    /// High-water mark captured at sweep start. `Some(id)` while a sweep is
    /// in progress; `None` between sweeps. Bounds the right edge of the
    /// cursor so continuous creation can't starve older candidates.
    sweep_upper_bound: Option<SandboxId>,
    /// Sandbox IDs observed in `list_candidates` results during the current
    /// sweep. Used to garbage-collect `first_missing_at` entries on sweep
    /// wrap: any key not in this set moved out of the candidate set through
    /// another path or reconciliation result, and its stale grace clock would
    /// otherwise leak forever.
    seen_this_sweep: HashSet<SandboxId>,
    planned_this_sweep: u32,
    planned_quarantines_this_sweep: u32,
    new_quarantines_this_sweep: u32,
    circuit_open: bool,
}

impl<P: PodPresence, S: OrphanActionSink> Reconciler<P, S> {
    pub fn new(config: ReconcilerConfig, presence: P, sink: S) -> Self {
        Self {
            config,
            presence,
            sink,
            first_missing_at: HashMap::new(),
            action_history: VecDeque::new(),
            scan_after: None,
            sweep_upper_bound: None,
            seen_this_sweep: HashSet::new(),
            planned_this_sweep: 0,
            planned_quarantines_this_sweep: 0,
            new_quarantines_this_sweep: 0,
            circuit_open: false,
        }
    }

    pub async fn tick(&mut self, now: Instant) -> TickReport {
        // Try the cursored list once; if the cursor reached the sweep's
        // high-water mark we wrap and retry this same tick so the next
        // sweep starts immediately. Two iterations is enough — after a wrap
        // an empty result means the table is genuinely empty.
        let candidates = 'fetch: {
            for attempt in 0..2 {
                if self.sweep_upper_bound.is_none() {
                    match self.sink.current_max_sandbox_id().await {
                        Ok(Some(id)) => self.sweep_upper_bound = Some(id),
                        Ok(None) => break 'fetch Vec::new(),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "reconciler current_max_sandbox_id failed"
                            );
                            return TickReport::default();
                        }
                    }
                    debug_assert!(self.scan_after.is_none());
                }
                let upper_bound = self.sweep_upper_bound.expect("set immediately above");
                match self
                    .sink
                    .list_candidates(self.config.batch_size, self.scan_after, upper_bound)
                    .await
                {
                    Ok(rows) if rows.is_empty() && attempt == 0 => {
                        // Cursor reached the sweep's high-water mark. Reset
                        // and let the next iteration capture a fresh bound
                        // and start over from the smallest id — that picks
                        // up rows created during this sweep.
                        //
                        // Garbage-collect `first_missing_at` for sandboxes
                        // that didn't appear in any of this sweep's pages.
                        // They moved out of the candidate set, so their grace
                        // clocks would otherwise accumulate indefinitely.
                        self.first_missing_at
                            .retain(|sandbox_id, _| self.seen_this_sweep.contains(sandbox_id));
                        self.seen_this_sweep.clear();
                        tracing::info!(
                            dry_run = self.config.dry_run,
                            "orphaned-sandbox reconcile sweep completed"
                        );
                        self.scan_after = None;
                        self.sweep_upper_bound = None;
                        self.planned_this_sweep = 0;
                        self.planned_quarantines_this_sweep = 0;
                        self.new_quarantines_this_sweep = 0;
                        self.circuit_open = false;
                    }
                    Ok(rows) => break 'fetch rows,
                    Err(err) => {
                        tracing::warn!(error = %err, "reconciler list_candidates failed");
                        return TickReport::default();
                    }
                }
            }
            Vec::new()
        };
        if candidates.is_empty() {
            return TickReport::default();
        }
        self.scan_after = candidates.last().map(|candidate| candidate.sandbox_id);
        self.seen_this_sweep
            .extend(candidates.iter().map(|c| c.sandbox_id));

        let mut report = TickReport {
            scanned: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
            ..TickReport::default()
        };

        // Trim the rate-limit window once per tick.
        while let Some(front) = self.action_history.front() {
            if now.duration_since(*front) > RATE_LIMIT_WINDOW {
                self.action_history.pop_front();
            } else {
                break;
            }
        }

        let mut active_quarantines = match self.sink.active_quarantine_count().await {
            Ok(count) => count,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "reconciler could not evaluate active-quarantine circuit breaker; \
                     quarantine will remain disabled for this sweep"
                );
                self.circuit_open = true;
                self.config.max_active_quarantines
            }
        };
        if self.config.dry_run {
            active_quarantines = active_quarantines.saturating_add(self.new_quarantines_this_sweep);
        }
        if active_quarantines >= self.config.max_active_quarantines {
            self.circuit_open = true;
        }

        for candidate in candidates {
            let pod_evidence = match candidate.kind {
                OrphanKind::LegacyDeleting => PodEvidence::NotApplicable,
                OrphanKind::Normal => {
                    let presence = self.presence.check_pod(&candidate.pod_name).await;
                    match presence {
                        PodPresenceStatus::Present => {
                            if candidate.lifecycle != SandboxLifecycle::Creating {
                                report.pod_present += 1;
                                self.first_missing_at.remove(&candidate.sandbox_id);
                                continue;
                            }
                            // A creating candidate is selected only after its
                            // materialization attempt expired. A present pod is
                            // therefore the orphan to remove, not proof that an
                            // active request still owns the row.
                            PodEvidence::Present
                        }
                        PodPresenceStatus::TerminalSucceeded => {
                            report.pod_unready += 1;
                            PodEvidence::TerminalSucceeded
                        }
                        PodPresenceStatus::TerminalFailed => {
                            report.pod_unready += 1;
                            PodEvidence::TerminalFailed
                        }
                        PodPresenceStatus::Unready => {
                            report.pod_unready += 1;
                            PodEvidence::Unready
                        }
                        PodPresenceStatus::Missing => PodEvidence::Missing,
                        PodPresenceStatus::Unknown => {
                            report.pod_unknown += 1;
                            reconciler_metrics::rows_skipped("pod_presence_unknown", 1);
                            continue;
                        }
                    }
                }
                OrphanKind::Forgotten => PodEvidence::NotApplicable,
            };

            // Forgotten and legacy-deleting rows have no pod-presence probe,
            // but still need the same repeated-observation grace as normal
            // candidates before a guarded mutation. Expired creates with a
            // present pod get the same repeated-observation delay.
            if !self.observe_missing_within_grace(&candidate, now, &mut report) {
                continue;
            }

            if self.action_history.len()
                >= usize::try_from(self.config.max_classifications_per_minute).unwrap_or(usize::MAX)
            {
                report.rate_limited += 1;
                reconciler_metrics::rows_skipped("rate_limited", 1);
                continue;
            }

            let reason = candidate.kind.classification_reason();
            let guard = match candidate.kind {
                OrphanKind::Normal => ClassifyGuard::PodName {
                    name: candidate.pod_name.clone(),
                    uid: candidate.pod_uid.clone(),
                },
                OrphanKind::Forgotten => ClassifyGuard::Forgotten,
                OrphanKind::LegacyDeleting => ClassifyGuard::LegacyDeleting {
                    pod_name: non_empty_string(candidate.pod_name.clone()),
                },
            };
            let plan = match self
                .sink
                .plan_orphan(candidate.sandbox_id, guard.clone())
                .await
            {
                Ok(Some(plan)) => plan,
                Ok(None) => {
                    report.skipped_terminal += 1;
                    self.first_missing_at.remove(&candidate.sandbox_id);
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        error = %err,
                        "reconciler disposition planning failed; will retry next tick"
                    );
                    continue;
                }
            };

            if !matches!(plan.disposition, OrphanDisposition::Defer(_)) {
                self.planned_this_sweep = self.planned_this_sweep.saturating_add(1);
            }
            let plans_quarantine =
                matches!(plan.disposition, OrphanDisposition::QuarantineAmbiguous);
            if plans_quarantine {
                self.planned_quarantines_this_sweep =
                    self.planned_quarantines_this_sweep.saturating_add(1);
                let quarantine_percentage_exceeded = self.config.max_quarantine_percentage == 0
                    || (self.planned_this_sweep >= QUARANTINE_PERCENTAGE_MIN_PLANS
                        && u64::from(self.planned_quarantines_this_sweep) * 100
                            > u64::from(self.planned_this_sweep)
                                * u64::from(self.config.max_quarantine_percentage));
                if self.new_quarantines_this_sweep >= self.config.max_new_quarantines_per_sweep
                    || active_quarantines >= self.config.max_active_quarantines
                    || quarantine_percentage_exceeded
                {
                    self.circuit_open = true;
                }
            }
            let quarantine_blocked = plans_quarantine && self.circuit_open;

            if self.config.dry_run {
                tracing::debug!(
                    sandbox_id = %candidate.sandbox_id,
                    pod_name = %candidate.pod_name,
                    reason = %reason,
                    from_lifecycle = %plan.from_lifecycle,
                    disposition = ?plan.disposition,
                    pod_evidence = %pod_evidence,
                    "reconciler dry-run planned orphaned sandbox disposition"
                );
                reconciler_metrics::rows_classified(
                    reason,
                    if quarantine_blocked {
                        ClassificationOutcome::QuarantineBlocked
                    } else {
                        plan.disposition.metric_outcome()
                    },
                    reconciler_metrics::Mode::DryRun,
                    plan.from_lifecycle,
                    pod_evidence,
                );
                report.dry_run_classified += 1;
                if quarantine_blocked {
                    report.circuit_breaker += 1;
                    reconciler_metrics::rows_skipped("circuit_breaker", 1);
                } else if plans_quarantine {
                    active_quarantines = active_quarantines.saturating_add(1);
                    self.new_quarantines_this_sweep =
                        self.new_quarantines_this_sweep.saturating_add(1);
                }
                if !matches!(plan.disposition, OrphanDisposition::Defer(_)) {
                    self.first_missing_at.remove(&candidate.sandbox_id);
                }
                continue;
            }

            if quarantine_blocked {
                reconciler_metrics::rows_classified(
                    reason,
                    ClassificationOutcome::QuarantineBlocked,
                    reconciler_metrics::Mode::Live,
                    plan.from_lifecycle,
                    pod_evidence,
                );
                report.circuit_breaker += 1;
                reconciler_metrics::rows_skipped("circuit_breaker", 1);
                continue;
            }

            match self.sink.classify_orphan(candidate.sandbox_id, guard).await {
                Ok(ClassifyOutcome::Deleted {
                    from_lifecycle,
                    reason: delete_reason,
                }) => {
                    tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        reason = %reason,
                        delete_reason = %delete_reason,
                        from_lifecycle = %from_lifecycle,
                        "reconciler finalized orphaned sandbox deletion"
                    );
                    reconciler_metrics::rows_classified(
                        reason,
                        ClassificationOutcome::Deleted,
                        reconciler_metrics::Mode::Live,
                        from_lifecycle,
                        pod_evidence,
                    );
                    report.deleted += 1;
                    self.first_missing_at.remove(&candidate.sandbox_id);
                    self.action_history.push_back(now);
                }
                Ok(ClassifyOutcome::RecoveredSuspended { from_lifecycle }) => {
                    tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        reason = %reason,
                        from_lifecycle = %from_lifecycle,
                        "reconciler recovered orphaned sandbox to suspended"
                    );
                    reconciler_metrics::rows_classified(
                        reason,
                        ClassificationOutcome::Suspended,
                        reconciler_metrics::Mode::Live,
                        from_lifecycle,
                        pod_evidence,
                    );
                    report.recovered_suspended += 1;
                    self.first_missing_at.remove(&candidate.sandbox_id);
                    self.action_history.push_back(now);
                }
                Ok(ClassifyOutcome::Quarantined { from_lifecycle }) => {
                    tracing::warn!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        reason = %reason,
                        from_lifecycle = %from_lifecycle,
                        "reconciler quarantined orphaned sandbox with no current durable artifact"
                    );
                    reconciler_metrics::rows_classified(
                        reason,
                        ClassificationOutcome::Quarantined,
                        reconciler_metrics::Mode::Live,
                        from_lifecycle,
                        pod_evidence,
                    );
                    report.quarantined += 1;
                    active_quarantines = active_quarantines.saturating_add(1);
                    self.new_quarantines_this_sweep =
                        self.new_quarantines_this_sweep.saturating_add(1);
                    self.first_missing_at.remove(&candidate.sandbox_id);
                    self.action_history.push_back(now);
                }
                Ok(ClassifyOutcome::Deferred {
                    from_lifecycle,
                    reason: defer_reason,
                }) => {
                    tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        reason = %reason,
                        defer_reason = %defer_reason,
                        from_lifecycle = %from_lifecycle,
                        "reconciler deferred orphaned sandbox disposition"
                    );
                    reconciler_metrics::rows_classified(
                        reason,
                        ClassificationOutcome::Deferred,
                        reconciler_metrics::Mode::Live,
                        from_lifecycle,
                        pod_evidence,
                    );
                    report.deferred += 1;
                }
                Ok(ClassifyOutcome::SkippedTerminal) => {
                    tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        "reconciler skipped orphaned sandbox because row is already terminal"
                    );
                    report.skipped_terminal += 1;
                    self.first_missing_at.remove(&candidate.sandbox_id);
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        error = %err,
                        "reconciler classify_orphan failed; will retry next tick"
                    );
                }
            }
        }

        reconciler_metrics::rows_scanned(report.scanned);
        if report.pod_present > 0 {
            reconciler_metrics::rows_skipped("pod_present", report.pod_present);
        }
        if report.pod_unready > 0 {
            reconciler_metrics::rows_skipped("pod_unready", report.pod_unready);
        }
        if report.within_grace > 0 {
            reconciler_metrics::rows_skipped("within_grace", report.within_grace);
        }

        tracing::info!(
            scanned = report.scanned,
            pod_present = report.pod_present,
            pod_unready = report.pod_unready,
            pod_unknown = report.pod_unknown,
            within_grace = report.within_grace,
            rate_limited = report.rate_limited,
            dry_run_classified = report.dry_run_classified,
            deleted = report.deleted,
            recovered_suspended = report.recovered_suspended,
            quarantined = report.quarantined,
            deferred = report.deferred,
            circuit_breaker = report.circuit_breaker,
            skipped_terminal = report.skipped_terminal,
            dry_run = self.config.dry_run,
            "orphaned-sandbox reconcile tick completed"
        );

        report
    }

    /// Updates `first_missing_at` for `candidate` and returns true iff the
    /// grace period has elapsed (caller may proceed to classify). The clock
    /// resets when *either* `pod_name` or `pod_uid` changes since the
    /// last observation — this catches both new-generation pods (different
    /// pod_name) and same-generation recreations by `ensure_pod_exists`
    /// (same pod_name, different pod_uid). For `OrphanKind::Forgotten`,
    /// pod_name is empty and pod_uid is None.
    fn observe_missing_within_grace(
        &mut self,
        candidate: &OrphanCandidate,
        now: Instant,
        report: &mut TickReport,
    ) -> bool {
        let first_seen = match self.first_missing_at.get(&candidate.sandbox_id) {
            Some((pod, uid, t)) if pod == &candidate.pod_name && uid == &candidate.pod_uid => *t,
            _ => {
                self.first_missing_at.insert(
                    candidate.sandbox_id,
                    (candidate.pod_name.clone(), candidate.pod_uid.clone(), now),
                );
                now
            }
        };

        if now.duration_since(first_seen) < self.config.grace_period {
            report.within_grace += 1;
            return false;
        }
        true
    }
}

/// Real `PodPresence` backed by the Kubernetes substrate's reflector cache.
pub struct ReflectorPresence {
    substrate: Arc<KubernetesSubstrate>,
}

impl ReflectorPresence {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }
}

impl PodPresence for ReflectorPresence {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus {
        if let Some(pod) = self.substrate.lookup_sandbox_pod(pod_name) {
            return classify_pod_presence(&pod);
        }
        match self.substrate.read_sandbox_pod(pod_name).await {
            Ok(Some(pod)) => classify_pod_presence(&pod),
            Ok(None) => PodPresenceStatus::Missing,
            Err(err) => {
                tracing::warn!(
                    pod_name,
                    error = %err,
                    "reconciler could not confirm pod presence"
                );
                PodPresenceStatus::Unknown
            }
        }
    }
}

pub(crate) fn classify_pod_presence(pod: &SandboxPodView) -> PodPresenceStatus {
    if pod.is_deleting() {
        return PodPresenceStatus::Unready;
    }

    if pod.phase() == Some("Running") && pod.pod_ip().is_some() {
        return PodPresenceStatus::Present;
    }
    match pod.phase() {
        Some("Succeeded") if pod_all_containers_terminated(pod) => {
            PodPresenceStatus::TerminalSucceeded
        }
        Some("Failed") if pod_all_containers_terminated(pod) => PodPresenceStatus::TerminalFailed,
        _ => PodPresenceStatus::Unready,
    }
}

/// Every app, init and ephemeral container has terminated, and there is at
/// least one app container status to say so.
fn pod_all_containers_terminated(pod: &SandboxPodView) -> bool {
    let statuses = pod.container_statuses();
    !statuses.containers.is_empty() && statuses.all().all(|state| state.terminated.is_some())
}

/// Real `OrphanActionSink` that joins the candidate scan against
/// `sandbox_k8s_executions` and classifies missing executions against durable
/// artifact evidence.
pub struct SubstrateActionSink {
    pool: PgPool,
    pod_cleaner: Option<Arc<dyn ProjectionPodCleaner>>,
}

impl SubstrateActionSink {
    pub fn new(pool: PgPool, substrate: Arc<KubernetesSubstrate>) -> Self {
        Self {
            pool,
            pod_cleaner: Some(substrate),
        }
    }

    #[cfg(test)]
    fn new_without_substrate(pool: PgPool) -> Self {
        Self {
            pool,
            pod_cleaner: None,
        }
    }

    #[cfg(test)]
    fn new_with_pod_cleaner(pool: PgPool, pod_cleaner: Arc<dyn ProjectionPodCleaner>) -> Self {
        Self {
            pool,
            pod_cleaner: Some(pod_cleaner),
        }
    }
}

#[async_trait::async_trait]
trait ProjectionPodCleaner: Send + Sync {
    async fn delete_projection_pod(
        &self,
        name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError>;
}

#[async_trait::async_trait]
impl ProjectionPodCleaner for KubernetesSubstrate {
    async fn delete_projection_pod(
        &self,
        name: &str,
    ) -> Result<K8sBlockingDeleteOutcome, K8sBlockingDeleteError> {
        self.delete_sandbox_pod_for_projection_cleanup(name).await
    }
}

impl OrphanActionSink for SubstrateActionSink {
    async fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error> {
        let limit = i64::from(batch_size);
        let rows = self
            .list_candidates_after(after_sandbox_id, upper_bound, limit)
            .await?;

        rows.into_iter()
            .map(|row| {
                let OrphanCandidateRow {
                    sandbox_id,
                    pod_name,
                    pod_uid,
                    lifecycle,
                } = row;
                let kind = if lifecycle == SandboxLifecycle::Deleting {
                    OrphanKind::LegacyDeleting
                } else if pod_name.is_none() {
                    OrphanKind::Forgotten
                } else {
                    OrphanKind::Normal
                };
                Ok(OrphanCandidate {
                    sandbox_id,
                    pod_name: pod_name.unwrap_or_default(),
                    pod_uid,
                    kind,
                    lifecycle,
                })
            })
            .collect()
    }

    async fn current_max_sandbox_id(&self) -> Result<Option<SandboxId>, sqlx::Error> {
        // Pre-PG-15 has no `max(uuid)` aggregate, so use ORDER BY DESC
        // LIMIT 1 instead. The primary key index makes this an index-only
        // scan that returns immediately. `fetch_optional` correctly returns
        // `None` for an empty table.
        let row: Option<(SandboxId,)> = sqlx::query_as(sql!(
            "SELECT id FROM sandboxes WHERE managed_by = $1 ORDER BY id DESC LIMIT 1"
        ))
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id,)| id))
    }

    async fn classify_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<ClassifyOutcome, OrphanClassifyError> {
        let guard_reason = match &guard {
            ClassifyGuard::PodName { .. } => "orphaned_missing_pod",
            ClassifyGuard::Forgotten => "orphaned_forgotten_execution",
            ClassifyGuard::LegacyDeleting { .. } => "orphaned_legacy_deleting",
        };
        let mut tx = self.pool.begin().await?;
        let Some(plan) = plan_orphan_in_tx(&mut tx, sandbox_id, &guard, true).await? else {
            tx.rollback().await?;
            return Ok(ClassifyOutcome::SkippedTerminal);
        };

        if let OrphanDisposition::Defer(reason) = plan.disposition {
            tx.rollback().await?;
            return Ok(ClassifyOutcome::Deferred {
                from_lifecycle: plan.from_lifecycle,
                reason,
            });
        }

        if let OrphanDisposition::FinalizeDelete(delete_reason) = plan.disposition {
            let from_lifecycle = plan.from_lifecycle;
            let final_guard = if delete_reason == OrphanDeleteReason::AbandonedCreate {
                if !claim_abandoned_create_delete(&mut tx, sandbox_id, &guard).await? {
                    tx.rollback().await?;
                    return Ok(ClassifyOutcome::SkippedTerminal);
                }
                tx.commit().await?;
                ClassifyGuard::LegacyDeleting {
                    pod_name: guard.observed_pod_cleanup_name(),
                }
            } else {
                tx.rollback().await?;
                guard.clone()
            };
            if !self
                .delete_observed_pod(sandbox_id, &guard, guard_reason)
                .await
            {
                return Ok(ClassifyOutcome::Deferred {
                    from_lifecycle,
                    reason: OrphanDeferReason::PodCleanupUnconfirmed,
                });
            }

            // Pod cleanup cannot run while holding the sandbox row lock.
            // Re-plan after cleanup so a concurrent lifecycle/execution
            // change prevents finalization rather than deleting fresh state.
            let mut finalize_tx = self.pool.begin().await?;
            let Some(final_plan) =
                plan_orphan_in_tx(&mut finalize_tx, sandbox_id, &final_guard, true).await?
            else {
                finalize_tx.rollback().await?;
                return Ok(ClassifyOutcome::SkippedTerminal);
            };
            let OrphanDisposition::FinalizeDelete(_) = final_plan.disposition else {
                finalize_tx.rollback().await?;
                return Ok(ClassifyOutcome::SkippedTerminal);
            };
            finalize_sandbox_db_delete_tx(&mut finalize_tx, sandbox_id).await?;
            finalize_tx.commit().await?;
            emit_orphan_reconcile_event(
                sandbox_id,
                SandboxEventType::DeleteCompleted,
                guard_reason,
                details_for_guard(&guard)
                    .with_from_state(from_lifecycle.as_str())
                    .with_to_state("deleted")
                    .result(delete_reason.as_str()),
            );
            return Ok(ClassifyOutcome::Deleted {
                from_lifecycle,
                reason: delete_reason,
            });
        }

        if matches!(plan.disposition, OrphanDisposition::RecoverSuspended)
            && let Some(row) = recover_orphan_to_suspended(&mut tx, sandbox_id, &guard).await?
        {
            // Node assignment retired ⇒ no node owns this sandbox's desired
            // state until a later restore re-places it (which enqueues fresh
            // refs). Drop the old node's queue ref/condition in the same
            // transaction — same rule as the quarantine branch below.
            delete_reconcile_state_for_elements(
                &mut tx,
                &[ReconcileElementRef::sandbox_desired_state(sandbox_id)],
            )
            .await?;
            tx.commit().await?;
            let result = if row.clear_marker() {
                "marker_cleared"
            } else {
                "artifact_current"
            };
            let mut details = details_for_guard(&guard)
                .with_from_state(row.from_lifecycle.as_str())
                .with_to_state("suspended")
                .result(result);
            if let Some(node_id) = row.active_node_id {
                details = details.source_node_id(node_id.to_string());
            }
            emit_orphan_reconcile_event(
                sandbox_id,
                SandboxEventType::LifecycleRevertedToSuspended,
                guard_reason,
                details,
            );
            self.delete_observed_pod(sandbox_id, &guard, guard_reason)
                .await;
            return Ok(ClassifyOutcome::RecoveredSuspended {
                from_lifecycle: row.from_lifecycle,
            });
        }

        if matches!(plan.disposition, OrphanDisposition::QuarantineAmbiguous)
            && let Some(row) = quarantine_orphan(&mut tx, sandbox_id, &guard).await?
        {
            // The quarantine retired the old node assignment, so no node
            // owns this sandbox's desired state any more. Drop the queue
            // ref/condition in the same transaction so the old node stops
            // retrying a poison ref that can never converge.
            delete_reconcile_state_for_elements(
                &mut tx,
                &[ReconcileElementRef::sandbox_desired_state(sandbox_id)],
            )
            .await?;
            let quarantine_id = Uuid::now_v7();
            let history_id = Uuid::now_v7();
            let evidence = quarantine_evidence(&guard, row.active_node_id);
            let (reason_code, reason_message) = quarantine_reason(&guard);
            let state_columns = quarantine_state_insert_columns();
            let state_conflict_assignments = quarantine_state_conflict_assignments();
            let query = format!(
                "INSERT INTO sandbox_quarantine_state ({state_columns})
                 VALUES ($1, $2, NOW(), NOW(), $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (sandbox_id) DO UPDATE SET {state_conflict_assignments}",
            );
            sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(sandbox_id)
                .bind(quarantine_id)
                .bind(row.from_lifecycle)
                .bind(SandboxQuarantineSource::ClusterController.as_str())
                .bind(reason_code.as_str())
                .bind(reason_message)
                .bind(Json(evidence.clone()))
                .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
                .bind(row.active_node_id)
                .execute(&mut *tx)
                .await?;

            let history_columns = quarantine_history_insert_columns();
            let query = format!(
                "INSERT INTO sandbox_quarantine_history ({history_columns})
                 VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7, $8, $9)",
            );
            sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(history_id)
                .bind(quarantine_id)
                .bind(sandbox_id)
                .bind(SandboxQuarantineSource::ClusterController.as_str())
                .bind(SandboxQuarantineHistoryAction::EnteredQuarantine.as_str())
                .bind(reason_code.as_str())
                .bind(reason_message)
                .bind(SandboxQuarantineNextAction::RequiresOperatorAttention.as_str())
                .bind(Json(evidence))
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;
            let mut details = details_for_guard(&guard)
                .with_from_state(row.from_lifecycle.as_str())
                .with_to_state("quarantined");
            if let Some(node_id) = row.active_node_id {
                details = details.source_node_id(node_id.to_string());
            }
            emit_orphan_reconcile_event(
                sandbox_id,
                SandboxEventType::SandboxQuarantined,
                guard_reason,
                details,
            );
            self.delete_observed_pod(sandbox_id, &guard, guard_reason)
                .await;
            return Ok(ClassifyOutcome::Quarantined {
                from_lifecycle: row.from_lifecycle,
            });
        }

        tx.rollback().await?;
        Ok(ClassifyOutcome::SkippedTerminal)
    }

    async fn plan_orphan(
        &self,
        sandbox_id: SandboxId,
        guard: ClassifyGuard,
    ) -> Result<Option<OrphanPlan>, OrphanClassifyError> {
        let mut tx = self.pool.begin().await?;
        let plan = plan_orphan_in_tx(&mut tx, sandbox_id, &guard, false).await?;
        tx.rollback().await?;
        Ok(plan)
    }

    async fn active_quarantine_count(&self) -> Result<u32, OrphanClassifyError> {
        let count: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*)::BIGINT
               FROM sandbox_quarantine_state quarantine
               JOIN sandboxes sandbox ON sandbox.id = quarantine.sandbox_id
              WHERE sandbox.lifecycle = 'quarantined'"
        ))
        .fetch_one(&self.pool)
        .await?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }
}

/// Turn an abandoned initial create into durable delete intent before the pod
/// is touched. The exact active execution and expired materialization attempt
/// are rechecked under the locks acquired by `plan_orphan_in_tx`; once this
/// commits, a successor activation cannot reuse the parent while cleanup is in
/// flight. The legacy-deleting path will retry both pod deletion and database
/// finalization after a crash or kube error.
async fn claim_abandoned_create_delete(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
) -> Result<bool, sqlx::Error> {
    // **The `Forgotten` guard needs its own claim, and gets one.**
    //
    // `AbandonedCreate` is decided from lifecycle and recoverability alone
    // (`from_lifecycle == Creating && !recoverable`), so it is reached through
    // *either* guard — and `ACTIVE_EXECUTION_REQUIRED_LIFECYCLES` documents why
    // `creating` is in the Forgotten path: a failed pod creation calls
    // `forget_pod_tracking` while the row is still `creating`, leaving a
    // non-recoverable shell with no active execution.
    //
    // Returning `false` for that guard, as an earlier revision did, made the
    // caller roll back and report `SkippedTerminal` on every sweep, forever, with
    // no other owner — the transient sweeper only ages `creating` to `failed`,
    // which is a different terminal state and does not clean up the shell. So a
    // shape this path exists to collect became the one it could never collect.
    //
    // The pod-based claim below cannot serve it: it requires an *active*
    // execution matching the observed pod, and Forgotten means precisely that
    // none is active. This branch therefore claims on the same invariants minus
    // the pod: still `creating`, still no active execution, the newest execution
    // still `forgotten`, and no in-flight `creating` snapshot.
    if matches!(guard, ClassifyGuard::Forgotten) {
        let forgotten = sqlx::query(sql!(
            "UPDATE sandboxes s
                SET lifecycle = 'deleting'
              WHERE s.id = $1
                AND s.lifecycle = 'creating'
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions active_execution
                     WHERE active_execution.sandbox_id = s.id
                       AND active_execution.active
                )
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions forgotten_execution
                     WHERE forgotten_execution.sandbox_id = s.id
                       AND forgotten_execution.state = 'forgotten'
                       AND forgotten_execution.generation = (
                           SELECT MAX(generation)
                             FROM sandbox_k8s_executions
                            WHERE sandbox_id = s.id
                       )
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM snapshots creating_snapshot
                     WHERE creating_snapshot.sandbox_id = s.id
                       AND creating_snapshot.state = 'creating'
                )"
        ))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
        return Ok(forgotten.rows_affected() == 1);
    }
    let ClassifyGuard::PodName { name, uid } = guard else {
        return Ok(false);
    };
    let result = sqlx::query(sql!(
        "UPDATE sandboxes s
            SET lifecycle = 'deleting'
           FROM sandbox_k8s_executions e
          WHERE s.id = $1
            AND s.lifecycle = 'creating'
            AND e.sandbox_id = s.id
            AND e.active
            AND e.pod_name = $2
            AND NULLIF(BTRIM(e.pod_uid), '') IS NOT DISTINCT FROM $3
            AND e.updated_at <= NOW() - ($4 * INTERVAL '1 second')
            AND (e.activation_attempt_expires_at IS NULL
                 OR e.activation_attempt_expires_at < NOW())
            AND NOT EXISTS (
                SELECT 1
                  FROM snapshots creating_snapshot
                 WHERE creating_snapshot.sandbox_id = s.id
                   AND creating_snapshot.state = 'creating'
            )"
    ))
    .bind(sandbox_id)
    .bind(name)
    .bind(uid)
    .bind(CREATE_AGE_THRESHOLD_SECS)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected() == 1)
}

impl SubstrateActionSink {
    /// Returns true only when no cleanup is needed or Kubernetes confirms the
    /// observed pod is absent. Recovery and quarantine use the result only
    /// for telemetry; irreversible database deletion requires confirmation.
    async fn delete_observed_pod(
        &self,
        sandbox_id: SandboxId,
        guard: &ClassifyGuard,
        reason: &'static str,
    ) -> bool {
        let Some(pod_cleaner) = &self.pod_cleaner else {
            return true;
        };
        let Some(pod_name) = guard.observed_pod_cleanup_name() else {
            return true;
        };

        match pod_cleaner.delete_projection_pod(&pod_name).await {
            Ok(K8sBlockingDeleteOutcome::Deleted | K8sBlockingDeleteOutcome::NotFound) => true,
            // `PreconditionFailed` is unreachable here — this cleanup sends no
            // UID precondition — but treated like an unconfirmed outcome so a
            // future precondition-carrying caller degrades safely.
            Ok(
                outcome @ (K8sBlockingDeleteOutcome::TimedOut
                | K8sBlockingDeleteOutcome::ObserveError
                | K8sBlockingDeleteOutcome::PreconditionFailed),
            ) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    result = outcome.as_str(),
                    "reconciler could not confirm stale Kubernetes pod cleanup"
                );
                emit_orphan_reconcile_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    reason,
                    details_for_guard(guard)
                        .result(outcome.as_str())
                        .logged_by_caller(),
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    error = %err,
                    "reconciler stale Kubernetes pod cleanup failed"
                );
                emit_orphan_reconcile_event(
                    sandbox_id,
                    SandboxEventType::CleanupFailed,
                    reason,
                    details_for_guard(guard)
                        .error(err.to_string())
                        .logged_by_caller(),
                );
                false
            }
        }
    }
}

#[derive(sqlx::FromRow)]
struct OrphanResolutionRow {
    from_lifecycle: SandboxLifecycle,
    active_node_id: Option<NodeId>,
    clear_marker: Option<bool>,
}

impl OrphanResolutionRow {
    fn clear_marker(&self) -> bool {
        self.clear_marker.unwrap_or(false)
    }
}

fn details_for_guard(guard: &ClassifyGuard) -> SandboxEventDetails {
    match guard {
        ClassifyGuard::PodName { name, uid } => {
            let details = SandboxEventDetails::default().pod_name(name.clone());
            if let Some(uid) = uid {
                details.pod_uid(uid.clone())
            } else {
                details
            }
        }
        ClassifyGuard::Forgotten => SandboxEventDetails::default(),
        ClassifyGuard::LegacyDeleting { pod_name } => {
            let details = SandboxEventDetails::default();
            if let Some(pod_name) = pod_name {
                details.pod_name(pod_name.clone())
            } else {
                details
            }
        }
    }
}

fn quarantine_evidence(guard: &ClassifyGuard, active_node_id: Option<NodeId>) -> serde_json::Value {
    let mut evidence = match guard {
        ClassifyGuard::PodName { name, uid } => serde_json::json!({
            "orphan_kind": "missing_pod",
            "pod_name": name,
            "pod_uid": uid,
            "artifact_current": false,
        }),
        ClassifyGuard::Forgotten => serde_json::json!({
            "orphan_kind": "forgotten_execution",
            "artifact_current": false,
        }),
        ClassifyGuard::LegacyDeleting { pod_name } => serde_json::json!({
            "orphan_kind": "legacy_deleting",
            "pod_name": pod_name,
            "artifact_current": false,
        }),
    };
    if let Some(node_id) = active_node_id
        && let Some(object) = evidence.as_object_mut()
    {
        object.insert(
            "active_node_id".to_string(),
            serde_json::Value::String(node_id.to_string()),
        );
    }
    evidence
}

fn quarantine_reason(guard: &ClassifyGuard) -> (SandboxQuarantineReasonCode, &'static str) {
    match guard {
        ClassifyGuard::PodName { .. } | ClassifyGuard::Forgotten => (
            SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact,
            "missing execution has no current durable full artifact",
        ),
        ClassifyGuard::LegacyDeleting { .. } => (
            SandboxQuarantineReasonCode::OrphanedExecutionLegacyDeletingState,
            "legacy deleting lifecycle has no automatic hard-delete owner",
        ),
    }
}

#[derive(sqlx::FromRow)]
struct OrphanPlanRow {
    from_lifecycle: SandboxLifecycle,
    recoverable: bool,
    creating_snapshot: bool,
}

async fn plan_orphan_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
    lock: bool,
) -> Result<Option<OrphanPlan>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let recoverable = eligibility.eligible;
    let guard_sql = classify_guard_sql(guard);
    let guard_from_sql = classify_guard_from_sql(guard);
    let lock_sql = if lock {
        format!(" FOR UPDATE OF {}", classify_guard_lock_sql(guard))
    } else {
        String::new()
    };
    let query = format!(
        "SELECT s.lifecycle AS from_lifecycle,
                ({recoverable}) AS recoverable,
                EXISTS (
                    SELECT 1
                      FROM snapshots creating_snapshot
                     WHERE creating_snapshot.sandbox_id = s.id
                       AND creating_snapshot.state = 'creating'
                ) AS creating_snapshot
           FROM {guard_from_sql}
          WHERE s.id = $1
            AND s.lifecycle <> ALL($2)
            AND {guard_sql}{lock_sql}"
    );
    let row = fetch_guarded_plan(tx, query, sandbox_id, guard).await?;
    let Some(row) = row else {
        return Ok(None);
    };

    let disposition = if row.creating_snapshot {
        OrphanDisposition::Defer(OrphanDeferReason::InFlightSnapshot)
    } else if matches!(guard, ClassifyGuard::LegacyDeleting { .. }) {
        OrphanDisposition::FinalizeDelete(OrphanDeleteReason::ExplicitDeleting)
    } else if row.from_lifecycle == SandboxLifecycle::Creating && !row.recoverable {
        OrphanDisposition::FinalizeDelete(OrphanDeleteReason::AbandonedCreate)
    } else if row.recoverable {
        OrphanDisposition::RecoverSuspended
    } else {
        OrphanDisposition::QuarantineAmbiguous
    };

    Ok(Some(OrphanPlan {
        from_lifecycle: row.from_lifecycle,
        disposition,
    }))
}

async fn fetch_guarded_plan(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    query: String,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
) -> Result<Option<OrphanPlanRow>, sqlx::Error> {
    let query = sqlx::query_as::<_, OrphanPlanRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(ORPHAN_RECONCILER_STOP_LIFECYCLES);
    match guard {
        ClassifyGuard::PodName { name, uid } => {
            query
                .bind(name)
                .bind(uid)
                .bind(LIVE_POD_REQUIRED_LIFECYCLES)
                .bind(PENDING_POD_LIFECYCLES)
                .bind(CREATE_AGE_THRESHOLD_SECS)
                .fetch_optional(&mut **tx)
                .await
        }
        ClassifyGuard::Forgotten => {
            query
                .bind(ACTIVE_EXECUTION_REQUIRED_LIFECYCLES)
                .fetch_optional(&mut **tx)
                .await
        }
        ClassifyGuard::LegacyDeleting { .. } => query.fetch_optional(&mut **tx).await,
    }
}

async fn recover_orphan_to_suspended(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
) -> Result<Option<OrphanResolutionRow>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    let artifact_marker_assignments =
        clear_current_local_artifact_marker_assignments("s", "pre.clear_marker");
    let finalization = suspended_k8s_finalization_sql(
        "s",
        "updated_sandbox",
        &artifact_marker_assignments,
        SuspendedTargetNodeAssignment::Clear,
    );
    let sandbox_assignments = finalization.sandbox_assignments();
    let retire_active_execution_cte = finalization.retire_active_execution_cte();
    let invalidate_open_port_exposures_cte = finalization.invalidate_open_port_exposures_cte();
    let guard_sql = classify_guard_sql(guard);
    let guard_from_sql = classify_guard_from_sql(guard);
    let guard_lock_sql = classify_guard_lock_sql(guard);
    let query = format!(
        "WITH pre AS (
             SELECT s.id,
                    s.lifecycle AS from_lifecycle,
                    s.active_node_id,
                    {clear_marker} AS clear_marker
               FROM {guard_from_sql}
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND {guard_sql}
                AND {eligible}
              FOR UPDATE OF {guard_lock_sql}
         ),
         updated_sandbox AS (
             UPDATE sandboxes s
                SET {sandbox_assignments}
               FROM pre
              WHERE s.id = pre.id
              RETURNING s.id,
                        pre.from_lifecycle,
                        pre.active_node_id,
                        pre.clear_marker
         ),
         {retire_active_execution_cte},
         {invalidate_open_port_exposures_cte}
         SELECT from_lifecycle, active_node_id, clear_marker
           FROM updated_sandbox"
    );
    fetch_guarded_resolution(tx, query, sandbox_id, guard).await
}

async fn quarantine_orphan(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
) -> Result<Option<OrphanResolutionRow>, sqlx::Error> {
    let eligibility = started_repair_revert_eligibility("s");
    let eligible = eligibility.eligible;
    let artifact_predicate = if matches!(guard, ClassifyGuard::LegacyDeleting { .. }) {
        "TRUE".to_string()
    } else {
        format!("NOT ({eligible})")
    };
    // Retire the old node assignment along with the lifecycle flip: the pod
    // is authoritatively gone, so leaving `active_node_id` in place keeps the
    // old node eligible for the same sandbox desired state (including open
    // port exposures against a torn-down CNI netns) forever. Open port
    // exposure rows keep their `desired_state='open'` user intent and only
    // have the application observation reset, so the intent re-applies if an
    // operator later recovers the sandbox.
    let finalization = quarantined_k8s_finalization_sql(
        "s",
        "updated_sandbox",
        "",
        SuspendedTargetNodeAssignment::Clear,
    );
    let sandbox_assignments = finalization.sandbox_assignments();
    let retire_active_execution_cte = finalization.retire_active_execution_cte();
    let invalidate_open_port_exposures_cte = finalization.invalidate_open_port_exposures_cte();
    let guard_sql = classify_guard_sql(guard);
    let guard_from_sql = classify_guard_from_sql(guard);
    let guard_lock_sql = classify_guard_lock_sql(guard);
    let query = format!(
        "WITH pre AS (
             SELECT s.id,
                    s.lifecycle AS from_lifecycle,
                    s.active_node_id
               FROM {guard_from_sql}
              WHERE s.id = $1
                AND s.lifecycle <> ALL($2)
                AND {guard_sql}
                AND {artifact_predicate}
              FOR UPDATE OF {guard_lock_sql}
         ),
         updated_sandbox AS (
             UPDATE sandboxes s
                SET {sandbox_assignments}
               FROM pre
              WHERE s.id = pre.id
              RETURNING s.id,
                        pre.from_lifecycle,
                        pre.active_node_id,
                        FALSE AS clear_marker
         ),
         {retire_active_execution_cte},
         {invalidate_open_port_exposures_cte}
         SELECT from_lifecycle, active_node_id, clear_marker
           FROM updated_sandbox"
    );
    fetch_guarded_resolution(tx, query, sandbox_id, guard).await
}

fn classify_guard_sql(guard: &ClassifyGuard) -> &'static str {
    match guard {
        ClassifyGuard::PodName { .. } => {
            "(
                (
                    s.lifecycle = ANY($5)
                    AND (
                        e.state NOT IN ('creating', 'recovering')
                        OR e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                    )
                )
                  OR (
                      s.lifecycle = ANY($6)
                      AND e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                      AND (e.activation_attempt_expires_at IS NULL
                           OR e.activation_attempt_expires_at < NOW())
                  )
            )"
        }
        ClassifyGuard::Forgotten => {
            "s.lifecycle = ANY($3)
             AND EXISTS (
                 SELECT 1
                   FROM sandbox_k8s_executions f
                  WHERE f.sandbox_id = s.id
                    AND f.state = 'forgotten'
                    AND f.generation = (
                        SELECT MAX(generation)
                          FROM sandbox_k8s_executions
                         WHERE sandbox_id = s.id
                    )
             )"
        }
        ClassifyGuard::LegacyDeleting { .. } => "s.lifecycle = 'deleting'",
    }
}

fn classify_guard_from_sql(guard: &ClassifyGuard) -> &'static str {
    match guard {
        ClassifyGuard::PodName { .. } => {
            "sandboxes s
             JOIN sandbox_k8s_executions e
              ON e.sandbox_id = s.id
              AND e.active
              AND e.pod_name = $3
              AND NULLIF(BTRIM(e.pod_uid), '') IS NOT DISTINCT FROM $4"
        }
        ClassifyGuard::Forgotten | ClassifyGuard::LegacyDeleting { .. } => "sandboxes s",
    }
}

fn classify_guard_lock_sql(guard: &ClassifyGuard) -> &'static str {
    match guard {
        ClassifyGuard::PodName { .. } => "s, e",
        ClassifyGuard::Forgotten | ClassifyGuard::LegacyDeleting { .. } => "s",
    }
}

async fn fetch_guarded_resolution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    query: String,
    sandbox_id: SandboxId,
    guard: &ClassifyGuard,
) -> Result<Option<OrphanResolutionRow>, sqlx::Error> {
    let query = sqlx::query_as::<_, OrphanResolutionRow>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(ORPHAN_RECONCILER_STOP_LIFECYCLES);
    match guard {
        ClassifyGuard::PodName { name, uid } => {
            query
                .bind(name)
                .bind(uid)
                .bind(LIVE_POD_REQUIRED_LIFECYCLES)
                .bind(PENDING_POD_LIFECYCLES)
                .bind(CREATE_AGE_THRESHOLD_SECS)
                .fetch_optional(&mut **tx)
                .await
        }
        ClassifyGuard::Forgotten => {
            query
                .bind(ACTIVE_EXECUTION_REQUIRED_LIFECYCLES)
                .fetch_optional(&mut **tx)
                .await
        }
        ClassifyGuard::LegacyDeleting { .. } => query.fetch_optional(&mut **tx).await,
    }
}

impl SubstrateActionSink {
    async fn list_candidates_after(
        &self,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        limit: i64,
    ) -> Result<Vec<OrphanCandidateRow>, sqlx::Error> {
        list_candidates_after(&self.pool, after_sandbox_id, upper_bound, limit).await
    }
}

async fn list_candidates_after(
    pool: &PgPool,
    after_sandbox_id: Option<SandboxId>,
    upper_bound: SandboxId,
    limit: i64,
) -> Result<Vec<OrphanCandidateRow>, sqlx::Error> {
    // The second predicate branch picks up "forgotten" rows: a sandbox
    // whose **most recent** execution is `state='forgotten'` (set by
    // `finish_active_execution_in(..., "forgotten")` inside
    // `forget_pod_tracking`). The "most recent" qualifier is critical:
    // if an old `forgotten` execution exists but a later execution
    // succeeded (e.g. a recovery + suspend with `state='suspended'`),
    // that newer execution carries recoverable on-disk state. Looking
    // only for *any* `forgotten` would quarantine that recoverable
    // sandbox and orphan its snapshot. Looking at the *latest*
    // execution's state is the precise orphan signal.
    //
    // The final branch picks up rows stranded in `lifecycle='deleting'` by
    // the old orphan cleanup path so their durable delete claim can complete.
    //
    // `s.id <= $3` bounds the sweep against new sandboxes created after
    // the high-water mark was captured; the next sweep picks them up.
    sqlx::query_as(sql!(
        r"
            SELECT s.id AS sandbox_id,
                   e.pod_name,
                   NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                   s.lifecycle
            FROM sandboxes s
            LEFT JOIN sandbox_k8s_executions e
              ON e.sandbox_id = s.id AND e.active
            WHERE s.managed_by = $9
              AND s.lifecycle <> ALL($1)
              AND (
                  -- Normal: active execution observed.
                  --   Live-pod lifecycles ($5) — pod must be alive unless
                  --   a fresh materialization lease says creation/recovery
                  --   is still in flight.
                  --   Pending-pod lifecycles ($6) — pod creation may still
                  --   be in flight, so require the execution row to be
                  --   stale by $7 seconds (crashed-create signal) and have
                  --   no unexpired materialization attempt before acting;
                  --   younger/currently-owned rows are presumed healthy.
                  --   Dormant lifecycles (stopped/suspended/created) are
                  --   excluded entirely — those are intentionally-dormant
                  --   sandboxes the user can recover via `start`.
                  (e.pod_name IS NOT NULL AND (
                      (
                          s.lifecycle = ANY($5)
                          AND (
                              e.state NOT IN ('creating', 'recovering')
                              OR e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                          )
                      )
                      OR (
                          s.lifecycle = ANY($6)
                          AND e.updated_at <= NOW() - ($7 * INTERVAL '1 second')
                          AND (e.activation_attempt_expires_at IS NULL
                               OR e.activation_attempt_expires_at < NOW())
                      )
                  ))
                  -- Forgotten: state='forgotten' is the precise orphan
                  -- signal. Uses the broader active-required set ($8)
                  -- because failed `creating`/`starting` rows must be
                  -- reachable here.
                  OR (
                      s.lifecycle = ANY($8)
                      AND EXISTS (
                          SELECT 1 FROM sandbox_k8s_executions f
                          WHERE f.sandbox_id = s.id
                            AND f.state = 'forgotten'
                            AND f.generation = (
                                SELECT MAX(generation)
                                FROM sandbox_k8s_executions
                                WHERE sandbox_id = s.id
                            )
                      )
                  )
                  OR s.lifecycle = 'deleting'
              )
              -- A node-local observed state excludes stable rows: the kube
              -- pod may be missing, but sbx-mgr still has user-owned state
              -- that can converge through node publish or a later wake. An
              -- expired initial create is different: no successful API
              -- response established user ownership, and preserving its
              -- Present pod is exactly the crash leak this path cleans up.
              --
              -- `deleting` is exempt for the same reason, and it has to be:
              -- `claim_abandoned_create_delete` commits that lifecycle as an
              -- explicit cleanup intent, so a published observed state means the
              -- pod is *still there* — the thing to delete, not a reason to skip.
              -- Without the exemption a claimed row whose pod had already
              -- published state filtered itself back out on the next pass, and
              -- `deleting` is not in `SWEEPABLE_LIFECYCLES` either, so nothing
              -- else would have picked it up.
              AND (
                  s.lifecycle IN ('creating', 'deleting')
                  OR NOT EXISTS (
                      SELECT 1
                      FROM sandbox_observed_states sos
                      WHERE sos.sandbox_id = s.id
                        AND (
                            s.active_node_id IS NULL
                            OR sos.node_id IS NOT DISTINCT FROM s.active_node_id
                        )
                  )
              )
              AND ($2::uuid IS NULL OR s.id > $2)
              AND s.id <= $3
            ORDER BY s.id
            LIMIT $4
            "
    ))
    .bind(ORPHAN_RECONCILER_STOP_LIFECYCLES)
    .bind(after_sandbox_id)
    .bind(upper_bound)
    .bind(limit)
    .bind(LIVE_POD_REQUIRED_LIFECYCLES)
    .bind(PENDING_POD_LIFECYCLES)
    .bind(CREATE_AGE_THRESHOLD_SECS)
    .bind(ACTIVE_EXECUTION_REQUIRED_LIFECYCLES)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(pool)
    .await
}

/// Run the orphaned-sandbox reconcile loop on the current task until the
/// shutdown token is cancelled. Spawned by the cluster-singleton
/// `space-cluster-controller` binary alongside other reconcilers. Returns
/// when shutdown fires.
pub async fn run(
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        enabled = config.enabled,
        interval_secs = config.interval.as_secs(),
        grace_period_secs = config.grace_period.as_secs(),
        batch_size = config.batch_size,
        max_classifications_per_minute = config.max_classifications_per_minute,
        max_active_quarantines = config.max_active_quarantines,
        max_new_quarantines_per_sweep = config.max_new_quarantines_per_sweep,
        max_quarantine_percentage = config.max_quarantine_percentage,
        dry_run = config.dry_run,
        "starting orphaned-sandbox reconciler"
    );
    if !config.enabled {
        tracing::info!("orphaned-sandbox reconciler disabled");
        shutdown_token.cancelled().await;
        tracing::info!("orphaned-sandbox reconciler stopping");
        return;
    }

    let mut quarantine_repair = quarantine_recovery::Repair::new(pool.clone(), kube.clone());
    let presence = ReflectorPresence::new(kube.clone());
    let sink = SubstrateActionSink::new(pool, kube);
    let mut reconciler = Reconciler::new(config.clone(), presence, sink);

    let mut interval = tokio::time::interval(config.interval);
    // Avoid burst-mode catch-up when a tick runs long: under DB or apiserver
    // pressure (the conditions this loop is designed to recover from) we
    // want backoff, not back-to-back replays.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; skip it so startup doesn't race
    // the kube reflector's initial sync.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let _ = reconciler.tick(started).await;
                quarantine_repair.tick(config.dry_run, &shutdown_token).await;
                reconciler_metrics::loop_latency(started);
            }
            () = shutdown_token.cancelled() => {
                tracing::info!("orphaned-sandbox reconciler stopping");
                return;
            }
        }
    }
}

#[cfg(test)]
mod lifecycle_writer_tests;
#[cfg(test)]
mod tests;
