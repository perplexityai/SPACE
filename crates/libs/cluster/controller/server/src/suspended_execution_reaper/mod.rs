//! Cluster-singleton reaper that retires stale `active = TRUE`
//! `sandbox_k8s_executions` rows on `lifecycle = 'suspended'` sandboxes —
//! and clears the stale placement bookkeeping (`placement`,
//! `active_node_id`) those rows pin — once the backing kube pod is
//! confirmed gone.
//!
//! ## Why this exists
//!
//! The API-driven Kubernetes suspend path finishes with
//! `finish_suspended_execution`: it retires the active execution row
//! (`active = FALSE, state = 'suspended'`) and marks the sandbox
//! `placement = 'unplaced', active_node_id = NULL`
//! (leaving `target_node_id` pinned as the local-snapshot affinity hint).
//!
//! The node-side idle auto-suspend path performs none of that bookkeeping:
//! the node suspends the VM locally and publishes `status = 'suspended'`,
//! and the internal API's published-state convergence promotes
//! `sandboxes.lifecycle` to `'suspended'` — and stops there. The execution
//! row stays `active = TRUE, state = 'running'` with a recorded pod IP,
//! the sandbox stays `placement = 'on_node'` with `active_node_id` set,
//! and the pod's container exits (`Succeeded`) and is later deleted by pod
//! cleanup. The control plane is left claiming a live execution on a pod
//! that no longer exists, indefinitely. The stale bookkeeping feeds the
//! wake/reattach path a long-dead pod name and stale pod IP, and makes
//! forensics/operator queries keyed on `active_node_id` see thousands of
//! "active" sandboxes pinned to long-gone nodes.
//!
//! ## Safety contract with the wake path
//!
//! Wake/restore claims a suspended sandbox by CAS-ing `lifecycle` from
//! `'suspended'` to `'restoring'` (`begin_restore_lifecycle`) *before*
//! touching the execution row, and execution claims
//! (`ensure_active_execution_claim`) lock the sandbox row `FOR UPDATE`.
//! The retire transaction takes that same `FOR UPDATE` lock and re-checks
//! `lifecycle = 'suspended'` under it, so it is serialized against both:
//! if a wake won the race, the in-transaction recheck (or the
//! generation/pod-name/pod-uid guard on the execution UPDATE) fails and
//! the candidate is skipped; if the reaper wins, the wake proceeds against
//! a cleanly retired execution — it inserts a fresh claim at the next
//! generation and restores from the snapshot, exactly like a wake after a
//! normal API-driven suspend.
//!
//! What the retire transaction deliberately does **not** touch:
//!
//! - `target_node_id` is preserved (and backfilled from `active_node_id`
//!   when unset) so the wake path's `COALESCE(target_node_id,
//!   active_node_id)` placement affinity is unchanged by the sweep.
//! - `placement_changed_at` is **not** bumped: the legacy
//!   (`current_local_artifact_id IS NULL`) snapshot-portability branch
//!   compares snapshot timestamps against it, and bumping it would
//!   retroactively classify existing ready snapshots as stale — turning
//!   the entire swept population into drain-blocking "local-only"
//!   sandboxes. The API-driven `finish_suspended_execution` does not bump
//!   it either.
//! - Artifact identity columns (`current_local_artifact_*`) are
//!   **cleared**, not preserved: preserving them would require node-local
//!   evidence that the marker names this exact suspend cycle's artifact.
//!   This reaper only has Postgres/S3 evidence, so it first proves that
//!   markerless restore will select a current durable full suspend snapshot
//!   and then clears any unproven marker. The cleared id is recorded on the
//!   `execution_retired` event for forensics.
//!
//! Pods confirmed **missing** (the reflector cache misses and a direct
//! apiserver read returns NotFound) are acted on. A terminal `Succeeded`
//! carrier pod is also safe to clean up only after an uncached apiserver
//! read verifies the same pod identity and all containers terminated; the
//! reaper then deletes the pod, requires a second uncached NotFound, and
//! only then runs the existing retire transaction. Other still-present pods
//! — `Failed`, terminating, `Pending`, or `Running` — are skipped.
//!
//! Ships default-disabled (`SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED`)
//! and, when enabled, runs in dry-run so each environment gets
//! observability (events, metrics, logs) before any writes happen.

use pplx_space_common_core::types::{LocalArtifactId, NodeId};

use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::kubernetes::{
    K8sBlockingDeleteOutcome, KubernetesSubstrate, SandboxPodKey, SandboxPodView,
};
use pplx_space_common_core::types::{PositiveDuration, SandboxId};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::cluster_reconciler::{
    self as metrics, Loop, Mode, OutcomeEvent, ReapOutcome,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::orphaned_sandboxes::{
    PodPresence, PodPresenceStatus, ReflectorPresence, classify_pod_presence,
};

pub mod store;

#[cfg(test)]
mod tests;

pub use store::ExecutionDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);
/// Default minimum age (anchored on both `sandboxes.lifecycle_changed_at`
/// and the execution row's `created_at`) before a candidate is eligible.
/// One hour is comfortably past any in-flight suspend completion,
/// published-state convergence lag, or pod teardown, while still converging
/// the steady-state leak within the same day.
pub(crate) const DEFAULT_MIN_AGE_SECS: u64 = 3_600;
pub(crate) const DEFAULT_BATCH_SIZE: u32 = 500;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default everywhere; each environment opts
    /// in via `SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED=true`.
    pub enabled: bool,
    pub tick: Duration,
    /// Minimum age of the suspend (and of the execution row) before the
    /// reaper considers the candidate. Always positive — a zero age would
    /// race the suspend-publish convergence window.
    pub min_age: PositiveDuration,
    /// Upper bound on candidates scanned (and therefore pods checked and
    /// rows retired) per tick.
    pub batch_size: u32,
    /// When true (the default), candidates are classified and reported via
    /// logs/metrics/events but nothing is written.
    pub dry_run: bool,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            min_age: PositiveDuration::new_panic(Duration::from_secs(DEFAULT_MIN_AGE_SECS)),
            batch_size: DEFAULT_BATCH_SIZE,
            dry_run: true,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.suspendedExecutionReaper.*`.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default(
                "SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED",
                default.enabled,
            ),
            tick: default.tick,
            min_age: default.min_age,
            batch_size: default.batch_size,
            dry_run: default.dry_run,
        }
    }
}

/// One suspended sandbox still carrying an active execution row, as observed
/// by the candidate scan. The execution identity (`generation`, `pod_name`,
/// `pod_uid`) is carried through to the retire transaction's CAS guard so a
/// row that changed under us is never retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleExecutionCandidate {
    pub sandbox_id: SandboxId,
    pub generation: i64,
    pub pod_name: String,
    pub pod_uid: Option<String>,
    /// Execution state observed at scan time (`running` for the node-side
    /// auto-suspend leak; occasionally `creating`). Recorded on the event as
    /// `from_state`.
    pub execution_state: String,
    /// Stale `sandboxes.active_node_id` the retire transaction clears.
    /// `None` for the rare candidate whose node pointer was already cleared
    /// while the execution row stayed active.
    pub active_node_id: Option<NodeId>,
    /// Prior-cycle `current_local_artifact_id` the retire transaction
    /// clears (see the module docs); recorded on the event for forensics.
    /// `None` for the common node-side auto-suspend candidate that never
    /// had one.
    pub current_local_artifact_id: Option<LocalArtifactId>,
}

/// Outcome of one retire transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireOutcome {
    /// Execution retired and sandbox placement bookkeeping cleared.
    Retired,
    /// The durable artifact proof that made the row eligible at scan time no
    /// longer held under the row lock. Leave projection untouched.
    SkippedSnapshotNotCurrent,
    /// The in-transaction guard lost: the lifecycle moved off `suspended`
    /// or the active execution changed identity under us. Another writer
    /// owns the row now; skip without retrying this tick.
    SkippedRaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPodAction {
    /// Verify that the terminal pod is a safe cleanup candidate, but do not
    /// delete it. Used by dry-run ticks so the report reflects what live mode
    /// would do without mutating Kubernetes.
    VerifyOnly,
    /// Delete the verified terminal pod and require confirmed absence before
    /// the stale execution can be retired.
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPodCleanupSkipReason {
    IdentityMismatch,
    NoLongerSucceeded,
    DeleteUnconfirmed,
    DeleteFailed,
    PodPresenceUnknown,
}

impl TerminalPodCleanupSkipReason {
    fn metric_reason(self) -> &'static str {
        match self {
            Self::IdentityMismatch => "terminal_pod_identity_mismatch",
            Self::NoLongerSucceeded => "terminal_pod_no_longer_succeeded",
            Self::DeleteUnconfirmed => "terminal_pod_delete_unconfirmed",
            Self::DeleteFailed => "terminal_pod_delete_failed",
            Self::PodPresenceUnknown => "terminal_pod_presence_unknown",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TerminalPodCleanupSkipCounts {
    identity_mismatch: u32,
    no_longer_succeeded: u32,
    delete_unconfirmed: u32,
    delete_failed: u32,
    pod_presence_unknown: u32,
}

impl TerminalPodCleanupSkipCounts {
    fn increment(&mut self, reason: TerminalPodCleanupSkipReason) {
        match reason {
            TerminalPodCleanupSkipReason::IdentityMismatch => self.identity_mismatch += 1,
            TerminalPodCleanupSkipReason::NoLongerSucceeded => self.no_longer_succeeded += 1,
            TerminalPodCleanupSkipReason::DeleteUnconfirmed => self.delete_unconfirmed += 1,
            TerminalPodCleanupSkipReason::DeleteFailed => self.delete_failed += 1,
            TerminalPodCleanupSkipReason::PodPresenceUnknown => self.pod_presence_unknown += 1,
        }
    }

    #[cfg(test)]
    fn iter(self) -> impl Iterator<Item = (TerminalPodCleanupSkipReason, u32)> {
        [
            (
                TerminalPodCleanupSkipReason::IdentityMismatch,
                self.identity_mismatch,
            ),
            (
                TerminalPodCleanupSkipReason::NoLongerSucceeded,
                self.no_longer_succeeded,
            ),
            (
                TerminalPodCleanupSkipReason::DeleteUnconfirmed,
                self.delete_unconfirmed,
            ),
            (
                TerminalPodCleanupSkipReason::DeleteFailed,
                self.delete_failed,
            ),
            (
                TerminalPodCleanupSkipReason::PodPresenceUnknown,
                self.pod_presence_unknown,
            ),
        ]
        .into_iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPodCleanupOutcome {
    ReadyToRetire,
    DeletedAndReadyToRetire,
    Skipped(TerminalPodCleanupSkipReason),
}

/// Terminal `Succeeded` pod cleanup surface. Production impl:
/// [`SuspendedExecutionPodAccess`]; tests use `FakePresence`.
pub trait TerminalSucceededPodCleaner: PodPresence {
    fn prepare_terminal_succeeded_pod(
        &self,
        candidate: &StaleExecutionCandidate,
        action: TerminalPodAction,
    ) -> impl std::future::Future<Output = TerminalPodCleanupOutcome> + Send;
}

/// DB surface the reaper depends on. Production impl: [`ExecutionDbStore`];
/// `tests::FakeSink` for unit tests.
pub trait RetireSink: Send + Sync + 'static {
    /// List candidates with `sandbox_id` in `(after_sandbox_id, upper_bound]`,
    /// ordered by `sandbox_id`, at most `batch_size` rows. The upper bound is
    /// captured once per sweep so continuous sandbox creation cannot keep
    /// extending the cursor's right edge (v7-UUID ids are time-ordered).
    fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        min_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<StaleExecutionCandidate>, sqlx::Error>> + Send;

    /// Largest `sandboxes.id` at call time, or `None` for an empty table.
    /// Used as the per-sweep high-water mark.
    fn current_max_sandbox_id(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<SandboxId>, sqlx::Error>> + Send;

    /// Run the retire transaction for one candidate whose pod was confirmed
    /// missing. Implementations must lock the sandbox row `FOR UPDATE`,
    /// re-check `lifecycle = 'suspended'` under the lock, and guard the
    /// execution UPDATE on the observed `(generation, pod_name, pod_uid)`.
    fn retire_stale_execution(
        &self,
        candidate: &StaleExecutionCandidate,
    ) -> impl std::future::Future<Output = Result<RetireOutcome, sqlx::Error>> + Send;
}

/// Kubernetes pod access used by the suspended execution reaper. Presence
/// checks stay cache-first for normal missing/present classification, while
/// terminal cleanup revalidates through uncached apiserver reads before and
/// after delete.
pub struct SuspendedExecutionPodAccess {
    substrate: Arc<KubernetesSubstrate>,
    presence: ReflectorPresence,
}

impl SuspendedExecutionPodAccess {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self {
            presence: ReflectorPresence::new(Arc::clone(&substrate)),
            substrate,
        }
    }
}

impl PodPresence for SuspendedExecutionPodAccess {
    async fn check_pod(&self, pod_name: &str) -> PodPresenceStatus {
        self.presence.check_pod(pod_name).await
    }
}

impl TerminalSucceededPodCleaner for SuspendedExecutionPodAccess {
    async fn prepare_terminal_succeeded_pod(
        &self,
        candidate: &StaleExecutionCandidate,
        action: TerminalPodAction,
    ) -> TerminalPodCleanupOutcome {
        let pod = match self
            .substrate
            .read_sandbox_pod_uncached(&candidate.pod_name)
            .await
        {
            Ok(Some(pod)) => pod,
            Ok(None) => return TerminalPodCleanupOutcome::ReadyToRetire,
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    pod_name = %candidate.pod_name,
                    error = %err,
                    "suspended_execution_reaper could not verify terminal pod"
                );
                return TerminalPodCleanupOutcome::Skipped(
                    TerminalPodCleanupSkipReason::PodPresenceUnknown,
                );
            }
        };

        if let Err(reason) = validate_terminal_succeeded_pod(&pod, candidate) {
            return TerminalPodCleanupOutcome::Skipped(reason);
        }
        if action == TerminalPodAction::VerifyOnly {
            return TerminalPodCleanupOutcome::ReadyToRetire;
        }

        let deleted_by_reaper = match self
            .substrate
            .delete_sandbox_pod_for_projection_cleanup(&candidate.pod_name)
            .await
        {
            Ok(K8sBlockingDeleteOutcome::Deleted) => true,
            Ok(K8sBlockingDeleteOutcome::NotFound) => false,
            // `PreconditionFailed` is unreachable here — this cleanup sends no
            // UID precondition — but skipping as unconfirmed is the safe
            // degradation for a future precondition-carrying caller.
            Ok(
                K8sBlockingDeleteOutcome::TimedOut
                | K8sBlockingDeleteOutcome::ObserveError
                | K8sBlockingDeleteOutcome::PreconditionFailed,
            ) => {
                return TerminalPodCleanupOutcome::Skipped(
                    TerminalPodCleanupSkipReason::DeleteUnconfirmed,
                );
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    pod_name = %candidate.pod_name,
                    error = %err,
                    "suspended_execution_reaper failed to delete terminal pod"
                );
                return TerminalPodCleanupOutcome::Skipped(
                    TerminalPodCleanupSkipReason::DeleteFailed,
                );
            }
        };

        match self
            .substrate
            .read_sandbox_pod_uncached(&candidate.pod_name)
            .await
        {
            Ok(None) if deleted_by_reaper => TerminalPodCleanupOutcome::DeletedAndReadyToRetire,
            Ok(None) => TerminalPodCleanupOutcome::ReadyToRetire,
            Ok(Some(_)) => {
                TerminalPodCleanupOutcome::Skipped(TerminalPodCleanupSkipReason::DeleteUnconfirmed)
            }
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    pod_name = %candidate.pod_name,
                    error = %err,
                    "suspended_execution_reaper could not confirm terminal pod deletion"
                );
                TerminalPodCleanupOutcome::Skipped(TerminalPodCleanupSkipReason::PodPresenceUnknown)
            }
        }
    }
}

fn validate_terminal_succeeded_pod(
    pod: &SandboxPodView,
    candidate: &StaleExecutionCandidate,
) -> Result<(), TerminalPodCleanupSkipReason> {
    if pod.name() != candidate.pod_name.as_str() {
        return Err(TerminalPodCleanupSkipReason::IdentityMismatch);
    }
    if let Some(expected_uid) = candidate.pod_uid.as_deref()
        && pod.uid() != Some(expected_uid)
    {
        return Err(TerminalPodCleanupSkipReason::IdentityMismatch);
    }
    if !pod_label_matches(
        pod,
        SandboxPodKey::SandboxId,
        &candidate.sandbox_id.to_string(),
    ) || !pod_label_matches(
        pod,
        SandboxPodKey::ExecutionGeneration,
        &candidate.generation.to_string(),
    ) || !pod_label_matches(pod, SandboxPodKey::ManagedBy, "space-api")
    {
        return Err(TerminalPodCleanupSkipReason::IdentityMismatch);
    }
    if classify_pod_presence(pod) != PodPresenceStatus::TerminalSucceeded {
        return Err(TerminalPodCleanupSkipReason::NoLongerSucceeded);
    }
    Ok(())
}

fn pod_label_matches(pod: &SandboxPodView, key: SandboxPodKey, expected: &str) -> bool {
    // Missing labels are tolerated intentionally: name + optional UID +
    // terminal status carry the hard identity guard, while these labels add
    // safety when present on SPACE-rendered pods.
    pod.label(key).is_none_or(|actual| actual == expected)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub pod_present: u32,
    pub pod_lingering: u32,
    pub pod_terminal_failed: u32,
    pub pod_unknown: u32,
    pub terminal_deleted: u32,
    pub terminal_cleanup_skipped: u32,
    pub terminal_cleanup_skip_reasons: TerminalPodCleanupSkipCounts,
    pub snapshot_not_current: u32,
    pub retired: u32,
    pub raced: u32,
}

/// Stateful per-tick reconciler core. Pure with respect to dependencies —
/// tests drive it directly with a fake presence and fake sink.
pub struct Reconciler<P: TerminalSucceededPodCleaner, S: RetireSink> {
    config: ReconcilerConfig,
    presence: P,
    sink: S,
    scan_after: Option<SandboxId>,
    /// High-water mark captured at sweep start. `Some(id)` while a sweep is
    /// in progress; `None` between sweeps. Bounds the right edge of the
    /// cursor so continuous creation can't starve older candidates, and —
    /// since dry-run never removes candidates from the set — guarantees the
    /// cursor still walks the whole population instead of rescanning the
    /// first page forever.
    sweep_upper_bound: Option<SandboxId>,
}

impl<P: TerminalSucceededPodCleaner, S: RetireSink> Reconciler<P, S> {
    pub fn new(config: ReconcilerConfig, presence: P, sink: S) -> Self {
        Self {
            config,
            presence,
            sink,
            scan_after: None,
            sweep_upper_bound: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        // Try the cursored list once; if the cursor reached the sweep's
        // high-water mark, wrap and retry within this tick so the next sweep
        // starts immediately. Two iterations is enough — after a wrap an
        // empty result means there are no candidates at all right now.
        let candidates = 'fetch: {
            for attempt in 0..2 {
                if self.sweep_upper_bound.is_none() {
                    match self.sink.current_max_sandbox_id().await {
                        Ok(Some(id)) => self.sweep_upper_bound = Some(id),
                        Ok(None) => break 'fetch Vec::new(),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "suspended_execution_reaper: current_max_sandbox_id failed"
                            );
                            return TickReport::default();
                        }
                    }
                    debug_assert!(self.scan_after.is_none());
                }
                let upper_bound = self.sweep_upper_bound.expect("set immediately above");
                match self
                    .sink
                    .list_candidates(
                        self.config.batch_size,
                        self.scan_after,
                        upper_bound,
                        self.config.min_age,
                    )
                    .await
                {
                    Ok(rows) if rows.is_empty() && attempt == 0 => {
                        // Cursor reached the sweep's high-water mark. Reset
                        // and let the next iteration capture a fresh bound
                        // and start over from the smallest id.
                        self.scan_after = None;
                        self.sweep_upper_bound = None;
                        continue;
                    }
                    Ok(rows) => break 'fetch rows,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "suspended_execution_reaper: list_candidates failed"
                        );
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

        let mut report = TickReport {
            scanned: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
            ..TickReport::default()
        };

        let mode = if self.config.dry_run {
            Mode::DryRun
        } else {
            Mode::Live
        };
        for candidate in candidates {
            let mut terminal_pod_deleted_by_reaper = false;
            match self.presence.check_pod(&candidate.pod_name).await {
                PodPresenceStatus::Present => {
                    // A Running pod under a suspended lifecycle is either
                    // the publish-vs-pod-exit window or a divergence this
                    // reaper must not resolve. Leave it alone.
                    report.pod_present += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("pod_present")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                    continue;
                }
                PodPresenceStatus::TerminalSucceeded => {
                    let action = if self.config.dry_run {
                        TerminalPodAction::VerifyOnly
                    } else {
                        TerminalPodAction::Delete
                    };
                    match self
                        .presence
                        .prepare_terminal_succeeded_pod(&candidate, action)
                        .await
                    {
                        TerminalPodCleanupOutcome::ReadyToRetire => {}
                        TerminalPodCleanupOutcome::DeletedAndReadyToRetire => {
                            terminal_pod_deleted_by_reaper = true;
                        }
                        TerminalPodCleanupOutcome::Skipped(reason) => {
                            OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                                .reason(reason.metric_reason())
                                .sandbox_id(candidate.sandbox_id)
                                .pod_name(&candidate.pod_name)
                                .warn();
                            report.terminal_cleanup_skipped += 1;
                            report.terminal_cleanup_skip_reasons.increment(reason);
                            continue;
                        }
                    }
                }
                PodPresenceStatus::TerminalFailed => {
                    report.pod_terminal_failed += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("pod_terminal_failed")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                    continue;
                }
                PodPresenceStatus::Unready => {
                    // Pod object still exists (Succeeded awaiting pod
                    // cleanup that did not pass the terminal safety check,
                    // or terminating). Converges on a later tick once the
                    // pod is actually deleted.
                    report.pod_lingering += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("pod_lingering")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                    continue;
                }
                PodPresenceStatus::Unknown => {
                    report.pod_unknown += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("pod_presence_unknown")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                    continue;
                }
                PodPresenceStatus::Missing => {}
            }

            if self.config.dry_run {
                OutcomeEvent::suspended_execution_reap(ReapOutcome::RetiredStaleExecution, mode)
                    .sandbox_id(candidate.sandbox_id)
                    .pod_name(&candidate.pod_name)
                    .generation(candidate.generation)
                    .detail(format!("execution_state={}", candidate.execution_state))
                    .info();
                emit_retire_event(&candidate, Some("dry_run"));
                report.retired += 1;
                continue;
            }

            match self.sink.retire_stale_execution(&candidate).await {
                Ok(RetireOutcome::Retired) => {
                    OutcomeEvent::suspended_execution_reap(
                        ReapOutcome::RetiredStaleExecution,
                        mode,
                    )
                    .sandbox_id(candidate.sandbox_id)
                    .pod_name(&candidate.pod_name)
                    .generation(candidate.generation)
                    .detail(format!("execution_state={}", candidate.execution_state))
                    .info();
                    emit_retire_event(&candidate, None);
                    if terminal_pod_deleted_by_reaper {
                        report.terminal_deleted += 1;
                        OutcomeEvent::suspended_execution_reap(
                            ReapOutcome::DeletedTerminalPod,
                            mode,
                        )
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .info();
                    }
                    report.retired += 1;
                }
                Ok(RetireOutcome::SkippedSnapshotNotCurrent) => {
                    report.snapshot_not_current += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("snapshot_not_current")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                }
                Ok(RetireOutcome::SkippedRaced) => {
                    report.raced += 1;
                    OutcomeEvent::suspended_execution_reap(ReapOutcome::Skipped, mode)
                        .reason("raced")
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name(&candidate.pod_name)
                        .debug();
                }
                Err(err) => {
                    tracing::warn!(
                        sandbox_id = %candidate.sandbox_id,
                        pod_name = %candidate.pod_name,
                        error = %err,
                        "suspended_execution_reaper: retire failed; will retry next tick"
                    );
                }
            }
        }

        metrics::scanned(Loop::SuspendedExecutionReaper, u64::from(report.scanned));

        report
    }
}

/// Emit the `execution_retired` sandbox event for one candidate. `result`
/// is `Some("dry_run")` for observation-only sweeps, `None` for live
/// retirements.
fn emit_retire_event(candidate: &StaleExecutionCandidate, result: Option<&'static str>) {
    let mut details = SandboxEventDetails::default()
        .reason("stale_suspended_execution")
        .operation("suspended_execution_reap")
        .trigger(SandboxEventTrigger::System)
        .with_from_state(candidate.execution_state.clone())
        .with_to_state("suspended")
        .pod_name(candidate.pod_name.clone())
        .execution_generation(candidate.generation.to_string());
    if let Some(pod_uid) = &candidate.pod_uid {
        details = details.pod_uid(pod_uid);
    }
    if let Some(node_id) = candidate.active_node_id {
        details = details.source_node_id(node_id.to_string());
    }
    if let Some(artifact_id) = candidate.current_local_artifact_id {
        details = details.local_artifact_id(artifact_id.to_string());
    }
    if let Some(result) = result {
        details = details.result(result);
    }
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::ExecutionRetired,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// Run the reaper on the current task until the shutdown token is cancelled.
/// Spawned by `space-cluster-controller` alongside the other
/// cluster-singleton reconcilers. Kubernetes-substrate only: pod presence is
/// the retire precondition, so there is nothing to do on the direct
/// substrate.
pub async fn run(
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "suspended_execution_reaper disabled \
             (SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED unset); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        min_age_secs = config.min_age.get().as_secs(),
        batch_size = config.batch_size,
        dry_run = config.dry_run,
        "starting suspended_execution_reaper"
    );

    let presence = SuspendedExecutionPodAccess::new(kube);
    let sink = ExecutionDbStore::new(pool);
    let mut reconciler = Reconciler::new(config.clone(), presence, sink);

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race the kube
    // reflector's initial sync.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let _ = reconciler.tick().await;
                metrics::loop_latency(Loop::SuspendedExecutionReaper, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("suspended_execution_reaper stopping");
                return;
            }
        }
    }
}
