//! Cluster-singleton repair loop for `lifecycle = 'started'` sandboxes whose
//! live Kubernetes execution evidence is already gone, but whose current
//! suspend cycle has a durable ready snapshot.
//!
//! The invalid-claim failure mode is a state-machine disagreement: user
//! traffic sees `started`, so the API does not run wake/restore, but there is
//! no active pod-backed execution to attach to. This loop moves only the DB
//! state that is already inactive back to `suspended` so the next user action
//! takes the normal restore path.
//!
//! Safety conditions:
//! - the row has been `started` for at least `DEFAULT_MIN_AGE_SECS`;
//! - no `sandbox_k8s_executions.active = TRUE` row exists;
//! - the tracked pod identity points at a Kubernetes pod that is confirmed
//!   missing (untracked sandboxes skip the probe);
//! - no execution row for the sandbox still carries a `pod_ip`;
//! - either the current artifact marker has a ready full `suspend` snapshot, or
//!   the newest restorable full snapshot is a current `suspend` snapshot.
//!
//! The newest `sandbox_k8s_executions` row supplies the tracked pod identity.
//! The apply asserts that its generation is unchanged before reverting the
//! lifecycle.
//!
//! The loop ships enabled and repairs eligible rows directly. It emits the
//! sandbox transition event after the CAS succeeds.

use pplx_space_common_core::types::LocalArtifactId;

use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::{PositiveDuration, SandboxId};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::cluster_reconciler::{
    self as metrics, Loop, Mode, OutcomeEvent, RepairOutcome,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::orphaned_sandboxes::{PodPresence, PodPresenceStatus, ReflectorPresence};

pub mod store;

#[cfg(test)]
mod tests;

pub use store::RepairDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_MIN_AGE_SECS: u64 = 300;
pub(crate) const DEFAULT_BATCH_SIZE: u32 = 500;
const REPAIR_CONDITION_SET: &str =
    "no_active_execution_missing_or_untracked_pod_no_pod_ip_stable_generation";
const REPAIR_CONDITION_SET_NEWEST_SNAPSHOT: &str = "no_active_execution_missing_or_untracked_pod_no_pod_ip_stable_generation_newest_snapshot_current";

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Enabled by default; operators can disable it with
    /// `SPACE_STARTED_SANDBOX_REPAIR_ENABLED=false`.
    pub enabled: bool,
    /// Interval between scans: [`DEFAULT_TICK`], or the `SPACE_STARTED_SANDBOX_REPAIR_TICK_SECS` test
    /// hook, which Kind sets so its smoke waits fewer seconds for a tick.
    pub tick: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: DEFAULT_TICK,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders this under
    /// `clusterController.reconcilers.startedSandboxRepair.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        Self {
            // Ships enabled and writes, so a malformed value must disable it
            // rather than leave it running.
            enabled: env_config::EnvConfig(env)
                .destructive_gate("SPACE_STARTED_SANDBOX_REPAIR_ENABLED"),
            tick: env_config::EnvConfig(env)
                .test_duration_secs("SPACE_STARTED_SANDBOX_REPAIR_TICK_SECS", DEFAULT_TICK),
        }
    }
}

/// The newest `sandbox_k8s_executions` row:
/// the pod to probe; the generation is re-asserted newest at apply time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewestExecutionRow {
    pub pod_name: String,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairCandidate {
    pub sandbox_id: SandboxId,
    /// `None` means there was no execution row at scan time. The probe is
    /// skipped and the apply re-asserts that the table is still empty.
    pub newest_execution: Option<NewestExecutionRow>,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    /// When true, the marker (`current_local_artifact_id`) points at a stale
    /// (pre-restore) artifact, but a newer durable suspend snapshot from this
    /// cycle is safe to restore. Recovery must clear the marker so the restore
    /// selector picks that newest snapshot. When false, the marker's own
    /// snapshot is current and is preserved (the original repair behavior).
    pub clear_marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Reverted with the marker preserved (its own snapshot was current).
    RevertedToSuspended,
    /// Reverted and cleared a still-stale marker so restore selects the newest
    /// durable snapshot. The clear decision is re-derived at apply time, so this
    /// reflects what actually happened — not the scan-time estimate.
    RevertedToSuspendedMarkerCleared,
    SkippedRaced,
}

pub trait RepairSink: Send + Sync + 'static {
    fn list_candidates(
        &self,
        batch_size: u32,
        after_sandbox_id: Option<SandboxId>,
        upper_bound: SandboxId,
        min_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<RepairCandidate>, sqlx::Error>> + Send;

    fn current_max_sandbox_id(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<SandboxId>, sqlx::Error>> + Send;

    fn revert_started_to_suspended(
        &self,
        candidate: &RepairCandidate,
        min_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<ApplyOutcome, sqlx::Error>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub pod_present: u32,
    pub pod_lingering: u32,
    pub pod_unknown: u32,
    pub reverted: u32,
    /// Subset of `reverted` recovered via the marker-stale branch (marker
    /// cleared, restore selects the newest snapshot). Lets tests assert that
    /// `tick()` routed a marker-stale candidate to the newest-snapshot
    /// classification, not just that the field propagated to the sink.
    pub reverted_via_newest: u32,
    pub raced: u32,
    pub errors: u32,
}

pub struct Reconciler<P: PodPresence, S: RepairSink> {
    presence: P,
    sink: S,
    scan_after: Option<SandboxId>,
    sweep_upper_bound: Option<SandboxId>,
}

impl<P: PodPresence, S: RepairSink> Reconciler<P, S> {
    pub fn new(presence: P, sink: S) -> Self {
        Self {
            presence,
            sink,
            scan_after: None,
            sweep_upper_bound: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let candidates = 'fetch: {
            for attempt in 0..2 {
                if self.sweep_upper_bound.is_none() {
                    match self.sink.current_max_sandbox_id().await {
                        Ok(Some(id)) => self.sweep_upper_bound = Some(id),
                        Ok(None) => break 'fetch Vec::new(),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "started_sandbox_repair: current_max_sandbox_id failed"
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
                        DEFAULT_BATCH_SIZE,
                        self.scan_after,
                        upper_bound,
                        default_min_age(),
                    )
                    .await
                {
                    Ok(rows) if rows.is_empty() && attempt == 0 => {
                        self.scan_after = None;
                        self.sweep_upper_bound = None;
                        continue;
                    }
                    Ok(rows) => break 'fetch rows,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "started_sandbox_repair: list_candidates failed"
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

        for candidate in candidates {
            if let Some(pod_name) = candidate
                .newest_execution
                .as_ref()
                .map(|row| row.pod_name.as_str())
            {
                match self.presence.check_pod(pod_name).await {
                    PodPresenceStatus::Present => {
                        report.pod_present += 1;
                        OutcomeEvent::started_sandbox_repair(RepairOutcome::Skipped, Mode::Live)
                            .reason("pod_present")
                            .sandbox_id(candidate.sandbox_id)
                            .pod_name(pod_name)
                            .debug();
                        continue;
                    }
                    PodPresenceStatus::TerminalSucceeded
                    | PodPresenceStatus::TerminalFailed
                    | PodPresenceStatus::Unready => {
                        report.pod_lingering += 1;
                        OutcomeEvent::started_sandbox_repair(RepairOutcome::Deferred, Mode::Live)
                            .reason("pod_lingering")
                            .sandbox_id(candidate.sandbox_id)
                            .pod_name(pod_name)
                            .debug();
                        continue;
                    }
                    PodPresenceStatus::Unknown => {
                        report.pod_unknown += 1;
                        OutcomeEvent::started_sandbox_repair(RepairOutcome::Deferred, Mode::Live)
                            .reason("pod_presence_unknown")
                            .sandbox_id(candidate.sandbox_id)
                            .pod_name(pod_name)
                            .debug();
                        continue;
                    }
                    PodPresenceStatus::Missing => {}
                }
            }

            match self
                .sink
                .revert_started_to_suspended(&candidate, default_min_age())
                .await
            {
                Ok(
                    outcome @ (ApplyOutcome::RevertedToSuspended
                    | ApplyOutcome::RevertedToSuspendedMarkerCleared),
                ) => {
                    // Classify on what the apply actually did, not the scan-time
                    // estimate: the marker-clear decision is re-derived in SQL.
                    let marker_cleared = outcome == ApplyOutcome::RevertedToSuspendedMarkerCleared;
                    let (condition_set, revert_outcome) = repair_classification(marker_cleared);
                    emit_repair_event(&candidate, condition_set, marker_cleared);
                    OutcomeEvent::started_sandbox_repair(revert_outcome, Mode::Live)
                        .detail(condition_set)
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name_opt(
                            candidate
                                .newest_execution
                                .as_ref()
                                .map(|row| row.pod_name.as_str()),
                        )
                        .warn();
                    report.reverted += 1;
                    if marker_cleared {
                        report.reverted_via_newest += 1;
                    }
                }
                Ok(ApplyOutcome::SkippedRaced) => {
                    report.raced += 1;
                    OutcomeEvent::started_sandbox_repair(RepairOutcome::CasLost, Mode::Live)
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name_opt(
                            candidate
                                .newest_execution
                                .as_ref()
                                .map(|row| row.pod_name.as_str()),
                        )
                        .debug();
                }
                Err(err) => {
                    report.errors += 1;
                    OutcomeEvent::started_sandbox_repair(RepairOutcome::Error, Mode::Live)
                        .sandbox_id(candidate.sandbox_id)
                        .pod_name_opt(
                            candidate
                                .newest_execution
                                .as_ref()
                                .map(|row| row.pod_name.as_str()),
                        )
                        .error(&err)
                        .warn();
                }
            }
        }

        metrics::scanned(Loop::StartedSandboxRepair, u64::from(report.scanned));
        report
    }
}

fn default_min_age() -> PositiveDuration {
    PositiveDuration::new_panic(Duration::from_secs(DEFAULT_MIN_AGE_SECS))
}

/// Maps a candidate to its (condition_set, metric outcome) pair. Marker-stale
/// candidates (recovered by clearing the marker and restoring the newest
/// snapshot) are reported distinctly from the marker-current repair.
fn repair_classification(clear_marker: bool) -> (&'static str, RepairOutcome) {
    let outcome = if clear_marker {
        RepairOutcome::RevertedToNewestSnapshotMarkerCleared
    } else {
        RepairOutcome::RevertedToDurableSnapshot
    };
    let condition_set = match clear_marker {
        false => REPAIR_CONDITION_SET,
        true => REPAIR_CONDITION_SET_NEWEST_SNAPSHOT,
    };
    (condition_set, outcome)
}

/// `(reason, artifact, result)` for the revert sandbox event. Marker-stale
/// recoveries record the distinct reason/result so a timeline can tell the two
/// repair branches apart.
fn repair_event_strings(clear_marker: bool) -> (&'static str, &'static str, &'static str) {
    if clear_marker {
        (
            "newest_suspend_snapshot_current_without_marker",
            "newest_suspend_snapshot",
            "reverted_to_suspended_marker_cleared",
        )
    } else {
        (
            "current_suspend_snapshot_without_live_k8s_execution",
            "current_suspend_snapshot",
            "reverted_to_suspended",
        )
    }
}

fn emit_repair_event(
    candidate: &RepairCandidate,
    condition_set: &'static str,
    marker_cleared: bool,
) {
    let (reason, artifact, result) = repair_event_strings(marker_cleared);
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation("started_sandbox_repair")
        .trigger(SandboxEventTrigger::System)
        .with_from_state("started")
        .with_to_state("suspended")
        .artifact(artifact)
        .condition_set(condition_set)
        .result(result);
    if let Some(row) = &candidate.newest_execution {
        details = details.pod_name(&row.pod_name);
    }
    if let Some(artifact_id) = candidate.current_local_artifact_id {
        details = details.local_artifact_id(artifact_id.to_string());
    }
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::LifecycleRevertedToSuspended,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

pub async fn run(
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "started_sandbox_repair disabled \
             (SPACE_STARTED_SANDBOX_REPAIR_ENABLED=false); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        min_age_secs = DEFAULT_MIN_AGE_SECS,
        batch_size = DEFAULT_BATCH_SIZE,
        "starting started_sandbox_repair"
    );

    let presence = ReflectorPresence::new(kube);
    let sink = RepairDbStore::new(pool);
    let mut reconciler = Reconciler::new(presence, sink);

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = reconciler.tick().await;
                tracing::info!(
                    scanned = report.scanned,
                    pod_present = report.pod_present,
                    pod_lingering = report.pod_lingering,
                    pod_unknown = report.pod_unknown,
                    reverted = report.reverted,
                    cas_lost = report.raced,
                    errors = report.errors,
                    "started_sandbox_repair tick complete"
                );
                metrics::loop_latency(Loop::StartedSandboxRepair, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("started_sandbox_repair stopping");
                return;
            }
        }
    }
}
