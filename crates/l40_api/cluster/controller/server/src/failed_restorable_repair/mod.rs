//! Cluster-singleton repair loop for `lifecycle = 'failed'` sandboxes whose
//! durable suspend snapshot is provably safe to restore from.
//!
//! The original wedge cohort came from cross-node restores cancelled
//! mid-staging: the local restore row was discarded, inventory reconciliation
//! materialized the remaining cache as `failed`, and the control plane adopted
//! that failure even though the canonical suspend snapshot remained intact in
//! S3. Node-side restore ownership prevents new attributable interrupted
//! restores from taking that route, but this loop still repairs already-failed
//! rows and any other failure source that satisfies the same durable-snapshot
//! proof. It moves them back to `suspended` so the next user action takes the
//! normal restore path.
//!
//! Detection is deliberately cause-agnostic: node-published failure adoption
//! writes only `lifecycle`, so provenance is not queryable — and the snapshot
//! gate makes it unnecessary. Any `failed` row that satisfies the safety
//! conditions is recoverable regardless of which path failed it:
//!
//! - `failed` for at least `MIN_AGE` (lets adoption-driven node cleanup and
//!   the observed-state projection settle), with any restore lease expired;
//! - no active execution and no execution `pod_ip`;
//! - no `sandbox_observed_states` row that could describe a live VM — a node
//!   still publishing the sandbox means either a live VM (flipping would let
//!   a later wake restore a second VM beside it) or remnants the node will
//!   converge itself. The exception is a `failed`, pid-less record on an
//!   unassigned sandbox: the placement-scoped publish fence has orphaned it
//!   (no publisher can update or delete it), so it is tolerated and purged
//!   by the repair write itself;
//! - a durable suspend snapshot passes `failed_repair_revert_eligibility`
//!   (exact marker backup, or activity-current newest suspend snapshot; the
//!   lifecycle/placement clocks are poisoned by the failed transition itself
//!   and deliberately not consulted).
//!
//! `quarantined` rows are out of scope by construction: quarantine is the
//! operator-attention lane and a distinct lifecycle value this loop never
//! selects.
//!
//! Ships enabled and repairs eligible rows directly. It emits the sandbox
//! transition event after the CAS succeeds.

use pplx_space_common_core::types::LocalArtifactId;

use std::time::{Duration, Instant};

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

pub mod store;

#[cfg(test)]
mod tests;

pub use store::RepairDbStore;

pub(crate) const TICK: Duration = Duration::from_secs(60);
/// Lets adoption-driven node cleanup and the observed-state projection
/// settle before repair. 180 s still exceeds the restore-lease TTL (the
/// substrate's 150 s node-RPC timeout), though lease expiry is enforced by
/// its own row-state guard rather than implied by this age.
pub(crate) const MIN_AGE_SECS: u64 = 180;
/// Candidates per tick; the keyset cursor sweeps the rest on later ticks.
pub(crate) const BATCH_SIZE: u32 = 500;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Enabled by default; operators can disable it with
    /// `SPACE_FAILED_RESTORABLE_REPAIR_ENABLED=false`.
    pub enabled: bool,
    /// Interval between scans: [`TICK`], or the `SPACE_FAILED_RESTORABLE_REPAIR_TICK_SECS` test
    /// hook, which Kind sets so its smoke waits fewer seconds for a tick.
    pub tick: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: TICK,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders this under
    /// `clusterController.reconcilers.failedRestorableRepair.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        Self {
            // Ships enabled and writes, so a malformed value must disable it
            // rather than leave it running.
            enabled: env_config::EnvConfig(env)
                .destructive_gate("SPACE_FAILED_RESTORABLE_REPAIR_ENABLED"),
            tick: env_config::EnvConfig(env)
                .test_duration_secs("SPACE_FAILED_RESTORABLE_REPAIR_TICK_SECS", TICK),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairCandidate {
    pub sandbox_id: SandboxId,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    /// Scan-time estimate of whether the marker is stale and must be cleared
    /// so restore selects the newest snapshot; re-derived at apply time.
    pub clear_marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Reverted with the marker preserved (its own backup is current).
    RevertedToSuspended,
    /// Reverted with the still-stale marker cleared so restore selects the
    /// newest durable snapshot.
    RevertedToSuspendedMarkerCleared,
    SkippedRaced,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub reverted: u32,
    /// Subset of `reverted` recovered via the marker-cleared branch.
    pub reverted_via_newest: u32,
    pub raced: u32,
    pub errors: u32,
}

pub struct Reconciler {
    store: RepairDbStore,
    /// Keyset cursor for the current sweep. Advances past every scanned
    /// candidate regardless of its outcome, so a batch of persistent racers
    /// or errors cannot starve the rows behind it; resets when a page comes
    /// back short.
    scan_after: Option<SandboxId>,
}

impl Reconciler {
    pub fn new(store: RepairDbStore) -> Self {
        Self {
            store,
            scan_after: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let candidates = match self
            .store
            .list_candidates(BATCH_SIZE, self.scan_after, min_age())
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "failed_restorable_repair: list_candidates failed");
                return TickReport::default();
            }
        };
        self.scan_after = if candidates.len() < BATCH_SIZE as usize {
            None
        } else {
            candidates.last().map(|candidate| candidate.sandbox_id)
        };
        if candidates.is_empty() {
            return TickReport::default();
        }

        let mut report = TickReport {
            scanned: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
            ..TickReport::default()
        };

        for candidate in candidates {
            match self
                .store
                .revert_failed_to_suspended(&candidate, min_age())
                .await
            {
                Ok(
                    outcome @ (ApplyOutcome::RevertedToSuspended
                    | ApplyOutcome::RevertedToSuspendedMarkerCleared),
                ) => {
                    // Classify on what the apply actually did: the marker-clear
                    // decision is re-derived in SQL.
                    let cleared = outcome == ApplyOutcome::RevertedToSuspendedMarkerCleared;
                    self.record_revert(&mut report, candidate, cleared);
                }
                Ok(ApplyOutcome::SkippedRaced) => {
                    report.raced += 1;
                    OutcomeEvent::failed_restorable_repair(RepairOutcome::CasLost, Mode::Live)
                        .sandbox_id(candidate.sandbox_id)
                        .debug();
                }
                Err(err) => {
                    report.errors += 1;
                    OutcomeEvent::failed_restorable_repair(RepairOutcome::Error, Mode::Live)
                        .sandbox_id(candidate.sandbox_id)
                        .error(&err)
                        .warn();
                }
            }
        }

        metrics::scanned(Loop::FailedRestorableRepair, u64::from(report.scanned));
        report
    }

    fn record_revert(
        &self,
        report: &mut TickReport,
        candidate: RepairCandidate,
        marker_cleared: bool,
    ) {
        let outcome = if marker_cleared {
            RepairOutcome::RevertedToNewestSnapshotMarkerCleared
        } else {
            RepairOutcome::RevertedToDurableSnapshot
        };
        emit_repair_event(&candidate, marker_cleared);
        OutcomeEvent::failed_restorable_repair(outcome, Mode::Live)
            .sandbox_id(candidate.sandbox_id)
            .warn();
        report.reverted += 1;
        if marker_cleared {
            report.reverted_via_newest += 1;
        }
    }
}

fn min_age() -> PositiveDuration {
    PositiveDuration::new_panic(Duration::from_secs(MIN_AGE_SECS))
}

fn emit_repair_event(candidate: &RepairCandidate, marker_cleared: bool) {
    let (reason, result) = if marker_cleared {
        (
            "newest_suspend_snapshot_activity_current_without_marker",
            "reverted_to_suspended_marker_cleared",
        )
    } else {
        (
            "restorable_suspend_snapshot_without_live_k8s_execution",
            "reverted_to_suspended",
        )
    };
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation("failed_restorable_repair")
        .trigger(SandboxEventTrigger::System)
        .with_from_state("failed")
        .with_to_state("suspended")
        .result(result);
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

pub async fn run(pool: PgPool, config: ReconcilerConfig, shutdown_token: CancellationToken) {
    if !config.enabled {
        tracing::info!(
            "failed_restorable_repair disabled \
             (SPACE_FAILED_RESTORABLE_REPAIR_ENABLED=false); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        min_age_secs = MIN_AGE_SECS,
        batch_size = BATCH_SIZE,
        "starting failed_restorable_repair"
    );

    let mut reconciler = Reconciler::new(RepairDbStore::new(pool));
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
                    reverted = report.reverted,
                    reverted_via_newest = report.reverted_via_newest,
                    cas_lost = report.raced,
                    errors = report.errors,
                    "failed_restorable_repair tick complete"
                );
                metrics::loop_latency(Loop::FailedRestorableRepair, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("failed_restorable_repair stopping");
                return;
            }
        }
    }
}
