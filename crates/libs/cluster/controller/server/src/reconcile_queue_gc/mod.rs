//! Bounded retention + gone-node cleanup for the state-reconciliation queue.
//!
//! Dead letters are pruned after a fixed retention window. Non-dead ("live")
//! refs are pruned for `managed_by='k8s'`, `drain_phase IS NULL` nodes that a
//! [`GoneDetector`] reports gone; the guarded delete rechecks node identity
//! (`k8s_node_uid`) under a `FOR UPDATE` lock and never deletes the `nodes`
//! row (a gone node's row can still have non-terminal sandboxes pinned to it).
//!
//! The store, candidate scan, guarded prune, per-tick budget, cursor paging,
//! and metrics are shared; only the gone-signal varies by detector:
//!
//! - [`Ec2Detector`] — the node's backing EC2 instance is `terminated`/absent
//!   (and, conservatively, its SPACE state is not still `ready`).
//! - [`KubePresenceDetector`] — the node's name is sustainably absent from the
//!   Kubernetes node reflector and a direct Node GET confirms `NotFound`.
//!   Catches a node gone from k8s without draining, which the EC2 signal misses
//!   and which heartbeat can't reliably detect (many live k8s nodes carry a
//!   `NULL` heartbeat).

use pplx_space_common_core::types::NodeId;

use std::sync::Arc;
use std::time::Duration;

use pplx_space_metrics::reconcile::{QueueGcClass, QueueGcDetector, QueueGcMode, QueueGcResult};
use tokio_util::sync::CancellationToken;

use crate::env_config;

pub mod detector;
pub mod store;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use detector::SubstrateKubeNodePresence;
pub use detector::{Ec2Detector, GoneDetector, GoneVerdict, KubePresenceDetector};
pub use store::QueueDbStore;

const DEFAULT_TICK: Duration = Duration::from_secs(300);
const DEFAULT_DEAD_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_GONE_LIVE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_DEAD_MAX_PER_TICK: u32 = 1_000;
const DEFAULT_GONE_LIVE_MAX_PER_TICK: u32 = 1_000;
const DEFAULT_GONE_MAX_NODES_PER_TICK: u32 = 25;
/// If any queue ref has a claim newer than this, the entire node is treated as
/// active and no refs are eligible. Tracks node-ctl's claim lease (tens of
/// seconds) with generous headroom; a genuinely gone node's claims are hours
/// stale. A fixed correctness constant, not a tunable knob.
const CLAIM_ACTIVE_GRACE: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    /// Scan + classify but never delete; emits `mode:dry_run` metrics for the
    /// observe-first rollout.
    pub dry_run: bool,
    pub tick: Duration,
    pub dead_retention: Duration,
    pub gone_live_retention: Duration,
    pub dead_max_per_tick: u32,
    pub gone_live_max_per_tick: u32,
    pub gone_max_nodes_per_tick: u32,
    /// Whether this instance runs the dead-letter pass. Only one instance
    /// should, to avoid double work; the EC2 instance owns it.
    pub run_dead_prune: bool,
}

impl ReconcilerConfig {
    /// EC2-detector instance config, read from `SPACE_RECONCILE_QUEUE_GC_*`.
    /// Owns the dead-letter pass.
    pub fn from_env_ec2() -> Self {
        Self::from_environment_ec2(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment_ec2(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        Self {
            enabled: config.truthy_default("SPACE_RECONCILE_QUEUE_GC_ENABLED", false),
            dry_run: false,
            // A test hook: Kind shortens the tick so its smoke waits seconds,
            // not up to five minutes, for one.
            tick: config.test_duration_secs("SPACE_RECONCILE_QUEUE_GC_TICK_SECS", DEFAULT_TICK),
            dead_retention: DEFAULT_DEAD_RETENTION,
            gone_live_retention: DEFAULT_GONE_LIVE_RETENTION,
            dead_max_per_tick: DEFAULT_DEAD_MAX_PER_TICK,
            gone_live_max_per_tick: DEFAULT_GONE_LIVE_MAX_PER_TICK,
            gone_max_nodes_per_tick: DEFAULT_GONE_MAX_NODES_PER_TICK,
            run_dead_prune: true,
        }
    }

    /// Kubernetes-presence-detector instance config, read from
    /// `SPACE_GONE_NODE_QUEUE_GC_*`. Does not run the dead-letter pass (the EC2
    /// instance owns it).
    pub fn from_env_kube() -> Self {
        Self::from_environment_kube(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment_kube(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Self {
        let config = env_config::EnvConfig(env);
        Self {
            enabled: config.destructive_gate("SPACE_GONE_NODE_QUEUE_GC_ENABLED"),
            dry_run: config.truthy_default("SPACE_GONE_NODE_QUEUE_GC_DRY_RUN", true),
            tick: DEFAULT_TICK,
            dead_retention: DEFAULT_DEAD_RETENTION,
            gone_live_retention: DEFAULT_GONE_LIVE_RETENTION,
            dead_max_per_tick: DEFAULT_DEAD_MAX_PER_TICK,
            gone_live_max_per_tick: DEFAULT_GONE_LIVE_MAX_PER_TICK,
            gone_max_nodes_per_tick: DEFAULT_GONE_MAX_NODES_PER_TICK,
            run_dead_prune: false,
        }
    }

    fn mode(&self) -> QueueGcMode {
        if self.dry_run {
            QueueGcMode::DryRun
        } else {
            QueueGcMode::Live
        }
    }
}

/// A `managed_by='k8s'`, `drain_phase IS NULL`, non-`ready` node holding stale
/// live refs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoneCandidate {
    pub node_id: NodeId,
    /// `node_k8s.k8s_node_name` — the EC2 `PrivateDnsName` on EKS and the k8s
    /// reflector store key. Used by both detectors.
    pub k8s_node_name: String,
    /// `node_k8s.k8s_node_uid` at scan time; the prune rechecks it so a
    /// same-name replacement Node is refused.
    pub k8s_node_uid: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneCounts {
    pub queue: u64,
    pub triggers: u64,
}

impl PruneCounts {
    pub(crate) fn total(self) -> u64 {
        self.queue.saturating_add(self.triggers)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GoneNodeEvaluation {
    pub eligible: PruneCounts,
    pub deleted: PruneCounts,
}

pub trait QueueGcSink: Send + Sync + 'static {
    fn prune_dead(
        &self,
        retention: Duration,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<PruneCounts, sqlx::Error>> + Send;

    fn list_gone_candidates(
        &self,
        retention: Duration,
        limit: u32,
        after: Option<NodeId>,
    ) -> impl std::future::Future<Output = Result<Vec<GoneCandidate>, sqlx::Error>> + Send;

    fn evaluate_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
        retention: Duration,
        claim_active_grace: Duration,
        limit: u32,
        mode: QueueGcMode,
    ) -> impl std::future::Future<Output = Result<GoneNodeEvaluation, sqlx::Error>> + Send;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub cache_cold: bool,
    pub dead_deleted: u64,
    /// Nodes confirmed gone and eligible for at least one bounded queue-GC
    /// operation. Uses the same final database guards in dry-run and live.
    pub gone_nodes: u64,
    pub gone_live_eligible: u64,
    pub gone_live_deleted: u64,
    pub candidates_scanned: usize,
    pub skipped_live: usize,
    /// Candidates the detector could not decide on (e.g. an EC2 lookup error);
    /// refs preserved, retried next tick.
    pub skipped_unknown: usize,
    pub skipped_drift: usize,
    pub rate_limited: usize,
    pub db_errors: usize,
}

impl TickReport {
    fn result(&self) -> QueueGcResult {
        if self.cache_cold {
            QueueGcResult::CacheCold
        } else if self.db_errors > 0 || self.skipped_unknown > 0 {
            QueueGcResult::Error
        } else {
            QueueGcResult::Success
        }
    }
}

pub struct Reconciler<S: QueueGcSink, D: GoneDetector> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    detector: D,
    scan_after: Option<NodeId>,
}

impl<S: QueueGcSink, D: GoneDetector> Reconciler<S, D> {
    pub fn new(config: ReconcilerConfig, sink: S, detector: D) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            detector,
            scan_after: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let detector = self.detector.label();
        let mode = self.config.mode();

        // Dry-run deletes nothing anywhere, including dead letters.
        if self.config.run_dead_prune && !self.config.dry_run {
            match self
                .sink
                .prune_dead(self.config.dead_retention, self.config.dead_max_per_tick)
                .await
            {
                Ok(counts) => report.dead_deleted = counts.total(),
                Err(error) => {
                    report.db_errors += 1;
                    tracing::warn!(%error, "reconcile_queue_gc: dead-letter prune failed");
                }
            }
        }

        // Establish membership; fail closed if the detector can't (e.g. a cold
        // or empty node reflector).
        if !self.detector.begin_tick().await {
            report.cache_cold = true;
            tracing::info!(
                detector = %detector,
                "reconcile_queue_gc: node membership unavailable; skipping tick"
            );
            emit_tick_metrics(&report, detector, mode);
            return report;
        }

        let candidates = match self
            .sink
            .list_gone_candidates(
                self.config.gone_live_retention,
                self.config.gone_max_nodes_per_tick,
                self.scan_after,
            )
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                report.db_errors += 1;
                tracing::warn!(%error, "reconcile_queue_gc: candidate scan failed");
                emit_tick_metrics(&report, detector, mode);
                return report;
            }
        };
        report.candidates_scanned = candidates.len();
        self.scan_after = if candidates.len()
            < usize::try_from(self.config.gone_max_nodes_per_tick).unwrap_or(usize::MAX)
        {
            None
        } else {
            candidates.last().map(|candidate| candidate.node_id)
        };

        let mut remaining = u64::from(self.config.gone_live_max_per_tick);
        for candidate in candidates {
            match self.detector.classify(&candidate).await {
                GoneVerdict::Live => {
                    report.skipped_live += 1;
                    continue;
                }
                GoneVerdict::Unknown => {
                    report.skipped_unknown += 1;
                    continue;
                }
                GoneVerdict::Gone => {}
            }

            if remaining == 0 {
                report.rate_limited += 1;
                continue;
            }

            let limit = u32::try_from(remaining).unwrap_or(u32::MAX);
            match self
                .sink
                .evaluate_gone_node(
                    candidate.node_id,
                    candidate.k8s_node_uid.clone(),
                    self.config.gone_live_retention,
                    CLAIM_ACTIVE_GRACE,
                    limit,
                    mode,
                )
                .await
            {
                Ok(evaluation) if evaluation.eligible.total() > 0 => {
                    report.gone_nodes = report.gone_nodes.saturating_add(1);
                    report.gone_live_eligible = report
                        .gone_live_eligible
                        .saturating_add(evaluation.eligible.total());
                    let budget_used = match mode {
                        QueueGcMode::DryRun => evaluation.eligible.total(),
                        QueueGcMode::Live => evaluation.deleted.total(),
                    };
                    remaining = remaining.saturating_sub(budget_used);

                    match mode {
                        QueueGcMode::DryRun => {
                            tracing::info!(
                                node_id = %candidate.node_id,
                                k8s_node_name = %candidate.k8s_node_name,
                                detector = %detector,
                                queue_eligible = evaluation.eligible.queue,
                                triggers_eligible = evaluation.eligible.triggers,
                                "reconcile_queue_gc: [dry-run] would prune refs for gone node"
                            );
                        }
                        QueueGcMode::Live if evaluation.deleted.total() > 0 => {
                            report.gone_live_deleted = report
                                .gone_live_deleted
                                .saturating_add(evaluation.deleted.total());
                            tracing::info!(
                                node_id = %candidate.node_id,
                                k8s_node_name = %candidate.k8s_node_name,
                                detector = %detector,
                                queue_deleted = evaluation.deleted.queue,
                                triggers_deleted = evaluation.deleted.triggers,
                                "reconcile_queue_gc: pruned refs for gone node"
                            );
                        }
                        QueueGcMode::Live => {
                            report.skipped_drift += 1;
                            tracing::info!(
                                node_id = %candidate.node_id,
                                k8s_node_name = %candidate.k8s_node_name,
                                "reconcile_queue_gc: eligibility changed before deletion; preserving refs"
                            );
                        }
                    }
                }
                Ok(_) => {
                    report.skipped_drift += 1;
                    tracing::info!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        "reconcile_queue_gc: identity or eligibility changed; preserving refs"
                    );
                }
                Err(error) => {
                    report.db_errors += 1;
                    tracing::warn!(
                        node_id = %candidate.node_id,
                        k8s_node_name = %candidate.k8s_node_name,
                        %error,
                        "reconcile_queue_gc: gone-node eligibility evaluation failed"
                    );
                }
            }
        }

        tracing::info!(
            detector = %detector,
            dry_run = self.config.dry_run,
            dead_deleted = report.dead_deleted,
            gone_nodes = report.gone_nodes,
            gone_live_eligible = report.gone_live_eligible,
            gone_live_deleted = report.gone_live_deleted,
            candidates_scanned = report.candidates_scanned,
            skipped_live = report.skipped_live,
            skipped_unknown = report.skipped_unknown,
            skipped_drift = report.skipped_drift,
            rate_limited = report.rate_limited,
            db_errors = report.db_errors,
            "reconcile_queue_gc: tick complete"
        );
        emit_tick_metrics(&report, detector, mode);
        report
    }
}

fn emit_tick_metrics(report: &TickReport, detector: QueueGcDetector, mode: QueueGcMode) {
    pplx_space_metrics::reconcile::queue_gc_pruned(
        QueueGcClass::Dead,
        detector,
        mode,
        report.dead_deleted,
    );
    pplx_space_metrics::reconcile::queue_gc_pruned(
        QueueGcClass::GoneLive,
        detector,
        mode,
        report.gone_live_deleted,
    );
    pplx_space_metrics::reconcile::queue_gc_eligible(detector, mode, report.gone_live_eligible);
    pplx_space_metrics::reconcile::queue_gc_gone_nodes(detector, mode, report.gone_nodes);
    pplx_space_metrics::reconcile::queue_gc_tick(report.result(), detector);
}

pub async fn run<S: QueueGcSink, D: GoneDetector>(
    config: ReconcilerConfig,
    sink: S,
    detector: D,
    shutdown_token: CancellationToken,
) {
    let detector_label = detector.label();
    if !config.enabled {
        tracing::info!(
            detector = %detector_label,
            "reconcile-queue GC is disabled; waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        detector = %detector_label,
        tick_secs = config.tick.as_secs(),
        gone_live_retention_secs = config.gone_live_retention.as_secs(),
        gone_live_max_per_tick = config.gone_live_max_per_tick,
        gone_max_nodes_per_tick = config.gone_max_nodes_per_tick,
        run_dead_prune = config.run_dead_prune,
        "starting reconcile-queue GC"
    );
    let mut reconciler = Reconciler::new(config.clone(), sink, detector);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick so a fresh deploy gets a full window for
    // the node reflector to warm before the first classification.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let _ = reconciler.tick().await;
            }
            _ = shutdown_token.cancelled() => return,
        }
    }
}
