//! Cluster-singleton reaper that terminalizes suspend-snapshot rows stuck
//! in `state = 'creating'` past the on-node upload worker's full retry
//! window.
//!
//! ## Why this exists
//!
//! The state-based node-drain gate (the `draining → drained` blocker and
//! the `drained → terminating` guard) keys "is the suspend-snapshot upload
//! still in flight?" on `snapshots.state = 'creating'`, deliberately **not**
//! on age — a short age cutoff in the per-tick drain predicate would
//! re-introduce the premature-give-up data-loss race for a slow-but-alive
//! upload.
//!
//! A `creating` row is normally terminalized by the on-node upload worker:
//! `state = 'ready'` on success, or `state = 'failed'` after it exhausts
//! retries (`MAX_ATTEMPTS = 5` × `JOB_TIMEOUT = 7200s` + backoff ≈ 10h) via
//! a best-effort `FinalizeArtifactBackup(FAILED)`. Two residual cases leave
//! a row `creating` forever:
//!
//! 1. The worker process dies **and** its best-effort mark-`failed` RPC also
//!    fails (cluster-api unreachable from the node at that moment), with no
//!    node restart to recover the durable job.
//! 2. Any other path that orphans a `creating` row without finalization.
//!
//! With the state-based gate, such a row blocks the source node's drain
//! **indefinitely** (a visible, alertable stuck-drain — *not* silent data
//! loss) and is excluded from the reupload/give-up path. This reaper is the
//! cluster-wide backstop for that rare double-failure.
//!
//! ## Provably-dead bound
//!
//! The reaper only flips rows older than the worker's *full* retry window,
//! so a slow-but-alive upload is never cut off. The worst-case wall-clock
//! from snapshot creation to permanent worker give-up is
//! `5 × 7200s + (5 + 30 + 120 + 300)s ≈ 10.13h` (per-attempt timeout × max
//! attempts, plus the fixed inter-attempt backoff schedule). The default
//! [`DEFAULT_MIN_AGE_SECS`] of 24h leaves >2× margin over that window, so a
//! reaped row is unambiguously past any legitimate upload — at that age,
//! ageing-out is safe (unlike in the per-tick drain predicate, where a short
//! age cutoff loses data).
//!
//! ## Age anchor
//!
//! The reaper ages on `COALESCE(last_attempt_at, timestamp)` — last observed
//! worker activity, falling back to the immutable creation stamp.
//!
//! `snapshots.timestamp` is set to `NOW()` at INSERT (in
//! `ClusterApi.BeginArtifactBackup`, which writes the `state = 'creating'`
//! row at the head of the upload pipeline) and is **never updated**. Ageing on
//! `timestamp` alone is wrong, though: wall-clock age over-counts when the
//! source node / sbx-mgr is down for part of the window and then recovers.
//! `BackgroundWorker::recover` requeues the durable upload job on sbx-mgr
//! restart and the remaining attempts run, so a slow-but-alive upload can
//! finalize long after creation — but a `timestamp`-only reaper would already
//! have flipped it to `failed`, and its late `READY` finalize would be
//! rejected as a crossed terminal state, falsely losing the sandbox.
//!
//! `last_attempt_at` closes that gap: the on-node upload worker bumps it to
//! `NOW()` at the start of each retry attempt, so a node outage that pauses
//! the worker's retry clock also pauses the reaper's clock. An upload that
//! resumes after a long downtime carries a recent heartbeat and is never
//! reaped mid-flight; only an upload with no recent attempt — the worker truly
//! gone — ages out.
//!
//! The heartbeat *writer* (a `TouchArtifactBackup` cluster-api RPC the worker
//! calls per attempt) lands in a follow-up. Until then `last_attempt_at` is
//! NULL and `COALESCE` collapses to the creation-time anchor, which is why the
//! reaper ships **default-disabled** — enable it only once the heartbeat
//! writer exists. The column itself is added in this change's migration so the
//! anchor never has to be revisited.
//!
//! ## After the flip
//!
//! Once a row is `failed`, the existing give-up path takes over:
//! `list_pinned_local_only_sandboxes_on_node → reupload → mark_sandbox_lost`
//! handles recovery-or-loss correctly, and the drain gate (which excludes
//! non-`creating` rows from the in-flight predicate) unblocks. The reaper
//! itself does no substrate cleanup and no node interaction — it is a pure,
//! substrate-agnostic Postgres CAS plus telemetry, so it runs in both Direct
//! and Kubernetes modes.

use pplx_space_common_core::types::{NodeId, SnapshotId};

use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{PositiveDuration, SandboxId};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::cluster_reconciler::{self as metrics, Loop, OutcomeEvent};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::env_config;

pub mod store;

#[cfg(test)]
mod tests;

pub use store::SnapshotDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
/// Default provably-dead age threshold. The on-node suspend-snapshot worker's
/// full retry window is `5 × 7200s + (5 + 30 + 120 + 300)s ≈ 10.13h`; 24h
/// leaves >2× margin so the reaper can never cut off a slow-but-alive (or
/// recently node-restarted, and therefore resumed) upload.
pub(crate) const DEFAULT_MIN_AGE_SECS: u64 = 86_400;

/// Stamped into `snapshots.failure_reason` when the reaper flips a row. Only
/// applied when the column is still NULL (`COALESCE`), so a reason the worker
/// managed to record on an earlier best-effort attempt is preserved.
pub(crate) const REAP_FAILURE_REASON: &str = "reaped by cluster-controller: stuck in state=creating past the suspend-snapshot \
     worker retry window";

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default everywhere; each environment opts
    /// in via `SPACE_STUCK_SNAPSHOT_REAPER_ENABLED=true`. Same shape as the
    /// sibling sweepers (`snapshot_storage_sweeper`,
    /// `transient_lifecycle_sweeper`).
    pub enabled: bool,
    pub tick: Duration,
    /// Minimum age a `creating` row must have before the reaper flips it.
    /// Always positive — `PositiveDuration` carries the non-zero invariant
    /// at the type level. "Disabled" is expressed via [`Self::enabled`], not
    /// a zero sentinel: a zero age here would reap every in-flight upload the
    /// instant it started, which is exactly the premature-give-up data-loss
    /// hazard the state-based gate was designed to avoid.
    pub min_age: PositiveDuration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            min_age: PositiveDuration::new_panic(Duration::from_secs(DEFAULT_MIN_AGE_SECS)),
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.stuckSnapshotReaper.*`.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default(
                "SPACE_STUCK_SNAPSHOT_REAPER_ENABLED",
                default.enabled,
            ),
            tick: default.tick,
            min_age: default.min_age,
        }
    }
}

/// One reaped suspend-snapshot row, surfaced by [`ReaperSink::reap_stuck_creating`]
/// so the reconciler can emit a per-snapshot sandbox event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapedSnapshot {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    /// Node that owned the upload. Nullable in the schema, so carried as an
    /// `Option`; recorded on the event as a diagnostic only.
    pub source_node_id: Option<NodeId>,
}

/// DB surface the reaper depends on. Production impl: [`SnapshotDbStore`];
/// `tests::FakeSink` for unit tests.
pub trait ReaperSink: Send + Sync + 'static {
    /// Atomically flip every `type = 'full'` suspend-snapshot row in
    /// `state = 'creating'` whose `timestamp < NOW() - min_age` to
    /// `state = 'failed'`, and return the rows actually claimed.
    /// Implementations must perform the match and UPDATE as a single
    /// `UPDATE ... RETURNING` statement so a row the on-node worker
    /// finalized under us (to `ready` or `failed`) drops out of the
    /// predicate and is left alone.
    fn reap_stuck_creating(
        &self,
        min_age: PositiveDuration,
    ) -> impl std::future::Future<Output = Result<Vec<ReapedSnapshot>, sqlx::Error>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub reaped: usize,
}

pub struct Reconciler<S: ReaperSink> {
    config: ReconcilerConfig,
    sink: Arc<S>,
}

impl<S: ReaperSink> Reconciler<S> {
    pub fn new(config: ReconcilerConfig, sink: S) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
        }
    }

    pub async fn tick(&self) -> TickReport {
        let mut report = TickReport::default();
        let min_age = self.config.min_age;
        let reaped = match self.sink.reap_stuck_creating(min_age).await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "stuck_snapshot_reaper: reap_stuck_creating failed; will retry next tick"
                );
                return report;
            }
        };
        if reaped.is_empty() {
            return report;
        }
        report.reaped = reaped.len();

        for row in reaped {
            let mut event = OutcomeEvent::stuck_snapshot_upload_failed()
                .sandbox_id(row.sandbox_id)
                .snapshot_id(row.snapshot_id)
                .age_secs(min_age.get().as_secs())
                .detail(
                    "flipped provably-abandoned creating suspend snapshot to state=failed; \
                     source node drain unblocked and give-up path re-enabled",
                );
            if let Some(node) = row.source_node_id {
                event = event.node(node.to_string());
            }
            event.warn();
            emit_reap_event(&row, min_age);
        }
        report
    }
}

/// Emit a `suspend_failed` sandbox event for one reaped snapshot. Reaping a
/// stuck `creating` row to `failed` is a failed durable suspend-snapshot
/// transition, so it belongs on the `space.sandbox` stream per the
/// observability conventions; the component identity lives in `source`
/// (`cluster_controller`) and the causality in `trigger` (`system`).
fn emit_reap_event(row: &ReapedSnapshot, min_age: PositiveDuration) {
    let mut details = SandboxEventDetails::default()
        .reason("stuck_creating_snapshot_reaped")
        .operation("stuck_snapshot_reap")
        .trigger(SandboxEventTrigger::System)
        .with_from_state("creating")
        .target_state("failed")
        .snapshot_id(row.snapshot_id.to_string())
        .age_secs(min_age.get().as_secs().to_string());
    if let Some(node) = row.source_node_id {
        details = details.source_node_id(node.to_string());
    }
    SandboxEvent::new(
        row.sandbox_id,
        SandboxEventType::SuspendFailed,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// Run the reaper on the current task until the shutdown token is cancelled.
/// Spawned by `space-cluster-controller` alongside the other
/// cluster-singleton reconcilers.
pub async fn run<S: ReaperSink>(
    config: ReconcilerConfig,
    sink: S,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "stuck_snapshot_reaper disabled \
             (SPACE_STUCK_SNAPSHOT_REAPER_ENABLED unset); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        min_age_secs = config.min_age.get().as_secs(),
        "starting stuck_snapshot_reaper"
    );

    let reconciler = Reconciler::new(config.clone(), sink);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race with the rest of
    // the controller coming online.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let _ = reconciler.tick().await;
                metrics::loop_latency(Loop::StuckSnapshotReaper, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("stuck_snapshot_reaper stopping");
                return;
            }
        }
    }
}

/// Convenience entry point for `reconcilers.rs`: builds the production sink
/// from a `PgPool` and runs the loop. The reaper needs only Postgres — no
/// substrate, S3, or kube client — so this is the whole wiring.
pub async fn run_with_pool(
    config: ReconcilerConfig,
    pool: PgPool,
    shutdown_token: CancellationToken,
) {
    let sink = SnapshotDbStore::new(pool);
    run(config, sink, shutdown_token).await;
}
