//! Periodic reconciler that fails `started` sandboxes whose running state was
//! lost with their node.
//!
//! ## Why this exists
//!
//! When the node running a `started` sandbox disappears without a drain (the
//! instance is terminated, its kube Node removed), the sandbox's memory and
//! node-local disk are gone with it. The API's started recovery replaces the
//! execution and looks for a restore source; when the only verified suspend
//! snapshot predates the sandbox's last command or access, restoring it would
//! silently roll back the user's later work, so the API refuses and leaves the
//! execution `recovery_failed`. Nothing else moves the row afterwards: it reads
//! `started` forever, and every connect repeats the same refusal.
//!
//! This loop gives that outcome a terminal state. It flips such a sandbox to
//! `lifecycle = 'failed'`, retires the failed recovery execution, and emits
//! `sandbox_lost` and `sandbox_failed`. ECW and native reads then report the
//! sandbox failed, and ECW control-plane calls answer with the fixed terminal
//! `Failed to place sandbox` error instead of the per-request refusal.
//!
//! ## Safety conditions
//!
//! A candidate (see [`store`]) must have:
//! - `lifecycle = 'started'`, unplaced, with no `active_node_id` or
//!   `target_node_id`;
//! - an active execution in `recovery_failed` with no pod identity, so the
//!   API's own recovery already ran and refused;
//! - no snapshot the failed-repair predicate would restore
//!   (`failed_repair_revert_eligibility`, the same rule `failed_restorable_repair`
//!   and the API's terminal recovery use), and no suspend upload in flight;
//! - a previous execution that ran on a node, retired at least
//!   [`ReconcilerConfig::min_age`] ago.
//!
//! The node that last ran it must then be **confirmed gone**, with the evidence
//! the existing lost-sandbox give-ups use: a direct Kubernetes GET reports the
//! Node absent, and EC2 reports no instance for its private DNS name, or only
//! `terminated` ones. Anything else, including lookup errors, keeps the row.
//! A node that might return is never acted on, because its local disk could
//! still hold the newer state.
//!
//! ## What it keeps
//!
//! The flip changes the lifecycle, retires the refused execution, and drops the
//! lost node's last observation of the sandbox, which still reports it running
//! and would otherwise keep ECW reads saying so. The current
//! local artifact marker and every snapshot row stay as they are, so snapshot
//! retention keeps protecting the older snapshot (it never prunes a sandbox's
//! marked snapshot or its newest ready ones, and a failed sandbox takes no new
//! ones). Nothing restores it automatically: `failed_restorable_repair` uses
//! the same eligibility rule and declines these rows.
//!
//! Off by default; `SPACE_LOST_STARTED_SANDBOX_RECONCILER_ENABLED=true` opts in.
//! The EC2 lookup is skipped entirely while it is disabled.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::orphaned_nodes::{InstanceLookup, InstanceLookupOutcome};
use crate::reconcile_queue_gc::detector::KubeNodePresence;

pub mod store;

#[cfg(test)]
mod tests;

pub use store::LostStartedDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_MAX_PER_TICK: u32 = 25;
/// How long the lost node's execution must have been retired. Leaves the API's
/// own recovery attempts, and a node that is only briefly unreachable, time to
/// settle before the instance verdict is trusted.
pub(crate) const DEFAULT_MIN_AGE: Duration = Duration::from_secs(600);
/// Bound on each node lookup, so one slow API call cannot stall the tick.
const NODE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
/// `reason` on the emitted events: a started sandbox whose node was lost.
pub(crate) const LOST_REASON: &str = "started_node_lost";

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default, like the other reconcilers that act
    /// on an EC2 instance verdict.
    pub enabled: bool,
    pub tick: Duration,
    /// Most sandboxes one tick may fail.
    pub max_per_tick: u32,
    pub min_age: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            max_per_tick: DEFAULT_MAX_PER_TICK,
            min_age: DEFAULT_MIN_AGE,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars. Helm renders these under
    /// `clusterController.reconcilers.lostStartedSandboxes.*`.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::EnvConfig(env).truthy_default(
                "SPACE_LOST_STARTED_SANDBOX_RECONCILER_ENABLED",
                default.enabled,
            ),
            ..default
        }
    }
}

/// A `started` sandbox whose recovery already refused, and the node it last
/// ran on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostStartedCandidate {
    pub sandbox_id: SandboxId,
    /// Generation of the active `recovery_failed` execution.
    pub recovery_generation: i64,
    /// `k8s_node_name` of the newest earlier execution that ran on a node,
    /// which is the EC2 private DNS name on EKS.
    pub lost_node_name: String,
}

/// Postgres-side surface. Production impl: [`LostStartedDbStore`].
pub trait LostStartedSink: Send + Sync + 'static {
    /// Up to `limit` candidates with `sandboxes.id > after`, ascending.
    fn list_candidates(
        &self,
        limit: i64,
        after: Option<SandboxId>,
        min_age: Duration,
    ) -> impl std::future::Future<Output = Result<Vec<LostStartedCandidate>, sqlx::Error>> + Send;

    /// Fail the sandbox and retire its recovery execution, re-checking every
    /// candidate condition under the sandbox lock. `Ok(false)` when the row
    /// changed since the scan.
    fn mark_lost(
        &self,
        candidate: &LostStartedCandidate,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
}

/// Why a candidate was or was not failed this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateOutcome {
    MarkedLost,
    /// The kube Node exists, or EC2 reports a live instance.
    NodePresent,
    /// A node lookup failed or timed out; retried next tick.
    LookupFailed,
    /// The row changed between scan and apply.
    Raced,
    ApplyFailed,
    RateLimited,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: usize,
    pub marked_lost: usize,
    pub node_present: usize,
    pub lookup_failed: usize,
    pub raced: usize,
    pub apply_failed: usize,
    pub rate_limited: usize,
}

impl TickReport {
    fn record(&mut self, outcome: CandidateOutcome) {
        let counter = match outcome {
            CandidateOutcome::MarkedLost => &mut self.marked_lost,
            CandidateOutcome::NodePresent => &mut self.node_present,
            CandidateOutcome::LookupFailed => &mut self.lookup_failed,
            CandidateOutcome::Raced => &mut self.raced,
            CandidateOutcome::ApplyFailed => &mut self.apply_failed,
            CandidateOutcome::RateLimited => &mut self.rate_limited,
        };
        *counter += 1;
    }
}

pub struct Reconciler<S, K, L> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    presence: Arc<K>,
    lookup: Arc<L>,
    /// Rolling cursor over `sandboxes.id`, so rows whose node is still present
    /// cannot starve the ones behind them. Wraps when a page comes back short.
    scan_after: Option<SandboxId>,
}

impl<S, K, L> Reconciler<S, K, L>
where
    S: LostStartedSink,
    K: KubeNodePresence,
    L: InstanceLookup,
{
    pub fn new(config: ReconcilerConfig, sink: S, presence: K, lookup: L) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            presence: Arc::new(presence),
            lookup: Arc::new(lookup),
            scan_after: None,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let limit = i64::from(self.config.max_per_tick.saturating_mul(2));
        let candidates = match self
            .sink
            .list_candidates(limit, self.scan_after, self.config.min_age)
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "lost_started_sandboxes: list_candidates failed");
                return report;
            }
        };
        report.scanned = candidates.len();
        self.scan_after = if (candidates.len() as i64) < limit {
            None
        } else {
            candidates.last().map(|candidate| candidate.sandbox_id)
        };

        let cap = usize::try_from(self.config.max_per_tick).unwrap_or(usize::MAX);
        for candidate in candidates {
            let outcome = if report.marked_lost >= cap {
                CandidateOutcome::RateLimited
            } else {
                self.process(&candidate).await
            };
            report.record(outcome);
        }
        report
    }

    async fn process(&self, candidate: &LostStartedCandidate) -> CandidateOutcome {
        match self.node_gone(candidate).await {
            Ok(true) => {}
            Ok(false) => return CandidateOutcome::NodePresent,
            Err(()) => return CandidateOutcome::LookupFailed,
        }
        match self.sink.mark_lost(candidate).await {
            Ok(true) => {
                emit_lost_events(candidate);
                CandidateOutcome::MarkedLost
            }
            Ok(false) => CandidateOutcome::Raced,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    %error,
                    "lost_started_sandboxes: mark lost failed; will retry next tick"
                );
                CandidateOutcome::ApplyFailed
            }
        }
    }

    /// Whether the node is confirmed gone: absent from Kubernetes and without
    /// a live EC2 instance. `Err` when either answer is unknown.
    async fn node_gone(&self, candidate: &LostStartedCandidate) -> Result<bool, ()> {
        let node_name = &candidate.lost_node_name;
        match tokio::time::timeout(
            NODE_LOOKUP_TIMEOUT,
            self.presence.node_exists(node_name.clone()),
        )
        .await
        {
            Ok(Ok(false)) => {}
            Ok(Ok(true)) => return Ok(false),
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    k8s_node_name = %node_name,
                    %error,
                    "lost_started_sandboxes: kube node lookup failed; keeping the sandbox"
                );
                return Err(());
            }
            Err(_) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    k8s_node_name = %node_name,
                    "lost_started_sandboxes: kube node lookup timed out; keeping the sandbox"
                );
                return Err(());
            }
        }
        match tokio::time::timeout(
            NODE_LOOKUP_TIMEOUT,
            self.lookup.lookup_by_private_dns(node_name.clone()),
        )
        .await
        {
            Ok(Ok(InstanceLookupOutcome::Gone)) => Ok(true),
            Ok(Ok(InstanceLookupOutcome::Live { .. })) => Ok(false),
            Ok(Err(error)) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    k8s_node_name = %node_name,
                    %error,
                    "lost_started_sandboxes: ec2 lookup failed; keeping the sandbox"
                );
                Err(())
            }
            Err(_) => {
                tracing::warn!(
                    sandbox_id = %candidate.sandbox_id,
                    k8s_node_name = %node_name,
                    "lost_started_sandboxes: ec2 lookup timed out; keeping the sandbox"
                );
                Err(())
            }
        }
    }
}

fn emit_lost_events(candidate: &LostStartedCandidate) {
    let details = SandboxEventDetails::default()
        .with_from_state("started")
        .with_to_state("failed")
        .source_node_name(candidate.lost_node_name.clone())
        .execution_generation(candidate.recovery_generation.to_string())
        .reason(LOST_REASON)
        .trigger(SandboxEventTrigger::System);
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::SandboxLost,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.clone());
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::SandboxFailed,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.operation("lost_started_sandboxes"));
}

/// Drive the reconciler loop until `shutdown_token` fires. A disabled config
/// waits on the token without ticking.
pub async fn run<S, K, L>(
    config: ReconcilerConfig,
    sink: S,
    presence: K,
    lookup: L,
    shutdown_token: CancellationToken,
) where
    S: LostStartedSink,
    K: KubeNodePresence,
    L: InstanceLookup,
{
    if !config.enabled {
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        max_per_tick = config.max_per_tick,
        min_age_secs = config.min_age.as_secs(),
        "starting lost-started-sandboxes reconciler"
    );
    let mut reconciler = Reconciler::new(config.clone(), sink, presence, lookup);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so inventory sync settles after a deploy.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let report = reconciler.tick().await;
                if report.marked_lost > 0 || report.lookup_failed > 0 || report.apply_failed > 0 {
                    tracing::info!(?report, "lost-started-sandboxes tick");
                }
            }
            _ = shutdown_token.cancelled() => break,
        }
    }
    tracing::info!("lost-started-sandboxes reconciler stopping");
}
