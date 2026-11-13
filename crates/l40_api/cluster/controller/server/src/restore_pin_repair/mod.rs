//! Cluster-singleton repair that pins unpinned suspended Kubernetes
//! sandboxes to the artifact their node holds.
//!
//! ## Why this exists
//!
//! A restore of a pinned row loads exactly its pinned artifact. An unpinned
//! row restores by capture order, which a re-upload of an older capture can
//! mislead when no capture records a generation, and which refuses outright
//! once a capture without a generation registers after one with a generation
//! (`pplx_space_sandbox_portability::restore_order_unordered`). A row a
//! node-local suspend finalized without pinning, from a build whose idle
//! suspends didn't pin or with a capture its node didn't report, is unpinned.
//!
//! The node a suspended sandbox is placed on keeps the artifact it suspended
//! in its own row, and reports it through the node-api `SandboxState` RPC.
//! Each tick asks that node about a batch of unpinned rows and pins each one
//! whose report the control plane's evidence agrees with
//! ([`pplx_space_sandbox_store::restore_pin_repair::repair_restore_pin`]).
//! A row it can't pin is logged with the reason, for an operator: its node
//! is gone or unreachable, no longer holds the sandbox suspended, or holds
//! an artifact that isn't the latest capture the control plane knows of.
//!
//! It runs for both substrates, on the running substrate's rows, and reaches
//! the node through the substrate's own endpoint resolution.
//!
//! The repair runs after a deploy, as a backfill: ship it disabled, enable it
//! with `SPACE_RESTORE_PIN_REPAIR_ENABLED=true`, and disable it again once
//! the unpinned population stops shrinking.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{self, StreamExt};
use pplx_space_api::controllers::substrate::{
    Substrate, SubstrateAdapter, connect_retrying_node_api_client_for_target,
    sandbox_state_with_node_api,
};
use pplx_space_common_core::types::{LocalArtifactId, NodeManagedBy, SandboxId};
use pplx_space_sandbox_store::restore_pin_repair::{
    RestorePinRepairCandidate, repair_restore_pin, restore_pin_repair_candidates,
};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::metrics::cluster_reconciler::{self as metrics, Loop};

#[cfg(test)]
mod tests;

const DEFAULT_TICK: Duration = Duration::from_secs(30);
const DEFAULT_BATCH: u32 = 100;
/// The most rows one tick asks about: each is a node-api call.
const MAX_BATCH: u32 = 1_000;
/// Node probes a tick runs at once.
const REPAIR_CONCURRENCY: usize = 8;

/// What one candidate came to.
enum Outcome {
    Pinned,
    Unrepairable,
    /// Not decided: shutdown, an unreachable node or a failed write, all
    /// retried on a later pass.
    Skipped,
}

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Off by default: a backfill an operator runs after a deploy.
    pub enabled: bool,
    pub tick: Duration,
    /// Rows asked about per tick, one node-api call each.
    pub batch: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            batch: DEFAULT_BATCH,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_env_vars::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_env_vars::Environment) -> Self {
        let env = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: env
                .0
                .var("SPACE_RESTORE_PIN_REPAIR_ENABLED")
                .ok()
                .and_then(|raw| pplx_space_env_vars::parse_env_flag(&raw))
                .unwrap_or(default.enabled),
            tick: env.duration_secs("SPACE_RESTORE_PIN_REPAIR_TICK_SECS", default.tick),
            batch: env.bounded_u32("SPACE_RESTORE_PIN_REPAIR_BATCH", default.batch, MAX_BATCH),
        }
    }
}

/// What a candidate's node says it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePin {
    /// The node holds the sandbox suspended, with this artifact pinned.
    Held(LocalArtifactId),
    /// The node's answer can't name the artifact: the reason.
    Unusable(&'static str),
}

/// The repair's Postgres and node-api surface. Production: [`PoolSink`].
pub trait RepairSink: Send + Sync + 'static {
    fn candidates(
        &self,
        after: Option<SandboxId>,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<RestorePinRepairCandidate>, String>> + Send;
    fn node_pin(
        &self,
        candidate: RestorePinRepairCandidate,
    ) -> impl Future<Output = Result<NodePin, String>> + Send;
    fn repair(
        &self,
        candidate: RestorePinRepairCandidate,
        artifact: LocalArtifactId,
    ) -> impl Future<Output = Result<bool, String>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub pinned: usize,
    pub unrepairable: usize,
}

pub struct Reconciler<S: RepairSink> {
    config: ReconcilerConfig,
    sink: Arc<S>,
    /// The last row a tick reached; the next tick resumes after it and wraps
    /// to the start once a pass finds nothing more.
    cursor: Mutex<Option<SandboxId>>,
}

impl<S: RepairSink> Reconciler<S> {
    pub fn new(config: ReconcilerConfig, sink: S) -> Self {
        Self {
            config,
            sink: Arc::new(sink),
            cursor: Mutex::new(None),
        }
    }

    /// One batch. It stops at `shutdown`, before the next node call or pin
    /// write, so a lost lease doesn't keep pinning.
    pub async fn tick(&self, shutdown: &CancellationToken) -> TickReport {
        let mut report = TickReport::default();
        let mut cursor = self.cursor.lock().await;
        let candidates = match self.sink.candidates(*cursor, self.config.batch).await {
            Ok(candidates) => candidates,
            Err(error) => {
                tracing::warn!(%error, "restore_pin_repair: listing candidates failed");
                return report;
            }
        };
        *cursor = candidates.last().map(|candidate| candidate.sandbox_id);
        // Candidates are independent, and a node that doesn't answer holds its
        // probe for the RPC budget: probe several at once.
        let outcomes: Vec<Outcome> = stream::iter(candidates)
            .map(|candidate| self.repair_one(candidate, shutdown))
            .buffer_unordered(REPAIR_CONCURRENCY)
            .collect()
            .await;
        for outcome in outcomes {
            match outcome {
                Outcome::Pinned => report.pinned += 1,
                Outcome::Unrepairable => report.unrepairable += 1,
                Outcome::Skipped => {}
            }
        }
        report
    }

    /// Ask `candidate`'s node and pin what it holds, unless `shutdown` fires
    /// first: before the node call, during it, or before the write.
    async fn repair_one(
        &self,
        candidate: RestorePinRepairCandidate,
        shutdown: &CancellationToken,
    ) -> Outcome {
        if shutdown.is_cancelled() {
            return Outcome::Skipped;
        }
        let answer = tokio::select! {
            biased;
            () = shutdown.cancelled() => return Outcome::Skipped,
            answer = self.sink.node_pin(candidate) => answer,
        };
        if shutdown.is_cancelled() {
            return Outcome::Skipped;
        }
        let reason = match answer {
            Ok(NodePin::Held(artifact)) => match self.sink.repair(candidate, artifact).await {
                Ok(true) => {
                    tracing::info!(
                        sandbox_id = %candidate.sandbox_id,
                        node_id = %candidate.node_id,
                        local_artifact_id = %artifact,
                        "restore_pin_repair: pinned the node's artifact"
                    );
                    return Outcome::Pinned;
                }
                Ok(false) => {
                    "the store refused the pin: the row changed since it was read, or the node's \
                     artifact has no ready backup from it that's still the latest capture"
                }
                Err(error) => {
                    tracing::warn!(sandbox_id = %candidate.sandbox_id, %error,
                        "restore_pin_repair: repair write failed");
                    return Outcome::Skipped;
                }
            },
            Ok(NodePin::Unusable(reason)) => reason,
            Err(error) => {
                tracing::warn!(sandbox_id = %candidate.sandbox_id,
                    node_id = %candidate.node_id, %error,
                    "restore_pin_repair: node unreachable; will retry next pass");
                return Outcome::Skipped;
            }
        };
        tracing::info!(
            sandbox_id = %candidate.sandbox_id,
            node_id = %candidate.node_id,
            reason,
            "restore_pin_repair: can't pin; an operator can pin its capture through the admin \
             restore-pin route"
        );
        Outcome::Unrepairable
    }
}

/// What a node's `SandboxState` report says about the pin.
pub fn node_pin_from_report(
    report: &pplx_space_api::controllers::substrate::NodeReportedArtifact,
) -> NodePin {
    if !report.present {
        return NodePin::Unusable("the node no longer holds the sandbox");
    }
    if report.state_degraded || report.pin_unreadable {
        return NodePin::Unusable("the node's report is degraded or its pin unreadable");
    }
    if !report.reports_suspended || report.vm_alive {
        return NodePin::Unusable("the node doesn't hold the sandbox suspended");
    }
    match report.pinned_artifact {
        Some(artifact) => NodePin::Held(artifact),
        None => NodePin::Unusable("the node holds no artifact for the sandbox"),
    }
}

/// The production sink: Postgres, and the node-api through the Kubernetes
/// substrate's endpoint resolution.
pub struct PoolSink {
    pool: PgPool,
    substrate: Arc<Substrate>,
}

impl PoolSink {
    pub fn new(pool: PgPool, substrate: Arc<Substrate>) -> Self {
        Self { pool, substrate }
    }
}

impl RepairSink for PoolSink {
    async fn candidates(
        &self,
        after: Option<SandboxId>,
        limit: u32,
    ) -> Result<Vec<RestorePinRepairCandidate>, String> {
        restore_pin_repair_candidates(
            &self.pool,
            self.substrate.managed_by(),
            after,
            i64::from(limit),
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn node_pin(&self, candidate: RestorePinRepairCandidate) -> Result<NodePin, String> {
        let Some(node) = self
            .substrate
            .node_api_target(candidate.node_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(NodePin::Unusable("the node is gone"));
        };
        if node.managed_by == NodeManagedBy::Space && node.direct_endpoint.is_none() {
            return Ok(NodePin::Unusable("the node has no node-api endpoint"));
        }
        let (addr, mut client) = connect_retrying_node_api_client_for_target(
            &*self.substrate,
            &node.as_target(),
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| error.to_string())?;
        let report = sandbox_state_with_node_api(&mut client, &addr, candidate.sandbox_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(node_pin_from_report(&report))
    }

    async fn repair(
        &self,
        candidate: RestorePinRepairCandidate,
        artifact: LocalArtifactId,
    ) -> Result<bool, String> {
        repair_restore_pin(&self.pool, candidate, artifact)
            .await
            .map_err(|error| error.to_string())
    }
}

/// Run the repair until shutdown. A disabled repair waits for shutdown.
pub async fn run<S: RepairSink>(config: ReconcilerConfig, sink: S, shutdown: CancellationToken) {
    if !config.enabled {
        tracing::info!(
            "restore_pin_repair disabled (SPACE_RESTORE_PIN_REPAIR_ENABLED unset); \
             waiting for shutdown"
        );
        shutdown.cancelled().await;
        return;
    }
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        batch = config.batch,
        "starting restore_pin_repair"
    );
    let reconciler = Reconciler::new(config.clone(), sink);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = reconciler.tick(&shutdown).await;
                if report != TickReport::default() {
                    tracing::info!(pinned = report.pinned, unrepairable = report.unrepairable,
                        "restore_pin_repair tick");
                }
                metrics::loop_latency(Loop::RestorePinRepair, started);
            }
            _ = shutdown.cancelled() => {
                tracing::info!("restore_pin_repair stopping");
                return;
            }
        }
    }
}
