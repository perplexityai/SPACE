//! Periodic give-up for `started` sandboxes whose source node is gone and
//! that no snapshot can restore without rolling them back.
//!
//! ## Why this exists
//!
//! When a started sandbox's pod disappears, started recovery re-arms its
//! active execution as `recovering` and looks for a restore source. It
//! accepts only the current suspend snapshot, and only if that snapshot
//! covers `last_command_at` and `last_access_at`: an older one would silently
//! roll the user back. With no such snapshot, the recovery fails, the
//! execution drops back to `recovery_failed`, and the wake answers
//! `RestoreContextUnavailable`. Nothing makes the sandbox terminal, so every
//! later wake runs the same cycle.
//!
//! Most of those failures are transient: the sandbox later suspends normally
//! once its node finishes the snapshot it was taking. They aren't transient
//! when the node the sandbox ran on is gone. Its VM memory and local disk
//! died with the instance, so no later snapshot can appear. The drain and
//! drained-node paths already give up on sandboxes pinned to a node that a
//! drain was managing; a node that dies outside a drain has no such path.
//!
//! ## What the loop does
//!
//! Each tick scans `started` Kubernetes sandboxes whose active execution is
//! `recovery_failed`, whose `active_node_id` names a node outside any drain
//! phase, and which have no snapshot that could restore them without a
//! rollback (see
//! [`pplx_space_sandbox_portability::started_restore_source_absent`]).
//! For each one it establishes that the source node is gone:
//!
//! - no `nodes` row has the sandbox's `active_node_id`, or
//! - the node row is registered and three independent checks agree:
//!   - its name has been absent from a warm, non-empty kube Node reflector for
//!     at least the absence grace (10 minutes by default, the gone-node queue
//!     GC's; sampled once a tick, so the actual wait can run up to two ticks
//!     past it), so
//!     a Node that is deleted and re-registered between two ticks is never
//!     judged on one snapshot;
//!   - EC2 has no live instance for it. Any instance state but `terminated`,
//!     including an unknown one, counts as live: the `orphaned_nodes` verdict.
//!     An empty answer counts as gone, because EC2 stops listing terminated
//!     instances a while after they terminate;
//!   - a direct Node GET returns NotFound.
//!
//!   Requiring the Kubernetes checks as well as EC2 keeps an EC2 answer that
//!   comes back empty for the wrong reason, such as a lookup against the
//!   wrong region or account, from failing the sandboxes of a live node.
//!   A cold or empty reflector defers every registered candidate.
//!
//! A gone source node flips the sandbox to `failed`, retires its execution,
//! and emits `sandbox_lost` and `sandbox_failed` events. The flip re-checks
//! every scan condition under the sandbox row lock, so a wake that re-armed
//! the execution in the meantime, a node re-registration, or a snapshot that
//! became ready all refuse it. Once the sandbox is `failed`, wakes answer
//! with the terminal-lifecycle rejection instead of re-running recovery, and
//! the node row stops being pinned for `orphaned_nodes`.
//!
//! ## Gating
//!
//! Off by default; `SPACE_LOST_STARTED_SANDBOXES_ENABLED=true` turns it on.
//! The registered-node arm needs AWS credentials, so the AWS SDK is only
//! initialized when the loop is enabled. Without EC2 access the lookup errors
//! defer each candidate, and only the deleted-node-row arm can act.

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use pplx_space_metrics::cluster_reconciler::{
    self as metrics, Loop, LostStartedOutcome, OutcomeEvent,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::orphaned_nodes::{InstanceLookup, InstanceLookupOutcome};
use crate::reconcile_queue_gc::detector::{
    DEFAULT_ABSENCE_GRACE, DIRECT_NODE_LOOKUP_TIMEOUT, KubeNodePresence,
};

pub mod store;

#[cfg(test)]
mod tests;

pub use store::LostStartedDbStore;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_BATCH_SIZE: u32 = 200;
/// Cap on terminal flips per tick, so at most 300 an hour at the default tick.
/// Limits the rate of damage if the gone-node checks regressed and suddenly
/// reported every node gone.
pub(crate) const DEFAULT_MAX_LOST_PER_TICK: u32 = 25;

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default; the operator opts in with
    /// `SPACE_LOST_STARTED_SANDBOXES_ENABLED=true`.
    pub enabled: bool,
    pub tick: Duration,
    pub batch_size: u32,
    pub max_lost_per_tick: u32,
    /// How long a registered node's name must stay absent from the reflector
    /// before EC2 and the API server are asked about it.
    pub absence_grace: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick: DEFAULT_TICK,
            batch_size: DEFAULT_BATCH_SIZE,
            max_lost_per_tick: DEFAULT_MAX_LOST_PER_TICK,
            absence_grace: DEFAULT_ABSENCE_GRACE,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config.truthy_default("SPACE_LOST_STARTED_SANDBOXES_ENABLED", default.enabled),
            ..default
        }
    }
}

/// The source node as the scan found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceNode {
    /// No `nodes` row has the sandbox's `active_node_id`.
    RowDeleted,
    /// A Kubernetes node row with its registered name, which is the EC2
    /// private DNS name on EKS, and the kube uid observed at scan time.
    Registered {
        k8s_node_name: String,
        k8s_node_uid: Option<String>,
    },
    /// The row exists but has no `node_k8s` name to look up.
    Unverifiable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostCandidate {
    pub sandbox_id: SandboxId,
    /// Generation of the active `recovery_failed` execution. The flip
    /// requires it unchanged, so a wake that re-armed the execution refuses.
    pub generation: i64,
    pub source_node_id: NodeId,
    pub source_node: SourceNode,
}

impl SourceNode {
    /// The `reason` a flip records for a source node it found gone.
    pub fn gone_reason(&self) -> &'static str {
        match self {
            Self::RowDeleted => "source_node_row_deleted",
            Self::Registered { .. } => "source_node_instance_terminated",
            Self::Unverifiable => "source_node_unverifiable",
        }
    }
}

pub trait LostStartedSink: Send + Sync + 'static {
    /// Up to `limit` candidates with `sandboxes.id > after`, ordered by id.
    fn list_candidates(
        &self,
        limit: u32,
        after: Option<SandboxId>,
    ) -> impl std::future::Future<Output = Result<Vec<LostCandidate>, sqlx::Error>> + Send;

    /// Flip the candidate to `failed`, once the tick established its source
    /// node is gone, if every scan condition still holds and the node is still
    /// gone the way `candidate.source_node` describes: no row for
    /// [`SourceNode::RowDeleted`], or the scanned uid outside any drain for
    /// [`SourceNode::Registered`]. `Ok(false)` means a guard refused.
    fn mark_lost(
        &self,
        candidate: &LostCandidate,
    ) -> impl std::future::Future<Output = Result<bool, sqlx::Error>> + Send;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub marked_lost: u32,
    pub source_live: u32,
    pub unverifiable: u32,
    pub lookup_errors: u32,
    pub cas_lost: u32,
    pub rate_limited: u32,
    pub errors: u32,
}

/// Whether a candidate's source node is gone. Answers are cached per node
/// name for the tick: every stuck sandbox from one dead node shares one.
#[derive(Debug, Clone, Copy)]
enum CachedVerdict {
    Gone,
    Live,
    /// The node row has no Kubernetes name to check.
    Unverifiable,
    /// The kube Node reflector is cold or empty, so absence means nothing.
    MembershipUnknown,
    /// Absent from the reflector, but not yet for the absence grace.
    AbsenceSettling,
    InstanceLookupFailed,
    NodeLookupFailed,
}

pub struct Reconciler<S: LostStartedSink, L: InstanceLookup, P: KubeNodePresence> {
    config: ReconcilerConfig,
    sink: S,
    lookup: L,
    presence: P,
    scan_after: Option<SandboxId>,
    /// When each registered node name was first seen absent from the
    /// reflector. Cleared for names a tick's sample lists again, and entirely
    /// on a tick whose membership is unknown. Samples are a tick apart, so a
    /// Node that returns and leaves again between two of them isn't seen; the
    /// EC2, direct GET and uid checks still apply.
    absent_since: HashMap<String, tokio::time::Instant>,
}

impl<S: LostStartedSink, L: InstanceLookup, P: KubeNodePresence> Reconciler<S, L, P> {
    pub fn new(config: ReconcilerConfig, sink: S, lookup: L, presence: P) -> Self {
        Self {
            config,
            sink,
            lookup,
            presence,
            scan_after: None,
            absent_since: HashMap::new(),
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let candidates = match self
            .sink
            .list_candidates(self.config.batch_size, self.scan_after)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "lost_started_sandboxes: list_candidates failed");
                return report;
            }
        };
        // A short page ends the sweep; the next tick starts from the
        // beginning. Advance before acting so a tick that rate-limits still
        // moves forward.
        self.scan_after = if candidates.len() < self.config.batch_size as usize {
            None
        } else {
            candidates.last().map(|candidate| candidate.sandbox_id)
        };
        report.scanned = u32::try_from(candidates.len()).unwrap_or(u32::MAX);

        // One reflector snapshot per tick. An empty set fails closed like a
        // cold one: a real cluster always has nodes, so empty means the
        // reflector froze, not that every node left.
        let live_nodes = self
            .presence
            .live_node_names()
            .filter(|names| !names.is_empty());
        match &live_nodes {
            Some(names) => self.absent_since.retain(|name, _| !names.contains(name)),
            None => self.absent_since.clear(),
        }
        let mut absent_since = std::mem::take(&mut self.absent_since);
        let mut verdicts: HashMap<String, CachedVerdict> = HashMap::new();
        for candidate in candidates {
            // Checked before the lookup, so a capped tick spends no EC2 calls
            // on candidates it can't act on.
            if report.marked_lost >= self.config.max_lost_per_tick {
                report.rate_limited += 1;
                continue;
            }
            let verdict = self
                .source_verdict(
                    &candidate,
                    live_nodes.as_ref(),
                    &mut absent_since,
                    &mut verdicts,
                )
                .await;
            let (outcome, reason) = match verdict {
                CachedVerdict::Gone => match self.sink.mark_lost(&candidate).await {
                    Ok(true) => {
                        report.marked_lost += 1;
                        OutcomeEvent::lost_started_sandbox(
                            LostStartedOutcome::GaveUpSourceNodeGone,
                        )
                        .reason(candidate.source_node.gone_reason())
                        .sandbox_id(candidate.sandbox_id)
                        .generation(candidate.generation)
                        .node(candidate.source_node_id.to_string())
                        .warn();
                        continue;
                    }
                    Ok(false) => {
                        report.cas_lost += 1;
                        (LostStartedOutcome::CasLost, None)
                    }
                    Err(err) => {
                        report.errors += 1;
                        OutcomeEvent::lost_started_sandbox(LostStartedOutcome::Error)
                            .sandbox_id(candidate.sandbox_id)
                            .generation(candidate.generation)
                            .error(&err)
                            .warn();
                        continue;
                    }
                },
                CachedVerdict::Live => {
                    report.source_live += 1;
                    (LostStartedOutcome::Skipped, Some("source_node_live"))
                }
                CachedVerdict::Unverifiable => {
                    report.unverifiable += 1;
                    (
                        LostStartedOutcome::Deferred,
                        Some("source_node_unverifiable"),
                    )
                }
                CachedVerdict::AbsenceSettling => {
                    report.unverifiable += 1;
                    (LostStartedOutcome::Deferred, Some("kube_absence_settling"))
                }
                CachedVerdict::MembershipUnknown => {
                    report.unverifiable += 1;
                    (
                        LostStartedOutcome::Deferred,
                        Some("kube_membership_unknown"),
                    )
                }
                CachedVerdict::InstanceLookupFailed => {
                    report.lookup_errors += 1;
                    (LostStartedOutcome::Deferred, Some("instance_lookup_failed"))
                }
                CachedVerdict::NodeLookupFailed => {
                    report.lookup_errors += 1;
                    (
                        LostStartedOutcome::Deferred,
                        Some("kube_node_lookup_failed"),
                    )
                }
            };
            let mut event = OutcomeEvent::lost_started_sandbox(outcome)
                .sandbox_id(candidate.sandbox_id)
                .generation(candidate.generation)
                .node(candidate.source_node_id.to_string());
            if let Some(reason) = reason {
                event = event.reason(reason);
            }
            event.debug();
        }

        self.absent_since = absent_since;
        metrics::scanned(Loop::LostStartedSandboxes, u64::from(report.scanned));
        report
    }

    /// Whether the candidate's source node is gone. A registered node is gone
    /// only when the reflector, EC2 and a direct Node GET all agree; the
    /// first one that says otherwise decides, so a node the reflector still
    /// lists costs no EC2 call.
    async fn source_verdict(
        &self,
        candidate: &LostCandidate,
        live_nodes: Option<&HashSet<String>>,
        absent_since: &mut HashMap<String, tokio::time::Instant>,
        verdicts: &mut HashMap<String, CachedVerdict>,
    ) -> CachedVerdict {
        let k8s_node_name = match &candidate.source_node {
            SourceNode::RowDeleted => return CachedVerdict::Gone,
            SourceNode::Unverifiable => return CachedVerdict::Unverifiable,
            SourceNode::Registered { k8s_node_name, .. } => k8s_node_name,
        };
        if let Some(verdict) = verdicts.get(k8s_node_name) {
            return *verdict;
        }
        let verdict = self
            .registered_node_verdict(k8s_node_name, live_nodes, absent_since)
            .await;
        verdicts.insert(k8s_node_name.clone(), verdict);
        verdict
    }

    async fn registered_node_verdict(
        &self,
        k8s_node_name: &str,
        live_nodes: Option<&HashSet<String>>,
        absent_since: &mut HashMap<String, tokio::time::Instant>,
    ) -> CachedVerdict {
        let Some(live_nodes) = live_nodes else {
            return CachedVerdict::MembershipUnknown;
        };
        if live_nodes.contains(k8s_node_name) {
            return CachedVerdict::Live;
        }
        let first_absent = *absent_since
            .entry(k8s_node_name.to_string())
            .or_insert_with(tokio::time::Instant::now);
        if first_absent.elapsed() < self.config.absence_grace {
            return CachedVerdict::AbsenceSettling;
        }
        // Bounded like the direct GET below, so one slow EC2 call can't hold
        // the whole tick.
        match timeout(
            DIRECT_NODE_LOOKUP_TIMEOUT,
            self.lookup.lookup_by_private_dns(k8s_node_name.to_string()),
        )
        .await
        {
            Ok(Ok(InstanceLookupOutcome::Gone)) => {}
            Ok(Ok(InstanceLookupOutcome::Live { .. })) => return CachedVerdict::Live,
            Ok(Err(err)) => {
                tracing::warn!(
                    k8s_node_name,
                    error = %err,
                    "lost_started_sandboxes: ec2 lookup failed; deferring its sandboxes"
                );
                return CachedVerdict::InstanceLookupFailed;
            }
            Err(_) => {
                tracing::warn!(
                    k8s_node_name,
                    timeout_secs = DIRECT_NODE_LOOKUP_TIMEOUT.as_secs(),
                    "lost_started_sandboxes: ec2 lookup timed out; deferring its sandboxes"
                );
                return CachedVerdict::InstanceLookupFailed;
            }
        }
        // The reflector can lag; only the API server's NotFound confirms it.
        match timeout(
            DIRECT_NODE_LOOKUP_TIMEOUT,
            self.presence.node_exists(k8s_node_name.to_string()),
        )
        .await
        {
            Ok(Ok(false)) => CachedVerdict::Gone,
            Ok(Ok(true)) => CachedVerdict::Live,
            Ok(Err(err)) => {
                tracing::warn!(
                    k8s_node_name,
                    error = %err,
                    "lost_started_sandboxes: direct node lookup failed; deferring its sandboxes"
                );
                CachedVerdict::NodeLookupFailed
            }
            Err(_) => {
                tracing::warn!(
                    k8s_node_name,
                    timeout_secs = DIRECT_NODE_LOOKUP_TIMEOUT.as_secs(),
                    "lost_started_sandboxes: direct node lookup timed out; deferring its sandboxes"
                );
                CachedVerdict::NodeLookupFailed
            }
        }
    }
}

/// Drive the loop until `shutdown_token` fires. A disabled config waits on
/// the token without ticking.
pub async fn run<S: LostStartedSink, L: InstanceLookup, P: KubeNodePresence>(
    config: ReconcilerConfig,
    sink: S,
    lookup: L,
    presence: P,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "lost_started_sandboxes disabled \
             (SPACE_LOST_STARTED_SANDBOXES_ENABLED unset); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        batch_size = config.batch_size,
        max_lost_per_tick = config.max_lost_per_tick,
        "starting lost_started_sandboxes"
    );
    let tick = config.tick;
    let mut reconciler = Reconciler::new(config, sink, lookup, presence);
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so node sync settles after a restart.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                // A tick of slow lookups can outlast the leader lease. Stop
                // mid-tick on cancellation. Dropping the tick drops any open
                // flip transaction, which rolls back; a flip whose commit was
                // already sent may still land, without its events.
                let report = tokio::select! {
                    report = reconciler.tick() => report,
                    _ = shutdown_token.cancelled() => {
                        tracing::info!("lost_started_sandboxes stopping mid-tick");
                        return;
                    }
                };
                tracing::info!(
                    scanned = report.scanned,
                    marked_lost = report.marked_lost,
                    source_live = report.source_live,
                    unverifiable = report.unverifiable,
                    lookup_errors = report.lookup_errors,
                    cas_lost = report.cas_lost,
                    rate_limited = report.rate_limited,
                    errors = report.errors,
                    "lost_started_sandboxes tick complete"
                );
                metrics::loop_latency(Loop::LostStartedSandboxes, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("lost_started_sandboxes stopping");
                return;
            }
        }
    }
}
