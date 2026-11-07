//! The dead-node verb: one loop, independent of drain phase, that decides a
//! node is lost and what reconciling every sandbox row it holds would do.
//!
//! ## Why this exists
//!
//! When a node's machine dies outside a drain, its `nodes` row and the
//! sandbox rows pinned to it stay as they were: wakes of those sandboxes fail,
//! local-only suspended sandboxes look recoverable, and loops keep working
//! against the node. Several loops each decide "gone" their own way. This one
//! decides it once ([`verdict::verdict`]) and applies one rule to every row
//! ([`outcome::row_outcome`]).
//!
//! ## Dry run only
//!
//! `SPACE_DEAD_NODE_MODE` selects `off` (the default) or `dry-run`; there is
//! no write mode yet. With `off` the reconciler set does not start the loop,
//! reads no terminator kind and builds no client for it. In `dry-run` each
//! tick judges up to [`MAX_NODES_PER_TICK`] nodes and reads up to
//! [`MAX_ROWS_PER_NODE`] sandbox rows of each, so at most 4,000 rows a tick.
//! It logs one line per node, counts each verdict and outcome in
//! `dead_node.*` metrics, and keeps the node's result in `dead_node_dry_runs`
//! (`pplx_space_sandbox_store::dead_node`) for the admin API. That report is
//! the only write: no sandbox, node, snapshot or execution row and no
//! `space.sandbox` event.
//!
//! Rows are read and their outcomes counted whatever the verdict: the report
//! says what reconciling them would do, and the verdict whether it would
//! happen. Only a `lost` node's rows would be reconciled.
//!
//! ## Which nodes
//!
//! Nodes of the running substrate outside any drain phase whose heartbeat is
//! stale by the API's node-status rule
//! (`pplx_space_sandbox_residency::node_heartbeat_fresh`), walked in id order
//! with a cursor so a backlog larger than the cap is reached across ticks.
//!
//! ## The verdict
//!
//! Unknown unless all hold, and any error reads unknown:
//!
//! - the heartbeat is stale;
//! - the reflector has not listed a Node with the recorded uid for the whole
//!   absence window (ten minutes, the gone-node queue GC's), sampled once a
//!   tick and restarted when the recorded uid changes, and a direct GET of
//!   the Node's name returns 404, so a same-name replacement reads unknown;
//! - the instance authority agrees. With the `aws` terminator, EC2 lists no
//!   live instance for the node's private DNS name while listing a reference
//!   node, a fresh node whose Node the reflector lists, `running` in the same
//!   kind of call; an empty answer from the wrong region, account or filter
//!   then reads unknown. With `kube-delete`, which Kind uses, the Node's
//!   absence is the machine's removal. With `noop`, a terminator kind that
//!   does not parse, or on the Direct substrate, which has no Kubernetes
//!   Node, every node reads unknown.
//!   Kind's fake EC2 answers from the Kubernetes Node itself (a Ready Node is
//!   `running`, a missing one has no instance), so it adds no evidence there;
//!   the smoke-only `SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY=ec2` test hook
//!   runs the EC2 arm against it to prove the lookup's wiring.

use pplx_space_common_core::types::{NodeId, SubstrateKind};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::cluster_reconciler::Mode as MetricMode;
use pplx_space_sandbox_store::dead_node::{DeadNodeReportWrite, DeadNodeRow, DeadNodeSample};
use pplx_space_sandbox_store::lost::DEAD_UPLOAD_SETTLE;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::env_config;
use crate::orphaned_nodes::aws::{DescribeInstancesClient, MatchedInstance};
use crate::reconcile_queue_gc::detector::{DEFAULT_ABSENCE_GRACE, DIRECT_NODE_LOOKUP_TIMEOUT};

#[cfg(feature = "kubernetes")]
pub mod kube;
pub mod outcome;
pub mod store;
pub mod verdict;

#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use kube::KubeNodeMembership;
pub use store::DeadNodeDbStore;

use outcome::{RowOutcome, row_outcome};
use verdict::{
    DirectGet, Heartbeat, InstanceEvidence, InstanceLookup, KubeEvidence, ReferenceLookup, Verdict,
    VerdictInputs,
};

pub(crate) const MODE_ENV: &str = "SPACE_DEAD_NODE_MODE";
pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
/// Nodes judged per tick.
pub(crate) const MAX_NODES_PER_TICK: u32 = 8;
/// Sandbox rows read per judged node; a node holding more reports
/// `rows_truncated`.
pub(crate) const MAX_ROWS_PER_NODE: u32 = 500;
/// Rows of each outcome the report lists.
pub(crate) const SAMPLES_PER_OUTCOME: usize = 10;
/// Fresh nodes read as reference candidates for the instance lookup.
const REFERENCE_CANDIDATES: u32 = 5;

/// `SPACE_DEAD_NODE_MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Off,
    DryRun,
}

impl Mode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::DryRun => "dry-run",
        }
    }

    /// Unset or empty is `off`. Any value other than `off` and `dry-run` is
    /// `off` with a warning: a value meant for a later mode must not run
    /// this one.
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            None | Some("" | "off") => Self::Off,
            Some("dry-run") => Self::DryRun,
            Some(other) => {
                tracing::warn!(
                    variable = MODE_ENV,
                    value = other,
                    "unrecognized dead-node mode (expected off or dry-run); keeping it off"
                );
                Self::Off
            }
        }
    }
}

/// The loop's config in `dry-run`; `off` has none, so it starts nothing.
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub tick: Duration,
    /// How long the recorded Node must stay absent from the reflector.
    pub absence_window: Duration,
    /// How long a full upload from the node must have been quiet before it
    /// no longer makes a row wait: the existing give-ups' settle window.
    pub upload_settle: Duration,
    pub max_nodes_per_tick: u32,
    pub max_rows_per_node: u32,
    /// Judge with EC2 whatever the terminator. Only the smoke harness sets it,
    /// through a test hook, so Kind can run the EC2 arm against its fake EC2.
    pub force_ec2: bool,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            absence_window: DEFAULT_ABSENCE_GRACE,
            upload_settle: DEAD_UPLOAD_SETTLE,
            max_nodes_per_tick: MAX_NODES_PER_TICK,
            max_rows_per_node: MAX_ROWS_PER_NODE,
            force_ec2: false,
        }
    }
}

impl ReconcilerConfig {
    /// `None` when the mode is off.
    pub fn from_env() -> Option<Self> {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Option<Self> {
        match Mode::parse(env.var(MODE_ENV).ok().as_deref()) {
            Mode::Off => return None,
            Mode::DryRun => {}
        }
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Some(Self {
            tick: config.test_duration_secs("SPACE_DEAD_NODE_TEST_TICK_SECS", default.tick),
            absence_window: config.test_duration_secs(
                "SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS",
                default.absence_window,
            ),
            upload_settle: config.test_duration_secs(
                "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS",
                default.upload_settle,
            ),
            force_ec2: config
                .test_raw("SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY")
                .is_some_and(|raw| raw.trim() == "ec2"),
            ..default
        })
    }
}

/// A node the verb judges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleNode {
    pub node_id: NodeId,
    pub managed_by: SubstrateKind,
    /// `node_k8s.k8s_node_name`, the EC2 private DNS name on EKS.
    pub k8s_node_name: Option<String>,
    /// `node_k8s.k8s_node_uid`: the Node the row recorded.
    pub k8s_node_uid: Option<String>,
}

/// A fresh node that may serve as the instance lookup's reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceNode {
    pub k8s_node_name: String,
    pub k8s_node_uid: String,
}

/// Postgres-side surface. Production impl: [`DeadNodeDbStore`].
pub trait DeadNodeSink: Send + Sync + 'static {
    /// Up to `limit` nodes of `managed_by` with `nodes.id > after`, by id,
    /// outside any drain phase, whose heartbeat is stale.
    fn stale_nodes(
        &self,
        managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<Vec<StaleNode>, sqlx::Error>> + Send;

    /// Up to `limit` fresh nodes of `managed_by` with a recorded Node.
    fn reference_nodes(
        &self,
        managed_by: SubstrateKind,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<Vec<ReferenceNode>, sqlx::Error>> + Send;

    /// Up to `limit` sandbox rows `node` holds, with their facts.
    fn rows_on_node(
        &self,
        node: &StaleNode,
        upload_settle: Duration,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<Vec<DeadNodeRow>, sqlx::Error>> + Send;

    fn write_report(
        &self,
        report: &DeadNodeReportWrite<'_>,
    ) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send;
}

/// Kubernetes Node membership, keyed by uid. Production impl:
/// [`KubeNodeMembership`].
pub trait NodeMembership: Send + Sync + 'static {
    type LookupError: std::error::Error + Send + Sync + 'static;

    /// The uids of every Node the reflector lists, or `None` before its first
    /// list completes.
    fn live_node_uids(&self) -> Option<HashSet<String>>;

    /// Whether the API server has a Node named `node_name`, whatever its uid.
    fn node_exists(
        &self,
        node_name: String,
    ) -> impl std::future::Future<Output = Result<bool, Self::LookupError>> + Send;
}

/// Membership for a substrate without Kubernetes Nodes: always unknown.
pub struct NoNodeMembership;

impl NodeMembership for NoNodeMembership {
    type LookupError = std::convert::Infallible;

    fn live_node_uids(&self) -> Option<HashSet<String>> {
        None
    }

    async fn node_exists(&self, _node_name: String) -> Result<bool, Self::LookupError> {
        Ok(true)
    }
}

/// Which [`InstanceAuthority`] the verb judges with, decided before any
/// client is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityKind {
    None,
    KubeDelete,
    Ec2,
}

/// Who can say a node's machine is gone, from the cluster's terminator.
#[derive(Clone)]
pub enum InstanceAuthority {
    None,
    KubeDelete,
    Ec2(Arc<dyn DescribeInstancesClient>),
}

impl InstanceAuthority {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::KubeDelete => "kube-delete",
            Self::Ec2(_) => "ec2",
        }
    }
}

/// Where a tick's database work failed: the `stage` tag of `dead_node.errors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorStage {
    /// The stale-node page.
    Scan,
    /// A node's sandbox rows.
    Rows,
    /// A node's report write.
    Report,
}

impl ErrorStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Rows => "rows",
            Self::Report => "report",
        }
    }
}

fn count_error(report: &mut TickReport, stage: ErrorStage) {
    report.errors += 1;
    crate::metrics::dead_node::error(MetricMode::DryRun, stage.as_str());
}

/// One judged node, as the tick logged and stored it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeReport {
    pub node_id: NodeId,
    pub verdict: Verdict,
    pub rows_truncated: bool,
    pub counts: BTreeMap<RowOutcome, u64>,
    pub samples: BTreeMap<RowOutcome, Vec<DeadNodeSample>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub nodes: Vec<NodeReport>,
    /// Database reads or report writes that failed; the node is judged again
    /// next sweep.
    pub errors: u32,
}

pub struct Reconciler<S: DeadNodeSink, K: NodeMembership> {
    config: ReconcilerConfig,
    sink: S,
    membership: K,
    authority: InstanceAuthority,
    managed_by: SubstrateKind,
    scan_after: Option<NodeId>,
    /// When each node's recorded uid was first seen absent from the
    /// reflector, with that uid. Dropped once the reflector lists the uid
    /// again, restarted when the recorded uid changes, and cleared entirely
    /// on a tick whose membership is unknown.
    absent_since: HashMap<NodeId, (String, Instant)>,
}

impl<S: DeadNodeSink, K: NodeMembership> Reconciler<S, K> {
    pub fn new(
        config: ReconcilerConfig,
        sink: S,
        membership: K,
        authority: InstanceAuthority,
        managed_by: SubstrateKind,
    ) -> Self {
        Self {
            config,
            sink,
            membership,
            authority,
            managed_by,
            scan_after: None,
            absent_since: HashMap::new(),
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let nodes = match self
            .sink
            .stale_nodes(
                self.managed_by,
                self.scan_after,
                self.config.max_nodes_per_tick,
            )
            .await
        {
            Ok(nodes) => nodes,
            Err(err) => {
                tracing::warn!(error = %err, "dead_node: stale node scan failed");
                count_error(&mut report, ErrorStage::Scan);
                return report;
            }
        };
        // A short page ends the sweep; the next tick starts again from the
        // beginning.
        self.scan_after = if nodes.len() < self.config.max_nodes_per_tick as usize {
            None
        } else {
            nodes.last().map(|node| node.node_id)
        };

        // One reflector sample per tick. Empty fails closed like cold: a
        // cluster always has the controller's own node.
        let live_uids = self
            .membership
            .live_node_uids()
            .filter(|uids| !uids.is_empty());
        match &live_uids {
            Some(uids) => self.absent_since.retain(|_, (uid, _)| !uids.contains(uid)),
            None => self.absent_since.clear(),
        }

        let mut reference: Option<ReferenceLookup> = None;
        for node in nodes {
            let kube = self.kube_evidence(&node, live_uids.as_ref()).await;
            let instance = match &self.authority {
                InstanceAuthority::None => InstanceEvidence::NoAuthority,
                InstanceAuthority::KubeDelete => InstanceEvidence::KubeDelete,
                InstanceAuthority::Ec2(client) => {
                    let node_lookup = instance_lookup(client.as_ref(), &node).await;
                    let reference = match reference {
                        Some(reference) => reference,
                        None => {
                            let looked_up = self
                                .reference_lookup(client.as_ref(), live_uids.as_ref())
                                .await;
                            reference = Some(looked_up);
                            looked_up
                        }
                    };
                    InstanceEvidence::Ec2 {
                        node: node_lookup,
                        reference,
                    }
                }
            };
            let verdict = verdict::verdict(&VerdictInputs {
                heartbeat: Heartbeat::Stale,
                kube,
                absence_window: self.config.absence_window,
                instance,
            });
            match self.judge_rows(&node, verdict).await {
                Ok(node_report) => report.nodes.push(node_report),
                Err((stage, err)) => {
                    tracing::warn!(
                        node_id = %node.node_id,
                        verdict = %verdict,
                        reason = verdict.reason(),
                        stage = stage.as_str(),
                        error = %err,
                        "dead_node: dry run for node failed; judging it again next sweep"
                    );
                    count_error(&mut report, stage);
                }
            }
        }
        report
    }

    async fn kube_evidence(
        &mut self,
        node: &StaleNode,
        live_uids: Option<&HashSet<String>>,
    ) -> KubeEvidence {
        let (Some(name), Some(uid)) = (&node.k8s_node_name, &node.k8s_node_uid) else {
            return KubeEvidence::NoRecordedIdentity;
        };
        let Some(live_uids) = live_uids else {
            return KubeEvidence::MembershipUnknown;
        };
        if live_uids.contains(uid) {
            self.absent_since.remove(&node.node_id);
            return KubeEvidence::Present;
        }
        let now = Instant::now();
        let entry = self
            .absent_since
            .entry(node.node_id)
            .or_insert_with(|| (uid.clone(), now));
        if entry.0 != *uid {
            *entry = (uid.clone(), now);
        }
        let absent_for = now.saturating_duration_since(entry.1);
        // The reflector can lag; only the API server's 404 confirms it.
        let direct_get = match timeout(
            DIRECT_NODE_LOOKUP_TIMEOUT,
            self.membership.node_exists(name.clone()),
        )
        .await
        {
            Ok(Ok(false)) => DirectGet::NotFound,
            Ok(Ok(true)) => DirectGet::Found,
            Ok(Err(err)) => {
                tracing::warn!(k8s_node_name = %name, error = %err, "dead_node: direct node lookup failed");
                DirectGet::Failed
            }
            Err(_) => {
                tracing::warn!(k8s_node_name = %name, "dead_node: direct node lookup timed out");
                DirectGet::Failed
            }
        };
        KubeEvidence::Absent {
            absent_for,
            direct_get,
        }
    }

    async fn reference_lookup(
        &self,
        client: &dyn DescribeInstancesClient,
        live_uids: Option<&HashSet<String>>,
    ) -> ReferenceLookup {
        let Some(live_uids) = live_uids else {
            return ReferenceLookup::Unavailable;
        };
        let candidates = match self
            .sink
            .reference_nodes(self.managed_by, REFERENCE_CANDIDATES)
            .await
        {
            Ok(candidates) => candidates,
            Err(err) => {
                tracing::warn!(error = %err, "dead_node: reference node read failed");
                return ReferenceLookup::Failed;
            }
        };
        let Some(reference) = candidates
            .into_iter()
            .find(|candidate| live_uids.contains(&candidate.k8s_node_uid))
        else {
            return ReferenceLookup::Unavailable;
        };
        match describe(client, &reference.k8s_node_name).await {
            Ok(matches) if matches.iter().any(|m| m.state == "running") => ReferenceLookup::Running,
            Ok(_) => ReferenceLookup::NotRunning,
            Err(()) => ReferenceLookup::Failed,
        }
    }

    /// Read and judge `node`'s rows, log and count them, then write the
    /// report; the telemetry goes out even when the write fails.
    async fn judge_rows(
        &self,
        node: &StaleNode,
        verdict: Verdict,
    ) -> Result<NodeReport, (ErrorStage, sqlx::Error)> {
        let cap = self.config.max_rows_per_node;
        let mut rows = self
            .sink
            .rows_on_node(node, self.config.upload_settle, cap.saturating_add(1))
            .await
            .map_err(|err| (ErrorStage::Rows, err))?;
        let rows_truncated = rows.len() > cap as usize;
        rows.truncate(cap as usize);
        let mut counts: BTreeMap<RowOutcome, u64> = BTreeMap::new();
        let mut samples: BTreeMap<RowOutcome, Vec<DeadNodeSample>> = BTreeMap::new();
        for row in &rows {
            let outcome = row_outcome(row);
            *counts.entry(outcome).or_default() += 1;
            let listed = samples.entry(outcome).or_default();
            if listed.len() < SAMPLES_PER_OUTCOME {
                listed.push(DeadNodeSample {
                    sandbox_id: row.sandbox_id,
                    lifecycle: row.stored_lifecycle.clone(),
                });
            }
        }
        let summary = counts
            .iter()
            .map(|(outcome, count)| format!("{outcome}={count}"))
            .collect::<Vec<_>>()
            .join(" ");
        tracing::info!(
            node_id = %node.node_id,
            k8s_node_name = node.k8s_node_name.as_deref().unwrap_or(""),
            mode = Mode::DryRun.as_str(),
            verdict = %verdict,
            reason = verdict.reason(),
            rows = rows.len(),
            rows_truncated,
            outcomes = %summary,
            "dead_node: dry run"
        );
        crate::metrics::dead_node::node_judged(
            MetricMode::DryRun,
            verdict.as_str(),
            verdict.reason(),
        );
        for (outcome, count) in &counts {
            crate::metrics::dead_node::rows(
                MetricMode::DryRun,
                verdict.as_str(),
                outcome.as_str(),
                *count,
            );
        }
        self.sink
            .write_report(&DeadNodeReportWrite {
                node_id: node.node_id,
                verdict: verdict.as_str(),
                reason: verdict.reason(),
                rows_truncated,
                counts: counts
                    .iter()
                    .map(|(outcome, count)| (outcome.as_str(), *count))
                    .collect(),
                samples: samples
                    .iter()
                    .map(|(outcome, rows)| (outcome.as_str(), rows.clone()))
                    .collect(),
            })
            .await
            .map_err(|err| (ErrorStage::Report, err))?;
        Ok(NodeReport {
            node_id: node.node_id,
            verdict,
            rows_truncated,
            counts,
            samples,
        })
    }
}

/// EC2's answer for the node. Any match not `terminated` is live: the
/// `orphaned_nodes` rule.
async fn instance_lookup(client: &dyn DescribeInstancesClient, node: &StaleNode) -> InstanceLookup {
    let Some(name) = &node.k8s_node_name else {
        return InstanceLookup::Failed;
    };
    match describe(client, name).await {
        Ok(matches) if matches.iter().any(MatchedInstance::is_live) => InstanceLookup::Live,
        Ok(_) => InstanceLookup::NoLiveMatch,
        Err(()) => InstanceLookup::Failed,
    }
}

async fn describe(
    client: &dyn DescribeInstancesClient,
    private_dns_name: &str,
) -> Result<Vec<MatchedInstance>, ()> {
    match timeout(
        DIRECT_NODE_LOOKUP_TIMEOUT,
        client.describe_instances_by_private_dns(private_dns_name.to_owned()),
    )
    .await
    {
        Ok(Ok(matches)) => Ok(matches),
        Ok(Err(err)) => {
            tracing::warn!(private_dns_name, error = %err, "dead_node: ec2 lookup failed");
            Err(())
        }
        Err(_) => {
            tracing::warn!(private_dns_name, "dead_node: ec2 lookup timed out");
            Err(())
        }
    }
}

/// Drive the loop until `shutdown_token` fires. The reconciler set calls it
/// only in `dry-run`: `off` has no [`ReconcilerConfig`].
pub async fn run<S: DeadNodeSink, K: NodeMembership>(
    config: ReconcilerConfig,
    sink: S,
    membership: K,
    authority: InstanceAuthority,
    managed_by: SubstrateKind,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        mode = Mode::DryRun.as_str(),
        authority = authority.as_str(),
        tick_secs = config.tick.as_secs(),
        absence_window_secs = config.absence_window.as_secs(),
        upload_settle_secs = config.upload_settle.as_secs(),
        max_nodes_per_tick = config.max_nodes_per_tick,
        max_rows_per_node = config.max_rows_per_node,
        "starting dead-node verb"
    );
    let tick = config.tick;
    let mut reconciler = Reconciler::new(config, sink, membership, authority, managed_by);
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                tokio::select! {
                    _ = reconciler.tick() => {}
                    _ = shutdown_token.cancelled() => break,
                }
            }
            _ = shutdown_token.cancelled() => break,
        }
    }
    tracing::info!("dead-node verb stopping");
}
