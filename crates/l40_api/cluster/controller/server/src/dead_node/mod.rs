//! The dead-node verb: one loop, independent of drain phase, that decides
//! which machines a node's sandbox rows ran on are dead, what reconciling each
//! row those machines held does, and in live mode does it.
//!
//! ## Why this exists
//!
//! When a node's machine dies outside a drain, its `nodes` row and the
//! sandbox rows pinned to it stay as they were: wakes of those sandboxes fail,
//! local-only suspended sandboxes look recoverable, and loops keep working
//! against the node. A same-name replacement reuses the node id and its
//! Kubernetes name, so the dead machine's rows look like the replacement's.
//! Several loops each decide "gone" their own way. This one decides it once
//! per exact instance ([`verdict::instance_verdict`]) and per node
//! ([`verdict::node_verdict`]), and applies one rule to every row
//! ([`outcome::row_outcome`]).
//!
//! ## Modes
//!
//! `SPACE_DEAD_NODE_MODE` selects `off` (the default), `dry-run` or `live`.
//! With `off` the reconciler set does not start the loop, reads no terminator
//! kind and builds no client for it. Each tick judges up to
//! [`MAX_NODES_PER_TICK`] nodes and reads up to [`MAX_ROWS_PER_NODE`] sandbox
//! rows of each. It logs one line per node, counts each verdict and outcome in
//! `dead_node.*` metrics, and keeps the node's result in
//! `nodes.dead_node_status` (`pplx_space_sandbox_store::dead_node`) for the
//! admin API. In `dry-run` that status is the only write. In `live` it also
//! applies each selected row's outcome, up to [`MAX_WRITES_PER_TICK`] rows a
//! tick, each in its own transaction that re-reads the row under its lock and
//! writes only if the outcome and the source execution are unchanged; then,
//! for a lost node, it deletes the node row, guarded by its Kubernetes uid and
//! recorded instance, with no tombstone. Where `live` runs, the
//! `lost_started_sandboxes` and `orphaned_nodes` loops do not, and
//! `orphaned_sandboxes` leaves rows on nodes with a stale heartbeat or a lost
//! verdict to it.
//!
//! ## Which nodes
//!
//! Nodes of the running substrate outside any drain phase, or a v2 drain's
//! node in any phase (judged by the same evidence: the drain's v2 needs this
//! verb `live` to delete the row a terminate leaves, and to reconcile a node
//! that died mid-drain, which nothing else does), whose heartbeat is
//! stale by the API's node-status rule
//! (`pplx_space_sandbox_residency::node_heartbeat_fresh`), or which hold a
//! non-terminal row whose latest execution on the node is stamped with an
//! instance other than the node's recorded one (a same-name replacement's
//! predecessor), walked in id order with a cursor so a backlog larger than
//! the cap is reached across ticks.
//!
//! ## The verdicts
//!
//! An instance is asked about by its exact ID: the node's recorded
//! `node_k8s.instance_id` while its heartbeat is stale, and every distinct
//! execution stamp on its rows other than the recorded instance of a node
//! whose fresh heartbeat comes from that instance. With the `aws` terminator
//! one `DescribeInstances` call with an `instance-id` filter asks about them
//! together with a reference: the recorded instance of a fresh node whose
//! Node the reflector lists. An instance is dead when EC2 lists it
//! `terminated` or not at all while listing the reference in a live state;
//! any error, a missing or terminated reference, or a malformed ID reads
//! unknown. With `kube-delete` or `noop`, and on the Direct substrate, there
//! is no instance authority and every instance reads unknown.
//!
//! The node is lost when its heartbeat is stale, the reflector has not listed
//! a Node with the recorded uid for the whole absence window (ten minutes,
//! the gone-node queue GC's; sampled once a tick and restarted when the
//! recorded uid changes), a direct GET of the Node's name returns 404, and
//! its recorded instance is dead.
//!
//! ## Which rows
//!
//! A row whose latest execution on the node is stamped with an instance is
//! selected when that instance is dead. A row with no stamped execution there
//! is selected only when the node is lost. [`outcome::row_outcome`] then
//! decides, and never writes off a row a live node's fresh, complete census
//! reports, or one a drain restore operation owns.
//!
//! Kind's fake EC2 answers instance IDs from the Kubernetes Nodes' fake AWS
//! `providerID`s and the instances terminated through it; a scenario selects
//! the `aws` terminator there to judge by it.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::cluster_reconciler::Mode as MetricMode;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use pplx_space_sandbox_store::dead_node::{DeadNodeRow, DeadNodeSample, DeadNodeStatusWrite};
use pplx_space_sandbox_store::lost::DEAD_UPLOAD_SETTLE;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::env_config;
use crate::gone_node_write_off::emit_lost_and_failed;
use crate::orphaned_nodes::aws::MatchedInstance;
use crate::reconcile_queue_gc::detector::{DEFAULT_ABSENCE_GRACE, DIRECT_NODE_LOOKUP_TIMEOUT};

#[cfg(feature = "kubernetes")]
pub mod kube;
pub mod outcome;
pub mod store;
pub mod verdict;

#[cfg(test)]
mod row_seed_tests;
#[cfg(test)]
mod tests;

#[cfg(feature = "kubernetes")]
pub use kube::KubeNodeMembership;
pub use store::DeadNodeDbStore;

use outcome::{RowOutcome, row_outcome};
use verdict::{
    DirectGet, Heartbeat, InstanceEvidence, InstanceVerdict, KubeEvidence, NodeVerdictInputs,
    Verdict, instance_verdict, node_verdict, well_formed_instance_id,
};

pub(crate) const MODE_ENV: &str = "SPACE_DEAD_NODE_MODE";
pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
/// Nodes judged per tick.
pub(crate) const MAX_NODES_PER_TICK: u32 = 8;
/// Sandbox rows read per judged node; a node holding more reports
/// `rows_truncated`, and live mode does not delete it that tick.
pub(crate) const MAX_ROWS_PER_NODE: u32 = 500;
/// Rows live mode writes per tick, across nodes; the rest wait for the next.
pub(crate) const MAX_WRITES_PER_TICK: u32 = 200;
/// Rows of each outcome the report lists.
pub(crate) const SAMPLES_PER_OUTCOME: usize = 10;
/// Fresh nodes read as reference candidates for the instance lookup.
const REFERENCE_CANDIDATES: u32 = 5;
/// The `operation` the failed event of a row written off as lost names.
const OPERATION: &str = "dead_node";

/// `SPACE_DEAD_NODE_MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Off,
    DryRun,
    Live,
}

impl Mode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::DryRun => "dry-run",
            Self::Live => "live",
        }
    }

    const fn metric(self) -> MetricMode {
        match self {
            Self::Live => MetricMode::Live,
            Self::Off | Self::DryRun => MetricMode::DryRun,
        }
    }

    /// Unset or empty is `off`. Any value other than `off`, `dry-run` and
    /// `live` is `off` with a warning: a value meant for a later mode must not
    /// run this one.
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            None | Some("" | "off") => Self::Off,
            Some("dry-run") => Self::DryRun,
            Some("live") => Self::Live,
            Some(other) => {
                tracing::warn!(
                    variable = MODE_ENV,
                    value = other,
                    "unrecognized dead-node mode (expected off, dry-run or live); keeping it off"
                );
                Self::Off
            }
        }
    }
}

/// The loop's config in `dry-run` or `live`; `off` has none, so it starts
/// nothing.
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// `dry-run` or `live`, never `off`.
    pub mode: Mode,
    pub tick: Duration,
    /// How long the recorded Node must stay absent from the reflector.
    pub absence_window: Duration,
    /// How long a full upload from the node must have been quiet before it
    /// no longer makes a row wait: the existing give-ups' settle window.
    pub upload_settle: Duration,
    pub max_nodes_per_tick: u32,
    pub max_rows_per_node: u32,
    pub max_writes_per_tick: u32,
}

impl ReconcilerConfig {
    fn with_mode(mode: Mode) -> Self {
        Self {
            mode,
            tick: DEFAULT_TICK,
            absence_window: DEFAULT_ABSENCE_GRACE,
            upload_settle: DEAD_UPLOAD_SETTLE,
            max_nodes_per_tick: MAX_NODES_PER_TICK,
            max_rows_per_node: MAX_ROWS_PER_NODE,
            max_writes_per_tick: MAX_WRITES_PER_TICK,
        }
    }

    /// `None` when the mode is off.
    pub fn from_env() -> Option<Self> {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Option<Self> {
        let mode = Mode::parse(env.var(MODE_ENV).ok().as_deref());
        if mode == Mode::Off {
            return None;
        }
        let config = env_config::EnvConfig(env);
        let default = Self::with_mode(mode);
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
            ..default
        })
    }

    pub const fn is_live(&self) -> bool {
        matches!(self.mode, Mode::Live)
    }
}

/// A node the verb judges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateNode {
    pub node_id: NodeId,
    pub managed_by: SubstrateKind,
    /// `node_k8s.k8s_node_name`, the EC2 private DNS name on EKS.
    pub k8s_node_name: Option<String>,
    /// `node_k8s.k8s_node_uid`: the Node the row recorded.
    pub k8s_node_uid: Option<String>,
    /// `node_k8s.instance_id`: the instance the node's last registration
    /// reported.
    pub instance_id: Option<String>,
    pub heartbeat: Heartbeat,
    /// A v2 drain's node, in the phase the tick read: once EC2 says it is
    /// gone, its seal protects nothing, and this verb reconciles its rows
    /// and deletes its row like any lost node's, whether the drain is
    /// `draining`, `drained` or `terminating`. `None` for a node outside any
    /// drain.
    pub v2_drain_phase: Option<String>,
}

/// A fresh node that may serve as the instance lookup's reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceNode {
    pub k8s_node_uid: String,
    pub instance_id: String,
}

/// What a row's write rests on, which the write re-checks under the node's
/// locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBasis {
    /// The row's source execution names a dead instance.
    InstanceDead,
    /// The row has no stamped execution on the node, and the node is lost:
    /// its heartbeat must still be stale.
    NodeLost,
}

/// What one row's write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    Applied,
    /// The node's drain phase, uid or recorded instance moved, or, for
    /// [`WriteBasis::NodeLost`], its heartbeat came back.
    NodeChanged,
    /// The row's outcome or source execution changed, or the row is gone.
    RowChanged,
    /// The guarded writes matched nothing left to change.
    NothingToChange,
}

/// One row's planned write.
pub struct RowWrite<'a> {
    pub sandbox_id: SandboxId,
    pub outcome: RowOutcome,
    pub source_execution: Option<Uuid>,
    pub basis: WriteBasis,
    /// Decides a re-read row's outcome the way the plan did.
    pub decide: &'a (dyn Fn(&DeadNodeRow) -> RowOutcome + Send + Sync),
}

/// Postgres-side surface. Production impl: [`DeadNodeDbStore`].
pub trait DeadNodeSink: Send + Sync + 'static {
    /// Up to `limit` nodes of `managed_by` with `nodes.id > after`, by id,
    /// outside any drain phase, whose heartbeat is stale or which hold a row
    /// stamped with an instance other than their recorded one.
    fn candidate_nodes(
        &self,
        managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<CandidateNode>, sqlx::Error>> + Send;

    /// Up to `limit` fresh nodes of `managed_by` with a recorded Node and
    /// instance.
    fn reference_nodes(
        &self,
        managed_by: SubstrateKind,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<ReferenceNode>, sqlx::Error>> + Send;

    /// Up to `limit` sandbox rows `node` holds, by id after `after`, with
    /// their facts.
    fn rows_on_node(
        &self,
        node: &CandidateNode,
        after: Option<SandboxId>,
        upload_settle: Duration,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<DeadNodeRow>, sqlx::Error>> + Send;

    /// Apply one row's outcome in its own transaction: re-check the node,
    /// lock and re-read the row, and write only if `write.decide` still gives
    /// `write.outcome` with the same source execution.
    fn write_row(
        &self,
        node: &CandidateNode,
        write: &RowWrite<'_>,
        upload_settle: Duration,
    ) -> impl Future<Output = Result<WriteResult, sqlx::Error>> + Send;

    /// Delete the lost node's row, guarded by its drain phase, stale
    /// heartbeat, uid and recorded instance, and by no live row pinning it.
    /// Returns whether it was deleted.
    fn delete_node(
        &self,
        node: &CandidateNode,
    ) -> impl Future<Output = Result<bool, sqlx::Error>> + Send;

    /// Returns whether the node row still existed to take the status.
    fn write_status(
        &self,
        status: &DeadNodeStatusWrite<'_>,
    ) -> impl Future<Output = Result<bool, sqlx::Error>> + Send;
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
    ) -> impl Future<Output = Result<bool, Self::LookupError>> + Send;
}

/// EC2 `DescribeInstances` by an `instance-id` filter. The impl forwards to
/// the AWS client; unit tests substitute a fake.
pub trait InstanceIdLookup: Send + Sync + 'static {
    fn describe_instances_by_ids(
        &self,
        instance_ids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>>;
}

impl InstanceIdLookup for pplx_space_aws_client::ec2::Client {
    fn describe_instances_by_ids(
        &self,
        instance_ids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>> {
        Box::pin(async move {
            let ids: Vec<&str> = instance_ids.iter().map(String::as_str).collect();
            let instances =
                pplx_space_aws_client::ec2::Client::describe_instances_by_ids(self, &ids)
                    .await
                    .map_err(|err| err.to_string())?;
            Ok(instances
                .into_iter()
                .map(|instance| MatchedInstance {
                    instance_id: instance.instance_id,
                    state: instance.state,
                })
                .collect())
        })
    }
}

/// Which [`InstanceAuthority`] the verb judges with, decided before any
/// client is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityKind {
    None,
    Ec2,
}

/// Who can say an instance is gone, from the cluster's terminator.
#[derive(Clone)]
pub enum InstanceAuthority {
    None,
    Ec2(Arc<dyn InstanceIdLookup>),
}

impl InstanceAuthority {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ec2(_) => "ec2",
        }
    }
}

/// Where a tick's database work failed: the `stage` tag of `dead_node.errors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorStage {
    /// The candidate-node page.
    Scan,
    /// A node's sandbox rows.
    Rows,
    /// A row's write.
    Write,
    /// The node row's delete.
    Delete,
    /// A node's status write.
    Report,
}

impl ErrorStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Rows => "rows",
            Self::Write => "write",
            Self::Delete => "delete",
            Self::Report => "report",
        }
    }
}

/// One judged node, as the tick logged and stored it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeReport {
    pub node_id: NodeId,
    pub verdict: Verdict,
    /// Each instance asked about, and its verdict.
    pub instances: BTreeMap<String, InstanceVerdict>,
    pub rows_truncated: bool,
    pub counts: BTreeMap<RowOutcome, u64>,
    /// Rows whose write committed, by outcome; empty in a dry run.
    pub applied: BTreeMap<RowOutcome, u64>,
    pub samples: BTreeMap<RowOutcome, Vec<DeadNodeSample>>,
    /// Live mode deleted the node row.
    pub deleted: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub nodes: Vec<NodeReport>,
    /// Database reads or writes that failed; the node is judged again next
    /// sweep.
    pub errors: u32,
}

/// Whether `row` is selected, and its outcome if so: by its source
/// execution's instance when that is stamped, by the node's verdict when it
/// isn't.
pub fn decide_row(
    row: &DeadNodeRow,
    node: Verdict,
    instances: &BTreeMap<String, InstanceVerdict>,
) -> RowOutcome {
    let selected = match stamped_instance(row) {
        Some(instance_id) => instances
            .get(instance_id)
            .is_some_and(|verdict| verdict.is_dead()),
        None => node.is_lost(),
    };
    if selected {
        row_outcome(row)
    } else {
        RowOutcome::Unselected
    }
}

fn stamped_instance(row: &DeadNodeRow) -> Option<&str> {
    row.source_execution
        .as_ref()
        .and_then(|execution| execution.instance_id.as_deref())
}

/// The instance verdict the node's own heartbeat gives: a fresh heartbeat
/// comes from the machine its last registration recorded.
const fn heartbeat_instance(heartbeat: Heartbeat) -> Option<InstanceVerdict> {
    match heartbeat {
        Heartbeat::Fresh => Some(InstanceVerdict::Live),
        Heartbeat::Stale => None,
    }
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
    /// Where each node's next row read starts, after a read the cap cut
    /// short: the next tick reads the rows after it, so a node holding more
    /// than the cap is worked through rather than read from the start again.
    row_cursor: HashMap<NodeId, SandboxId>,
}

/// One tick's shared state: the reference instance, looked up once, and the
/// write budget.
struct TickState {
    reference: Option<Option<String>>,
    writes_left: u32,
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
            row_cursor: HashMap::new(),
        }
    }

    fn count_error(&self, report: &mut TickReport, stage: ErrorStage) {
        report.errors += 1;
        crate::metrics::dead_node::error(self.config.mode.metric(), stage.as_str());
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let nodes = match self
            .sink
            .candidate_nodes(
                self.managed_by,
                self.scan_after,
                self.config.max_nodes_per_tick,
            )
            .await
        {
            Ok(nodes) => nodes,
            Err(err) => {
                tracing::warn!(error = %err, "dead_node: candidate node scan failed");
                self.count_error(&mut report, ErrorStage::Scan);
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

        let mut state = TickState {
            reference: None,
            writes_left: if self.config.is_live() {
                self.config.max_writes_per_tick
            } else {
                0
            },
        };
        for node in nodes {
            self.judge_node(&node, live_uids.as_ref(), &mut state, &mut report)
                .await;
        }
        report
    }

    async fn judge_node(
        &mut self,
        node: &CandidateNode,
        live_uids: Option<&HashSet<String>>,
        state: &mut TickState,
        report: &mut TickReport,
    ) {
        let cap = self.config.max_rows_per_node;
        let after = self.row_cursor.get(&node.node_id).copied();
        let mut rows = match self
            .sink
            .rows_on_node(
                node,
                after,
                self.config.upload_settle,
                cap.saturating_add(1),
            )
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(
                    node_id = %node.node_id,
                    error = %err,
                    "dead_node: reading the node's rows failed; judging it again next sweep"
                );
                self.count_error(report, ErrorStage::Rows);
                return;
            }
        };
        let rows_truncated = rows.len() > cap as usize;
        rows.truncate(cap as usize);
        match rows.last() {
            Some(last) if rows_truncated => {
                self.row_cursor.insert(node.node_id, last.sandbox_id);
            }
            _ => {
                self.row_cursor.remove(&node.node_id);
            }
        }

        // The instances to ask about: every stamp on the rows, and the
        // recorded instance while the heartbeat is stale. A fresh heartbeat
        // comes from the recorded instance, which is live without asking.
        let mut asked: BTreeSet<String> = rows
            .iter()
            .filter_map(stamped_instance)
            .map(str::to_owned)
            .collect();
        let mut instances: BTreeMap<String, InstanceVerdict> = BTreeMap::new();
        if let Some(recorded) = &node.instance_id {
            match heartbeat_instance(node.heartbeat) {
                Some(verdict) => {
                    asked.remove(recorded);
                    instances.insert(recorded.clone(), verdict);
                }
                None => {
                    asked.insert(recorded.clone());
                }
            }
        }
        if !asked.is_empty() {
            let evidence = self.instance_evidence(&asked, live_uids, state).await;
            instances.extend(asked.into_iter().map(|id| {
                let verdict = instance_verdict(&id, &evidence);
                (id, verdict)
            }));
        }

        let kube = match node.heartbeat {
            Heartbeat::Stale => self.kube_evidence(node, live_uids).await,
            // Not read: a fresh heartbeat decides the node verdict first.
            Heartbeat::Fresh => KubeEvidence::Present,
        };
        let verdict = node_verdict(&NodeVerdictInputs {
            heartbeat: node.heartbeat,
            kube,
            absence_window: self.config.absence_window,
            recorded_instance: node
                .instance_id
                .as_ref()
                .and_then(|id| instances.get(id).copied()),
        });

        let mut counts: BTreeMap<RowOutcome, u64> = BTreeMap::new();
        let mut applied: BTreeMap<RowOutcome, u64> = BTreeMap::new();
        let mut samples: BTreeMap<RowOutcome, Vec<DeadNodeSample>> = BTreeMap::new();
        let mut write_failed = false;
        let decide = |row: &DeadNodeRow| decide_row(row, verdict, &instances);
        for row in &rows {
            let outcome = decide(row);
            *counts.entry(outcome).or_default() += 1;
            let listed = samples.entry(outcome).or_default();
            if listed.len() < SAMPLES_PER_OUTCOME {
                listed.push(DeadNodeSample {
                    sandbox_id: row.sandbox_id,
                    lifecycle: row.stored_lifecycle.clone(),
                });
            }
            if !outcome.writes() || state.writes_left == 0 {
                continue;
            }
            state.writes_left -= 1;
            let write = RowWrite {
                sandbox_id: row.sandbox_id,
                outcome,
                source_execution: row.source_execution.as_ref().map(|execution| execution.id),
                basis: if stamped_instance(row).is_some() {
                    WriteBasis::InstanceDead
                } else {
                    WriteBasis::NodeLost
                },
                decide: &decide,
            };
            match self
                .sink
                .write_row(node, &write, self.config.upload_settle)
                .await
            {
                Ok(WriteResult::Applied) => {
                    *applied.entry(outcome).or_default() += 1;
                    emit_events(node, row, outcome);
                }
                Ok(result) => {
                    tracing::info!(
                        node_id = %node.node_id,
                        sandbox_id = %row.sandbox_id,
                        outcome = %outcome,
                        result = ?result,
                        "dead_node: row write refused; judging it again next sweep"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        node_id = %node.node_id,
                        sandbox_id = %row.sandbox_id,
                        outcome = %outcome,
                        error = %err,
                        "dead_node: row write failed; judging it again next sweep"
                    );
                    write_failed = true;
                    self.count_error(report, ErrorStage::Write);
                }
            }
        }

        let mut deleted = false;
        if self.config.is_live() && verdict.is_lost() && !rows_truncated && !write_failed {
            match self.sink.delete_node(node).await {
                Ok(true) => deleted = true,
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(
                        node_id = %node.node_id,
                        error = %err,
                        "dead_node: deleting the node row failed"
                    );
                    self.count_error(report, ErrorStage::Delete);
                }
            }
        }

        let summary = |map: &BTreeMap<RowOutcome, u64>| {
            map.iter()
                .map(|(outcome, count)| format!("{outcome}={count}"))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let instance_summary = instances
            .iter()
            .map(|(id, verdict)| format!("{id}={}", instance_label(*verdict)))
            .collect::<Vec<_>>()
            .join(" ");
        tracing::info!(
            node_id = %node.node_id,
            k8s_node_name = node.k8s_node_name.as_deref().unwrap_or(""),
            mode = self.config.mode.as_str(),
            verdict = %verdict,
            reason = verdict.reason(),
            instances = %instance_summary,
            rows = rows.len(),
            rows_truncated,
            outcomes = %summary(&counts),
            applied = %summary(&applied),
            node_deleted = deleted,
            "dead_node: judged node"
        );
        let metric_mode = self.config.mode.metric();
        crate::metrics::dead_node::node_judged(metric_mode, verdict.as_str(), verdict.reason());
        for (outcome, count) in &counts {
            crate::metrics::dead_node::rows(
                metric_mode,
                verdict.as_str(),
                outcome.as_str(),
                *count,
            );
        }
        for (outcome, count) in &applied {
            crate::metrics::dead_node::rows_applied(metric_mode, outcome.as_str(), *count);
        }
        if deleted {
            crate::metrics::dead_node::node_deleted(metric_mode);
        }

        if !deleted {
            let labels: BTreeMap<String, String> = instances
                .iter()
                .map(|(id, verdict)| (id.clone(), instance_label(*verdict)))
                .collect();
            let status = DeadNodeStatusWrite {
                node_id: node.node_id,
                mode: self.config.mode.as_str(),
                verdict: verdict.as_str(),
                reason: verdict.reason(),
                rows_truncated,
                instances: labels
                    .iter()
                    .map(|(id, label)| (id.as_str(), label.as_str()))
                    .collect(),
                counts: counts
                    .iter()
                    .map(|(outcome, count)| (outcome.as_str(), *count))
                    .collect(),
                applied: applied
                    .iter()
                    .map(|(outcome, count)| (outcome.as_str(), *count))
                    .collect(),
                samples: samples
                    .iter()
                    .map(|(outcome, rows)| (outcome.as_str(), rows.clone()))
                    .collect(),
            };
            if let Err(err) = self.sink.write_status(&status).await {
                tracing::warn!(
                    node_id = %node.node_id,
                    error = %err,
                    "dead_node: status write failed"
                );
                self.count_error(report, ErrorStage::Report);
            }
        }

        report.nodes.push(NodeReport {
            node_id: node.node_id,
            verdict,
            instances,
            rows_truncated,
            counts,
            applied,
            samples,
            deleted,
        });
    }

    /// EC2's answer for `asked` and the tick's reference instance, in one
    /// call.
    async fn instance_evidence(
        &self,
        asked: &BTreeSet<String>,
        live_uids: Option<&HashSet<String>>,
        state: &mut TickState,
    ) -> InstanceEvidence {
        let InstanceAuthority::Ec2(client) = &self.authority else {
            return InstanceEvidence::NoAuthority;
        };
        let reference = match &state.reference {
            Some(reference) => reference.clone(),
            None => {
                let looked_up = self.reference_instance(live_uids).await;
                state.reference = Some(looked_up.clone());
                looked_up
            }
        };
        let Some(reference) = reference else {
            return InstanceEvidence::ReferenceUnavailable;
        };
        // A malformed ID is never asked about: it reads unknown before any
        // answer is read.
        let mut ids: Vec<String> = asked
            .iter()
            .filter(|id| well_formed_instance_id(id))
            .cloned()
            .collect();
        if !ids.contains(&reference) {
            ids.push(reference.clone());
        }
        match timeout(
            DIRECT_NODE_LOOKUP_TIMEOUT,
            client.describe_instances_by_ids(ids),
        )
        .await
        {
            Ok(Ok(matches)) => {
                let mut listed: BTreeMap<String, String> = BTreeMap::new();
                for instance in matches {
                    // A live entry wins over a terminated one: only an
                    // instance EC2 lists solely as `terminated` is dead.
                    let entry = listed.entry(instance.instance_id.clone()).or_default();
                    if entry.is_empty() || instance.is_live() {
                        *entry = instance.state;
                    }
                }
                InstanceEvidence::Answered { listed, reference }
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "dead_node: ec2 instance lookup failed");
                InstanceEvidence::LookupFailed
            }
            Err(_) => {
                tracing::warn!("dead_node: ec2 instance lookup timed out");
                InstanceEvidence::LookupFailed
            }
        }
    }

    /// The recorded instance of a fresh node whose Node the reflector lists:
    /// the control that an absent instance is not a call to the wrong region,
    /// account or filter.
    async fn reference_instance(&self, live_uids: Option<&HashSet<String>>) -> Option<String> {
        let live_uids = live_uids?;
        let candidates = match self
            .sink
            .reference_nodes(self.managed_by, REFERENCE_CANDIDATES)
            .await
        {
            Ok(candidates) => candidates,
            Err(err) => {
                tracing::warn!(error = %err, "dead_node: reference node read failed");
                return None;
            }
        };
        candidates
            .into_iter()
            .find(|candidate| {
                live_uids.contains(&candidate.k8s_node_uid)
                    && well_formed_instance_id(&candidate.instance_id)
            })
            .map(|candidate| candidate.instance_id)
    }

    async fn kube_evidence(
        &mut self,
        node: &CandidateNode,
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
}

fn instance_label(verdict: InstanceVerdict) -> String {
    match verdict {
        InstanceVerdict::Dead(_) => "dead".to_owned(),
        InstanceVerdict::Live => "live".to_owned(),
        InstanceVerdict::Unknown(reason) => format!("unknown:{}", reason.as_str()),
    }
}

/// The events of a committed write: `sandbox_lost` and `sandbox_failed` for a
/// row written off, `sandbox_lost` alone for a quarantined row without a
/// durable copy, which keeps its lifecycle, and `delete_completed` for a
/// delete the verb finished, as `orphaned_sandboxes` emits for one it
/// finishes.
fn emit_events(node: &CandidateNode, row: &DeadNodeRow, outcome: RowOutcome) {
    let details = || {
        SandboxEventDetails::default()
            .source_node_id(node.node_id.to_string())
            .reason("dead_node")
            .trigger(pplx_space_events::SandboxEventTrigger::System)
    };
    match outcome {
        RowOutcome::Lost | RowOutcome::LostSnapshotRetained => emit_lost_and_failed(
            row.sandbox_id,
            details()
                .with_from_state(row.stored_lifecycle.clone())
                .with_to_state("failed"),
            OPERATION,
        ),
        RowOutcome::FinishDelete => SandboxEvent::new(
            row.sandbox_id,
            SandboxEventType::DeleteCompleted,
            SandboxEventSource::ClusterController,
        )
        .emit_with(
            details()
                .with_from_state(row.stored_lifecycle.clone())
                .with_to_state("deleted"),
        ),
        RowOutcome::QuarantineLost => SandboxEvent::new(
            row.sandbox_id,
            SandboxEventType::SandboxLost,
            SandboxEventSource::ClusterController,
        )
        .emit_with(details()),
        _ => {}
    }
}

/// Drive the loop until `shutdown_token` fires. The reconciler set calls it
/// only in `dry-run` and `live`: `off` has no [`ReconcilerConfig`].
pub async fn run<S: DeadNodeSink, K: NodeMembership>(
    config: ReconcilerConfig,
    sink: S,
    membership: K,
    authority: InstanceAuthority,
    managed_by: SubstrateKind,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        mode = config.mode.as_str(),
        authority = authority.as_str(),
        tick_secs = config.tick.as_secs(),
        absence_window_secs = config.absence_window.as_secs(),
        upload_settle_secs = config.upload_settle.as_secs(),
        max_nodes_per_tick = config.max_nodes_per_tick,
        max_rows_per_node = config.max_rows_per_node,
        max_writes_per_tick = config.max_writes_per_tick,
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
