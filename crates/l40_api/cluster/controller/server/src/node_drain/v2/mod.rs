//! The node drain's v2 loop.
//!
//! A v2 drain is a forced drain the admin route started with `engine: v2`
//! (`nodes.drain_engine = 'v2'`), which wrote the epoch the node is sealed
//! under (`drain_status.epoch`) in the transaction that moved the phase,
//! before any node call. The node does the work: sandbox-manager's
//! `DrainNode` seals it and makes every resident durable elsewhere. This loop
//! asks it, each tick, and records what it reports:
//!
//! - **`draining`, waiting**: the admin route starts every v2 drain waiting
//!   for one of the cluster's evacuation slots (`drain_status.waiting`).
//!   At most [`DEFAULT_MAX_EVACUATING_NODES`] nodes evacuate at once
//!   (admitted and not yet `drained`, or undrained with their cancel not yet
//!   finished); a waiting node whose heartbeat is fresh is admitted, oldest
//!   drain first, once one frees. Until then it stays cordoned and gets no
//!   `DrainNode` call, so it isn't sealed and the drain gate refuses no user
//!   call on it.
//! - **`draining` and `drained`**: reconcile the cordon, call `DrainNode`
//!   under the epoch, and for each sandbox the node reports evacuated,
//!   enqueue the drain restore operation that brings one that was `started`
//!   back `started` elsewhere, in one transaction and at most once per
//!   snapshot; record one that was `paused` once it is durably `suspended`,
//!   restoring nothing, for its next resume or wake to restore elsewhere; or
//!   release a stopped one from the node with its disk pinned, for its next
//!   start or wake to materialize on any node. The report is stored in
//!   `drain_status`. A `draining` node moves to `drained` once the report is
//!   `done` under the row's own epoch and every evacuation is recorded. A
//!   `drained` node stays there, its report stored again every tick; nothing
//!   terminates a v2 node yet.
//! - **undrained** (no phase, `drain_status.cancel`): cancel the epoch on the
//!   node, and take back what the drain evacuated: the node hands back each
//!   sandbox the drain suspended, the loop records it as a drain records it
//!   (enqueueing the restore of one that was `started`, which may land on
//!   the node again), and the node forgets the ones recorded. Once the node is
//!   unsealed with nothing left to hand back, clear the v2 columns. Until
//!   then the node keeps its evacuation slot, as its running suspends and
//!   uploads finish; a drain that never left waiting holds none, and its
//!   cancel finishes on the first call the node answers with nothing to hand
//!   back.
//!
//! A node whose heartbeat is stale is asked about at EC2, by its recorded
//! instance, each tick; the answer is recorded in `drain_last_error` and
//! changes nothing, except that a node holding an evacuation slot frees it
//! early when EC2 lists its instance `shutting-down` or `terminated`.
//!
//! An evacuation the loop can't record for [`UNRECORDED_BOUND`] is reported
//! `blocked`, as the node reports work that doesn't settle.
//!
//! Every v1 writer keyed on drain phase skips a v2 row, so this loop is the
//! only one that moves one. It keeps nothing between ticks but its tick
//! timer and [`V2LoopState`]: the nodes it listed, so a node that leaves its
//! listing, however its row lost the drain, has its v2 gauges zeroed.

mod admission;
mod step;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_k8s_node_id::Ec2InstanceId;
use pplx_space_sbx_mgr_client::proto::{DrainEvacuated, DrainNodeResponse, DrainPrior};
use tokio_util::sync::CancellationToken;

pub use admission::DEFAULT_MAX_EVACUATING_NODES;
use admission::admit;
use step::step;
#[cfg(test)]
use step::{Recording, UNRECORDED_BOUND, outstanding_rows, report_json};

/// How long one EC2 call may take.
const AWS_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// What EC2 answers for one instance, by its id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstanceEvidence {
    /// Listed once, `terminated`.
    Terminated,
    /// Listed once, `shutting-down`.
    ShuttingDown,
    /// Listed once in any other state, or listed more than once: the machine
    /// may still run.
    Listed(String),
    /// Not listed.
    NotListed,
    /// No answer: an EC2 error or timeout, no recorded instance, or no EC2
    /// to ask. Why.
    Unknown(String),
}

impl InstanceEvidence {
    /// EC2's states for one instance id, as it listed them.
    pub fn from_states(states: &[String]) -> Self {
        match states {
            [] => Self::NotListed,
            [state] if state == "terminated" => Self::Terminated,
            [state] if state == "shutting-down" => Self::ShuttingDown,
            [state] => Self::Listed(state.clone()),
            many => Self::Listed(format!("listed {} times", many.len())),
        }
    }

    /// Evidence that the machine is going away for good, which nothing can
    /// undo: `shutting-down` or `terminated`.
    pub const fn irreversible(&self) -> bool {
        matches!(self, Self::Terminated | Self::ShuttingDown)
    }
}

impl std::fmt::Display for InstanceEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Terminated => f.write_str("terminated"),
            Self::ShuttingDown => f.write_str("shutting-down"),
            Self::Listed(state) => f.write_str(state),
            Self::NotListed => f.write_str("not listed"),
            Self::Unknown(why) => write!(f, "unknown ({why})"),
        }
    }
}

/// What the v2 loop asks of EC2.
pub trait V2InstanceAuthority: Send + Sync + 'static {
    /// EC2's answer for `instance_id`.
    fn describe_instance(
        &self,
        instance_id: &str,
    ) -> impl std::future::Future<Output = InstanceEvidence> + Send;
}

/// The v2 loop's authority: EC2, with the `aws` terminator, or none, which
/// reads every instance unknown.
pub enum V2Terminator {
    Ec2 {
        ec2: pplx_space_aws_client::ec2::Client,
    },
    Refuse,
}

impl V2InstanceAuthority for V2Terminator {
    async fn describe_instance(&self, instance_id: &str) -> InstanceEvidence {
        let Self::Ec2 { ec2, .. } = self else {
            return InstanceEvidence::Unknown("the terminator is not aws".to_owned());
        };
        if Ec2InstanceId::parse(instance_id).is_none() {
            return InstanceEvidence::Unknown(format!("{instance_id} is not an instance id"));
        }
        match tokio::time::timeout(
            AWS_CALL_TIMEOUT,
            ec2.describe_instances_by_ids(&[instance_id]),
        )
        .await
        {
            Ok(Ok(instances)) => {
                let states: Vec<String> = instances
                    .into_iter()
                    .filter(|instance| instance.instance_id == instance_id)
                    .map(|instance| instance.state)
                    .collect();
                InstanceEvidence::from_states(&states)
            }
            Ok(Err(error)) => InstanceEvidence::Unknown(error.to_string()),
            Err(_) => InstanceEvidence::Unknown("the EC2 call timed out".to_owned()),
        }
    }
}

/// A v2 drain's node, as the loop reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V2Node {
    pub id: NodeId,
    pub phase: Option<String>,
    /// `drain_status.epoch`; the admin route writes it with the phase.
    pub epoch: Option<u64>,
    /// `drain_status.cancel`: the drain was undrained.
    pub cancel: bool,
    /// Since when, in Unix seconds, each evacuation the last stored report
    /// names unrecorded has been.
    pub unrecorded_since: BTreeMap<String, i64>,
    /// `drain_status.waiting`: a `draining` node still waiting for an
    /// evacuation slot.
    pub waiting: bool,
    /// `node_k8s.instance_id`: the EC2 instance the node's registration
    /// recorded.
    pub instance_id: Option<String>,
    /// The node's heartbeat is fresh by the API's node-status rule.
    pub heartbeat_fresh: bool,
}

/// What the loop keeps between ticks: the nodes its last listing returned.
/// In memory only: a controller that restarts starts with no listing.
#[derive(Debug, Default)]
pub struct V2LoopState {
    listed: Mutex<HashSet<NodeId>>,
}

impl V2LoopState {
    /// Remember `listed` as this tick's listing. Returns the nodes the last
    /// listing returned that this one doesn't: deleted, cancelled, or taken
    /// by a registration.
    fn relist(&self, listed: HashSet<NodeId>) -> Vec<NodeId> {
        let mut last = self
            .listed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let left = last.difference(&listed).copied().collect();
        *last = listed;
        left
    }
}

/// What recording one reported evacuation found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvacuationRecord {
    /// Nothing more to do for it: its restore is enqueued or its stopped row
    /// released, by this call or an earlier one, or the sandbox has moved on.
    Recorded,
    /// Not yet, and why: the catalog doesn't hold the artifact as ready, the
    /// node's suspend isn't projected onto the row yet, or the row can't
    /// take the report.
    NotYet(String),
}

impl EvacuationRecord {
    pub const fn is_recorded(&self) -> bool {
        matches!(self, Self::Recorded)
    }
}

/// What the v2 loop reads and writes.
pub trait DrainV2Sink: Send + Sync + 'static {
    fn list_v2_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<V2Node>, String>> + Send;
    /// Admit the waiting drain under `epoch` to an evacuation slot; false
    /// when the row is no longer that epoch's waiting `draining` node.
    fn admit_evacuation(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Converge the node's cordon, as the v1 drain does.
    fn ensure_cordon(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    /// `forget`, with `cancel`: the evacuations recorded since a cancel call
    /// reported them, for the node to forget.
    fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
        forget: &[String],
    ) -> impl std::future::Future<Output = Result<DrainNodeResponse, String>> + Send;
    fn record_evacuation(
        &self,
        node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> impl std::future::Future<Output = Result<EvacuationRecord, String>> + Send;
    /// Store `report` under `epoch`; false when the row's epoch moved on.
    fn record_report(
        &self,
        node_id: NodeId,
        epoch: u64,
        report: serde_json::Value,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// `draining -> drained` under `epoch`, with a stored `done` report for
    /// it.
    fn advance_drained(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Clear an undrained row's v2 columns once its epoch is cancelled.
    fn finish_cancel(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    fn record_error(
        &self,
        node_id: NodeId,
        message: &str,
    ) -> impl std::future::Future<Output = ()> + Send;
}

/// What one tick did, for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct V2TickReport {
    pub admitted: Vec<NodeId>,
    pub drained: Vec<NodeId>,
    pub cancelled: Vec<NodeId>,
}

/// Unix seconds now, the clock `Recording::since` is kept in.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// How many v2 nodes one tick calls at once: each call waits on its node,
/// and a slow node must not hold up the rest.
const NODE_CONCURRENCY: usize = 8;

/// What one tick did to a node.
enum NodeStep {
    Cancelled,
    Drained,
}

/// One pass over every v2 node, [`NODE_CONCURRENCY`] at a time, with at
/// most `max_evacuating` of them evacuating. A node the last tick listed and
/// this one doesn't has its v2 gauges zeroed. EC2 is asked first, about each
/// node whose heartbeat is stale; admission and each node's step read its
/// answer.
pub async fn tick<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    max_evacuating: usize,
    state: &V2LoopState,
) -> V2TickReport {
    use futures_util::StreamExt as _;

    let mut report = V2TickReport::default();
    let mut nodes = match sink.list_v2_nodes().await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::warn!(%error, "node_drain v2: listing v2 nodes failed");
            return report;
        }
    };
    for node_id in state.relist(nodes.iter().map(|node| node.id).collect()) {
        crate::metrics::node_drain::clear_v2_gauges(&node_id.to_string());
    }
    let asked: Vec<(NodeId, Option<String>)> = nodes
        .iter()
        .filter(|node| node.epoch.is_some() && !node.heartbeat_fresh)
        .map(|node| (node.id, node.instance_id.clone()))
        .collect();
    let evidence: HashMap<NodeId, InstanceEvidence> = futures_util::stream::iter(asked)
        .map(|(node_id, instance_id)| async move {
            let answer = match instance_id {
                Some(instance_id) => authority.describe_instance(&instance_id).await,
                None => InstanceEvidence::Unknown("the node records no EC2 instance".to_owned()),
            };
            (node_id, answer)
        })
        .buffered(NODE_CONCURRENCY)
        .collect()
        .await;
    report.admitted = admit(sink, &mut nodes, max_evacuating, &evidence).await;
    let steps: Vec<_> = futures_util::stream::iter(nodes.into_iter().map(|node| {
        let evidence = evidence.get(&node.id).cloned();
        (node, evidence)
    }))
    .map(|(node, evidence)| async move { (node.id, step(sink, &node, evidence.as_ref()).await) })
    .buffered(NODE_CONCURRENCY)
    .collect()
    .await;
    for (node_id, step) in steps {
        match step {
            Some(NodeStep::Cancelled) => report.cancelled.push(node_id),
            Some(NodeStep::Drained) => report.drained.push(node_id),
            None => {}
        }
    }
    report
}

/// Run the v2 loop every `tick_every` until `shutdown`.
pub async fn run<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: S,
    authority: A,
    tick_every: Duration,
    max_evacuating: usize,
    shutdown: CancellationToken,
) {
    tracing::info!(
        tick_secs = tick_every.as_secs(),
        max_evacuating,
        "starting the node drain's v2 loop"
    );
    let state = V2LoopState::default();
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        let report = tick(&sink, &authority, max_evacuating, &state).await;
        if report != V2TickReport::default() {
            tracing::info!(?report, "node_drain v2: tick");
        }
    }
    tracing::info!("node drain's v2 loop stopping");
}

#[cfg(test)]
mod tests;
