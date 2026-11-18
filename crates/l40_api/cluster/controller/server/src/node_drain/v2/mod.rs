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
//!   `done` under the row's own epoch and every evacuation is recorded. On
//!   every `drained` tick the loop also releases the `suspended` rows that
//!   still name the node and no longer need it: durable, with no restore in
//!   flight (`pplx_space_sandbox_store::drain_terminate`).
//! - **`terminating`**: `:terminate` moved it here, gated on a fresh `done`
//!   report under the row's epoch and on nothing naming the node. The loop
//!   never writes a Kubernetes object here. It runs the terminate
//!   transaction under the node's row lock: the node is still `terminating`
//!   at the epoch with the report `done`; the release runs again; nothing
//!   may name the node (the require-empty predicate, whose blockers go to
//!   `drain_last_error`); and the fence `drain_status.terminate_instance`
//!   records the instance to terminate, `node_k8s.instance_id`. Only the
//!   `aws` terminator fences. After the fence, the loop asks EC2 about that
//!   instance on its dispatch backoff and, while EC2 lists it in a state
//!   other than `shutting-down` or `terminated` and no dispatch has
//!   succeeded, asks Auto Scaling to terminate exactly it, recording
//!   `dispatched_at` on success; it never dispatches after that, and an
//!   instance EC2 doesn't list is never dispatched. The first time EC2
//!   lists the fenced instance `terminated`, the loop records it
//!   (`drain_status.terminated_observed_at`): EC2 stops listing a terminated
//!   instance after a while, so from then on an unlisted instance reads as
//!   terminated. Once it does and a direct GET of the Kubernetes Node
//!   answers 404, the loop deletes the row while the heartbeat is stale and
//!   the row still carries the fence, a dispatch and the recorded
//!   `terminated`, with no tombstone ([`super::guarded_delete`]). A row an
//!   earlier build's terminator acted on (`drain_status.terminated`) reads as
//!   fenced on its recorded instance and dispatched.
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
//! A node whose machine is gone some other way is let go: once EC2 answers
//! `terminated` for its recorded instance (asked while its heartbeat is
//! stale), or for a fenced instance no dispatch was confirmed for, or when
//! the delete of a terminated node's row finds a row or execution still
//! naming it, the loop clears the v2 columns and the phase under the node's
//! lock and records why in `drain_last_error`. The node is then a plain
//! crashed node to `lost_started_sandboxes`, `orphaned_nodes` and
//! `orphaned_sandboxes`; the drain writes no row off. Any other answer, an
//! EC2 error, or no answer is recorded and changes nothing. A node that
//! holds an evacuation slot frees it early only when EC2 lists its instance
//! `shutting-down` or `terminated`.
//!
//! An evacuation the loop can't record for [`UNRECORDED_BOUND`] is reported
//! `blocked`, as the node reports work that doesn't settle.
//!
//! Every v1 writer keyed on drain phase skips a v2 row, so this loop is the
//! only one that moves one. It keeps nothing between ticks but its tick
//! timer and [`V2LoopState`]: the dispatch backoff, where a lost backoff
//! only retries sooner, and the nodes it listed, so a node that leaves its
//! listing, however its row lost the drain, has its v2 gauges zeroed.

mod admission;
mod cleanup;
mod step;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::SandboxId;
use pplx_space_k8s_node_id::Ec2InstanceId;
use pplx_space_sbx_mgr_client::proto::{DrainEvacuated, DrainNodeResponse, DrainPrior};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub use admission::DEFAULT_MAX_EVACUATING_NODES;
use admission::admit;
#[cfg(test)]
use cleanup::{DISPATCH_BACKOFF_BASE, TERMINATE_BOUND};
use cleanup::{DispatchBackoff, REFUSED_TERMINATE, fence, let_go, release, terminated_or_dispatch};
pub use cleanup::{Fence, TerminatedDelete};
use step::step;
#[cfg(test)]
use step::{Recording, UNRECORDED_BOUND, outstanding_rows, report_json};

/// How long one EC2 or Auto Scaling call may take.
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

/// What the v2 loop asks of EC2 and Auto Scaling.
pub trait V2InstanceAuthority: Send + Sync + 'static {
    /// Whether this authority terminates: only the `aws` terminator's does.
    /// One that doesn't never fences a node.
    fn terminates(&self) -> bool;

    /// EC2's answer for `instance_id`.
    fn describe_instance(
        &self,
        instance_id: &str,
    ) -> impl std::future::Future<Output = InstanceEvidence> + Send;

    /// Ask Auto Scaling to terminate exactly `instance_id`.
    fn terminate_instance(
        &self,
        instance_id: &str,
        decrement_capacity: bool,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// The v2 loop's authority: EC2 and Auto Scaling, with the `aws`
/// terminator, or none, which leaves a node `terminating` unfenced and reads
/// every instance unknown.
pub enum V2Terminator {
    Ec2 {
        ec2: pplx_space_aws_client::ec2::Client,
        autoscaling: pplx_space_aws_client::autoscaling::Client,
    },
    Refuse,
}

impl V2InstanceAuthority for V2Terminator {
    fn terminates(&self) -> bool {
        matches!(self, Self::Ec2 { .. })
    }

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

    async fn terminate_instance(
        &self,
        instance_id: &str,
        decrement_capacity: bool,
    ) -> Result<(), String> {
        let Self::Ec2 { autoscaling, .. } = self else {
            return Err(REFUSED_TERMINATE.to_owned());
        };
        let instance = Ec2InstanceId::parse(instance_id)
            .ok_or_else(|| format!("{instance_id} is not an instance id"))?;
        match tokio::time::timeout(
            AWS_CALL_TIMEOUT,
            super::aws::terminate_fenced_instance(autoscaling, &instance, decrement_capacity),
        )
        .await
        {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("the Auto Scaling call timed out; its outcome is unknown".to_owned()),
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
    /// `node_k8s.k8s_node_name`.
    pub k8s_node_name: Option<String>,
    /// `node_k8s.k8s_node_uid`.
    pub k8s_node_uid: Option<String>,
    /// `node_k8s.instance_id`: the EC2 instance the node's registration
    /// recorded.
    pub instance_id: Option<String>,
    /// The node's heartbeat is fresh by the API's node-status rule.
    pub heartbeat_fresh: bool,
    /// `drain_status.terminate_instance`: the fence, the instance the
    /// terminate transaction chose to terminate. A row an earlier build's
    /// terminator acted on (`drain_status.terminated`) is fenced on its
    /// recorded instance.
    pub fence: Option<String>,
    /// Auto Scaling accepted the fenced instance's termination:
    /// `drain_status.dispatched_at` is recorded, or an earlier build's
    /// terminator acted on the row (`drain_status.terminated`).
    pub dispatched: bool,
    /// `drain_status.dispatched_at`, in Unix seconds: when Auto Scaling
    /// accepted the fenced instance's termination; `None` on a row the
    /// earlier build's terminator acted on.
    pub dispatched_at: Option<i64>,
    /// `drain_status.terminated_observed_at`, in Unix seconds: when the loop
    /// first saw EC2 list the fenced instance `terminated`.
    pub terminated_observed_at: Option<i64>,
    /// `nodes.terminate_decrement_capacity`, which the termination passes on.
    pub decrement_capacity: bool,
}

/// What the loop keeps between ticks: the [`DispatchBackoff`], and the
/// nodes its last listing returned. In memory only: a controller that
/// restarts starts with no listing.
#[derive(Debug, Default)]
pub struct V2LoopState {
    backoff: DispatchBackoff,
    listed: Mutex<HashSet<NodeId>>,
}

impl V2LoopState {
    /// Remember `listed` as this tick's listing. Returns the nodes the last
    /// listing returned that this one doesn't: deleted, let go by the loop
    /// or by an operator, cancelled, or taken by a registration.
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
    /// Release the durably suspended rows that name a `drained` node under
    /// `epoch` with its report `done`
    /// (`pplx_space_sandbox_store::drain_terminate::release_durable_suspended`);
    /// the ids released.
    fn release_suspended(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, String>> + Send;
    /// The terminate transaction, under the node's row lock: re-check the
    /// epoch's unfenced `terminating` node with its report `done`, release
    /// again, require that nothing names the node, and write the fence.
    fn fence(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<Fence, String>> + Send;
    /// Record that Auto Scaling accepted the termination of the fenced
    /// `instance_id`; false when the row no longer carries that fence
    /// without a dispatch.
    fn record_dispatched(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Record that EC2 listed the fenced `instance_id` `terminated`
    /// (`drain_status.terminated_observed_at`), keeping the first time it
    /// did; false when the row is no longer that epoch's `terminating` node
    /// fenced on that instance.
    fn record_terminated_observed(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Whether the API server has a Node named `k8s_node_name`, by a direct
    /// GET: false only on its 404.
    fn node_exists(
        &self,
        k8s_node_name: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Delete the row of `node`, whose fenced instance `fence` EC2 says is
    /// terminated, or no longer lists after the row recorded it
    /// `terminated`, and whose Node is gone; let go of it instead when a row
    /// or an execution still names it.
    fn delete_terminated(
        &self,
        node: &V2Node,
        epoch: u64,
        fence: &str,
    ) -> impl std::future::Future<Output = Result<TerminatedDelete, String>> + Send;
    /// Let go of `node` under `epoch`, in the phase and with the fence the
    /// tick read: clear its phase and v2 columns under its row lock and
    /// record `trace` as its last error. False when the row moved on.
    fn let_go(
        &self,
        node: &V2Node,
        epoch: u64,
        trace: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
}

/// What one tick did, for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct V2TickReport {
    pub admitted: Vec<NodeId>,
    pub drained: Vec<NodeId>,
    pub cancelled: Vec<NodeId>,
    /// Nodes whose terminate transaction wrote the fence.
    pub fenced: Vec<NodeId>,
    /// Nodes whose fenced instance's termination Auto Scaling accepted.
    pub dispatched: Vec<NodeId>,
    /// Nodes whose row the loop deleted.
    pub deleted: Vec<NodeId>,
    /// Nodes the loop let go.
    pub let_go: Vec<NodeId>,
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
    Fenced,
    Dispatched,
    Deleted,
    LetGo,
}

/// One pass over every v2 node, [`NODE_CONCURRENCY`] at a time, with at
/// most `max_evacuating` of them evacuating. A node the last tick listed and
/// this one doesn't has its v2 gauges zeroed. EC2 is asked first, about each
/// fenced node that the backoff lets ask and each other node whose heartbeat
/// is stale; admission and each node's step read its answer.
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
    let backoff = &state.backoff;
    backoff.retain(
        &nodes
            .iter()
            .filter(|node| node.fenced().is_some())
            .map(|node| node.id)
            .collect(),
    );
    let now = Instant::now();
    let asked: Vec<(NodeId, Option<String>)> = nodes
        .iter()
        .filter(|node| node.epoch.is_some())
        .filter(|node| match node.fenced() {
            Some(_) => backoff.due(node.id, now),
            None => !node.heartbeat_fresh,
        })
        .map(|node| (node.id, node.asked_instance().map(str::to_owned)))
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
    .map(|(node, evidence)| async move {
        (
            node.id,
            step(sink, authority, &node, evidence.as_ref(), backoff).await,
        )
    })
    .buffered(NODE_CONCURRENCY)
    .collect()
    .await;
    for (node_id, step) in steps {
        match step {
            Some(NodeStep::Cancelled) => report.cancelled.push(node_id),
            Some(NodeStep::Drained) => report.drained.push(node_id),
            Some(NodeStep::Fenced) => report.fenced.push(node_id),
            Some(NodeStep::Dispatched) => report.dispatched.push(node_id),
            Some(NodeStep::Deleted) => report.deleted.push(node_id),
            Some(NodeStep::LetGo) => report.let_go.push(node_id),
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
