//! The node drain's v2 loop.
//!
//! A v2 drain is a drain the admin route started with `engine: v2`
//! (`nodes.drain_engine = 'v2'`), which wrote the epoch the node is sealed
//! under in the transaction that moved the phase, before any node call. The
//! node does the work: sandbox-manager's `DrainNode` seals it and makes every
//! resident durable elsewhere. This loop asks it, each tick, and records what
//! it reports:
//!
//! - **gentle**: a gentle drain (`drain_mode` gentle) keeps its epoch in
//!   `drain_status.gentle_epoch` rather than `epoch`, with the operator's
//!   `deadline`, so a controller that predates gentle drains finds no epoch
//!   and never calls the node for it. Its calls name the epoch in the
//!   request's `gentle_epoch` with `epoch` 0, which a sandbox-manager or
//!   node-api relay that predates gentle drains refuses before it seals
//!   anything ([`drain_request`]); the node holds every `started` sandbox,
//!   and the loop takes an answer only when it echoes `gentle_epoch`. At the
//!   deadline, by the database clock (the listing's `deadline_due`), or
//!   once an operator's `:drain` with `mode: forced` has set `drain_mode`, the loop turns the drain forced in one statement
//!   (`drain_mode`, `epoch` for `gentle_epoch`, `escalated`), and calls
//!   forced from then on, under the same epoch. A gentle drain that reaches
//!   `drained` holds nothing, and its epoch moves to `epoch` in the same
//!   statement, so everything from `drained` on reads it as a forced one's.
//!
//! - **`draining`, waiting**: the admin route starts every v2 drain waiting
//!   for one of the cluster's evacuation slots (`drain_status.waiting`).
//!   At most [`DEFAULT_MAX_EVACUATING_NODES`] nodes evacuate at once
//!   (admitted and not yet `drained`, or undrained with their cancel not yet
//!   finished; an admitted gentle drain not due to turn forced whose stored
//!   report lists nothing but held sandboxes counts no slot while it does);
//!   a waiting node whose heartbeat is fresh is admitted, oldest
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
//! lock and records why in `drain_last_error`. The first two need the
//! machine EC2 answered about to be the one the node registered: its
//! Kubernetes Node is gone, or the Node's `providerID` names the instance.
//! A live Node on another instance, or naming none, keeps the drain
//! waiting, since a stale recorded instance would have EC2 answer about
//! another machine; the delete already needs the Node gone. The node is then a plain
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
mod gentle;
mod step;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use tokio::time::Instant;

pub use admission::DEFAULT_MAX_EVACUATING_NODES;
use admission::admit;
#[cfg(test)]
use cleanup::{DISPATCH_BACKOFF_BASE, TERMINATE_BOUND};
use cleanup::{DispatchBackoff, fence, let_go, release, terminated_or_dispatch};
pub use cleanup::{Fence, TerminatedDelete};
pub use gentle::report_holds_only;
use gentle::{HOLD_ACTION, escalate};
use step::step;
#[cfg(test)]
use step::{OutstandingRows, Recording, UNRECORDED_BOUND, outstanding_rows, report_json};

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

/// A v2 drain's node, as the loop reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V2Node {
    pub id: NodeId,
    pub phase: Option<String>,
    /// `drain_status.epoch`, or `gentle_epoch` for a gentle drain; the admin
    /// route writes it with the phase.
    pub epoch: Option<u64>,
    /// The epoch is `drain_status.gentle_epoch`: a gentle drain not yet
    /// turned forced, nor `drained`.
    pub gentle: bool,
    /// `drain_mode` is `forced`: on a gentle drain, an operator escalated it.
    pub mode_forced: bool,
    /// `drain_status.deadline`, in Unix seconds: when a gentle drain turns
    /// forced.
    pub deadline: Option<i64>,
    /// The database clock has reached `deadline`. The loop decides a
    /// deadline by this alone, the clock the escalation's own statement
    /// reads, so a controller whose clock runs behind doesn't postpone it.
    pub deadline_due: bool,
    /// The stored report is the node's gentle answer under this epoch and
    /// lists nothing but held sandboxes: every `pending` entry's action is
    /// `hold`, and nothing is `blocked` or `unrecorded`
    /// ([`report_holds_only`]).
    pub report_holds_only: bool,
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

/// The `DrainNode` request of a drain call under `epoch`. A gentle one names
/// the epoch in `gentle_epoch` alone and leaves `epoch` 0: a sandbox-manager
/// that predates gentle drains, or a node-api relay that drops the field it
/// doesn't know, reads epoch 0 and refuses the call before it seals or
/// suspends anything, so no node runs a gentle drain as a forced one.
pub fn drain_request(epoch: u64, gentle: bool) -> DrainNodeRequest {
    if gentle {
        DrainNodeRequest {
            gentle_epoch: epoch,
            ..DrainNodeRequest::default()
        }
    } else {
        DrainNodeRequest {
            epoch,
            ..DrainNodeRequest::default()
        }
    }
}

/// The `DrainNode` request that cancels `epoch`, with the evacuations to
/// forget.
pub fn cancel_request(epoch: u64, forget: &[String]) -> DrainNodeRequest {
    DrainNodeRequest {
        epoch,
        cancel: true,
        forget: forget.to_vec(),
        ..DrainNodeRequest::default()
    }
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

/// Why a `DrainNode` call failed: the gRPC status the node, or its
/// node-api relay, answered with, or `None` for a failure before any answer
/// (the endpoint lookup, the connection), and the error as the loop records
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainCallError {
    code: Option<tonic::Code>,
    status_message: String,
    text: String,
}

impl DrainCallError {
    /// The status the call answered with.
    pub fn status(status: &tonic::Status) -> Self {
        Self {
            code: Some(status.code()),
            status_message: status.message().to_owned(),
            text: status.to_string(),
        }
    }

    /// A failure before the node answered.
    pub fn other(text: impl Into<String>) -> Self {
        Self {
            code: None,
            status_message: String::new(),
            text: text.into(),
        }
    }
}

impl std::fmt::Display for DrainCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// What the v2 loop reads and writes.
/// What the API server says of a node's machine ([`DrainV2Sink::node_machine`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeMachine {
    /// The Node is gone.
    Gone,
    /// The Node is live, on the EC2 instance its AWS `providerID` names, or
    /// on none it names.
    Live(Option<String>),
}

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
    /// Call the node's `DrainNode` with `request` ([`drain_request`],
    /// [`cancel_request`]).
    fn drain_node(
        &self,
        node_id: NodeId,
        request: DrainNodeRequest,
    ) -> impl std::future::Future<Output = Result<DrainNodeResponse, DrainCallError>> + Send;
    /// Turn the gentle `draining` drain under `epoch` forced, once its
    /// deadline has passed or an operator set `drain_mode` forced:
    /// `drain_mode` forced, the epoch moved from `gentle_epoch` to `epoch`,
    /// and `escalated` recorded. False when the row is no longer that
    /// epoch's gentle `draining` drain due to turn.
    fn escalate(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
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
    /// The machine the API server's Node named `k8s_node_name` runs on, by a
    /// direct GET: gone only on its 404, else the EC2 instance its AWS
    /// `providerID` names, if any.
    fn node_machine(
        &self,
        k8s_node_name: &str,
    ) -> impl std::future::Future<Output = Result<NodeMachine, String>> + Send;
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

#[cfg(test)]
mod tests;
