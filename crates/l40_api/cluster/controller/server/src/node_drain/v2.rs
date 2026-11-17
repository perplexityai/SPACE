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
//!   enqueue the drain restore operation that brings a suspended one back to
//!   its prior lifecycle elsewhere, in one transaction and at most once per
//!   snapshot, or release a stopped one from the node with its disk pinned,
//!   for its next start or wake to materialize on any node. The report is stored in
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
//!   (enqueueing the restore to its prior lifecycle, which may land on the
//!   node again), and the node forgets the ones recorded. Once the node is
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::SandboxId;
use pplx_space_k8s_node_id::Ec2InstanceId;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, DrainPrior,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

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

/// The `drain_last_error` of a `terminating` node no authority terminates.
pub const REFUSED_TERMINATE: &str = "a v2 node is terminated only through the aws terminator (SPACE_NODE_DRAIN_TERMINATOR_KIND=aws)";

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

impl V2Node {
    /// Holding an evacuation slot: admitted and not yet `drained`, or
    /// undrained after its admission with its cancel not yet finished, which
    /// lets the node's running suspends and uploads finish. The slot frees
    /// once the node leaves v2: its cancel finished, or its row deleted. An
    /// admitted gentle drain frees it while it only holds
    /// ([`Self::only_holding`]), and takes it again, whatever the other
    /// drains hold, whenever its report lists anything else and once it is
    /// due to turn forced or is escalated: until those finish the cluster
    /// may evacuate more nodes than the cap, and admission admits none while
    /// it does.
    fn evacuating(&self) -> bool {
        self.epoch.is_some()
            && !self.waiting
            && match self.phase.as_deref() {
                Some("draining") => !self.only_holding(),
                None => self.cancel,
                Some(_) => false,
            }
    }

    /// A gentle drain that isn't due to turn forced, whose stored report
    /// under its epoch lists only held sandboxes: the node suspends and
    /// uploads nothing for it, so it needs no slot.
    fn only_holding(&self) -> bool {
        self.gentle && !self.mode_forced && !self.deadline_due && self.report_holds_only
    }

    /// [`Self::evacuating`], unless EC2 has said the node's instance is
    /// going away for good: a stopped or unlisted instance, a stale
    /// heartbeat or no answer keeps the slot.
    fn holds_slot(&self, evidence: Option<&InstanceEvidence>) -> bool {
        self.evacuating() && !evidence.is_some_and(InstanceEvidence::irreversible)
    }

    fn awaiting_slot(&self) -> bool {
        self.epoch.is_some() && self.phase.as_deref() == Some("draining") && self.waiting
    }

    /// `terminating` with its fence written.
    fn fenced(&self) -> Option<&str> {
        self.fence
            .as_deref()
            .filter(|_| self.phase.as_deref() == Some("terminating"))
    }

    /// The instance to ask EC2 about: the fence once written, else the
    /// recorded instance.
    fn asked_instance(&self) -> Option<&str> {
        self.fenced().or(self.instance_id.as_deref())
    }

    /// A gentle `draining` drain due to turn forced: the database clock has
    /// reached its deadline, or an operator escalated it.
    fn escalation_due(&self) -> bool {
        self.gentle
            && self.phase.as_deref() == Some("draining")
            && (self.mode_forced || self.deadline_due)
    }
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

/// How long an evacuation may stay unrecorded before the report names it
/// `blocked`: the node's own bound on a transition it doesn't drive.
pub const UNRECORDED_BOUND: Duration = Duration::from_secs(15 * 60);

/// How long EC2 may go on listing a fenced instance not `terminated` after
/// Auto Scaling accepted its termination before the loop says so in
/// `drain_last_error`.
pub const TERMINATE_BOUND: Duration = Duration::from_secs(15 * 60);

/// The first and longest waits between a fenced node's EC2 checks.
const DISPATCH_BACKOFF_BASE: Duration = Duration::from_secs(10);
const DISPATCH_BACKOFF_CAP: Duration = Duration::from_secs(120);

/// When each fenced node is next asked about: its EC2 check, and the
/// dispatch it may lead to, wait out a backoff that doubles from
/// [`DISPATCH_BACKOFF_BASE`] to [`DISPATCH_BACKOFF_CAP`]. In memory only:
/// a controller that restarts asks again at once, and only a crash, a lost
/// `dispatched_at` write or an unknown outcome repeats a dispatch.
#[derive(Debug, Default)]
struct DispatchBackoff {
    next: Mutex<HashMap<NodeId, (Instant, u32)>>,
}

impl DispatchBackoff {
    fn due(&self, node_id: NodeId, now: Instant) -> bool {
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&node_id)
            .is_none_or(|(at, _)| *at <= now)
    }

    /// Wait out the next backoff before asking about `node_id` again.
    fn defer(&self, node_id: NodeId, now: Instant) {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attempts = next.get(&node_id).map_or(0, |(_, attempts)| *attempts);
        let wait = DISPATCH_BACKOFF_BASE
            .saturating_mul(2_u32.saturating_pow(attempts.min(16)))
            .min(DISPATCH_BACKOFF_CAP);
        next.insert(node_id, (now + wait, attempts.saturating_add(1)));
    }

    /// Keep only the fenced nodes' entries.
    fn retain(&self, fenced: &HashSet<NodeId>) {
        self.next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|node_id, _| fenced.contains(node_id));
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

/// What the terminate transaction did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fence {
    /// The fence is written: the instance to terminate.
    Fenced(String),
    /// Rows or executions still name the node; the blockers are in
    /// `drain_last_error`.
    Held(String),
    /// The node records no instance to terminate.
    NoInstance,
    /// The row is no longer the epoch's unfenced `terminating` v2 node with
    /// a `done` report.
    Moved,
}

/// What the delete of a terminated node's row did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminatedDelete {
    /// The row is gone, with no tombstone.
    Deleted,
    /// A row or execution guard refused the delete, so the drain let the
    /// node go: the trace it recorded.
    LetGo(String),
    /// The row no longer carries the fence: a registration took it for
    /// another instance. Nothing to do.
    Unfenced,
    /// Not yet, and why: a fresh heartbeat, a moved identity or phase.
    Wait(String),
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

    /// The refusal of a sandbox-manager that predates gentle drains, or of
    /// one behind a node-api relay that drops `gentle_epoch`: it reads the
    /// gentle call's `epoch` 0 and refuses it, before it seals or suspends
    /// anything, with its epoch check's `InvalidArgument` "epoch must be
    /// nonzero ...". A node with gentle drains never answers that to a
    /// gentle call.
    fn refused_by_an_older_node(&self) -> bool {
        self.code == Some(tonic::Code::InvalidArgument)
            && self.status_message.starts_with("epoch must be nonzero")
    }
}

impl std::fmt::Display for DrainCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// The `pending` action a gentle drain's node reports for a sandbox it
/// holds running.
const HOLD_ACTION: &str = "hold";

/// Whether `report`, a stored `drain_status.report`, is the node's gentle
/// answer sealed under `epoch` that lists nothing but held sandboxes: every
/// `pending` entry's action is `hold`, and `blocked` and `unrecorded` are
/// empty. A report of any other shape, or none, isn't.
pub fn report_holds_only(report: Option<&serde_json::Value>, epoch: Option<u64>) -> bool {
    let Some(report) = report else {
        return false;
    };
    let entries = |key: &str| report.get(key).and_then(serde_json::Value::as_array);
    epoch.is_some()
        && report
            .get("sealed_epoch")
            .and_then(serde_json::Value::as_u64)
            == epoch
        && report.get("gentle").and_then(serde_json::Value::as_bool) == Some(true)
        && entries("blocked").is_some_and(Vec::is_empty)
        && entries("unrecorded").is_some_and(Vec::is_empty)
        && entries("pending").is_some_and(|pending| {
            pending.iter().all(|entry| {
                entry.get("action").and_then(serde_json::Value::as_str) == Some(HOLD_ACTION)
            })
        })
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

/// What the loop recorded of one report: each evacuation's record, and
/// since when each one still unrecorded has been, in Unix seconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recording {
    pub records: Vec<(String, EvacuationRecord)>,
    pub since: BTreeMap<String, i64>,
    pub now: i64,
}

impl Recording {
    /// The unrecorded evacuations, with their reason and since when.
    fn unrecorded(&self) -> impl Iterator<Item = (&str, &str, i64)> {
        self.records
            .iter()
            .filter_map(|(sandbox_id, record)| match record {
                EvacuationRecord::NotYet(reason) => Some((
                    sandbox_id.as_str(),
                    reason.as_str(),
                    self.since.get(sandbox_id).copied().unwrap_or(self.now),
                )),
                EvacuationRecord::Recorded => None,
            })
    }

    /// The unrecorded evacuations past [`UNRECORDED_BOUND`], with why.
    fn overdue(&self) -> Vec<(&str, String)> {
        let bound = i64::try_from(UNRECORDED_BOUND.as_secs()).unwrap_or(i64::MAX);
        self.unrecorded()
            .filter(|(_, _, since)| self.now.saturating_sub(*since) >= bound)
            .map(|(sandbox_id, reason, _)| {
                (
                    sandbox_id,
                    format!(
                        "not recorded for over {} minutes: {reason}",
                        UNRECORDED_BOUND.as_secs() / 60
                    ),
                )
            })
            .collect()
    }

    fn all_recorded(&self) -> bool {
        self.records.iter().all(|(_, record)| record.is_recorded())
    }
}

/// The JSON `drain_status.report` holds: the node's answer and what the loop
/// recorded of it. An evacuation unrecorded past [`UNRECORDED_BOUND`] is
/// `blocked` as well, unless the node already reports its sandbox blocked.
pub fn report_json(response: &DrainNodeResponse, recording: &Recording) -> serde_json::Value {
    let prior = |value: i32| match DrainPrior::try_from(value) {
        Ok(DrainPrior::Started) => "started",
        Ok(DrainPrior::Paused) => "paused",
        Ok(DrainPrior::Stopped) => "stopped",
        _ => "unknown",
    };
    serde_json::json!({
        "sealed_epoch": response.sealed_epoch,
        "gentle": response.gentle_epoch.is_some(),
        "done": response.done,
        "evacuated": response.evacuated.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "prior": prior(entry.prior),
            "artifact_id": entry.artifact_id,
        })).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(|entry| if entry.reason.is_empty() {
            serde_json::json!({
                "sandbox_id": entry.sandbox_id,
                "action": entry.action,
            })
        } else {
            serde_json::json!({
                "sandbox_id": entry.sandbox_id,
                "action": entry.action,
                "reason": entry.reason,
            })
        }).collect::<Vec<_>>(),
        "blocked": response.blocked.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "reason": entry.reason,
        })).chain(overdue_unblocked(response, recording).map(|(sandbox_id, reason)| serde_json::json!({
            "sandbox_id": sandbox_id,
            "reason": reason,
        }))).collect::<Vec<_>>(),
        "unrecorded": recording.unrecorded().map(|(sandbox_id, reason, since)| serde_json::json!({
            "sandbox_id": sandbox_id,
            "reason": reason,
            "since": since,
        })).collect::<Vec<_>>(),
    })
}

/// Unix seconds now, the clock `Recording::since` is kept in.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Record each evacuation `response` reports, carrying over since when each
/// unrecorded one has been from the node's last stored report.
async fn record_evacuations<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    response: &DrainNodeResponse,
) -> Recording {
    let now = unix_now();
    let mut recording = Recording {
        now,
        ..Recording::default()
    };
    for evacuated in &response.evacuated {
        let record = match sink.record_evacuation(node.id, evacuated).await {
            Ok(record) => record,
            Err(error) => EvacuationRecord::NotYet(format!("recording failed: {error}")),
        };
        if !record.is_recorded() {
            recording.since.insert(
                evacuated.sandbox_id.clone(),
                node.unrecorded_since
                    .get(&evacuated.sandbox_id)
                    .copied()
                    .unwrap_or(now),
            );
        }
        recording
            .records
            .push((evacuated.sandbox_id.clone(), record));
    }
    recording
}

/// The evacuations unrecorded past the bound whose sandbox the node doesn't
/// already report blocked, with why.
fn overdue_unblocked<'a>(
    response: &'a DrainNodeResponse,
    recording: &'a Recording,
) -> impl Iterator<Item = (&'a str, String)> {
    recording.overdue().into_iter().filter(|(sandbox_id, _)| {
        !response
            .blocked
            .iter()
            .any(|entry| entry.sandbox_id.as_deref() == Some(*sandbox_id))
    })
}

/// How many of the node's rows `response` and the loop's `recording` of it
/// leave blocked, pending and held, each row counted once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct OutstandingRows {
    /// The node reports the row blocked, or its evacuation is unrecorded
    /// past the bound. A blocked entry that names no sandbox counts on its
    /// own.
    blocked: u64,
    /// Not blocked, and the node reports it pending with work under way,
    /// its window's `queued` and `disk` among the reasons, or its evacuation
    /// is unrecorded within the bound.
    pending: u64,
    /// Not blocked, and every pending entry the node reports for it is a
    /// gentle drain's `hold`: a running sandbox the drain waits for, with
    /// nothing started for it.
    held: u64,
}

fn outstanding_rows(response: &DrainNodeResponse, recording: &Recording) -> OutstandingRows {
    let mut blocked: BTreeSet<&str> = recording
        .overdue()
        .into_iter()
        .map(|(sandbox_id, _)| sandbox_id)
        .collect();
    let mut unnamed = 0_u64;
    for entry in &response.blocked {
        match entry.sandbox_id.as_deref() {
            Some(sandbox_id) => {
                blocked.insert(sandbox_id);
            }
            None => unnamed += 1,
        }
    }
    let working: BTreeSet<&str> = response
        .pending
        .iter()
        .filter(|entry| entry.action != HOLD_ACTION)
        .map(|entry| entry.sandbox_id.as_str())
        .chain(recording.unrecorded().map(|(sandbox_id, _, _)| sandbox_id))
        .filter(|sandbox_id| !blocked.contains(sandbox_id))
        .collect();
    let held: BTreeSet<&str> = response
        .pending
        .iter()
        .filter(|entry| entry.action == HOLD_ACTION)
        .map(|entry| entry.sandbox_id.as_str())
        .filter(|sandbox_id| !blocked.contains(sandbox_id) && !working.contains(sandbox_id))
        .collect();
    OutstandingRows {
        blocked: blocked.len() as u64 + unnamed,
        pending: working.len() as u64,
        held: held.len() as u64,
    }
}

/// Publish what `response` and the loop's `recording` of it leave to do,
/// as [`outstanding_rows`] counts it.
fn publish_report(node: &V2Node, response: &DrainNodeResponse, recording: &Recording) {
    let rows = outstanding_rows(response, recording);
    crate::metrics::node_drain::v2_report(
        &node.id.to_string(),
        rows.blocked,
        rows.pending,
        rows.held,
    );
}

/// How many v2 nodes one tick calls at once: each call waits on its node,
/// and a slow node must not hold up the rest.
const NODE_CONCURRENCY: usize = 8;

/// How many v2 nodes may evacuate at once by default
/// (`SPACE_DRAIN_V2_MAX_EVACUATING_NODES`). Each node runs its own window of
/// suspends and uploads (sandbox-manager's `SPACE_DRAIN_NODE_WINDOW`, 8), so
/// 4 nodes put at most 32 sandboxes' suspends and uploads in flight across
/// the cluster, half the 64 the v1 drain shared with its restores.
pub const DEFAULT_MAX_EVACUATING_NODES: usize = 4;

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

async fn step<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    evidence: Option<&InstanceEvidence>,
    backoff: &DispatchBackoff,
) -> Option<NodeStep> {
    let Some(epoch) = node.epoch else {
        // The admin route writes the epoch with the phase; a row without
        // one gets no call, sealed under nothing.
        tracing::warn!(node_id = %node.id, "node_drain v2: drain_status has no epoch");
        return None;
    };
    if let Some(fence) = node.fenced() {
        let evidence = evidence?;
        return terminated_or_dispatch(sink, authority, node, epoch, fence, evidence, backoff)
            .await;
    }
    if let Some(evidence) = evidence {
        let instance = node.instance_id.as_deref().unwrap_or("<none>");
        if *evidence == InstanceEvidence::Terminated {
            let trace =
                format!("v2 drain let go: instance {instance} terminated outside the drain");
            return let_go(sink, node, epoch, &trace)
                .await
                .then_some(NodeStep::LetGo);
        }
        sink.record_error(
            node.id,
            &format!(
                "the node's heartbeat is stale; EC2 answers {evidence} for its instance {instance}"
            ),
        )
        .await;
    }
    match node.phase.as_deref() {
        None if node.cancel => cancel(sink, node, epoch)
            .await
            .then_some(NodeStep::Cancelled),
        None => None,
        Some("draining") if node.waiting => {
            escalate(sink, node, epoch).await;
            // Fenced, but not sealed: it waits for a slot.
            if let Err(error) = sink.ensure_cordon(node.id).await {
                sink.record_error(node.id, &format!("cordon convergence failed: {error}"))
                    .await;
            }
            None
        }
        Some("draining") | Some("drained") => {
            let drained = drain(sink, node, epoch).await;
            if node.phase.as_deref() == Some("drained") {
                release(sink, node.id, epoch).await;
            }
            drained.then_some(NodeStep::Drained)
        }
        Some("terminating") => fence(sink, authority, node.id, epoch).await,
        Some(other) => {
            tracing::warn!(node_id = %node.id, phase = other, "node_drain v2: unknown phase");
            None
        }
    }
}

/// Admit waiting drains whose heartbeat is fresh, oldest first (the epoch is
/// the drain's start), while fewer than `max_evacuating` nodes hold a slot
/// ([`V2Node::holds_slot`], by this tick's `evidence`). Returns the nodes
/// admitted; each is marked evacuating in `nodes`, so this tick seals it.
async fn admit<S: DrainV2Sink>(
    sink: &S,
    nodes: &mut [V2Node],
    max_evacuating: usize,
    evidence: &HashMap<NodeId, InstanceEvidence>,
) -> Vec<NodeId> {
    let evacuating = nodes
        .iter()
        .filter(|node| node.holds_slot(evidence.get(&node.id)))
        .count();
    // Only a node that heartbeats is sealed: a dead one would hold the slot
    // until EC2 says it is gone.
    let mut waiting: Vec<&mut V2Node> = nodes
        .iter_mut()
        .filter(|node| node.awaiting_slot() && node.heartbeat_fresh)
        .collect();
    waiting.sort_by_key(|node| node.epoch);
    let mut admitted = Vec::new();
    for node in waiting
        .into_iter()
        .take(max_evacuating.saturating_sub(evacuating))
    {
        let Some(epoch) = node.epoch else { continue };
        match sink.admit_evacuation(node.id, epoch).await {
            Ok(true) => {
                node.waiting = false;
                admitted.push(node.id);
            }
            Ok(false) => {}
            Err(error) => {
                sink.record_error(node.id, &format!("admitting the drain failed: {error}"))
                    .await;
            }
        }
    }
    admitted
}

/// Turn a gentle drain due to turn forced ([`V2Node::escalation_due`]).
/// Returns whether the drain still runs gently.
async fn escalate<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    if !node.escalation_due() {
        return node.gentle;
    }
    match sink.escalate(node.id, epoch).await {
        Ok(true) => {
            tracing::info!(
                node_id = %node.id,
                epoch,
                deadline = node.deadline,
                by_operator = node.mode_forced,
                "node_drain v2: the gentle drain turned forced"
            );
            false
        }
        // The row moved on: the next tick reads it again, and a call this
        // tick stays gentle, which suspends no running sandbox.
        Ok(false) => true,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("turning the gentle drain forced failed: {error}"),
            )
            .await;
            true
        }
    }
}

/// Ask the node, record what it reports, and advance a `draining` node that
/// is done. Returns whether the node moved to `drained`.
async fn drain<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    let cordon_error = sink
        .ensure_cordon(node.id)
        .await
        .err()
        .map(|error| format!("cordon convergence failed: {error}"));
    if let Some(error) = &cordon_error {
        sink.record_error(node.id, error).await;
    }
    let gentle = escalate(sink, node, epoch).await;
    let response = match sink.drain_node(node.id, drain_request(epoch, gentle)).await {
        Ok(response) => response,
        Err(error) => {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            let error = if gentle && error.refused_by_an_older_node() {
                format!(
                    "gentle DrainNode failed: {error}; the node's sandbox-manager, or its \
                     node-api relay, predates gentle drains and refused the call before it \
                     sealed anything: upgrade the node, or escalate the drain with mode forced"
                )
            } else if gentle {
                format!("gentle DrainNode failed: {error}")
            } else {
                format!("DrainNode failed: {error}")
            };
            sink.record_error(node.id, &error).await;
            return false;
        }
    };
    if gentle && response.gentle_epoch != Some(epoch) {
        // Only a node that drained gently under the epoch says so; one that
        // didn't may have suspended running sandboxes, and its report isn't
        // the gentle drain's.
        crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
        sink.record_error(
            node.id,
            &format!(
                "the node answered the gentle DrainNode under epoch {epoch} without \
                 acknowledging it (gentle_epoch {:?}); the drain records nothing from it",
                response.gentle_epoch
            ),
        )
        .await;
        return false;
    }
    let recording = record_evacuations(sink, node, &response).await;
    publish_report(node, &response, &recording);
    let all_recorded = recording.all_recorded();
    if !response.blocked.is_empty() {
        tracing::warn!(
            node_id = %node.id,
            blocked = response.blocked.len(),
            "node_drain v2: the node reports residents it can't make durable; the drain \
             can't end until they are resolved"
        );
    }
    match sink
        .record_report(node.id, epoch, report_json(&response, &recording))
        .await
    {
        // Storing the report clears the last error; a cordon that failed
        // this tick keeps its error until one succeeds.
        Ok(true) => {
            if let Some(error) = &cordon_error {
                sink.record_error(node.id, error).await;
            }
        }
        Ok(false) => return false,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("storing the drain report failed: {error}"),
            )
            .await;
            return false;
        }
    }
    if node.phase.as_deref() != Some("draining")
        || !response.done
        || response.sealed_epoch != Some(epoch)
        || !all_recorded
    {
        return false;
    }
    match sink.advance_drained(node.id, epoch).await {
        Ok(moved) => moved,
        Err(error) => {
            sink.record_error(node.id, &format!("advancing to drained failed: {error}"))
                .await;
            false
        }
    }
}

/// Cancel an undrained node's epoch and take back what the drain
/// evacuated: record each sandbox the node hands back, which enqueues its
/// restore, and have the node forget the ones recorded. Clear the v2 columns
/// once the node no longer holds the seal and has nothing left to hand back.
/// Returns whether the columns were cleared.
async fn cancel<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    let call = |forget: Vec<String>| async move {
        let answer = sink
            .drain_node(node.id, cancel_request(epoch, &forget))
            .await;
        if let Err(error) = &answer {
            crate::metrics::node_drain::v2_unanswered(&node.id.to_string());
            sink.record_error(node.id, &format!("DrainNode cancel failed: {error}"))
                .await;
        }
        answer.ok()
    };
    let Some(response) = call(Vec::new()).await else {
        return false;
    };
    if response.sealed_epoch == Some(epoch) {
        return false;
    }
    let recording = record_evacuations(sink, node, &response).await;
    let recorded: Vec<String> = recording
        .records
        .iter()
        .filter(|(_, record)| record.is_recorded())
        .map(|(sandbox_id, _)| sandbox_id.clone())
        .collect();
    let response = if recorded.is_empty() {
        response
    } else {
        let Some(response) = call(recorded).await else {
            return false;
        };
        response
    };
    match sink
        .record_report(node.id, epoch, report_json(&response, &recording))
        .await
    {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("storing the cancel's report failed: {error}"),
            )
            .await;
            return false;
        }
    }
    // An earlier node answers a cancel with none of these lists: it has
    // nothing to hand back.
    let handed_back =
        response.evacuated.is_empty() && response.pending.is_empty() && response.blocked.is_empty();
    if !handed_back {
        publish_report(node, &response, &recording);
        return false;
    }
    match sink.finish_cancel(node.id, epoch).await {
        Ok(cleared) => {
            if cleared {
                crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            }
            cleared
        }
        Err(error) => {
            sink.record_error(node.id, &format!("clearing the cancel failed: {error}"))
                .await;
            false
        }
    }
}

/// Release the durably suspended rows still naming a `drained` node.
async fn release<S: DrainV2Sink>(sink: &S, node_id: NodeId, epoch: u64) {
    match sink.release_suspended(node_id, epoch).await {
        Ok(released) if !released.is_empty() => tracing::info!(
            node_id = %node_id,
            released = released.len(),
            "node_drain v2: released durably suspended rows from the drained node"
        ),
        Ok(_) => {}
        Err(error) => {
            sink.record_error(
                node_id,
                &format!("releasing the durably suspended rows failed: {error}"),
            )
            .await;
        }
    }
}

/// Run the terminate transaction of an unfenced `terminating` node. Only
/// an authority that terminates fences: with none, the node waits for an
/// operator, who may still undrain it.
async fn fence<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node_id: NodeId,
    epoch: u64,
) -> Option<NodeStep> {
    if !authority.terminates() {
        sink.record_error(node_id, REFUSED_TERMINATE).await;
        return None;
    }
    match sink.fence(node_id, epoch).await {
        Ok(Fence::Fenced(instance_id)) => {
            tracing::info!(
                node_id = %node_id,
                instance_id,
                "node_drain v2: fenced the node's instance for termination"
            );
            Some(NodeStep::Fenced)
        }
        Ok(Fence::Held(_) | Fence::Moved) => None,
        Ok(Fence::NoInstance) => {
            sink.record_error(node_id, "the node records no EC2 instance to terminate")
                .await;
            None
        }
        Err(error) => {
            sink.record_error(
                node_id,
                &format!("the terminate transaction failed: {error}"),
            )
            .await;
            None
        }
    }
}

/// A fenced node, with EC2's answer for its fenced instance. The first
/// `terminated` answer is recorded; after it, an instance EC2 no longer
/// lists reads as terminated, since EC2 stops listing a terminated instance
/// after a while. Dispatch the termination while none is confirmed and EC2
/// lists the instance in a state other than `shutting-down` or
/// `terminated`; an instance it doesn't list, or no answer, dispatches
/// nothing. Once the instance reads terminated, delete the row if the
/// termination was confirmed and the Node is gone, or let the node go if it
/// wasn't. Every path that waits defers the next check.
async fn terminated_or_dispatch<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    node: &V2Node,
    epoch: u64,
    fence: &str,
    evidence: &InstanceEvidence,
    backoff: &DispatchBackoff,
) -> Option<NodeStep> {
    let now = Instant::now();
    if *evidence == InstanceEvidence::Terminated && node.terminated_observed_at.is_none() {
        match sink.record_terminated_observed(node.id, epoch, fence).await {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                backoff.defer(node.id, now);
                sink.record_error(
                    node.id,
                    &format!("recording that {fence} is terminated failed: {error}"),
                )
                .await;
                return None;
            }
        }
    }
    let terminated = match evidence {
        InstanceEvidence::Terminated => true,
        InstanceEvidence::NotListed => node.terminated_observed_at.is_some(),
        _ => false,
    };
    if !node.dispatched {
        if terminated {
            let trace = format!(
                "v2 drain let go: fenced instance {fence} terminated; dispatch unconfirmed"
            );
            return let_go(sink, node, epoch, &trace)
                .await
                .then_some(NodeStep::LetGo);
        }
        let InstanceEvidence::Listed(_) = evidence else {
            backoff.defer(node.id, now);
            let why = if *evidence == InstanceEvidence::NotListed {
                format!(
                    "EC2 does not list fenced instance {fence}, and was never seen to list it \
                     terminated; not dispatching its termination"
                )
            } else {
                format!(
                    "EC2 answers {evidence} for fenced instance {fence}; not dispatching its \
                     termination"
                )
            };
            sink.record_error(node.id, &why).await;
            return None;
        };
        backoff.defer(node.id, now);
        return match authority
            .terminate_instance(fence, node.decrement_capacity)
            .await
        {
            Ok(()) => match sink.record_dispatched(node.id, epoch, fence).await {
                Ok(recorded) => {
                    if recorded {
                        tracing::info!(
                            node_id = %node.id,
                            instance_id = fence,
                            "node_drain v2: Auto Scaling accepted the fenced instance's termination"
                        );
                    }
                    recorded.then_some(NodeStep::Dispatched)
                }
                Err(error) => {
                    sink.record_error(
                        node.id,
                        &format!("recording the dispatch of {fence} failed: {error}"),
                    )
                    .await;
                    None
                }
            },
            Err(error) => {
                crate::metrics::node_drain::v2_terminate_failure(&node.id.to_string());
                sink.record_error(
                    node.id,
                    &format!("terminating fenced instance {fence} failed: {error}"),
                )
                .await;
                None
            }
        };
    }
    if !terminated {
        backoff.defer(node.id, now);
        let elapsed = node
            .dispatched_at
            .map(|dispatched_at| unix_now().saturating_sub(dispatched_at));
        let bound = i64::try_from(TERMINATE_BOUND.as_secs()).unwrap_or(i64::MAX);
        if *evidence == InstanceEvidence::NotListed {
            sink.record_error(
                node.id,
                &format!(
                    "EC2 does not list fenced instance {fence}, and was never seen to list it \
                     terminated; not deleting the row"
                ),
            )
            .await;
        } else if let InstanceEvidence::Unknown(_) = evidence {
            // No answer says nothing about the machine, however long ago the
            // dispatch was: say so on every check, not only past the bound.
            // A row an earlier build's terminator acted on has no dispatch
            // time to report.
            let since = elapsed.map_or_else(String::new, |elapsed| {
                format!(" {} minutes ago", elapsed / 60)
            });
            sink.record_error(
                node.id,
                &format!(
                    "the termination of fenced instance {fence} was dispatched{since}; EC2 \
                     answers {evidence}, so the drain can't tell whether it is terminated; \
                     not deleting the row"
                ),
            )
            .await;
        } else if let Some(elapsed) = elapsed.filter(|elapsed| *elapsed >= bound) {
            sink.record_error(
                node.id,
                &format!(
                    "Auto Scaling accepted the termination of {fence}; EC2 still answers \
                     {evidence} after {} minutes",
                    elapsed / 60
                ),
            )
            .await;
        }
        return None;
    }
    let Some(node_name) = node.k8s_node_name.as_deref() else {
        backoff.defer(node.id, now);
        sink.record_error(node.id, "the node has no Kubernetes name to look up")
            .await;
        return None;
    };
    match sink.node_exists(node_name).await {
        Ok(false) => {}
        Ok(true) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!(
                    "fenced instance {fence} is terminated; waiting for its Kubernetes Node \
                     {node_name} to go"
                ),
            )
            .await;
            return None;
        }
        Err(error) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!("looking up the Kubernetes Node {node_name} failed: {error}"),
            )
            .await;
            return None;
        }
    }
    match sink.delete_terminated(node, epoch, fence).await {
        Ok(TerminatedDelete::Deleted) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_node_deleted();
            tracing::info!(
                node_id = %node.id,
                instance_id = fence,
                "node_drain v2: deleted the row of the node whose instance it terminated"
            );
            Some(NodeStep::Deleted)
        }
        Ok(TerminatedDelete::LetGo(trace)) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go("guard_refused");
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            Some(NodeStep::LetGo)
        }
        Ok(TerminatedDelete::Unfenced) => None,
        Ok(TerminatedDelete::Wait(why)) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!("fenced instance {fence} is terminated; not deleting the row yet: {why}"),
            )
            .await;
            None
        }
        Err(error) => {
            backoff.defer(node.id, now);
            sink.record_error(
                node.id,
                &format!("deleting the terminated node's row failed: {error}"),
            )
            .await;
            None
        }
    }
}

/// Let `node` go, recording `trace`. Returns whether the row was cleared.
async fn let_go<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64, trace: &str) -> bool {
    match sink.let_go(node, epoch, trace).await {
        Ok(true) => {
            crate::metrics::node_drain::clear_v2_gauges(&node.id.to_string());
            crate::metrics::node_drain::v2_let_go(if node.fenced().is_some() {
                "dispatch_unconfirmed"
            } else {
                "terminated_outside_the_drain"
            });
            tracing::warn!(node_id = %node.id, trace, "node_drain v2: let the node go");
            true
        }
        Ok(false) => false,
        Err(error) => {
            sink.record_error(node.id, &format!("letting the node go failed: {error}"))
                .await;
            false
        }
    }
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
