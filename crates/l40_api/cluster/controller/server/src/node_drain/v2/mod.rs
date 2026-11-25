//! The node drain's v2 loop.
//!
//! A v2 drain is a drain the admin route started with `engine: v2`
//! (`nodes.drain_engine = 'v2'`), which wrote the drain's generation and its
//! deadline in the transaction that moved the phase, before any node call.
//! The node does the work: sandbox-manager's `DrainNode` seals it and makes
//! every resident durable elsewhere, holding running sandboxes until a call
//! asks it to evacuate. Each call is level-triggered: it states the drain
//! and gets the node's state back, so the loop asks again on every tick and
//! keeps nothing of the node's between ticks. Per v2 node, each tick:
//!
//! 1. **Let-go first.** A node whose machine is gone some other way is let
//!    go: its heartbeat is stale, EC2 lists its instance `terminated` (now,
//!    or not at all once the drain recorded it `terminated`,
//!    `drain_status.terminated_observed_at`, which the loop records for
//!    every v2 row the first time EC2 says so), and its Kubernetes Node is
//!    gone or its `providerID` names that instance. EC2 not listing an
//!    instance it was never seen to list `terminated` isn't death. The loop
//!    clears the v2 columns and the phase under the node's lock and records
//!    why in `drain_last_error`; the node is then a plain crashed node to
//!    `lost_started_sandboxes` and `orphaned_sandboxes`, and the drain writes
//!    no row off.
//! 2. **Cancel pending** (a v2 engine with no phase, after `:undrain`): send
//!    `Cancel` under the generation, every tick until the node answers it.
//!    The node unseals and hands nothing back: a sandbox the drain suspended
//!    stays suspended until a call wakes it. Then lift the cordon, which
//!    `:undrain` leaves on a v2 node so nothing places on a node still
//!    sealed, and clear the engine and status, both under the row's lock and
//!    only while the row is still this generation's cancel. An `Aborted`
//!    call, an error or no answer is unanswered (`node_drain.v2.unanswered`),
//!    goes to `drain_last_error`, and is retried next tick, forever: giving
//!    up would uncordon a sealed node or orphan its seal.
//! 3. **Draining**: converge the cordon; a cordon refused because the Node
//!    under the name is another one than the drain's (its UID moved) gets no
//!    `DrainNode` call this tick. Call `DrainNode{generation, Drain{evacuate}}`,
//!    `evacuate` once the database clock has reached the deadline. An
//!    `Aborted` call is another call in progress on the node: the tick skips
//!    the node. An answer that isn't a drain report under the generation
//!    goes to `drain_last_error` and the loop records nothing from it.
//!    Otherwise record each evacuation the node reports: a suspended one
//!    once the row reads `suspended` and the catalog holds its snapshot
//!    ready, writing nothing (its next call restores it elsewhere). A v2
//!    drain doesn't move a stopped sandbox: the node reports one pending,
//!    and a stopped disk reported evacuated is never recorded, so the drain
//!    isn't finished. The loop writes no sandbox row while it drains. The
//!    report is stored in
//!    `drain_status`, each recorded evacuation is added to
//!    `evacuated_total`, and each pending entry the node reports running,
//!    but for a warm-pool member, to `observed_running`. Past its deadline, a drain whose evacuated set
//!    hasn't grown for the stall bound (15 minutes) is stalled, in
//!    `drain_last_error` and on the `node_drain.v2.stalled` gauge; the mark
//!    changes nothing else. Once the report has nothing pending or blocked
//!    and every evacuation is recorded, the node moves to `drained`.
//! 4. **Drained**: no sandbox-row write, and the node isn't called. Every
//!    suspended row still homed on the node whose snapshot is durable is
//!    added to `evacuated_total` (a v2 drain never moves a stopped sandbox,
//!    so none is counted), a `drain_status` write only: the node evicts a
//!    durable copy on its own, so one it evicted before a report named it
//!    evacuated is counted too.
//! 5. **Terminating** (`:terminate` moved it here, from `drained`): one fresh
//!    `DrainNode` call, whose report is recorded as a draining one's. Only
//!    an answer with nothing pending or blocked lets the loop fence, so a
//!    node that still holds a stopped sandbox, which it reports pending, is
//!    never fenced: the terminate transaction, under the node's row lock,
//!    cleans up the references of the rows that no longer need the node, the
//!    only sandbox-row writes the loop makes (the durable suspended rows
//!    naming it are released, and their executions retired:
//!    `pplx_space_sandbox_store::drain_terminate`), requires that nothing
//!    names the node (whose blockers go to `drain_last_error`), and records
//!    the instance to terminate, `node_k8s.instance_id`, as the fence
//!    (`drain_status.terminate_instance`). Any other outcome, a failed call
//!    (an unreadable seal among them) or a report that isn't empty, fences
//!    nothing: the node returns to `drained`, with why in
//!    `drain_last_error`, and the operator retries.
//!    Only the `aws` terminator fences. After the fence, the loop asks EC2
//!    about that instance on its dispatch backoff and, while EC2 lists it in
//!    a state other than `shutting-down` or `terminated` and no dispatch has
//!    succeeded, asks Auto Scaling to terminate exactly it, recording
//!    `dispatched_at` on success; it never dispatches after that, and an
//!    instance EC2 doesn't list is never dispatched. Once the instance reads
//!    terminated and a direct GET of the Kubernetes Node answers 404, the
//!    loop deletes the row while the heartbeat is stale and the row still
//!    carries the fence, a dispatch and the recorded `terminated`, with no
//!    tombstone ([`super::guarded_delete`]); a row or execution still naming
//!    the node lets it go instead.
//!
//! The admin route admits a v2 drain to one of the cluster's evacuation
//! slots when it starts it, or refuses it; this loop admits nothing. Each
//! tick counts, for every listed node, the sandboxes of its observed set
//! that are `suspended` and have been for longer than the bound
//! (`node_drain.v2.evacuated_still_suspended`): a drain restores nothing,
//! so a running sandbox it suspended waits for a call.
//!
//! Every v1 writer keyed on drain phase skips a v2 row, so this loop is the
//! only one that moves one. It keeps nothing between ticks but its tick
//! timer and [`V2LoopState`]: the dispatch backoff, where a lost backoff
//! only retries sooner, and the nodes it listed, so a node that leaves its
//! listing, however its row lost the drain, has its v2 gauges zeroed.

mod cleanup;
mod step;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_k8s_node_id::Ec2InstanceId;
use pplx_space_node_registration_store::drain_v2::StoredV2Report;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sbx_mgr_client::proto::{
    DrainEvacuated, DrainNodeRequest, DrainNodeResponse, drain_node_request,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use cleanup::{
    DispatchBackoff, REFUSED_TERMINATE, count_drained, fence, let_go, terminated_or_dispatch,
};
pub use cleanup::{Fence, TerminatedDelete};
use step::step;
#[cfg(test)]
use step::{OutstandingRows, Recording, outstanding_rows, stalled, stored_report};

/// The v2 loop's bounds. Each has a test-hooks override
/// ([`Self::from_environment`]) so Kind and tests can shrink it; a release
/// build reads the defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V2LoopConfig {
    /// How long past its deadline a drain may evacuate no further sandbox
    /// before the loop marks it stalled.
    pub stall_bound: Duration,
    /// How long a sandbox a drain saw running may stay suspended before
    /// `node_drain.v2.evacuated_still_suspended` counts it.
    pub still_suspended_after: Duration,
    /// How long EC2 may go on listing a fenced instance not `terminated`
    /// after Auto Scaling accepted its termination before the loop says so
    /// in `drain_last_error`.
    pub terminate_bound: Duration,
    /// The first wait between a fenced node's EC2 checks, which doubles up
    /// to [`DISPATCH_BACKOFF_CAP`].
    pub dispatch_backoff_base: Duration,
}

/// The longest wait between a fenced node's EC2 checks.
pub const DISPATCH_BACKOFF_CAP: Duration = Duration::from_secs(120);

/// The test-hooks overrides of [`V2LoopConfig`], each in whole seconds.
pub const STALL_BOUND_TEST_ENV: &str = "SPACE_DRAIN_V2_TEST_STALL_BOUND_SECONDS";
pub const STILL_SUSPENDED_AFTER_TEST_ENV: &str =
    "SPACE_DRAIN_V2_TEST_STILL_SUSPENDED_AFTER_SECONDS";
pub const TERMINATE_BOUND_TEST_ENV: &str = "SPACE_DRAIN_V2_TEST_TERMINATE_BOUND_SECONDS";
pub const DISPATCH_BACKOFF_BASE_TEST_ENV: &str =
    "SPACE_DRAIN_V2_TEST_DISPATCH_BACKOFF_BASE_SECONDS";

impl Default for V2LoopConfig {
    fn default() -> Self {
        Self {
            stall_bound: Duration::from_secs(15 * 60),
            still_suspended_after: Duration::from_secs(15 * 60),
            terminate_bound: Duration::from_secs(15 * 60),
            dispatch_backoff_base: Duration::from_secs(10),
        }
    }
}

impl V2LoopConfig {
    /// The defaults, each overridden by its test hook when the build has
    /// `test-hooks` and the hook names a positive number of seconds. A base
    /// above [`DISPATCH_BACKOFF_CAP`] is the cap.
    pub fn from_environment(env: &dyn pplx_space_env_vars::Environment) -> Self {
        let env = crate::env_config::EnvConfig(env);
        let defaults = Self::default();
        Self {
            stall_bound: env.test_duration_secs(STALL_BOUND_TEST_ENV, defaults.stall_bound),
            still_suspended_after: env.test_duration_secs(
                STILL_SUSPENDED_AFTER_TEST_ENV,
                defaults.still_suspended_after,
            ),
            terminate_bound: env
                .test_duration_secs(TERMINATE_BOUND_TEST_ENV, defaults.terminate_bound),
            dispatch_backoff_base: env
                .test_duration_secs(
                    DISPATCH_BACKOFF_BASE_TEST_ENV,
                    defaults.dispatch_backoff_base,
                )
                .min(DISPATCH_BACKOFF_CAP),
        }
    }
}

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
    /// `drain_status.generation`; the admin route writes it with the phase.
    pub generation: Option<u64>,
    /// `drain_status.deadline_at`, in Unix seconds: until when the drain
    /// holds running sandboxes.
    pub deadline_at: Option<i64>,
    /// The database clock had reached the deadline when the tick listed the
    /// node: every `DrainNode` call asks the node to evacuate.
    pub evacuate: bool,
    /// The last report stored for the generation, typed; `None` before the
    /// first, and for one that doesn't read.
    pub report: Option<StoredV2Report>,
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
    /// terminate transaction chose to terminate.
    pub fence: Option<String>,
    /// `drain_status.dispatched_at`, in Unix seconds: when Auto Scaling
    /// accepted the fenced instance's termination.
    pub dispatched_at: Option<i64>,
    /// `drain_status.terminated_observed_at`, in Unix seconds: when the loop
    /// first saw EC2 list the drain's instance `terminated`.
    pub terminated_observed_at: Option<i64>,
    /// `nodes.terminate_decrement_capacity`, which the termination passes on.
    pub decrement_capacity: bool,
    /// The database clock when the tick listed the node, in Unix seconds: the
    /// clock of the drain's deadline, of the stored report's stamps, and of
    /// the stall rules.
    pub db_now: i64,
}

impl V2Node {
    /// Since when, in Unix seconds, each evacuation the last stored report
    /// names unrecorded has been.
    fn unrecorded_since(&self) -> BTreeMap<&str, i64> {
        self.report
            .iter()
            .flat_map(|report| &report.unrecorded)
            .map(|entry| (entry.sandbox_id.as_str(), entry.since))
            .collect()
    }

    /// The sandboxes the last stored report lists evacuated, and the last
    /// time that set grew (`evacuated_grew_at`); `None` before the first
    /// report.
    fn stored_evacuated(&self) -> Option<(BTreeSet<&str>, i64)> {
        self.report.as_ref().map(|report| {
            (
                report
                    .evacuated
                    .iter()
                    .map(|entry| entry.sandbox_id.as_str())
                    .collect(),
                report.evacuated_grew_at,
            )
        })
    }
}

/// The `DrainNode` request of a drain call under `generation`: hold running
/// sandboxes, or, with `evacuate`, suspend them too.
pub fn drain_request(generation: u64, evacuate: bool) -> DrainNodeRequest {
    DrainNodeRequest {
        generation,
        op: Some(drain_node_request::Op::Drain(drain_node_request::Drain {
            evacuate,
        })),
    }
}

/// The `DrainNode` request that cancels `generation`.
pub fn cancel_request(generation: u64) -> DrainNodeRequest {
    DrainNodeRequest {
        generation,
        op: Some(drain_node_request::Op::Cancel(
            drain_node_request::Cancel {},
        )),
    }
}

/// What the loop keeps between ticks: its [`V2LoopConfig`], the
/// [`DispatchBackoff`], and the nodes its last listing returned. In memory
/// only: a controller that restarts starts with no listing.
#[derive(Debug)]
pub struct V2LoopState {
    config: V2LoopConfig,
    backoff: DispatchBackoff,
    listed: Mutex<HashSet<NodeId>>,
}

impl Default for V2LoopState {
    fn default() -> Self {
        Self::new(V2LoopConfig::default())
    }
}

impl V2LoopState {
    pub fn new(config: V2LoopConfig) -> Self {
        Self {
            config,
            backoff: DispatchBackoff::new(config.dispatch_backoff_base),
            listed: Mutex::default(),
        }
    }

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
    /// Nothing more to do for it: its row reads `suspended` on the reported
    /// snapshot, or the sandbox has moved on.
    Recorded,
    /// Not yet, and why: the catalog doesn't hold the artifact as ready, the
    /// node's suspend isn't projected onto the row yet, or the row can't
    /// take the report, a stopped sandbox's disk among them.
    NotYet(String),
}

impl EvacuationRecord {
    pub const fn is_recorded(&self) -> bool {
        matches!(self, Self::Recorded)
    }
}

/// Why a `DrainNode` call failed, as the loop records it: the gRPC status the
/// node, or its node-api relay, answered with, or a failure before any
/// answer (the endpoint lookup, the connection).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainCallError {
    text: String,
    aborted: bool,
}

impl DrainCallError {
    /// The status the call answered with.
    pub fn status(status: &tonic::Status) -> Self {
        Self {
            text: status.to_string(),
            aborted: status.code() == tonic::Code::Aborted,
        }
    }

    /// A failure before the node answered.
    pub fn other(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            aborted: false,
        }
    }

    /// The node refused the call because another `DrainNode` call was in
    /// progress on it (`Aborted`): it changed nothing.
    pub const fn aborted(&self) -> bool {
        self.aborted
    }
}

impl std::fmt::Display for DrainCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// Why the cordon of a v2 drain's node didn't converge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CordonFailure {
    /// The Kubernetes Node under the drain's node name is another one than
    /// the drain's: its UID isn't the one registration recorded. The node
    /// isn't called this tick.
    UidMismatch(String),
    /// Anything else.
    Other(String),
}

impl std::fmt::Display for CordonFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UidMismatch(text) | Self::Other(text) => f.write_str(text),
        }
    }
}

/// What the API server says of a node's machine ([`DrainV2Sink::node_machine`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeMachine {
    /// The Node is gone.
    Gone,
    /// The Node is live, on the EC2 instance its AWS `providerID` names, or
    /// on none it names.
    Live(Option<String>),
}

/// What the v2 loop reads and writes.
pub trait DrainV2Sink: Send + Sync + 'static {
    fn list_v2_nodes(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<V2Node>, String>> + Send;
    /// Converge the node's cordon, as the v1 drain does.
    fn ensure_cordon(
        &self,
        node_id: NodeId,
    ) -> impl std::future::Future<Output = Result<(), CordonFailure>> + Send;
    /// Call the node's `DrainNode` with `request` ([`drain_request`],
    /// [`cancel_request`]).
    fn drain_node(
        &self,
        node_id: NodeId,
        request: DrainNodeRequest,
    ) -> impl std::future::Future<Output = Result<DrainNodeResponse, DrainCallError>> + Send;
    fn record_evacuation(
        &self,
        node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> impl std::future::Future<Output = Result<EvacuationRecord, String>> + Send;
    /// Store `report` under `generation`, add `running`, the sandboxes it
    /// lists running, to `drain_status.observed_running`, and `evacuated`,
    /// the evacuations recorded, to `drain_status.evacuated_total`; both
    /// only grow. False when the row's generation moved on.
    fn record_report(
        &self,
        node_id: NodeId,
        generation: u64,
        report: StoredV2Report,
        running: Vec<String>,
        evacuated: Vec<String>,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// `draining -> drained` under `generation`, with a stored report for it
    /// that is finished.
    fn advance_drained(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// `terminating -> drained` under `generation`, unfenced, recording
    /// `refused` in `drain_last_error`.
    fn return_drained(
        &self,
        node_id: NodeId,
        generation: u64,
        refused: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Lift the node's cordon and clear an undrained row's v2 columns once
    /// its generation is cancelled on the node, both under the row's lock
    /// and only while the row is still that generation's cancel, so a drain
    /// of the node started meanwhile, which takes that lock, cordons after
    /// it. False, with nothing done, when the row is no longer that
    /// generation's cancel.
    fn finish_cancel(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// For each v2 node, how many of the sandboxes its drain has seen
    /// running (`drain_status.observed_running`) are `suspended`, their
    /// lifecycle unchanged for longer than `after`.
    fn evacuated_still_suspended(
        &self,
        after: Duration,
    ) -> impl std::future::Future<Output = Result<HashMap<NodeId, u64>, String>> + Send;
    fn record_error(
        &self,
        node_id: NodeId,
        message: &str,
    ) -> impl std::future::Future<Output = ()> + Send;
    /// Add every durably suspended row still homed on the `drained` node
    /// under `generation` to its drain's `evacuated_total`, under the node's
    /// row lock
    /// (`pplx_space_sandbox_store::drain_terminate::durable_suspended_rows_naming`):
    /// a `drain_status` write only, no sandbox row. The ids it found.
    fn count_drained_evacuations(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, String>> + Send;
    /// The terminate transaction, under the node's row lock: re-check the
    /// generation's unfenced `terminating` node, release its durable
    /// suspended rows, require that nothing names the node, and write the
    /// fence.
    fn fence(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> impl std::future::Future<Output = Result<Fence, String>> + Send;
    /// Record that Auto Scaling accepted the termination of the fenced
    /// `instance_id`; false when the row no longer carries that fence
    /// without a dispatch.
    fn record_dispatched(
        &self,
        node_id: NodeId,
        generation: u64,
        instance_id: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Record that EC2 listed `instance_id`, the instance the drain asks
    /// about (its fence, else its recorded instance), `terminated`
    /// (`drain_status.terminated_observed_at`), keeping the first time it
    /// did; false when the row is no longer that generation's v2 drain
    /// asking about that instance.
    fn record_terminated_observed(
        &self,
        node_id: NodeId,
        generation: u64,
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
        generation: u64,
        fence: &str,
    ) -> impl std::future::Future<Output = Result<TerminatedDelete, String>> + Send;
    /// Let go of `node` under `generation`, in the phase and with the fence
    /// the tick read: clear its phase and v2 columns under its row lock and
    /// record `trace` as its last error. False when the row moved on.
    fn let_go(
        &self,
        node: &V2Node,
        generation: u64,
        trace: &str,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
}

/// What one tick did, for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct V2TickReport {
    pub drained: Vec<NodeId>,
    pub cancelled: Vec<NodeId>,
    /// Nodes whose terminate transaction wrote the fence.
    pub fenced: Vec<NodeId>,
    /// Nodes whose terminating tick's fresh check refused the fence, which
    /// returned to `drained`.
    pub returned: Vec<NodeId>,
    /// Nodes whose fenced instance's termination Auto Scaling accepted.
    pub dispatched: Vec<NodeId>,
    /// Nodes whose row the loop deleted.
    pub deleted: Vec<NodeId>,
    /// Nodes the loop let go.
    pub let_go: Vec<NodeId>,
}

/// How many v2 nodes one tick calls at once: each call waits on its node,
/// and a slow node must not hold up the rest.
const NODE_CONCURRENCY: usize = 8;

/// What one tick did to a node.
enum NodeStep {
    Cancelled,
    Drained,
    Fenced,
    Returned,
    Dispatched,
    Deleted,
    LetGo,
}

/// One pass over every v2 node, [`NODE_CONCURRENCY`] at a time. A node the
/// last tick listed and this one doesn't has its v2 gauges zeroed. EC2 is
/// asked first, about each fenced node that the backoff lets ask and each
/// other node whose heartbeat is stale; each node's step reads its answer.
/// Then the sandboxes each listed node's drain left suspended longer than
/// the config's bound are counted.
pub async fn tick<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    state: &V2LoopState,
) -> V2TickReport {
    use futures_util::StreamExt as _;

    let mut report = V2TickReport::default();
    let nodes = match sink.list_v2_nodes().await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::warn!(%error, "node_drain v2: listing v2 nodes failed");
            return report;
        }
    };
    for node_id in state.relist(nodes.iter().map(|node| node.id).collect()) {
        crate::metrics::node_drain::clear_v2_gauges(&node_id.to_string());
    }
    let listed: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
    let backoff = &state.backoff;
    let config = &state.config;
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
        .filter(|node| node.generation.is_some())
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
        .buffer_unordered(NODE_CONCURRENCY)
        .collect()
        .await;
    let steps: Vec<_> = futures_util::stream::iter(nodes.into_iter().map(|node| {
        let evidence = evidence.get(&node.id).cloned();
        (node, evidence)
    }))
    .map(|(node, evidence)| async move {
        (
            node.id,
            step(sink, authority, &node, evidence.as_ref(), backoff, config).await,
        )
    })
    .buffer_unordered(NODE_CONCURRENCY)
    .collect()
    .await;
    for (node_id, step) in steps {
        match step {
            Some(NodeStep::Cancelled) => report.cancelled.push(node_id),
            Some(NodeStep::Drained) => report.drained.push(node_id),
            Some(NodeStep::Fenced) => report.fenced.push(node_id),
            Some(NodeStep::Returned) => report.returned.push(node_id),
            Some(NodeStep::Dispatched) => report.dispatched.push(node_id),
            Some(NodeStep::Deleted) => report.deleted.push(node_id),
            Some(NodeStep::LetGo) => report.let_go.push(node_id),
            None => {}
        }
    }
    publish_still_suspended(sink, &listed, config.still_suspended_after).await;
    report
}

/// Count, for each listed node, the sandboxes its drain left suspended
/// longer than `after` ([`DrainV2Sink::evacuated_still_suspended`]); a
/// listed node with none reads zero. A count that fails leaves the gauges
/// as they were. With no node listed there is no gauge to publish, so
/// nothing is counted: with drain v2 off, which lists none, a tick runs
/// only its list.
async fn publish_still_suspended<S: DrainV2Sink>(sink: &S, listed: &[NodeId], after: Duration) {
    if listed.is_empty() {
        return;
    }
    match sink.evacuated_still_suspended(after).await {
        Ok(counts) => {
            for node_id in listed {
                crate::metrics::node_drain::v2_evacuated_still_suspended(
                    &node_id.to_string(),
                    counts.get(node_id).copied().unwrap_or(0),
                );
            }
        }
        Err(error) => tracing::warn!(
            %error,
            "node_drain v2: counting the sandboxes drains left suspended failed"
        ),
    }
}

/// Run the v2 loop every `tick_every` until `shutdown`, under `config`'s
/// bounds.
pub async fn run<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: S,
    authority: A,
    tick_every: Duration,
    config: V2LoopConfig,
    shutdown: CancellationToken,
) {
    tracing::info!(
        tick_secs = tick_every.as_secs(),
        ?config,
        "starting the node drain's v2 loop"
    );
    let state = V2LoopState::new(config);
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {}
        }
        let report = tick(&sink, &authority, &state).await;
        if report != V2TickReport::default() {
            tracing::info!(?report, "node_drain v2: tick");
        }
    }
    tracing::info!("node drain's v2 loop stopping");
}

#[cfg(test)]
mod tests;
