//! The node drain's v2 loop.
//!
//! A v2 drain is a drain the admin route started with `engine: v2`
//! (`nodes.drain_engine = 'v2'`), which wrote the drain's generation and its
//! deadline in the transaction that moved the phase, before any node call.
//! The node does the work: sandbox-manager's `DrainNode` seals it and makes
//! every resident durable elsewhere, holding running sandboxes until a call
//! asks it to evacuate. Each call is level-triggered: it states the drain
//! and gets the node's state back, so the loop asks again on every tick and
//! keeps nothing of the node's between ticks.
//!
//! The tick lists each node once and may act on it later. Every effect on
//! the node, a cordon, its lift and each `DrainNode` call, is bound to the
//! node as listed: just before it, one statement re-reads that the row is
//! still a v2 drain in the listed phase, under the listed generation, on
//! the listed Kubernetes UID and recorded instance, and a row that moved on
//! (an undrain or another phase change, a registration that took it for
//! another machine, or a cleared drain) is skipped for the tick, with no
//! effect and nothing recorded; the next listing decides. A
//! cordon or its lift patches the Node under the name only while its UID is
//! the listed one, and names that UID in the patch, which the API server
//! refuses once the Node under the name is another one.
//!
//! Each node's step decides one outcome for the tick: what moved, the status
//! its row shows afterwards (`drain_last_error`: what it waits for or what
//! failed, or nothing to report) and what its report gauges read. One
//! writer applies it: it writes the status only when it differs from what
//! the listing read, once, under the listed generation, logs at WARN only
//! when it changes, and publishes the gauges of every report the tick
//! stored, draining or terminating, and an unanswered call on both paths.
//! A guarded transition that records a status with its move (the terminate
//! transaction's fence or what holds it, the dispatch, the return to
//! `drained`, the let-go's trace) keeps it: the outcome leaves it as it is.
//!
//! Per v2 node, each tick:
//!
//! 1. **Let-go first.** A node whose machine is gone some other way is let
//!    go: its heartbeat is stale, EC2 lists its instance `terminated` (now,
//!    or not at all once the drain recorded it `terminated`,
//!    `drain_status.terminated_observed_at`, which the loop records for
//!    every v2 row the first time EC2 says so, with the instance it is
//!    about, `drain_status.terminated_observed_instance`: a record about
//!    another instance doesn't count, and one without an instance counts
//!    for whichever instance the drain asks about), and its Kubernetes Node is
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
//!    sealed, and once it is lifted clear the engine, `drain_status` and
//!    the status (`drain_last_error`), only while
//!    the row is still this generation's cancel. No row lock spans the
//!    lift. While one controller runs ticks, the API refuses a `:drain`
//!    until the clear lands, so no drain of the node starts and cordons in
//!    between; a lift from an outgoing controller's last tick can still land
//!    after a new drain's cordon, which that drain's next tick restores.
//!    An `Aborted` call, an error or no answer is unanswered
//!    (`node_drain.v2.unanswered`),
//!    goes to `drain_last_error`, and is retried next tick, forever: giving
//!    up would uncordon a sealed node or orphan its seal.
//! 3. **Draining**: converge the cordon; a cordon refused because the Node
//!    under the name is another one than the drain's (its UID moved, or the
//!    API server refused the patch naming it) gets no `DrainNode` call this
//!    tick. Call `DrainNode{generation, Drain{evacuate}}`,
//!    `evacuate` once the database clock has reached the deadline. An
//!    `Aborted` call is another call in progress on the node: the tick skips
//!    the node. An answer that isn't a drain report under the generation
//!    goes to `drain_last_error` and the loop records nothing from it.
//!    Otherwise record each evacuation the node reports: a suspended one
//!    once the row reads `suspended` and the catalog holds its snapshot
//!    ready, writing nothing (its next call restores it elsewhere). One
//!    default-deny answer decides it
//!    (`pplx_space_sandbox_store::drain_terminate::evacuation_verdict`): a
//!    row it doesn't release by name, one in a lifecycle that isn't settled,
//!    not homed on another node, among them, isn't recorded. A v2
//!    drain doesn't move a stopped sandbox: the node reports one pending,
//!    and a stopped disk reported evacuated is never recorded, so the drain
//!    isn't finished. The loop writes no sandbox row while it drains. The
//!    report is stored in
//!    `drain_status`, each recorded evacuation is added to
//!    `evacuated_total`, and each pending entry the node reports running,
//!    but for a warm-pool member, to `observed_running`. A drain the loop
//!    asks to evacuate whose evacuated set hasn't grown for the stall bound
//!    (15 minutes), counted from the later of its last growth and the
//!    listing time of the generation's first evacuating tick whose report
//!    the loop stored (`report.evacuate_asked_at`; a failed or aborted
//!    evacuating call stamps nothing, and a report without it counts from
//!    the next evacuating tick), is stalled, in `drain_last_error` and on
//!    the `node_drain.v2.stalled` gauge; the mark changes nothing else. A
//!    hold shortened into the past thus doesn't stall at once. Once the
//!    stored report is finished (nothing pending, nothing blocked, every
//!    evacuation recorded: `StoredV2Report::finished`, the one rule), the
//!    node moves to `drained`.
//! 4. **Drained**: no sandbox-row write, and the node isn't called. Every
//!    suspended row still naming the node that the terminate would
//!    release (its snapshot durable, no restore or activation of it in
//!    flight) is added to `evacuated_total` (a v2 drain never moves a
//!    stopped sandbox, so none is counted), a `drain_status` write only,
//!    which re-checks the phase instead of locking the row: the node evicts
//!    a durable copy on its own, so one it evicted before a report named it
//!    evacuated is counted too. A tick whose cordon and count succeed has
//!    nothing to report, and clears an earlier failure; why a terminate
//!    returned the node here stays until the next `:terminate` or
//!    `:undrain`, through the tick's own failures (a paced warning then
//!    says what failed), unless the loop finds the node's machine
//!    terminated and waits on it. The report gauges keep the refused
//!    report's rows, with the node neither stalled nor unanswered.
//! 5. **Terminating** (`:terminate` moved it here, from `drained`): one fresh
//!    `DrainNode` call, whose report is recorded as a draining one's. Only
//!    an answer with nothing pending or blocked lets the loop fence, so a
//!    node that still holds a stopped sandbox, which it reports pending, is
//!    never fenced: the terminate transaction, under the node's row lock,
//!    cleans up the references of the rows that no longer need the node, the
//!    only sandbox-row writes the loop makes (the durable suspended rows
//!    naming it are released, their executions retired, and counted in
//!    `evacuated_total`: `pplx_space_sandbox_store::drain_terminate`),
//!    requires that nothing
//!    names the node (whose blockers go to `drain_last_error`), and records
//!    the instance to terminate, `node_k8s.instance_id`, as the fence
//!    (`drain_status.terminate_instance`). Any other outcome, a failed call
//!    (an unreadable seal among them) or a report that isn't finished, fences
//!    nothing: the node returns to `drained`, with why in
//!    `drain_last_error`, and the operator retries. So does a node that
//!    records no instance to terminate, before any call when the listing
//!    shows none, else from the terminate transaction. A call refused
//!    because another `DrainNode` call is in progress on the node
//!    (`Aborted`) changed nothing: the node stays `terminating`, its status
//!    saying so, and the next tick checks again.
//!    Only the `aws` terminator fences. After the fence, the loop asks EC2
//!    about that instance on its dispatch backoff (one EC2 call per tick
//!    asks about every instance the tick asks about) and, while EC2 lists it in
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
//! only retries sooner, the nodes it listed, so a node that leaves its
//! listing, however its row lost the drain, has its v2 gauges zeroed, and
//! the pacer of its warnings about a node's node-wide problems. Once
//! shutdown is requested, no tick starts and a tick takes no new node. The
//! steps already running go on until the controller's shutdown grace
//! (`SHUTDOWN_DRAIN_TIMEOUT`, 10 s) ends; the process then exits and drops
//! any still running, so a dispatch in flight then has an unknown outcome,
//! as a timed-out one does.

mod cleanup;
mod step;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
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
    DispatchBackoff, NO_INSTANCE_TO_TERMINATE, NOT_AWS_TERMINATOR, TERMINATE_REFUSED,
    count_drained, fence, let_go, return_drained, terminated_or_dispatch,
};
pub use cleanup::{Fence, TerminatedDelete};
use step::{OutstandingRows, step};
#[cfg(test)]
use step::{
    Recording, TERMINATE_WAITS_FOR_A_CALL, evacuated_grew_at, outstanding_rows, stalled,
    stored_report,
};

use crate::warn_pacer::PerNodeWarnPacer;

/// The v2 loop's bounds. Each has a test-hooks override
/// ([`Self::from_environment`]) so Kind and tests can shrink it; a release
/// build reads the defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V2LoopConfig {
    /// How long, counted from the stall stamp (`evacuated_grew_at`), a
    /// drain may evacuate no further sandbox before the loop marks it
    /// stalled.
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

    /// EC2's answer for each of `instance_ids`, in one call: every id asked
    /// has an answer, and an error or a timeout is
    /// [`InstanceEvidence::Unknown`] for every one.
    fn describe_instances(
        &self,
        instance_ids: &[String],
    ) -> impl std::future::Future<Output = HashMap<String, InstanceEvidence>> + Send;

    /// Ask Auto Scaling to terminate exactly `instance_id`.
    fn terminate_instance(
        &self,
        instance_id: &str,
        decrement_capacity: bool,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// EC2's answer for each of `asked`, from the instances one call listed: an
/// id listed under none of them is [`InstanceEvidence::NotListed`].
pub fn evidence_by_id(
    asked: &[&str],
    listed: Vec<pplx_space_aws_client::ec2::Instance>,
) -> HashMap<String, InstanceEvidence> {
    let mut states: HashMap<String, Vec<String>> = asked
        .iter()
        .map(|instance_id| ((*instance_id).to_owned(), Vec::new()))
        .collect();
    for instance in listed {
        if let Some(listed) = states.get_mut(&instance.instance_id) {
            listed.push(instance.state);
        }
    }
    states
        .into_iter()
        .map(|(instance_id, states)| (instance_id, InstanceEvidence::from_states(&states)))
        .collect()
}

/// [`InstanceEvidence::Unknown`] for each of `instance_ids`, with `why`.
fn unknown_for<'a>(
    instance_ids: impl IntoIterator<Item = &'a str>,
    why: &str,
) -> HashMap<String, InstanceEvidence> {
    instance_ids
        .into_iter()
        .map(|instance_id| {
            (
                instance_id.to_owned(),
                InstanceEvidence::Unknown(why.to_owned()),
            )
        })
        .collect()
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

    async fn describe_instances(
        &self,
        instance_ids: &[String],
    ) -> HashMap<String, InstanceEvidence> {
        let Self::Ec2 { ec2, .. } = self else {
            return unknown_for(
                instance_ids.iter().map(String::as_str),
                "the terminator is not aws",
            );
        };
        let (asked, malformed): (Vec<&str>, Vec<&str>) = instance_ids
            .iter()
            .map(String::as_str)
            .partition(|instance_id| Ec2InstanceId::parse(instance_id).is_some());
        let mut answers: HashMap<String, InstanceEvidence> = malformed
            .into_iter()
            .map(|instance_id| {
                (
                    instance_id.to_owned(),
                    InstanceEvidence::Unknown(format!("{instance_id} is not an instance id")),
                )
            })
            .collect();
        if asked.is_empty() {
            return answers;
        }
        // The client asks with an `instance-id` filter, so an id EC2 doesn't
        // know is left out of the answer rather than failing the call.
        answers.extend(
            match tokio::time::timeout(AWS_CALL_TIMEOUT, ec2.describe_instances_by_ids(&asked))
                .await
            {
                Ok(Ok(instances)) => evidence_by_id(&asked, instances),
                Ok(Err(error)) => unknown_for(asked, &error.to_string()),
                Err(_) => unknown_for(asked, "the EC2 call timed out"),
            },
        );
        answers
    }

    async fn terminate_instance(
        &self,
        instance_id: &str,
        decrement_capacity: bool,
    ) -> Result<(), String> {
        let Self::Ec2 { autoscaling, .. } = self else {
            return Err(NOT_AWS_TERMINATOR.to_owned());
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
    /// `nodes.drain_last_error`, the status the row showed when the tick
    /// listed it.
    pub last_error: Option<String>,
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
/// [`DispatchBackoff`], the nodes its last listing returned, and its warning
/// pacer; and the shutdown token its ticks watch. In memory only: a
/// controller that restarts starts with no listing.
pub struct V2LoopState {
    config: V2LoopConfig,
    backoff: DispatchBackoff,
    listed: Mutex<HashSet<NodeId>>,
    warn_pacer: PerNodeWarnPacer,
    shutdown: CancellationToken,
}

impl std::fmt::Debug for V2LoopState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("V2LoopState")
            .field("config", &self.config)
            .field("backoff", &self.backoff)
            .field("listed", &self.listed)
            .finish_non_exhaustive()
    }
}

/// How often the loop repeats a warning about a node while it lasts: a
/// report that lists node-wide problems, a drain with no generation, a
/// phase this build doesn't know.
const BLOCKED_WARN_INTERVAL: Duration = Duration::from_secs(10 * 60);

impl Default for V2LoopState {
    fn default() -> Self {
        Self::new(V2LoopConfig::default())
    }
}

impl V2LoopState {
    pub fn new(config: V2LoopConfig) -> Self {
        Self::with_shutdown(config, CancellationToken::new())
    }

    /// A state whose ticks take no new node once `shutdown` is cancelled.
    pub fn with_shutdown(config: V2LoopConfig, shutdown: CancellationToken) -> Self {
        Self {
            config,
            backoff: DispatchBackoff::new(config.dispatch_backoff_base),
            listed: Mutex::default(),
            warn_pacer: PerNodeWarnPacer::new(BLOCKED_WARN_INTERVAL),
            shutdown,
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
    moved: bool,
}

impl DrainCallError {
    /// The status the call answered with.
    pub fn status(status: &tonic::Status) -> Self {
        Self {
            text: status.to_string(),
            aborted: status.code() == tonic::Code::Aborted,
            moved: false,
        }
    }

    /// A failure before the node answered.
    pub fn other(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            aborted: false,
            moved: false,
        }
    }

    /// No call was made: the row is no longer the drain the tick listed
    /// ([`DrainV2Sink::drain_node`]).
    pub fn moved() -> Self {
        Self {
            text: "the node's row is no longer the drain the tick listed".to_owned(),
            aborted: false,
            moved: true,
        }
    }

    /// The call wasn't made because the row moved on since the tick listed
    /// it: the node is skipped this tick, with nothing recorded.
    pub const fn is_moved(&self) -> bool {
        self.moved
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
    /// the drain's: its UID isn't the one the tick listed, or the API server
    /// refused the patch that named that UID. The node isn't called this
    /// tick.
    UidMismatch(String),
    /// Nothing was patched: the row is no longer the drain the tick listed.
    /// The node is skipped this tick, with nothing recorded.
    Moved,
    /// Anything else.
    Other(String),
}

impl std::fmt::Display for CordonFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UidMismatch(text) | Self::Other(text) => f.write_str(text),
            Self::Moved => f.write_str("the node's row is no longer the drain the tick listed"),
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
    /// Cordon `node`, the node as the tick listed it: first re-read that its
    /// row is still that drain, in the listed phase, under the listed
    /// generation, on the listed Kubernetes UID and recorded instance
    /// ([`CordonFailure::Moved`] if not), then patch the Node under its name only while its UID is the
    /// listed one ([`CordonFailure::UidMismatch`] if not). A Node that is
    /// gone has no cordon to converge.
    fn ensure_cordon(
        &self,
        node: &V2Node,
    ) -> impl std::future::Future<Output = Result<(), CordonFailure>> + Send;
    /// Call the `DrainNode` of `node`, the node as the tick listed it, with
    /// `request` ([`drain_request`], [`cancel_request`]), once its row still
    /// reads as listed, as [`Self::ensure_cordon`] re-reads it; else make no
    /// call ([`DrainCallError::moved`]).
    fn drain_node(
        &self,
        node: &V2Node,
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
    /// Once `node`'s listed generation is cancelled on the node, lift its
    /// cordon, then clear the undrained row's v2 columns, with no row lock
    /// across the Kubernetes calls: the lift only while the row still reads
    /// as the tick listed it and the Node under the name has the listed
    /// UID, the clear only after a lift that succeeded, and only while the
    /// row is still that generation's cancel. While one controller runs
    /// ticks, no drain of the node can start meanwhile: the API refuses a
    /// `:drain` while the cancel is pending. False, with nothing cleared,
    /// when the row is no longer that generation's cancel, or was listed
    /// with no generation.
    fn finish_cancel(
        &self,
        node: &V2Node,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// For each v2 node, how many of the sandboxes its drain has seen
    /// running (`drain_status.observed_running`) are `suspended`, their
    /// lifecycle unchanged for longer than `after`.
    fn evacuated_still_suspended(
        &self,
        after: Duration,
    ) -> impl std::future::Future<Output = Result<HashMap<NodeId, u64>, String>> + Send;
    /// Make `drain_last_error` of the v2 drain under `generation` read
    /// `status` (`None` clears it), the status the tick decided on; only on
    /// a row that is still that generation's v2 drain and doesn't read it
    /// already. Whether it wrote.
    fn write_status(
        &self,
        node_id: NodeId,
        generation: u64,
        status: Option<&str>,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;
    /// Add every row still naming the `drained` node under `generation`
    /// that the terminate would release to its drain's `evacuated_total`
    /// (`pplx_space_sandbox_store::drain_terminate::releasable_suspended_rows_naming`),
    /// with no row lock: the write re-checks that the row is still that
    /// generation's `drained` node. A `drain_status` write only, no sandbox
    /// row. The ids it found.
    fn count_drained_evacuations(
        &self,
        node_id: NodeId,
        generation: u64,
    ) -> impl std::future::Future<Output = Result<Vec<SandboxId>, String>> + Send;
    /// The terminate transaction, under the node's row lock: re-check the
    /// generation's unfenced `terminating` node, release its durable
    /// suspended rows and count them in `evacuated_total`, require that
    /// nothing names the node, and write the fence.
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
    /// about (its fence, else its recorded instance), `terminated`, with the
    /// instance (`drain_status.terminated_observed_at`,
    /// `drain_status.terminated_observed_instance`): keeps the first time of
    /// a record about that instance, or of one that names none, and replaces
    /// one about another instance; false when the row is no longer that
    /// generation's v2 drain asking about that instance.
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

/// What one tick moved for a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeStep {
    Cancelled,
    Drained,
    Fenced,
    Returned,
    Dispatched,
    Deleted,
    LetGo,
}

/// What one tick did to a v2 node: what moved, the status its row shows
/// after the tick, and what its report gauges read. The step decides it;
/// [`apply`] alone writes the status and publishes the gauges, but for a
/// move that clears the node's gauges with its own write (the cancel, the
/// let-go, the delete), which keeps that with the move.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NodeOutcome {
    step: Option<NodeStep>,
    status: Status,
    gauges: Gauges,
}

/// The status a v2 node's row shows after a tick, its `drain_last_error`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum Status {
    /// The tick has nothing to report: the row shows none.
    #[default]
    Clear,
    /// The row shows this.
    Says(String),
    /// The row keeps what it holds: a guarded transition wrote the status
    /// with its move this tick (the fence or what holds it, the dispatch,
    /// the return to `drained`, the let-go's trace), the node left the
    /// drain, a `drained` node keeps why a terminate returned it there, or
    /// the tick acted on nothing (the row moved on since the listing,
    /// another call held the node, the dispatch backoff isn't due).
    Kept,
}

/// What a v2 node's report gauges read after a tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Gauges {
    /// As they were.
    #[default]
    Kept,
    /// What the report the tick stored leaves to do, and whether the drain
    /// is stalled, on the draining and the terminating path alike.
    Report {
        rows: OutstandingRows,
        stalled: bool,
    },
    /// The node didn't answer the tick's `DrainNode` call.
    Unanswered,
}

impl Gauges {
    /// The same gauges with the stall off: a node a refused terminate
    /// returns to `drained` isn't draining any more, so it isn't stalled.
    fn unstalled(self) -> Self {
        match self {
            Self::Report { rows, .. } => Self::Report {
                rows,
                stalled: false,
            },
            other => other,
        }
    }
}

impl NodeOutcome {
    /// Nothing moved, and the row shows `status`.
    fn says(status: impl Into<String>) -> Self {
        Self {
            status: Status::Says(status.into()),
            ..Self::default()
        }
    }

    /// Nothing moved, and the row keeps its status.
    fn kept() -> Self {
        Self {
            status: Status::Kept,
            ..Self::default()
        }
    }

    /// `step` moved, writing the row's status with its move.
    fn moved(step: NodeStep) -> Self {
        Self {
            step: Some(step),
            ..Self::kept()
        }
    }

    /// The node didn't answer the tick's call: `status` says how.
    fn unanswered(status: String) -> Self {
        Self {
            gauges: Gauges::Unanswered,
            ..Self::says(status)
        }
    }

    fn with_gauges(self, gauges: Gauges) -> Self {
        Self { gauges, ..self }
    }

    /// `note` as the status, when the tick has nothing else to report.
    fn or_note(self, note: Option<String>) -> Self {
        match (self.status, note) {
            (Status::Clear, Some(note)) => Self {
                status: Status::Says(note),
                ..self
            },
            (status, _) => Self { status, ..self },
        }
    }
}

/// Apply `outcome`, what `node`'s tick did: publish its gauges, and make
/// the row's `drain_last_error` show its status. The status is written at
/// most once per tick, only when it differs from what the listing read,
/// and only on the row of the generation the tick listed; a status a
/// guarded transition wrote with its move is left as it is. A status that
/// changes is logged at WARN; one that repeats isn't, and a report's
/// node-wide problems are warned about at most every
/// [`BLOCKED_WARN_INTERVAL`] while they last, as the step warns about a
/// drain with no generation or an unknown phase. Returns what moved.
async fn apply<S: DrainV2Sink>(
    sink: &S,
    node: &V2Node,
    outcome: NodeOutcome,
    warn_pacer: &PerNodeWarnPacer,
) -> Option<NodeStep> {
    let label = node.id.to_string();
    match outcome.gauges {
        Gauges::Kept => {}
        Gauges::Report { rows, stalled } => {
            crate::metrics::node_drain::v2_report(
                &label,
                rows.blocked,
                rows.pending,
                rows.held,
                stalled,
            );
            if rows.blocked > 0 && warn_pacer.due(node.id, "blocked") {
                tracing::warn!(
                    node_id = %node.id,
                    blocked = rows.blocked,
                    "node_drain v2: the node reports node-wide problems; the drain can't end \
                     until they are resolved"
                );
            }
        }
        Gauges::Unanswered => crate::metrics::node_drain::v2_unanswered(&label),
    }
    let status = match outcome.status {
        Status::Kept => return outcome.step,
        Status::Clear => None,
        Status::Says(status) => Some(status),
    };
    let Some(generation) = node.generation else {
        return outcome.step;
    };
    if status == node.last_error {
        return outcome.step;
    }
    match sink
        .write_status(node.id, generation, status.as_deref())
        .await
    {
        Ok(true) => match &status {
            Some(status) => tracing::warn!(node_id = %node.id, status, "node_drain v2"),
            None => tracing::info!(node_id = %node.id, "node_drain v2: nothing to report"),
        },
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(node_id = %node.id, %error, "node_drain v2: writing the node's status failed");
        }
    }
    outcome.step
}

/// One pass over every v2 node, [`NODE_CONCURRENCY`] at a time. A node the
/// last tick listed and this one doesn't has its v2 gauges zeroed. EC2 is
/// asked first, in one call about every instance the tick asks about: each
/// fenced node's that the backoff lets ask and each other node's whose
/// heartbeat is stale; each node's step reads its answer, and its outcome
/// is applied ([`apply`]). Once the state's shutdown token is cancelled, a
/// tick doesn't start, and one under way takes no new node; the steps
/// already running go on until the controller's shutdown grace ends, which
/// drops any still running. Then the sandboxes each listed node's drain
/// left suspended longer than the config's bound are counted.
pub async fn tick<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    state: &V2LoopState,
) -> V2TickReport {
    use futures_util::StreamExt as _;

    let mut report = V2TickReport::default();
    if state.shutdown.is_cancelled() {
        return report;
    }
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
    state.warn_pacer.retain_live(&listed);
    let backoff = &state.backoff;
    let config = &state.config;
    let warn_pacer = &state.warn_pacer;
    let shutdown = &state.shutdown;
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
    let instance_ids: Vec<String> = asked
        .iter()
        .filter_map(|(_, instance_id)| instance_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let answers = if instance_ids.is_empty() {
        HashMap::new()
    } else {
        authority.describe_instances(&instance_ids).await
    };
    let evidence: HashMap<NodeId, InstanceEvidence> = asked
        .into_iter()
        .map(|(node_id, instance_id)| {
            let answer = match instance_id {
                Some(instance_id) => answers.get(&instance_id).cloned().unwrap_or_else(|| {
                    InstanceEvidence::Unknown("EC2 gave no answer for it".to_owned())
                }),
                None => InstanceEvidence::Unknown("the node records no EC2 instance".to_owned()),
            };
            (node_id, answer)
        })
        .collect();
    let steps: Vec<_> = futures_util::stream::iter(nodes.into_iter().map(|node| {
        let evidence = evidence.get(&node.id).cloned();
        (node, evidence)
    }))
    .take_while(|_| std::future::ready(!shutdown.is_cancelled()))
    .map(|(node, evidence)| async move {
        let outcome = step(
            sink,
            authority,
            &node,
            evidence.as_ref(),
            backoff,
            config,
            warn_pacer,
        )
        .await;
        (node.id, apply(sink, &node, outcome, warn_pacer).await)
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
    if !shutdown.is_cancelled() {
        publish_still_suspended(sink, &listed, config.still_suspended_after).await;
    }
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
    let state = V2LoopState::with_shutdown(config, shutdown.clone());
    let mut interval = tokio::time::interval(tick_every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            // Shutdown first: a tick longer than `tick_every` leaves the
            // interval ready.
            biased;
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
