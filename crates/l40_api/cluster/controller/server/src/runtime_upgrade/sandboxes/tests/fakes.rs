//! Fake sandbox upgrade dispatcher and memory-retirement source for reconciler
//! tests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};
use pplx_space_runtime_upgrade_campaign_store::CampaignId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::*;

mod helpers;
mod stop;

pub(super) use helpers::{
    FakeMemoryRetirement, active_k8s_stopped_execution as active_k8s_stopped_execution_for_test,
};
use helpers::{
    has_active_item_lease, pause_and_hang_past_deadline, settle_started_lifecycle,
    start_claim_refusal,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Call {
    Stop(Uuid),
    Upgrade(Uuid),
    Start(Uuid),
}

#[derive(Default)]
pub(super) struct FakeState {
    pub(super) calls: Vec<Call>,
    /// The source each upgrade was guarded on and the target it was asked
    /// for, in call order.
    pub(super) upgrades: Vec<(EnvironmentReference, EnvironmentReference)>,
    /// The source each stop was guarded on and the target it was for, in
    /// call order.
    pub(super) stops: Vec<(EnvironmentReference, EnvironmentReference)>,
    /// What a stop returns per sandbox; `Stopped` otherwise.
    pub(super) stop_script: HashMap<Uuid, GuardedStop>,
    /// Pre-commit errors a stop returns per sandbox, in order, before touching
    /// Postgres or recording `stopped_at`.
    pub(super) stop_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Post-commit errors a stop returns per sandbox after committing the
    /// guarded stop in Postgres and recording `stopped_at` on the item.
    pub(super) post_commit_stop_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Virtual-clock advance a stop performs before hanging indefinitely after
    /// committing the guarded stop and `stopped_at`, deterministically tripping
    /// `item_deadline` without wall-clock races.
    pub(super) post_commit_stop_deadline_advances: HashMap<Uuid, Duration>,
    /// Virtual-clock advance a stop performs after the node admits the campaign
    /// stop and before the post-RPC Postgres commit runs, tripping `item_deadline`.
    pub(super) admitted_stop_pre_commit_deadline_advances: HashMap<Uuid, Duration>,
    /// Virtual-clock advance a stop performs after recording initiation and
    /// before the node admits the campaign stop, tripping `item_deadline`.
    pub(super) unadmitted_stop_pre_commit_deadline_advances: HashMap<Uuid, Duration>,
    /// Pre-admission node errors returned after initiation is recorded, without
    /// admitting the campaign stop on the node (`is_stop_not_admitted`).
    pub(super) pre_admission_stop_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Sandboxes whose node stop RPC races `converge_published_stopped` on `NodeId`
    /// before `stop`'s post-RPC Postgres commit runs.
    pub(super) converge_stopped_during_in_flight_stop: HashMap<Uuid, NodeId>,
    /// Campaign-item stop episodes admitted on the node (`(campaign_id, sandbox_id)`
    /// mapped to `InitiatedStop::dispatched_at`), cleared when that episode is
    /// cleared or superseded by `start`.
    pub(super) node_admitted_campaign_stops: HashMap<(CampaignId, Uuid), jiff::Timestamp>,
    /// One-shot hooks run right after `stop` commits `stopped` and `stopped_at`
    /// in Postgres, before returning `GuardedStop::Stopped`.
    pub(super) on_stop_commit: HashMap<Uuid, Box<dyn FnOnce() + Send>>,
    /// Sandboxes where a concurrent wake moves the row to `starting` right
    /// after `stop` commits `stopped_at` and before `stop` returns.
    pub(super) post_stop_commit_starting: HashSet<Uuid>,
    /// Sandboxes where a lingering prior-attempt `stop` settles `stopped` (with
    /// `stopped_at`) right before the current `stop` claims the row, causing
    /// the current `stop` to return `GuardedStop::Declined`.
    pub(super) lingering_stops_on_stop_call: HashSet<Uuid>,
    /// Sandboxes where a lingering prior-attempt `stop` enters `stopping` right
    /// before the current `stop` claims the row, causing the current `stop` to
    /// return `GuardedStop::Declined`.
    pub(super) lingering_stopping_on_stop_call: HashSet<Uuid>,
    /// Sandboxes whose runtime is already stopped on the node when an unstarted
    /// `stop` dials the node RPC (for example, after guest poweroff ahead of
    /// node-status reconcile), causing `stop` to decline without adopting it.
    pub(super) already_stopped_on_node: HashSet<Uuid>,
    /// Stop-and-start progress recorded per `(campaign_id, sandbox_id)` when
    /// `stop` or `start` settles in Postgres for a campaign item.
    pub(super) item_stopped_at: HashMap<(CampaignId, Uuid), RecordedStop>,
    /// Errors `stopped_at` returns per sandbox, in order.
    pub(super) stopped_at_read_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Sandboxes deleted right after `stopped_at` is read, exercising deletion
    /// between `inspect_subject`'s initial and post-`stopped_at` reads.
    pub(super) delete_on_stopped_at_read: HashSet<Uuid>,
    /// Errors `clear_stopped_at` returns per sandbox, in order.
    pub(super) clear_stopped_at_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// The source each start was guarded on, in call order.
    pub(super) start_sources: Vec<EnvironmentReference>,
    /// The target each start was asked for, in call order.
    pub(super) start_targets: Vec<EnvironmentReference>,
    /// The `(sandbox, stop_fence, target)` fence each start received, in call order.
    pub(super) start_fences: Vec<(Uuid, StopFence, EnvironmentReference)>,
    /// Sandboxes that a user starts and stops again right before `start` claims
    /// the stopped row, exercising `stopped_at` fencing inside `start`.
    pub(super) user_restarts_before_start: HashSet<Uuid>,
    /// Sandboxes where a user queues a `:stop` operation right before `start`
    /// claims the stopped row, without advancing `sandboxes.lifecycle_changed_at`.
    pub(super) external_stop_before_start: HashSet<Uuid>,
    /// Sandboxes where a user completes a `:kill` operation while `stop` is in
    /// flight (`initiated_at <= completed_at < stopped_at`), before the node
    /// reports `sandboxes.lifecycle = 'stopped'`.
    pub(super) external_kill_during_in_flight_stop: HashSet<Uuid>,
    /// Sandboxes whose `sandboxes.lifecycle_changed_at` advances to a new
    /// running episode while `stop` is in flight before post-RPC settlement.
    pub(super) in_flight_stop_episode_replacements: HashSet<Uuid>,
    /// Sandboxes whose active Kubernetes execution is replaced with a new pod
    /// generation/UID while `stop` is in flight before post-RPC settlement.
    pub(super) replace_k8s_pod_during_in_flight_stop: HashMap<Uuid, StoppedK8sExecution>,
    /// Sandboxes whose active Kubernetes execution `pod_uid` changes in place at
    /// unchanged `generation` and `lifecycle_changed_at` while `stop` is in flight.
    pub(super) update_active_k8s_pod_uid_during_in_flight_stop:
        HashMap<Uuid, pplx_space_k8s_ids::PodUid>,
    /// Sandboxes that move onto `target` while `RecordedStop::Initiated` remains
    /// set right before `stop` declines.
    pub(super) move_to_target_after_initiated_stop: HashSet<Uuid>,
    /// Sandboxes where a lingering prior `start` transitions the row to
    /// `starting` right before `start` claims the fenced `stopped` row.
    pub(super) lingering_starting_before_start: HashSet<Uuid>,
    /// Sandboxes where a lingering prior `start` settles the row on `started`
    /// under `target` right before `start` claims the fenced `stopped` row.
    pub(super) lingering_started_on_target_before_start: HashSet<Uuid>,
    /// Pre-claim results a start returns per sandbox, in order; `Started` once exhausted.
    pub(super) start_script: HashMap<Uuid, VecDeque<Result<TargetStart, DispatchError>>>,
    /// Pre-dispatch errors a start returns per sandbox after claiming
    /// `'stopped' -> 'starting'` + `StartingOnTarget`, rolling `'starting'` back
    /// to `'stopped'` (`RETURNING lifecycle_changed_at`) and restoring
    /// `RecordedStop::Stopped` with the refreshed `stopped_at`.
    pub(super) pre_dispatch_start_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Post-claim errors a start returns per sandbox after claiming
    /// `'stopped' -> 'starting'` in Postgres and recording `StartingOnTarget`.
    pub(super) post_claim_start_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Claim-time policy/retirement shifts injected inside `FakeDispatcher::stop`
    /// and `FakeDispatcher::start` after the driver's pre-dispatch checks pass.
    pub(super) allow_source_before_stop: HashSet<Uuid>,
    pub(super) unretire_source_before_stop: HashSet<Uuid>,
    pub(super) allow_source_before_start: HashSet<Uuid>,
    pub(super) unretire_source_before_start: HashSet<Uuid>,
    /// Virtual-clock advance a start performs before hanging indefinitely right
    /// after claiming `'stopped' -> 'starting'` and recording `StartingOnTarget`,
    /// before dispatching the node start RPC (simulating controller exit or
    /// cancellation before node dispatch).
    pub(super) undispatched_start_deadline_advances: HashMap<Uuid, Duration>,
    /// Virtual-clock advance a start performs before hanging indefinitely after
    /// claiming `'stopped' -> 'starting'`, recording `StartingOnTarget`, and
    /// dispatching the node start RPC, deterministically tripping `item_deadline`
    /// without wall-clock races.
    pub(super) post_claim_start_deadline_advances: HashMap<Uuid, Duration>,
    /// Campaign-item start episodes dispatched to the node (`(campaign_id, sandbox_id)`
    /// mapped to `StartingClaim::stop`), cleared when that episode settles or is
    /// cleared.
    pub(super) node_dispatched_campaign_starts: HashMap<(CampaignId, Uuid), StopFence>,
    /// The result for every start without a script.
    pub(super) start_default: Option<Result<TargetStart, DispatchError>>,
    /// Upgrades or starts in progress, by node, and the most seen at once.
    pub(super) in_flight: HashMap<Option<NodeId>, u64>,
    pub(super) max_in_flight: u64,
    pub(super) max_on_node: u64,
    /// Results to return per sandbox, in order; `Upgraded` once exhausted.
    pub(super) script: HashMap<Uuid, VecDeque<Result<StoppedUpgrade, DispatchError>>>,
    /// The result for every sandbox without a script.
    pub(super) default: Option<Result<StoppedUpgrade, DispatchError>>,
}

/// Records every call and answers from a script. Each upgrade or start yields
/// once so the items leased in one tick overlap without wall-clock sleeps.
#[derive(Clone, Default)]
pub(super) struct FakeDispatcher {
    state: Arc<Mutex<FakeState>>,
    nodes: Arc<HashMap<Uuid, NodeId>>,
    /// When set, `GuardedStop::Stopped` writes `stopped` to `sandboxes` and
    /// `TargetStart::Started` writes `started` on `target` to Postgres.
    persist_pool: Option<PgPool>,
    /// Optional memory-retirement source checked at `stop` and `start` claim time.
    memory_retirement: Option<FakeMemoryRetirement>,
}

impl FakeDispatcher {
    pub(super) fn with_nodes(nodes: HashMap<Uuid, NodeId>) -> Self {
        Self {
            nodes: Arc::new(nodes),
            ..Self::default()
        }
    }

    pub(super) fn persisting(mut self, pool: PgPool) -> Self {
        self.persist_pool = Some(pool);
        self
    }

    pub(super) fn with_memory_retirement(
        mut self,
        memory_retirement: FakeMemoryRetirement,
    ) -> Self {
        self.memory_retirement = Some(memory_retirement);
        self
    }

    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    pub(super) fn answer_all(&self, result: Result<StoppedUpgrade, DispatchError>) {
        self.state().default = Some(result);
    }

    pub(super) fn script(
        &self,
        sandbox: Uuid,
        results: Vec<Result<StoppedUpgrade, DispatchError>>,
    ) {
        self.state().script.insert(sandbox, results.into());
    }

    pub(super) fn script_start(
        &self,
        sandbox: Uuid,
        results: Vec<Result<TargetStart, DispatchError>>,
    ) {
        self.state().start_script.insert(sandbox, results.into());
    }

    pub(super) fn script_pre_dispatch_start_error(&self, sandbox: Uuid, error: DispatchError) {
        self.state()
            .pre_dispatch_start_errors
            .entry(sandbox)
            .or_default()
            .push_back(error);
    }

    pub(super) fn script_post_claim_start_error(&self, sandbox: Uuid, error: DispatchError) {
        self.state()
            .post_claim_start_errors
            .entry(sandbox)
            .or_default()
            .push_back(error);
    }

    pub(super) fn recorded_stopped_at(
        &self,
        campaign_id: CampaignId,
        sandbox: Uuid,
    ) -> Option<jiff::Timestamp> {
        self.state()
            .item_stopped_at
            .get(&(campaign_id, sandbox))
            .and_then(|recorded| recorded.as_fence().map(|fence| fence.stopped_at))
    }

    pub(super) fn recorded_stop(
        &self,
        campaign_id: CampaignId,
        sandbox: Uuid,
    ) -> Option<RecordedStop> {
        self.state()
            .item_stopped_at
            .get(&(campaign_id, sandbox))
            .cloned()
    }

    pub(super) fn on_stop_commit(&self, sandbox: Uuid, hook: impl FnOnce() + Send + 'static) {
        self.state().on_stop_commit.insert(sandbox, Box::new(hook));
    }

    pub(super) fn calls(&self) -> Vec<Call> {
        self.state().calls.clone()
    }

    /// The sandboxes upgrades were asked for, sorted, repeats included.
    pub(super) fn upgraded(&self) -> Vec<Uuid> {
        let mut upgraded: Vec<Uuid> = self
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Upgrade(sandbox) => Some(sandbox),
                Call::Stop(_) | Call::Start(_) => None,
            })
            .collect();
        upgraded.sort();
        upgraded
    }

    /// The sandboxes starts were asked for, sorted, repeats included.
    pub(super) fn started(&self) -> Vec<Uuid> {
        let mut started: Vec<Uuid> = self
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Start(sandbox) => Some(sandbox),
                Call::Stop(_) | Call::Upgrade(_) => None,
            })
            .collect();
        started.sort();
        started
    }

    pub(super) fn peaks(&self) -> (u64, u64) {
        let state = self.state();
        (state.max_in_flight, state.max_on_node)
    }

    fn enter_in_flight(&self, sandbox: Uuid, call: Call) -> Option<NodeId> {
        let node = self.nodes.get(&sandbox).copied();
        let mut state = self.state();
        state.calls.push(call);
        *state.in_flight.entry(node).or_default() += 1;
        let total = state.in_flight.values().sum();
        state.max_in_flight = state.max_in_flight.max(total);
        if node.is_some() {
            let on_node = state.in_flight[&node];
            state.max_on_node = state.max_on_node.max(on_node);
        }
        node
    }
}

impl SandboxUpgradeDispatcher for FakeDispatcher {
    async fn upgrade_stopped(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<StoppedUpgrade, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        self.state().upgrades.push((source, target));
        let node = self.enter_in_flight(sandbox, Call::Upgrade(sandbox));
        tokio::task::yield_now().await;
        let explicit_outcome = {
            let mut state = self.state();
            *state.in_flight.entry(node).or_default() -= 1;
            let scripted = state.script.get_mut(&sandbox).and_then(VecDeque::pop_front);
            scripted.or_else(|| state.default.clone())
        };
        if let Some(pool) = &self.persist_pool {
            if explicit_outcome.is_none() {
                let inputs = Inputs::read(pool)
                    .await
                    .expect("reading policy and catalog before fake stopped upgrade");
                let Some(subject) = store::subject_sandbox(pool, sandbox)
                    .await
                    .expect("reading subject sandbox before fake stopped upgrade")
                else {
                    return Ok(StoppedUpgrade::NotMoved(
                        "the sandbox was deleted".to_owned(),
                    ));
                };
                if subject.lifecycle != SandboxLifecycle::Stopped.as_str() {
                    return Ok(StoppedUpgrade::NotMoved(
                        "the sandbox is no longer stopped".to_owned(),
                    ));
                }
                let attributed = subject
                    .effective_execution()
                    .and_then(|exec| inputs.attributed(exec));
                if attributed == Some(target) {
                    return Ok(StoppedUpgrade::AlreadyOnTarget);
                }
                if attributed != Some(source) {
                    return Ok(StoppedUpgrade::NotMoved(
                        "the sandbox is no longer on the campaign's source".to_owned(),
                    ));
                }
            }
            if !has_active_item_lease(pool, item).await
                && !matches!(explicit_outcome, Some(Ok(StoppedUpgrade::AlreadyOnTarget)))
            {
                return Ok(StoppedUpgrade::Held(
                    "the campaign item lease is no longer active".to_owned(),
                ));
            }
        }
        let outcome = explicit_outcome.unwrap_or(Ok(StoppedUpgrade::Upgraded));
        if matches!(outcome, Ok(StoppedUpgrade::Upgraded))
            && let Some(pool) = &self.persist_pool
        {
            sqlx::query(sql!(
                "UPDATE sandbox_observed_states
                 SET execution_metadata = $2,
                     runtime_version = NULL,
                     state_format_version = NULL
                 WHERE sandbox_id = $1"
            ))
            .bind(sandbox)
            .bind(serde_json::json!({ "environment": target.to_string() }))
            .execute(pool)
            .await
            .expect("persisting fake stopped upgrade target");
        }
        outcome
    }

    async fn stop(
        &self,
        item: &LeasedItem,
        record_stop: bool,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<GuardedStop, DispatchError> {
        self.dispatch_stop(item, record_stop, sandbox_id, source, target)
            .await
    }

    async fn start(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        stop: StopFence,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<TargetStart, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        {
            let mut state = self.state();
            state.start_sources.push(source);
            state.start_targets.push(target);
            state.start_fences.push((sandbox, stop.clone(), target));
        }
        let node = self.enter_in_flight(sandbox, Call::Start(sandbox));
        tokio::task::yield_now().await;
        let (
            explicit_outcome,
            pre_dispatch_error,
            undispatched_start_deadline_advance,
            post_claim_error,
            post_claim_deadline_advance,
            user_restarted,
            external_stopped,
            lingering_starting,
            lingering_started_on_target,
            allow_before_start,
            unretire_before_start,
        ) = {
            let mut state = self.state();
            *state.in_flight.entry(node).or_default() -= 1;
            let user_restarted = state.user_restarts_before_start.remove(&sandbox);
            let external_stopped = state.external_stop_before_start.remove(&sandbox);
            let lingering_starting = state.lingering_starting_before_start.remove(&sandbox);
            let lingering_started_on_target = state
                .lingering_started_on_target_before_start
                .remove(&sandbox);
            let allow_before_start = state.allow_source_before_start.remove(&sandbox);
            let unretire_before_start = state.unretire_source_before_start.remove(&sandbox);
            let pre_dispatch_error = state
                .pre_dispatch_start_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            let undispatched_start_deadline_advance =
                state.undispatched_start_deadline_advances.remove(&sandbox);
            let post_claim_error = state
                .post_claim_start_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            let post_claim_deadline_advance =
                state.post_claim_start_deadline_advances.remove(&sandbox);
            let scripted = state
                .start_script
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            (
                scripted.or_else(|| state.start_default.clone()),
                pre_dispatch_error,
                undispatched_start_deadline_advance,
                post_claim_error,
                post_claim_deadline_advance,
                user_restarted,
                external_stopped,
                lingering_starting,
                lingering_started_on_target,
                allow_before_start,
                unretire_before_start,
            )
        };
        if unretire_before_start && let Some(retirement) = &self.memory_retirement {
            retirement.unretire(source);
        }
        if allow_before_start && let Some(pool) = &self.persist_pool {
            let mut tx = pool
                .begin()
                .await
                .expect("opening transaction for simulated pre-start source allow");
            pplx_space_runtime_environment_policy_store::set_environment_state(
                &mut *tx,
                source,
                RuntimeEnvironmentState::Allowed,
                &change(),
            )
            .await
            .expect("allowing source before fake start");
            tx.commit()
                .await
                .expect("committing simulated pre-start source allow");
        }
        if external_stopped && let Some(pool) = &self.persist_pool {
            use pplx_space_public_api::{
                OperationKind, OperationState, OperationTargetType, PublicSandboxId,
            };
            let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
            sqlx::query(sql!(
                "INSERT INTO public_operations (
                     operation_id, org_id, actor_user_id, kind, target_type, target_id,
                     state, requested_at
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            ))
            .bind(Uuid::now_v7())
            .bind(SEED_ORG_ID)
            .bind(SEED_USER_ID)
            .bind(OperationKind::StopSandbox.as_str())
            .bind(OperationTargetType::Sandbox.as_str())
            .bind(target_id)
            .bind(OperationState::Queued.as_str())
            .bind(to_offset_date_time(stop.stopped_at))
            .execute(pool)
            .await
            .expect("inserting simulated queued external stop before start claim");
        }
        if lingering_starting && let Some(pool) = &self.persist_pool {
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET lifecycle_changed_at = lifecycle_changed_at - INTERVAL '1 second'
                 WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await
            .expect("aging stopped timestamp before simulated lingering starting");
            set_lifecycle(pool, sandbox, SandboxLifecycle::Starting.as_str())
                .await
                .expect("simulating lingering starting before start claim");
        }
        if lingering_started_on_target && let Some(pool) = &self.persist_pool {
            self.record_item_starting_on_target(
                item.campaign_id,
                item.subject_kind,
                sandbox,
                StartingClaim::from_stop(stop.clone()),
            )
            .await;
            self.state()
                .node_dispatched_campaign_starts
                .insert((item.campaign_id, sandbox), stop.clone());
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET lifecycle_changed_at = lifecycle_changed_at - INTERVAL '1 second'
                 WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await
            .expect("aging stopped timestamp before simulated lingering target start");
            set_lifecycle(pool, sandbox, SandboxLifecycle::Started.as_str())
                .await
                .expect("simulating lingering started on target before start claim");
            set_bound(pool, sandbox, target)
                .await
                .expect("binding simulated lingering start to target");
        }
        if user_restarted && let Some(pool) = &self.persist_pool {
            // Simulate a user starting and stopping the sandbox between the
            // driver's read/stop and `start`'s guarded row claim; backdating
            // before the user's stop ensures `sandboxes_touch_lifecycle_changed_at`
            // stamps a strictly newer `lifecycle_changed_at` without sleeping.
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET lifecycle_changed_at = lifecycle_changed_at - INTERVAL '1 second'
                 WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(pool)
            .await
            .expect("aging stopped timestamp before simulated user restart");
            set_lifecycle(pool, sandbox, SandboxLifecycle::Started.as_str())
                .await
                .expect("simulating user start before start claim");
            set_lifecycle(pool, sandbox, SandboxLifecycle::Stopped.as_str())
                .await
                .expect("simulating user stop before start claim");
        }
        let mut outcome = explicit_outcome.clone().unwrap_or(Ok(TargetStart::Started));
        let should_claim = pre_dispatch_error.is_some()
            || undispatched_start_deadline_advance.is_some()
            || post_claim_error.is_some()
            || post_claim_deadline_advance.is_some()
            || matches!(
                outcome,
                Ok(TargetStart::Started | TargetStart::AlreadyOnTarget)
            );
        if should_claim {
            let mut already_on_target = false;
            let mut pre_claim_placement: Option<(String, Option<NodeId>)> = None;
            let claim = if let Some(pool) = &self.persist_pool {
                let mut tx = pool
                    .begin()
                    .await
                    .expect("opening fake start claim transaction");
                // Canonical lock order across every multi-table dispatcher
                // transaction: lock `sandboxes` before `runtime_upgrade_items`.
                pre_claim_placement = sqlx::query_as(sql!(
                    "SELECT placement, active_node_id FROM sandboxes WHERE id = $1 FOR UPDATE"
                ))
                .bind(sandbox)
                .fetch_optional(&mut *tx)
                .await
                .expect("locking sandbox row before fake start");
                let item_phase: Option<String> = sqlx::query_scalar(sql!(
                    "SELECT phase
                     FROM runtime_upgrade_items
                     WHERE campaign_id = $1
                       AND subject_kind = $2
                       AND subject_id = $3
                     FOR UPDATE"
                ))
                .bind(item.campaign_id)
                .bind(item.subject_kind.as_str())
                .bind(sandbox)
                .fetch_optional(&mut *tx)
                .await
                .expect("locking campaign item row inside fake start claim transaction");
                if !has_active_item_lease(&mut *tx, item).await
                    || matches!(item_phase.as_deref(), None | Some("done" | "skipped"))
                {
                    return Ok(TargetStart::HeldStopped(
                        "the campaign item lease is no longer active".to_owned(),
                    ));
                }
                match store::external_stop_since(
                    &mut *tx,
                    sandbox,
                    to_offset_date_time(stop.initiated_at),
                    to_offset_date_time(stop.dispatched_at),
                )
                .await
                .expect("checking external stop operations before fake start")
                {
                    store::ExternalOperation::Succeeded => {
                        return Ok(TargetStart::NotOnTarget(
                            super::super::force_restarts::EXTERNAL_STOP_SUPERSEDED.to_owned(),
                        ));
                    }
                    store::ExternalOperation::InFlight => {
                        return Ok(TargetStart::HeldStopped(
                            super::super::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT.to_owned(),
                        ));
                    }
                    store::ExternalOperation::None => {}
                }
                let Some(subject) = store::subject_sandbox(&mut *tx, sandbox)
                    .await
                    .expect("reading subject sandbox before fake start")
                else {
                    return Ok(TargetStart::NotOnTarget(
                        "the sandbox was deleted".to_owned(),
                    ));
                };
                let subject_changed_at = to_jiff(subject.lifecycle_changed_at);
                let since = to_offset_date_time(stop.stopped_at);
                let settled_since = to_offset_date_time(stop.settled_stopped_at);
                let exec_owned = if let Some(exec) = &stop.k8s_stopped_execution {
                    match store::stopped_execution_ownership(
                        &mut *tx,
                        sandbox,
                        since,
                        settled_since,
                        exec.generation,
                        &exec.pod_uid,
                    )
                    .await
                    .expect("checking stopped execution ownership before fake start")
                    {
                        store::StoppedExecutionOwnership::Owned => true,
                        store::StoppedExecutionOwnership::InFlight => {
                            return Ok(TargetStart::HeldStopped(
                                super::super::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT
                                    .to_owned(),
                            ));
                        }
                        store::StoppedExecutionOwnership::Superseded => false,
                    }
                } else {
                    subject_changed_at == stop.settled_stopped_at && !subject.pinned_stop_disk
                };
                let stop_owned =
                    if subject.lifecycle == SandboxLifecycle::Stopped.as_str() && exec_owned {
                        match store::external_activation_since(&mut *tx, sandbox, since)
                            .await
                            .expect("checking external activations before fake start")
                        {
                            store::ExternalOperation::None => true,
                            store::ExternalOperation::InFlight => {
                                return Ok(TargetStart::HeldStopped(
                                    super::super::force_restarts::EXTERNAL_OPERATION_IN_FLIGHT
                                        .to_owned(),
                                ));
                            }
                            store::ExternalOperation::Succeeded => false,
                        }
                    } else {
                        false
                    };
                if !stop_owned {
                    return Ok(TargetStart::NotOnTarget(
                        "the sandbox changed lifecycle since this campaign stopped it".to_owned(),
                    ));
                }
                let inputs = Inputs::read_conn(&mut *tx)
                    .await
                    .expect("reading policy and catalog before fake start");
                if let Some(refusal) = start_claim_refusal(
                    &inputs,
                    &subject,
                    source,
                    target,
                    self.memory_is_retired(source),
                ) {
                    return Ok(refusal);
                }
                let attributed = subject
                    .effective_execution()
                    .and_then(|exec| inputs.attributed(exec));
                already_on_target = attributed == Some(target);
                let claimed_starting_at: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
                    "UPDATE sandboxes
                     SET lifecycle = $3,
                         placement = 'on_node',
                         active_node_id = COALESCE($5, active_node_id)
                     WHERE id = $1
                       AND lifecycle = $2
                       AND lifecycle_changed_at = $4
                     RETURNING lifecycle_changed_at"
                ))
                .bind(sandbox)
                .bind(SandboxLifecycle::Stopped.as_str())
                .bind(SandboxLifecycle::Starting.as_str())
                .bind(subject.lifecycle_changed_at)
                .bind(node)
                .fetch_optional(&mut *tx)
                .await
                .expect("claiming fake start lifecycle");
                let Some(claimed_starting_at) = claimed_starting_at else {
                    return Ok(TargetStart::NotOnTarget(
                        "the sandbox changed lifecycle since this campaign stopped it".to_owned(),
                    ));
                };
                if !store::mark_leased_starting_on_target(&mut *tx, item)
                    .await
                    .expect("marking fake leased item starting on target")
                {
                    let _ = tx.rollback().await;
                    return Ok(TargetStart::HeldStopped(
                        "the campaign item lease is no longer active".to_owned(),
                    ));
                }
                let (prev_placement, prev_active_node_id) = match &pre_claim_placement {
                    Some((placement, active_node)) => {
                        (Some(placement.clone()), active_node.map(NodeId::as_uuid))
                    }
                    None => (None, None),
                };
                let claim = StartingClaim {
                    stop: stop.clone(),
                    starting_at: Some(to_jiff(claimed_starting_at)),
                    prev_placement,
                    prev_active_node_id,
                };
                tx.commit()
                    .await
                    .expect("committing fake start claim transaction");
                {
                    let mut state = self.state();
                    state
                        .node_admitted_campaign_stops
                        .remove(&(item.campaign_id, sandbox));
                    state
                        .node_dispatched_campaign_starts
                        .remove(&(item.campaign_id, sandbox));
                }
                self.record_item_starting_on_target_sync(item.campaign_id, sandbox, claim.clone());
                claim
            } else {
                let claim = StartingClaim::from_stop(stop.clone());
                self.record_item_starting_on_target_sync(item.campaign_id, sandbox, claim.clone());
                claim
            };
            if let Some(advance) = undispatched_start_deadline_advance {
                pause_and_hang_past_deadline(advance).await;
            }
            if let Some(error) = pre_dispatch_error {
                if let Some(pool) = &self.persist_pool {
                    let mut tx = pool
                        .begin()
                        .await
                        .expect("opening transaction to roll back fake undispatched start");
                    let _locked_sandbox: Option<i32> = sqlx::query_scalar(sql!(
                        "SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"
                    ))
                    .bind(sandbox)
                    .fetch_optional(&mut *tx)
                    .await
                    .expect("locking sandbox row during pre-dispatch start rollback");
                    let item_phase: Option<String> = sqlx::query_scalar(sql!(
                        "SELECT phase
                         FROM runtime_upgrade_items
                         WHERE campaign_id = $1
                           AND subject_kind = $2
                           AND subject_id = $3
                         FOR UPDATE"
                    ))
                    .bind(item.campaign_id)
                    .bind(item.subject_kind.as_str())
                    .bind(sandbox)
                    .fetch_optional(&mut *tx)
                    .await
                    .expect("locking campaign item row during pre-dispatch start rollback");
                    if has_active_item_lease(&mut *tx, item).await
                        && !matches!(item_phase.as_deref(), None | Some("done" | "skipped"))
                    {
                        let (prev_placement, prev_active_node_id) =
                            pre_claim_placement.unwrap_or_else(|| ("on_node".to_owned(), node));
                        let starting_at = claim.starting_at.map(to_offset_date_time);
                        let rolled_back: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
                            "UPDATE sandboxes
                             SET lifecycle = $2,
                                 placement = $4,
                                 active_node_id = $5
                             WHERE id = $1
                               AND lifecycle = $3
                               AND ($6::timestamptz IS NULL OR lifecycle_changed_at = $6)
                             RETURNING lifecycle_changed_at"
                        ))
                        .bind(sandbox)
                        .bind(SandboxLifecycle::Stopped.as_str())
                        .bind(SandboxLifecycle::Starting.as_str())
                        .bind(prev_placement)
                        .bind(prev_active_node_id)
                        .bind(starting_at)
                        .fetch_optional(&mut *tx)
                        .await
                        .expect("rolling back fake undispatched start to stopped");
                        if let Some(rolled_back) = rolled_back {
                            store::clear_leased_starting_on_target(&mut *tx, item)
                                .await
                                .expect(
                                    "clearing starting_on_target during pre-dispatch start rollback",
                                );
                            tx.commit()
                                .await
                                .expect("committing fake undispatched start rollback");
                            self.revert_item_starting_to_stopped_sync(
                                item.campaign_id,
                                sandbox,
                                &claim,
                                to_jiff(rolled_back),
                            );
                        }
                    }
                }
                return Err(error);
            }
            self.state()
                .node_dispatched_campaign_starts
                .insert((item.campaign_id, sandbox), stop.clone());
            if let Some(advance) = post_claim_deadline_advance {
                pause_and_hang_past_deadline(advance).await;
            }
            if let Some(error) = post_claim_error {
                return Err(error);
            }
            if let Some(pool) = &self.persist_pool {
                settle_started_lifecycle(pool, sandbox)
                    .await
                    .expect("settling fake start lifecycle");
                if let Some(target_node) = node {
                    sqlx::query(sql!(
                        "INSERT INTO sandbox_observed_states (
                             sandbox_id, node_id, runtime, runtime_version,
                             state_format_name, state_format_version, execution_metadata
                         )
                         VALUES ($1, $2, 'firecracker', NULL, 'firecracker-snapshot', NULL, $3)
                         ON CONFLICT (sandbox_id) DO UPDATE
                         SET node_id = EXCLUDED.node_id,
                             execution_metadata = EXCLUDED.execution_metadata,
                             runtime_version = NULL,
                             state_format_version = NULL"
                    ))
                    .bind(sandbox)
                    .bind(target_node)
                    .bind(serde_json::json!({ "environment": target.to_string() }))
                    .execute(pool)
                    .await
                    .expect("upserting fake start target");
                } else {
                    sqlx::query(sql!(
                        "UPDATE sandbox_observed_states
                         SET execution_metadata = $2,
                             runtime_version = NULL,
                             state_format_version = NULL
                         WHERE sandbox_id = $1"
                    ))
                    .bind(sandbox)
                    .bind(serde_json::json!({ "environment": target.to_string() }))
                    .execute(pool)
                    .await
                    .expect("persisting fake start target");
                }
                if explicit_outcome.is_none() && already_on_target {
                    outcome = Ok(TargetStart::AlreadyOnTarget);
                }
            }
            self.record_item_started_on_target(item.campaign_id, item.subject_kind, sandbox, stop)
                .await;
        }
        outcome
    }

    async fn recover_undispatched_start(
        &self,
        item: &LeasedItem,
        sandbox_id: SandboxId,
        claim: StartingClaim,
    ) -> Result<StartingRecovery, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let key = (item.campaign_id, sandbox);
        if self.state().node_dispatched_campaign_starts.get(&key) == Some(&claim.stop) {
            return Ok(StartingRecovery::StillStarting);
        }
        let Some(pool) = &self.persist_pool else {
            return Ok(StartingRecovery::StillStarting);
        };
        let mut tx = pool
            .begin()
            .await
            .expect("opening transaction for undispatched fake start recovery");
        let _locked_sandbox: Option<i32> =
            sqlx::query_scalar(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
                .bind(sandbox)
                .fetch_optional(&mut *tx)
                .await
                .expect("locking sandbox row during undispatched start recovery");
        let phase: Option<String> = sqlx::query_scalar(sql!(
            "SELECT phase
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3
             FOR UPDATE"
        ))
        .bind(item.campaign_id)
        .bind(item.subject_kind.as_str())
        .bind(sandbox)
        .fetch_optional(&mut *tx)
        .await
        .expect("locking campaign item row during undispatched start recovery");
        if !has_active_item_lease(&mut *tx, item).await
            || matches!(phase.as_deref(), None | Some("done" | "skipped"))
        {
            return Ok(StartingRecovery::StillStarting);
        }
        {
            let state = self.state();
            if state.node_dispatched_campaign_starts.get(&key) == Some(&claim.stop) {
                return Ok(StartingRecovery::StillStarting);
            }
            if state.item_stopped_at.get(&key)
                != Some(&RecordedStop::StartingOnTarget(claim.clone()))
            {
                return Ok(StartingRecovery::Settled);
            }
        }
        let starting_at = claim.starting_at.map(to_offset_date_time);
        let rolled_back: Option<time::OffsetDateTime> = sqlx::query_scalar(sql!(
            "UPDATE sandboxes
             SET lifecycle = $2,
                 placement = COALESCE($4, placement),
                 active_node_id = CASE
                     WHEN $5 THEN $6
                     ELSE active_node_id
                 END
             WHERE id = $1
               AND lifecycle = $3
               AND ($7::timestamptz IS NULL OR lifecycle_changed_at = $7)
             RETURNING lifecycle_changed_at"
        ))
        .bind(sandbox)
        .bind(SandboxLifecycle::Stopped.as_str())
        .bind(SandboxLifecycle::Starting.as_str())
        .bind(claim.prev_placement.as_deref())
        .bind(claim.prev_placement.is_some())
        .bind(claim.prev_active_node_id)
        .bind(starting_at)
        .fetch_optional(&mut *tx)
        .await
        .expect("rolling back undispatched starting sandbox to stopped");
        let Some(rolled_back) = rolled_back else {
            return Ok(StartingRecovery::Settled);
        };
        store::clear_leased_starting_on_target(&mut *tx, item)
            .await
            .expect("clearing starting_on_target during undispatched fake start recovery");
        let restored = StopFence {
            initiated_at: claim.stop.initiated_at,
            dispatched_at: claim.stop.dispatched_at,
            stopped_at: claim.stop.stopped_at,
            settled_stopped_at: to_jiff(rolled_back),
            k8s_stopped_execution: claim.stop.k8s_stopped_execution.clone(),
        };
        tx.commit()
            .await
            .expect("committing undispatched fake start recovery");
        {
            let mut state = self.state();
            if state.item_stopped_at.get(&key) == Some(&RecordedStop::StartingOnTarget(claim)) {
                state
                    .item_stopped_at
                    .insert(key, RecordedStop::Stopped(restored.clone()));
            }
        }
        Ok(StartingRecovery::RestoredStopped(restored))
    }

    async fn item_stop(&self, item: &LeasedItem) -> Result<Option<RecordedStop>, DispatchError> {
        let sandbox = item.subject_id;
        let (recorded, delete_after_read) = {
            let mut state = self.state();
            if let Some(error) = state
                .stopped_at_read_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front)
            {
                return Err(error);
            }
            (
                state
                    .item_stopped_at
                    .get(&(item.campaign_id, sandbox))
                    .cloned(),
                state.delete_on_stopped_at_read.remove(&sandbox),
            )
        };
        if delete_after_read && let Some(pool) = &self.persist_pool {
            sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
                .bind(sandbox)
                .execute(pool)
                .await
                .expect("deleting sandbox after fake stopped_at read");
        }
        Ok(recorded)
    }

    async fn clear_stopped_at(
        &self,
        item: &LeasedItem,
        expected: RecordedStop,
    ) -> Result<(), DispatchError> {
        {
            let mut state = self.state();
            if let Some(error) = state
                .clear_stopped_at_errors
                .get_mut(&item.subject_id)
                .and_then(VecDeque::pop_front)
            {
                return Err(error);
            }
        }
        let key = (item.campaign_id, item.subject_id);
        if let Some(pool) = &self.persist_pool {
            let mut tx = pool
                .begin()
                .await
                .expect("opening transaction before clearing fake stopped_at");
            let _locked_phase: Option<String> = sqlx::query_scalar(sql!(
                "SELECT phase
                 FROM runtime_upgrade_items
                 WHERE campaign_id = $1
                   AND subject_kind = $2
                   AND subject_id = $3
                 FOR UPDATE"
            ))
            .bind(item.campaign_id)
            .bind(item.subject_kind.as_str())
            .bind(item.subject_id)
            .fetch_optional(&mut *tx)
            .await
            .expect("locking campaign item row before clearing fake stopped_at");
            if !has_active_item_lease(&mut *tx, item).await {
                return Ok(());
            }
            let Some(continuing) = store::continuing_run(&mut *tx, item)
                .await
                .expect("checking active lease and continuation before clearing fake stopped_at")
            else {
                return Ok(());
            };
            let should_clear = {
                let state = self.state();
                match state.item_stopped_at.get(&key) {
                    Some(
                        current @ (RecordedStop::Initiated(_)
                        | RecordedStop::Stopped(_)
                        | RecordedStop::StartingOnTarget(_)),
                    ) => current == &expected,
                    Some(current @ RecordedStop::StartedOnTarget(_)) => {
                        !continuing && current == &expected
                    }
                    None => false,
                }
            };
            if should_clear {
                if matches!(expected, RecordedStop::StartingOnTarget(_)) {
                    store::clear_leased_starting_on_target(&mut *tx, item)
                        .await
                        .expect("clearing starting_on_target during clear_stopped_at");
                }
                tx.commit()
                    .await
                    .expect("committing cleared fake stopped_at");
                let mut state = self.state();
                let still_matches = match state.item_stopped_at.get(&key) {
                    Some(
                        current @ (RecordedStop::Initiated(_)
                        | RecordedStop::Stopped(_)
                        | RecordedStop::StartingOnTarget(_)),
                    ) => current == &expected,
                    Some(current @ RecordedStop::StartedOnTarget(_)) => {
                        !continuing && current == &expected
                    }
                    None => false,
                };
                if still_matches {
                    state.item_stopped_at.remove(&key);
                    state.node_admitted_campaign_stops.remove(&key);
                    state.node_dispatched_campaign_starts.remove(&key);
                }
            }
            return Ok(());
        }
        let mut state = self.state();
        let should_clear = match state.item_stopped_at.get(&key) {
            Some(
                current @ (RecordedStop::Initiated(_)
                | RecordedStop::Stopped(_)
                | RecordedStop::StartingOnTarget(_)),
            ) => current == &expected,
            Some(current @ RecordedStop::StartedOnTarget(_)) => current == &expected,
            None => false,
        };
        if should_clear {
            state.item_stopped_at.remove(&key);
            state.node_admitted_campaign_stops.remove(&key);
            state.node_dispatched_campaign_starts.remove(&key);
        }
        Ok(())
    }
}
