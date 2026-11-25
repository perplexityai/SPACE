//! The sandbox driver under the reconciler, against a real Postgres and a
//! fake dispatcher. The loop's own steps (the budget, the canary, leases,
//! passes) are under test in `runtime_upgrade::tests`; these cover what the
//! driver decides: which sandboxes are on the source, which lifecycles count,
//! and what each item's upgrade does.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_content_digest::Digest;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDb, insert_ready_worker_node,
    try_pg_test_db_with_max_connections,
};
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};
use pplx_space_runtime_environment_policy_store::{PolicyChange, PolicyChangeReason};
use pplx_space_runtime_upgrade_campaign_store::test_support::{CampaignSeed, insert_campaign};
use pplx_space_runtime_upgrade_campaign_store::{
    CampaignId, CampaignPhase, ItemCounts, SubjectKind,
};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::super::{Drivers, Reconciler, ReconcilerConfig, TickReport};
use super::*;

mod force_restart_recovery;
mod force_restarts;
mod moves;
mod passes;

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// The environment campaigns move off.
fn source() -> EnvironmentReference {
    environment(0)
}

/// The cluster default campaigns move to.
fn target() -> EnvironmentReference {
    environment(1)
}

const K8S: SubstrateKind = SubstrateKind::Kubernetes;

async fn test_db() -> TestResult<Option<TestDb>> {
    try_pg_test_db_with_max_connections(8).await
}

fn change() -> PolicyChange {
    PolicyChange {
        actor: None,
        reason: PolicyChangeReason::new("runtime upgrade test").expect("valid reason"),
    }
}

fn to_jiff(value: time::OffsetDateTime) -> jiff::Timestamp {
    jiff::Timestamp::from_nanosecond(value.unix_timestamp_nanos()).expect("valid test timestamp")
}

fn to_offset_date_time(value: jiff::Timestamp) -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp_nanos(value.as_nanosecond())
        .expect("valid sqlx timestamp")
}

/// The catalog and policy a campaign off `source()` runs under: both
/// environments known, the source in `state`, the target the default, and
/// an upgrade edge between them when `edge` is set.
async fn seed_policy(pool: &PgPool, state: RuntimeEnvironmentState, edge: bool) -> TestResult {
    for (environment, edges) in [
        (
            source(),
            if edge {
                vec![target().to_string()]
            } else {
                vec![]
            },
        ),
        (target(), vec![]),
    ] {
        sqlx::query(sql!(
            "INSERT INTO runtime_environments (
                 environment_ref, runtime, runtime_version, version_sort_key,
                 kernel_version, state_format_name, state_format_version, upgrade_edges
             )
             VALUES ($1, 'firecracker', $2, '{1}', '6.1.155', 'firecracker-snapshot', $3, $4)"
        ))
        .bind(environment.to_string())
        .bind(if environment == source() {
            "1.15.1"
        } else {
            "1.17.0"
        })
        .bind(if environment == source() {
            "9.0.0"
        } else {
            "12.0.0"
        })
        .bind(edges)
        .execute(pool)
        .await?;
    }
    let mut tx = pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_default(&mut tx, target(), &change()).await?;
    pplx_space_runtime_environment_policy_store::set_environment_state(
        &mut tx,
        source(),
        state,
        &change(),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The campaigns these tests drive: from `source()` to `target()`, with room
/// for several sandboxes on one node.
fn seed() -> CampaignSeed {
    CampaignSeed {
        from: source(),
        to: target(),
        per_node: 5,
        failure_budget: 5,
        ..CampaignSeed::default()
    }
}

/// What a sandbox's node last published about its execution.
enum Published {
    /// Bound to an environment.
    Bound(EnvironmentReference),
    /// Unbound: runtime version, kernel and state format version only.
    Unbound {
        runtime_version: &'static str,
        kernel: &'static str,
        state_format_version: &'static str,
    },
    /// Unbound from before execution metadata: kernel component only, with no
    /// runtime or state-format version.
    PreMetadata { kernel: &'static str },
    /// A publish that carried no execution.
    Nothing,
}

async fn insert_sandbox(
    pool: &PgPool,
    managed_by: SubstrateKind,
    lifecycle: SandboxLifecycle,
    node: NodeId,
    published: Published,
) -> TestResult<Uuid> {
    let sandbox_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         VALUES ($1, $2, $3, $4, $5, 'on_node', 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle.as_str())
    .execute(pool)
    .await?;
    let (metadata, runtime_version, state_format_version) = match published {
        Published::Bound(environment) => (
            Some(serde_json::json!({ "environment": environment.to_string() })),
            None,
            None,
        ),
        Published::Unbound {
            runtime_version,
            kernel,
            state_format_version,
        } => (
            Some(serde_json::json!({ "components": [{ "kind": "kernel", "version": kernel }] })),
            Some(runtime_version),
            Some(state_format_version),
        ),
        Published::PreMetadata { kernel } => (
            Some(serde_json::json!({ "components": [{ "kind": "kernel", "version": kernel }] })),
            None,
            None,
        ),
        Published::Nothing => (None, None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, runtime, runtime_version, state_format_name,
             state_format_version, execution_metadata
         )
         VALUES ($1, $2, 'firecracker', $3, 'firecracker-snapshot', $4, $5)"
    ))
    .bind(sandbox_id)
    .bind(node)
    .bind(runtime_version)
    .bind(state_format_version)
    .bind(metadata)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// A stopped Kubernetes sandbox on the source environment.
async fn stopped_on_source(pool: &PgPool, node: NodeId) -> TestResult<Uuid> {
    insert_sandbox(
        pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(source()),
    )
    .await
}

/// An unbound sandbox whose execution matches no catalog environment.
async fn unattributable(
    pool: &PgPool,
    node: NodeId,
    runtime_version: &'static str,
) -> TestResult<Uuid> {
    insert_sandbox(
        pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version,
            kernel: "5.10.0",
            state_format_version: "9.0.0",
        },
    )
    .await
}

/// A worker node whose status is reachable evidence of its environments.
async fn node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    upsert_snapshot(pool, node, SnapshotSeed::default()).await?;
    Ok(node)
}

/// Record `sandboxes` as pending items of `campaign`, as a scan would.
async fn record(pool: &PgPool, campaign: CampaignId, sandboxes: &[Uuid]) -> TestResult {
    store::insert_pending(
        pool,
        campaign,
        SubjectKind::Sandbox,
        sandboxes,
        MAX_ATTEMPTS,
    )
    .await?;
    Ok(())
}

async fn counts(pool: &PgPool, campaign: CampaignId) -> TestResult<ItemCounts> {
    Ok(store::item_counts(pool, campaign).await?)
}

async fn campaign_phase(pool: &PgPool, campaign: CampaignId) -> TestResult<String> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT phase FROM runtime_upgrade_campaigns WHERE id = $1"
    ))
    .bind(campaign)
    .fetch_one(pool)
    .await?)
}

/// An item's phase, error and attempts.
async fn item(
    pool: &PgPool,
    campaign: CampaignId,
    sandbox: Uuid,
) -> TestResult<(String, Option<String>, i64)> {
    Ok(sqlx::query_as::<_, (String, Option<String>, i64)>(sql!(
        "SELECT phase, last_error, attempts FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .fetch_one(pool)
    .await?)
}

async fn set_lifecycle(pool: &PgPool, sandbox: Uuid, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

async fn set_bound(pool: &PgPool, sandbox: Uuid, environment: EnvironmentReference) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states
         SET execution_metadata = $2,
             runtime_version = NULL,
             state_format_version = NULL
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .bind(serde_json::json!({ "environment": environment.to_string() }))
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Stop(Uuid),
    Upgrade(Uuid),
    Start(Uuid),
}

#[derive(Default)]
struct FakeState {
    calls: Vec<Call>,
    /// The target each upgrade was asked for, in call order.
    targets: Vec<EnvironmentReference>,
    /// The source each stop was guarded on, in call order.
    stop_sources: Vec<EnvironmentReference>,
    /// What a stop returns per sandbox; `Stopped` otherwise.
    stop_script: HashMap<Uuid, GuardedStop>,
    /// Pre-commit errors a stop returns per sandbox, in order, before touching
    /// Postgres or recording [`ForceRestartProgress`].
    stop_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Post-commit errors a stop returns per sandbox after committing the
    /// guarded stop in Postgres and recording [`ForceRestartProgress::Stopped`].
    post_commit_stop_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// Virtual-clock advance a stop performs before hanging indefinitely after
    /// committing the guarded stop and [`ForceRestartProgress::Stopped`],
    /// deterministically tripping `item_deadline` without wall-clock races.
    post_commit_stop_deadline_advances: HashMap<Uuid, Duration>,
    /// Durable force-restart progress persisted in the substrate by
    /// `(campaign_id, sandbox_id)`.
    force_restart_progress: HashMap<(CampaignId, Uuid), ForceRestartProgress>,
    /// Errors `force_restart_progress` returns per sandbox, in order.
    progress_read_errors: HashMap<Uuid, VecDeque<DispatchError>>,
    /// The target each start was asked for, in call order.
    start_targets: Vec<EnvironmentReference>,
    /// The `(sandbox, stopped_at, target)` fence each start received, in call order.
    start_fences: Vec<(Uuid, jiff::Timestamp, EnvironmentReference)>,
    /// Sandboxes that a user starts and stops again right before `start` claims
    /// the stopped row, exercising `stopped_at` fencing inside `start`.
    user_restarts_before_start: HashSet<Uuid>,
    /// Results a start returns per sandbox, in order; `Started` once exhausted.
    start_script: HashMap<Uuid, VecDeque<Result<TargetStart, DispatchError>>>,
    /// The result for every start without a script.
    start_default: Option<Result<TargetStart, DispatchError>>,
    /// Upgrades or starts in progress, by node, and the most seen at once.
    in_flight: HashMap<Option<NodeId>, u64>,
    max_in_flight: u64,
    max_on_node: u64,
    /// Results to return per sandbox, in order; `Upgraded` once exhausted.
    script: HashMap<Uuid, VecDeque<Result<StoppedUpgrade, DispatchError>>>,
    /// The result for every sandbox without a script.
    default: Option<Result<StoppedUpgrade, DispatchError>>,
}

/// Records every call and answers from a script. Each upgrade or start takes
/// `delay`, so the items of one tick overlap.
#[derive(Clone, Default)]
struct FakeDispatcher {
    state: Arc<Mutex<FakeState>>,
    nodes: Arc<HashMap<Uuid, NodeId>>,
    delay: Duration,
    /// When set, `GuardedStop::Stopped` writes `stopped` to `sandboxes` and
    /// `TargetStart::Started` writes `started` on `target` to Postgres.
    persist_pool: Option<PgPool>,
}

impl FakeDispatcher {
    fn with_nodes(nodes: HashMap<Uuid, NodeId>) -> Self {
        Self {
            nodes: Arc::new(nodes),
            delay: Duration::from_millis(20),
            ..Self::default()
        }
    }

    fn persisting(mut self, pool: PgPool) -> Self {
        self.persist_pool = Some(pool);
        self
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    fn answer_all(&self, result: Result<StoppedUpgrade, DispatchError>) {
        self.state().default = Some(result);
    }

    fn script(&self, sandbox: Uuid, results: Vec<Result<StoppedUpgrade, DispatchError>>) {
        self.state().script.insert(sandbox, results.into());
    }

    fn script_start(&self, sandbox: Uuid, results: Vec<Result<TargetStart, DispatchError>>) {
        self.state().start_script.insert(sandbox, results.into());
    }

    fn tracked_driver_stops(&self) -> usize {
        self.state()
            .force_restart_progress
            .values()
            .filter(|entry| matches!(entry, ForceRestartProgress::Stopped { .. }))
            .count()
    }

    fn tracked_started_on_target(&self) -> usize {
        self.state()
            .force_restart_progress
            .values()
            .filter(|entry| matches!(entry, ForceRestartProgress::StartedOnTarget))
            .count()
    }

    fn calls(&self) -> Vec<Call> {
        self.state().calls.clone()
    }

    /// The sandboxes upgrades were asked for, sorted, repeats included.
    fn upgraded(&self) -> Vec<Uuid> {
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
    fn started(&self) -> Vec<Uuid> {
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

    fn peaks(&self) -> (u64, u64) {
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
        sandbox_id: SandboxId,
        target: EnvironmentReference,
    ) -> Result<StoppedUpgrade, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        self.state().targets.push(target);
        let node = self.enter_in_flight(sandbox, Call::Upgrade(sandbox));
        tokio::time::sleep(self.delay).await;
        let outcome = {
            let mut state = self.state();
            *state.in_flight.entry(node).or_default() -= 1;
            let scripted = state.script.get_mut(&sandbox).and_then(VecDeque::pop_front);
            scripted
                .or_else(|| state.default.clone())
                .unwrap_or(Ok(StoppedUpgrade::Upgraded))
        };
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
        campaign_id: Option<CampaignId>,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
    ) -> Result<GuardedStop, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let scripted = {
            let mut state = self.state();
            state.calls.push(Call::Stop(sandbox));
            state.stop_sources.push(source);
            if let Some(error) = state
                .stop_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front)
            {
                return Err(error);
            }
            state.stop_script.remove(&sandbox)
        };
        if let Some(scripted) = scripted {
            if let (Some(campaign_id), GuardedStop::Stopped { stopped_at }) =
                (campaign_id, &scripted)
            {
                self.state().force_restart_progress.insert(
                    (campaign_id, sandbox),
                    ForceRestartProgress::Stopped {
                        stopped_at: *stopped_at,
                    },
                );
            }
            return Ok(scripted);
        }
        let stopped_at = if let Some(pool) = &self.persist_pool {
            let changed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
                "UPDATE sandboxes
                 SET lifecycle = $2
                 WHERE id = $1
                 RETURNING lifecycle_changed_at"
            ))
            .bind(sandbox)
            .bind(SandboxLifecycle::Stopped.as_str())
            .fetch_one(pool)
            .await
            .expect("persisting fake stop");
            to_jiff(changed_at)
        } else {
            jiff::Timestamp::UNIX_EPOCH
        };
        let (post_deadline_advance, post_error) = {
            let mut state = self.state();
            if let Some(campaign_id) = campaign_id {
                state.force_restart_progress.insert(
                    (campaign_id, sandbox),
                    ForceRestartProgress::Stopped { stopped_at },
                );
            }
            let advance = state.post_commit_stop_deadline_advances.remove(&sandbox);
            let error = state
                .post_commit_stop_errors
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            (advance, error)
        };
        if let Some(advance) = post_deadline_advance {
            struct ResumeClockOnDrop;
            impl Drop for ResumeClockOnDrop {
                fn drop(&mut self) {
                    tokio::time::resume();
                }
            }
            let _resume_clock = ResumeClockOnDrop;
            tokio::time::pause();
            tokio::time::advance(advance).await;
            std::future::pending::<()>().await;
        }
        if let Some(error) = post_error {
            return Err(error);
        }
        Ok(GuardedStop::Stopped { stopped_at })
    }

    async fn start(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
        stopped_at: jiff::Timestamp,
        target: EnvironmentReference,
    ) -> Result<TargetStart, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        {
            let mut state = self.state();
            state.start_targets.push(target);
            state.start_fences.push((sandbox, stopped_at, target));
        }
        let node = self.enter_in_flight(sandbox, Call::Start(sandbox));
        tokio::time::sleep(self.delay).await;
        let (explicit_outcome, user_restarted) = {
            let mut state = self.state();
            *state.in_flight.entry(node).or_default() -= 1;
            let user_restarted = state.user_restarts_before_start.remove(&sandbox);
            let scripted = state
                .start_script
                .get_mut(&sandbox)
                .and_then(VecDeque::pop_front);
            (
                scripted.or_else(|| state.start_default.clone()),
                user_restarted,
            )
        };
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
        if matches!(
            outcome,
            Ok(TargetStart::Started | TargetStart::AlreadyOnTarget)
        ) && let Some(pool) = &self.persist_pool
        {
            let already_on_target: bool = sqlx::query_scalar(sql!(
                "SELECT execution_metadata ->> 'environment' = $2
                 FROM sandbox_observed_states
                 WHERE sandbox_id = $1"
            ))
            .bind(sandbox)
            .bind(target.to_string())
            .fetch_optional(pool)
            .await
            .expect("reading recorded environment before fake start")
            .flatten()
            .unwrap_or(false);
            let claimed = sqlx::query(sql!(
                "UPDATE sandboxes
                 SET lifecycle = $3
                 WHERE id = $1
                   AND lifecycle = $2
                   AND lifecycle_changed_at = $4"
            ))
            .bind(sandbox)
            .bind(SandboxLifecycle::Stopped.as_str())
            .bind(SandboxLifecycle::Started.as_str())
            .bind(to_offset_date_time(stopped_at))
            .execute(pool)
            .await
            .expect("persisting fake start lifecycle")
            .rows_affected();
            if claimed == 0 {
                self.state()
                    .force_restart_progress
                    .remove(&(campaign_id, sandbox));
                return Ok(TargetStart::NotOnTarget(
                    "the sandbox changed lifecycle since this campaign stopped it".to_owned(),
                ));
            }
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
            if explicit_outcome.is_none() && already_on_target {
                outcome = Ok(TargetStart::AlreadyOnTarget);
            }
        }
        match &outcome {
            Ok(TargetStart::Started | TargetStart::AlreadyOnTarget) => {
                self.state().force_restart_progress.insert(
                    (campaign_id, sandbox),
                    ForceRestartProgress::StartedOnTarget,
                );
            }
            Ok(TargetStart::NotOnTarget(_)) => {
                self.state()
                    .force_restart_progress
                    .remove(&(campaign_id, sandbox));
            }
            Ok(TargetStart::HeldStopped(_)) | Err(_) => {}
        }
        outcome
    }

    async fn force_restart_progress(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
    ) -> Result<Option<ForceRestartProgress>, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let mut state = self.state();
        if let Some(error) = state
            .progress_read_errors
            .get_mut(&sandbox)
            .and_then(VecDeque::pop_front)
        {
            return Err(error);
        }
        Ok(state
            .force_restart_progress
            .get(&(campaign_id, sandbox))
            .copied())
    }

    async fn clear_force_restart_progress(
        &self,
        campaign_id: CampaignId,
        sandbox_id: SandboxId,
    ) -> Result<(), DispatchError> {
        self.state()
            .force_restart_progress
            .remove(&(campaign_id, sandbox_id.as_uuid()));
        Ok(())
    }

    fn retain_force_restart_campaigns(&self, unfinished: &[CampaignId]) {
        self.state()
            .force_restart_progress
            .retain(|(campaign_id, _), _| unfinished.contains(campaign_id));
    }
}

#[derive(Default)]
struct FakeMemoryRetirementState {
    retired: HashSet<EnvironmentReference>,
    generation: u64,
    script: VecDeque<Result<bool, String>>,
}

#[derive(Clone, Default)]
struct FakeMemoryRetirement {
    state: Arc<Mutex<FakeMemoryRetirementState>>,
}

impl FakeMemoryRetirement {
    fn retired(environment: EnvironmentReference) -> Self {
        let source = Self::default();
        source.retire(environment);
        source
    }

    fn retire(&self, environment: EnvironmentReference) {
        let mut state = self.state.lock().unwrap();
        if state.retired.insert(environment) {
            state.generation += 1;
        }
    }

    fn unretire(&self, environment: EnvironmentReference) {
        let mut state = self.state.lock().unwrap();
        if state.retired.remove(&environment) {
            state.generation += 1;
        }
    }

    fn script_reads(&self, reads: impl IntoIterator<Item = Result<bool, String>>) {
        self.state.lock().unwrap().script.extend(reads);
    }
}

impl MemoryRetirementSource for FakeMemoryRetirement {
    async fn memory_retirement(
        &self,
        environment: EnvironmentReference,
    ) -> Result<MemoryRetirement, String> {
        let mut state = self.state.lock().unwrap();
        let generation = state.generation;
        let retired = match state.script.pop_front() {
            Some(scripted) => scripted?,
            None => state.retired.contains(&environment),
        };
        Ok(MemoryRetirement {
            retired,
            generation,
        })
    }
}

/// A reconciler that runs only the sandbox driver over `dispatcher`,
/// examining `page` sandboxes per tick.
fn paging(pool: &PgPool, dispatcher: FakeDispatcher, page: u32) -> Reconciler {
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(SandboxDriver::new(pool.clone(), K8S.as_str(), dispatcher)),
        ReconcilerConfig {
            enabled: true,
            candidate_page: page,
            ..ReconcilerConfig::default()
        },
    )
}

fn reconciler(pool: &PgPool, dispatcher: FakeDispatcher) -> Reconciler {
    paging(pool, dispatcher, ReconcilerConfig::default().candidate_page)
}

fn force_restart_paging(
    pool: &PgPool,
    dispatcher: FakeDispatcher,
    memory_retirement: FakeMemoryRetirement,
    page: u32,
) -> Reconciler {
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            pool.clone(),
            K8S.as_str(),
            dispatcher,
            memory_retirement,
        )),
        ReconcilerConfig {
            enabled: true,
            candidate_page: page,
            ..ReconcilerConfig::default()
        },
    )
}

fn force_restart_reconciler(
    pool: &PgPool,
    dispatcher: FakeDispatcher,
    memory_retirement: FakeMemoryRetirement,
) -> Reconciler {
    force_restart_paging(
        pool,
        dispatcher,
        memory_retirement,
        ReconcilerConfig::default().candidate_page,
    )
}

fn force_restart_seed() -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::SandboxForceRestarts,
        ..seed()
    }
}

async fn resident_on_source(
    pool: &PgPool,
    lifecycle: SandboxLifecycle,
    node: NodeId,
) -> TestResult<Uuid> {
    insert_sandbox(pool, K8S, lifecycle, node, Published::Bound(source())).await
}

/// One tick, and every dispatch it started.
async fn tick(reconciler: &Reconciler) -> TickReport {
    let report = reconciler.tick().await;
    reconciler.settle().await;
    report
}

/// Tick until `campaign` leaves `rolling` or `limit` ticks pass.
async fn tick_until_finished(
    reconciler: &Reconciler,
    pool: &PgPool,
    campaign: CampaignId,
    limit: usize,
) -> TestResult<usize> {
    for ticks in 1..=limit {
        tick(reconciler).await;
        if campaign_phase(pool, campaign).await? != CampaignPhase::Rolling.as_str() {
            return Ok(ticks);
        }
    }
    Ok(limit)
}
