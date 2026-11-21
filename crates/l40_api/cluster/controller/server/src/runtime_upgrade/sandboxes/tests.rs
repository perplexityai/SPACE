//! The sandbox driver under the reconciler, against a real Postgres and a
//! fake dispatcher. The loop's own steps (the budget, the canary, leases,
//! passes) are under test in `runtime_upgrade::tests`; these cover what the
//! driver decides: which sandboxes are on the source, which lifecycles count,
//! and what each item's upgrade does.

use std::collections::{HashMap, VecDeque};
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Stop(Uuid),
    Upgrade(Uuid),
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
    /// Upgrades in progress, by node, and the most seen at once.
    in_flight: HashMap<Option<NodeId>, u64>,
    max_in_flight: u64,
    max_on_node: u64,
    /// Results to return per sandbox, in order; `Upgraded` once exhausted.
    script: HashMap<Uuid, VecDeque<Result<StoppedUpgrade, DispatchError>>>,
    /// The result for every sandbox without a script.
    default: Option<Result<StoppedUpgrade, DispatchError>>,
}

/// Records every call and answers from a script. Each upgrade takes
/// `delay`, so the items of one tick overlap.
#[derive(Clone, Default)]
struct FakeDispatcher {
    state: Arc<Mutex<FakeState>>,
    nodes: Arc<HashMap<Uuid, NodeId>>,
    delay: Duration,
}

impl FakeDispatcher {
    fn with_nodes(nodes: HashMap<Uuid, NodeId>) -> Self {
        Self {
            nodes: Arc::new(nodes),
            delay: Duration::from_millis(20),
            ..Self::default()
        }
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
                Call::Stop(_) => None,
            })
            .collect();
        upgraded.sort();
        upgraded
    }

    fn peaks(&self) -> (u64, u64) {
        let state = self.state();
        (state.max_in_flight, state.max_on_node)
    }
}

impl SandboxUpgradeDispatcher for FakeDispatcher {
    async fn upgrade_stopped(
        &self,
        sandbox_id: SandboxId,
        target: EnvironmentReference,
    ) -> Result<StoppedUpgrade, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let node = self.nodes.get(&sandbox).copied();
        {
            let mut state = self.state();
            state.calls.push(Call::Upgrade(sandbox));
            state.targets.push(target);
            *state.in_flight.entry(node).or_default() += 1;
            let total = state.in_flight.values().sum();
            state.max_in_flight = state.max_in_flight.max(total);
            if node.is_some() {
                let on_node = state.in_flight[&node];
                state.max_on_node = state.max_on_node.max(on_node);
            }
        }
        tokio::time::sleep(self.delay).await;
        let mut state = self.state();
        *state.in_flight.entry(node).or_default() -= 1;
        let scripted = state.script.get_mut(&sandbox).and_then(VecDeque::pop_front);
        scripted
            .or_else(|| state.default.clone())
            .unwrap_or(Ok(StoppedUpgrade::Upgraded))
    }

    async fn stop(
        &self,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
    ) -> Result<GuardedStop, DispatchError> {
        let sandbox = sandbox_id.as_uuid();
        let mut state = self.state();
        state.calls.push(Call::Stop(sandbox));
        state.stop_sources.push(source);
        Ok(state
            .stop_script
            .remove(&sandbox)
            .unwrap_or(GuardedStop::Stopped))
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
