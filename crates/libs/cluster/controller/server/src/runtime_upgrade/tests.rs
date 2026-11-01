//! The reconciler against a real Postgres and a fake dispatcher.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    Digest, EnvironmentReference, NodeId, SandboxId, SandboxLifecycle, SubstrateKind,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDb, insert_ready_worker_node,
    try_pg_test_db_with_max_connections,
};
use pplx_space_runtime_environment_policy_store::{
    PolicyChange, PolicyChangeReason, RuntimeEnvironmentState,
};
use pplx_space_runtime_upgrade_campaign_store::test_support::{CampaignSeed, insert_campaign};
use pplx_space_runtime_upgrade_campaign_store::{CampaignId, CampaignPhase, ItemCounts};
use pplx_space_test_env::{TestEnv, TestResult};
use sqlx::PgPool;

use super::*;

mod disabled;
mod leases;

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
}

async fn insert_sandbox(
    pool: &PgPool,
    managed_by: SubstrateKind,
    lifecycle: SandboxLifecycle,
    node: NodeId,
    published: Published,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
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
            serde_json::json!({ "environment": environment.to_string() }),
            None,
            None,
        ),
        Published::Unbound {
            runtime_version,
            kernel,
            state_format_version,
        } => (
            serde_json::json!({ "components": [{ "kind": "kernel", "version": kernel }] }),
            Some(runtime_version),
            Some(state_format_version),
        ),
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
async fn stopped_on_source(pool: &PgPool, node: NodeId) -> TestResult<SandboxId> {
    insert_sandbox(
        pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(source()),
    )
    .await
}

async fn node(pool: &PgPool) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;
    Ok(node)
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

async fn item(
    pool: &PgPool,
    campaign: CampaignId,
    sandbox: SandboxId,
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Stop(SandboxId),
    Upgrade(SandboxId),
}

#[derive(Default)]
struct FakeState {
    calls: Vec<Call>,
    /// The source and target each upgrade was asked for, in call order.
    targets: Vec<(EnvironmentReference, EnvironmentReference)>,
    /// The source each stop was guarded on, in call order.
    stop_sources: Vec<EnvironmentReference>,
    /// What a stop returns per sandbox; `Stopped` otherwise.
    stop_script: HashMap<SandboxId, GuardedStop>,
    /// Upgrades that ran to the end rather than being dropped.
    finished: u64,
    /// Upgrades in progress, by node, and the most seen at once.
    in_flight: HashMap<Option<NodeId>, u64>,
    max_in_flight: u64,
    max_on_node: u64,
    /// Results to return per sandbox, in order; `Upgraded` once exhausted.
    script: HashMap<SandboxId, VecDeque<Result<StoppedUpgrade, DispatchError>>>,
    /// The result for every sandbox without a script.
    default: Option<Result<StoppedUpgrade, DispatchError>>,
}

/// Records every call and answers from a script. Each upgrade takes
/// `delay`, so the items of one tick overlap.
#[derive(Clone, Default)]
struct FakeDispatcher {
    state: Arc<Mutex<FakeState>>,
    nodes: Arc<HashMap<SandboxId, NodeId>>,
    delay: Duration,
}

impl FakeDispatcher {
    fn with_nodes(nodes: HashMap<SandboxId, NodeId>) -> Self {
        Self {
            nodes: Arc::new(nodes),
            delay: Duration::from_millis(20),
            ..Self::default()
        }
    }

    fn answer_all(&self, result: Result<StoppedUpgrade, DispatchError>) {
        self.state.lock().unwrap().default = Some(result);
    }

    fn script(&self, sandbox: SandboxId, results: Vec<Result<StoppedUpgrade, DispatchError>>) {
        self.state
            .lock()
            .unwrap()
            .script
            .insert(sandbox, results.into());
    }

    fn calls(&self) -> Vec<Call> {
        self.state.lock().unwrap().calls.clone()
    }

    fn upgraded(&self) -> Vec<SandboxId> {
        let mut upgraded: Vec<SandboxId> = self
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
        let state = self.state.lock().unwrap();
        (state.max_in_flight, state.max_on_node)
    }
}

impl SandboxUpgradeDispatcher for FakeDispatcher {
    async fn upgrade_stopped(
        &self,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
        target: EnvironmentReference,
    ) -> Result<StoppedUpgrade, DispatchError> {
        let node = self.nodes.get(&sandbox_id).copied();
        {
            let mut state = self.state.lock().unwrap();
            state.calls.push(Call::Upgrade(sandbox_id));
            state.targets.push((source, target));
            *state.in_flight.entry(node).or_default() += 1;
            let total = state.in_flight.values().sum();
            state.max_in_flight = state.max_in_flight.max(total);
            if node.is_some() {
                let on_node = state.in_flight[&node];
                state.max_on_node = state.max_on_node.max(on_node);
            }
        }
        tokio::time::sleep(self.delay).await;
        let mut state = self.state.lock().unwrap();
        state.finished += 1;
        *state.in_flight.entry(node).or_default() -= 1;
        let scripted = state
            .script
            .get_mut(&sandbox_id)
            .and_then(VecDeque::pop_front);
        scripted
            .or_else(|| state.default.clone())
            .unwrap_or(Ok(StoppedUpgrade::Upgraded))
    }

    async fn stop(
        &self,
        sandbox_id: SandboxId,
        source: EnvironmentReference,
    ) -> Result<GuardedStop, DispatchError> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(Call::Stop(sandbox_id));
        state.stop_sources.push(source);
        Ok(state
            .stop_script
            .remove(&sandbox_id)
            .unwrap_or(GuardedStop::Stopped))
    }
}

fn reconciler(pool: &PgPool, dispatcher: FakeDispatcher) -> Reconciler<FakeDispatcher> {
    Reconciler::new(
        pool.clone(),
        K8S,
        dispatcher,
        ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        },
    )
}

/// One tick, and every dispatch it started.
async fn tick(reconciler: &Reconciler<FakeDispatcher>) -> TickReport {
    let report = reconciler.tick().await;
    reconciler.settle().await;
    report
}

/// Tick until `campaign` leaves `rolling` or `limit` ticks pass.
async fn tick_until_finished(
    reconciler: &Reconciler<FakeDispatcher>,
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

#[tokio::test(flavor = "current_thread")]
async fn stopped_sandboxes_on_the_source_are_upgraded_within_the_caps_and_the_campaign_completes()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 1,
            per_cluster: 2,
            ..seed()
        },
    )
    .await?;
    let busy = node(&db.pool).await?;
    let quiet = node(&db.pool).await?;
    let mut nodes = HashMap::new();
    let mut expected = Vec::new();
    for node in [busy, busy, busy, quiet, quiet] {
        let sandbox = stopped_on_source(&db.pool, node).await?;
        nodes.insert(sandbox, node);
        expected.push(sandbox);
    }
    expected.sort();
    // Not this campaign's: another environment, a running sandbox it doesn't
    // take, and another substrate's sandbox.
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        quiet,
        Published::Bound(environment(7)),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Stopped,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    let dispatcher = FakeDispatcher::with_nodes(nodes);
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let ticks = tick_until_finished(&reconciler, &db.pool, campaign, 10).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded(), expected);
    assert_eq!(
        dispatcher.state.lock().unwrap().targets,
        vec![(source(), target()); 5],
        "every upgrade is guarded on the campaign's source and asked for its target"
    );
    assert_eq!(dispatcher.peaks(), (2, 1), "cluster cap 2, node cap 1");
    // The busy node's three sandboxes take a tick each, and the next tick
    // completes the drained campaign.
    assert_eq!(ticks, 4);
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            done: 5,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn running_and_paused_sandboxes_move_only_when_asked_and_their_source_is_blocked()
-> TestResult {
    for (state, include_running, takes_running) in [
        (RuntimeEnvironmentState::Blocked, true, true),
        (RuntimeEnvironmentState::Blocked, false, false),
        (RuntimeEnvironmentState::Deprecated, true, false),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                include_running,
                ..seed()
            },
        )
        .await?;
        let node = node(&db.pool).await?;
        let stopped = stopped_on_source(&db.pool, node).await?;
        let mut residents = Vec::new();
        for lifecycle in [SandboxLifecycle::Started, SandboxLifecycle::Paused] {
            residents.push(
                insert_sandbox(&db.pool, K8S, lifecycle, node, Published::Bound(source())).await?,
            );
        }
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

        let calls = dispatcher.calls();
        assert!(
            calls.contains(&Call::Upgrade(stopped)),
            "{state} {include_running}"
        );
        assert!(!calls.contains(&Call::Stop(stopped)));
        for resident in residents {
            let stop = calls.iter().position(|call| *call == Call::Stop(resident));
            let upgrade = calls
                .iter()
                .position(|call| *call == Call::Upgrade(resident));
            if takes_running {
                assert!(stop < upgrade && stop.is_some(), "stopped, then upgraded");
                assert!(
                    dispatcher
                        .state
                        .lock()
                        .unwrap()
                        .stop_sources
                        .iter()
                        .all(|guard| *guard == source()),
                    "every stop is guarded on the campaign's source"
                );
            } else {
                assert_eq!((stop, upgrade), (None, None), "{state} {include_running}");
            }
        }
        assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failures_pause_the_campaign_at_its_budget() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 1,
            per_cluster: 1,
            failure_budget: 2,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    for _ in 0..4 {
        stopped_on_source(&db.pool, node).await?;
    }
    let dispatcher = FakeDispatcher::default();
    dispatcher.answer_all(Err(DispatchError::Failed("the boot failed".to_owned())));
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let counters = pplx_space_metrics::test_capture::capture_counters_guard();
    let mut paused = Vec::new();
    for _ in 0..5 {
        paused.extend(tick(&reconciler).await.paused);
    }

    assert_eq!(paused, vec![campaign]);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "paused");
    assert_eq!(
        dispatcher.upgraded().len(),
        2,
        "nothing is leased once paused"
    );
    let counts = counts(&db.pool, campaign).await?;
    assert_eq!((counts.failed, counts.pending), (2, 2));
    let counters = counters.finish();
    let sum = |name: &str, tag: Option<&str>| -> u64 {
        counters
            .iter()
            .filter(|counter| {
                counter.name == name && tag.is_none_or(|tag| counter.tags.contains(&tag.to_owned()))
            })
            .map(|counter| counter.value)
            .sum()
    };
    assert_eq!(sum("runtime_upgrade.campaign.auto_paused", None), 1);
    assert_eq!(
        sum(
            "runtime_upgrade.campaign.item_outcome",
            Some("outcome:failed")
        ),
        2
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retryable_failures_retry_until_the_attempts_run_out() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            failure_budget: 5,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let flaky = stopped_on_source(&db.pool, node).await?;
    let recovers = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let busy = || {
        Err(DispatchError::Retryable(
            "another upgrade step is running".to_owned(),
        ))
    };
    dispatcher.script(flaky, vec![busy(); MAX_ATTEMPTS as usize]);
    dispatcher.script(recovers, vec![busy(), Ok(StoppedUpgrade::AlreadyOnTarget)]);
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    for _ in 0..MAX_ATTEMPTS + 1 {
        tick(&reconciler).await;
    }

    let (phase, error, attempts) = item(&db.pool, campaign, flaky).await?;
    assert_eq!(phase, "failed");
    assert_eq!(attempts, MAX_ATTEMPTS as i64);
    assert_eq!(
        error.as_deref(),
        Some("another upgrade step is running (gave up after 5 attempts)")
    );
    assert_eq!(item(&db.pool, campaign, recovers).await?.0, "done");
    assert_eq!(
        dispatcher
            .upgraded()
            .iter()
            .filter(|sandbox| **sandbox == flaky)
            .count(),
        MAX_ATTEMPTS as usize
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_is_held_while_the_policy_doesnt_call_for_its_move() -> TestResult {
    for (default_is_target, state, reason) in [
        (
            false,
            RuntimeEnvironmentState::Deprecated,
            "target_not_default",
        ),
        (true, RuntimeEnvironmentState::Allowed, "source_allowed"),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        if !default_is_target {
            let mut tx = db.pool.begin().await?;
            pplx_space_runtime_environment_policy_store::set_default(
                &mut tx,
                environment(5),
                &change(),
            )
            .await?;
            tx.commit().await?;
        }
        let campaign = insert_campaign(&db.pool, seed()).await?;
        let node = node(&db.pool).await?;
        stopped_on_source(&db.pool, node).await?;
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        tick(&reconciler).await;
        tick(&reconciler).await;

        assert_eq!(
            counts(&db.pool, campaign).await?,
            ItemCounts::default(),
            "{reason}"
        );
        assert!(dispatcher.calls().is_empty());
        assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
        let held: Vec<_> = counters
            .finish()
            .into_iter()
            .filter(|counter| counter.name == "runtime_upgrade.campaign.held")
            .collect();
        assert_eq!(held.len(), 2);
        assert!(held[0].tags.contains(&format!("reason:{reason}")));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandboxes_without_an_edge_are_counted_stranded_and_the_campaign_stays_open() -> TestResult
{
    for state in [
        RuntimeEnvironmentState::Blocked,
        RuntimeEnvironmentState::Deprecated,
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, false).await?;
        let campaign = insert_campaign(&db.pool, seed()).await?;
        let node = node(&db.pool).await?;
        for _ in 0..3 {
            stopped_on_source(&db.pool, node).await?;
        }
        // Recorded while an edge reached the target, which has since gone.
        let recorded = stopped_on_source(&db.pool, node).await?;
        store::insert_pending_sandboxes(&db.pool, campaign, &[recorded], MAX_ATTEMPTS).await?;
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        let values = pplx_space_metrics::test_capture::capture_values_guard();
        for _ in 0..3 {
            tick(&reconciler).await;
        }

        assert_eq!(
            campaign_phase(&db.pool, campaign).await?,
            "rolling",
            "{state}: a campaign that moves nothing stays open"
        );
        assert_eq!(
            counts(&db.pool, campaign).await?,
            ItemCounts {
                pending: 1,
                ..ItemCounts::default()
            },
            "{state}: nothing new recorded, and the earlier item isn't leased"
        );
        assert!(dispatcher.calls().is_empty());
        let stranded: Vec<f64> = values
            .finish()
            .into_iter()
            .filter(|value| value.name == "runtime_upgrade.campaign.stranded")
            .map(|value| value.value)
            .collect();
        assert_eq!(
            stranded,
            vec![3.0; 3],
            "{state}: reported at every pass's end"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_executions_are_attributed_through_the_catalog() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Matches the source's catalog row exactly.
    let matching = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
    )
    .await?;
    // Another kernel matches no catalog row.
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "5.10.0",
            state_format_version: "9.0.0",
        },
    )
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(dispatcher.upgraded(), vec![matching]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_canary_upgrades_one_sandbox_and_waits_for_resume() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    for _ in 0..3 {
        stopped_on_source(&db.pool, node).await?;
    }
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    for _ in 0..3 {
        tick(&reconciler).await;
    }
    assert_eq!(dispatcher.upgraded().len(), 1);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "canary");

    // An operator's resume releases the canary.
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'rolling' WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(dispatcher.upgraded().len(), 3);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandboxes_that_changed_since_they_were_found_are_skipped_or_retried() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let deleted = stopped_on_source(&db.pool, node).await?;
    let restarted = stopped_on_source(&db.pool, node).await?;
    let starting = stopped_on_source(&db.pool, node).await?;
    let not_moved = stopped_on_source(&db.pool, node).await?;
    let mut conn = db.pool.acquire().await?;
    store::insert_pending_sandboxes(
        &mut conn,
        campaign,
        &[deleted, restarted, starting, not_moved],
        MAX_ATTEMPTS,
    )
    .await?;
    drop(conn);
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(deleted)
        .execute(&db.pool)
        .await?;
    for (sandbox, lifecycle) in [(restarted, "started"), (starting, "starting")] {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox)
            .bind(lifecycle)
            .execute(&db.pool)
            .await?;
    }
    let dispatcher = FakeDispatcher::default();
    dispatcher.script(
        not_moved,
        vec![Ok(StoppedUpgrade::NotMoved(
            "the environment is allowed again".to_owned(),
        ))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;

    assert_eq!(
        item(&db.pool, campaign, deleted).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox was deleted".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, restarted).await?.1.as_deref(),
        Some("the sandbox is started, and this campaign moves only stopped sandboxes")
    );
    assert_eq!(
        item(&db.pool, campaign, starting).await?,
        (
            "pending".to_owned(),
            Some("the sandbox is starting; retrying once it settles".to_owned()),
            0
        ),
        "a sandbox mid-transition wasn't attempted"
    );
    assert_eq!(
        item(&db.pool, campaign, not_moved).await?,
        (
            "skipped".to_owned(),
            Some("the environment is allowed again".to_owned()),
            1
        )
    );
    assert_eq!(dispatcher.upgraded(), vec![not_moved]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_running_sandbox_is_stopped_only_while_it_and_the_policy_still_call_for_it() -> TestResult
{
    for (state, published, reason) in [
        // Found stopped; a start since has cold-booted it onto the target.
        (
            RuntimeEnvironmentState::Blocked,
            target(),
            "the sandbox is no longer on the campaign's source",
        ),
        // Still on the source, but the source is no longer blocked, so
        // nothing consents to losing its memory.
        (
            RuntimeEnvironmentState::Deprecated,
            source(),
            "the policy no longer moves this campaign's running sandboxes",
        ),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                include_running: true,
                ..seed()
            },
        )
        .await?;
        let node = node(&db.pool).await?;
        let sandbox = insert_sandbox(
            &db.pool,
            K8S,
            SandboxLifecycle::Started,
            node,
            Published::Bound(published),
        )
        .await?;
        let mut conn = db.pool.acquire().await?;
        store::insert_pending_sandboxes(&mut conn, campaign, &[sandbox], MAX_ATTEMPTS).await?;
        drop(conn);
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        tick(&reconciler).await;

        assert_eq!(dispatcher.calls(), Vec::new(), "{state}: nothing stopped");
        assert_eq!(
            item(&db.pool, campaign, sandbox).await?,
            ("skipped".to_owned(), Some(reason.to_owned()), 1),
            "{state}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stop_the_substrate_declines_skips_the_item_without_an_upgrade() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            include_running: true,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let sandbox = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::Bound(source()),
    )
    .await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.state.lock().unwrap().stop_script.insert(
        sandbox,
        GuardedStop::Declined("restarted on another environment".to_owned()),
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;

    assert_eq!(dispatcher.calls(), vec![Call::Stop(sandbox)]);
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        (
            "skipped".to_owned(),
            Some("restarted on another environment".to_owned()),
            1
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_campaigns_are_reported_but_not_driven() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::Templates,
            ..seed()
        },
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase)
         VALUES ($1, 'template_release', $2, 'pending')"
    ))
    .bind(campaign)
    .bind(uuid::Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let node = node(&db.pool).await?;
    stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let values = pplx_space_metrics::test_capture::capture_values_guard();
    tick(&reconciler).await;

    assert!(dispatcher.calls().is_empty());
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            ..ItemCounts::default()
        }
    );
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    let values = values.finish();
    let gauge = |name: &str, tags: &[String]| {
        values
            .iter()
            .filter(|value| value.name == name && tags.iter().all(|tag| value.tags.contains(tag)))
            .map(|value| value.value)
            .collect::<Vec<_>>()
    };
    let campaign_tag = format!("campaign_id:{campaign}");
    assert_eq!(
        gauge(
            "runtime_upgrade.campaign.items",
            &[campaign_tag.clone(), "phase:pending".to_owned()]
        ),
        vec![1.0]
    );
    assert_eq!(
        gauge(
            "runtime_upgrade.campaign.items",
            &[campaign_tag, "phase:done".to_owned()]
        ),
        vec![0.0],
        "every phase is reported, zero included"
    );
    assert_eq!(
        gauge(
            "runtime_upgrade.campaigns",
            &["kind:templates".to_owned(), "phase:rolling".to_owned()]
        ),
        vec![1.0]
    );
    assert_eq!(
        gauge(
            "runtime_upgrade.campaigns",
            &["kind:sandboxes".to_owned(), "phase:canary".to_owned()]
        ),
        vec![0.0]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_completes_only_after_a_whole_pass_of_subjects() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let mut expected = Vec::new();
    for _ in 0..5 {
        expected.push(stopped_on_source(&db.pool, node).await?);
    }
    expected.sort();
    let dispatcher = FakeDispatcher::default();
    let reconciler = Reconciler::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        ReconcilerConfig {
            enabled: true,
            candidate_page: 2,
            ..ReconcilerConfig::default()
        },
    );

    // Pages of two: each tick drains what it found, but the pass ends only
    // on the third page, the first shorter than the page size.
    let mut upgraded_by_tick = Vec::new();
    for _ in 0..2 {
        tick(&reconciler).await;
        upgraded_by_tick.push(dispatcher.upgraded().len());
        assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    }
    assert_eq!(upgraded_by_tick, [2, 4]);
    let last_page = tick(&reconciler).await;
    assert_eq!(
        (last_page.inserted, last_page.leased),
        (1, 1),
        "the last page's one sandbox"
    );
    assert!(
        last_page.completed.is_empty(),
        "its item was still in flight"
    );
    assert_eq!(tick(&reconciler).await.completed, vec![campaign]);
    assert_eq!(dispatcher.upgraded(), expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_skipped_because_it_had_started_is_upgraded_once_it_stops() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    store::insert_pending_sandboxes(&db.pool, campaign, &[sandbox], MAX_ATTEMPTS).await?;
    let set_lifecycle = |lifecycle: &'static str| {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox)
            .bind(lifecycle)
            .execute(&db.pool)
    };
    // It started between being found and being leased.
    set_lifecycle("started").await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "skipped");
    assert!(dispatcher.calls().is_empty());

    // Stopped again, the next pass finds it on the source once more.
    set_lifecycle("stopped").await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(dispatcher.upgraded(), vec![sandbox]);
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        ("done".to_owned(), None, 2),
        "the skipped lease still counts"
    );
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_completes_only_when_a_pass_ends_not_partway_through_the_next() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // In id order (ids are time-ordered UUIDv7s, a millisecond apart): one
    // sandbox on the source, then two whose unbound executions match no
    // catalog environment.
    let apart = || tokio::time::sleep(Duration::from_millis(2));
    let first = stopped_on_source(&db.pool, node).await?;
    for _ in 0..2 {
        apart().await;
        insert_sandbox(
            &db.pool,
            K8S,
            SandboxLifecycle::Stopped,
            node,
            Published::Unbound {
                runtime_version: "1.15.1",
                kernel: "5.10.0",
                state_format_version: "9.0.0",
            },
        )
        .await?;
    }
    let dispatcher = FakeDispatcher::default();
    // The first upgrade is retried, so the first pass ends with it pending.
    dispatcher.script(
        first,
        vec![Err(DispatchError::Retryable("busy".to_owned()))],
    );
    let reconciler = Reconciler::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        ReconcilerConfig {
            enabled: true,
            candidate_page: 2,
            ..ReconcilerConfig::default()
        },
    );
    tick(&reconciler).await;
    tick(&reconciler).await;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    assert_eq!(item(&db.pool, campaign, first).await?.0, "done");

    // A sandbox on the source stops after the first pass, behind the next
    // pass's first page, which records nothing.
    apart().await;
    let later = stopped_on_source(&db.pool, node).await?;
    let mid_pass = tick(&reconciler).await;
    assert!(mid_pass.completed.is_empty(), "the pass hasn't reached it");

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    let mut expected = vec![first, later];
    expected.sort();
    assert_eq!(
        dispatcher
            .upgraded()
            .iter()
            .filter(|id| **id == later)
            .count(),
        1
    );
    assert_eq!(
        {
            let mut upgraded = dispatcher.upgraded();
            upgraded.dedup();
            upgraded
        },
        expected
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_scan_after_a_pass_ended_doesnt_complete_the_campaign() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let first = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());
    // The pass ends while `first` is still in flight, so it doesn't complete.
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, first).await?.0, "done");
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // Another sandbox stops on the source, and recording it fails.
    let later = stopped_on_source(&db.pool, node).await?;
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_item() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'items are down'; END $$;
         CREATE TRIGGER refuse_item BEFORE INSERT ON runtime_upgrade_items
             FOR EACH ROW EXECUTE FUNCTION refuse_item();"
    ))
    .execute(&db.pool)
    .await?;
    let failed = tick(&reconciler).await;
    assert!(failed.completed.is_empty());
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    sqlx::raw_sql(sql!("DROP TRIGGER refuse_item ON runtime_upgrade_items"))
        .execute(&db.pool)
        .await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert!(dispatcher.upgraded().contains(&later));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finished_dispatches_are_collected_on_the_next_tick() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    assert_eq!(reconciler.tick().await.leased, 1);
    assert_eq!(
        reconciler.lock_in_flight().len(),
        1,
        "the tick doesn't wait for it"
    );
    // Once the dispatch has run to the end, a reap collects it.
    for _ in 0..200 {
        reconciler.reap_dispatches();
        if reconciler.lock_in_flight().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(counts(&db.pool, campaign).await?.done, 1);
    assert_eq!(reconciler.lock_in_flight().len(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_the_node_keeps_declining_is_dispatched_a_bounded_number_of_times() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let declined = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.answer_all(Ok(StoppedUpgrade::NotMoved(
        "the node finds no certified edge".to_owned(),
    )));
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let ticks =
        tick_until_finished(&reconciler, &db.pool, campaign, 2 * MAX_ATTEMPTS as usize).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded().len(), MAX_ATTEMPTS as usize);
    assert_eq!(ticks, MAX_ATTEMPTS as usize + 1);
    assert_eq!(item(&db.pool, campaign, declined).await?.0, "skipped");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_plan_that_changes_mid_pass_restarts_the_pass() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    // Blocked with no edge yet: the source's sandboxes are stranded.
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, false).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // In id order, a millisecond apart: two on the source, then one whose
    // unbound execution matches no catalog environment.
    let apart = || tokio::time::sleep(Duration::from_millis(2));
    let mut on_source = Vec::new();
    for _ in 0..2 {
        on_source.push(stopped_on_source(&db.pool, node).await?);
        apart().await;
    }
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "5.10.0",
            state_format_version: "9.0.0",
        },
    )
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = Reconciler::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        ReconcilerConfig {
            enabled: true,
            candidate_page: 2,
            ..ReconcilerConfig::default()
        },
    );
    // The first page counts both source sandboxes as stranded.
    tick(&reconciler).await;
    assert!(dispatcher.calls().is_empty());

    // An edge is certified mid-pass. Continuing would end the pass on a
    // page with nothing to record and complete the campaign without them.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = ARRAY[$2] WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(target().to_string())
    .execute(&db.pool)
    .await?;
    let restarted = tick(&reconciler).await;
    assert!(restarted.completed.is_empty());
    assert_eq!(restarted.inserted, 2, "the pass started over");

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    let mut expected = on_source;
    expected.sort();
    assert_eq!(dispatcher.upgraded(), expected);
    Ok(())
}

/// A reconciler that examines `page` sandboxes per tick.
fn paging(pool: &PgPool, dispatcher: FakeDispatcher, page: u32) -> Reconciler<FakeDispatcher> {
    Reconciler::new(
        pool.clone(),
        K8S,
        dispatcher,
        ReconcilerConfig {
            enabled: true,
            candidate_page: page,
            ..ReconcilerConfig::default()
        },
    )
}

/// An unbound sandbox whose execution matches no catalog environment.
async fn unattributable(
    pool: &PgPool,
    node: NodeId,
    runtime_version: &'static str,
) -> TestResult<SandboxId> {
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

#[tokio::test(flavor = "current_thread")]
async fn a_skipped_sandbox_the_catalog_attributes_elsewhere_is_retired_not_waited_on() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Skipped while the catalog put it on the source; no entry matches its
    // unbound execution now.
    let sandbox = unattributable(&db.pool, node, "1.99.0").await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase, attempts)
         VALUES ($1, 'sandbox', $2, 'skipped', 1)"
    ))
    .bind(campaign)
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report.completed, vec![campaign]);
    assert_eq!(dispatcher.calls(), Vec::new());
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        ("skipped".to_owned(), None, MAX_ATTEMPTS as i64)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_declined_once_on_a_multi_page_pass_is_moved_before_completion() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let apart = || tokio::time::sleep(Duration::from_millis(2));
    let first = stopped_on_source(&db.pool, node).await?;
    for _ in 0..2 {
        apart().await;
        unattributable(&db.pool, node, "1.15.1").await?;
    }
    let dispatcher = FakeDispatcher::default();
    // Declined while its pass goes on to a page with nothing to record.
    dispatcher.script(
        first,
        vec![Ok(StoppedUpgrade::NotMoved("the policy moved".to_owned()))],
    );
    let reconciler = paging(&db.pool, dispatcher.clone(), 2);
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, first).await?.0, "skipped");
    let pass_end = tick(&reconciler).await;
    assert!(pass_end.completed.is_empty(), "the pass recorded an item");

    tick_until_finished(&reconciler, &db.pool, campaign, 6).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, first).await?.0, "done");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_catalog_change_mid_pass_restarts_the_pass() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let apart = || tokio::time::sleep(Duration::from_millis(2));
    // Kernel 5.10.0 on the source's runtime: no catalog match yet.
    let late = unattributable(&db.pool, node, "1.15.1").await?;
    for _ in 0..2 {
        apart().await;
        unattributable(&db.pool, node, "1.16.0").await?;
    }
    let dispatcher = FakeDispatcher::default();
    let reconciler = paging(&db.pool, dispatcher.clone(), 2);
    tick(&reconciler).await;
    assert!(dispatcher.calls().is_empty());

    // The source's catalog row now names kernel 5.10.0, so `late`, on the
    // pass's first page, is on the source.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET kernel_version = '5.10.0' WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    let restarted = tick(&reconciler).await;
    assert!(restarted.completed.is_empty());
    assert_eq!(restarted.inserted, 1, "the pass started over");

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded(), vec![late]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_declined_sandbox_is_found_again_and_a_deleted_one_doesnt_hold_completion() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let retryable = stopped_on_source(&db.pool, node).await?;
    let gone = stopped_on_source(&db.pool, node).await?;
    store::insert_pending_sandboxes(&db.pool, campaign, &[retryable, gone], MAX_ATTEMPTS).await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(gone)
        .execute(&db.pool)
        .await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.script(
        retryable,
        vec![Ok(StoppedUpgrade::NotMoved("the policy moved".to_owned()))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    // Both skipped: one declined, one deleted.
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, retryable).await?.0, "skipped");
    assert_eq!(item(&db.pool, campaign, gone).await?.0, "skipped");
    // The next pass finds the declined one again and moves it; the deleted
    // one isn't a candidate.
    let refound = tick(&reconciler).await;
    assert_eq!(refound.inserted, 1);
    assert!(refound.completed.is_empty());
    assert_eq!(item(&db.pool, campaign, retryable).await?.0, "done");

    // A quiet pass completes the campaign although the deleted sandbox's
    // item stays skipped with leases left.
    let quiet = tick(&reconciler).await;
    assert_eq!(quiet.completed, vec![campaign]);
    assert_eq!(
        item(&db.pool, campaign, gone).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox was deleted".to_owned()),
            1
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn scan_state_is_dropped_for_campaigns_that_finished() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let unfinished = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Paused,
            ..seed()
        },
    )
    .await?;
    let finished = CampaignId(uuid::Uuid::now_v7());
    let reconciler = reconciler(&db.pool, FakeDispatcher::default());
    for campaign in [unfinished, finished] {
        reconciler
            .passes
            .lock()
            .unwrap()
            .insert(campaign, Pass::default());
    }

    tick(&reconciler).await;

    let passes = reconciler.passes.lock().unwrap();
    assert_eq!(passes.keys().copied().collect::<Vec<_>>(), vec![unfinished]);
    Ok(())
}

#[test]
fn the_reconciler_is_off_unless_switched_on() {
    let mut env = TestEnv::default();
    assert_eq!(
        ReconcilerConfig::from_environment(&env),
        ReconcilerConfig::default()
    );
    assert!(!ReconcilerConfig::default().enabled);
    for value in ["0", "false", "off", "", "yes please"] {
        env.set(ENABLED_ENV, value);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
    for value in ["1", "yes", "ON", "enabled"] {
        env.set(ENABLED_ENV, value);
        assert!(
            ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
    env.set(ENABLED_ENV, "true");
    env.set(TICK_TEST_ENV, "2");
    assert_eq!(
        ReconcilerConfig::from_environment(&env),
        ReconcilerConfig {
            enabled: true,
            tick: Duration::from_secs(2),
            ..ReconcilerConfig::default()
        }
    );
    let default = ReconcilerConfig::default();
    assert_eq!(
        (
            default.tick,
            default.lease,
            default.candidate_page,
            default.item_deadline
        ),
        (
            Duration::from_secs(30),
            Duration::from_secs(300),
            500,
            Duration::from_secs(1800)
        )
    );
    env.set(ITEM_DEADLINE_ENV, "90");
    assert_eq!(
        ReconcilerConfig::from_environment(&env).item_deadline,
        Duration::from_secs(90)
    );
    env.set(ITEM_DEADLINE_ENV, u64::MAX.to_string());
    assert_eq!(
        ReconcilerConfig::from_environment(&env).item_deadline,
        Duration::from_secs(24 * 60 * 60),
        "clamped, so a deadline never overflows the clock"
    );
    for value in ["0", "soon"] {
        env.set(ITEM_DEADLINE_ENV, value);
        assert_eq!(
            ReconcilerConfig::from_environment(&env).item_deadline,
            Duration::from_secs(1800),
            "{value:?}"
        );
    }
}
