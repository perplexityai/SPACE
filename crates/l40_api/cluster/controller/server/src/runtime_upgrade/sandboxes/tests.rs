//! The sandbox driver under the reconciler, against a real Postgres and a
//! fake dispatcher. The loop's own steps (the budget, the canary, leases,
//! passes) are under test in `runtime_upgrade::tests`; these cover what the
//! driver decides: which sandboxes are on the source, which lifecycles count,
//! and what each item's upgrade does.

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

mod fakes;
mod force_restart_drain_fences;
mod force_restart_edge_fences;
mod force_restart_fences;
mod force_restart_recovery;
mod force_restart_start_recovery;
mod force_restarts;
mod moves;
mod passes;

use fakes::*;

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

fn db_handle(pool: &PgPool) -> pplx_space_postgres_pool::Db {
    pplx_space_postgres_pool::Db::from(pool.clone())
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
    pplx_space_runtime_environment_policy_store::set_default(&mut *tx, target(), &change()).await?;
    pplx_space_runtime_environment_policy_store::set_environment_state(
        &mut *tx,
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
             active_node_id, network_policy_generation
         )
         VALUES ($1, $2, $3, $4, $5, 'on_node', $6, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle.as_str())
    .bind(node)
    .execute(pool)
    .await?;
    if managed_by == K8S {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                 pod_ip, state, active
             )
             VALUES ($1, 1, $2, 'space', $3, '10.0.0.10'::inet, 'running', TRUE)"
        ))
        .bind(sandbox_id)
        .bind(format!("sbx-{sandbox_id}"))
        .bind(format!("pod-{sandbox_id}"))
        .execute(pool)
        .await?;
    }
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

/// `count` stopped sandboxes whose unbound executions match no catalog
/// environment, in id order, so a test can give the first one a role by
/// republishing it.
async fn unattributable_in_order(
    pool: &PgPool,
    node: NodeId,
    runtime_version: &'static str,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let mut sandboxes = Vec::new();
    for _ in 0..count {
        sandboxes.push(unattributable(pool, node, runtime_version).await?);
    }
    sandboxes.sort();
    Ok(sandboxes)
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

/// A reconciler that runs only the sandbox driver over `dispatcher`,
/// examining `page` sandboxes per tick.
fn paging(pool: &PgPool, dispatcher: FakeDispatcher, page: u32) -> Reconciler {
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(SandboxDriver::new(pool.clone(), K8S, dispatcher)),
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
    let dispatcher = dispatcher.with_memory_retirement(memory_retirement.clone());
    Reconciler::new(
        pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            pool.clone(),
            K8S,
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

/// Tick at most `limit` times until `campaign` records at least one `done` item.
async fn tick_until_canary_done(
    reconciler: &Reconciler,
    pool: &PgPool,
    campaign: CampaignId,
    limit: usize,
) -> TestResult {
    for _ in 0..limit {
        let current = counts(pool, campaign).await?;
        if current.done > 0 {
            return Ok(());
        }
        tick(reconciler).await;
    }
    let current = counts(pool, campaign).await?;
    if current.done > 0 {
        return Ok(());
    }
    let phase = campaign_phase(pool, campaign).await?;
    Err(format!(
        "campaign {campaign} didn't complete its canary within {limit} ticks (phase={phase}, counts={current:?})"
    )
    .into())
}
