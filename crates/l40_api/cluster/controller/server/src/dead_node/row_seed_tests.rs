//! Seeding for the dead-node verb's Postgres-backed tests: nodes, sandbox
//! rows pinned to them, their snapshots and their executions.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Snapshot {
    /// A durable full snapshot taken after the row's last placement: the
    /// portability predicate covers it.
    Covered,
    /// A durable full snapshot from before the last placement: ready, not
    /// covering.
    Uncovered,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pin {
    Active,
    Target,
    Both,
}

pub(crate) async fn insert_node(pool: &PgPool, name: &str) -> TestResult<NodeId> {
    insert_k8s_node(pool, name, &format!("uid-{name}"), None).await
}

/// A Kubernetes node with no heartbeat, recording `uid` and `instance_id`.
pub(crate) async fn insert_k8s_node(
    pool: &PgPool,
    name: &str,
    uid: &str,
    instance_id: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.9'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(node_id)
    .bind(name)
    .bind(uid)
    .bind(instance_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A row last placed two hours ago, its lifecycle changed an hour ago.
pub(crate) async fn insert_row(
    pool: &PgPool,
    lifecycle: &str,
    managed_by: SubstrateKind,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             lifecycle_changed_at, placement_changed_at)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '2 hours')"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(crate) async fn insert_pinned(
    pool: &PgPool,
    lifecycle: &str,
    pin: Pin,
    node: NodeId,
) -> TestResult<SandboxId> {
    let (active, target) = match pin {
        Pin::Active => (Some(node), None),
        Pin::Target => (None, Some(node)),
        Pin::Both => (Some(node), Some(node)),
    };
    insert_row(pool, lifecycle, SubstrateKind::Kubernetes, active, target).await
}

/// One snapshot row. `age_secs` sets `timestamp`; `attempt_age_secs`, when
/// given, sets `last_attempt_at`. A `ready` one gets a storage location.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotSeed {
    pub(crate) kind: &'static str,
    pub(crate) state: &'static str,
    pub(crate) source_node: Option<NodeId>,
    pub(crate) age_secs: i64,
    pub(crate) attempt_age_secs: Option<i64>,
    pub(crate) artifact: Option<(Uuid, &'static str)>,
}

impl SnapshotSeed {
    pub(crate) fn full(state: &'static str, age_secs: i64) -> Self {
        Self {
            kind: "full",
            state,
            source_node: None,
            age_secs,
            attempt_age_secs: None,
            artifact: None,
        }
    }
}

pub(crate) async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    seed: SnapshotSeed,
) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    let (artifact_id, artifact_origin) = match seed.artifact {
        Some((id, origin)) => (Some(id), Some(origin)),
        None => (None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, source_node_id, timestamp,
             last_attempt_at, local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3::UUID, $4, $5, $6,
                 NOW() - make_interval(secs => $7::BIGINT),
                 NOW() - make_interval(secs => $8::BIGINT),
                 $9, CASE WHEN $9::UUID IS NULL THEN NULL ELSE $4 END, $10)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .bind(seed.kind)
    .bind(seed.state)
    .bind(seed.source_node)
    .bind(seed.age_secs)
    .bind(seed.attempt_age_secs)
    .bind(artifact_id)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    if seed.state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub(crate) async fn seed_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    snapshot: Snapshot,
) -> TestResult {
    match snapshot {
        Snapshot::Covered => {
            insert_snapshot(pool, sandbox_id, SnapshotSeed::full("ready", 1800)).await
        }
        Snapshot::Uncovered => {
            insert_snapshot(pool, sandbox_id, SnapshotSeed::full("ready", 3 * 3600)).await
        }
        Snapshot::None => Ok(()),
    }
}

/// A `creating` full upload from `node` that started just now.
pub(crate) async fn seed_pending_upload(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node: NodeId,
) -> TestResult {
    insert_snapshot(
        pool,
        sandbox_id,
        SnapshotSeed {
            source_node: Some(node),
            ..SnapshotSeed::full("creating", 0)
        },
    )
    .await
}

/// An execution of the sandbox on `node_name`; `None` is a pod not scheduled
/// yet.
pub(crate) async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_name: Option<&str>,
    active: bool,
) -> TestResult {
    insert_stamped_execution(pool, sandbox_id, 1, node_name, None, active).await?;
    Ok(())
}

/// Execution `generation` of the sandbox on `node_name`, stamped with
/// `instance_id`. Returns its id.
pub(crate) async fn insert_stamped_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    node_name: Option<&str>,
    instance_id: Option<&str>,
    active: bool,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             terminated_at, node_instance_id)
         VALUES ($1, $2, $3, 'sandboxes', $4, 'running', $5,
                 CASE WHEN $5 THEN NULL ELSE NOW() END, $6)
         RETURNING id"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(node_name)
    .bind(active)
    .bind(instance_id)
    .fetch_one(pool)
    .await?)
}
