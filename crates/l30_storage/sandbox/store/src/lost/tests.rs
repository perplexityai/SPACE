use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::{SandboxId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{DEAD_UPLOAD_SETTLE, write_off_lost_sandboxes};

/// A `failed` sandbox still placed on `node_id`: a flip's output before the
/// write-off.
async fn insert_failed_on_node(pool: &PgPool, node_id: NodeId) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'failed', 'on_node', $5, $5, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(id)
}

/// An execution with a pod IP; an inactive one already carries its
/// `terminated_at`.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
    active: bool,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (id, sandbox_id, generation, pod_name, pod_namespace, pod_ip, state,
             active, terminated_at)
         VALUES ($1, $2, $3, $4, 'space-sandboxes', '10.0.0.5'::INET,
                 $5, $6, CASE WHEN $6 THEN NULL ELSE NOW() - INTERVAL '1 day' END)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("sbx-test-{id}"))
    .bind(state)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_queue_row(pool: &PgPool, node_id: NodeId, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending
            (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, 'full')"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

/// A `creating` full upload from `node_id`, created twice
/// [`DEAD_UPLOAD_SETTLE`] ago and never heartbeated.
async fn insert_quiet_upload(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, source_node_id)
         VALUES ($1, $2, 'full', 'creating', NOW() - make_interval(secs => $3), $4)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(DEAD_UPLOAD_SETTLE.as_secs_f64() * 2.0)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(id)
}

type Placement = (String, Option<NodeId>, Option<NodeId>);

async fn placement(pool: &PgPool, id: SandboxId) -> TestResult<Placement> {
    Ok(sqlx::query_as(sql!(
        "SELECT placement, active_node_id, target_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// `(active, state, pod_ip IS NULL, terminated_at)`.
type ExecutionState = (bool, String, bool, Option<time::OffsetDateTime>);

async fn execution(pool: &PgPool, id: Uuid) -> TestResult<ExecutionState> {
    Ok(sqlx::query_as(sql!(
        "SELECT active, state, pod_ip IS NULL, terminated_at
           FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn queued(pool: &PgPool, node_id: NodeId, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
            SELECT 1 FROM node_local_backup_pending
             WHERE node_id = $1 AND sandbox_id = $2
         )"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn snapshot_state(pool: &PgPool, id: Uuid) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

/// The write-off deletes the gone node's queue rows for the given sandboxes,
/// retires their active execution, and unplaces them. A queue row on another
/// node, an inactive execution, a sandbox not in the list and the node's
/// uploads are left alone.
#[tokio::test(flavor = "current_thread")]
async fn write_off_lost_sandboxes_clears_only_the_gone_nodes_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let gone = NodeId::new();
    let live = NodeId::new();
    let lost = insert_failed_on_node(pool, gone).await?;
    let bystander = insert_failed_on_node(pool, gone).await?;

    let lost_active = insert_execution(pool, lost, 2, "running", true).await?;
    let lost_inactive = insert_execution(pool, lost, 1, "terminated", false).await?;
    let bystander_active = insert_execution(pool, bystander, 1, "running", true).await?;
    insert_queue_row(pool, gone, lost).await?;
    insert_queue_row(pool, live, lost).await?;
    insert_queue_row(pool, gone, bystander).await?;
    let upload = insert_quiet_upload(pool, lost, gone).await?;
    let inactive_before = execution(pool, lost_inactive).await?;

    let mut tx = pool.begin().await?;
    write_off_lost_sandboxes(&mut tx, gone, &[lost]).await?;
    tx.commit().await?;

    assert_eq!(
        placement(pool, lost).await?,
        ("unplaced".to_owned(), None, Some(gone)),
        "the lost sandbox is unplaced and keeps its last target"
    );
    assert_eq!(
        placement(pool, bystander).await?,
        ("on_node".to_owned(), Some(gone), Some(gone))
    );

    let (active, state, pod_ip_cleared, terminated_at) = execution(pool, lost_active).await?;
    assert!(!active);
    assert_eq!(state, "forgotten");
    assert!(pod_ip_cleared);
    assert!(terminated_at.is_some());
    assert_eq!(execution(pool, lost_inactive).await?, inactive_before);
    let (active, state, pod_ip_cleared, terminated_at) = execution(pool, bystander_active).await?;
    assert!(active);
    assert_eq!(state, "running");
    assert!(!pod_ip_cleared);
    assert!(terminated_at.is_none());

    assert!(!queued(pool, gone, lost).await?);
    assert!(
        queued(pool, live, lost).await?,
        "a live node's queue row is its pending work"
    );
    assert!(queued(pool, gone, bystander).await?);

    assert_eq!(
        snapshot_state(pool, upload).await?,
        "creating",
        "dead uploads are the snapshot store's write"
    );
    Ok(())
}
