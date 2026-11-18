//! The retire of a released row's execution, against Postgres.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::{SandboxId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

const NODE_NAME: &str = "worker-drain-terminate.synthetic.internal";

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.6'::INET, 'worker', $2)"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A row placed an hour ago.
async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             placement_changed_at, command_ready_pending)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1, NOW() - INTERVAL '1 hour', TRUE)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

/// An active execution of the row on [`NODE_NAME`] in `state`, last updated
/// `age` ago.
async fn insert_execution(
    pool: &PgPool,
    id: SandboxId,
    state: &str,
    age_secs: i64,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             updated_at)
         VALUES ($1, 1, $2, 'sandboxes', $3, $4, TRUE,
                 NOW() - make_interval(secs => $5::BIGINT))
         RETURNING id"
    ))
    .bind(id)
    .bind(format!("space-sbx-{}", id.as_uuid().simple()))
    .bind(NODE_NAME)
    .bind(state)
    .bind(age_secs)
    .fetch_one(pool)
    .await?)
}

async fn execution_active(pool: &PgPool, execution_id: Uuid) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT active FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(execution_id)
    .fetch_one(pool)
    .await?)
}

/// The retire takes only the execution it names, and only while active.
#[tokio::test(flavor = "current_thread")]
async fn the_retire_takes_only_the_named_active_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let id = insert_sandbox(pool, "suspended", Some(node), Some(node)).await?;
    let active = insert_execution(pool, id, "suspended", 0).await?;
    let mut tx = pool.begin().await?;
    assert!(!retire_source_execution(&mut tx, id, Uuid::now_v7()).await?);
    assert!(retire_source_execution(&mut tx, id, active).await?);
    assert!(!retire_source_execution(&mut tx, id, active).await?);
    tx.commit().await?;
    assert!(!execution_active(pool, active).await?);
    Ok(())
}
