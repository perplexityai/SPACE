//! The one-row reads against Postgres.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

/// A Kubernetes node, under a v2 drain when `v2`.
async fn insert_node(pool: &PgPool, v2: bool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, drain_engine, drain_status)
         VALUES ($1, '10.0.0.7'::INET, 'worker', 'k8s',
                 CASE WHEN $2 THEN 'v2' END,
                 CASE WHEN $2 THEN '{\"generation\": 1, \"deadline_at\": 0}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(v2)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A `suspended` row naming `target` as its target node, and no active node.
async fn insert_sandbox(pool: &PgPool, target: Option<NodeId>) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement, target_node_id,
             network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'suspended',
                 CASE WHEN $4::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END, $4, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

/// The read names the row's home node and whether a v2 drain of it is
/// recorded; a row with no node reads no node and no drain, and a missing
/// row reads nothing.
#[tokio::test(flavor = "current_thread")]
async fn home_node_v2_drain_reads_the_home_nodes_engine() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let v2 = insert_node(pool, true).await?;
    let plain = insert_node(pool, false).await?;
    let on_v2 = insert_sandbox(pool, Some(v2)).await?;
    let on_plain = insert_sandbox(pool, Some(plain)).await?;
    let nowhere = insert_sandbox(pool, None).await?;

    assert_eq!(
        home_node_v2_drain(pool, on_v2).await?,
        Some((Some(v2), true))
    );
    assert_eq!(
        home_node_v2_drain(pool, on_plain).await?,
        Some((Some(plain), false))
    );
    assert_eq!(
        home_node_v2_drain(pool, nowhere).await?,
        Some((None, false))
    );
    assert_eq!(home_node_v2_drain(pool, SandboxId::new()).await?, None);
    Ok(())
}
