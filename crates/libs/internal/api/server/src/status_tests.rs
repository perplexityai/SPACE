use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};

use pplx_space_test_env::TestResult;

async fn insert_started_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    managed_by: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started',
             'on_node',
             $2,
             $2,
             $3,
             1
         )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn complete_inventory_marks_missing_space_sandbox_deleted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    // Otherwise identical, but Kubernetes-managed: the census does not own it.
    let k8s_sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_started_sandbox(&db.pool, sandbox_id, node_id, "space").await?;
    insert_started_sandbox(&db.pool, k8s_sandbox_id, node_id, "k8s").await?;

    let mut conn = db.pool.acquire().await?;
    prune_absent_node_sandboxes(&mut conn, node_id, &[]).await?;
    drop(conn);

    let row = |id: SandboxId| {
        sqlx::query_as::<_, (String, String, Option<NodeId>, Option<NodeId>)>(
            "SELECT lifecycle, placement, active_node_id, target_node_id
             FROM sandboxes
             WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&db.pool)
    };

    assert_eq!(
        row(sandbox_id).await?,
        ("deleted".to_owned(), "unplaced".to_owned(), None, None)
    );
    assert_eq!(
        row(k8s_sandbox_id).await?,
        (
            "started".to_owned(),
            "on_node".to_owned(),
            Some(node_id),
            Some(node_id)
        )
    );
    Ok(())
}
