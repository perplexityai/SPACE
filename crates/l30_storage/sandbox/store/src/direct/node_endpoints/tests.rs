//! The node-endpoint reads against Postgres, with the nodes' `node_direct`
//! rows seeded.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, insert_suspended_sandbox,
    pg_test_db,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::{DirectNodeEndpoint, active_node, local_state_node, provenance_source_node};

async fn insert_direct_worker(pool: &PgPool, endpoint: &str) -> TestDbResult<DirectNodeEndpoint> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    Ok(DirectNodeEndpoint {
        id,
        node_api_endpoint: endpoint.to_owned(),
    })
}

async fn sandbox_on(
    pool: &PgPool,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
) -> TestDbResult<SandboxId> {
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $3 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(active_node_id)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[tokio::test(flavor = "current_thread")]
async fn each_read_answers_for_its_node_column() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let active = insert_direct_worker(pool, "http://10.0.0.1:9800").await?;
    let target = insert_direct_worker(pool, "http://10.0.0.2:9800").await?;

    let both = sandbox_on(pool, Some(active.id), Some(target.id)).await?;
    assert_eq!(active_node(pool, both).await?, Some(active.clone()));
    assert_eq!(
        local_state_node(pool, both).await?,
        Some(active.clone()),
        "local state prefers the active node"
    );
    assert_eq!(
        provenance_source_node(pool, both).await?,
        Some(target.clone()),
        "provenance prefers the target node"
    );

    let released = sandbox_on(pool, None, Some(target.id)).await?;
    assert_eq!(active_node(pool, released).await?, None);
    assert_eq!(
        local_state_node(pool, released).await?,
        Some(target.clone())
    );
    assert_eq!(
        provenance_source_node(pool, released).await?,
        Some(target.clone())
    );

    assert_eq!(active_node(pool, SandboxId::new()).await?, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_node_that_is_not_a_registered_direct_worker_answers_none() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kubernetes = NodeId::new();
    insert_ready_worker_node(pool, kubernetes).await?;
    let deleted = insert_direct_worker(pool, "http://10.0.0.3:9800").await?;
    sqlx::query(sql!(
        "UPDATE node_direct SET deleted_at = NOW() WHERE node_id = $1"
    ))
    .bind(deleted.id)
    .execute(pool)
    .await?;

    for node_id in [kubernetes, deleted.id] {
        let sandbox_id = sandbox_on(pool, Some(node_id), Some(node_id)).await?;
        assert_eq!(active_node(pool, sandbox_id).await?, None);
        assert_eq!(local_state_node(pool, sandbox_id).await?, None);
        assert_eq!(provenance_source_node(pool, sandbox_id).await?, None);
    }
    Ok(())
}
