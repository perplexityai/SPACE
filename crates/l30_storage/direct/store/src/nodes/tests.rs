//! The admin Direct node registry's `node_direct` statements against
//! Postgres, each one's losing case included.

use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_postgres_pool::DbErrorKind;
use sqlx::PgPool;

use super::*;

async fn insert_node(pool: &PgPool, node_id: NodeId) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.60'::INET, 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A node with its `node_direct` row at `endpoint`, deleted when `retired`.
async fn insert_registered_node(
    pool: &PgPool,
    node_id: NodeId,
    endpoint: &str,
    retired: bool,
) -> TestDbResult<()> {
    insert_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, deleted_at)
         VALUES ($1, 'registry-node', $2, CASE WHEN $3::BOOL THEN NOW() END)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every column of the node's `node_direct` row, to show what a statement
/// left.
async fn registration(pool: &PgPool, node_id: NodeId) -> TestDbResult<Option<serde_json::Value>> {
    Ok(sqlx::query_scalar::<_, serde_json::Value>(sql!(
        "SELECT to_jsonb(nd) FROM node_direct nd WHERE nd.node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(pool)
    .await?)
}

fn metadata() -> serde_json::Value {
    serde_json::json!({"rack": "a"})
}

fn write<'a>(endpoint: &'a str, metadata: &'a serde_json::Value) -> NodeDirectWrite<'a> {
    NodeDirectWrite {
        name: Some("worker-a"),
        node_api_endpoint: endpoint,
        metadata,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_without_a_registration_writes_nothing() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let metadata = metadata();
    let unregistered = NodeId::new();
    insert_node(pool, unregistered).await?;
    assert_eq!(
        reactivate(pool, unregistered, &write("10.0.0.60:9802", &metadata)).await?,
        None
    );
    assert_eq!(registration(pool, unregistered).await?, None);

    // A retired registration takes the write and is live again.
    let retired = NodeId::new();
    insert_registered_node(pool, retired, "10.0.0.61:9802", true).await?;
    let row = reactivate(pool, retired, &write("10.0.0.62:9802", &metadata))
        .await?
        .expect("the registration is there");
    assert_eq!(row.node_id, retired);
    assert_eq!(row.name.as_deref(), Some("worker-a"));
    assert_eq!(row.node_api_endpoint, "10.0.0.62:9802");
    assert_eq!(row.metadata, metadata);
    assert_eq!(row.deleted_at, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reactivate_onto_a_live_endpoint_fails_and_keeps_the_row() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let metadata = metadata();
    let live = NodeId::new();
    insert_registered_node(pool, live, "10.0.0.60:9802", false).await?;
    let retired = NodeId::new();
    insert_registered_node(pool, retired, "10.0.0.61:9802", true).await?;
    let before = registration(pool, retired).await?;

    let error = reactivate(pool, retired, &write("10.0.0.60:9802", &metadata))
        .await
        .expect_err("a second live registration at one endpoint fails");
    assert_eq!(error.kind(), DbErrorKind::UniqueViolation);
    assert_eq!(error.constraint(), Some("idx_node_direct_endpoint_active"));
    assert_eq!(registration(pool, retired).await?, before);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_at_a_live_endpoint_fails_and_writes_nothing() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let metadata = metadata();
    let live = NodeId::new();
    insert_registered_node(pool, live, "10.0.0.60:9802", false).await?;
    let live_before = registration(pool, live).await?;
    let node_id = NodeId::new();
    insert_node(pool, node_id).await?;

    let error = register(pool, node_id, &write("10.0.0.60:9802", &metadata))
        .await
        .expect_err("a second live registration at one endpoint fails");
    assert_eq!(error.kind(), DbErrorKind::UniqueViolation);
    assert_eq!(error.constraint(), Some("idx_node_direct_endpoint_active"));
    assert_eq!(registration(pool, node_id).await?, None);
    assert_eq!(registration(pool, live).await?, live_before);

    // A retired registration doesn't hold its endpoint.
    let retired = NodeId::new();
    insert_registered_node(pool, retired, "10.0.0.61:9802", true).await?;
    let row = register(pool, node_id, &write("10.0.0.61:9802", &metadata)).await?;
    assert_eq!(row.node_id, node_id);
    assert_eq!(row.node_api_endpoint, "10.0.0.61:9802");
    assert_eq!(row.metadata, metadata);
    assert_eq!(row.deleted_at, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn update_live_of_a_retired_registration_writes_nothing() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let metadata = metadata();
    let update = NodeDirectUpdate {
        name: Some("renamed"),
        node_api_endpoint: Some("10.0.0.69:9802"),
        metadata: Some(&metadata),
    };
    let retired = NodeId::new();
    insert_registered_node(pool, retired, "10.0.0.60:9802", true).await?;
    let before = registration(pool, retired).await?;
    assert_eq!(update_live(pool, retired, &update).await?, None);
    assert_eq!(registration(pool, retired).await?, before);
    let unregistered = NodeId::new();
    insert_node(pool, unregistered).await?;
    assert_eq!(update_live(pool, unregistered, &update).await?, None);
    assert_eq!(registration(pool, unregistered).await?, None);

    // A live registration takes each value given and keeps the rest.
    let live = NodeId::new();
    insert_registered_node(pool, live, "10.0.0.61:9802", false).await?;
    let row = update_live(
        pool,
        live,
        &NodeDirectUpdate {
            name: None,
            node_api_endpoint: None,
            metadata: Some(&metadata),
        },
    )
    .await?
    .expect("the registration is live");
    assert_eq!(row.name.as_deref(), Some("registry-node"));
    assert_eq!(row.node_api_endpoint, "10.0.0.61:9802");
    assert_eq!(row.metadata, metadata);
    Ok(())
}
