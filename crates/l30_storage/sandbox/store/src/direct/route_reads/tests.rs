//! The route reads against Postgres, with the Direct route identity and the
//! node's `node_direct` row seeded.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_node_model::NodeManagedBy;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use super::{authoritative_route_identity, service_route_row};

async fn insert_direct_worker(pool: &PgPool, node_id: NodeId, endpoint: &str) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    Ok(())
}

/// A started Direct sandbox on `active_node_id`, or unplaced without one.
async fn started_sandbox(pool: &PgPool, active_node_id: Option<NodeId>) -> TestDbResult<SandboxId> {
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', active_node_id = $2, target_node_id = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(active_node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_route_identity(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    execution_generation: i64,
    attempt: Uuid,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, $3, $2, 'space', $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(execution_generation)
    .bind(attempt)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn authoritative_route_identity_prefers_the_direct_route_identity() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (column_node, routed_node) = (NodeId::new(), NodeId::new());
    insert_direct_worker(pool, column_node, "http://10.0.0.1:9800").await?;
    insert_direct_worker(pool, routed_node, "http://10.0.0.2:9800").await?;
    let routed = started_sandbox(pool, Some(column_node)).await?;
    let attempt = Uuid::now_v7();
    insert_route_identity(pool, routed, routed_node, 4, attempt).await?;
    let unrouted = started_sandbox(pool, Some(column_node)).await?;

    let identity = authoritative_route_identity(pool, routed)
        .await?
        .expect("the row exists");
    assert_eq!(identity.active_node_id, Some(routed_node));
    assert_eq!(identity.active_node_managed_by, Some(NodeManagedBy::Space));
    assert_eq!(identity.active_execution_generation, Some(4));
    assert_eq!(identity.active_pod_uid, None);
    assert_eq!(
        identity.active_activation_attempt_id,
        Some(SandboxActivationAttemptId::from_uuid(attempt))
    );

    let identity = authoritative_route_identity(pool, unrouted)
        .await?
        .expect("the row exists");
    assert_eq!(identity.active_node_id, Some(column_node));
    assert_eq!(identity.active_node_managed_by, Some(NodeManagedBy::Space));
    assert_eq!(identity.active_execution_generation, None);
    assert_eq!(identity.active_activation_attempt_id, None);

    assert!(
        authoritative_route_identity(pool, SandboxId::new())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn service_route_row_joins_the_route_identity_only_while_placed() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_direct_worker(pool, node_id, "http://10.0.0.3:9800").await?;
    let placed = started_sandbox(pool, Some(node_id)).await?;
    let attempt = Uuid::now_v7();
    insert_route_identity(pool, placed, node_id, 3, attempt).await?;
    let unplaced = started_sandbox(pool, None).await?;
    insert_route_identity(pool, unplaced, node_id, 5, Uuid::now_v7()).await?;

    let row = service_route_row(pool, placed)
        .await?
        .expect("the row exists");
    assert_eq!(row.sandbox_id, placed);
    assert_eq!(row.org_id, OrgId::from_uuid(SEED_ORG_ID));
    assert_eq!(row.lifecycle, "started");
    assert_eq!(row.active_node_id, Some(node_id));
    assert_eq!(row.node_address.as_deref(), Some("127.0.0.1"));
    assert_eq!(row.node_managed_by, Some(NodeManagedBy::Space));
    assert_eq!(row.k8s_node_name, None);
    assert_eq!(
        row.direct_node_api_endpoint.as_deref(),
        Some("http://10.0.0.3:9800")
    );
    assert_eq!(row.sandbox_pod_uid, None);
    assert_eq!(row.execution_generation.map(|value| value.get()), Some(3));
    assert_eq!(
        row.activation_attempt_id,
        Some(SandboxActivationAttemptId::from_uuid(attempt))
    );
    assert_eq!(row.ecw_credential_epoch.get(), 1, "no epoch row reads as 1");
    assert!(!row.command_ready_pending);

    // A row with no active node does not route by an identity it kept.
    let row = service_route_row(pool, unplaced)
        .await?
        .expect("the row exists");
    assert_eq!(row.active_node_id, None);
    assert_eq!(row.direct_node_api_endpoint, None);
    assert_eq!(row.execution_generation, None);
    assert_eq!(row.activation_attempt_id, None);

    assert!(service_route_row(pool, SandboxId::new()).await?.is_none());
    Ok(())
}
