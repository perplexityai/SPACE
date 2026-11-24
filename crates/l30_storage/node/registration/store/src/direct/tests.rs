//! The Direct node hard deletes against Postgres: which nodes each guard
//! deletes, and that a node the guard does not match keeps its rows.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db, try_pg_test_db,
};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::*;

const RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

async fn row_count(pool: &PgPool, table: &str, node_id: NodeId) -> Result<i64, sqlx::Error> {
    let query = match table {
        "node_direct" => "SELECT COUNT(*) FROM node_direct WHERE node_id = $1",
        "node_template_inventory" => {
            "SELECT COUNT(*) FROM node_template_inventory WHERE node_id = $1"
        }
        "node_status_snapshots" => "SELECT COUNT(*) FROM node_status_snapshots WHERE node_id = $1",
        "node_runtime_installations" => {
            "SELECT COUNT(*) FROM node_runtime_installations WHERE node_id = $1"
        }
        "node_facet_observed_status" => {
            "SELECT COUNT(*) FROM node_facet_observed_status WHERE node_id = $1"
        }
        "node_reconcile_triggers" => {
            "SELECT COUNT(*) FROM node_reconcile_triggers WHERE node_id = $1"
        }
        _ => unreachable!("unknown test table {table}"),
    };
    sqlx::query_scalar::<_, i64>(query)
        .bind(node_id)
        .fetch_one(pool)
        .await
}

async fn node_exists(pool: &PgPool, node_id: NodeId) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM nodes WHERE id = $1)"))
        .bind(node_id)
        .fetch_one(pool)
        .await
}

/// A node row managed by `managed_by` in `state`, with a `node_direct` row
/// deleted `deleted_days_ago` days ago, or live when `None`.
async fn insert_direct_node(
    pool: &PgPool,
    node_id: NodeId,
    managed_by: &str,
    state: &str,
    deleted_days_ago: Option<i32>,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, 'worker', $2, $3)"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(state)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
         VALUES ($1, '10.0.0.1:9802', NOW() - ($2::INT * INTERVAL '1 day'))"
    ))
    .bind(node_id)
    .bind(deleted_days_ago)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_direct_node_removes_node_and_projection_rows() -> TestDbResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = NodeId::new();

    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.12'::INET, 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint)
         VALUES ($1, 'worker-1', '10.0.0.12:9802')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id)
         VALUES (space_uuid_v7(), 'node-delete-test', '00000000-0000-0000-0000-000000000001')"
    ))
    .execute(pool)
    .await?;
    sqlx::query(
        sql!("INSERT INTO node_template_inventory (node_id, template_name, content_hash, runtime, arch)
         VALUES ($1, 'node-delete-test', 'sha256:1111111111111111111111111111111111111111111111111111111111111111', 'fc', 'aarch64')"),
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services, updated_at
         )
         VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_installations (node_id, runtime, runtime_version)
         VALUES ($1, 'fc', '1.0.0')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (node_id, facet, element, observed_at)
         VALUES ($1, 'template_inventory', '', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_triggers (node_id, reason)
         VALUES ($1, 'node_updated')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;

    let deleted = delete_direct_node(pool, node_id, NodeManagedBy::Space).await?;
    assert_eq!(deleted, 1);

    assert!(!node_exists(pool, node_id).await?);
    for table in [
        "node_direct",
        "node_template_inventory",
        "node_status_snapshots",
        "node_runtime_installations",
        "node_facet_observed_status",
        "node_reconcile_triggers",
    ] {
        assert_eq!(
            row_count(pool, table, node_id).await?,
            0,
            "{table} should cascade with node deletion"
        );
    }

    let deleted_again = delete_direct_node(pool, node_id, NodeManagedBy::Space).await?;
    assert_eq!(deleted_again, 0);
    Ok(())
}

/// The guard matches neither a node managed by another substrate nor one
/// without a `node_direct` row: each delete reports 0 and leaves the rows.
#[tokio::test(flavor = "current_thread")]
async fn delete_direct_node_leaves_a_node_its_guard_does_not_match() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let kubernetes = NodeId::new();
    insert_direct_node(pool, kubernetes, "k8s", "registered", None).await?;
    let unregistered = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.2'::INET, 'worker', 'space', 'registered')"
    ))
    .bind(unregistered)
    .execute(pool)
    .await?;

    assert_eq!(
        delete_direct_node(pool, kubernetes, NodeManagedBy::Space).await?,
        0
    );
    assert_eq!(
        delete_direct_node(pool, unregistered, NodeManagedBy::Space).await?,
        0
    );

    assert!(node_exists(pool, kubernetes).await?);
    assert_eq!(row_count(pool, "node_direct", kubernetes).await?, 1);
    assert!(node_exists(pool, unregistered).await?);
    Ok(())
}

/// Only the expired, unpinned tombstone of the bound substrate goes. A recent
/// tombstone, a live node, a registered node whose `node_direct` row is past
/// retention, one a quarantined sandbox pins as its active node (quarantine
/// is not terminal here), and one managed by another substrate all stay with
/// their `node_direct` rows.
#[tokio::test(flavor = "current_thread")]
async fn prune_expired_direct_node_tombstones_deletes_only_expired_unpinned_ones()
-> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let expired = NodeId::new();
    insert_direct_node(pool, expired, "space", "deleted", Some(8)).await?;
    let recent = NodeId::new();
    insert_direct_node(pool, recent, "space", "deleted", Some(1)).await?;
    let live = NodeId::new();
    insert_direct_node(pool, live, "space", "registered", None).await?;
    let retired = NodeId::new();
    insert_direct_node(pool, retired, "space", "registered", Some(8)).await?;
    let pinned = NodeId::new();
    insert_direct_node(pool, pinned, "space", "deleted", Some(8)).await?;
    let kubernetes = NodeId::new();
    insert_direct_node(pool, kubernetes, "k8s", "deleted", Some(8)).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle,
             active_node_id, target_node_id, network_policy_generation
         )
         VALUES (space_uuid_v7(), $1::uuid, $2::uuid, 'space', 'quarantined', $3, NULL, 1)"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(pinned)
    .execute(pool)
    .await?;

    let deleted =
        prune_expired_direct_node_tombstones(pool, RETENTION_SECS, 100, NodeManagedBy::Space)
            .await?;
    assert_eq!(deleted, 1);
    assert!(!node_exists(pool, expired).await?);
    assert_eq!(row_count(pool, "node_direct", expired).await?, 0);
    for node_id in [recent, live, retired, pinned, kubernetes] {
        assert!(node_exists(pool, node_id).await?);
        assert_eq!(row_count(pool, "node_direct", node_id).await?, 1);
    }

    // Nothing left that the guard matches: the next prune deletes nothing.
    let deleted_again =
        prune_expired_direct_node_tombstones(pool, RETENTION_SECS, 100, NodeManagedBy::Space)
            .await?;
    assert_eq!(deleted_again, 0);
    for node_id in [recent, live, retired, pinned, kubernetes] {
        assert!(node_exists(pool, node_id).await?);
    }
    Ok(())
}

/// The batch takes the oldest tombstones first, up to `max_records`.
#[tokio::test(flavor = "current_thread")]
async fn prune_expired_direct_node_tombstones_takes_the_oldest_first() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    // `NodeId::new()` is a UUIDv7, ordered by creation, so `newer` sorts
    // first by id and by insert: only `ORDER BY nd.deleted_at` picks `oldest`.
    let newer = NodeId::new();
    insert_direct_node(pool, newer, "space", "deleted", Some(8)).await?;
    let oldest = NodeId::new();
    insert_direct_node(pool, oldest, "space", "deleted", Some(10)).await?;

    let deleted =
        prune_expired_direct_node_tombstones(pool, RETENTION_SECS, 1, NodeManagedBy::Space).await?;
    assert_eq!(deleted, 1);
    assert!(!node_exists(pool, oldest).await?);
    assert!(node_exists(pool, newer).await?);
    Ok(())
}
