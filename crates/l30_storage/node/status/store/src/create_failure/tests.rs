//! The create-failure write against Postgres: what it records on a node's
//! first snapshot, what it keeps of an existing one, and that it writes
//! nothing for an unregistered node.

use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct Snapshot {
    reachable: bool,
    error: Option<String>,
    role: Option<String>,
    scheduling_status: Option<String>,
    scheduling_reason: Option<String>,
    network_slots_ready: Option<i32>,
    health_healthy: Option<bool>,
    services: serde_json::Value,
    window_secs: i64,
}

async fn snapshot(pool: &PgPool, node_id: NodeId) -> TestResult<Option<Snapshot>> {
    Ok(sqlx::query_as(sql!(
        "SELECT reachable, error, role, scheduling_status, scheduling_reason,
                network_slots_ready, health_healthy, services,
                EXTRACT(EPOCH FROM stale_after - observed_at)::BIGINT AS window_secs
           FROM node_status_snapshots
          WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_optional(pool)
    .await?)
}

/// A published snapshot with ready slots and a healthy node.
async fn published(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role, scheduling_status,
             network_slots_ready, health_healthy, services, updated_at
         )
         VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE, 'worker', 'ready',
                 5, TRUE, '[{\"name\": \"node-ctl\"}]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// No `nodes` row, no snapshot.
#[tokio::test(flavor = "current_thread")]
async fn an_unregistered_node_gets_no_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    record_create_failure(
        &db.pool,
        node_id,
        false,
        "connect refused",
        10,
        "unavailable",
    )
    .await?;
    assert_eq!(snapshot(&db.pool, node_id).await?, None);
    Ok(())
}

/// A node's first snapshot records the failure: an unreachable node carries
/// the reason as its error and is unhealthy, fresh for the given window.
#[tokio::test(flavor = "current_thread")]
async fn a_first_snapshot_records_the_failure() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    record_create_failure(
        &db.pool,
        node_id,
        false,
        "connect refused",
        10,
        "unavailable",
    )
    .await?;
    assert_eq!(
        snapshot(&db.pool, node_id).await?,
        Some(Snapshot {
            reachable: false,
            error: Some("connect refused".to_string()),
            role: Some("worker".to_string()),
            scheduling_status: Some("unavailable".to_string()),
            scheduling_reason: Some("connect refused".to_string()),
            network_slots_ready: None,
            health_healthy: Some(false),
            services: serde_json::json!([]),
            window_secs: 10,
        })
    );
    Ok(())
}

/// On an existing snapshot a reachable failure keeps health and services; a
/// `full`, `draining` or `unavailable` status zeroes the ready slots and any
/// other keeps them; an unreachable one is unhealthy.
#[tokio::test(flavor = "current_thread")]
async fn an_existing_snapshot_keeps_what_the_failure_does_not_say() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (reachable, status, slots, healthy) in [
        (true, "full", Some(0), Some(true)),
        (true, "draining", Some(0), Some(true)),
        (true, "degraded", Some(5), Some(true)),
        (false, "unavailable", Some(0), Some(false)),
    ] {
        let node_id = NodeId::new();
        insert_ready_worker_node(&db.pool, node_id).await?;
        published(&db.pool, node_id).await?;
        record_create_failure(&db.pool, node_id, reachable, "refused", 10, status).await?;
        let row = snapshot(&db.pool, node_id)
            .await?
            .expect("the snapshot is still there");
        assert_eq!(
            row,
            Snapshot {
                reachable,
                error: (!reachable).then(|| "refused".to_string()),
                role: Some("worker".to_string()),
                scheduling_status: Some(status.to_string()),
                scheduling_reason: Some("refused".to_string()),
                network_slots_ready: slots,
                health_healthy: healthy,
                services: serde_json::json!([{"name": "node-ctl"}]),
                window_secs: 10,
            },
            "{status}"
        );
    }
    Ok(())
}
