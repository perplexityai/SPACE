use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const RETENTION_SECS: i64 = 60;
const OLD_SECS: i64 = 120;

/// A worker managed by `managed_by` in `state`, registered with Kubernetes
/// as `uid` when `managed_by` is `k8s`.
async fn insert_node(
    pool: &PgPool,
    managed_by: &str,
    state: &str,
    uid: &str,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', $2, $3)"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(state)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("ip-{node_id}.ec2.internal"))
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn insert_ref(pool: &PgPool, node_id: NodeId, age_secs: i64, dead: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_queue (
             node_id, facet, element, requested_at, updated_at, dead_at
         ) VALUES (
             $1, 'sandbox-desired-state', 'sandbox:a',
             NOW() - ($2 * INTERVAL '1 second'),
             NOW() - ($2 * INTERVAL '1 second'),
             CASE WHEN $3 THEN NOW() ELSE NULL END
         )"
    ))
    .bind(node_id)
    .bind(age_secs)
    .bind(dead)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_trigger(pool: &PgPool, node_id: NodeId, age_secs: i64) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_reconcile_triggers (node_id, reason, requested_at, updated_at)
         VALUES ($1, 'test', NOW() - ($2 * INTERVAL '1 second'), NOW())"
    ))
    .bind(node_id)
    .bind(age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lists_unready_undrained_k8s_nodes_with_stale_live_work_in_id_order() -> TestResult {
    let db = pg_test_db().await?;
    let stale_ref = insert_node(&db.pool, "k8s", "offline", "uid-ref").await?;
    insert_ref(&db.pool, stale_ref, OLD_SECS, false).await?;
    let stale_trigger = insert_node(&db.pool, "k8s", "registered", "uid-trigger").await?;
    insert_trigger(&db.pool, stale_trigger, OLD_SECS).await?;
    let ready = insert_node(&db.pool, "k8s", "ready", "uid-ready").await?;
    insert_ref(&db.pool, ready, OLD_SECS, false).await?;
    let draining = insert_node(&db.pool, "k8s", "offline", "uid-draining").await?;
    insert_ref(&db.pool, draining, OLD_SECS, false).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(draining)
    .execute(&db.pool)
    .await?;
    let fresh = insert_node(&db.pool, "k8s", "offline", "uid-fresh").await?;
    insert_ref(&db.pool, fresh, RETENTION_SECS / 2, false).await?;
    insert_trigger(&db.pool, fresh, RETENTION_SECS / 2).await?;
    let dead = insert_node(&db.pool, "k8s", "offline", "uid-dead").await?;
    insert_ref(&db.pool, dead, OLD_SECS, true).await?;
    let direct = insert_node(&db.pool, "space", "offline", "uid-direct").await?;
    insert_ref(&db.pool, direct, OLD_SECS, false).await?;

    let listed =
        unready_nodes_with_stale_reconcile_work(&db.pool, None, RETENTION_SECS, 100).await?;

    let mut expected = vec![
        StaleReconcileWorkNode {
            id: stale_ref,
            k8s_node_name: format!("ip-{stale_ref}.ec2.internal"),
            k8s_node_uid: Some("uid-ref".to_owned()),
        },
        StaleReconcileWorkNode {
            id: stale_trigger,
            k8s_node_name: format!("ip-{stale_trigger}.ec2.internal"),
            k8s_node_uid: Some("uid-trigger".to_owned()),
        },
    ];
    expected.sort_by_key(|node| node.id);
    assert_eq!(listed, expected);

    let page = unready_nodes_with_stale_reconcile_work(&db.pool, None, RETENTION_SECS, 1).await?;
    assert_eq!(page, expected[..1]);
    let rest =
        unready_nodes_with_stale_reconcile_work(&db.pool, Some(page[0].id), RETENTION_SECS, 100)
            .await?;
    assert_eq!(rest, expected[1..]);
    Ok(())
}
