use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_sandbox_residency::CENSUS_INVENTORY_MAX_AGE;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::fresh;

/// A worker node whose snapshot row stamped its backup inventory `secs_ago`
/// seconds ago, or never (`None`).
async fn insert_node_reported(pool: &PgPool, secs_ago: Option<f64>) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, sandbox_inventory_pruned_at,
             local_backup_inventory_reported_at, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + make_interval(secs => 60), TRUE,
             'worker', 'ready', '[]'::jsonb, NOW(),
             CASE WHEN $2::FLOAT8 IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $2)
             END,
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(secs_ago)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// The gate believes a report inside the window and nothing else: not a stale
/// one, not a row that never reported, and not a node with no row at all
/// (the state between registration and first census).
#[tokio::test(flavor = "current_thread")]
async fn only_a_report_inside_the_window_is_fresh() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // Twice the window, so a change to the constant can't make the stale case
    // fresh.
    let stale = (CENSUS_INVENTORY_MAX_AGE.as_secs() * 2) as f64;
    for (secs_ago, expected, name) in [
        (Some(30.0), true, "a report inside the window"),
        (Some(stale), false, "a report older than the window"),
        (None, false, "a node that never reported"),
    ] {
        let node = insert_node_reported(pool, secs_ago).await?;
        assert_eq!(
            fresh(pool, node).await?,
            expected,
            "{name} must read as fresh={expected}"
        );
    }

    let bare = NodeId::new();
    insert_ready_worker_node(pool, bare).await?;
    assert!(
        !fresh(pool, bare).await?,
        "a node with no snapshot row must not be believed"
    );
    Ok(())
}
