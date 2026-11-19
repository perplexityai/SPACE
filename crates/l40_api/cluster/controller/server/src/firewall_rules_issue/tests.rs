use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;

use super::*;

/// Catches a wakeup queued with another reason, which the node wouldn't
/// read as a firewall change. The store's tests cover the rest of issuance.
#[tokio::test(flavor = "current_thread")]
async fn the_wakeup_queues_firewall_rules_changed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let mut tx = db.pool.begin().await?;
    assert_eq!(FirewallWakeup.wake(&mut tx, node).await?, 1);
    tx.commit().await?;
    let reasons = sqlx::query_scalar::<_, Vec<String>>(sql!(
        "SELECT reasons::text[] FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        reasons,
        [ReconcileReason::FirewallRulesChanged.as_str().to_owned()]
    );
    Ok(())
}
