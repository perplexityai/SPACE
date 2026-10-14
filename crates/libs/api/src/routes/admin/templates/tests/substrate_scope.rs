//! Template reconciles target every Kubernetes node, but only the Direct
//! nodes that keep a live registry row.

use super::*;

#[tokio::test]
async fn reconcile_targets_require_a_live_registration_only_for_direct_nodes() -> TestResult {
    let db = pg_test_db().await?;
    let kubernetes = NodeId::new();
    let unregistered_direct = NodeId::new();
    let registered_direct = NodeId::new();
    for (node_id, managed_by) in [
        (kubernetes, "k8s"),
        (unregistered_direct, "space"),
        (registered_direct, "space"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.80'::INET, 'worker', $2, 'ready')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, '10.0.0.80:9802')"
    ))
    .bind(registered_direct)
    .execute(&db.pool)
    .await?;

    let mut tx = db.pool.begin().await?;
    let targets = reconcile_target_node_ids(&mut tx).await?;
    tx.rollback().await?;

    assert!(targets.contains(&kubernetes), "{targets:?}");
    assert!(targets.contains(&registered_direct), "{targets:?}");
    assert!(!targets.contains(&unregistered_direct), "{targets:?}");
    Ok(())
}
