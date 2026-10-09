use pplx_space_common_core::types::NodeId;

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
};

use super::*;

#[tokio::test]
async fn placement_nodes_cover_active_target_and_latest_execution_only() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let active = NodeId::new();
    let target = NodeId::new();
    let execution = NodeId::new();
    let unrelated = NodeId::new();
    for (node_id, name) in [
        (active, "worker-active"),
        (target, "worker-target"),
        (execution, "worker-execution"),
        (unrelated, "worker-unrelated"),
    ] {
        insert_ready_worker_node(&db.pool, node_id).await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $2 || '-uid')"
        ))
        .bind(node_id)
        .bind(name)
        .execute(&db.pool)
        .await?;
    }
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, user_id, org_id, active_node_id, target_node_id, network_policy_generation)
         VALUES ($1, $2, $3, $4, $5, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(active)
    .bind(target)
    .execute(&db.pool)
    .await?;
    // Only the latest generation's node counts; an older execution on the
    // unrelated node must not authorize it.
    for (generation, node) in [(6, "worker-unrelated"), (7, "worker-execution")] {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active
             ) VALUES ($1, $2, 'sandbox-pod-' || $2, 'space', $3, 'terminated', false)"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(node)
        .execute(&db.pool)
        .await?;
    }

    let store = PgCredentialStore::new(db.pool.clone());
    let mut names: Vec<String> = store
        .sandbox_placement_nodes(sandbox_id)
        .await?
        .into_iter()
        .map(|node| {
            assert_eq!(node.uid.as_str(), format!("{}-uid", node.name.as_str()));
            node.name.as_str().to_string()
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["worker-active", "worker-execution", "worker-target"]
    );
    assert!(
        store
            .sandbox_placement_nodes(SandboxId::new())
            .await?
            .is_empty()
    );
    Ok(())
}
