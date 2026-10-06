use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
};

use super::*;

fn node_scope(name: &str, uid: &str) -> SandboxOwnerScope {
    SandboxOwnerScope::KubernetesNode {
        name: K8sNodeName::parse(name).expect("synthetic node name is valid"),
        uid: K8sNodeUid::parse(uid).expect("synthetic node UID is valid"),
    }
}

#[tokio::test]
async fn owner_scope_requires_the_active_execution_node_incarnation() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let execution_node_id = NodeId::new();
    let sibling_node_id = NodeId::new();
    let node_name = "shared-worker-node";
    let execution_uid = "execution-node-uid";
    let sibling_uid = "same-name-sibling-uid";

    insert_ready_worker_node(&db.pool, execution_node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.2', 'builder', 'k8s', 'ready')"
    ))
    .bind(sibling_node_id)
    .execute(&db.pool)
    .await?;
    for (node_id, uid) in [
        (execution_node_id, execution_uid),
        (sibling_node_id, sibling_uid),
    ] {
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $3)"
        ))
        .bind(node_id)
        .bind(node_name)
        .bind(uid)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, k8s_node_name,
             k8s_node_uid, state, active
         ) VALUES ($1, 1, 'sandbox-pod', 'space', $2, $3, 'running', TRUE)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_name)
    .bind(execution_uid)
    .execute(&db.pool)
    .await?;

    let store = PgCredentialStore::new(db.pool.clone());
    for (scope, allowed) in [
        (SandboxOwnerScope::AuthorityOperator, true),
        (node_scope(node_name, execution_uid), true),
        (node_scope(node_name, sibling_uid), false),
        (node_scope("different-worker-node", execution_uid), false),
    ] {
        assert_eq!(
            store.sandbox_owner(sandbox_id, scope).await?.is_some(),
            allowed
        );
    }

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&db.pool)
    .await?;
    assert!(
        store
            .sandbox_owner(sandbox_id, node_scope(node_name, execution_uid))
            .await?
            .is_none(),
        "an inactive execution must not authorize its former node"
    );

    Ok(())
}
