use pplx_space_common_core::types::SandboxId;

use pplx_space_db_test_fixtures::pg_test_db;

use super::*;

use pplx_space_test_env::TestResult;

async fn insert_sandbox(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started',
             'unplaced',
             'k8s',
             $2,
             1
         )"
    ))
    .bind(sandbox_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn fetch_treats_blank_pod_uid_as_absent() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&db.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, active
         )
         VALUES ($1, 1, $2, 'space', '   ', '10.90.0.8'::inet, 'worker-1', 'running', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{sandbox_id}-g1"))
    .execute(&db.pool)
    .await?;

    let pods = fetch(&db.pool, sandbox_id).await?;

    assert_eq!(pods.len(), 1);
    assert_eq!(pods[0].pod_uid, None);
    assert_eq!(pods[0].pod_ip.as_deref(), Some("10.90.0.8"));
    Ok(())
}
