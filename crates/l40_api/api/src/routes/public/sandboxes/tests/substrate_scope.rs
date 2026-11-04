//! A sandbox's current failure episode is read from its own substrate's
//! execution record: the active Kubernetes execution for a `'k8s'` row, the
//! Direct route identity for a `'space'` row.
//!
//! Every sandbox here carries an observed failure whose payload matches both
//! shapes, and each pair differs only in `managed_by`, so only the substrate
//! predicates decide which record counts.

use super::*;

#[derive(Clone, Copy)]
enum ExecutionRecord {
    KubernetesExecution,
    DirectRoute,
}

async fn insert_failed_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    managed_by: &str,
    record: ExecutionRecord,
) -> TestDbResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let activation_id = Uuid::now_v7();
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by, active_node_id,
             network_policy_override_json, network_policy_generation
         ) VALUES ($1, $2, $3, 'failed', 'on_node', $4, $5, $6, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(node_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    match record {
        ExecutionRecord::KubernetesExecution => {
            sqlx::query(pplx_space_common_core::sql!(
                "INSERT INTO sandbox_k8s_executions (
                     sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
                     route_activation_id
                 ) VALUES ($1, $2, $3, 'space', $4, 'failed', TRUE, $5)"
            ))
            .bind(sandbox_id)
            .bind(CURRENT_EXECUTION_GENERATION)
            .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
            .bind(CURRENT_POD_UID)
            .bind(activation_id)
            .execute(pool)
            .await?;
        }
        ExecutionRecord::DirectRoute => {
            sqlx::query(pplx_space_common_core::sql!(
                "INSERT INTO sandbox_direct_route_identities (
                     sandbox_id, route_generation, current_node_id, substrate_kind,
                     execution_generation, activation_attempt_id
                 ) VALUES ($1, 1, $2, 'space', $3, $4)"
            ))
            .bind(sandbox_id)
            .bind(node_id)
            .bind(CURRENT_EXECUTION_GENERATION)
            .bind(activation_id)
            .execute(pool)
            .await?;
        }
    }
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, failure_reason
         ) VALUES ($1, $2, 'failed', $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(INTERNAL_FAILURE_REASON)
    .execute(pool)
    .await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at
         ) VALUES (
             $1,
             $2,
             $3,
             jsonb_build_object(
                 $4::TEXT,
                 jsonb_build_object(
                     $5::TEXT, $6::BIGINT,
                     $7::TEXT, $8::TEXT,
                     $9::TEXT, $10::TEXT
                 )
             ),
             NOW()
         )"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(sandbox_id.to_string())
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(CURRENT_EXECUTION_GENERATION)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(CURRENT_POD_UID)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(activation_id.to_string())
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[tokio::test]
async fn current_failure_reads_the_execution_record_of_the_sandbox_substrate() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let mut expected = Vec::new();
    for (managed_by, record, has_current_failure) in [
        ("k8s", ExecutionRecord::KubernetesExecution, true),
        ("space", ExecutionRecord::KubernetesExecution, false),
        ("space", ExecutionRecord::DirectRoute, true),
        ("k8s", ExecutionRecord::DirectRoute, false),
    ] {
        let sandbox_id = insert_failed_sandbox(&db.pool, node_id, managed_by, record).await?;
        expected.push((sandbox_id, has_current_failure));
    }
    expected.sort();

    let mut by_id = Vec::new();
    for (sandbox_id, _) in &expected {
        let row = fetch_public_sandbox(
            &db.pool,
            OrgId::from_uuid(SEED_ORG_ID),
            PublicSandboxId::from(*sandbox_id),
        )
        .await?
        .expect("sandbox");
        by_id.push((row.id, row.has_current_failure));
    }
    assert_eq!(by_id, expected, "sandbox read");

    let mut listed = fetch_sandboxes_page(&db.pool, OrgId::from_uuid(SEED_ORG_ID), None, None, 100)
        .await?
        .into_iter()
        .map(|row| (row.id, row.has_current_failure))
        .filter(|(id, _)| expected.iter().any(|(expected_id, _)| expected_id == id))
        .collect::<Vec<_>>();
    listed.sort();
    assert_eq!(listed, expected, "sandbox list");
    Ok(())
}
