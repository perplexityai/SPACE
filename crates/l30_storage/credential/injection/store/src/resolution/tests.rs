use pplx_space_common_core::types::NodeId;

use super::*;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_ready_worker_node, pg_test_db,
};
use serde_json::json;

#[tokio::test]
async fn reads_coherent_network_policy_references_and_rejects_replaced_execution()
-> TestDbResult<()> {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let reference_id = CredentialReferenceId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'worker-a', 'worker-a-uid')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, active_node_id, network_policy_generation)
         VALUES ($1, $2, $3, $4, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active)
         VALUES ($1, 3, 'credential-sandbox', 'space', 'worker-a', 'running', true)"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&db.pool)
    .await?;

    let secret_arn = "arn:aws:secretsmanager:us-west-2:123456789012:secret:example-AbCdEf";
    sqlx::query(sql!(
        "INSERT INTO credential_references (id, org_id, upstream_provider, upstream_provider_config)
         VALUES ($1, $2, 'aws_secrets_manager', $3)"
    ))
    .bind(reference_id)
    .bind(SEED_ORG_ID)
    .bind(Json(json!({"secret_arn": secret_arn})))
    .execute(&db.pool)
    .await?;
    let policy_json = json!({
        "version": "v1",
        "egress": {"default_action": "allow"},
        "tls_interception": "on",
        "network_credential_rules": [{
            "rule_id": "example-api",
            "destination": {
                "exact_host": "api.example.com",
                "path_prefix": "/v1"
            },
            "credential_header_bindings": [{
                "header_name": "Authorization",
                "credential_reference_id": reference_id.to_string()
            }],
            "supported_http_methods": ["GET", "POST"]
        }]
    });
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET network_policy_override_json = $2, network_policy_generation = 9
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(Json(policy_json.clone()))
    .execute(&db.pool)
    .await?;

    let policy_candidate = load_network_policy_candidate(&db.pool, sandbox_id)
        .await?
        .expect("network-policy reference snapshot");
    assert_eq!(policy_candidate.state.organization_id, SEED_ORG_ID);
    assert_eq!(policy_candidate.state.execution_generation.get(), 3);
    assert!(
        policy_candidate
            .effective_network_policy_ref
            .starts_with(&format!(
                "cluster:sandbox:{}:generation:9:sha256:",
                sandbox_id.as_uuid()
            ))
    );
    assert_eq!(policy_candidate.policy.network_credential_rules.len(), 1);
    assert_eq!(
        policy_candidate.policy.network_credential_rules[0].credential_header_bindings[0]
            .credential_reference_id,
        reference_id.as_uuid()
    );
    assert_eq!(policy_candidate.references.len(), 1);
    assert_eq!(policy_candidate.references[0].id, reference_id);
    assert_eq!(policy_candidate.references[0].state, ReferenceState::Active);
    assert!(matches!(
        &policy_candidate.references[0].config,
        Config::AwsSecretsManager { secret_arn: configured_arn }
            if configured_arn == secret_arn
    ));
    let mut role_tx = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL ROLE space_credmgr"))
        .execute(&mut *role_tx)
        .await?;
    assert_eq!(
        load_network_policy_candidate(&mut *role_tx, sandbox_id)
            .await?
            .expect("credential-manager role can read its bounded policy snapshot"),
        policy_candidate
    );
    let can_write_policy: bool = sqlx::query_scalar(sql!(
        "SELECT has_column_privilege(current_user, 'sandboxes',
                                     'network_policy_override_json', 'UPDATE')"
    ))
    .fetch_one(&mut *role_tx)
    .await?;
    assert!(
        !can_write_policy,
        "Credential Manager must remain read-only"
    );
    role_tx.commit().await?;

    let mut retired = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE credential_references SET state = 'retired' WHERE id = $1"
    ))
    .bind(reference_id)
    .execute(&mut *retired)
    .await?;
    let retired_candidate = load_network_policy_candidate(&mut *retired, sandbox_id)
        .await?
        .expect("retired references remain visible to the authority");
    assert_eq!(
        retired_candidate.references[0].state,
        ReferenceState::Retired
    );
    retired.rollback().await?;

    let mut foreign = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE credential_references
         SET org_id = '00000000-0000-0000-0000-000000000002'
         WHERE id = $1"
    ))
    .bind(reference_id)
    .execute(&mut *foreign)
    .await?;
    assert!(matches!(
        load_network_policy_candidate(&mut *foreign, sandbox_id).await?,
        Some(candidate) if candidate.references.is_empty()
    ));
    foreign.rollback().await?;

    let mut missing = db.pool.begin().await?;
    let missing_reference_id = CredentialReferenceId::new();
    let mut missing_policy = policy_json.clone();
    missing_policy["network_credential_rules"][0]["credential_header_bindings"][0]["credential_reference_id"] =
        json!(missing_reference_id.to_string());
    sqlx::query(sql!(
        "UPDATE sandboxes SET network_policy_override_json = $2 WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(Json(missing_policy))
    .execute(&mut *missing)
    .await?;
    assert!(matches!(
        load_network_policy_candidate(&mut *missing, sandbox_id).await?,
        Some(candidate) if candidate.references.is_empty()
    ));
    missing.rollback().await?;

    let template_name = "credential-policy-template";
    sqlx::query(sql!(
        "INSERT INTO templates
             (id, name, org_id, network_policy_json, network_policy_generation)
         VALUES ($1, $2, $3, $4, 12)"
    ))
    .bind(Uuid::now_v7())
    .bind(template_name)
    .bind(SEED_ORG_ID)
    .bind(Json(policy_json.clone()))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET template_id = $2, network_policy_override_json = NULL
         WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(template_name)
    .execute(&db.pool)
    .await?;
    let inherited = load_network_policy_candidate(&db.pool, sandbox_id)
        .await?
        .expect("template policy snapshot");
    assert!(
        inherited
            .effective_network_policy_ref
            .starts_with("cluster:template:credential-policy-template:generation:12:sha256:")
    );

    let mut malformed = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE credential_references SET upstream_provider_config = '{}'::jsonb WHERE id = $1"
    ))
    .bind(reference_id)
    .execute(&mut *malformed)
    .await?;
    assert!(matches!(
        load_network_policy_candidate(&mut *malformed, sandbox_id).await,
        Err(ResolutionStateError::InvalidData(_))
    ));
    malformed.rollback().await?;

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = 'replaced-worker'
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&db.pool)
    .await?;
    assert!(
        load_network_policy_candidate(&db.pool, sandbox_id)
            .await?
            .is_none()
    );
    Ok(())
}
