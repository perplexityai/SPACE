use pplx_space_common_core::types::OrgId;

use super::*;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

#[test]
fn port_fingerprint_includes_action_and_target() {
    let request = OpenPortExposureRequest {
        protocol: Some(PortExposureProtocol::Tcp),
        visibility: Some(PortExposureVisibility::Public),
    };
    let first = port_action_fingerprint("sbx_1", 8080, &PortAction::Open(request.clone()))
        .expect("fingerprint");
    let second =
        port_action_fingerprint("sbx_1", 8081, &PortAction::Open(request)).expect("fingerprint");
    let third = port_action_fingerprint(
        "sbx_1",
        8080,
        &PortAction::Close(ClosePortExposureRequest {
            protocol: Some(PortExposureProtocol::Tcp),
        }),
    )
    .expect("fingerprint");

    assert_ne!(first, second);
    assert_ne!(first, third);
    validate_port(0).unwrap_err();
    validate_port(8080).unwrap();
}

#[tokio::test]
async fn record_port_response_replaces_expired_idempotency_record() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug, byok_enabled)
         VALUES ($1, $2, $3, false)"
    ))
    .bind(org_id)
    .bind(format!("Org {org_id}"))
    .bind(format!("org-{}", org_id.as_uuid().simple()))
    .execute(&db.pool)
    .await?;
    let key = IdempotencyKey::parse("expired-open-port-key")?;
    let path_template = OperationPathTemplate::new(OPEN_PORT_PATH_TEMPLATE);
    let action = PortAction::Open(OpenPortExposureRequest {
        protocol: Some(PortExposureProtocol::Tcp),
        visibility: Some(PortExposureVisibility::Public),
    });
    let fingerprint = port_action_fingerprint("sbx_expired_port", 8080, &action)
        .expect("port fingerprint should serialize");
    let response = PortExposureResponse {
        exposure: PortExposure {
            port: 8080,
            protocol: PortExposureProtocol::Tcp,
            visibility: PortExposureVisibility::Public,
            desired_state: PortExposureDesiredState::Open,
            apply_state: PortExposureApplyState::Pending,
            last_error: None,
            last_bridge_status: None,
            last_applied_bridge_generation: None,
            last_observed_at: None,
            created_at: "2026-08-27T00:00:00Z".to_string(),
            updated_at: "2026-08-27T00:00:00Z".to_string(),
        },
    };
    sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (
             id,
             org_id,
             api_product,
             method,
             path_template,
             idempotency_key,
             request_fingerprint,
             response_json,
             expires_at
         )
         VALUES ($1, $2, 'native', 'POST', $3, $4, $5, $6, NOW() - INTERVAL '1 second')"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(path_template.as_str())
    .bind(key.as_str())
    .bind(fingerprint.as_str())
    .bind(SqlJson(serde_json::to_value(&response)?))
    .execute(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;

    let recorded = record_idempotent_port_response(
        &state,
        org_id,
        key.clone(),
        path_template,
        fingerprint.clone(),
        &response,
    )
    .await
    .expect("expired idempotency record should be replaceable");

    assert_eq!(recorded, response);
    let replay = find_idempotent_port_response(&state, org_id, &key, path_template, &fingerprint)
        .await
        .expect("replacement idempotency record should be readable")
        .expect("replacement idempotency record should exist");
    assert_eq!(replay, response);
    Ok(())
}
