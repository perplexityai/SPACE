use super::*;
use crate::public_operations::{ClaimLimits, ClaimPublicOperations};
use axum::{Router, body::Body, http::Request};
use pplx_space_accounts::OrgId;
use pplx_space_common_core::{sql, types::SandboxId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_public_api::OperationState;
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

const KEY: &str = "synthetic-volume-management-api-key";

async fn call(
    app: &Router,
    method: &str,
    path: &str,
    key: Option<&str>,
) -> TestDbResult<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"));
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let response = app.clone().oneshot(request.body(Body::empty())?).await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[tokio::test]
async fn inspect_delete_replay_and_completion_use_durable_volume_authority() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let sandbox = SandboxId::new();
    let org = OrgId::from_uuid(SEED_ORG_ID);
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
        VALUES ($1, $2, 'volume-test', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes
        (id, user_id, org_id, lifecycle, network_policy_override_json, network_policy_generation)
        VALUES ($1, $2, $3, 'stopped', $4, 1)"
    ))
    .bind(sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    let mount = serde_json::from_value(json!({
        "type":"ebs","size_gib":128,"permissions":"read_write","mount":"/data"
    }))?;
    let mut tx = db.pool.begin().await?;
    let id = store::reserve(
        &mut tx,
        store::NewBlockVolume {
            sandbox_id: sandbox,
            org_id: org,
            managed_by: "test",
            mount_spec: &mount,
            availability_zone: "test-region-1a",
            resource_limit: 2,
        },
    )
    .await?;
    tx.commit().await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.volume_management_organizations.clear();
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state.clone());
    let path = format!("/v1/volumes/{id}");
    let (status, value) = call(&app, "GET", &path, None).await?;
    assert_eq!(status, StatusCode::OK, "{value}");
    let visible: PublicVolume = serde_json::from_value(value)?;
    assert_eq!(visible.volume_id, id);
    assert_eq!(visible.mount_spec, mount);
    assert!(!visible.delete_requested);
    assert_eq!(
        call(&app, "DELETE", &path, Some("delete")).await?.0,
        StatusCode::FORBIDDEN
    );
    state.volume_management_organizations.insert(org);
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    assert_eq!(
        call(&app, "DELETE", &path, None).await?.0,
        StatusCode::BAD_REQUEST
    );
    // A requested disk is not evidence of observed detach.
    assert_eq!(
        call(&app, "DELETE", &path, Some("delete")).await?.0,
        StatusCode::CONFLICT
    );
    let row = store::get(&db.pool, sandbox).await?.unwrap();
    assert!(
        store::observe(
            &db.pool,
            &row,
            store::VolumePhase::Available,
            Some("vol-00000000000000001"),
            None
        )
        .await?
    );
    let (first, replay) = tokio::join!(
        call(&app, "DELETE", &path, Some("delete")),
        call(&app, "DELETE", &path, Some("delete"))
    );
    let first = first?;
    let replay = replay?;
    assert_eq!(first.0, StatusCode::ACCEPTED, "{first:?}");
    assert_eq!(replay.0, StatusCode::ACCEPTED, "{replay:?}");
    let first: PublicOperationRecord = serde_json::from_value(first.1)?;
    let replay: PublicOperationRecord = serde_json::from_value(replay.1)?;
    assert_eq!(first.id, replay.id);
    let operations = PublicOperationStore::new(db.pool.clone());
    let claim = operations
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteVolume],
        })
        .await?;
    let operation = &claim.operations[0];
    assert_eq!(operation.id, first.id);
    assert_eq!(operation.kind, OperationKind::DeleteVolume);
    assert_eq!(
        crate::block_volumes::execute_delete(&db.pool, operation)
            .await
            .state,
        OperationState::Queued
    );
    let row = store::get(&db.pool, sandbox).await?.unwrap();
    assert!(row.delete_requested);
    // This boundary test supplies an observed cloud deletion to the store.
    // It does not claim to exercise an actual EBS delete.
    assert!(store::observe(&db.pool, &row, store::VolumePhase::Deleted, None, None).await?);
    let completed = crate::block_volumes::execute_delete(&db.pool, operation).await;
    assert_eq!(completed.state, OperationState::Succeeded);
    assert!(completed.result_json.is_none());
    assert_eq!(
        operation.target_id.as_deref(),
        Some(id.to_string().as_str())
    );
    let (status, value) = call(&app, "GET", &path, None).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(value["delete_requested"].as_bool().unwrap());
    assert_eq!(value["state"], "deleted");
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/v1/volumes/{}", Uuid::now_v7()),
            None
        )
        .await?
        .0,
        StatusCode::NOT_FOUND
    );
    Ok(())
}
