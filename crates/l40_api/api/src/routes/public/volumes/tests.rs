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
    call_with_body(app, method, path, key, None).await
}

async fn call_with_body(
    app: &Router,
    method: &str,
    path: &str,
    key: Option<&str>,
    body: Option<Value>,
) -> TestDbResult<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"));
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let body = match body {
        Some(body) => {
            request = request.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&body)?)
        }
        None => Body::empty(),
    };
    let response = app.clone().oneshot(request.body(body)?).await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

#[tokio::test]
async fn independent_create_replay_listing_and_delete_survive_without_a_sandbox() -> TestDbResult<()>
{
    let policy = json!({
        "organization_ids":[SEED_ORG_ID], "availability_zone":"test-region-1a",
        "volume_management":true, "independent_volumes":true
    })
    .to_string();
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_EBS_ADMISSION_POLICY",
        Some(std::ffi::OsStr::new(&policy)),
    )]) {
        return Ok(());
    }
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1,$2,'volume-test',$3,ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    let spec = json!({"type":"ebs","size_gib":128});
    let created = call_with_body(
        &app,
        "POST",
        "/v1/volumes",
        Some("create-independent"),
        Some(spec.clone()),
    )
    .await?;
    assert_eq!(created.0, StatusCode::ACCEPTED, "{created:?}");
    let created: PublicOperationRecord = serde_json::from_value(created.1)?;
    let replay = call_with_body(
        &app,
        "POST",
        "/v1/volumes",
        Some("create-independent"),
        Some(spec),
    )
    .await?;
    assert_eq!(replay.0, StatusCode::ACCEPTED);
    assert_eq!(
        serde_json::from_value::<PublicOperationRecord>(replay.1)?.id,
        created.id
    );
    let operations = PublicOperationStore::new(db.pool.clone());
    let claim = operations
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::CreateVolume],
        })
        .await?;
    let create_operation = &claim.operations[0];
    assert_eq!(create_operation.id, created.id);
    assert_eq!(
        crate::block_volumes::observe_operation(&db.pool, create_operation)
            .await
            .state,
        OperationState::Queued
    );
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let resources = store::resource::list(&db.pool, org, None).await?;
    assert_eq!(resources.len(), 1);
    let resource = &resources[0];
    assert!(resource.sandbox_id.is_none());
    let path = format!("/v1/volumes/{}", resource.id);
    let (status, visible) = call(&app, "GET", &path, None).await?;
    assert_eq!(status, StatusCode::OK, "{visible}");
    assert_eq!(visible["retained"], true);
    assert!(visible["sandbox_id"].is_null() && visible["mount"].is_null());
    let (status, page) = call(&app, "GET", "/v1/volumes", None).await?;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["volumes"].as_array().unwrap().len(), 1);
    let foreign_org = OrgId::new();
    let foreign_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $2)"
    ))
    .bind(foreign_org)
    .bind(format!("volume-tenant-{foreign_org}"))
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    store::resource::reserve(
        &mut tx,
        store::resource::NewResource {
            id: foreign_id,
            org_id: foreign_org,
            managed_by: "test",
            availability_zone: "test-region-1a",
            resource_limit: 2,
        },
    )
    .await?;
    tx.commit().await?;
    let foreign_path = format!("/v1/volumes/{foreign_id}");
    assert_eq!(
        call(&app, "GET", &foreign_path, None).await?.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &foreign_path, Some("foreign-delete"))
            .await?
            .0,
        StatusCode::NOT_FOUND
    );
    let (_, page) = call(&app, "GET", "/v1/volumes", None).await?;
    assert_eq!(page["volumes"].as_array().unwrap().len(), 1);
    assert_eq!(page["volumes"][0]["volume_id"], resource.id.to_string());
    assert!(
        !store::resource::get(&db.pool, foreign_org, foreign_id)
            .await?
            .unwrap()
            .delete_requested
    );
    assert!(
        store::resource::observe(
            &db.pool,
            resource,
            store::VolumePhase::Available,
            Some("vol-00000000000000001"),
            None
        )
        .await?
    );
    let deleted = call(&app, "DELETE", &path, Some("delete-independent")).await?;
    assert_eq!(deleted.0, StatusCode::ACCEPTED, "{deleted:?}");
    let deleted: PublicOperationRecord = serde_json::from_value(deleted.1)?;
    let replay = call(&app, "DELETE", &path, Some("delete-independent")).await?;
    assert_eq!(
        serde_json::from_value::<PublicOperationRecord>(replay.1)?.id,
        deleted.id
    );
    assert!(
        store::resource::get(&db.pool, org, resource.id)
            .await?
            .unwrap()
            .delete_requested
    );
    // Provisioning stays complete even if observation races a later binding
    // or deletion, instead of waiting forever for the transient available phase.
    let resource = store::resource::get(&db.pool, org, resource.id)
        .await?
        .unwrap();
    assert!(
        store::resource::observe(
            &db.pool,
            &resource,
            store::VolumePhase::Deleting,
            None,
            None,
        )
        .await?
    );
    assert_eq!(
        crate::block_volumes::observe_operation(&db.pool, create_operation)
            .await
            .state,
        OperationState::Succeeded
    );
    Ok(())
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
    assert_eq!(visible.mount, Some(mount.mount));
    assert_eq!(visible.permissions, Some(mount.permissions));
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
        crate::block_volumes::observe_operation(&db.pool, operation)
            .await
            .state,
        OperationState::Queued
    );
    let row = store::get(&db.pool, sandbox).await?.unwrap();
    assert!(row.delete_requested);
    // This boundary test supplies an observed cloud deletion to the store.
    // It does not claim to exercise an actual EBS delete.
    assert!(store::observe(&db.pool, &row, store::VolumePhase::Deleted, None, None).await?);
    let completed = crate::block_volumes::observe_operation(&db.pool, operation).await;
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
