use super::*;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_feature_flags::FeatureFlagClient;
use pplx_space_public_api::{
    PublicSandboxId,
    routes::{self as native, NativeSurface},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const KEY: &str = "hidden-operations-test-key";

#[test]
fn the_env_override_names_every_org_or_a_list_of_them() {
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let other = OrgId::from_uuid(Uuid::from_u128(2));
    for (raw, expected) in [
        (None, Some(HiddenOperationsGate::FlagOnly)),
        (Some(""), Some(HiddenOperationsGate::FlagOnly)),
        (Some(" "), Some(HiddenOperationsGate::FlagOnly)),
        (Some("*"), Some(HiddenOperationsGate::AllOrgs)),
        (
            Some(&format!("{org}, {other}")),
            Some(HiddenOperationsGate::Orgs(BTreeSet::from([org, other]))),
        ),
        (Some(&format!("{org},*")), None),
        (Some(&format!("{org},")), None),
        (Some("not-an-org"), None),
    ] {
        assert_eq!(HiddenOperationsGate::parse(raw).ok(), expected, "{raw:?}");
    }
}

/// Each hidden operation is enforced by exactly one of the two checks: it owns
/// its route, or it is a sandbox action, which `parse_action_segment` checks.
/// A hidden operation sharing any other mount with a visible one would be
/// served to every org.
#[test]
fn every_hidden_operation_is_enforced_by_its_route_or_its_action() {
    for operation in NATIVE_V1_OPERATIONS.iter().copied() {
        let method = axum::http::Method::from_bytes(
            operation.method().openapi_name().to_uppercase().as_bytes(),
        )
        .unwrap();
        let route = route_hidden_operation(operation.relative_runtime_path(), &method);
        let action = operation.surface() == NativeSurface::ControlPlane
            && operation.runtime_path() == native::CHECKPOINT_SANDBOX.runtime_path()
            && operation.runtime_path() != operation.path();
        if !operation.status().is_hidden_by_default() {
            assert_eq!(route, None, "{operation:?} is hidden by its route");
        } else if action {
            assert_eq!(route, None, "{operation:?}");
            let action = operation
                .path()
                .rsplit_once(':')
                .and_then(|(_, action)| {
                    pplx_space_public_api::SandboxLifecycleAction::parse_known(action).ok()
                })
                .expect("a sandbox action path names a lifecycle action");
            assert_eq!(
                super::super::sandboxes::lifecycle_operation(action),
                operation
            );
        } else {
            assert_eq!(route, Some(operation), "{operation:?} is not enforced");
        }
    }
}

async fn app(
    gate: HiddenOperationsGate,
    flags: FeatureFlagClient,
) -> TestDbResult<(TestDb, Router, String)> {
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'hidden-operations-test', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let sandbox_id = PublicSandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
             network_policy_generation)
         VALUES ($1, $2, $3, 'started', $4, 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&db.pool)
    .await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.native_hidden_operations = gate;
    state.feature_flags = flags;
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .nest(
            crate::routes::PUBLIC_API_SHARED_ROOT,
            crate::routes::public::v1_router(state.clone()),
        )
        .with_state(state);
    Ok((db, app, sandbox_id.to_string()))
}

async fn send(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
) -> TestDbResult<(StatusCode, Value)> {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header("idempotency-key", format!("key-{method}-{path}"))
        .header("x-request-id", "req-hidden")
        .body(Body::from(serde_json::to_vec(&body)?))?;
    let response = app.clone().oneshot(request).await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

#[tokio::test]
async fn hidden_operations_answer_as_unmounted_unless_the_org_is_enabled() -> TestDbResult<()> {
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let other = OrgId::from_uuid(Uuid::from_u128(2));
    let flag_on =
        || FeatureFlagClient::fixed_bools([(SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key(), true)]);
    for (gate, flags, enabled) in [
        (
            HiddenOperationsGate::FlagOnly,
            FeatureFlagClient::disabled(),
            false,
        ),
        (
            HiddenOperationsGate::Orgs(BTreeSet::from([other])),
            FeatureFlagClient::disabled(),
            false,
        ),
        (
            HiddenOperationsGate::Orgs(BTreeSet::from([org])),
            FeatureFlagClient::disabled(),
            true,
        ),
        (
            HiddenOperationsGate::AllOrgs,
            FeatureFlagClient::disabled(),
            true,
        ),
        (HiddenOperationsGate::FlagOnly, flag_on(), true),
    ] {
        let case = format!("{gate:?} with flag provider {:?}", flags.provider_state());
        let (_db, app, sandbox) = app(gate, flags).await?;
        let (unmounted_status, unmounted) = send(
            &app,
            "GET",
            &format!("/v1/sandboxes/{sandbox}/no-such-route"),
            Value::Null,
        )
        .await?;
        assert_eq!(unmounted_status, StatusCode::NOT_FOUND, "{unmounted}");

        // A route only hidden operations own: before owner scoping, so even
        // a malformed id answers as the unmounted route does.
        for root in ["/v1", crate::routes::PUBLIC_API_SHARED_ROOT] {
            for (sandbox, visible_status) in [
                (sandbox.as_str(), StatusCode::OK),
                ("not-a-sandbox-id", StatusCode::BAD_REQUEST),
            ] {
                let path = format!("{root}/sandboxes/{sandbox}/snapshots");
                let (status, body) = send(&app, "GET", &path, Value::Null).await?;
                if enabled {
                    assert_eq!(status, visible_status, "{case}: {path}: {body}");
                } else {
                    assert_eq!(
                        (status, &body),
                        (StatusCode::NOT_FOUND, &unmounted),
                        "{case}: {path}"
                    );
                }
            }
        }

        // An action sharing its mount with visible actions.
        let path = format!("/v1/sandboxes/{sandbox}:rollback");
        let (status, body) = send(&app, "POST", &path, json!({"snapshot_name": "s"})).await?;
        if enabled {
            assert_eq!(status, StatusCode::ACCEPTED, "{case}: {path}: {body}");
        } else {
            assert_eq!(
                (status, &body),
                (StatusCode::NOT_FOUND, &unmounted),
                "{case}: {path}"
            );
        }

        // A visible incubating action on the same mount is served regardless.
        let path = format!("/v1/sandboxes/{sandbox}:stop");
        let (status, body) = send(&app, "POST", &path, Value::Null).await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{case}: {path}: {body}");
    }
    Ok(())
}
