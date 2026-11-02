use super::*;
use axum::http::Method;
use pplx_space_common_core::types::OrgId;
use serde_json::{Value, json};

use crate::routes::admin::test_fixtures::{TestResult, admin_router_harness};

#[test]
fn api_key_request_validation_normalizes_names_and_surfaces() {
    assert_eq!(api_key_name("  deployment key  "), Ok("deployment key"));
    api_key_name("\n").unwrap_err();
    api_key_name(&"x".repeat(API_KEY_NAME_MAX_CHARS + 1)).unwrap_err();

    assert_eq!(
        allowed_surfaces(vec![ApiSurface::Native, ApiSurface::Ecw]),
        Ok(vec![ApiSurface::Ecw, ApiSurface::Native])
    );
    allowed_surfaces(Vec::new()).unwrap_err();
    allowed_surfaces(vec![ApiSurface::Ecw, ApiSurface::Ecw]).unwrap_err();
}

#[test]
fn generated_api_keys_are_opaque_256_bit_secrets() {
    let key = generate_api_key();
    assert_eq!(key.len(), 64);
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
}

/// An operator manages a tenant organization's keys through the admin routes:
/// the key belongs to the tenant, is attributed to the operator, calls the
/// tenant's APIs, lists its last use, and lists as revoked on request. Each write
/// is audited with the organization and key, never the secret.
#[tokio::test(flavor = "current_thread")]
async fn an_operator_creates_uses_and_revokes_a_key_for_another_organization() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let org = harness.tenant_org;
    let keys_path = format!("/orgs/{org}/api-keys");

    let (status, created) = harness
        .as_operator(
            Method::POST,
            &keys_path,
            &[],
            Some(json!({"name": "tenant key", "allowed_surfaces": ["native"]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let secret = created["secret"]
        .as_str()
        .expect("one-time secret")
        .to_owned();
    let key = &created["api_key"];
    let key_id = key["id"].as_str().expect("key id").to_owned();
    assert_eq!(key["org_id"], org.to_string());
    assert_eq!(key["created_by"], harness.operator.id.to_string());
    assert_eq!(key["created_by_email"], harness.operator.email);
    assert_eq!(key["last_used_at"], Value::Null);
    assert_eq!(key["revoked_at"], Value::Null);

    let context = crate::auth::get_api_key_org(&harness.db.pool, &secret, ApiSurface::Native)
        .await?
        .expect("the key authenticates on its surface");
    assert_eq!(context.org.id, org);
    assert_eq!(
        context.actor.map(|actor| (actor.id, actor.org_id)),
        Some((harness.tenant_admin, org)),
        "an operator's key acts as a user of the tenant organization"
    );
    assert!(
        crate::auth::get_api_key_org(&harness.db.pool, &secret, ApiSurface::Ecw)
            .await?
            .is_none(),
        "the key calls only the APIs it was created for"
    );

    let (status, listed) = harness
        .as_operator(Method::GET, &keys_path, &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let listed_key = only_key(&listed, &key_id);
    assert!(listed_key["last_used_at"].is_string(), "{listed_key}");
    assert_eq!(listed_key["revoked_at"], Value::Null);

    let key_path = format!("{keys_path}/{key_id}");
    let (status, _) = harness
        .as_operator(Method::DELETE, &key_path, &[], None)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        crate::auth::get_api_key_org(&harness.db.pool, &secret, ApiSurface::Native)
            .await?
            .is_none(),
        "a revoked key stops authenticating"
    );
    let (_, listed) = harness
        .as_operator(Method::GET, &keys_path, &[], None)
        .await;
    assert_eq!(
        listed,
        json!([]),
        "the default list leaves revoked keys out"
    );
    let with_revoked = format!("{keys_path}?include_revoked=true");
    let (_, listed) = harness
        .as_operator(Method::GET, &with_revoked, &[], None)
        .await;
    let revoked_at = only_key(&listed, &key_id)["revoked_at"].clone();
    assert!(
        revoked_at.is_string(),
        "a revoked key lists as revoked on request"
    );
    let (status, _) = harness
        .as_operator(Method::DELETE, &key_path, &[], None)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "revoking again succeeds");
    let (_, listed) = harness
        .as_operator(Method::GET, &with_revoked, &[], None)
        .await;
    assert_eq!(only_key(&listed, &key_id)["revoked_at"], revoked_at);

    let created_events = harness.audit_events(&keys_path).await;
    assert_eq!(created_events.len(), 1, "{created_events:?}");
    let event = &created_events[0];
    assert_eq!(event["method"], "POST");
    assert_eq!(event["status_code"], 201);
    assert_eq!(event["actor_user_id"], harness.operator.id.to_string());
    assert_eq!(event["target_org_id"], org.to_string());
    assert_eq!(event["target_type"], "api_key");
    assert_eq!(event["target_id"], key_id.as_str());
    assert!(
        !event.to_string().contains(&secret),
        "the audit event never carries the secret"
    );
    let revoked_events = harness.audit_events(&key_path).await;
    assert_eq!(revoked_events.len(), 2, "{revoked_events:?}");
    for event in &revoked_events {
        assert_eq!(event["method"], "DELETE");
        assert_eq!(event["actor_user_id"], harness.operator.id.to_string());
        assert_eq!(event["target_org_id"], org.to_string());
        assert_eq!(event["target_id"], key_id.as_str());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn key_routes_refuse_non_operators_and_invalid_requests() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let org = harness.tenant_org;
    let keys_path = format!("/orgs/{org}/api-keys");
    let body = json!({"name": "tenant key", "allowed_surfaces": ["ecw"]});
    harness
        .assert_refused_to_non_operators(Method::POST, &keys_path, Some(body.clone()))
        .await;
    harness
        .assert_refused_to_non_operators(Method::GET, &keys_path, None)
        .await;
    harness
        .assert_refused_to_non_operators(
            Method::DELETE,
            &format!("{keys_path}/{}", ApiKeyId::new()),
            None,
        )
        .await;

    let (status, error) = harness
        .as_operator(
            Method::POST,
            &keys_path,
            &[],
            Some(json!({"name": " ", "allowed_surfaces": ["ecw"]})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &keys_path,
            &[],
            Some(json!({"name": "no surfaces", "allowed_surfaces": []})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let missing_org_keys = format!("/orgs/{}/api-keys", OrgId::new());
    let (status, _) = harness
        .as_operator(Method::POST, &missing_org_keys, &[], Some(body))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = harness
        .as_operator(
            Method::DELETE,
            &format!("{keys_path}/{}", ApiKeyId::new()),
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, listed) = harness
        .as_operator(Method::GET, &keys_path, &[], None)
        .await;
    assert_eq!(listed, json!([]), "no refused request created a key");
    Ok(())
}

#[track_caller]
fn only_key<'a>(listed: &'a Value, key_id: &str) -> &'a Value {
    let keys = listed.as_array().expect("key list");
    assert_eq!(keys.len(), 1, "{listed}");
    assert_eq!(keys[0]["id"], key_id);
    &keys[0]
}
