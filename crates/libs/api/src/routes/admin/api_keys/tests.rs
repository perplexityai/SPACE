use super::*;
use axum::http::Method;
use pplx_space_common_core::{sql, types::OrgId};
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
    assert_eq!(
        key["created_by"],
        harness.tenant_admin.to_string(),
        "the key acts as the organization's oldest user, recorded at creation"
    );
    assert_eq!(
        key["created_by_email"],
        format!("{}@example.invalid", harness.tenant_admin)
    );
    assert_eq!(key["last_used_at"], Value::Null);
    assert_eq!(key["revoked_at"], Value::Null);

    let context = crate::auth::get_api_key_org(&harness.db.pool, &secret, ApiSurface::Native)
        .await?
        .expect("the key authenticates on its surface");
    assert_eq!(context.org.id, org);
    assert_eq!(
        context.actor.map(|actor| (actor.id, actor.org_id)),
        Some((harness.tenant_admin, org)),
        "an operator's key acts as its recorded user of the tenant organization"
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
    assert_eq!(event["target_user_id"], harness.tenant_admin.to_string());
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

/// The user a new key acts as is chosen at creation and stored as its
/// `created_by`, so `get_api_key_org` resolves it with the lookup every earlier
/// release runs, which this change leaves untouched: `created_by` when it is a
/// user of the key's organization. A rollout or a revert can't strand the key
/// without an actor.
#[tokio::test(flavor = "current_thread")]
async fn a_new_key_acts_as_its_chosen_user_under_the_released_lookup() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let pool = &harness.db.pool;
    let org = harness.tenant_org;
    // Two users older than the harness's tenant admin, created at the same
    // instant, so the default must break the tie by id.
    let mut tied = Vec::new();
    for _ in 0..2 {
        let user_id = UserId::new();
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role, created_at)
             VALUES ($1, $2, '!', 'Tied', $3, 'member', '2020-01-01T00:00:00Z')"
        ))
        .bind(user_id)
        .bind(format!("{user_id}@example.invalid"))
        .bind(org)
        .execute(pool)
        .await?;
        tied.push(user_id);
    }
    let oldest = *tied.iter().min().expect("two users");
    let keys_path = format!("/orgs/{org}/api-keys");

    for (requested, expected) in [
        (None, oldest),
        (Some(harness.tenant_admin), harness.tenant_admin),
    ] {
        let mut body = json!({"name": "chosen user", "allowed_surfaces": ["native", "ecw"]});
        if let Some(user_id) = requested {
            body["user_id"] = json!(user_id);
        }
        let (status, created) = harness
            .as_operator(Method::POST, &keys_path, &[], Some(body))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert_eq!(created["api_key"]["created_by"], expected.to_string());
        let secret = created["secret"].as_str().expect("secret");
        for surface in [ApiSurface::Native, ApiSurface::Ecw] {
            let context = crate::auth::get_api_key_org(pool, secret, surface)
                .await?
                .expect("the key authenticates");
            assert_eq!(
                context.actor.map(|actor| (actor.id, actor.org_id)),
                Some((expected, org)),
                "{requested:?} on {surface}"
            );
        }
        let created_events = harness.audit_events(&keys_path).await;
        assert_eq!(created_events[0]["target_user_id"], expected.to_string());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_key_needs_a_user_of_its_organization_to_act_as() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let keys_path = format!("/orgs/{}/api-keys", harness.tenant_org);
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &keys_path,
            &[],
            Some(json!({
                "name": "foreign user",
                "allowed_surfaces": ["ecw"],
                "user_id": harness.operator.id,
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let event = &harness.audit_events(&keys_path).await[0];
    assert_eq!(event["target_org_id"], harness.tenant_org.to_string());
    assert_eq!(event["target_user_id"], Value::Null);

    let empty_org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Empty', $2)"
    ))
    .bind(empty_org)
    .bind(format!("empty-{empty_org}"))
    .execute(&harness.db.pool)
    .await?;
    let empty_keys = format!("/orgs/{empty_org}/api-keys");
    let (status, error) = harness
        .as_operator(
            Method::POST,
            &empty_keys,
            &[],
            Some(json!({"name": "nobody", "allowed_surfaces": ["ecw"]})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert!(
        error["error"]
            .as_str()
            .unwrap_or_default()
            .contains("create a user first"),
        "{error}"
    );
    let (_, listed) = harness
        .as_operator(Method::GET, &empty_keys, &[], None)
        .await;
    assert_eq!(listed, json!([]));
    Ok(())
}

/// A key request names an organization and key in its audit event only once
/// they resolve: a refused create for a missing organization names none, and
/// revoking another organization's key through this organization's path names
/// neither organization.
#[tokio::test(flavor = "current_thread")]
async fn key_audit_targets_name_only_what_resolved() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    let other_org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Other', $2)"
    ))
    .bind(other_org)
    .bind(format!("other-{other_org}"))
    .execute(&harness.db.pool)
    .await?;
    let (status, created) = harness
        .as_operator(
            Method::POST,
            &format!("/orgs/{}/api-keys", harness.tenant_org),
            &[],
            Some(json!({"name": "tenant key", "allowed_surfaces": ["ecw"]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let key_id = created["api_key"]["id"].as_str().expect("key id");

    let wrong_org_path = format!("/orgs/{other_org}/api-keys/{key_id}");
    let (status, _) = harness
        .as_operator(Method::DELETE, &wrong_org_path, &[], None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let missing_org_path = format!("/orgs/{}/api-keys", OrgId::new());
    let (status, _) = harness
        .as_operator(
            Method::POST,
            &missing_org_path,
            &[],
            Some(json!({"name": "nowhere", "allowed_surfaces": ["ecw"]})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    for path in [&wrong_org_path, &missing_org_path] {
        let events = harness.audit_events(path).await;
        assert_eq!(events.len(), 1, "{path}: {events:?}");
        assert_eq!(events[0]["target_org_id"], Value::Null, "{path}");
        assert_eq!(events[0]["target_id"], Value::Null, "{path}");
    }

    let (status, _) = harness
        .as_operator(
            Method::DELETE,
            &format!("/orgs/{}/api-keys/{key_id}", harness.tenant_org),
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "the key was untouched");
    Ok(())
}

#[track_caller]
fn only_key<'a>(listed: &'a Value, key_id: &str) -> &'a Value {
    let keys = listed.as_array().expect("key list");
    assert_eq!(keys.len(), 1, "{listed}");
    assert_eq!(keys[0]["id"], key_id);
    &keys[0]
}
