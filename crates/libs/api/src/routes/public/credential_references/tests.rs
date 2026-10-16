use super::*;
use axum::{Router, body::Body, http::Request};
use http_body_util::BodyExt;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db, pg_test_db_with_max_connections,
    wait_for_lock_waiter,
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET_ARN: &str = "arn:aws:secretsmanager:us-west-2:123456789012:secret:example-AbCdEf";
const SESSION: &str = "credential-reference-test-session";

fn registration(key: &str) -> Value {
    json!({"provider": "aws_secrets_manager", "request_id": key, "secret_arn": SECRET_ARN})
}

async fn call(router: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {SESSION}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

async fn public_router(pool: &sqlx::PgPool) -> TestDbResult<Router> {
    sqlx::query(sql!("UPDATE users SET password_hash = $1 WHERE id = $2"))
        .bind(crate::auth::hash_password(&Uuid::now_v7().to_string())?)
        .bind(SEED_USER_ID)
        .execute(pool)
        .await?;
    sqlx::query(sql!("INSERT INTO sessions (user_id, token_hash, expires_at) VALUES ($1, $2, NOW() + INTERVAL '1 hour')"))
        .bind(SEED_USER_ID)
        .bind(crate::auth::hash_token(SESSION))
        .execute(pool)
        .await?;
    let state = AppState::from_pool(pool.clone()).await?;
    Ok(Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state))
}

#[tokio::test]
async fn registration_keeps_idempotency_org_scope_and_retirement() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let router = public_router(&db.pool).await?;

    let path = "/v1/credential-references";
    let (first, replay) = tokio::join!(
        call(&router, "POST", path, registration("register")),
        call(&router, "POST", path, registration("register"))
    );
    assert_eq!(first.0, StatusCode::CREATED, "{first:?}");
    assert_eq!(first, replay);
    let id: Uuid = first.1["credential_reference_id"]
        .as_str()
        .unwrap()
        .parse()?;
    assert_eq!(id.get_version_num(), 7);
    let alias = call(&router, "POST", path, registration("alias")).await;
    assert_eq!(alias.0, StatusCode::CREATED);
    assert_ne!(alias.1, first.1);

    let mut changed = registration("register");
    changed["secret_arn"] = json!(format!("{SECRET_ARN}-changed"));
    assert_eq!(
        call(&router, "POST", path, changed).await.0,
        StatusCode::CONFLICT
    );
    let mut untrusted = registration("untrusted");
    untrusted["provider_instance_id"] = json!("retired-concept");
    assert_eq!(
        call(&router, "POST", path, untrusted).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let reference_path = format!("{path}/{id}");
    sqlx::query(sql!("UPDATE credential_references SET org_id = '00000000-0000-0000-0000-000000000002' WHERE id = $1"))
        .bind(id)
        .execute(&db.pool)
        .await?;
    assert_eq!(
        call(&router, "DELETE", &reference_path, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    sqlx::query(sql!(
        "UPDATE credential_references SET org_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    for _ in 0..2 {
        assert_eq!(
            call(&router, "DELETE", &reference_path, Value::Null)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        call(&router, "POST", path, registration("register"))
            .await
            .1,
        first.1
    );
    Ok(())
}

const OTHER_ORG_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0002);

fn binding_policy(reference: Uuid) -> Value {
    json!({
        "version": "v1",
        "egress": {"default_action": "allow"},
        "tls_interception": "on",
        "network_credential_rules": [{
            "rule_id": "example-api",
            "destination": {"exact_host": "api.example.com"},
            "credential_header_bindings": [{
                "header_name": "Authorization",
                "credential_reference_id": reference.to_string()
            }],
            "supported_http_methods": ["GET"]
        }]
    })
}

async fn register_reference(router: &Router, key: &str) -> Uuid {
    let (status, body) = call(
        router,
        "POST",
        "/v1/credential-references",
        registration(key),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body:?}");
    body["credential_reference_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn retire(router: &Router, reference: Uuid) -> StatusCode {
    call(
        router,
        "DELETE",
        &format!("/v1/credential-references/{reference}"),
        Value::Null,
    )
    .await
    .0
}

async fn reference_state(pool: &sqlx::PgPool, reference: Uuid) -> TestDbResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT state FROM credential_references WHERE id = $1"
    ))
    .bind(reference)
    .fetch_one(pool)
    .await?)
}

async fn insert_sandbox(
    pool: &sqlx::PgPool,
    org: Uuid,
    template: Option<&str>,
    policy: Option<Value>,
) -> TestDbResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, template_id,
                                network_policy_override_json, network_policy_generation)
         VALUES ($1, $2, $3, 'started', $4, $5, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(org)
    .bind(template)
    .bind(policy.map(sqlx::types::Json))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_template(
    pool: &sqlx::PgPool,
    name: &str,
    owner: Option<Uuid>,
    policy: Value,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id, visibility, network_policy_json)
         VALUES ($1, $2, $3, CASE WHEN $3::uuid IS NULL THEN 'generic' ELSE 'org_specific' END, $4)"
    ))
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(owner)
    .bind(sqlx::types::Json(policy))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn retirement_refuses_a_reference_bound_by_a_sandbox_override() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let router = public_router(&db.pool).await?;
    let reference = register_reference(&router, "override-binding").await;
    let sandbox =
        insert_sandbox(&db.pool, SEED_ORG_ID, None, Some(binding_policy(reference))).await?;

    assert_eq!(
        retire(&router, reference).await,
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(reference_state(&db.pool, reference).await?, "active");

    sqlx::query(sql!(
        "UPDATE sandboxes SET network_policy_override_json = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(sqlx::types::Json(
        json!({"version": "v1", "egress": {"default_action": "deny"}}),
    ))
    .execute(&db.pool)
    .await?;
    assert_eq!(retire(&router, reference).await, StatusCode::NO_CONTENT);
    assert_eq!(reference_state(&db.pool, reference).await?, "retired");
    Ok(())
}

#[tokio::test]
async fn retirement_follows_template_inheritance_until_the_sandbox_tombstone() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let router = public_router(&db.pool).await?;
    let reference = register_reference(&router, "template-binding").await;
    insert_template(
        &db.pool,
        "credential-template",
        None,
        binding_policy(reference),
    )
    .await?;

    // A standing template binding attaches the reference before any sandbox uses it.
    assert_eq!(
        retire(&router, reference).await,
        StatusCode::PRECONDITION_FAILED
    );

    // A live sandbox without an override inherits the binding even after its
    // template is deleted.
    let sandbox = insert_sandbox(&db.pool, SEED_ORG_ID, Some("credential-template"), None).await?;
    sqlx::query(sql!(
        "UPDATE templates SET desired_state = 'deleted' WHERE name = 'credential-template'"
    ))
    .execute(&db.pool)
    .await?;
    assert_eq!(
        retire(&router, reference).await,
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(reference_state(&db.pool, reference).await?, "active");

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(retire(&router, reference).await, StatusCode::NO_CONTENT);
    assert_eq!(reference_state(&db.pool, reference).await?, "retired");
    Ok(())
}

#[tokio::test]
async fn foreign_policies_neither_attach_nor_retire_a_reference() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let router = public_router(&db.pool).await?;
    let reference = register_reference(&router, "foreign-binding").await;
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Other', 'other-credential-org')
         ON CONFLICT (id) DO NOTHING"
    ))
    .bind(OTHER_ORG_ID)
    .execute(&db.pool)
    .await?;
    // Another organization can name the identifier, but its policies can never
    // resolve this organization's reference and must not block retirement.
    insert_sandbox(
        &db.pool,
        OTHER_ORG_ID,
        None,
        Some(binding_policy(reference)),
    )
    .await?;
    insert_template(
        &db.pool,
        "other-org-template",
        Some(OTHER_ORG_ID),
        binding_policy(reference),
    )
    .await?;
    insert_sandbox(&db.pool, OTHER_ORG_ID, Some("other-org-template"), None).await?;

    sqlx::query(sql!(
        "UPDATE credential_references SET org_id = $2 WHERE id = $1"
    ))
    .bind(reference)
    .bind(OTHER_ORG_ID)
    .execute(&db.pool)
    .await?;
    assert_eq!(retire(&router, reference).await, StatusCode::NOT_FOUND);
    sqlx::query(sql!(
        "UPDATE credential_references SET org_id = $2 WHERE id = $1"
    ))
    .bind(reference)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    assert_eq!(retire(&router, reference).await, StatusCode::NO_CONTENT);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn retirement_waits_for_an_in_flight_bind_and_then_refuses() -> TestDbResult<()> {
    // The held bind, the retirement, and the lock probe each need a connection.
    let db = pg_test_db_with_max_connections(4).await?;
    let router = public_router(&db.pool).await?;
    let reference = register_reference(&router, "in-flight-binding").await;
    let sandbox = insert_sandbox(&db.pool, SEED_ORG_ID, None, None).await?;
    let policy: pplx_space_net_common::NetworkPolicyV1 =
        serde_json::from_value(binding_policy(reference))?;

    // The writer's lock is held until commit, as in every policy write path.
    let mut bind = db.pool.begin().await?;
    pplx_space_credential_injection_store::references::lock_for_binding(&mut *bind, &policy)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET network_policy_override_json = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(sqlx::types::Json(&policy))
    .execute(&mut *bind)
    .await?;
    let retirement = tokio::spawn({
        let router = router.clone();
        async move { retire(&router, reference).await }
    });
    wait_for_lock_waiter(&db.pool).await?;
    assert!(
        !retirement.is_finished(),
        "retirement must wait for the bind"
    );
    bind.commit().await?;
    assert_eq!(retirement.await?, StatusCode::PRECONDITION_FAILED);
    assert_eq!(reference_state(&db.pool, reference).await?, "active");
    Ok(())
}
