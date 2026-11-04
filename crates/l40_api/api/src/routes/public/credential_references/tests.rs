use super::*;
use axum::{Router, body::Body, http::Request};
use http_body_util::BodyExt;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
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

#[tokio::test]
async fn registration_keeps_idempotency_org_scope_and_retirement() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    sqlx::query(sql!("UPDATE users SET password_hash = $1 WHERE id = $2"))
        .bind(crate::auth::hash_password(&Uuid::now_v7().to_string())?)
        .bind(SEED_USER_ID)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!("INSERT INTO sessions (user_id, token_hash, expires_at) VALUES ($1, $2, NOW() + INTERVAL '1 hour')"))
        .bind(SEED_USER_ID)
        .bind(crate::auth::hash_token(SESSION))
        .execute(&db.pool)
        .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let router = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);

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
