use crate::federated_identity::FederatedIdentityPolicy;
use axum::{
    Extension, Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
    routing::get,
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::{AuthFederatedOwner, require_e2b_api_key};
use crate::federated_identity::{
    FEDERATED_ORG_ID_HEADER, FEDERATED_USER_ID_HEADER, FederatedIdentityConfig,
};
use crate::routes::AppState;

use pplx_space_test_env::TestResult;

const TRUSTED_KEY_ID: &str = "00000000-0000-0000-0000-0000000000aa";
const TRUSTED_KEY: &str = "e2b_trusted_federated_key";
const TENANT_KEY: &str = "e2b_tenant_key";
const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";
const UPSTREAM_ORG_ID: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";

struct AppFixture {
    state: AppState,
    _db: TestDb,
}

async fn app_fixture() -> TestResult<AppFixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();

    seed_api_key(&pool, Uuid::parse_str(TRUSTED_KEY_ID)?, TRUSTED_KEY, true).await?;
    seed_api_key(&pool, Uuid::now_v7(), TENANT_KEY, false).await?;

    let mut state = AppState::from_pool(pool).await?;
    state.federated_identity = FederatedIdentityConfig {
        enabled: true,
        policy: FederatedIdentityPolicy::parse(Some("test-issuer"), None, Some(r#"["0"]"#))?,
    };

    Ok(AppFixture { state, _db: db })
}

async fn seed_api_key(
    pool: &PgPool,
    id: Uuid,
    raw_key: &str,
    federated_owner_assertions_enabled: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys (
            id,
            org_id,
            name,
            key_hash,
            federated_owner_assertions_enabled
        ) VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(id)
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(raw_key)
    .bind(crate::auth::hash_token(raw_key))
    .bind(federated_owner_assertions_enabled)
    .execute(pool)
    .await?;
    Ok(())
}

async fn probe(owner: Option<Extension<AuthFederatedOwner>>) -> String {
    match owner {
        Some(Extension(AuthFederatedOwner(owner))) => format!(
            "federated:{}:{}:{}",
            owner.issuer, owner.org_id, owner.subject_id
        ),
        None => "local".to_string(),
    }
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/probe", get(probe))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_e2b_api_key,
        ))
        .with_state(state)
}

async fn send(
    app: Router,
    api_key: &str,
    owner_headers: &[(&str, &str)],
) -> TestResult<(StatusCode, String)> {
    let mut request = Request::builder()
        .uri("/probe")
        .header("x-api-key", api_key);
    for (name, value) in owner_headers {
        request = request.header(*name, *value);
    }
    let response = app.oneshot(request.body(Body::empty())?).await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

#[tokio::test]
async fn trusted_key_without_owner_headers_stays_local() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, body) = send(app(fixture.state.clone()), TRUSTED_KEY, &[]).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "local");
    Ok(())
}

#[tokio::test]
async fn trusted_key_with_valid_tuple_attaches_federated_owner() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, body) = send(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        &[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "123"),
        ],
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, format!("federated:test-issuer:{UPSTREAM_ORG_ID}:123"));
    Ok(())
}

#[tokio::test]
async fn trusted_key_without_org_header_fails_closed() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, _) = send(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        &[(FEDERATED_USER_ID_HEADER, "123")],
    )
    .await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn trusted_key_with_placeholder_subject_fails_closed() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, _) = send(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        &[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "0"),
        ],
    )
    .await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn untrusted_key_with_owner_headers_is_rejected() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, _) = send(
        app(fixture.state.clone()),
        TENANT_KEY,
        &[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "123"),
        ],
    )
    .await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn untrusted_key_without_owner_headers_stays_local() -> TestResult {
    let fixture = app_fixture().await?;

    let (status, body) = send(app(fixture.state.clone()), TENANT_KEY, &[]).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "local");
    Ok(())
}

#[tokio::test]
async fn disabled_mode_ignores_owner_headers() -> TestResult {
    let mut fixture = app_fixture().await?;
    fixture.state.federated_identity = FederatedIdentityConfig::disabled();

    let (status, body) = send(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        &[
            (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
            (FEDERATED_USER_ID_HEADER, "123"),
        ],
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "local");
    Ok(())
}
