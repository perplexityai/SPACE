//! Native accepts an asserted effective owner only from a credential trusted
//! to make one, and refuses an unusable assertion rather than ignoring it.

use axum::{
    Extension, Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
    routing::{get, post},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_public_api::routes::{self as routes};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::{AuthFederatedOwner, require_native_auth};
use crate::federated_identity::{
    FEDERATED_ORG_ID_HEADER, FEDERATED_USER_ID_HEADER, FederatedIdentityConfig,
    FederatedIdentityPolicy,
};
use crate::routes::AppState;

const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";
const TRUSTED_KEY: &str = "native_trusted_key";
const PLAIN_KEY: &str = "native_plain_key";
const UPSTREAM_ORG_ID: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";
const READ_URI: &str = "/v1/sandboxes/sbx_1";
const WRITE_URI: &str = "/v1/sandboxes/sbx_1/files:write";

struct Fixture {
    state: AppState,
    _db: TestDb,
}

async fn fixture(federated_enabled: bool) -> TestResult<Fixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    seed_key(&pool, TRUSTED_KEY, true).await?;
    seed_key(&pool, PLAIN_KEY, false).await?;
    let mut state = AppState::from_pool(pool).await?;
    state.federated_identity = FederatedIdentityConfig {
        enabled: federated_enabled,
        policy: FederatedIdentityPolicy::parse(Some("test-issuer"), None, Some(r#"["0"]"#))?,
    };
    Ok(Fixture { state, _db: db })
}

async fn seed_key(pool: &PgPool, raw_key: &str, assertions_enabled: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys
             (id, org_id, name, key_hash, allowed_surfaces, federated_owner_assertions_enabled)
         VALUES ($1, $2, $3, $4, ARRAY['native'], $5)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(raw_key)
    .bind(crate::auth::hash_token(raw_key))
    .bind(assertions_enabled)
    .execute(pool)
    .await?;
    Ok(())
}

async fn probe(owner: Option<Extension<AuthFederatedOwner>>) -> String {
    match owner {
        Some(Extension(AuthFederatedOwner(owner))) => format!(
            "federated:{}:{}",
            owner.org_id.as_str(),
            owner.subject_id.as_str()
        ),
        None => "no-owner".to_string(),
    }
}

fn app(state: AppState) -> Router {
    let inner: Router<AppState> = Router::new()
        .route(routes::GET_SANDBOX.relative_runtime_path(), get(probe))
        .route(routes::WRITE_FILE.relative_runtime_path(), post(probe))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_native_auth,
        ));
    Router::new().nest("/v1", inner).with_state(state)
}

async fn send(
    app: Router,
    bearer: &str,
    owner_headers: &[(&str, &str)],
) -> TestResult<(StatusCode, String)> {
    send_to(app, bearer, "GET", READ_URI, owner_headers).await
}

async fn send_to(
    app: Router,
    bearer: &str,
    method: &str,
    uri: &str,
    owner_headers: &[(&str, &str)],
) -> TestResult<(StatusCode, String)> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {bearer}"));
    for (name, value) in owner_headers {
        request = request.header(*name, *value);
    }
    let response = app.oneshot(request.body(Body::empty())?).await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

fn owner_tuple() -> [(&'static str, &'static str); 2] {
    [
        (FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID),
        (FEDERATED_USER_ID_HEADER, "123"),
    ]
}

#[tokio::test]
async fn a_valid_assertion_reaches_the_handler_as_an_effective_owner() -> TestResult {
    let fixture = fixture(true).await?;

    let (status, body) = send(app(fixture.state.clone()), TRUSTED_KEY, &owner_tuple()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, format!("federated:{UPSTREAM_ORG_ID}:123"));
    Ok(())
}

#[tokio::test]
async fn a_trusted_assertion_lifts_the_actor_allowlist() -> TestResult {
    // Resource authorization runs on the public router, after authentication.
    let fixture = fixture(true).await?;

    let (status, _) = send_to(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        "POST",
        WRITE_URI,
        &owner_tuple(),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn an_untrusted_key_may_not_assert_an_owner() -> TestResult {
    let fixture = fixture(true).await?;

    let (status, _) = send(app(fixture.state.clone()), PLAIN_KEY, &owner_tuple()).await?;

    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn an_incomplete_owner_tuple_is_refused_not_ignored() -> TestResult {
    // Ignoring it would run the mutation as the credential's own org.
    let fixture = fixture(true).await?;

    let (status, _) = send(
        app(fixture.state.clone()),
        TRUSTED_KEY,
        &[(FEDERATED_ORG_ID_HEADER, UPSTREAM_ORG_ID)],
    )
    .await?;

    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn an_assertion_is_refused_when_federated_identity_is_off() -> TestResult {
    let fixture = fixture(false).await?;

    let (status, _) = send(app(fixture.state.clone()), TRUSTED_KEY, &owner_tuple()).await?;

    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn a_trusted_key_without_an_assertion_still_reads() -> TestResult {
    let fixture = fixture(true).await?;

    let (status, body) = send(app(fixture.state.clone()), TRUSTED_KEY, &[]).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "no-owner");
    Ok(())
}

async fn federated_row_versions(pool: &PgPool) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(sql!(
        "SELECT o.xmin::text, u.xmin::text
         FROM organizations o
         JOIN users u ON u.org_id = o.id
         WHERE o.owner_source = 'federated_identity'
           AND o.upstream_org_id = $1
           AND u.upstream_subject_id = '123'"
    ))
    .bind(UPSTREAM_ORG_ID)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn a_materialized_owner_authenticates_without_rewriting_its_rows() -> TestResult {
    // Every asserted request authenticates; the upsert path would rewrite and
    // lock the tenant's organization row each time.
    let fixture = fixture(true).await?;
    let (status, _) = send(app(fixture.state.clone()), TRUSTED_KEY, &owner_tuple()).await?;
    assert_eq!(status, StatusCode::OK);
    let materialized = federated_row_versions(&fixture.state.pool).await?;

    let (status, body) = send(app(fixture.state.clone()), TRUSTED_KEY, &owner_tuple()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, format!("federated:{UPSTREAM_ORG_ID}:123"));
    assert_eq!(
        federated_row_versions(&fixture.state.pool).await?,
        materialized
    );
    Ok(())
}

#[tokio::test]
async fn identity_failures_preserve_retry_responses() -> TestResult {
    use crate::federated_identity::{MaterializeOwnerError, ResolvePrincipalError};
    use axum::response::IntoResponse;
    use pplx_space_db_test_fixtures::raise_sqlstate;

    let db = pg_test_db().await?;
    type WrapError = fn(sqlx::Error) -> ResolvePrincipalError;
    let stages: [WrapError; 6] = [
        ResolvePrincipalError::Lookup,
        ResolvePrincipalError::Reload,
        |error| MaterializeOwnerError::Begin(error).into(),
        |error| MaterializeOwnerError::Organization(error).into(),
        |error| MaterializeOwnerError::User(error).into(),
        |error| MaterializeOwnerError::Commit(error).into(),
    ];
    for wrap in stages {
        for (code, contention) in [
            ("40001", true),
            ("40P01", true),
            ("55P03", true),
            ("57014", false),
        ] {
            let response =
                super::federated_identity_error(wrap(raise_sqlstate(&db.pool, code).await))
                    .into_response();
            let retryable = contention;
            assert_eq!(
                response.status(),
                if retryable {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            );
            assert_eq!(response.headers().contains_key("retry-after"), retryable);
        }
        let response =
            super::federated_identity_error(wrap(sqlx::Error::PoolTimedOut)).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key("retry-after"));
    }
    Ok(())
}
