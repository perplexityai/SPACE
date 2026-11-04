//! Native accepts a bearer service credential scoped to the native surface,
//! and does so without inventing a user for it.

use pplx_space_common_core::types::UserId;

use axum::{
    Extension, Router,
    body::Body,
    http::{Request, StatusCode},
    middleware,
    routing::{get, post},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::{AuthOrg, AuthUser, require_auth, require_native_auth};
use crate::routes::AppState;
use pplx_space_public_api::routes::{self as routes};

const SEED_ORG_ID: &str = "00000000-0000-0000-0000-000000000002";
const NATIVE_KEY: &str = "native_only_service_key";
const ECW_KEY: &str = "ecw_only_service_key";

struct Fixture {
    state: AppState,
    _db: TestDb,
}

async fn fixture() -> TestResult<Fixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    seed_key(&pool, NATIVE_KEY, &["native"]).await?;
    seed_key(&pool, ECW_KEY, &["ecw"]).await?;
    let state = AppState::from_pool(pool).await?;
    Ok(Fixture { state, _db: db })
}

async fn seed_key(pool: &PgPool, raw_key: &str, surfaces: &[&str]) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .bind(raw_key)
    .bind(crate::auth::hash_token(raw_key))
    .bind(surfaces)
    .execute(pool)
    .await?;
    Ok(())
}

const SESSION_TOKEN: &str = "a_real_user_session";

async fn seed_session(fixture: &Fixture) -> TestResult<String> {
    // The seeded accounts carry `password_hash = '!'`, which `get_session_user`
    // treats as login-disabled, so this needs a user of its own.
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'not-disabled', 'Session User', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("session-{user_id}@test"))
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .execute(&fixture.state.pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO sessions (user_id, token_hash, expires_at)
         VALUES ($1, $2, NOW() + INTERVAL '1 hour')"
    ))
    .bind(user_id)
    .bind(crate::auth::hash_token(SESSION_TOKEN))
    .execute(&fixture.state.pool)
    .await?;
    Ok(SESSION_TOKEN.to_string())
}

/// Reports which identities the middleware attached, so a missing `AuthUser`
/// is an observable outcome rather than an absence nothing checks.
async fn probe(org: Option<Extension<AuthOrg>>, user: Option<Extension<AuthUser>>) -> String {
    let org = org.map_or("no-org".to_string(), |Extension(AuthOrg(org))| {
        format!("org:{}", org.id)
    });
    let user = user.map_or("no-user".to_string(), |Extension(AuthUser(user))| {
        format!("user:{}", user.id)
    });
    format!("{org} {user}")
}

/// Mirrors production: the operations are registered mount-relative and the
/// router is nested under both public prefixes, so the allowlist is exercised
/// through the same `MatchedPath` the real service sees.
fn app(state: AppState) -> Router {
    let inner: Router<AppState> = Router::new()
        .route(routes::GET_SANDBOX.relative_runtime_path(), get(probe))
        .route(routes::WRITE_FILE.relative_runtime_path(), post(probe))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_native_auth,
        ));
    Router::new()
        .nest("/v1", inner.clone())
        .nest("/api/v1", inner)
        .with_state(state)
}

/// Both public mounts, because the allowlist compares paths and a prefix it
/// does not expect fails closed rather than loudly.
const READ_URIS: [&str; 2] = ["/v1/sandboxes/sbx_1", "/api/v1/sandboxes/sbx_1"];
const WRITE_URIS: [&str; 2] = [
    "/v1/sandboxes/sbx_1/files:write",
    "/api/v1/sandboxes/sbx_1/files:write",
];

async fn send(app: Router, bearer: &str) -> TestResult<(StatusCode, String)> {
    send_to(app, bearer, "GET", READ_URIS[0]).await
}

async fn send_to(
    app: Router,
    bearer: &str,
    method: &str,
    uri: &str,
) -> TestResult<(StatusCode, String)> {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {bearer}"))
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

#[tokio::test]
async fn a_native_scoped_key_authenticates_without_a_user() -> TestResult {
    let fixture = fixture().await?;

    for uri in READ_URIS {
        let (status, body) = send_to(app(fixture.state.clone()), NATIVE_KEY, "GET", uri).await?;

        assert_eq!(status, StatusCode::OK, "{uri}");
        // The org comes from the credential; the caller is a service, so there
        // is no user to attribute the work to yet.
        assert!(body.starts_with("org:"), "{uri}: {body}");
        assert!(body.ends_with("no-user"), "{uri}: {body}");
    }
    Ok(())
}

#[tokio::test]
async fn an_ecw_scoped_key_is_rejected_on_native() -> TestResult {
    let fixture = fixture().await?;

    let (status, _) = send(app(fixture.state.clone()), ECW_KEY).await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn an_unknown_bearer_is_still_rejected() -> TestResult {
    let fixture = fixture().await?;

    let (status, _) = send(app(fixture.state.clone()), "not-a-credential").await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn the_session_router_still_refuses_a_service_credential() -> TestResult {
    // `require_auth` also guards /auth/v1/me, /cluster and DELETE /session. A
    // service credential has no session to manage, so the fallback must not
    // reach them.
    let fixture = fixture().await?;
    let session_only = Router::new()
        .route(routes::GET_SANDBOX.runtime_path(), get(probe))
        .layer(middleware::from_fn_with_state(
            fixture.state.clone(),
            require_auth,
        ))
        .with_state(fixture.state.clone());

    let (status, _) = send(session_only, NATIVE_KEY).await?;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn a_credential_with_no_resolvable_actor_cannot_reach_a_mutation() -> TestResult {
    // The keys this fixture seeds live in an organization with no users, so
    // no actor resolves and there is nobody to attribute the work to. Such a
    // credential stays read-only rather than acting as nobody.
    let fixture = fixture().await?;

    for uri in WRITE_URIS {
        let (status, _) = send_to(app(fixture.state.clone()), NATIVE_KEY, "POST", uri).await?;

        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}");
    }
    Ok(())
}

#[tokio::test]
async fn a_session_may_still_reach_a_mutation() -> TestResult {
    // The allowlist constrains service credentials only; a real session is
    // unaffected by it.
    let fixture = fixture().await?;
    let token = seed_session(&fixture).await?;

    for uri in WRITE_URIS {
        let (status, _) = send_to(app(fixture.state.clone()), &token, "POST", uri).await?;

        assert_eq!(status, StatusCode::OK, "{uri}");
    }
    Ok(())
}

#[test]
fn no_allowlisted_operation_shares_a_mount_with_a_refused_one() {
    // `:action` operations share one runtime path -- MatchedPath cannot see
    // the action segment -- so allowlisting one sibling would silently admit
    // every sibling on the same path and method. The check cannot distinguish
    // them; it can only refuse the situation.
    for allowed in super::SERVICE_CREDENTIAL_OPERATIONS {
        let leaked = routes::NATIVE_V1_OPERATIONS.iter().find(|candidate| {
            !super::SERVICE_CREDENTIAL_OPERATIONS.contains(candidate)
                && candidate.relative_runtime_path() == allowed.relative_runtime_path()
                && candidate.method() == allowed.method()
        });
        assert!(
            leaked.is_none(),
            "{} shares a mount with refused {}",
            allowed.operation_id(),
            leaked.expect("checked").operation_id()
        );
    }
}

#[tokio::test]
async fn every_allowlisted_operation_reaches_its_real_handler() -> TestResult {
    // The probe router proves the middleware attaches identities; it cannot
    // prove the handlers behind the allowlist accept them. Driving the real
    // router catches the case where a handler grows an `Extension<AuthUser>`
    // and a service credential starts getting 500 MissingExtension instead of
    // the 403 the allowlist intends.
    let fixture = fixture().await?;
    let router: Router = Router::new()
        .nest(
            "/v1",
            crate::routes::public::v1_router(fixture.state.clone()),
        )
        .with_state(fixture.state.clone());

    for operation in super::SERVICE_CREDENTIAL_OPERATIONS {
        let uri = format!(
            "/v1{}",
            operation
                .relative_runtime_path()
                .replace("{sandbox_id}", "sbx_00000000-0000-0000-0000-000000000001")
                .replace("{process_id}", "01a0c509-3920-778f-a032-6d1cad5de3df")
                .replace("{operation_id}", "01a0c509-3920-778f-a032-6d1cad5de3df")
        );
        let method = operation.method().openapi_name().to_uppercase();

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.as_str())
                    .uri(&uri)
                    .header("authorization", format!("Bearer {NATIVE_KEY}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?,
            )
            .await?;

        // A 4xx means the handler ran and rejected the fake ids, which is the
        // point. A 5xx means it never got the identities it needed.
        assert!(
            !response.status().is_server_error(),
            "{} returned {} for {method} {uri}",
            operation.operation_id(),
            response.status()
        );
    }
    Ok(())
}

/// A credential whose organization has a user resolves an actor, which is the
/// case a dedicated single-tenant key is in.
async fn seed_key_with_actor(pool: &PgPool, raw_key: &str) -> TestResult<UserId> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'service-actor', 'Service Actor', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("actor-{user_id}@test"))
    .bind(Uuid::parse_str(SEED_ORG_ID)?)
    .execute(pool)
    .await?;
    seed_key(pool, raw_key, &["native"]).await?;
    Ok(user_id)
}

#[tokio::test]
async fn a_credential_acting_as_itself_may_mutate_as_its_actor() -> TestResult {
    // Its own organization is the tenant, so org-scoped lookups are already
    // correct -- the same authority an ECW credential has always had.
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let actor_id = seed_key_with_actor(&pool, "native_key_with_actor").await?;
    let state = AppState::from_pool(pool).await?;

    for uri in WRITE_URIS {
        let (status, body) =
            send_to(app(state.clone()), "native_key_with_actor", "POST", uri).await?;

        assert_eq!(status, StatusCode::OK, "{uri}");
        assert!(body.ends_with(&format!("user:{actor_id}")), "{uri}: {body}");
    }
    Ok(())
}
