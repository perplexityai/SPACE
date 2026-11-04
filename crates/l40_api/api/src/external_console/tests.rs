//! External console identity: settings validation, the CSRF rule, and the
//! three console gates exercised through the real routers against a real
//! database.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::UserId;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::{
    AuthenticateFuture, CONSOLE_REQUEST_HEADER, ConsoleAuthConfig, ExternalConsole,
    ExternalConsoleAssertion, ExternalConsoleAuthenticator, ExternalConsoleConfigError,
    ExternalConsoleIdentity, ExternalConsoleRejection, ExternalConsoleSettings, OriginRefusal,
    is_console_request,
};
use crate::routes::AppState;

const MANAGING_ORG_ID: &str = "00000000-0000-0000-0000-000000000001";
const CONSOLE_ORIGIN: &str = "https://console.example.test";
const ASSERTION_HEADER: &str = "x-test-assertion";

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

fn var(value: &str) -> Result<String, std::env::VarError> {
    Ok(value.to_owned())
}

fn unset() -> Result<String, std::env::VarError> {
    Err(std::env::VarError::NotPresent)
}

#[test]
fn settings_require_at_least_one_trusted_origin() {
    for value in ["", " ", ",", " , "] {
        assert_eq!(
            ExternalConsoleSettings::from_vars(var(value), unset(), unset()),
            Err(ExternalConsoleConfigError::NoTrustedOrigins),
            "{value:?}"
        );
    }
    assert_eq!(
        ExternalConsoleSettings::from_vars(unset(), unset(), unset()),
        Err(ExternalConsoleConfigError::NoTrustedOrigins)
    );
}

#[test]
fn settings_accept_only_exact_origins() {
    for origin in [
        "https://console.example.test",
        "https://console.example.test:8443",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://[::1]:3000",
    ] {
        let settings = ExternalConsoleSettings::new([origin]);
        assert!(
            settings.is_ok(),
            "{origin:?} should be accepted: {settings:?}"
        );
    }
    for origin in [
        "console.example.test",
        "https://",
        "https://console.example.test/",
        "https://console.example.test/path",
        "https://console.example.test?x=1",
        "https://console.example.test#x",
        "https://user@console.example.test",
        "https://Console.example.test",
        "https://console.example.test:",
        "https://console.example.test:port",
        "ftp://console.example.test",
        "null",
        "*",
    ] {
        assert_eq!(
            ExternalConsoleSettings::new([origin]),
            Err(ExternalConsoleConfigError::InvalidTrustedOrigin(
                origin.to_owned()
            )),
            "{origin:?} should be rejected"
        );
    }
}

#[test]
fn settings_parse_the_environment_contract() -> TestResult {
    let settings = ExternalConsoleSettings::from_vars(
        var(" https://a.example.test , https://b.example.test,https://a.example.test "),
        var("  Company SSO  "),
        var(" /sso/logout "),
    )?;
    assert_eq!(
        settings.trusted_origins(),
        ["https://a.example.test", "https://b.example.test"]
    );
    assert_eq!(settings.provider_name(), "Company SSO");
    assert_eq!(settings.logout_url(), Some("/sso/logout"));

    let defaults = ExternalConsoleSettings::from_vars(var(CONSOLE_ORIGIN), unset(), var(" "))?;
    assert_eq!(defaults.provider_name(), "Single sign-on");
    assert_eq!(defaults.logout_url(), None);
    Ok(())
}

#[test]
fn settings_bound_the_origin_list() {
    let origins: Vec<String> = (0..17)
        .map(|index| format!("https://console{index}.example.test"))
        .collect();
    assert_eq!(
        ExternalConsoleSettings::new(&origins),
        Err(ExternalConsoleConfigError::TooManyTrustedOrigins)
    );
}

#[test]
fn settings_reject_unsafe_logout_urls_and_provider_names() -> TestResult {
    let base = ExternalConsoleSettings::new([CONSOLE_ORIGIN])?;
    for url in [
        "/logout",
        "/sso/logout",
        "https://idp.example.test/logout?return=1",
    ] {
        assert!(base.clone().with_logout_url(url).is_ok(), "{url:?}");
    }
    for url in [
        "",
        "logout",
        "//evil.example.test/logout",
        "/\\evil.example.test",
        "http://idp.example.test/logout",
        "javascript:alert(1)",
        "https://",
        "https://user@idp.example.test/logout",
        "/log out",
    ] {
        assert_eq!(
            base.clone().with_logout_url(url),
            Err(ExternalConsoleConfigError::InvalidLogoutUrl),
            "{url:?}"
        );
    }
    for name in ["", "   ", "line\nbreak", &"x".repeat(65)] {
        assert_eq!(
            base.clone().with_provider_name(name),
            Err(ExternalConsoleConfigError::InvalidProviderName),
            "{name:?}"
        );
    }
    Ok(())
}

#[test]
fn auth_config_serializes_as_the_console_contract() -> TestResult {
    assert_eq!(
        serde_json::to_value(ConsoleAuthConfig::Password)?,
        serde_json::json!({ "mode": "password" })
    );
    assert_eq!(
        serde_json::to_value(ConsoleAuthConfig::External {
            provider_name: "Company SSO".to_owned(),
            logout_url: Some("/logout".to_owned()),
        })?,
        serde_json::json!({
            "mode": "external",
            "provider_name": "Company SSO",
            "logout_url": "/logout",
        })
    );
    assert_eq!(
        serde_json::to_value(ConsoleAuthConfig::External {
            provider_name: "Company SSO".to_owned(),
            logout_url: None,
        })?,
        serde_json::json!({ "mode": "external", "provider_name": "Company SSO" })
    );
    Ok(())
}

#[test]
fn identity_debug_does_not_print_the_email() {
    let rendered = format!("{:?}", ExternalConsoleIdentity::new("person@example.test"));
    assert!(!rendered.contains("person@example.test"), "{rendered}");
}

// ---------------------------------------------------------------------------
// CSRF rule
// ---------------------------------------------------------------------------

fn console(settings: ExternalConsoleSettings) -> ExternalConsole {
    ExternalConsole::new(Arc::new(HeaderAuthenticator), settings)
}

fn headers(pairs: &[(&'static str, &str)]) -> TestResult<HeaderMap> {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(*name, value.parse()?);
    }
    Ok(map)
}

#[test]
fn safe_methods_need_no_origin() -> TestResult {
    let console = console(ExternalConsoleSettings::new([CONSOLE_ORIGIN])?);
    for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
        assert_eq!(
            console.check_request_origin(&method, &HeaderMap::new()),
            Ok(()),
            "{method}"
        );
    }
    Ok(())
}

#[test]
fn console_requests_carry_the_console_header_exactly_once() -> TestResult {
    assert!(is_console_request(&headers(&[(
        CONSOLE_REQUEST_HEADER,
        "1"
    )])?));
    assert!(!is_console_request(&HeaderMap::new()));
    for value in ["", "0", "true", "1 ", "yes"] {
        assert!(
            !is_console_request(&headers(&[(CONSOLE_REQUEST_HEADER, value)])?),
            "{value:?}"
        );
    }
    let mut twice = headers(&[(CONSOLE_REQUEST_HEADER, "1")])?;
    twice.append(CONSOLE_REQUEST_HEADER, "1".parse()?);
    assert!(!is_console_request(&twice));
    Ok(())
}

#[test]
fn state_changing_methods_need_a_trusted_origin() -> TestResult {
    let console = console(ExternalConsoleSettings::new([
        CONSOLE_ORIGIN,
        "http://localhost:3000",
    ])?);
    for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
        let accepted = headers(&[("origin", CONSOLE_ORIGIN), (CONSOLE_REQUEST_HEADER, "1")])?;
        assert_eq!(console.check_request_origin(&method, &accepted), Ok(()));
        let second = headers(&[
            ("origin", "http://localhost:3000"),
            (CONSOLE_REQUEST_HEADER, "1"),
        ])?;
        assert_eq!(console.check_request_origin(&method, &second), Ok(()));

        for (refused, why) in [
            (
                headers(&[(CONSOLE_REQUEST_HEADER, "1")])?,
                OriginRefusal::Missing,
            ),
            (
                headers(&[
                    ("origin", "https://evil.example.test"),
                    (CONSOLE_REQUEST_HEADER, "1"),
                ])?,
                OriginRefusal::Untrusted,
            ),
            (
                headers(&[("origin", "null"), (CONSOLE_REQUEST_HEADER, "1")])?,
                OriginRefusal::Untrusted,
            ),
            (
                headers(&[
                    ("origin", "https://console.example.test.evil.test"),
                    (CONSOLE_REQUEST_HEADER, "1"),
                ])?,
                OriginRefusal::Untrusted,
            ),
            (
                headers(&[
                    ("origin", "http://console.example.test"),
                    (CONSOLE_REQUEST_HEADER, "1"),
                ])?,
                OriginRefusal::Untrusted,
            ),
        ] {
            assert_eq!(
                console.check_request_origin(&method, &refused),
                Err(why),
                "{method} {refused:?}"
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Gates, through the real routers
// ---------------------------------------------------------------------------

/// Test authenticator: the `x-test-assertion` header stands in for a signed
/// assertion the real adapter would verify.
struct HeaderAuthenticator;

impl ExternalConsoleAuthenticator for HeaderAuthenticator {
    fn authenticate<'a>(&'a self, headers: &'a HeaderMap) -> AuthenticateFuture<'a> {
        Box::pin(async move {
            let Some(value) = headers.get(ASSERTION_HEADER) else {
                return ExternalConsoleAssertion::Absent;
            };
            match value.to_str().unwrap_or_default() {
                "invalid" => {
                    ExternalConsoleAssertion::Rejected(ExternalConsoleRejection::Unauthenticated)
                }
                "forbidden" => {
                    ExternalConsoleAssertion::Rejected(ExternalConsoleRejection::Forbidden)
                }
                "unavailable" => {
                    ExternalConsoleAssertion::Rejected(ExternalConsoleRejection::Unavailable)
                }
                other => match other.strip_prefix("email:") {
                    Some(email) => {
                        ExternalConsoleAssertion::Verified(ExternalConsoleIdentity::new(email))
                    }
                    None => ExternalConsoleAssertion::Rejected(
                        ExternalConsoleRejection::Unauthenticated,
                    ),
                },
            }
        })
    }
}

struct Seeded {
    admin_email: String,
    admin_id: UserId,
    admin_session: String,
    member_email: String,
    member_id: UserId,
    member_password: String,
    member_session: String,
    locked_member_email: String,
    federated_email: String,
    disabled_service_email: String,
    native_key: String,
    ecw_key: String,
}

struct Fixture {
    password_state: AppState,
    external_state: AppState,
    seeded: Seeded,
    _db: TestDb,
}

impl Fixture {
    fn public(&self) -> Router {
        crate::routes::router(self.external_state.clone())
    }

    fn admin(&self) -> Router {
        crate::routes::dedicated_admin_router(self.external_state.clone())
    }

    fn shared(&self) -> Router {
        crate::routes::shared_host_router(self.external_state.clone())
    }

    fn password_public(&self) -> Router {
        crate::routes::router(self.password_state.clone())
    }

    fn password_admin(&self) -> Router {
        crate::routes::dedicated_admin_router(self.password_state.clone())
    }

    fn password_shared(&self) -> Router {
        crate::routes::shared_host_router(self.password_state.clone())
    }
}

async fn fixture() -> TestResult<Fixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let suffix = Uuid::now_v7().simple().to_string();
    let org_id = Uuid::parse_str(MANAGING_ORG_ID)?;

    let admin_email = format!("admin-{suffix}@example.test");
    let admin_password = Uuid::now_v7().to_string();
    let admin_id = seed_local_user(
        &pool,
        &admin_email,
        Some(&admin_password),
        org_id,
        "organization_admin",
    )
    .await?;
    let admin_session = seed_session(&pool, admin_id).await?;

    let member_email = format!("member-{suffix}@example.test");
    let member_password = Uuid::now_v7().to_string();
    let member_id = seed_local_user(
        &pool,
        &member_email,
        Some(&member_password),
        org_id,
        "member",
    )
    .await?;
    let member_session = seed_session(&pool, member_id).await?;

    let locked_member_email = format!("locked-{suffix}@example.test");
    seed_local_user(&pool, &locked_member_email, None, org_id, "member").await?;

    let disabled_service_email = format!("service-{suffix}@example.test");
    seed_local_user_with_hash(
        &pool,
        &disabled_service_email,
        "disabled-service-account",
        org_id,
        "member",
    )
    .await?;

    let federated_email = format!("federated-{suffix}@example.test");
    seed_federated_user(&pool, &federated_email, &suffix).await?;

    let native_key = format!("native-{suffix}");
    seed_key(&pool, &native_key, org_id, &["native"]).await?;
    let ecw_key = format!("ecw-{suffix}");
    seed_key(&pool, &ecw_key, org_id, &["ecw"]).await?;

    let password_state = AppState::from_pool(pool.clone()).await?;
    let mut external_state = password_state.clone();
    external_state.external_console = Some(ExternalConsole::new(
        Arc::new(HeaderAuthenticator),
        ExternalConsoleSettings::new([CONSOLE_ORIGIN])?
            .with_provider_name("Company SSO")?
            .with_logout_url("/sso/logout")?,
    ));

    Ok(Fixture {
        password_state,
        external_state,
        seeded: Seeded {
            admin_email,
            admin_id,
            admin_session,
            member_email,
            member_id,
            member_password,
            member_session,
            locked_member_email,
            federated_email,
            disabled_service_email,
            native_key,
            ecw_key,
        },
        _db: db,
    })
}

async fn seed_local_user(
    pool: &PgPool,
    email: &str,
    password: Option<&str>,
    org_id: Uuid,
    role: &str,
) -> TestResult<UserId> {
    let hash = match password {
        Some(password) => crate::auth::hash_password(password)?,
        None => "!".to_owned(),
    };
    seed_local_user_with_hash(pool, email, &hash, org_id, role).await
}

async fn seed_local_user_with_hash(
    pool: &PgPool,
    email: &str,
    password_hash: &str,
    org_id: Uuid,
    role: &str,
) -> TestResult<UserId> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, $3, 'Console User', $4, $5)"
    ))
    .bind(user_id)
    .bind(email)
    .bind(password_hash)
    .bind(org_id)
    .bind(role)
    .execute(pool)
    .await?;
    Ok(user_id)
}

async fn seed_federated_user(pool: &PgPool, email: &str, suffix: &str) -> TestResult {
    let org_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug, owner_source, upstream_issuer, upstream_org_id)
         VALUES ($1, $2, $3, 'federated_identity', 'https://issuer.example.test', $4)"
    ))
    .bind(org_id)
    .bind(format!("Federated {suffix}"))
    .bind(format!("federated-{suffix}"))
    .bind(format!("org-{suffix}"))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role,
                            owner_source, upstream_issuer, upstream_subject_id)
         VALUES ($1, $2, '!', 'Federated User', $3, 'organization_admin',
                 'federated_identity', 'https://issuer.example.test', $4)"
    ))
    .bind(UserId::new())
    .bind(email)
    .bind(org_id)
    .bind(format!("subject-{suffix}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_session(pool: &PgPool, user_id: UserId) -> TestResult<String> {
    let token = format!("session-{}", Uuid::now_v7().simple());
    sqlx::query(sql!(
        "INSERT INTO sessions (user_id, token_hash, expires_at)
         VALUES ($1, $2, NOW() + INTERVAL '1 hour')"
    ))
    .bind(user_id)
    .bind(crate::auth::hash_token(&token))
    .execute(pool)
    .await?;
    Ok(token)
}

async fn seed_key(pool: &PgPool, raw_key: &str, org_id: Uuid, surfaces: &[&str]) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(raw_key)
    .bind(crate::auth::hash_token(raw_key))
    .bind(surfaces)
    .execute(pool)
    .await?;
    Ok(())
}

async fn session_exists(pool: &PgPool, token: &str) -> TestResult<bool> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE token_hash = $1)"
    ))
    .bind(crate::auth::hash_token(token))
    .fetch_one(pool)
    .await?)
}

/// One request: method, path, and headers.
struct Call<'a> {
    method: Method,
    uri: &'a str,
    headers: Vec<(&'static str, String)>,
    body: Option<serde_json::Value>,
    console: bool,
}

impl<'a> Call<'a> {
    fn get(uri: &'a str) -> Self {
        Self {
            method: Method::GET,
            uri,
            headers: Vec::new(),
            body: None,
            console: false,
        }
    }

    fn new(method: Method, uri: &'a str) -> Self {
        Self {
            method,
            uri,
            headers: Vec::new(),
            body: None,
            console: false,
        }
    }

    /// A console request: carries the console request header.
    fn console(mut self) -> Self {
        self.console = true;
        self
    }

    /// A console request carrying `value` as its assertion.
    fn assertion(self, value: impl Into<String>) -> Self {
        self.console().machine_assertion(value)
    }

    /// An assertion a proxy attached to a request that is not a console
    /// request.
    fn machine_assertion(mut self, value: impl Into<String>) -> Self {
        self.headers.push((ASSERTION_HEADER, value.into()));
        self
    }

    fn signed_in_as(self, email: &str) -> Self {
        self.assertion(format!("email:{email}"))
    }

    fn bearer(mut self, token: &str) -> Self {
        self.headers
            .push(("authorization", format!("Bearer {token}")));
        self
    }

    fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// The Origin the console sends on a state-changing request.
    fn console_origin(self) -> Self {
        self.console().header("origin", CONSOLE_ORIGIN)
    }

    fn json(mut self, body: serde_json::Value) -> Self {
        self.body = Some(body);
        self
    }

    async fn send(self, app: Router) -> TestResult<(StatusCode, serde_json::Value)> {
        let mut builder = Request::builder().method(self.method).uri(self.uri);
        if self.console {
            builder = builder.header(CONSOLE_REQUEST_HEADER, "1");
        }
        for (name, value) in &self.headers {
            builder = builder.header(*name, value);
        }
        let body = match self.body {
            Some(body) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&body)?)
            }
            None => Body::empty(),
        };
        let response = app.oneshot(builder.body(body)?).await?;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await?;
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
            })
        };
        Ok((status, json))
    }
}

fn user_id_of(body: &serde_json::Value) -> Option<String> {
    body.get("user")?.get("id")?.as_str().map(str::to_owned)
}

#[tokio::test]
async fn auth_config_reports_the_mode_on_both_surfaces() -> TestResult {
    let fixture = fixture().await?;
    for app in [fixture.public(), fixture.admin(), fixture.shared()] {
        let (status, body) = Call::get("/auth/v1/config").send(app).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            serde_json::json!({
                "mode": "external",
                "provider_name": "Company SSO",
                "logout_url": "/sso/logout",
            })
        );
    }
    for app in [
        fixture.password_public(),
        fixture.password_admin(),
        fixture.password_shared(),
    ] {
        let (status, body) = Call::get("/auth/v1/config").send(app).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "mode": "password" }));
    }
    Ok(())
}

#[tokio::test]
async fn an_allowed_member_reaches_the_console_without_a_password() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for app in [fixture.public(), fixture.admin(), fixture.shared()] {
        let (status, body) = Call::get("/auth/v1/me")
            .signed_in_as(&seeded.member_email)
            .send(app.clone())
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(user_id_of(&body), Some(seeded.member_id.to_string()));
        assert_eq!(body["user"]["role"], "member");

        let (status, body) = Call::get("/auth/v1/cluster")
            .signed_in_as(&seeded.member_email)
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    for uri in ["/v1/sandboxes", "/api/v1/sandboxes"] {
        let (status, body) = Call::get(uri)
            .signed_in_as(&seeded.member_email)
            .send(fixture.public())
            .await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
    }
    let (status, body) = Call::get("/api/v1/sandboxes")
        .signed_in_as(&seeded.member_email)
        .send(fixture.shared())
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}

#[tokio::test]
async fn a_member_keeps_member_permissions_and_cannot_use_admin_operations() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for (app, uri) in [
        (fixture.admin(), "/admin/api/v1/orgs"),
        (fixture.shared(), "/admin/api/v1/orgs"),
    ] {
        let (status, body) = Call::get(uri)
            .signed_in_as(&seeded.member_email)
            .send(app.clone())
            .await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "member {uri}: {body}");

        let (status, body) = Call::new(Method::POST, uri)
            .signed_in_as(&seeded.member_email)
            .console_origin()
            .json(serde_json::json!({ "name": "nope", "slug": "nope" }))
            .send(app.clone())
            .await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "member POST {uri}: {body}");

        let (status, body) = Call::get(uri)
            .signed_in_as(&seeded.admin_email)
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::OK, "admin {uri}: {body}");
    }

    let (status, body) = Call::get("/auth/v1/me")
        .signed_in_as(&seeded.admin_email)
        .send(fixture.admin())
        .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(user_id_of(&body), Some(seeded.admin_id.to_string()));
    assert_eq!(body["user"]["role"], "organization_admin");
    Ok(())
}

#[tokio::test]
async fn admin_writes_through_external_sign_in_are_audited_and_csrf_protected() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;
    let slug = format!("console-{}", Uuid::now_v7().simple());
    let body = serde_json::json!({ "name": "Console Org", "slug": slug });

    for (call, expected) in [
        (
            Call::new(Method::POST, "/admin/api/v1/orgs")
                .signed_in_as(&seeded.admin_email)
                .json(body.clone()),
            StatusCode::FORBIDDEN,
        ),
        (
            Call::new(Method::POST, "/admin/api/v1/orgs")
                .signed_in_as(&seeded.admin_email)
                .header("origin", "https://evil.example.test")
                .json(body.clone()),
            StatusCode::FORBIDDEN,
        ),
        // A cross-site form cannot add the console request header, and without
        // it the assertion is not consulted at all.
        (
            Call::new(Method::POST, "/admin/api/v1/orgs")
                .machine_assertion(format!("email:{}", seeded.admin_email))
                .header("origin", CONSOLE_ORIGIN)
                .json(body.clone()),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let (status, response) = call.send(fixture.admin()).await?;
        assert_eq!(status, expected, "{response}");
    }

    let (status, response) = Call::new(Method::POST, "/admin/api/v1/orgs")
        .signed_in_as(&seeded.admin_email)
        .console_origin()
        .json(body)
        .send(fixture.admin())
        .await?;
    assert!(status.is_success(), "{status}: {response}");

    // Only the admitted write is audited, attributed to the SPACE user the
    // identity mapped to; the refused attempts never reached the handler.
    let audited: Vec<(String, String, i32)> = sqlx::query_as(sql!(
        "SELECT method, path, status_code FROM admin_audit_events WHERE actor_user_id = $1"
    ))
    .bind(seeded.admin_id)
    .fetch_all(&fixture.external_state.pool)
    .await?;
    assert_eq!(audited.len(), 1, "{audited:?}");
    assert_eq!(audited[0].0, "POST");
    assert!(audited[0].1.ends_with("/orgs"), "{audited:?}");
    assert_eq!(i32::from(status.as_u16()), audited[0].2);
    Ok(())
}

#[tokio::test]
async fn unknown_and_ineligible_identities_are_refused() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;
    let unknown = "nobody@example.test".to_owned();
    let differently_cased = seeded.member_email.to_uppercase();

    for email in [
        &unknown,
        &differently_cased,
        &seeded.federated_email,
        &seeded.disabled_service_email,
    ] {
        for (app, uri) in [
            (fixture.public(), "/auth/v1/me"),
            (fixture.public(), "/api/v1/sandboxes"),
            (fixture.admin(), "/auth/v1/me"),
            (fixture.admin(), "/admin/api/v1/orgs"),
        ] {
            let (status, body) = Call::get(uri).signed_in_as(email).send(app).await?;
            assert_eq!(status, StatusCode::FORBIDDEN, "{email} {uri}: {body}");
        }
    }

    // A local user without a password is still a SPACE user: the external
    // sign-in replaces the password, and its member role still applies.
    let (status, body) = Call::get("/auth/v1/me")
        .signed_in_as(&seeded.locked_member_email)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["user"]["role"], "member");
    Ok(())
}

#[tokio::test]
async fn refused_assertions_never_fall_back_to_another_credential() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for (assertion, expected) in [
        ("invalid", StatusCode::UNAUTHORIZED),
        ("not-a-known-shape", StatusCode::UNAUTHORIZED),
        ("forbidden", StatusCode::FORBIDDEN),
        ("unavailable", StatusCode::SERVICE_UNAVAILABLE),
    ] {
        for (app, uri, credential) in [
            (fixture.public(), "/auth/v1/me", &seeded.member_session),
            (
                fixture.public(),
                "/api/v1/sandboxes",
                &seeded.member_session,
            ),
            (fixture.public(), "/api/v1/sandboxes", &seeded.native_key),
            (fixture.admin(), "/auth/v1/me", &seeded.admin_session),
            (fixture.admin(), "/admin/api/v1/orgs", &seeded.admin_session),
        ] {
            let (status, body) = Call::get(uri)
                .assertion(assertion)
                .bearer(credential)
                .send(app)
                .await?;
            assert_eq!(status, expected, "{assertion} {uri}: {body}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn password_sessions_cannot_bypass_external_mode() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for (app, uri, token) in [
        (fixture.public(), "/auth/v1/me", &seeded.member_session),
        (fixture.public(), "/auth/v1/cluster", &seeded.member_session),
        (
            fixture.public(),
            "/api/v1/sandboxes",
            &seeded.member_session,
        ),
        (
            fixture.shared(),
            "/api/v1/sandboxes",
            &seeded.member_session,
        ),
        (fixture.admin(), "/auth/v1/me", &seeded.admin_session),
        (fixture.admin(), "/admin/api/v1/orgs", &seeded.admin_session),
        (
            fixture.shared(),
            "/admin/api/v1/orgs",
            &seeded.admin_session,
        ),
    ] {
        let (status, body) = Call::get(uri).bearer(token).send(app).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {body}");
    }

    // The same sessions still work once the mode is off.
    let (status, _) = Call::get("/auth/v1/me")
        .bearer(&seeded.member_session)
        .send(fixture.password_public())
        .await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = Call::get("/admin/api/v1/orgs")
        .bearer(&seeded.admin_session)
        .send(fixture.password_admin())
        .await?;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn password_login_is_disabled_and_logout_revokes_nothing() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    let (status, body) = Call::new(Method::POST, "/auth/v1/sessions")
        .json(serde_json::json!({
            "email": seeded.member_email,
            "password": seeded.member_password,
        }))
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.get("token").is_none(), "{body}");

    // Logout is a state-changing request, so it needs the console context.
    let (status, body) = Call::new(Method::DELETE, "/auth/v1/session")
        .signed_in_as(&seeded.member_email)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, _) = Call::new(Method::DELETE, "/auth/v1/session")
        .signed_in_as(&seeded.member_email)
        .console_origin()
        .bearer(&seeded.member_session)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        session_exists(&fixture.external_state.pool, &seeded.member_session).await?,
        "external logout must not act on a credential it did not authenticate with"
    );

    let (status, _) = Call::new(Method::DELETE, "/auth/v1/session")
        .bearer(&seeded.member_session)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Password mode keeps the original contract.
    let (status, body) = Call::new(Method::POST, "/auth/v1/sessions")
        .json(serde_json::json!({
            "email": seeded.member_email,
            "password": seeded.member_password,
        }))
        .send(fixture.password_public())
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("token").and_then(|token| token.as_str()).is_some());
    let (status, _) = Call::new(Method::DELETE, "/auth/v1/session")
        .bearer(&seeded.member_session)
        .send(fixture.password_public())
        .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(!session_exists(&fixture.password_state.pool, &seeded.member_session).await?);
    Ok(())
}

#[tokio::test]
async fn native_state_changing_requests_require_the_console_context() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;
    let create = serde_json::json!({});

    for (call, expected) in [
        (
            Call::new(Method::POST, "/api/v1/sandboxes")
                .signed_in_as(&seeded.member_email)
                .json(create.clone()),
            StatusCode::FORBIDDEN,
        ),
        (
            Call::new(Method::POST, "/api/v1/sandboxes")
                .signed_in_as(&seeded.member_email)
                .header("origin", "https://evil.example.test")
                .json(create.clone()),
            StatusCode::FORBIDDEN,
        ),
        (
            Call::new(Method::POST, "/api/v1/sandboxes")
                .machine_assertion(format!("email:{}", seeded.member_email))
                .header("origin", CONSOLE_ORIGIN)
                .json(create.clone()),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let (status, body) = call.send(fixture.public()).await?;
        assert_eq!(status, expected, "{body}");
    }

    // With the console context the gate admits the request and the handler
    // decides; whatever it answers, it is not an authentication refusal.
    let (status, body) = Call::new(Method::POST, "/api/v1/sandboxes")
        .signed_in_as(&seeded.member_email)
        .console_origin()
        .json(create)
        .send(fixture.public())
        .await?;
    assert!(
        status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN,
        "{status}: {body}"
    );
    Ok(())
}

#[tokio::test]
async fn console_sign_in_is_a_single_credential_and_cannot_assert_an_owner() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    let (status, body) = Call::get("/api/v1/sandboxes")
        .signed_in_as(&seeded.member_email)
        .bearer(&seeded.native_key)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    let (status, body) = Call::get("/api/v1/sandboxes")
        .signed_in_as(&seeded.member_email)
        .header(
            crate::federated_identity::FEDERATED_ORG_ID_HEADER,
            "upstream-org",
        )
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    Ok(())
}

#[tokio::test]
async fn machine_credentials_keep_working_in_external_mode() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for app in [fixture.public(), fixture.password_public()] {
        let (status, body) = Call::get("/api/v1/sandboxes")
            .bearer(&seeded.native_key)
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    for app in [fixture.shared(), fixture.password_shared()] {
        let (status, body) = Call::get("/e2b/api/sandboxes")
            .header("x-api-key", seeded.ecw_key.clone())
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    // An ECW request never consults the console authenticator, even when a
    // proxy attached an assertion to it.
    let (status, body) = Call::get("/e2b/api/sandboxes")
        .machine_assertion("invalid")
        .header("x-api-key", seeded.ecw_key.clone())
        .send(fixture.shared())
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Neither does a native machine request that reached SPACE through the
    // same identity-aware proxy: whatever assertion the proxy attached, the
    // service credential alone decides, as it would without the proxy.
    for assertion in ["invalid", "forbidden", "unavailable"] {
        let (status, body) = Call::get("/api/v1/sandboxes")
            .machine_assertion(assertion)
            .bearer(&seeded.native_key)
            .send(fixture.public())
            .await?;
        assert_eq!(status, StatusCode::OK, "{assertion}: {body}");
    }
    // ...and such a request still cannot use a password session.
    let (status, body) = Call::get("/api/v1/sandboxes")
        .machine_assertion(format!("email:{}", seeded.member_email))
        .bearer(&seeded.member_session)
        .send(fixture.public())
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    Ok(())
}

#[tokio::test]
async fn console_requests_without_an_assertion_are_refused() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for (app, uri) in [
        (fixture.public(), "/auth/v1/me"),
        (fixture.public(), "/api/v1/sandboxes"),
        (fixture.shared(), "/api/v1/sandboxes"),
        (fixture.admin(), "/auth/v1/me"),
        (fixture.admin(), "/admin/api/v1/orgs"),
    ] {
        let (status, body) = Call::get(uri).console().send(app.clone()).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {body}");
        // A console request is decided by its assertion, so a service
        // credential does not stand in for a missing one.
        let (status, body) = Call::get(uri)
            .console()
            .bearer(&seeded.native_key)
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri} with a key: {body}");
    }
    Ok(())
}

#[tokio::test]
async fn password_mode_ignores_assertion_headers() -> TestResult {
    let fixture = fixture().await?;
    let seeded = &fixture.seeded;

    for (app, uri) in [
        (fixture.password_public(), "/auth/v1/me"),
        (fixture.password_public(), "/api/v1/sandboxes"),
        (fixture.password_admin(), "/admin/api/v1/orgs"),
    ] {
        let (status, body) = Call::get(uri)
            .signed_in_as(&seeded.admin_email)
            .send(app)
            .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {body}");
    }
    Ok(())
}

#[tokio::test]
async fn cross_origin_preflights_never_allow_credentials() -> TestResult {
    // The CSRF rule assumes a cross-site page can never make the browser send
    // the proxy's cookie along with the console header.
    let fixture = fixture().await?;
    for app in [fixture.public(), fixture.admin(), fixture.shared()] {
        let preflight = Request::builder()
            .method(Method::OPTIONS)
            .uri("/auth/v1/me")
            .header("origin", "https://evil.example.test")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", CONSOLE_REQUEST_HEADER)
            .body(Body::empty())?;
        let response = app.oneshot(preflight).await?;
        assert!(
            response
                .headers()
                .get("access-control-allow-credentials")
                .is_none(),
            "{:?}",
            response.headers()
        );
    }
    Ok(())
}
