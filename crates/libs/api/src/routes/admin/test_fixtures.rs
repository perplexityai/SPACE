//! Postgres fixture for the admin store test suites.
//!
//! Thin shim over the workspace-shared `pplx-space-db-test-fixtures` crate:
//! [`postgres_fixture`] returns `Ok(None)` when no Docker daemon is reachable
//! so the suites skip silently (a green run that skipped these is not a green
//! run that exercised them — bring Docker up). The shared crate clones a
//! per-test database from a migrated template, so every `db/postgres`
//! migration (including the node-group tables) is already applied.

pub(super) use pplx_space_db_test_fixtures::TestDb as Fixture;

/// `Result` alias with a `()` default, matching the call sites' `-> TestResult`.
pub(super) type TestResult<T = ()> = pplx_space_db_test_fixtures::TestDbResult<T>;

/// Graceful fixture: `Ok(None)` when the Postgres testcontainer is
/// unavailable, otherwise a migrated per-test database. `Fixture.pool` is the
/// connection pool.
pub(super) async fn postgres_fixture() -> TestResult<Option<Fixture>> {
    pplx_space_db_test_fixtures::try_pg_test_db().await
}

/// The admin router behind its real auth and audit middleware, with three
/// callers: a managing-organization admin (the operator), an admin of a tenant
/// organization, and nobody.
pub(super) struct AdminRouterHarness {
    pub(super) db: Fixture,
    app: axum::Router,
    pub(super) operator: crate::models::User,
    pub(super) operator_token: String,
    pub(super) tenant_org: pplx_space_common_core::types::OrgId,
    pub(super) tenant_admin: pplx_space_common_core::types::UserId,
    pub(super) tenant_admin_token: String,
}

/// The managing organization the migrations seed.
const MANAGING_ORG: uuid::Uuid = uuid::Uuid::from_u128(0x1);

pub(super) async fn admin_router_harness() -> TestResult<Option<AdminRouterHarness>> {
    use pplx_space_common_core::{
        sql,
        types::{OrgId, UserId},
    };

    let Some(db) = postgres_fixture().await? else {
        return Ok(None);
    };
    let tenant_org = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Tenant', $2)"
    ))
    .bind(tenant_org)
    .bind(format!("tenant-{tenant_org}"))
    .execute(&db.pool)
    .await?;
    let mut sessions = Vec::new();
    for org_id in [OrgId::from_uuid(MANAGING_ORG), tenant_org] {
        let user_id = UserId::new();
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'synthetic-password-hash', 'Admin', $3, 'organization_admin')"
        ))
        .bind(user_id)
        .bind(format!("{user_id}@example.invalid"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;
        let (token, token_hash) = crate::auth::generate_session_token();
        sqlx::query(sql!(
            "INSERT INTO sessions (user_id, token_hash, expires_at)
             VALUES ($1, $2, NOW() + INTERVAL '1 hour')"
        ))
        .bind(user_id)
        .bind(token_hash)
        .execute(&db.pool)
        .await?;
        sessions.push((user_id, token));
    }
    let (tenant_admin, tenant_admin_token) = sessions.pop().expect("tenant session");
    let (operator_id, operator_token) = sessions.pop().expect("operator session");
    let operator =
        sqlx::query_as::<_, crate::models::User>(sql!("SELECT * FROM users WHERE id = $1"))
            .bind(operator_id)
            .fetch_one(&db.pool)
            .await?;
    let state = crate::routes::AppState::from_pool(db.pool.clone()).await?;
    let app = super::shared_host_router(state.clone()).with_state(state);
    Ok(Some(AdminRouterHarness {
        db,
        app,
        operator,
        operator_token,
        tenant_org,
        tenant_admin,
        tenant_admin_token,
    }))
}

impl AdminRouterHarness {
    /// Send a request to `/admin/api/v1{path}` as `token`'s caller, or with
    /// no credential.
    pub(super) async fn call(
        &self,
        token: Option<&str>,
        method: axum::http::Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        use tower::ServiceExt as _;

        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(format!("/admin/api/v1{path}"))
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let body = body.map_or_else(axum::body::Body::empty, |body| {
            axum::body::Body::from(body.to_string())
        });
        let response = self
            .app
            .clone()
            .oneshot(request.body(body).expect("request"))
            .await
            .expect("router answers");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
            })
        };
        (status, value)
    }

    pub(super) async fn as_operator(
        &self,
        method: axum::http::Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        self.call(Some(&self.operator_token), method, path, headers, body)
            .await
    }

    /// Assert that a tenant organization's admin is refused with 403, and a
    /// caller with no credential with 401.
    pub(super) async fn assert_refused_to_non_operators(
        &self,
        method: axum::http::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) {
        let headers = [("idempotency-key", "refused-caller")];
        let (status, _) = self
            .call(
                Some(&self.tenant_admin_token),
                method.clone(),
                path,
                &headers,
                body.clone(),
            )
            .await;
        assert_eq!(
            status,
            axum::http::StatusCode::FORBIDDEN,
            "tenant admin on {method} {path}"
        );
        let (status, _) = self.call(None, method.clone(), path, &headers, body).await;
        assert_eq!(
            status,
            axum::http::StatusCode::UNAUTHORIZED,
            "no credential on {method} {path}"
        );
    }

    /// The persisted audit events for `/admin/api/v1{path}`, newest first, as
    /// `GET /audit-events` lists them. The middleware records the path below
    /// the version mount.
    pub(super) async fn audit_events(&self, path: &str) -> Vec<serde_json::Value> {
        let (status, body) = self
            .as_operator(
                axum::http::Method::GET,
                "/audit-events?limit=250",
                &[],
                None,
            )
            .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        body["events"]
            .as_array()
            .expect("events")
            .iter()
            .filter(|event| event["path"] == path)
            .cloned()
            .collect()
    }
}
