use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use pplx_space_common_core::{sql, types::SubstrateKind};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::AppState;
use crate::{
    auth::{generate_session_token, hash_token, verify_password},
    external_console::ConsoleAuthConfig,
    middleware::auth::{AuthOrg, AuthUser, bearer_token, require_auth},
    models::{Cluster, LoginRequest, LoginResponse, Organization, User},
};

const SESSION_TTL_SECONDS: i64 = 60 * 60 * 24 * 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ConsoleSubstrate {
    Space,
    K8s,
}

impl ConsoleSubstrate {
    fn from_managed_by(managed_by: &str) -> Self {
        match managed_by.parse::<SubstrateKind>() {
            Ok(SubstrateKind::Kubernetes) => Self::K8s,
            Ok(SubstrateKind::Space) | Err(_) => Self::Space,
        }
    }
}

#[derive(Debug, Serialize)]
struct MeResponse {
    user: User,
    org: Organization,
    substrate: ConsoleSubstrate,
}

#[derive(Debug, thiserror::Error)]
enum AuthRouteError {
    #[error("Invalid email or password")]
    InvalidCredentials,
    #[error("Missing or invalid Authorization header")]
    MissingBearerToken,
    /// External console mode: identity comes from the console's sign-in
    /// provider, so this process issues no password sessions.
    #[error("Password sign-in is disabled; sign in through the console's identity provider")]
    PasswordLoginDisabled,
    #[error("Internal server error")]
    Internal(#[source] sqlx::Error),
    #[error("Internal server error")]
    Password(#[from] bcrypt::BcryptError),
    /// A statement lost a race for a lock. The login path reads `users` and
    /// inserts a `sessions` row, and the session delete takes a row lock, so
    /// any of them can abort with `55P03` under the role-level bound; the
    /// identical request succeeds on retry.
    ///
    /// The whole contention set answers 503 here, unlike the `/admin` surface
    /// (see `admin::support::db_error`), which reserves a 409 for
    /// `40001`/`40P01`. That 409 says "your write raced another admin write",
    /// which is not something to tell someone trying to log in.
    #[error("operation conflicted with a concurrent change; retry the request")]
    Contended,
}

/// Classify rather than derive `#[from]`: the derive would send every SQLSTATE
/// to `Internal`, and a lost lock race is not an internal failure.
impl From<sqlx::Error> for AuthRouteError {
    fn from(error: sqlx::Error) -> Self {
        if pplx_space_common_svc::sql::contention::is_lock_contention(&error) {
            tracing::warn!(%error, "session auth statement lost a lock race; retryable");
            return Self::Contended;
        }
        Self::Internal(error)
    }
}

impl AuthRouteError {
    fn status(&self) -> StatusCode {
        match self {
            Self::InvalidCredentials | Self::MissingBearerToken => StatusCode::UNAUTHORIZED,
            Self::PasswordLoginDisabled => StatusCode::FORBIDDEN,
            Self::Internal(_) | Self::Password(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Contended => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl IntoResponse for AuthRouteError {
    fn into_response(self) -> Response {
        // Delegated for the contention case only: `ControllerError`'s renderer
        // already produces this surface's `{"error": …}` body, attaches the
        // paced `Retry-After` (an un-hinted retryable 503 makes the console
        // retry at RTT against the row that is already contended), and records
        // `otel.status_description` on the span. Hand-rolling it here dropped
        // the last of those.
        if matches!(self, Self::Contended) {
            return crate::controllers::ControllerError::lock_contention().into_response();
        }
        let status = self.status();
        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}

pub(crate) fn router(state: AppState) -> Router<AppState> {
    let protected = Router::new()
        .route("/me", get(me))
        .route("/cluster", get(cluster))
        .route("/session", delete(delete_current_session))
        .layer(middleware::from_fn_with_state(state, require_auth));

    Router::new().nest(
        "/auth/v1",
        Router::new()
            .route("/config", get(auth_config))
            .route("/sessions", post(create_session))
            .merge(protected),
    )
}

/// Unauthenticated auth-mode discovery, so the console can choose between its
/// password form and the external sign-in flow before it holds a credential.
async fn auth_config(State(state): State<AppState>) -> Response {
    let config = state
        .external_console
        .as_ref()
        .map_or(ConsoleAuthConfig::Password, |external_console| {
            external_console.auth_config()
        });
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(config),
    )
        .into_response()
}

async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, AuthRouteError> {
    if state.external_console.is_some() {
        return Err(AuthRouteError::PasswordLoginDisabled);
    }
    Ok(Json(
        create_session_for_password(&state.pool, request).await?,
    ))
}

async fn me(
    State(state): State<AppState>,
    Extension(AuthUser(user)): Extension<AuthUser>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
) -> Json<MeResponse> {
    Json(MeResponse {
        user,
        org,
        substrate: ConsoleSubstrate::from_managed_by(state.sandbox_controller.managed_by()),
    })
}

async fn cluster(State(state): State<AppState>) -> Result<Json<Cluster>, AuthRouteError> {
    Ok(Json(fetch_cluster(&state.pool).await?))
}

async fn delete_current_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<StatusCode, AuthRouteError> {
    // External console mode has no SPACE session to revoke: the console ends
    // the provider session through its logout URL. A bearer token sent
    // anyway is left alone, because this request did not authenticate with
    // it.
    if state.external_console.is_some() {
        return Ok(StatusCode::NO_CONTENT);
    }
    let token = bearer_token(&headers).ok_or(AuthRouteError::MissingBearerToken)?;
    delete_session(&state.pool, token).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_session_for_password(
    pool: &PgPool,
    request: LoginRequest,
) -> Result<LoginResponse, AuthRouteError> {
    let mut tx = pool.begin().await?;
    // Hold the password read through session insertion so resets revoke every
    // session authenticated with the old password.
    let Some(user) =
        sqlx::query_as::<_, User>(sql!("SELECT * FROM users WHERE email = $1 FOR SHARE"))
            .bind(&request.email)
            .fetch_optional(&mut *tx)
            .await?
    else {
        return Err(AuthRouteError::InvalidCredentials);
    };

    if !user.password_login_configured()
        || !verify_password(&request.password, &user.password_hash)?
    {
        return Err(AuthRouteError::InvalidCredentials);
    }

    let org = sqlx::query_as::<_, Organization>(sql!("SELECT * FROM organizations WHERE id = $1"))
        .bind(user.org_id)
        .fetch_one(&mut *tx)
        .await?;

    let (token, token_hash) = generate_session_token();
    sqlx::query(sql!(
        "INSERT INTO sessions (id, user_id, token_hash, expires_at)
         VALUES ($1, $2, $3, NOW() + ($4::bigint * INTERVAL '1 second'))"
    ))
    .bind(Uuid::now_v7())
    .bind(user.id)
    .bind(&token_hash)
    .bind(SESSION_TTL_SECONDS)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(LoginResponse { token, user, org })
}

async fn delete_session(pool: &PgPool, raw_token: &str) -> Result<(), AuthRouteError> {
    let token_hash = hash_token(raw_token);
    sqlx::query(sql!("DELETE FROM sessions WHERE token_hash = $1"))
        .bind(token_hash)
        .execute(pool)
        .await?;
    Ok(())
}

async fn fetch_cluster(pool: &PgPool) -> Result<Cluster, AuthRouteError> {
    Ok(
        sqlx::query_as::<_, Cluster>(sql!("SELECT * FROM clusters WHERE singleton"))
            .fetch_one(pool)
            .await?,
    )
}

#[cfg(test)]
mod tests;
