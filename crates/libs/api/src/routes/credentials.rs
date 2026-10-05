use pplx_space_common_core::types::{OrgId, UserId};

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{EcwCredentialEpoch, SandboxId};
use pplx_space_common_svc::bearer_token::SPACE_INTERNAL_TOKEN_ENV;
use pplx_space_common_svc::sql::contention;
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{
    auth::hash_token,
    middleware::auth::AuthUser,
    models::{
        CreateCredentialRequest, CredentialDocument, CredentialMetadata, MergedCredential,
        UpdateCredentialRequest,
    },
    routes::AppState,
};

const METADATA_COLUMNS: &str = "id, key, created_at, updated_at";

#[derive(Debug, thiserror::Error)]
enum SealError {
    #[error("credential document serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("credmgr connect failed: {0}")]
    Connect(#[from] pplx_space_credmgr_client::ConnectError),
    #[error("credmgr Encrypt failed: {0}")]
    Encrypt(#[from] tonic::Status),
}

impl SealError {
    fn into_response(self) -> Response {
        // credmgr owns this operation's database work of this operation's database work —
        // `active_kek_for_org` and the `INSERT ... ON CONFLICT` in
        // `provision_active_kek`, both on the `space_creds` login this crate
        // shares with it. It answers a lost race (and its own
        // `LostRaceWithoutWinner`, and a provider outage) with gRPC
        // `Unavailable`, which means "retry" by definition. Flattening that to
        // a 500 here would discard the classification one process later and
        // make the `space_creds` bound exactly the bad trade it was held out
        // of: a slow success becoming a fast permanent-looking failure.
        if let Self::Encrypt(status) = &self
            && status.code() == tonic::Code::Unavailable
        {
            tracing::warn!(error = %status, "credmgr reported the seal is retryable");
            return credential_retryable_response();
        }
        tracing::error!(error = %self, "failed to seal credential");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Credential encryption unavailable"})),
        )
            .into_response()
    }
}

/// 503 + a paced `Retry-After` for a seal that credmgr reported retryable.
///
/// Keeps this surface's `Credential encryption unavailable` wording rather than
/// the lock-race message, because gRPC `Unavailable` covers more than
/// contention here — `LostRaceWithoutWinner` and a key-provider outage reach it
/// too, and all three are "come back" rather than "your write raced".
fn credential_retryable_response() -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error": "Credential encryption unavailable"})),
    )
        .into_response();
    if let Some(value) = crate::controllers::retry_after_header_value(
        crate::controllers::substrate::LOCK_CONTENTION_RETRY_AFTER,
    ) {
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, value);
    }
    response
}

/// 503 + a paced `Retry-After` for a credential statement that lost a race for
/// a lock.
///
/// Every statement on this surface can reach it. The credential writes are
/// `INSERT ... ON CONFLICT` upserts, which block on a concurrent unfinished
/// insert of the same key and raise `55P03` with no locking clause in the SQL
/// to give it away; the reads take no lock of their own but still block behind
/// DDL on the table they read.
///
/// This is the boundary `space_creds`' exclusion from the role-level
/// `lock_timeout` was waiting on: bounding a lane whose callers report every
/// SQL error as a permanent 500 trades a slow success for a fast failure. See
/// `db/README.md`.
///
/// The `warn` is here because the call sites return this *instead of* running
/// their own `error!`, and a bounded lock wait is the expected outcome of
/// enabling the bound rather than a fault to page on. The client-facing message
/// carries no database detail; the log has it.
///
/// Rendered by [`ControllerError::lock_contention`] rather than hand-built:
/// that produces the identical 503 + `{"error": …}` body + `Retry-After` this
/// surface wants, and it also records `otel.status_description` on the span,
/// which a hand-rolled response silently omits.
fn credential_contended_response(error: &sqlx::Error) -> Response {
    tracing::warn!(%error, "credential statement lost a lock race; retryable");
    crate::controllers::ControllerError::lock_contention().into_response()
}

/// The response for a `sqlx` error anywhere on this surface: a lost lock race
/// is retryable, anything else is the opaque 500.
///
/// `log` runs only on the non-contention path, so each call site keeps its own
/// `error!` with its own identifying fields (`user_id` / `org_id` /
/// `sandbox_id`) while a bounded lock wait still lands at `warn`. A helper
/// taking a `&'static str` could not do both.
///
/// One function rather than a guard arm per handler: the arms were identical
/// across the credential handlers. A handler added later without the guard
/// reports a lost race as permanent and nothing
/// catches it.
fn credential_db_error(error: &sqlx::Error, log: impl FnOnce()) -> Response {
    if contention::is_lock_contention(error) {
        return credential_contended_response(error);
    }
    log();
    internal_error_response()
}

fn internal_error_response() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": "Internal server error"})),
    )
        .into_response()
}

fn not_found_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error": "Credential not found"})),
    )
        .into_response()
}

fn credential_write_error(error: &sqlx::Error) -> Response {
    if contention::is_lock_contention(error) {
        return credential_contended_response(error);
    }
    if let Some(db) = error.as_database_error()
        && db.is_unique_violation()
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "Credential key already exists"})),
        )
            .into_response();
    }
    internal_error_response()
}

async fn seal_credential(
    org_id: OrgId,
    user_id: Option<UserId>,
    row_id: Uuid,
    value: String,
    injection_rules: Option<serde_json::Value>,
) -> Result<(Vec<u8>, Vec<u8>), SealError> {
    let document = serde_json::to_vec(&CredentialDocument {
        value,
        injection_rules,
    })?;

    let mut client = pplx_space_credmgr_client::connect_from_env().await?;
    let response = client
        .encrypt(pplx_space_credmgr_client::proto::EncryptRequest {
            organization_id: org_id.to_string(),
            user_id: user_id.map(|id| id.to_string()),
            plaintext: document,
            aad: row_id.as_bytes().to_vec(),
            purpose: "credential".to_string(),
        })
        .await?
        .into_inner();
    Ok((response.ciphertext, response.wrapped_dek))
}

async fn list_user_credentials_for(pool: &PgPool, user_id: UserId) -> Response {
    match sqlx::query_as::<_, CredentialMetadata>(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT {METADATA_COLUMNS} FROM user_credentials WHERE user_id = $1 ORDER BY key"
    ))))
    .bind(user_id)
    .fetch_all(pool)
    .await
    {
        Ok(credentials) => Json(credentials).into_response(),
        Err(error) => credential_db_error(&error, || {
            tracing::error!(%error, %user_id, "failed to list user credentials");
        }),
    }
}

async fn create_user_credential_for(
    pool: &PgPool,
    user_id: UserId,
    org_id: OrgId,
    request: CreateCredentialRequest,
) -> Response {
    let id = Uuid::now_v7();
    let (sealed, wrapped_key) = match seal_credential(
        org_id,
        Some(user_id),
        id,
        request.value,
        request.injection_rules,
    )
    .await
    {
        Ok(sealed) => sealed,
        Err(error) => return error.into_response(),
    };

    match sqlx::query_as::<_, CredentialMetadata>(sqlx::AssertSqlSafe(sql!(format!(
        "INSERT INTO user_credentials (id, user_id, key, value, wrapped_key, org_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING {METADATA_COLUMNS}"
    ))))
    .bind(id)
    .bind(user_id)
    .bind(&request.key)
    .bind(&sealed)
    .bind(&wrapped_key)
    .bind(org_id)
    .fetch_one(pool)
    .await
    {
        Ok(credential) => (StatusCode::CREATED, Json(credential)).into_response(),
        Err(error) => credential_write_error(&error),
    }
}

async fn update_user_credential_for(
    pool: &PgPool,
    user_id: UserId,
    org_id: OrgId,
    id: Uuid,
    request: UpdateCredentialRequest,
) -> Response {
    let (sealed, wrapped_key) = match seal_credential(
        org_id,
        Some(user_id),
        id,
        request.value,
        request.injection_rules,
    )
    .await
    {
        Ok(sealed) => sealed,
        Err(error) => return error.into_response(),
    };

    match sqlx::query_as::<_, CredentialMetadata>(sqlx::AssertSqlSafe(sql!(format!(
        "UPDATE user_credentials
         SET value = $1, wrapped_key = $2, updated_at = NOW()
         WHERE id = $3 AND user_id = $4
         RETURNING {METADATA_COLUMNS}"
    ))))
    .bind(&sealed)
    .bind(&wrapped_key)
    .bind(id)
    .bind(user_id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(credential)) => Json(credential).into_response(),
        Ok(None) => not_found_response(),
        Err(error) => credential_write_error(&error),
    }
}

pub async fn list_user_credentials(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
) -> impl IntoResponse {
    list_user_credentials_for(state.credentials_pool.pool(), actor.id).await
}

pub async fn create_user_credential(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Json(request): Json<CreateCredentialRequest>,
) -> impl IntoResponse {
    create_user_credential_for(
        state.credentials_pool.pool(),
        actor.id,
        actor.org_id,
        request,
    )
    .await
}

pub async fn update_user_credential(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateCredentialRequest>,
) -> impl IntoResponse {
    update_user_credential_for(
        state.credentials_pool.pool(),
        actor.id,
        actor.org_id,
        id,
        request,
    )
    .await
}

pub async fn delete_user_credential(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    match sqlx::query(sql!(
        "DELETE FROM user_credentials WHERE id = $1 AND user_id = $2"
    ))
    .bind(id)
    .bind(actor.id)
    .execute(state.credentials_pool.pool())
    .await
    {
        Ok(result) if result.rows_affected() == 0 => not_found_response(),
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => credential_db_error(&error, || {
            tracing::error!(%error, user_id = %actor.id, %id, "failed to delete user credential");
        }),
    }
}

#[derive(sqlx::FromRow)]
struct SealedCredentialRow {
    id: Uuid,
    key: String,
    value: Vec<u8>,
    wrapped_key: Vec<u8>,
}

pub async fn get_sandbox_credentials(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(sandbox_id): Path<Uuid>,
) -> impl IntoResponse {
    if !valid_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Missing or invalid internal token"})),
        )
            .into_response();
    }

    let sandbox = sqlx::query_as::<_, (UserId, OrgId)>(sql!(
        "SELECT user_id, org_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(&state.pool)
    .await;
    let (user_id, org_id) = match sandbox {
        Ok(Some(row)) => row,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Sandbox not found"})),
            )
                .into_response();
        }
        Err(error) => {
            return credential_db_error(&error, || {
                tracing::error!(%error, %sandbox_id, "failed to load sandbox credentials owner");
            });
        }
    };

    let org_credentials = match sqlx::query_as::<_, SealedCredentialRow>(sql!(
        "SELECT id, key, value, wrapped_key FROM org_credentials WHERE org_id = $1"
    ))
    .bind(org_id)
    .fetch_all(state.credentials_pool.pool())
    .await
    {
        Ok(credentials) => credentials,
        Err(error) => {
            return credential_db_error(&error, || {
                tracing::error!(%error, %org_id, "failed to load sealed org credentials");
            });
        }
    };

    let user_credentials = match sqlx::query_as::<_, SealedCredentialRow>(sql!(
        "SELECT id, key, value, wrapped_key FROM user_credentials WHERE user_id = $1"
    ))
    .bind(user_id)
    .fetch_all(state.credentials_pool.pool())
    .await
    {
        Ok(credentials) => credentials,
        Err(error) => {
            return credential_db_error(&error, || {
                tracing::error!(%error, %user_id, "failed to load sealed user credentials");
            });
        }
    };

    let mut merged = org_credentials
        .into_iter()
        .map(|credential| {
            (
                credential.key.clone(),
                MergedCredential {
                    id: credential.id,
                    key: credential.key,
                    sealed: credential.value,
                    wrapped_key: credential.wrapped_key,
                    user_id: None,
                },
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    for credential in user_credentials {
        merged.insert(
            credential.key.clone(),
            MergedCredential {
                id: credential.id,
                key: credential.key,
                sealed: credential.value,
                wrapped_key: credential.wrapped_key,
                user_id: Some(user_id),
            },
        );
    }

    Json(merged.into_values().collect::<Vec<_>>()).into_response()
}

pub async fn advance_sandbox_ecw_credential_epoch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(sandbox_id): Path<SandboxId>,
) -> impl IntoResponse {
    if !valid_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Missing or invalid internal token"})),
        )
            .into_response();
    }

    let epoch = sqlx::query_scalar::<_, EcwCredentialEpoch>(sql!(
        "INSERT INTO sandbox_ecw_credential_epochs (sandbox_id, epoch)
         SELECT id, 2
           FROM sandboxes
          WHERE id = $1
         ON CONFLICT (sandbox_id) DO UPDATE
             SET epoch = sandbox_ecw_credential_epochs.epoch + 1,
                 updated_at = NOW()
         RETURNING epoch"
    ))
    .bind(sandbox_id)
    .fetch_optional(&state.pool)
    .await;
    match epoch {
        Ok(Some(epoch)) => Json(serde_json::json!({
            "sandboxId": sandbox_id,
            "credentialEpoch": epoch,
        }))
        .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Sandbox not found"})),
        )
            .into_response(),
        Err(error) => credential_db_error(&error, || {
            tracing::error!(%error, %sandbox_id, "failed to advance sandbox ECW credential epoch");
        }),
    }
}

fn valid_internal_token(headers: &HeaderMap) -> bool {
    let expected = std::env::var(SPACE_INTERNAL_TOKEN_ENV).unwrap_or_default();
    let provided = headers
        .get("x-internal-token")
        .and_then(|value| value.to_str().ok());
    valid_internal_token_value(&expected, provided)
}

fn valid_internal_token_value(expected: &str, provided: Option<&str>) -> bool {
    if expected.is_empty() {
        return false;
    }
    let Some(provided) = provided else {
        return false;
    };

    let expected_hash = hash_token(expected);
    let provided_hash = hash_token(provided);
    expected_hash
        .as_bytes()
        .ct_eq(provided_hash.as_bytes())
        .unwrap_u8()
        == 1
}

#[cfg(test)]
mod tests;
