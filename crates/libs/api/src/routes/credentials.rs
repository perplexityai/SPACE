use pplx_space_common_core::types::{OrgId, UserId};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{EcwCredentialEpoch, SandboxId};
use pplx_space_common_svc::bearer_token::SPACE_INTERNAL_TOKEN_ENV;
use pplx_space_common_svc::sql::contention;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::{auth::hash_token, models::MergedCredential, routes::AppState};

/// Internal credential reads can block behind DDL even though they take no
/// locks themselves. Preserve the retryable 503, `Retry-After`, and span status
/// that `ControllerError::lock_contention` provides for a lost lock race.
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
/// One function rather than a guard arm per call site: the arms are identical
/// across the internal sandbox credential handlers (`get_sandbox_credentials`
/// and `advance_sandbox_ecw_credential_epoch`). A call site added later without
/// the guard reports a lost race as permanent and nothing catches it.
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
