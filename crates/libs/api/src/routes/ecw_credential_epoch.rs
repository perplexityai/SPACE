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

use crate::{auth::hash_token, routes::AppState};

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
        Err(error) => db_error(&error, sandbox_id),
    }
}

fn db_error(error: &sqlx::Error, sandbox_id: SandboxId) -> Response {
    if contention::is_lock_contention(error) {
        tracing::warn!(%error, %sandbox_id, "ECW credential epoch statement lost a lock race");
        return crate::controllers::ControllerError::lock_contention().into_response();
    }
    tracing::error!(%error, %sandbox_id, "failed to advance sandbox ECW credential epoch");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": "Internal server error"})),
    )
        .into_response()
}

pub(super) fn valid_internal_token(headers: &HeaderMap) -> bool {
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
