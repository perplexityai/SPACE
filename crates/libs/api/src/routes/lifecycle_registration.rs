//! Trusted product policy registration and advisory inventory. No lifecycle actions.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_store::lifecycle_registration::{self as store, PolicyBinding};
use serde::{Deserialize, Serialize};

use super::{AppState, ecw_credential_epoch::valid_internal_token};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationRequest {
    enabled: bool,
    binding: PolicyBinding,
    expected_revision: i64,
}

pub async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(sandbox_id): Path<SandboxId>,
    Json(request): Json<RegistrationRequest>,
) -> Response {
    if !valid_internal_token(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "Missing or invalid internal token",
        );
    }
    match store::register(
        &state.pool,
        sandbox_id,
        request.enabled,
        &request.binding,
        request.expected_revision,
    )
    .await
    {
        Ok(Some(revision)) => {
            Json(serde_json::json!({"registration_revision": revision})).into_response()
        }
        Ok(None) => match pplx_space_sandbox_store::get(&state.pool, sandbox_id).await {
            Ok(Some(_)) => match store::revision(&state.pool, sandbox_id).await {
                Ok(revision) => (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": "Registration revision changed", "registration_revision": revision
                    })),
                )
                    .into_response(),
                Err(store::RegistrationError::Database(err)) => database_error(err),
                Err(store::RegistrationError::Invalid(_)) => error_response(),
            },
            Ok(None) => error(StatusCode::NOT_FOUND, "Sandbox not found"),
            Err(pplx_space_sandbox_store::SandboxStoreError::Database(err)) => database_error(err),
        },
        Err(store::RegistrationError::Invalid(_)) => {
            error(StatusCode::BAD_REQUEST, "Invalid registration")
        }
        Err(store::RegistrationError::Database(err)) => database_error(err),
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateQuery {
    limit: Option<u16>,
    cursor: Option<String>,
}

#[derive(Serialize)]
struct Candidate {
    sandbox_id: SandboxId,
    binding: PolicyBinding,
    registration_revision: i64,
    #[serde(serialize_with = "serialize_lifecycle")]
    lifecycle: SandboxLifecycle,
    #[serde(with = "time::serde::rfc3339::option")]
    last_access_at: Option<time::OffsetDateTime>,
}

#[derive(Serialize)]
struct CandidatePage {
    items: Vec<Candidate>,
    next_cursor: Option<String>,
}

pub async fn candidates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CandidateQuery>,
) -> Response {
    if !valid_internal_token(&headers) {
        return error(
            StatusCode::UNAUTHORIZED,
            "Missing or invalid internal token",
        );
    }
    let limit = query.limit.unwrap_or(100);
    if !(1..=500).contains(&limit) {
        return error(StatusCode::BAD_REQUEST, "limit must be between 1 and 500");
    }
    let after = match query.cursor.as_deref().map(decode_cursor).transpose() {
        Ok(after) => after,
        Err(()) => return error(StatusCode::BAD_REQUEST, "Invalid cursor"),
    };
    let mut rows = match store::candidates(&state.pool, after, limit + 1).await {
        Ok(rows) => rows,
        Err(store::RegistrationError::Database(err)) => return database_error(err),
        Err(store::RegistrationError::Invalid(_)) => return error_response(),
    };
    let more = rows.len() > usize::from(limit);
    rows.truncate(usize::from(limit));
    let next_cursor = if more {
        rows.last().map(|row| encode_cursor(row.sandbox_id))
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|row| Candidate {
            sandbox_id: row.sandbox_id,
            registration_revision: row.registration_revision,
            lifecycle: row.lifecycle,
            last_access_at: row.last_access_at,
            binding: PolicyBinding {
                user_id: row.user_id,
                organization_uuid: row.organization_uuid,
                session_id: row.session_id,
                context_uuid: row.context_uuid,
            },
        })
        .collect();
    Json(CandidatePage { items, next_cursor }).into_response()
}

fn serialize_lifecycle<S: serde::Serializer>(
    lifecycle: &SandboxLifecycle,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(lifecycle.as_str())
}

fn encode_cursor(id: SandboxId) -> String {
    URL_SAFE_NO_PAD.encode(format!("lifecycle-v1:{id}"))
}

fn decode_cursor(cursor: &str) -> Result<SandboxId, ()> {
    if cursor.len() > 128 {
        return Err(());
    }
    let decoded = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| ())?;
    let text = std::str::from_utf8(&decoded).map_err(|_| ())?;
    text.strip_prefix("lifecycle-v1:")
        .ok_or(())?
        .parse()
        .map_err(|_| ())
}

fn error(status: StatusCode, message: &'static str) -> Response {
    (status, Json(serde_json::json!({"error": message}))).into_response()
}

fn error_response() -> Response {
    error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

fn database_error(err: sqlx::Error) -> Response {
    if pplx_space_common_svc::sql::contention::is_lock_contention(&err) {
        return crate::controllers::ControllerError::lock_contention().into_response();
    }
    tracing::error!(%err, "lifecycle registration database operation failed");
    error_response()
}

#[cfg(test)]
mod tests;
