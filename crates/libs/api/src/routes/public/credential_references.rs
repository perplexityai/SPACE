use super::error::PublicApiError;
use crate::{
    credential_injection::references::{self as admission, ReferenceError},
    middleware::auth::AuthOrg,
    routes::AppState,
};
use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use pplx_space_common_core::types::{CredentialReferenceId, IdempotencyKey};
use pplx_space_public_api::credential_references as wire;

pub(crate) async fn register(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Json(request): Json<wire::RegisterCredentialReferenceRequest>,
) -> Result<(StatusCode, Json<wire::CredentialReference>), PublicApiError> {
    let definition = admission::definition(&request).map_err(reference_error)?;
    let request_id = IdempotencyKey::parse(request.request_id())
        .map_err(|_| PublicApiError::bad_request("invalid request_id"))?;
    let reference = admission::register(&state.pool, org.id, &request_id, &definition)
        .await
        .map_err(reference_error)?;
    Ok((StatusCode::CREATED, Json(reference)))
}

pub(crate) async fn retire(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(id): Path<String>,
) -> Result<StatusCode, PublicApiError> {
    let id = id
        .parse::<CredentialReferenceId>()
        .map_err(|_| PublicApiError::bad_request("invalid credential_reference_id"))?;
    admission::retire(&state.pool, org.id, id)
        .await
        .map_err(reference_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn reference_error(error: ReferenceError) -> PublicApiError {
    match error {
        ReferenceError::NotFound => PublicApiError::not_found(error.to_string()),
        ReferenceError::Attached => PublicApiError::failed_precondition(error.to_string()),
        ReferenceError::IdempotencyConflict => {
            PublicApiError::idempotency_conflict(error.to_string())
        }
        ReferenceError::Invalid(_) => PublicApiError::bad_request(error.to_string()),
        ReferenceError::Database(error) => error.into(),
    }
}

#[cfg(test)]
mod tests;
