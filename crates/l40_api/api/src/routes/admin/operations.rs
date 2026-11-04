use pplx_space_common_core::types::OrgId;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_public_api::{OperationId, OperationKind, OperationState, OperationTargetType};
use serde::Deserialize;

use crate::middleware::auth::AuthUser;
use crate::public_operations::{
    ListAdminPublicOperations, PublicOperationStore, PublicOperationStoreError,
};
use crate::routes::AppState;
use crate::routes::admin::support::{self, ContentionScope, require_managing_org_admin};

const DEFAULT_ADMIN_OPERATION_LIMIT: u16 = 100;
const MAX_ADMIN_OPERATION_LIMIT: u16 = 250;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListAdminOperationsQuery {
    limit: Option<u16>,
    org_id: Option<OrgId>,
    state: Option<String>,
    kind: Option<String>,
    target_type: Option<String>,
    target_id: Option<String>,
}

pub async fn list_admin_operations(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<ListAdminOperationsQuery>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let input = match query.into_store_input() {
        Ok(input) => input,
        Err(err) => return err.into_response(),
    };

    let store = PublicOperationStore::new(state.pool.clone());
    match store.list_operations_admin(input).await {
        Ok(operations) => Json(operations).into_response(),
        Err(err) => store_error(&err).into_response(),
    }
}

pub async fn get_admin_operation(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(operation_id): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let operation_id = match operation_id.parse::<OperationId>() {
        Ok(id) => id,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "operation id must be an op_ prefixed UUID",
            )
            .into_response();
        }
    };

    let store = PublicOperationStore::new(state.pool.clone());
    match store.fetch_operation_admin(operation_id).await {
        Ok(Some(operation)) => Json(operation).into_response(),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "Operation not found").into_response(),
        Err(err) => store_error(&err).into_response(),
    }
}

/// The response for a [`PublicOperationStore`] read failure on this surface.
///
/// Both handlers here are `GET`s that write nothing, so the scope is
/// [`ContentionScope::ReadOnly`]: every contention SQLSTATE answers a
/// retryable 503, and none takes the write-specific 409, which would report a
/// read as the caller's write losing a race.
fn store_error(error: &PublicOperationStoreError) -> (StatusCode, Json<serde_json::Value>) {
    if let Some((status, message)) =
        support::store_contention_disposition(ContentionScope::ReadOnly, error)
    {
        return api_error(status, message);
    }
    tracing::warn!(error = %error, "admin operations store read failed");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
}

impl ListAdminOperationsQuery {
    fn into_store_input(self) -> Result<ListAdminPublicOperations, AdminOperationError> {
        Ok(ListAdminPublicOperations {
            limit: self.limit(),
            org_id: self.org_id,
            state: parse_optional_enum("state", self.state, OperationState::parse_known)?,
            kind: parse_optional_enum("kind", self.kind, OperationKind::parse_known)?,
            target_type: parse_optional_enum(
                "target_type",
                self.target_type,
                OperationTargetType::parse_known,
            )?,
            target_id: self.target_id,
        })
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_ADMIN_OPERATION_LIMIT,
            Some(limit) if limit > MAX_ADMIN_OPERATION_LIMIT => MAX_ADMIN_OPERATION_LIMIT,
            Some(limit) => limit,
        }
    }
}

#[derive(Debug)]
struct AdminOperationError {
    message: String,
}

impl IntoResponse for AdminOperationError {
    fn into_response(self) -> axum::response::Response {
        api_error(StatusCode::BAD_REQUEST, self.message).into_response()
    }
}

/// Parse a filter with the enum's *strict* parser, so an unknown value is a 400
/// rather than an `Other(_)` filter that silently matches nothing. The open
/// response enums implement an infallible `FromStr`, which is right for
/// decoding responses and wrong for validating request input.
fn parse_optional_enum<T, E>(
    field: &'static str,
    value: Option<String>,
    parse_known: fn(&str) -> Result<T, E>,
) -> Result<Option<T>, AdminOperationError> {
    value
        .map(|value| {
            parse_known(&value).map_err(|_| AdminOperationError {
                message: format!("invalid {field}"),
            })
        })
        .transpose()
}

fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

#[cfg(test)]
mod tests;
