//! Operator access to any organization's sandbox through routes that mirror
//! public, org-scoped ones: terminal sessions and the snapshot list.

use axum::{
    Extension, Json,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_public_api::{CreateTerminalSessionRequest, PublicSandboxId};

use super::{AdminSandboxTarget, fetch_admin_sandbox};
use crate::middleware::auth::AuthUser;
use crate::models::Sandbox;
use crate::public_operations::OperationPathTemplate;
use crate::routes::AppState;
use crate::routes::admin::audit::AdminAuditTarget;
use crate::routes::public::error::{PublicApiError, database_error};
use crate::routes::public::sessions::{
    create_terminal_session_for_sandbox, idempotency_key as session_idempotency_key,
};
use crate::routes::public::snapshots::{ListSnapshotsQuery, snapshot_page};

/// The idempotency scope of the admin terminal route, distinct from the public
/// route's so a request with the same key and body on the other route never
/// replays this route's session and rotates its attach token, nor the reverse.
const ADMIN_TERMINAL_SESSION_PATH_TEMPLATE: &str =
    "/v1/sandboxes/{sandbox_id}/terminal-sessions:admin";

/// Open a terminal session in any organization's sandbox from the operator
/// surface.
///
/// The admin equivalent of the public `POST /v1/sandboxes/{id}/terminal-sessions`,
/// which answers "sandbox not found" for a sandbox outside the caller's org.
/// Like [`super::create_command`], it resolves the sandbox without an org
/// filter and creates the session under the sandbox's owning org through the
/// public route's session store: its attach token connects to this one session
/// only and expires on the same schedule. Its idempotency keys are its own (see
/// [`ADMIN_TERMINAL_SESSION_PATH_TEMPLATE`]). The request's audit event names
/// the sandbox and its organization once the sandbox resolves, whether or not
/// the session is created.
pub async fn create_terminal_session(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(spec): Path<String>,
    Json(request): Json<CreateTerminalSessionRequest>,
) -> Response {
    let (public_id, sandbox) = match resolve_sandbox(&state, &spec).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    let result = async {
        let idempotency_key = session_idempotency_key(&headers)?;
        create_terminal_session_for_sandbox(
            &state,
            sandbox.org_id,
            actor.id,
            public_id,
            request,
            idempotency_key,
            OperationPathTemplate::new(ADMIN_TERMINAL_SESSION_PATH_TEMPLATE),
        )
        .await
    }
    .await
    .map(|session| (StatusCode::CREATED, Json(session)));
    with_target(AdminAuditTarget::sandbox(sandbox.org_id, public_id), result)
}

/// List any organization's sandbox snapshots from the operator surface.
///
/// The admin equivalent of the public `GET /v1/sandboxes/{id}/snapshots`: the
/// same page, cursor and limit, read under the sandbox's owning organization,
/// whether or not that organization can see the public route. The read is
/// audited once the sandbox resolves, including when its query is refused; the
/// event names the sandbox and its organization.
pub async fn list_snapshots(
    State(state): State<AppState>,
    Path(spec): Path<String>,
    query: Result<Query<ListSnapshotsQuery>, QueryRejection>,
) -> Response {
    let (public_id, sandbox) = match resolve_sandbox(&state, &spec).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    let result = async {
        let Query(query) =
            query.map_err(|rejection| PublicApiError::bad_request(rejection.body_text()))?;
        snapshot_page(&state.pool, sandbox.org_id, public_id, &query).await
    }
    .await
    .map(Json);
    with_target(AdminAuditTarget::sandbox(sandbox.org_id, public_id), result)
}

/// The sandbox `spec` names, in any organization and any form
/// [`AdminSandboxTarget::parse`] accepts, with its canonical `sbx_` ID.
async fn resolve_sandbox(
    state: &AppState,
    spec: &str,
) -> Result<(PublicSandboxId, Sandbox), PublicApiError> {
    let target = AdminSandboxTarget::parse(spec)
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    // Contention on this lookup is classified as on `create_command`.
    let sandbox = fetch_admin_sandbox(&state.pool, target)
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    Ok((target.public_id, sandbox))
}

/// A handler's answer, success or refusal, carrying the resolved `target` for
/// the audit event.
pub(super) fn with_target(
    target: AdminAuditTarget,
    result: Result<impl IntoResponse, PublicApiError>,
) -> Response {
    match result {
        Ok(response) => (Extension(target), response).into_response(),
        Err(error) => (Extension(target), error).into_response(),
    }
}
