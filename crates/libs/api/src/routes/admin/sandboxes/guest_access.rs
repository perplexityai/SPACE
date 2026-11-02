//! Operator access to any organization's sandbox through routes that mirror
//! public, org-scoped ones: terminal sessions and the snapshot list.

use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use pplx_space_public_api::{
    CreateTerminalSessionRequest, PublicPage, PublicSandboxId, PublicSession, PublicSnapshot,
};

use super::{AdminSandboxTarget, fetch_admin_sandbox};
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::audit::AdminAuditTarget;
use crate::routes::public::error::{PublicApiError, database_error};
use crate::routes::public::sessions::{
    create_terminal_session_for_sandbox, idempotency_key as session_idempotency_key,
};
use crate::routes::public::snapshots::{ListSnapshotsQuery, snapshot_page};

/// Open a terminal session in any organization's sandbox from the operator
/// surface.
///
/// The admin equivalent of the public `POST /v1/sandboxes/{id}/terminal-sessions`,
/// which answers "sandbox not found" for a sandbox outside the caller's org.
/// Like [`super::create_command`], it resolves the sandbox without an org filter and
/// creates the session under the sandbox's owning org, so the session is the
/// public route's: its attach token connects to this one session only and
/// expires on the same schedule. The request's audit event names the sandbox
/// and its organization.
pub async fn create_terminal_session(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(spec): Path<String>,
    Json(request): Json<CreateTerminalSessionRequest>,
) -> Result<(StatusCode, Extension<AdminAuditTarget>, Json<PublicSession>), PublicApiError> {
    let public_id = PublicSandboxId::from_str(&spec)
        .map_err(|_| PublicApiError::bad_request("sandbox id must be an sbx_ prefixed UUID"))?;
    // Contention on this lookup is classified as on `create_command`.
    let sandbox = fetch_admin_sandbox(&state.pool, AdminSandboxTarget::from_public_id(public_id))
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    let idempotency_key = session_idempotency_key(&headers)?;
    let session = create_terminal_session_for_sandbox(
        &state,
        sandbox.org_id,
        actor.id,
        public_id,
        request,
        idempotency_key,
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Extension(AdminAuditTarget::sandbox(sandbox.org_id, public_id)),
        Json(session),
    ))
}

/// List any organization's sandbox snapshots from the operator surface.
///
/// The admin equivalent of the public `GET /v1/sandboxes/{id}/snapshots`: the
/// same page, cursor and limit, read under the sandbox's owning organization,
/// whether or not that organization can see the public route. The read is
/// audited: its audit event names the sandbox and its organization.
pub async fn list_snapshots(
    State(state): State<AppState>,
    Path(spec): Path<String>,
    Query(query): Query<ListSnapshotsQuery>,
) -> Result<
    (
        Extension<AdminAuditTarget>,
        Json<PublicPage<PublicSnapshot>>,
    ),
    PublicApiError,
> {
    let public_id = PublicSandboxId::from_str(&spec)
        .map_err(|_| PublicApiError::bad_request("sandbox id must be an sbx_ prefixed UUID"))?;
    let sandbox = fetch_admin_sandbox(&state.pool, AdminSandboxTarget::from_public_id(public_id))
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    let page = snapshot_page(&state.pool, sandbox.org_id, public_id, &query).await?;

    Ok((
        Extension(AdminAuditTarget::sandbox(sandbox.org_id, public_id)),
        Json(page),
    ))
}
