use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::{ApiKeyId, OrgId, UserId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{
    api_key_store::{self, CreateApiKey, RevokeOutcome},
    audit::AdminAuditTarget,
    support::{self, ContentionScope, api_error},
};
use crate::{api_surface::ApiSurface, auth::hash_token, routes::AppState};

const API_KEY_NAME_MAX_CHARS: usize = 128;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateApiKeyRequest {
    name: String,
    allowed_surfaces: Vec<ApiSurface>,
    /// The user of the organization the key acts as, recorded as its
    /// `created_by`. Defaults to the organization's oldest user.
    #[serde(default)]
    user_id: Option<UserId>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ListApiKeysQuery {
    /// List revoked keys too, with their `revoked_at`. Off by default, so a
    /// caller that doesn't read `revoked_at` never shows a revoked key as
    /// live.
    #[serde(default)]
    include_revoked: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct ApiKeyMetadata {
    id: ApiKeyId,
    org_id: OrgId,
    name: String,
    /// The user the key acts as. A key created through this API records the
    /// user chosen at creation; the calling admin is recorded in the request's
    /// audit event.
    created_by: Option<UserId>,
    created_by_email: Option<String>,
    allowed_surfaces: Vec<ApiSurface>,
    /// When the key last authenticated through this API, to the precision of
    /// `auth::API_KEY_LAST_USED_GRANULARITY`; `None` if it never has.
    /// Data-plane uses are not recorded: a key used only for ECW guest traffic
    /// keeps its last API use.
    #[serde(with = "time::serde::rfc3339::option")]
    last_used_at: Option<OffsetDateTime>,
    /// When the key was revoked; `None` while it is live. A revoked key no
    /// longer authenticates.
    #[serde(with = "time::serde::rfc3339::option")]
    revoked_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
}

#[derive(Serialize)]
pub(super) struct CreatedApiKey {
    api_key: ApiKeyMetadata,
    secret: String,
}

impl From<api_key_store::ApiKeyRow> for ApiKeyMetadata {
    fn from(row: api_key_store::ApiKeyRow) -> Self {
        Self {
            id: row.id,
            org_id: row.org_id,
            name: row.name,
            created_by: row.created_by,
            created_by_email: row.created_by_email,
            allowed_surfaces: row.allowed_surfaces,
            last_used_at: row.last_used_at,
            revoked_at: row.revoked_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// List an organization's keys, newest first, revoked ones with
/// `include_revoked=true`. Any organization's keys can be listed; the admin
/// surface already requires a managing-organization admin.
pub(super) async fn list_api_keys(
    State(state): State<AppState>,
    Path(org_id): Path<OrgId>,
    Query(query): Query<ListApiKeysQuery>,
) -> Response {
    match api_key_store::organization_exists(&state.pool, org_id).await {
        Ok(false) => {
            return api_error(StatusCode::NOT_FOUND, "Organization not found").into_response();
        }
        Ok(true) => {}
        Err(error) => return read_error(error),
    }
    match api_key_store::list_for_organization(&state.pool, org_id, query.include_revoked).await {
        Ok(keys) => Json(
            keys.into_iter()
                .map(ApiKeyMetadata::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => read_error(error),
    }
}

/// Create a key for any organization.
///
/// The key belongs to `org_id`, calls the APIs in `allowed_surfaces`, and acts
/// as the user chosen here: `user_id`, which must be a user of the
/// organization, or else the organization's oldest user. That user is stored
/// as the key's `created_by`, the column every release's key lookup reads, so
/// no release resolves the key to a different user or to none. An
/// organization with no users answers 409.
///
/// The request's audit event names the calling admin, the organization, the
/// new key and its user; the secret appears only in this response.
pub(super) async fn create_api_key(
    State(state): State<AppState>,
    Path(org_id): Path<OrgId>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Response {
    match api_key_store::organization_exists(&state.pool, org_id).await {
        Ok(true) => {}
        Ok(false) => {
            return api_error(StatusCode::NOT_FOUND, "Organization not found").into_response();
        }
        Err(error) => return read_error(error),
    }
    let org_target = Extension(AdminAuditTarget::api_key(org_id, None, None));
    let name = match api_key_name(&request.name) {
        Ok(name) => name,
        Err(message) => {
            return (org_target, api_error(StatusCode::BAD_REQUEST, message)).into_response();
        }
    };
    let allowed_surfaces = match allowed_surfaces(request.allowed_surfaces) {
        Ok(surfaces) => surfaces,
        Err(message) => {
            return (org_target, api_error(StatusCode::BAD_REQUEST, message)).into_response();
        }
    };
    let user_id = match key_user(&state, org_id, request.user_id).await {
        Ok(user_id) => user_id,
        Err(refusal) => return (org_target, refusal.into_response()).into_response(),
    };
    let secret = generate_api_key();
    let key_hash = hash_token(&secret);
    let api_key_id = ApiKeyId::new();
    let input = CreateApiKey {
        id: api_key_id,
        org_id,
        name,
        key_hash: &key_hash,
        created_by: user_id,
        allowed_surfaces: &allowed_surfaces,
    };
    match api_key_store::create(&state.pool, input).await {
        Ok(row) => (
            StatusCode::CREATED,
            [(header::CACHE_CONTROL, "no-store")],
            Extension(AdminAuditTarget::api_key(
                org_id,
                Some(api_key_id),
                Some(user_id),
            )),
            Json(CreatedApiKey {
                api_key: row.into(),
                secret,
            }),
        )
            .into_response(),
        Err(error) => (
            Extension(AdminAuditTarget::api_key(org_id, None, Some(user_id))),
            support::db_error(error),
        )
            .into_response(),
    }
}

/// Why no user could be chosen for a new key.
enum KeyUserRefusal {
    NotInOrganization,
    NoUsers,
    Read(sqlx::Error),
}

impl KeyUserRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::NotInOrganization => api_error(
                StatusCode::BAD_REQUEST,
                "user_id must name a user of the organization",
            )
            .into_response(),
            Self::NoUsers => api_error(
                StatusCode::CONFLICT,
                "The organization has no users for the key to act as; create a user first",
            )
            .into_response(),
            Self::Read(error) => read_error(error),
        }
    }
}

/// The user a new key of `org_id` acts as.
async fn key_user(
    state: &AppState,
    org_id: OrgId,
    requested: Option<UserId>,
) -> Result<UserId, KeyUserRefusal> {
    match requested {
        Some(user_id) => {
            match api_key_store::user_in_organization(&state.pool, org_id, user_id).await {
                Ok(true) => Ok(user_id),
                Ok(false) => Err(KeyUserRefusal::NotInOrganization),
                Err(error) => Err(KeyUserRefusal::Read(error)),
            }
        }
        None => match api_key_store::oldest_user(&state.pool, org_id).await {
            Ok(Some(user_id)) => Ok(user_id),
            Ok(None) => Err(KeyUserRefusal::NoUsers),
            Err(error) => Err(KeyUserRefusal::Read(error)),
        },
    }
}

/// Revoke a key. It stops authenticating at once and lists, with
/// `revoked_at`, under `include_revoked`. Revoking a revoked key succeeds and
/// keeps its first revocation time.
///
/// The audit event names the key and its organization only when the key is
/// one of the path's organization's.
pub(super) async fn delete_api_key(
    State(state): State<AppState>,
    Path((org_id, api_key_id)): Path<(OrgId, ApiKeyId)>,
) -> Response {
    match api_key_store::revoke(&state.pool, org_id, api_key_id).await {
        Ok(RevokeOutcome::Revoked | RevokeOutcome::AlreadyRevoked) => (
            StatusCode::NO_CONTENT,
            Extension(AdminAuditTarget::api_key(org_id, Some(api_key_id), None)),
        )
            .into_response(),
        Ok(RevokeOutcome::NotFound) => {
            api_error(StatusCode::NOT_FOUND, "API key not found").into_response()
        }
        Err(error) => support::db_error(error).into_response(),
    }
}

fn api_key_name(raw: &str) -> Result<&str, &'static str> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("API key name must not be empty");
    }
    if name.chars().count() > API_KEY_NAME_MAX_CHARS {
        return Err("API key name must be at most 128 characters");
    }
    if name.chars().any(char::is_control) {
        return Err("API key name must not contain control characters");
    }
    Ok(name)
}

fn allowed_surfaces(surfaces: Vec<ApiSurface>) -> Result<Vec<ApiSurface>, &'static str> {
    let mut ecw = false;
    let mut native = false;
    for surface in surfaces {
        let seen = match surface {
            ApiSurface::Ecw => &mut ecw,
            ApiSurface::Native => &mut native,
        };
        if *seen {
            return Err("Allowed APIs must not contain duplicates");
        }
        *seen = true;
    }
    match (ecw, native) {
        (false, false) => Err("Select at least one allowed API"),
        (true, false) => Ok(vec![ApiSurface::Ecw]),
        (false, true) => Ok(vec![ApiSurface::Native]),
        (true, true) => Ok(vec![ApiSurface::Ecw, ApiSurface::Native]),
    }
}

fn generate_api_key() -> String {
    let mut bytes = [0_u8; 32];
    rand::fill(&mut bytes);
    hex::encode(bytes)
}

fn read_error(error: sqlx::Error) -> Response {
    if let Some((status, message)) =
        support::contention_disposition(ContentionScope::ReadOnly, &error)
    {
        return api_error(status, message).into_response();
    }
    tracing::warn!(%error, "admin API key read failed");
    api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
}

#[cfg(test)]
mod tests;
