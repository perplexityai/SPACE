//! `GET /sandboxes` and `GET /sandboxes/spaced-versions` on the admin
//! surface: the cross-org sandbox list, its cursor and filters, and the count
//! of sandboxes by template and `spaced` version.

use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, TemplateName, UserId};
use pplx_space_placement::StoredSandboxLifecycle;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::sandbox_store::{
    self, AdminSandboxFilter, NodeAssignment, SandboxSearch, SpacedVersionCount,
};
use super::support::{self, ContentionScope, api_error, require_managing_org_admin};
use crate::controllers::sandbox::SandboxPageCursor;
use crate::middleware::auth::AuthUser;
use crate::models::{Sandbox, SandboxResponse};
use crate::routes::AppState;

const DEFAULT_ADMIN_SANDBOX_LIMIT: u32 = 1000;
const MAX_ADMIN_SANDBOX_LIMIT: u32 = 5000;
const CURSOR_VERSION: &str = "v1";

type QueryError = (StatusCode, Json<serde_json::Value>);

/// The list's query string. Unknown parameters are ignored, as they were
/// before the filters existed, so a console that sends a filter this server
/// predates gets the unfiltered list rather than an error.
#[derive(Debug, Default, Deserialize)]
pub struct AdminSandboxListQuery {
    limit: Option<u32>,
    org_id: Option<OrgId>,
    /// Present (empty for the first page) to page through the list; the
    /// response is then `{sandboxes, next_cursor}` instead of a bare array.
    cursor: Option<String>,
    /// Comma-separated stored lifecycles.
    lifecycle: Option<String>,
    template: Option<String>,
    node_id: Option<NodeId>,
    assigned: Option<bool>,
    user_id: Option<UserId>,
    created_after: Option<String>,
    created_before: Option<String>,
    prefix: Option<String>,
}

/// One page of the list, served when the request carries `cursor`.
#[derive(Debug, Serialize)]
struct AdminSandboxPage {
    sandboxes: Vec<SandboxResponse>,
    /// Pass as `cursor` for the next page; `None` on the last page.
    next_cursor: Option<String>,
}

/// How the request pages: not at all (the legacy bare array), or from the
/// start or a cursor.
#[derive(Debug, Clone, Copy)]
enum Paging {
    Unpaged,
    First,
    After(SandboxPageCursor),
}

impl Paging {
    fn is_paged(self) -> bool {
        !matches!(self, Self::Unpaged)
    }

    fn after(self) -> Option<SandboxPageCursor> {
        match self {
            Self::After(cursor) => Some(cursor),
            Self::Unpaged | Self::First => None,
        }
    }
}

#[derive(Debug)]
struct ListRequest {
    limit: u32,
    paging: Paging,
    filter: AdminSandboxFilter,
}

impl AdminSandboxListQuery {
    fn into_request(self) -> Result<ListRequest, QueryError> {
        let limit = self
            .limit
            .unwrap_or(DEFAULT_ADMIN_SANDBOX_LIMIT)
            .clamp(1, MAX_ADMIN_SANDBOX_LIMIT);
        let paging = match self.cursor.as_deref() {
            None => Paging::Unpaged,
            Some("") => Paging::First,
            Some(cursor) => Paging::After(decode_cursor(cursor)?),
        };
        let node = match (self.node_id, self.assigned) {
            (Some(_), Some(false)) => {
                return Err(bad_request(
                    "node_id cannot be combined with assigned=false",
                ));
            }
            (Some(node_id), _) => Some(NodeAssignment::Node(node_id)),
            (None, Some(true)) => Some(NodeAssignment::Assigned),
            (None, Some(false)) => Some(NodeAssignment::Unassigned),
            (None, None) => None,
        };
        let created_after = self
            .created_after
            .as_deref()
            .map(|value| parse_timestamp("created_after", value))
            .transpose()?;
        let created_before = self
            .created_before
            .as_deref()
            .map(|value| parse_timestamp("created_before", value))
            .transpose()?;
        if let (Some(after), Some(before)) = (created_after, created_before)
            && after >= before
        {
            return Err(bad_request(
                "created_after must be earlier than created_before",
            ));
        }
        let filter = AdminSandboxFilter {
            org_id: self.org_id,
            lifecycles: self
                .lifecycle
                .as_deref()
                .map(parse_lifecycles)
                .transpose()?,
            template: self
                .template
                .map(|template| {
                    TemplateName::parse(template)
                        .map_err(|error| bad_request(format!("invalid template: {error}")))
                })
                .transpose()?,
            node,
            user_id: self.user_id,
            created_after,
            created_before,
            search: self
                .prefix
                .filter(|prefix| !prefix.is_empty())
                .map(SandboxSearch::new),
        };
        Ok(ListRequest {
            limit,
            paging,
            filter,
        })
    }
}

fn bad_request(message: impl Into<String>) -> QueryError {
    api_error(StatusCode::BAD_REQUEST, message)
}

fn parse_lifecycles(value: &str) -> Result<Vec<StoredSandboxLifecycle>, QueryError> {
    let mut lifecycles = Vec::new();
    for part in value.split(',') {
        let lifecycle = StoredSandboxLifecycle::from_str(part)
            .map_err(|_| bad_request(format!("unknown lifecycle {part:?}")))?;
        if !lifecycles.contains(&lifecycle) {
            lifecycles.push(lifecycle);
        }
    }
    Ok(lifecycles)
}

fn parse_timestamp(name: &str, value: &str) -> Result<OffsetDateTime, QueryError> {
    let invalid = || bad_request(format!("{name} must be an RFC 3339 timestamp"));
    let timestamp = jiff::Timestamp::from_str(value).map_err(|_| invalid())?;
    OffsetDateTime::from_unix_timestamp_nanos(timestamp.as_nanosecond()).map_err(|_| invalid())
}

/// `v1:<created_at unix nanoseconds>:<id>`, base64url without padding. Not
/// signed: it carries only a position in a list the caller can already read
/// in full.
fn encode_cursor(sandbox: &Sandbox) -> String {
    URL_SAFE_NO_PAD.encode(format!(
        "{CURSOR_VERSION}:{}:{}",
        sandbox.created_at.unix_timestamp_nanos(),
        sandbox.id
    ))
}

fn decode_cursor(cursor: &str) -> Result<SandboxPageCursor, QueryError> {
    let invalid = || bad_request("invalid cursor");
    let raw = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid())?;
    let mut parts = raw.splitn(3, ':');
    let (Some(CURSOR_VERSION), Some(nanos), Some(id)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    let created_at = nanos
        .parse::<i128>()
        .ok()
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .ok_or_else(invalid)?;
    let id = SandboxId::from_str(id).map_err(|_| invalid())?;
    Ok(SandboxPageCursor { created_at, id })
}

pub async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<AdminSandboxListQuery>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request = match query.into_request() {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    // One lookahead row tells a paged request whether another page follows.
    let max_rows = i64::from(request.limit) + i64::from(request.paging.is_paged());
    let mut sandboxes = match sandbox_store::list_sandboxes(
        &state.pool,
        &request.filter,
        request.paging.after(),
        max_rows,
    )
    .await
    {
        Ok(sandboxes) => sandboxes,
        // `ReadOnly`: the busiest admin read on this surface, and a plain
        // `SELECT` over `sandboxes`, so it blocks behind DDL on the hottest
        // table in the schema. It wrote nothing, so every contention SQLSTATE
        // answers a retryable 503 and none takes the write-specific 409 — see
        // `support::contention_disposition`.
        Err(error) => {
            if let Some((status, message)) =
                support::contention_disposition(ContentionScope::ReadOnly, &error)
            {
                return api_error(status, message).into_response();
            }
            tracing::warn!(%error, "admin sandbox list query failed");
            return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
                .into_response();
        }
    };
    let next_cursor = match usize::try_from(request.limit) {
        Ok(limit) if sandboxes.len() > limit => {
            sandboxes.truncate(limit);
            sandboxes.last().map(encode_cursor)
        }
        _ => None,
    };
    // The node-reported rows with no sandbox row are not in the keyset order
    // and belong to no org, so they come once, on the first page of the
    // unfiltered list.
    let include_observed_only =
        !matches!(request.paging, Paging::After(_)) && request.filter.is_unfiltered();
    match state
        .sandbox_controller
        .list_responses(&state.pool, &sandboxes, include_observed_only)
        .await
    {
        Ok(sandboxes) if !request.paging.is_paged() => Json(sandboxes).into_response(),
        Ok(sandboxes) => Json(AdminSandboxPage {
            sandboxes,
            next_cursor,
        })
        .into_response(),
        // A known remainder, in two halves, neither of which is useful
        // without the other.
        //
        // `list_responses` reaches `responses::fetch_sandbox_observed_states`
        // and `fetch_observed_only_sandboxes`, both of which erase the
        // SQLSTATE into `ControllerError::internal` — so no contention
        // identity exists to honour here yet. And this arm flattens the
        // `ControllerError` to a 500 regardless of the status it carried, so
        // classifying those reads alone would not surface it either.
        //
        // Adding an `is_lock_contention()` check here today would be a guard
        // for a state nothing produces. Projection needs to preserve the
        // database error identity, and this handler must also preserve the
        // resulting status. These are independent fixes: classification
        // recovers contention identity, while flattening discards every status,
        // not just contention. Neither alone makes this path retryable.
        // Same shape on `sandboxes::get_sandbox`.
        Err(error) => support::controller_read_error(&error, || {
            tracing::warn!(%error, "admin sandbox response projection failed");
        }),
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpacedVersionCountQuery {
    org_id: Option<OrgId>,
}

#[derive(Debug, Serialize)]
struct SpacedVersionCounts {
    counts: Vec<SpacedVersionCount>,
}

/// Every sandbox that is not deleted, counted by template, the `spaced`
/// version its node last reported, and stored lifecycle.
pub async fn count_spaced_versions(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<SpacedVersionCountQuery>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    match sandbox_store::count_by_spaced_version(&state.pool, query.org_id).await {
        Ok(counts) => Json(SpacedVersionCounts { counts }).into_response(),
        Err(error) => support::read_db_error(error),
    }
}

#[cfg(test)]
mod tests;
