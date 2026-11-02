//! `GET /sandboxes` and `GET /sandboxes/spaced-versions` on the admin
//! surface: the cross-org sandbox list, its cursor and filters, and the count
//! of sandboxes by template and `spaced` version.

use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pplx_space_common_core::types::{OrgId, SandboxId, TemplateName, UserId};
use pplx_space_placement::StoredSandboxLifecycle;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::support::{self, ContentionScope, api_error, require_managing_org_admin};
use crate::controllers::sandbox::SandboxPageCursor;
use crate::controllers::sandbox::list_page::admin::{
    AdminSandboxFilter, PageBudget, SandboxSearch, SpacedVersionCount,
};
use crate::middleware::auth::AuthUser;
use crate::models::{Sandbox, SandboxResponse};
use crate::routes::AppState;

const DEFAULT_ADMIN_SANDBOX_LIMIT: u32 = 1000;
const MAX_ADMIN_SANDBOX_LIMIT: u32 = 5000;
const CURSOR_VERSION: &str = "v1";

/// The list's query parameters. With `cursor` any other parameter is
/// refused; without it, other parameters are ignored, as they were before
/// paging existed, so callers written against that list keep working.
const LIST_PARAMETERS: [&str; 9] = [
    "limit",
    "org_id",
    "cursor",
    "lifecycle",
    "template",
    "user_id",
    "created_after",
    "created_before",
    "prefix",
];
const NANOSECONDS_PER_MICROSECOND: i128 = 1_000;

type QueryError = (StatusCode, Json<serde_json::Value>);

/// The list's query string; see [`LIST_PARAMETERS`] for unknown parameters.
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
    /// Parses the list's query string, answering a malformed one with the
    /// admin JSON error rather than axum's plain-text rejection.
    fn from_uri(uri: &Uri) -> Result<Self, QueryError> {
        let query: Self = parse_query(uri)?;
        if query.cursor.is_some() {
            let pairs: Vec<(String, String)> = parse_query(uri)?;
            if let Some((name, _)) = pairs
                .iter()
                .find(|(name, _)| !LIST_PARAMETERS.contains(&name.as_str()))
            {
                return Err(bad_request(format!("unknown query parameter {name:?}")));
            }
        }
        Ok(query)
    }

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
        let created_after = self
            .created_after
            .as_deref()
            .map(|value| parse_timestamp("created_after", value))
            .transpose()?;
        // Postgres keeps microseconds and the bound value is truncated to
        // them, so a bound inside a microsecond rounds up: a row created
        // earlier in that microsecond is still before it.
        let created_before = self
            .created_before
            .as_deref()
            .map(|value| parse_timestamp("created_before", value).and_then(round_up_to_microsecond))
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
            user_id: self.user_id,
            created_after,
            created_before,
            search: match self.prefix {
                Some(prefix) if prefix.contains('\0') => {
                    return Err(bad_request("prefix must not contain a NUL character"));
                }
                Some(prefix) if !prefix.is_empty() => Some(SandboxSearch::new(prefix)),
                _ => None,
            },
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

fn parse_query<T: DeserializeOwned>(uri: &Uri) -> Result<T, QueryError> {
    Query::<T>::try_from_uri(uri)
        .map(|Query(query)| query)
        .map_err(|rejection| bad_request(rejection.body_text()))
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

fn round_up_to_microsecond(value: OffsetDateTime) -> Result<OffsetDateTime, QueryError> {
    let nanos = value.unix_timestamp_nanos();
    let rounded = match nanos.rem_euclid(NANOSECONDS_PER_MICROSECOND) {
        0 => nanos,
        within => nanos + NANOSECONDS_PER_MICROSECOND - within,
    };
    OffsetDateTime::from_unix_timestamp_nanos(rounded)
        .map_err(|_| bad_request("created_before must be an RFC 3339 timestamp"))
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
    uri: Uri,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let request = match AdminSandboxListQuery::from_uri(&uri).and_then(|query| query.into_request())
    {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    let budget = if request.paging.is_paged() {
        PageBudget::Bounded
    } else {
        PageBudget::Unbounded
    };
    let page = match state
        .sandbox_controller
        .admin_sandbox_page(
            &state.pool,
            &request.filter,
            request.paging.after(),
            request.limit,
            budget,
        )
        .await
    {
        Ok(page) => page,
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
    let sandboxes = page.rows;
    let next_cursor = if page.has_more {
        sandboxes.last().map(encode_cursor)
    } else {
        None
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
struct SpacedVersionCountQuery {
    org_id: Option<OrgId>,
}

#[derive(Debug, Serialize)]
struct SpacedVersionCounts {
    counts: Vec<SpacedVersionCount>,
}

/// The listed sandboxes whose status is not `deleted`, counted by the
/// template, `spaced` version, status and resource type the list shows; see
/// `SandboxController::admin_spaced_version_counts`.
pub async fn count_spaced_versions(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    uri: Uri,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let query: SpacedVersionCountQuery = match parse_query(&uri) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    match state
        .sandbox_controller
        .admin_spaced_version_counts(&state.pool, query.org_id)
        .await
    {
        Ok(counts) => Json(SpacedVersionCounts { counts }).into_response(),
        Err(error) => support::controller_read_error(&error, || {
            tracing::warn!(%error, "admin spaced version count failed");
        }),
    }
}

#[cfg(test)]
mod tests;
