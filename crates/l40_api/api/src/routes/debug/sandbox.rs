use pplx_space_common_core::types::SandboxId;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use time::OffsetDateTime;

use crate::{
    middleware::auth::AuthUser,
    models::User,
    routes::{
        AppState,
        admin::sandbox_id::parse_admin_sandbox_id,
        admin::support::{self, ContentionScope},
        authz::is_managing_org_admin,
        debug::{
            response::{DebugSection, SandboxDebugResponse},
            sources::{self, source_error},
        },
    },
};

const SCHEMA_VERSION: &str = "1";

fn api_error(status: StatusCode, error: &'static str) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}

/// The response for a lost lock race, or `None` when `error` is a real
/// failure.
///
/// This surface is mounted inside the admin v1 router (by
/// `super::admin_v1_debug_router`, which `admin::v1_router` merges), so it takes
/// its disposition from the same place the `/admin` handlers do rather than
/// reproducing the split. Every statement reachable from here is a read, so the
/// scope is always [`ContentionScope::ReadOnly`]: every contention SQLSTATE
/// answers a retryable 503, and none of them takes the write-specific 409,
/// which would report a read as the caller's write losing a race.
fn contention_response(error: &sqlx::Error) -> Option<Response> {
    let (status, message) = support::contention_disposition(ContentionScope::ReadOnly, error)?;
    Some((status, Json(serde_json::json!({ "error": message }))).into_response())
}

/// This module's copy of the operator gate, kept only because it answers in
/// `Response` rather than `support`'s tuple. See
/// `support::require_managing_org_admin` for why the contention arm classifies
/// and why it is `ReadOnly`.
async fn require_managing_org_admin(state: &AppState, actor: &User) -> Result<(), Box<Response>> {
    match is_managing_org_admin(&state.pool, actor).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(Box::new(api_error(
            StatusCode::FORBIDDEN,
            "Insufficient permissions",
        ))),
        // Classified here or the handler's own 503 is unreachable: this
        // preflight `SELECT` on `clusters` runs before every debug request and
        // blocks behind DDL exactly like any other read.
        Err(error) => Err(Box::new(contention_response(&error).unwrap_or_else(|| {
            tracing::error!(%error, actor_id = %actor.id, "failed to authorize sandbox debug read");
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }))),
    }
}

pub async fn get_sandbox_debug(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> Response {
    if let Err(response) = require_managing_org_admin(&state, &actor).await {
        return *response;
    }
    let Ok(sandbox_id) = parse_admin_sandbox_id(&spec) else {
        return api_error(StatusCode::BAD_REQUEST, "invalid_sandbox_id");
    };

    match fetch_postgres_debug_sections(&state, sandbox_id).await {
        Ok(Some(response)) => Json(response).into_response(),
        Ok(None) => api_error(StatusCode::NOT_FOUND, "sandbox_not_found"),
        // Only the lifecycle read is fatal here; the other sections degrade
        // into a per-section `DebugSection::error`. A lock timeout on that one
        // read is the whole response, so it takes the retryable 503.
        Err(error) => contention_response(&error).unwrap_or_else(|| {
            tracing::error!(%sandbox_id, ?error, "failed to load sandbox debug response");
            api_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }),
    }
}

async fn fetch_postgres_debug_sections(
    state: &AppState,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxDebugResponse>, sqlx::Error> {
    let Some(sandbox) = sources::lifecycle::fetch(&state.pool, sandbox_id).await? else {
        return Ok(None);
    };

    let observed = sources::observed::fetch(&state.pool, sandbox_id);
    let pods = sources::pods::fetch(&state.pool, sandbox_id);
    let snapshots = sources::snapshots::fetch(&state.pool, sandbox_id);
    let ports = sources::ports::fetch(&state.pool, sandbox_id);
    let timeline = sources::timeline::fetch(&state.pool, sandbox_id);
    let quarantine = sources::quarantine::fetch(&state.pool, sandbox_id);

    let (observed, pods, snapshots, ports, timeline, quarantine) =
        tokio::join!(observed, pods, snapshots, ports, timeline, quarantine);
    let observed = observed.map_err(source_error);
    let sections = sandbox.into_sections(observed.as_ref().ok().cloned().flatten());

    Ok(Some(SandboxDebugResponse {
        sandbox_id,
        fetched_at: OffsetDateTime::now_utc(),
        schema_version: SCHEMA_VERSION,
        lifecycle: DebugSection::data(sections.lifecycle),
        placement: DebugSection::data(sections.placement),
        resources: match observed {
            Ok(_) => DebugSection::data(sections.resources),
            Err(error) => DebugSection::error(error),
        },
        pods: pods
            .map(DebugSection::data)
            .unwrap_or_else(|error| DebugSection::error(source_error(error))),
        snapshots: snapshots
            .map(DebugSection::data)
            .unwrap_or_else(|error| DebugSection::error(source_error(error))),
        ports: ports
            .map(DebugSection::data)
            .unwrap_or_else(|error| DebugSection::error(source_error(error))),
        timeline: timeline
            .map(DebugSection::data)
            .unwrap_or_else(|error| DebugSection::error(source_error(error))),
        quarantine: quarantine
            .map(DebugSection::data)
            .unwrap_or_else(|error| DebugSection::error(source_error(error))),
    }))
}

#[cfg(test)]
mod tests;
