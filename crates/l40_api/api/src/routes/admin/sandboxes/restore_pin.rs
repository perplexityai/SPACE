//! The operator's way out of a refused restore. An unpinned sandbox whose
//! captures can't be ordered refuses to restore off its node; an operator
//! who knows which capture holds its latest state reads the candidates and
//! pins one (`docs/runbooks/restore-source-unordered.md`).

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_sandbox_store::restore_pin::{self, RestorePinState};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::guest_access::{resolve_sandbox, with_target};
use crate::controllers::substrate::NodeReportedArtifact;
use crate::routes::AppState;
use crate::routes::admin::audit::AdminAuditTarget;
use crate::routes::public::error::{PublicApiError, database_error};

/// `GET /admin/v1/sandboxes/{spec}/restore-pin`.
#[derive(Debug, Serialize)]
pub struct RestorePinResponse {
    pub lifecycle: String,
    /// Pass back unchanged to pin: a sandbox that changed lifecycle since may
    /// hold a later state.
    #[serde(with = "time::serde::rfc3339::option")]
    pub lifecycle_changed_at: Option<OffsetDateTime>,
    pub pinned_artifact_id: Option<LocalArtifactId>,
    /// The unpinned restore refuses: its captures can't be ordered.
    pub unordered: bool,
    /// The captures a pin accepts, latest first registration first.
    pub captures: Vec<RestorePinCapture>,
}

#[derive(Debug, Serialize)]
pub struct RestorePinCapture {
    pub local_artifact_id: LocalArtifactId,
    pub generation: Option<i64>,
    #[serde(with = "time::serde::rfc3339")]
    pub first_registered_at: OffsetDateTime,
    pub source_node_id: Option<NodeId>,
}

impl From<RestorePinState> for RestorePinResponse {
    fn from(state: RestorePinState) -> Self {
        Self {
            lifecycle: state.lifecycle,
            lifecycle_changed_at: state.lifecycle_changed_at,
            pinned_artifact_id: state.pinned_artifact,
            unordered: state.unordered,
            captures: state
                .captures
                .into_iter()
                .map(|capture| RestorePinCapture {
                    local_artifact_id: capture.local_artifact_id,
                    generation: capture.generation,
                    first_registered_at: capture.first_registered_at,
                    source_node_id: capture.source_node_id,
                })
                .collect(),
        }
    }
}

/// `POST /admin/v1/sandboxes/{spec}/restore-pin`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestorePinRequest {
    pub local_artifact_id: LocalArtifactId,
    /// The `lifecycle_changed_at` the read returned.
    #[serde(with = "time::serde::rfc3339")]
    pub lifecycle_changed_at: OffsetDateTime,
}

/// The most the pin waits for the sandbox's node to say what it holds, endpoint
/// resolution included, so the request stays within an API handler's latency
/// bound.
const NODE_CHECK_BUDGET: std::time::Duration = std::time::Duration::from_secs(8);

/// Why a pin of `requested` would be ignored, given what the sandbox's node
/// reports holding (`None`: no such node). A node
/// that holds the sandbox suspended restores its own local copy whatever the
/// pin names, so a pin is admitted only for that copy, or when the node
/// holds none.
pub(crate) fn node_copy_conflict(
    report: Option<&NodeReportedArtifact>,
    requested: LocalArtifactId,
) -> Option<String> {
    let report = report.filter(|report| report.present)?;
    if report.state_degraded || report.pin_unreadable {
        return Some(
            "the sandbox's node holds a local copy it can't attribute to a capture".to_owned(),
        );
    }
    if report.vm_alive || !report.reports_suspended {
        return Some("the sandbox's node is running it, not holding it suspended".to_owned());
    }
    match report.pinned_artifact {
        Some(held) if held == requested => None,
        Some(held) => Some(format!(
            "the sandbox's node holds capture {held} locally, and a restore there loads it, \
             not the pin"
        )),
        None => Some(
            "the sandbox's node holds a local copy with no recorded capture, and a restore \
             there loads it, not the pin"
                .to_owned(),
        ),
    }
}

/// Read a sandbox's restore state and the captures an operator may pin.
pub async fn get_restore_pin(State(state): State<AppState>, Path(spec): Path<String>) -> Response {
    let (public_id, sandbox) = match resolve_sandbox(&state, &spec).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    let result = restore_pin::restore_pin_state(&state.pool, sandbox.id)
        .await
        .map_err(database_error)
        .and_then(|found| found.ok_or_else(|| PublicApiError::not_found("sandbox not found")))
        .map(|found| Json(RestorePinResponse::from(found)));
    with_target(AdminAuditTarget::sandbox(sandbox.org_id, public_id), result)
}

/// Pin a suspended, unpinned sandbox to one of the captures
/// [`get_restore_pin`] lists. `409` when the guard refuses: the sandbox
/// changed lifecycle since the read, isn't suspended, is already pinned, the
/// artifact isn't a listed capture, or the sandbox's node still holds another
/// capture locally ([`node_copy_conflict`]). `503` when that node can't be
/// asked.
pub async fn pin_restore_source(
    State(state): State<AppState>,
    Path(spec): Path<String>,
    Json(request): Json<RestorePinRequest>,
) -> Response {
    let (public_id, sandbox) = match resolve_sandbox(&state, &spec).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    let result = async {
        // A node still holding the sandbox suspended restores its own copy:
        // pin only that one.
        let placed = restore_pin::restore_pin_state(&state.pool, sandbox.id)
            .await
            .map_err(database_error)?
            .and_then(|found| found.placed_node_id);
        if let Some(node_id) = placed {
            let ask = state
                .sandbox_controller
                .node_reported_artifact(node_id, sandbox.id);
            let report = tokio::time::timeout(NODE_CHECK_BUDGET, ask)
                .await
                .map_err(|_| {
                    PublicApiError::service_unavailable(format!(
                        "the sandbox's node didn't say what it holds within {NODE_CHECK_BUDGET:?}"
                    ))
                })?
                .map_err(|error| {
                    PublicApiError::service_unavailable(format!(
                        "can't ask the sandbox's node what it holds: {error}"
                    ))
                })?;
            if let Some(conflict) = node_copy_conflict(report.as_ref(), request.local_artifact_id) {
                return Err(PublicApiError::conflict(format!("not pinned: {conflict}")));
            }
        }
        let pinned = restore_pin::pin_restore_source(
            &state.pool,
            sandbox.id,
            request.local_artifact_id,
            request.lifecycle_changed_at,
        )
        .await
        .map_err(database_error)?;
        if !pinned {
            return Err(PublicApiError::conflict(
                "not pinned: the sandbox changed lifecycle since the read, isn't suspended, is \
                 already pinned, or the artifact isn't one of the listed captures",
            ));
        }
        tracing::info!(
            sandbox_id = %sandbox.id,
            local_artifact_id = %request.local_artifact_id,
            "operator pinned the sandbox's restore source"
        );
        restore_pin::restore_pin_state(&state.pool, sandbox.id)
            .await
            .map_err(database_error)?
            .map(|found| Json(RestorePinResponse::from(found)))
            .ok_or_else(|| PublicApiError::not_found("sandbox not found"))
    }
    .await;
    with_target(AdminAuditTarget::sandbox(sandbox.org_id, public_id), result)
}

#[cfg(test)]
mod tests;
