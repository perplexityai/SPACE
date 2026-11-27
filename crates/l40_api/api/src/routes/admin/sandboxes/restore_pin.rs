//! The operator's way out of a refused restore. An unpinned sandbox whose
//! captures can't be ordered refuses to restore off its node; an operator
//! who knows which capture holds its latest state reads the candidates and
//! pins one (`crates/l50_app/admin/cli/README.md`, "Restore refused:
//! captures can't be ordered").

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::restore_pin::{self, RestorePinState};
use pplx_space_snapshot_model::LocalArtifactId;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;
use time::OffsetDateTime;

use super::guest_access::{resolve_sandbox, with_target};
use crate::controllers::sandbox::SandboxController;
use crate::controllers::substrate::NodeReportedArtifact;
use crate::routes::AppState;
use crate::routes::admin::audit::AdminAuditTarget;
use crate::routes::public::error::{PublicApiError, database_error};

/// `GET /admin/api/v1/sandboxes/{spec}/restore-pin`.
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

/// `POST /admin/api/v1/sandboxes/{spec}/restore-pin`.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestorePinRequest {
    pub local_artifact_id: LocalArtifactId,
    /// The `lifecycle_changed_at` the read returned.
    #[serde(with = "time::serde::rfc3339")]
    pub lifecycle_changed_at: OffsetDateTime,
}

/// The most the pin waits for the sandbox's node to say what it holds, endpoint
/// resolution included. With [`NODE_GONE_CHECK_BUDGET`] after a node that
/// doesn't answer, a pin waits on the node at most 10 s.
const NODE_CHECK_BUDGET: Duration = Duration::from_secs(8);

/// The most the pin waits, after a node didn't answer, to learn whether that
/// node is gone.
const NODE_GONE_CHECK_BUDGET: Duration = Duration::from_secs(2);

/// Why a pin of `requested` would be ignored, given what the sandbox's node
/// reports holding (`None`: a node with no registration, a Direct node whose
/// registration is deleted, or a Kubernetes worker confirmed gone). A node
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
    if report.vm_alive {
        return Some("the sandbox's node is running it, not holding it suspended".to_owned());
    }
    if !report.reports_suspended {
        return Some("the sandbox's node holds it, but not suspended".to_owned());
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

/// Why the guarded pin would refuse `request`, read from `state` before the
/// node is asked, so a stale token, a sandbox that isn't suspended (or an
/// unowned `restoring` row awaiting a pre-claim pin), an existing pin or an
/// unlisted capture is a `409` without a node round trip.
/// The guarded write checks all of them again.
pub(crate) fn stale_pin_request(
    state: &RestorePinState,
    request: &RestorePinRequest,
) -> Option<&'static str> {
    if state.lifecycle_changed_at != Some(request.lifecycle_changed_at) {
        return Some("the sandbox changed lifecycle since the read");
    }
    if state.lifecycle != "suspended"
        && !(state.lifecycle == "restoring" && state.restoring_pinnable)
    {
        return Some("the sandbox isn't suspended or awaiting a pre-claim restore pin");
    }
    if state.pinned_artifact.is_some() {
        return Some("the sandbox is already pinned");
    }
    if !state
        .captures
        .iter()
        .any(|capture| capture.local_artifact_id == request.local_artifact_id)
    {
        return Some("the artifact isn't one of the listed captures");
    }
    None
}

/// What a pin answers once its guarded write lands: the state the guard was
/// checked against, now pinned to `artifact`. The guard required the
/// lifecycle token it read, and a pin doesn't change the lifecycle, so the
/// token stays valid; nor does it change which captures the history holds or
/// whether they're ordered. Reading the row again could return a later
/// lifecycle that another writer set after the pin.
pub(crate) fn pinned_response(
    mut state: RestorePinState,
    artifact: LocalArtifactId,
) -> RestorePinResponse {
    state.pinned_artifact = Some(artifact);
    RestorePinResponse::from(state)
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

/// Pin an unpinned sandbox, `suspended` or an unowned `restoring` row awaiting
/// a pre-claim pin, to one of the captures [`get_restore_pin`] lists. `409`
/// when the guard refuses: the sandbox changed lifecycle since the read, is
/// in neither of those states, is already pinned, the
/// artifact isn't a listed capture, the sandbox's node still holds another
/// capture locally ([`node_copy_conflict`]), its restore node changed since
/// the read, or, on Direct, a full row of the capture lacks the runtime
/// metadata its restore loads it with. `503` when that node can't be
/// asked, unless it's a Kubernetes worker confirmed gone
/// ([`SandboxController::node_confirmed_gone`]): that one holds no copy, and
/// a sandbox the drained-node terminator keeps for its durable captures has
/// no other node to ask. A missing Node doesn't prove its instance is gone:
/// one deleted while the instance still runs re-registers under the same
/// name and node id, and a restore there loads its local copy instead of the
/// pin. This route can't see the instance, so the operator confirms it's
/// terminated before pinning (the runbook in
/// `crates/l50_app/admin/cli/README.md`). A Direct node whose registration is
/// deleted isn't asked, because no restore loads from it.
pub async fn pin_restore_source(
    State(state): State<AppState>,
    Path(spec): Path<String>,
    Json(request): Json<RestorePinRequest>,
) -> Response {
    let (public_id, sandbox) = match resolve_sandbox(&state, &spec).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    let result = pin_checked(
        &state.pool,
        &state.sandbox_controller,
        sandbox.id,
        &request,
        NODE_CHECK_BUDGET,
    )
    .await
    .map(Json);
    with_target(AdminAuditTarget::sandbox(sandbox.org_id, public_id), result)
}

/// [`pin_restore_source`] for a resolved sandbox: read its restore state, ask
/// its node what it holds for at most `node_check_budget`, and pin. A node
/// that doesn't answer and is confirmed gone holds nothing.
pub(crate) async fn pin_checked(
    pool: &PgPool,
    controller: &SandboxController,
    sandbox_id: SandboxId,
    request: &RestorePinRequest,
    node_check_budget: Duration,
) -> Result<RestorePinResponse, PublicApiError> {
    // A node still holding the sandbox suspended restores its own copy:
    // pin only that one. Ask the node the restore resolves, which outlives
    // the placement pointers ([`RestorePinState::restore_node_id`]).
    let read = restore_pin::restore_pin_state(pool, sandbox_id)
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    if let Some(stale) = stale_pin_request(&read, request) {
        return Err(PublicApiError::conflict(format!("not pinned: {stale}")));
    }
    if let Some(node_id) = read.restore_node_id {
        let ask = controller.node_reported_artifact(node_id, sandbox_id);
        let report = match tokio::time::timeout(node_check_budget, ask).await {
            Ok(Ok(report)) => report,
            unanswered => {
                let why = match unanswered {
                    Ok(Err(error)) => {
                        format!("can't ask the sandbox's node what it holds: {error}")
                    }
                    _ => format!(
                        "the sandbox's node didn't say what it holds within {node_check_budget:?}"
                    ),
                };
                if !node_confirmed_gone(controller, node_id).await {
                    return Err(PublicApiError::service_unavailable(why));
                }
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    node_id = %node_id,
                    "the sandbox's node is confirmed gone; pinning as if it holds no copy"
                );
                None
            }
        };
        if let Some(conflict) = node_copy_conflict(report.as_ref(), request.local_artifact_id) {
            return Err(PublicApiError::conflict(format!("not pinned: {conflict}")));
        }
    }
    let pinned = restore_pin::pin_restore_source(
        pool,
        sandbox_id,
        request.local_artifact_id,
        request.lifecycle_changed_at,
        read.restore_node_id,
    )
    .await
    .map_err(database_error)?;
    if !pinned {
        return Err(PublicApiError::conflict(
            "not pinned: the sandbox changed lifecycle or restore node since the read, isn't \
             suspended or awaiting a pre-claim restore pin, is already pinned, or the \
             artifact isn't one of the listed captures",
        ));
    }
    tracing::info!(
        sandbox_id = %sandbox_id,
        local_artifact_id = %request.local_artifact_id,
        "operator pinned the sandbox's restore source"
    );
    Ok(pinned_response(read, request.local_artifact_id))
}

/// Whether the node that didn't answer is confirmed gone
/// ([`SandboxController::node_confirmed_gone`]), so it holds no copy a
/// restore could load. Anything short of that confirmation, including a
/// check that fails or outlasts [`NODE_GONE_CHECK_BUDGET`], is `false`: the
/// node may still hold one.
async fn node_confirmed_gone(controller: &SandboxController, node_id: NodeId) -> bool {
    let check = controller.node_confirmed_gone(node_id);
    match tokio::time::timeout(NODE_GONE_CHECK_BUDGET, check).await {
        Ok(Ok(gone)) => gone,
        Ok(Err(error)) => {
            tracing::warn!(node_id = %node_id, %error, "couldn't check whether the sandbox's node is gone");
            false
        }
        Err(_) => {
            tracing::warn!(node_id = %node_id, "checking whether the sandbox's node is gone timed out");
            false
        }
    }
}

#[cfg(all(test, feature = "kubernetes"))]
mod gone_node_tests;
#[cfg(test)]
mod tests;
