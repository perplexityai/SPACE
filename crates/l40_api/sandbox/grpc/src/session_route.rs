use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_common_grpc::{SANDBOX_SERVICE_ROUTE_ERROR_METADATA, SANDBOX_STATE_METADATA};
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

const SESSION_ROUTE_AT_REST: &str = "session_route_at_rest";
const SANDBOX_ID_METADATA: &str = "space-sandbox-id";

/// Mark a FAILED_PRECONDITION for a session route the sandbox has no node
/// for as one nothing is about to give it: the sandbox is not heading to
/// running, no wake is queued, and none could be queued for it. The marker
/// names the sandbox and its state, so the answer can say how to wake it.
/// Absence keeps the old meaning, a route that is not ready yet, so a client
/// may wait and attach again.
pub fn mark_session_route_at_rest(
    mut status: Status,
    sandbox_id: SandboxId,
    state: SandboxLifecycle,
) -> Status {
    status.metadata_mut().insert(
        SANDBOX_SERVICE_ROUTE_ERROR_METADATA,
        MetadataValue::from_static(SESSION_ROUTE_AT_REST),
    );
    status.metadata_mut().insert(
        SANDBOX_STATE_METADATA,
        MetadataValue::from_static(state.as_str()),
    );
    if let Ok(sandbox_id) = MetadataValue::try_from(sandbox_id.to_string()) {
        status
            .metadata_mut()
            .insert(SANDBOX_ID_METADATA, sandbox_id);
    }
    status
}

/// What [`mark_session_route_at_rest`] recorded, each part `None` when it did
/// not survive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionRouteAtRest {
    pub sandbox_id: Option<SandboxId>,
    pub state: Option<SandboxLifecycle>,
}

/// The at-rest mark on `status`, or `None` when it has none.
pub fn session_route_at_rest(status: &Status) -> Option<SessionRouteAtRest> {
    let metadata = status.metadata();
    let marked = status.code() == Code::FailedPrecondition
        && metadata
            .get(SANDBOX_SERVICE_ROUTE_ERROR_METADATA)
            .and_then(|value| value.to_str().ok())
            == Some(SESSION_ROUTE_AT_REST);
    marked.then(|| SessionRouteAtRest {
        sandbox_id: metadata
            .get(SANDBOX_ID_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
        state: metadata
            .get(SANDBOX_STATE_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
    })
}

#[cfg(test)]
mod tests;
