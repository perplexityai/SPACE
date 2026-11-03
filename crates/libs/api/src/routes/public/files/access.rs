//! What the native file access RPCs decide for a request. The handler runs
//! behind every layer the request's public route runs behind
//! ([`crate::routes::public::native_file_access_router`]), in place of the
//! route's own handler, and stops where that handler would send the sandbox
//! its call.

use axum::{
    Extension,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::{IdempotencyKey, NodeId, OrgId, SandboxId, UserId};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_native_files::{FileOperation, guest_key};
use pplx_space_public_api::RequestFingerprint;

use super::{
    AuthorizedSandbox, PublicApiError, bind_request_key, controller_error, ensure_sandbox_owner,
    idempotency_key, unprotected_mutation_error,
};
use crate::{
    controllers::substrate::ExecutionActivityKind,
    middleware::auth::{AuthOrg, AuthUser},
    models::Sandbox,
    routes::AppState,
};

/// What an RPC asks of the request it runs through the router.
#[derive(Clone, Debug)]
pub(crate) struct FileAccessCall {
    pub(crate) operation: FileOperation,
    pub(crate) step: FileAccessStep,
}

#[derive(Clone, Debug)]
pub(crate) enum FileAccessStep {
    /// Credentials and owner scope only.
    Authorize,
    /// Everything the route's handler does before its sandbox call. The
    /// fingerprint is the body's, for an operation whose key is bound.
    Prepare {
        fingerprint: Option<RequestFingerprint>,
    },
}

/// Who an admitted request acts for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileAccessAuthorization {
    pub(crate) org_id: OrgId,
    /// The effective owner, `None` for a service credential acting for none.
    pub(crate) owner: Option<UserId>,
}

/// Where a prepared request's sandbox call goes.
#[derive(Clone, Debug)]
pub(crate) struct FileAccessGrant {
    pub(crate) authorization: FileAccessAuthorization,
    pub(crate) sandbox_id: SandboxId,
    /// The node the sandbox's authoritative route identity names.
    pub(crate) node_id: Option<NodeId>,
    /// The URI of the runtime this request connected to.
    pub(crate) endpoint: String,
    pub(crate) space_api_route_token: Option<String>,
    /// The key the sandbox sees, for an operation it replays by key.
    pub(crate) guest_key: Option<IdempotencyKey>,
}

/// Set on the response for an admitted request. A refused request answers
/// with the error its public route would answer with instead.
#[derive(Clone, Debug)]
pub(crate) enum FileAccessDecision {
    Authorized(FileAccessAuthorization),
    Granted(FileAccessGrant),
}

pub(crate) async fn decide(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    actor: Option<Extension<AuthUser>>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    call: Option<Extension<FileAccessCall>>,
    headers: HeaderMap,
    Path(sandbox_id): Path<String>,
) -> Result<Response, PublicApiError> {
    // Only the RPCs build requests for this router, and every one says what
    // it asks.
    let Some(Extension(call)) = call else {
        return Err(PublicApiError::internal());
    };
    let sandbox = ensure_sandbox_owner(authorized, &state.pool, org.id, &sandbox_id).await?;
    let authorization = FileAccessAuthorization {
        org_id: org.id,
        owner: actor.map(|Extension(AuthUser(user))| user.id),
    };
    let decision = match call.step {
        FileAccessStep::Authorize => FileAccessDecision::Authorized(authorization),
        FileAccessStep::Prepare { fingerprint } => FileAccessDecision::Granted(
            prepare(
                &state,
                &sandbox,
                call.operation,
                &headers,
                fingerprint,
                authorization,
            )
            .await?,
        ),
    };
    let mut response = StatusCode::OK.into_response();
    response.extensions_mut().insert(decision);
    Ok(response)
}

/// The route handler's steps before its sandbox call, in its order: the key
/// is checked and bound before anything reaches for the runtime, so a
/// refused request neither charges an activation nor wakes the sandbox.
async fn prepare(
    state: &AppState,
    sandbox: &Sandbox,
    operation: FileOperation,
    headers: &HeaderMap,
    fingerprint: Option<RequestFingerprint>,
    authorization: FileAccessAuthorization,
) -> Result<FileAccessGrant, PublicApiError> {
    let key = operation
        .takes_idempotency_key()
        .then(|| idempotency_key(headers))
        .transpose()?;
    let mut guest = None;
    if operation.binds_idempotency_key() {
        let (Some(key), Some(fingerprint)) = (&key, &fingerprint) else {
            return Err(PublicApiError::internal());
        };
        bind_request_key(state, sandbox, operation.route(), key, fingerprint).await?;
        if operation.sends_guest_key() {
            guest = Some(guest_key(operation.route(), key, fingerprint)?);
        }
    }
    let grant = state
        .sandbox_controller
        .spaced_runtime_grant(
            &state.pool,
            sandbox,
            SandboxApiMetricIdentity::native(metric_name(operation)),
            Some(ExecutionActivityKind::Access),
        )
        .await
        .map_err(|error| {
            if operation.is_unprotected_mutation() {
                unprotected_mutation_error(error)
            } else {
                controller_error(error)
            }
        })?;
    Ok(FileAccessGrant {
        authorization,
        sandbox_id: grant.sandbox.id,
        node_id: grant.sandbox.active_node_id,
        endpoint: grant.client.endpoint().to_string(),
        space_api_route_token: grant
            .space_api_route_token
            .map(|token| token.secret().to_owned()),
        guest_key: guest,
    })
}

/// The name each route's handler reports its sandbox calls under.
fn metric_name(operation: FileOperation) -> &'static str {
    match operation {
        FileOperation::Stat => "files_stat",
        FileOperation::List => "files_list",
        FileOperation::Read => "files_read",
        FileOperation::Write => "files_write",
        FileOperation::WriteBatch => "files_write_batch",
        FileOperation::Mkdir => "files_mkdir",
        FileOperation::Remove => "files_remove",
        FileOperation::Rename => "files_rename",
        FileOperation::ChangesEnable => "files_changes_enable",
        FileOperation::ChangesDisable => "files_changes_disable",
        FileOperation::Modified => "files_modified",
    }
}
