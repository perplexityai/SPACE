use pplx_space_common_core::sql;
use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{FromRequest, Path, Query, Request, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use pplx_space_common_core::env::{
    DEFAULT_SANDBOX_ALLOW_PLACEMENT, SANDBOX_ALLOW_PLACEMENT_ENV, env_flag_var,
};
use pplx_space_common_core::types::{IdempotencyKey, NodePoolName, OrgId, SandboxId, UserId};
use pplx_space_public_api::{
    CreateCommandRequest, CreateSandboxRequest as PublicCreateSandboxRequest, OperationId,
    OperationKind, OperationTargetType, PublicCommand, PublicSandboxId, RequestFingerprint,
    SandboxLifecycleAction, SandboxOperationResponse,
    UpdateSandboxRequest as PublicUpdateSandboxRequest,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::audit::{self, AdminSandboxAuditEntry};
use crate::middleware::auth::AuthUser;
use crate::models::{Sandbox, SandboxResponse, User};
use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
    CreatePublicOperationResult, HttpOperationMethod, OperationPathTemplate, PublicOperationStore,
    PublicOperationStoreError,
};
use crate::public_sandbox_operations::{
    CreateSandboxOperationPayload, SandboxLifecycleOperationPayload, UpdateSandboxOperationPayload,
};
use crate::routes::AppState;
use crate::routes::admin::support::{
    self, ContentionScope, IDEMPOTENCY_KEY_HEADER, require_managing_org_admin,
};
use crate::routes::public::error::PublicApiError;
use crate::routes::public::sessions::{
    create_command_session, idempotency_key as session_idempotency_key,
};

const ADMIN_SANDBOX_ACTION_PREFIX: &str = "force-";
const DEFAULT_ADMIN_SANDBOX_LIMIT: u32 = 1000;
const MAX_ADMIN_SANDBOX_LIMIT: u32 = 5000;
/// Distinct from the public `/v1/sandboxes` template so admin and public
/// idempotency records never collide on the same key.
const ADMIN_CREATE_SANDBOX_PATH_TEMPLATE: &str = "/v1/sandboxes:admin-create";
/// Distinct from the public `/v1/sandboxes/{sandbox_id}` template so admin and
/// public idempotency records never collide on the same key.
const ADMIN_UPDATE_SANDBOX_PATH_TEMPLATE: &str = "/v1/sandboxes/{sandbox_id}:admin-update";
const ADMIN_UPDATE_ACTION_LABEL: &str = "admin-update";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdminSandboxTarget {
    public_id: PublicSandboxId,
    domain_id: SandboxId,
}

impl AdminSandboxTarget {
    fn from_public_id(public_id: PublicSandboxId) -> Self {
        Self {
            public_id,
            domain_id: public_id.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AdminSandboxListQuery {
    limit: Option<u32>,
    org_id: Option<OrgId>,
}

pub async fn list_sandboxes(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Query(query): Query<AdminSandboxListQuery>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_ADMIN_SANDBOX_LIMIT)
        .clamp(1, MAX_ADMIN_SANDBOX_LIMIT);
    let sandboxes = match fetch_admin_sandboxes(&state.pool, limit, query.org_id).await {
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
    match state
        .sandbox_controller
        .list_responses(&state.pool, &sandboxes, true)
        .await
    {
        Ok(response) => Json(response).into_response(),
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
        // Same shape on `get_sandbox` below.
        Err(error) => super::support::controller_read_error(&error, || {
            tracing::warn!(%error, "admin sandbox response projection failed");
        }),
    }
}

pub async fn get_sandbox(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let public_id = match PublicSandboxId::from_str(&spec) {
        Ok(value) => value,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "sandbox id must be an sbx_ prefixed UUID",
            )
            .into_response();
        }
    };
    let target = AdminSandboxTarget::from_public_id(public_id);
    let response = match fetch_admin_sandbox_response(&state.pool, &state, target).await {
        Ok(Some(response)) => response,
        Ok(None) => {
            return api_error(StatusCode::NOT_FOUND, "Sandbox not found").into_response();
        }
        // Not classified, for the reason spelled out on `list_sandboxes`'s
        // projection arm: this flattens a `ControllerError` rather than
        // honouring the status it carries, and the reads behind it do not
        // classify yet either.
        Err(error) => {
            return super::support::controller_read_error(&error, || {
                tracing::warn!(
                    sandbox_id = %target.public_id,
                    %error,
                    "admin sandbox lookup failed"
                );
            });
        }
    };
    Json(response).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCreateSandboxRequest {
    /// Organization the sandbox is created under. The sandbox gets the
    /// org's policy (BYOK volume-encryption stamp, warm-pool bypass, …)
    /// exactly as if an org member had created it.
    pub org_id: OrgId,
    /// User in that org recorded as the sandbox owner. Defaults to the
    /// org's oldest user when omitted.
    pub user_id: Option<UserId>,
    /// The sandbox spec — the same shape as the public
    /// `POST /v1/sandboxes` body.
    pub sandbox: PublicCreateSandboxRequest,
    /// Optional isolation node pool to pin the sandbox to at create time.
    /// Operator-only; honored only when `SPACE_ALLOW_SANDBOX_PLACEMENT` is on
    /// (a pin with the gate off is rejected). Absent on public/ECW/E2B.
    #[serde(default)]
    pub node_pool: Option<NodePoolName>,
}

#[derive(Debug)]
pub(super) struct AdminCreateJson(AdminCreateSandboxRequest);

impl<S> FromRequest<S> for AdminCreateJson
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<AdminCreateSandboxRequest>::from_request(request, state)
            .await
            .map(|Json(request)| Self(request))
            .map_err(|rejection: JsonRejection| {
                let status = if matches!(&rejection, JsonRejection::JsonDataError(_)) {
                    StatusCode::BAD_REQUEST
                } else {
                    rejection.status()
                };
                api_error(status, rejection.body_text())
            })
    }
}

/// Create a sandbox under an arbitrary org from the operator surface.
///
/// The console admin's own session drives the public create path only for
/// the admin's org; this handler lets a managing-org admin create under any
/// org (e.g. to exercise a BYOK org's encrypted-create path). It enqueues
/// the same `create_sandbox` public operation the public route does — the
/// operation's org is the target org and its actor is a user in that org,
/// which the create executor requires.
pub async fn create_sandbox(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    AdminCreateJson(request): AdminCreateJson,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let idempotency_key = match idempotency_key(&headers) {
        Ok(value) => value,
        Err(err) => return err.into_response(),
    };
    let org_id = request.org_id;
    match create_sandbox_for_org(
        &state.pool,
        request,
        &idempotency_key,
        state
            .sandbox_controller
            .substrate_capabilities()
            .supports_node_pool_placement,
    )
    .await
    {
        Ok(response) => {
            audit::record_sandbox(AdminSandboxAuditEntry {
                actor: &actor,
                action: "admin-create",
                sandbox_id: response.sandbox_id,
                org_id,
                operation_id: Some(response.operation_id),
                success: true,
                error: None,
            });
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Err(err) => {
            tracing::warn!(%org_id, error = %err.message, "admin sandbox create failed");
            err.into_response()
        }
    }
}

async fn create_sandbox_for_org(
    pool: &sqlx::PgPool,
    request: AdminCreateSandboxRequest,
    idempotency_key: &IdempotencyKey,
    substrate_supports_node_pool_placement: bool,
) -> Result<SandboxOperationResponse, AdminSandboxActionError> {
    let org_id = request.org_id;
    let internal = |error: &dyn std::fmt::Display| {
        tracing::warn!(org_id = %org_id.as_uuid(), %error, "admin sandbox create failed");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    };

    // Placement is off by default; a pin issued while the gate is off is
    // rejected outright (no persisted intent, no deferred replay).
    validate_admin_node_pool(
        request.node_pool.as_ref(),
        env_flag_var(SANDBOX_ALLOW_PLACEMENT_ENV, DEFAULT_SANDBOX_ALLOW_PLACEMENT),
        substrate_supports_node_pool_placement,
    )?;

    let org_exists: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)"
    ))
    .bind(org_id.as_uuid())
    .fetch_one(pool)
    .await
    .map_err(|error| create_precheck_failure(org_id, &error))?;
    if !org_exists {
        return Err(AdminSandboxActionError::new(
            StatusCode::NOT_FOUND,
            "Organization not found",
        ));
    }

    // The create executor resolves the operation's actor as a user of the
    // operation's org, so the owner must genuinely belong to the target org.
    let owner_id: Option<UserId> = match request.user_id {
        Some(user_id) => {
            sqlx::query_scalar(sql!("SELECT id FROM users WHERE id = $1 AND org_id = $2"))
                .bind(user_id)
                .bind(org_id.as_uuid())
                .fetch_optional(pool)
                .await
                .map_err(|error| create_precheck_failure(org_id, &error))?
        }
        None => sqlx::query_scalar(sql!(
            "SELECT id FROM users WHERE org_id = $1 ORDER BY created_at, id LIMIT 1"
        ))
        .bind(org_id.as_uuid())
        .fetch_optional(pool)
        .await
        .map_err(|error| create_precheck_failure(org_id, &error))?,
    };
    let Some(owner_id) = owner_id else {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            match request.user_id {
                Some(_) => "user_id does not belong to the organization",
                None => {
                    "the organization has no users to own the sandbox; \
                     create one or pass user_id"
                }
            },
        ));
    };

    let fingerprint = admin_create_fingerprint(
        org_id,
        owner_id,
        &request.sandbox,
        request.node_pool.as_ref(),
    )?;
    let sandbox_id = PublicSandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let payload_json = CreateSandboxOperationPayload {
        sandbox_id,
        request: request.sandbox,
        node_pool: request.node_pool,
        federated_create: None,
        pool_claim: None,
    }
    .into_json();

    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(owner_id),
            api_product: ApiProduct::Admin,
            kind: OperationKind::CreateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(ADMIN_CREATE_SANDBOX_PATH_TEMPLATE),
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: crate::public_operations::request_traceparent(),
        })
        .await
        .map_err(|error| {
            store_action_error(ContentionScope::Writing, &error).unwrap_or_else(|| internal(&error))
        })?;

    match result {
        CreatePublicOperationResult::Created(operation_id) => Ok(SandboxOperationResponse {
            operation_id,
            sandbox_id,
            operation: None,
        }),
        CreatePublicOperationResult::Existing(operation_id) => {
            sandbox_operation_response(&store, org_id, operation_id).await
        }
        CreatePublicOperationResult::IdempotencyConflict { operation_id, .. } => {
            Err(AdminSandboxActionError::idempotency_conflict(operation_id))
        }
        CreatePublicOperationResult::AdmissionDenied => Err(AdminSandboxActionError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "operation unexpectedly refused admission",
        )),
    }
}

fn validate_admin_node_pool(
    node_pool: Option<&NodePoolName>,
    placement_enabled: bool,
    substrate_supports_node_pool_placement: bool,
) -> Result<(), AdminSandboxActionError> {
    if node_pool.is_none() {
        return Ok(());
    }
    if !placement_enabled {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "sandbox node-pool placement is disabled",
        ));
    }
    if !substrate_supports_node_pool_placement {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "sandbox node-pool placement requires the Kubernetes substrate",
        ));
    }
    Ok(())
}

fn admin_create_fingerprint(
    org_id: OrgId,
    owner_id: UserId,
    sandbox: &PublicCreateSandboxRequest,
    node_pool: Option<&NodePoolName>,
) -> Result<RequestFingerprint, AdminSandboxActionError> {
    // Preserve the legacy fingerprint shape exactly when no pin is set: only add
    // the `node_pool` key when present, so pre-placement requests keep the same
    // hash across a deploy and idempotent retries don't conflict.
    let mut fingerprint = serde_json::json!({
        "org_id": org_id.as_uuid(),
        "user_id": owner_id,
        "sandbox": sandbox,
    });
    if let Some(node_pool) = node_pool
        && let Some(object) = fingerprint.as_object_mut()
    {
        object.insert(
            "node_pool".to_string(),
            serde_json::Value::from(node_pool.as_str()),
        );
    }
    let bytes = serde_json::to_vec(&fingerprint).map_err(|error| {
        tracing::error!(%error, "failed to serialize admin create request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(%error, "failed to build admin create request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

pub async fn dispatch_sandbox_action(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(spec): Path<String>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let (target, action) = match parse_spec(&spec) {
        Ok(value) => value,
        Err(err) => return err.into_response(),
    };
    let idempotency_key = match idempotency_key(&headers) {
        Ok(value) => value,
        Err(err) => {
            audit_failure(&actor, target, action, None, err.message.as_str());
            return err.into_response();
        }
    };
    let sandbox = match fetch_admin_sandbox(&state.pool, target).await {
        Ok(Some(sandbox)) => Some(sandbox),
        Ok(None) if action == SandboxLifecycleAction::Delete => None,
        Ok(None) => {
            let err = AdminSandboxActionError::new(StatusCode::NOT_FOUND, "Sandbox not found");
            audit_failure(&actor, target, action, None, err.message.as_str());
            return err.into_response();
        }
        Err(error) => {
            // `ReadOnly`: a plain `SELECT` on `sandboxes`, before this action
            // has written anything. Classified here so the three verbs on this
            // path do not answer one DDL event three different ways — the list
            // handler above already answers 503.
            let err = sandbox_lookup_error(&error);
            if err.status == StatusCode::INTERNAL_SERVER_ERROR {
                tracing::warn!(sandbox_id = %target.public_id, %error, "admin sandbox lookup failed");
            }
            audit_failure(&actor, target, action, None, err.message.as_str());
            return err.into_response();
        }
    };

    let operation_org_id = sandbox
        .as_ref()
        .map(|sandbox| sandbox.org_id)
        .unwrap_or_else(|| actor.org_id);
    let actor_user_id = actor.id;
    match enqueue_admin_lifecycle_operation(
        &state.pool,
        operation_org_id,
        actor_user_id,
        target,
        action,
        &idempotency_key,
    )
    .await
    {
        Ok(response) => {
            audit::record_sandbox(AdminSandboxAuditEntry {
                actor: &actor,
                action: admin_action_label(action),
                sandbox_id: target.public_id,
                org_id: operation_org_id,
                operation_id: Some(response.operation_id),
                success: true,
                error: None,
            });
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Err(err) => {
            audit::record_sandbox(AdminSandboxAuditEntry {
                actor: &actor,
                action: admin_action_label(action),
                sandbox_id: target.public_id,
                org_id: operation_org_id,
                operation_id: err.operation_id,
                success: false,
                error: Some(err.message.as_str()),
            });
            err.into_response()
        }
    }
}

/// Run a one-shot command in a sandbox from the operator surface.
///
/// The admin console lists sandboxes across every org, so the org-scoped public
/// command endpoint rejects every sandbox the operator does not personally own
/// with "sandbox not found". This handler resolves the sandbox without an org
/// filter and runs the command under the sandbox's owning org, mirroring
/// `dispatch_sandbox_action`. Managing-org admin is already enforced by the
/// admin auth middleware, and the same middleware records an admin audit event
/// for the request.
pub async fn create_command(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(spec): Path<String>,
    Json(request): Json<CreateCommandRequest>,
) -> Result<(StatusCode, Json<PublicCommand>), PublicApiError> {
    let public_id = PublicSandboxId::from_str(&spec)
        .map_err(|_| PublicApiError::bad_request("sandbox id must be an sbx_ prefixed UUID"))?;
    let target = AdminSandboxTarget::from_public_id(public_id);
    let sandbox = fetch_admin_sandbox(&state.pool, target)
        .await
        // `From<sqlx::Error> for PublicApiError` classifies contention (and
        // attaches `Retry-After`), so this lookup answers the same 503 the list
        // handler does rather than a flat 500. It logs both cases itself — a
        // lost race at `warn` under "public api statement lost a lock race",
        // a real failure at `error` under "public api database error" — so
        // there is no hand-rolled log here. The `sandbox_id` is recoverable
        // from the `http.server` span's `url.path`, which carries it.
        .map_err(PublicApiError::from)?
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    let idempotency_key = session_idempotency_key(&headers)?;
    let command = create_command_session(
        &state.public_session_store(),
        sandbox.org_id,
        actor.id,
        public_id,
        request,
        idempotency_key,
        || Ok(()),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(command)))
}

/// Update a sandbox (network policy, auto-lifecycle, metadata) from the
/// operator surface.
///
/// The admin console lists sandboxes across every org, so the org-scoped
/// public `PATCH /v1/sandboxes/{id}` rejects every sandbox the operator does
/// not personally own with "sandbox not found". This handler resolves the
/// sandbox without an org filter and enqueues the update under the sandbox's
/// owning org, mirroring `dispatch_sandbox_action`.
pub async fn update_sandbox(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    headers: HeaderMap,
    Path(spec): Path<String>,
    Json(request): Json<PublicUpdateSandboxRequest>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let public_id = match PublicSandboxId::from_str(&spec) {
        Ok(value) => value,
        Err(_) => {
            return AdminSandboxActionError::new(
                StatusCode::BAD_REQUEST,
                "sandbox id must be an sbx_ prefixed UUID",
            )
            .into_response();
        }
    };
    let target = AdminSandboxTarget::from_public_id(public_id);
    if request
        .resources
        .as_ref()
        .and_then(|resources| resources.disk_mib)
        .is_some()
    {
        return AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "disk_mib is not mutable in place; recreate or restore the sandbox with the new disk size",
        )
        .into_response();
    }
    let idempotency_key = match idempotency_key(&headers) {
        Ok(value) => value,
        Err(err) => {
            audit_update_failure(&actor, target, None, err.message.as_str());
            return err.into_response();
        }
    };
    let sandbox = match fetch_admin_sandbox(&state.pool, target).await {
        Ok(Some(sandbox)) => sandbox,
        Ok(None) => {
            let err = AdminSandboxActionError::new(StatusCode::NOT_FOUND, "Sandbox not found");
            audit_update_failure(&actor, target, None, err.message.as_str());
            return err.into_response();
        }
        Err(error) => {
            // `ReadOnly`, for the reason on `dispatch_sandbox_action`'s lookup.
            let err = sandbox_lookup_error(&error);
            if err.status == StatusCode::INTERNAL_SERVER_ERROR {
                tracing::warn!(sandbox_id = %target.public_id, %error, "admin sandbox lookup failed");
            }
            audit_update_failure(&actor, target, None, err.message.as_str());
            return err.into_response();
        }
    };

    let operation_org_id = sandbox.org_id;
    let actor_user_id = actor.id;
    match enqueue_admin_update_operation(
        &state.pool,
        operation_org_id,
        actor_user_id,
        target,
        request,
        &idempotency_key,
    )
    .await
    {
        Ok(response) => {
            audit::record_sandbox(AdminSandboxAuditEntry {
                actor: &actor,
                action: ADMIN_UPDATE_ACTION_LABEL,
                sandbox_id: target.public_id,
                org_id: operation_org_id,
                operation_id: Some(response.operation_id),
                success: true,
                error: None,
            });
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Err(err) => {
            audit::record_sandbox(AdminSandboxAuditEntry {
                actor: &actor,
                action: ADMIN_UPDATE_ACTION_LABEL,
                sandbox_id: target.public_id,
                org_id: operation_org_id,
                operation_id: err.operation_id,
                success: false,
                error: Some(err.message.as_str()),
            });
            err.into_response()
        }
    }
}

async fn enqueue_admin_update_operation(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    actor_user_id: UserId,
    target: AdminSandboxTarget,
    request: PublicUpdateSandboxRequest,
    idempotency_key: &IdempotencyKey,
) -> Result<SandboxOperationResponse, AdminSandboxActionError> {
    let sandbox_id_string = target.public_id.to_string();
    let fingerprint = admin_update_fingerprint(target, &request)?;
    let payload_json = UpdateSandboxOperationPayload {
        sandbox_id: target.public_id,
        request,
    }
    .into_json();
    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor_user_id),
            api_product: ApiProduct::Admin,
            kind: OperationKind::UpdateSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Patch,
                path_template: OperationPathTemplate::new(ADMIN_UPDATE_SANDBOX_PATH_TEMPLATE),
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: crate::public_operations::request_traceparent(),
        })
        .await
        .map_err(|error| {
            store_action_error(ContentionScope::Writing, &error).unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = %target.public_id,
                    %error,
                    "failed to enqueue admin sandbox update operation"
                );
                AdminSandboxActionError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                )
            })
        })?;

    match result {
        CreatePublicOperationResult::Created(operation_id) => Ok(SandboxOperationResponse {
            operation_id,
            sandbox_id: target.public_id,
            operation: None,
        }),
        CreatePublicOperationResult::Existing(operation_id) => {
            sandbox_operation_response(&store, org_id, operation_id).await
        }
        CreatePublicOperationResult::IdempotencyConflict { operation_id, .. } => {
            Err(AdminSandboxActionError::idempotency_conflict(operation_id))
        }
        CreatePublicOperationResult::AdmissionDenied => Err(AdminSandboxActionError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "operation unexpectedly refused admission",
        )),
    }
}

fn admin_update_fingerprint(
    target: AdminSandboxTarget,
    request: &PublicUpdateSandboxRequest,
) -> Result<RequestFingerprint, AdminSandboxActionError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "sandbox_id": target.public_id.to_string(),
        "request": request,
        "admin": true,
    }))
    .map_err(|error| {
        tracing::error!(%error, "failed to serialize admin update request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(%error, "failed to build admin update request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

fn audit_update_failure(
    actor: &User,
    target: AdminSandboxTarget,
    operation_id: Option<OperationId>,
    error: &str,
) {
    audit::record_sandbox(AdminSandboxAuditEntry {
        actor,
        action: ADMIN_UPDATE_ACTION_LABEL,
        sandbox_id: target.public_id,
        org_id: actor.org_id,
        operation_id,
        success: false,
        error: Some(error),
    });
}

async fn enqueue_admin_lifecycle_operation(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    actor_user_id: UserId,
    target: AdminSandboxTarget,
    action: SandboxLifecycleAction,
    idempotency_key: &IdempotencyKey,
) -> Result<SandboxOperationResponse, AdminSandboxActionError> {
    let sandbox_id_string = target.public_id.to_string();
    let fingerprint = lifecycle_request_fingerprint(target, action)?;
    let payload_json = SandboxLifecycleOperationPayload {
        sandbox_id: target.public_id,
        action,
        rollback: None,
        wake_activity: None,
    }
    .into_json();
    let store = PublicOperationStore::new(pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor_user_id),
            api_product: ApiProduct::Admin,
            kind: action.operation_kind(),
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: admin_action_path_template(action),
                key: idempotency_key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: crate::public_operations::request_traceparent(),
        })
        .await
        .map_err(|error| {
            store_action_error(ContentionScope::Writing, &error).unwrap_or_else(|| {
                tracing::warn!(
                    sandbox_id = %target.public_id,
                    action = action.as_str(),
                    %error,
                    "failed to enqueue admin sandbox lifecycle operation"
                );
                AdminSandboxActionError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                )
            })
        })?;

    match result {
        CreatePublicOperationResult::Created(operation_id) => Ok(SandboxOperationResponse {
            operation_id,
            sandbox_id: target.public_id,
            operation: None,
        }),
        CreatePublicOperationResult::Existing(operation_id) => {
            sandbox_operation_response(&store, org_id, operation_id).await
        }
        CreatePublicOperationResult::IdempotencyConflict { operation_id, .. } => {
            Err(AdminSandboxActionError::idempotency_conflict(operation_id))
        }
        CreatePublicOperationResult::AdmissionDenied => Err(AdminSandboxActionError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "operation unexpectedly refused admission",
        )),
    }
}

async fn sandbox_operation_response(
    store: &PublicOperationStore,
    org_id: OrgId,
    operation_id: OperationId,
) -> Result<SandboxOperationResponse, AdminSandboxActionError> {
    let operation = store
        .fetch_operation(org_id, operation_id)
        .await
        .map_err(|error| {
            store_action_error(ContentionScope::ReadOnly, &error).unwrap_or_else(|| {
                tracing::warn!(%operation_id, %error, "failed to fetch idempotent admin operation");
                AdminSandboxActionError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                )
            })
        })?
        .ok_or_else(|| {
            AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        })?;
    let target_id = operation.target_id.as_deref().ok_or_else(|| {
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    let sandbox_id = PublicSandboxId::from_str(target_id).map_err(|_| {
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    Ok(SandboxOperationResponse {
        operation_id,
        sandbox_id,
        operation: None,
    })
}

async fn fetch_admin_sandbox(
    pool: &sqlx::PgPool,
    target: AdminSandboxTarget,
) -> Result<Option<Sandbox>, sqlx::Error> {
    crate::controllers::sandbox::fetch_sandbox(pool, target.domain_id).await
}

async fn fetch_admin_sandboxes(
    pool: &sqlx::PgPool,
    limit: u32,
    org_id: Option<OrgId>,
) -> Result<Vec<Sandbox>, sqlx::Error> {
    let sql = crate::controllers::sandbox::sandbox_query(
        "SELECT {sandbox_columns}
         FROM {sandbox_from}
         WHERE ($2::uuid IS NULL OR s.org_id = $2)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT $1",
    );
    sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(sql.as_str())))
        .bind(i64::from(limit))
        .bind(org_id.map(OrgId::as_uuid))
        .fetch_all(pool)
        .await
}

async fn fetch_admin_sandbox_response(
    pool: &sqlx::PgPool,
    state: &AppState,
    target: AdminSandboxTarget,
) -> Result<Option<SandboxResponse>, crate::controllers::ControllerError> {
    if let Some(sandbox) = state
        .sandbox_controller
        .sandbox(pool, target.domain_id)
        .await?
    {
        return state
            .sandbox_controller
            .tracked_response(pool, sandbox)
            .await
            .map(Some);
    }
    state
        .sandbox_controller
        .untracked_response(pool, target.domain_id)
        .await
}

fn parse_spec(
    spec: &str,
) -> Result<(AdminSandboxTarget, SandboxLifecycleAction), AdminSandboxActionError> {
    let Some((sandbox_id, action)) = spec.rsplit_once(':') else {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "sandbox action path must use /v1/sandboxes/{sandbox_id}:force-action on the admin host",
        ));
    };
    if sandbox_id.is_empty() || action.is_empty() {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "invalid sandbox action path",
        ));
    }
    let sandbox_id = PublicSandboxId::from_str(sandbox_id).map_err(|_| {
        AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "sandbox id must be an sbx_ prefixed UUID",
        )
    })?;
    let Some(action) = action.strip_prefix(ADMIN_SANDBOX_ACTION_PREFIX) else {
        return Err(AdminSandboxActionError::new(
            StatusCode::NOT_FOUND,
            "admin sandbox actions must use the force- prefix",
        ));
    };
    let action = SandboxLifecycleAction::parse_known(action).map_err(|_| {
        AdminSandboxActionError::new(StatusCode::NOT_FOUND, "unknown admin sandbox action")
    })?;
    Ok((AdminSandboxTarget::from_public_id(sandbox_id), action))
}

fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, AdminSandboxActionError> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY_HEADER) else {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header is required",
        ));
    };
    IdempotencyKey::parse_header(value.as_bytes())
        .map_err(|error| AdminSandboxActionError::new(StatusCode::BAD_REQUEST, error.to_string()))
}

fn lifecycle_request_fingerprint(
    target: AdminSandboxTarget,
    action: SandboxLifecycleAction,
) -> Result<RequestFingerprint, AdminSandboxActionError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "sandbox_id": target.public_id.to_string(),
        "action": action.as_str(),
        "admin": true,
    }))
    .map_err(|error| {
        tracing::error!(%error, "failed to serialize admin lifecycle request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    let digest = Sha256::digest(bytes);
    RequestFingerprint::parse(hex::encode(digest)).map_err(|error| {
        tracing::error!(%error, "failed to build admin request fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

fn admin_action_label(action: SandboxLifecycleAction) -> &'static str {
    match action {
        SandboxLifecycleAction::Start => "force-start",
        SandboxLifecycleAction::Stop => "force-stop",
        SandboxLifecycleAction::Pause => "force-pause",
        SandboxLifecycleAction::Resume => "force-resume",
        SandboxLifecycleAction::Suspend => "force-suspend",
        SandboxLifecycleAction::Restore => "force-restore",
        SandboxLifecycleAction::Wake => "force-wake",
        SandboxLifecycleAction::Kill => "force-kill",
        SandboxLifecycleAction::Checkpoint => "force-checkpoint",
        SandboxLifecycleAction::EnableSnapshots => "force-enable_snapshots",
        SandboxLifecycleAction::DisableSnapshots => "force-disable_snapshots",
        SandboxLifecycleAction::Rollback => "force-rollback",
        SandboxLifecycleAction::Delete => "force-delete",
    }
}

fn admin_action_path_template(action: SandboxLifecycleAction) -> OperationPathTemplate {
    OperationPathTemplate::new(match action {
        SandboxLifecycleAction::Start => "/v1/sandboxes/{sandbox_id}:force-start",
        SandboxLifecycleAction::Stop => "/v1/sandboxes/{sandbox_id}:force-stop",
        SandboxLifecycleAction::Pause => "/v1/sandboxes/{sandbox_id}:force-pause",
        SandboxLifecycleAction::Resume => "/v1/sandboxes/{sandbox_id}:force-resume",
        SandboxLifecycleAction::Suspend => "/v1/sandboxes/{sandbox_id}:force-suspend",
        SandboxLifecycleAction::Restore => "/v1/sandboxes/{sandbox_id}:force-restore",
        SandboxLifecycleAction::Wake => "/v1/sandboxes/{sandbox_id}:force-wake",
        SandboxLifecycleAction::Kill => "/v1/sandboxes/{sandbox_id}:force-kill",
        SandboxLifecycleAction::Checkpoint => "/v1/sandboxes/{sandbox_id}:force-checkpoint",
        SandboxLifecycleAction::EnableSnapshots => {
            "/v1/sandboxes/{sandbox_id}:force-enable_snapshots"
        }
        SandboxLifecycleAction::DisableSnapshots => {
            "/v1/sandboxes/{sandbox_id}:force-disable_snapshots"
        }
        SandboxLifecycleAction::Rollback => "/v1/sandboxes/{sandbox_id}:force-rollback",
        SandboxLifecycleAction::Delete => "/v1/sandboxes/{sandbox_id}:force-delete",
    })
}

fn audit_failure(
    actor: &User,
    target: AdminSandboxTarget,
    action: SandboxLifecycleAction,
    operation_id: Option<OperationId>,
    error: &str,
) {
    audit::record_sandbox(AdminSandboxAuditEntry {
        actor,
        action: admin_action_label(action),
        sandbox_id: target.public_id,
        org_id: actor.org_id,
        operation_id,
        success: false,
        error: Some(error),
    });
}

/// The action error for a failed `organizations`/`users` precheck on
/// `create_sandbox_for_org`.
///
/// `ReadOnly`: all three are plain reads that run before the create writes
/// anything, so every contention SQLSTATE answers a retryable status and none
/// takes the write-specific 409.
///
/// A free function rather than the `&dyn Display` closure those call sites
/// used, because the closure was not testable and this is. The `internal`
/// closure still exists and still maps the `create_operation` fallback below —
/// only these three reads moved off it.
///
/// Why they were missed: that erasure drops the `sqlx::Error`'s type at the
/// map site, which defeats a sweep for the error mapper's shape *and* one that
/// enumerates functions returning a database error and audits their callers.
/// There is no such function here — the query is inline and its type is gone
/// one expression later. Sibling mappers that erase the error are safe only
/// when their contention checks run first; moving erasure ahead of those
/// checks silently turns retryable failures into 500s. Preserve the typed
/// error until after classification.
fn create_precheck_failure(org_id: OrgId, error: &sqlx::Error) -> AdminSandboxActionError {
    match support::contention_disposition(ContentionScope::ReadOnly, error) {
        Some((status, message)) => AdminSandboxActionError::retryable(status, message),
        None => {
            tracing::warn!(org_id = %org_id.as_uuid(), %error, "admin sandbox create failed");
            AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }
    }
}

/// The action error for a failed `sandboxes` lookup.
///
/// `fetch_admin_sandbox` is a plain read that runs before the action writes
/// anything, so `ReadOnly`: every contention SQLSTATE answers a retryable
/// status and none takes the write-specific 409.
fn sandbox_lookup_error(error: &sqlx::Error) -> AdminSandboxActionError {
    match support::contention_disposition(ContentionScope::ReadOnly, error) {
        Some((status, message)) => AdminSandboxActionError::retryable(status, message),
        None => {
            AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }
    }
}

/// The action error for a [`PublicOperationStore`] failure, with a lost lock
/// race classified.
///
/// `public_operations` is written on every admin create, update, and lifecycle
/// action, and read back on the idempotent-replay path, so all four arms can
/// lose a lock race and all four answered a flat 500. `scope` is
/// [`ContentionScope::Writing`] for the `create_operation` calls — the
/// operation is keyed on the caller's `Idempotency-Key`, so a replay of the
/// same request adopts the existing operation rather than opening a second one
/// — and [`ContentionScope::ReadOnly`] for the replay read, which wrote
/// nothing and so takes the retryable 503 rather than the 409.
fn store_action_error(
    scope: ContentionScope,
    error: &PublicOperationStoreError,
) -> Option<AdminSandboxActionError> {
    let (status, message) = support::store_contention_disposition(scope, error)?;
    Some(AdminSandboxActionError::retryable(status, message))
}

#[derive(Debug)]
struct AdminSandboxActionError {
    status: StatusCode,
    message: String,
    operation_id: Option<OperationId>,
    /// Whether the caller should send the identical request again.
    ///
    /// Carried in the body because the status alone cannot say it here: a
    /// contention 409 and the permanent `IdempotencyConflict` 409 are the only
    /// two 409s these handlers return, so a client branching on status cannot
    /// tell "your key was reused with a different request" — which no retry
    /// fixes — from "you lost a lock race", which the identical retry
    /// resolves. `templates` closes the same gap by forcing the flag on in
    /// `contention_envelope`.
    ///
    /// Derived from the status by [`Self::new`], on the same rule
    /// `templates::api_error` uses — 429/503/500 are retryable, everything
    /// else is not — so a plain 500 from a read that wrote nothing keeps the
    /// repo's polarity instead of inverting it. Exactly two cases override the
    /// derivation, both because the status cannot express them: the contention
    /// 409 via [`Self::retryable`], and the reused idempotency key via
    /// [`Self::idempotency_conflict`]. Every other construction goes through
    /// [`Self::new`], so one condition cannot answer differently on two
    /// routes.
    retryable: bool,
}

impl AdminSandboxActionError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            operation_id: None,
            retryable: matches!(
                status,
                StatusCode::TOO_MANY_REQUESTS
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::INTERNAL_SERVER_ERROR
            ),
        }
    }

    /// [`Self::new`] for an error the identical request can resolve, where the
    /// status alone would say otherwise.
    fn retryable(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            ..Self::new(status, message)
        }
    }

    /// The reused-idempotency-key 409, which no retry resolves.
    ///
    /// A constructor rather than three struct literals so the flag's value
    /// lives in one place that a test can drive — the literals it replaced
    /// could be flipped without failing anything.
    fn idempotency_conflict(operation_id: Option<OperationId>) -> Self {
        Self {
            operation_id,
            retryable: false,
            ..Self::new(
                StatusCode::CONFLICT,
                "idempotency key was already used with a different request",
            )
        }
    }
}

impl IntoResponse for AdminSandboxActionError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(serde_json::json!({
                "error": self.message,
                "operation_id": self.operation_id.map(|id| id.to_string()),
                "retryable": self.retryable,
            })),
        )
            .into_response()
    }
}

fn api_error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": message.into()})))
}

#[cfg(test)]
mod tests;
