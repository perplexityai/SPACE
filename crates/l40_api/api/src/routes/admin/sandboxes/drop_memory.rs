//! `POST /admin/api/v1/sandboxes/{sandbox_id}:drop-memory`.
//!
//! Drops a suspended sandbox's memory offline: the sandbox stays `suspended`,
//! repinned to a disk-only copy of its suspend snapshot. The route checks the
//! switch, the request and the sandbox's eligibility, then enqueues a
//! `drop_sandbox_memory` operation, which the cluster controller runs (see
//! [`crate::public_sandbox_memory_operations`]): only it holds the object
//! store the copy needs.
//!
//! A sandbox whose memory is already dropped isn't refused: the operation
//! finds it done and succeeds, so a retry with a new `Idempotency-Key` gets
//! the same answer as a replay.

use axum::{
    Extension, Json,
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use pplx_space_accounts::OrgId;
use pplx_space_guest_process::IdempotencyKey;
use pplx_space_public_api::{
    OperationId, OperationKind, OperationTargetType, RequestFingerprint, SandboxOperationResponse,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{
    AdminSandboxActionError, AdminSandboxTarget, fetch_admin_sandbox, idempotency_key,
    sandbox_lookup_error, sandbox_operation_response, store_action_error,
};
use crate::controllers::sandbox::memory_drop::{MemoryDrop, MemoryDropIneligible};
use crate::models::User;
use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
    CreatePublicOperationResult, HttpOperationMethod, OperationPathTemplate, PublicOperationStore,
};
use crate::public_sandbox_memory_operations::SandboxMemoryDropOperationPayload;
use crate::routes::AppState;
use crate::routes::admin::audit::{self, AdminAuditTarget, AdminSandboxAuditEntry};
use crate::routes::admin::support::ContentionScope;
use pplx_space_sandbox_store::memory_drop::MemoryDropRefusal;

/// The action suffix this route answers on `/sandboxes/{spec}`.
pub(super) const DROP_MEMORY_SUFFIX: &str = ":drop-memory";

/// The audit action, and the fingerprint's `action`.
const ACTION: &str = "drop-memory";

const PATH_TEMPLATE: &str = "/v1/sandboxes/{sandbox_id}:drop-memory";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DropMemoryRequest {
    reason: String,
}

/// Parse the body's required, non-blank `reason`, trimmed.
fn parse_reason(body: &[u8]) -> Result<String, AdminSandboxActionError> {
    let request: DropMemoryRequest = serde_json::from_slice(body).map_err(|error| {
        AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            format!("the body must be {{\"reason\": \"...\"}}: {error}"),
        )
    })?;
    let reason = request.reason.trim();
    if reason.is_empty() {
        return Err(AdminSandboxActionError::new(
            StatusCode::BAD_REQUEST,
            "reason must not be empty",
        ));
    }
    Ok(reason.to_string())
}

pub(super) async fn drop_memory(
    state: &AppState,
    actor: &User,
    headers: &HeaderMap,
    sandbox_spec: &str,
    body: &Bytes,
) -> axum::response::Response {
    let target = match AdminSandboxTarget::parse(sandbox_spec) {
        Ok(target) => target,
        Err(error) => {
            return AdminSandboxActionError::new(StatusCode::BAD_REQUEST, error.to_string())
                .into_response();
        }
    };
    // Parsed up front, but refused only after the switch, so the audit
    // records the reason whenever one was given.
    let reason = parse_reason(body);
    let audit_reason = reason.as_ref().ok().cloned();
    let audit_reason = audit_reason.as_deref();
    let sandbox = match fetch_admin_sandbox(&state.pool, target).await {
        Ok(Some(sandbox)) => sandbox,
        Ok(None) => {
            let err = AdminSandboxActionError::new(StatusCode::NOT_FOUND, "Sandbox not found");
            record(
                actor,
                target,
                actor.org_id,
                None,
                Err(&err.message),
                audit_reason,
            );
            return err.into_response();
        }
        Err(error) => {
            let err = sandbox_lookup_error(&error);
            if err.status == StatusCode::INTERNAL_SERVER_ERROR {
                tracing::warn!(sandbox_id = %target.public_id, %error, "admin sandbox lookup failed");
            }
            record(
                actor,
                target,
                actor.org_id,
                None,
                Err(&err.message),
                audit_reason,
            );
            return err.into_response();
        }
    };
    let org_id = sandbox.org_id;
    // `move` so the block takes `reason` by value; the references it also
    // captures are `Copy`.
    let result = async move {
        if !state.memory_drop_enabled {
            return Err(AdminSandboxActionError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "the memory drop is disabled",
            ));
        }
        let reason = reason?;
        let idempotency_key = idempotency_key(headers)?;
        match MemoryDrop::eligibility(&state.pool, target.domain_id)
            .await
            .map_err(|error| AdminSandboxActionError::new(error.status(), error.to_string()))?
        {
            None | Some(MemoryDropIneligible::Row(MemoryDropRefusal::AlreadyDropped)) => {}
            // Transient: a drop in flight holds the seal, or a fork of the
            // sandbox is running. A retry, including one with the same
            // `Idempotency-Key` as the drop in flight, succeeds once it ends.
            Some(
                ineligible @ (MemoryDropIneligible::Row(MemoryDropRefusal::Sealed)
                | MemoryDropIneligible::ForkActive),
            ) => {
                return Err(AdminSandboxActionError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "the sandbox's memory can't be dropped right now: {}; retry",
                        ineligible.reason()
                    ),
                ));
            }
            Some(ineligible) => {
                return Err(AdminSandboxActionError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "the sandbox's memory can't be dropped: {}",
                        ineligible.reason()
                    ),
                ));
            }
        }
        enqueue(state, org_id, actor, target, reason, &idempotency_key).await
    }
    .await;
    let response = match result {
        Ok(response) => {
            record(
                actor,
                target,
                org_id,
                Some(response.operation_id),
                Ok(()),
                audit_reason,
            );
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Err(err) => {
            record(
                actor,
                target,
                org_id,
                err.operation_id,
                Err(&err.message),
                audit_reason,
            );
            err.into_response()
        }
    };
    (
        Extension(AdminAuditTarget::sandbox(org_id, target.public_id)),
        response,
    )
        .into_response()
}

async fn enqueue(
    state: &AppState,
    org_id: OrgId,
    actor: &User,
    target: AdminSandboxTarget,
    reason: String,
    idempotency_key: &IdempotencyKey,
) -> Result<SandboxOperationResponse, AdminSandboxActionError> {
    let fingerprint = fingerprint(target, &reason)?;
    let sandbox_id_string = target.public_id.to_string();
    let store = PublicOperationStore::new(state.pool.clone());
    let result = store
        .create_operation(CreatePublicOperation {
            org_id,
            actor_user_id: Some(actor.id),
            api_product: ApiProduct::Admin,
            kind: OperationKind::DropSandboxMemory,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&sandbox_id_string),
            payload_json: SandboxMemoryDropOperationPayload {
                sandbox_id: target.public_id,
                reason,
            }
            .into_json(),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(PATH_TEMPLATE),
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
                    "failed to enqueue the memory drop"
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

/// The request's idempotency fingerprint: the same key with another reason
/// is a different request.
fn fingerprint(
    target: AdminSandboxTarget,
    reason: &str,
) -> Result<RequestFingerprint, AdminSandboxActionError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "sandbox_id": target.public_id.to_string(),
        "action": ACTION,
        "reason": reason,
        "admin": true,
    }))
    .map_err(|error| {
        tracing::error!(%error, "failed to serialize the memory drop fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })?;
    RequestFingerprint::parse(hex::encode(Sha256::digest(bytes))).map_err(|error| {
        tracing::error!(%error, "failed to build the memory drop fingerprint");
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    })
}

fn record(
    actor: &User,
    target: AdminSandboxTarget,
    org_id: OrgId,
    operation_id: Option<OperationId>,
    outcome: Result<(), &str>,
    reason: Option<&str>,
) {
    audit::record_sandbox(AdminSandboxAuditEntry {
        actor,
        action: ACTION,
        sandbox_id: target.public_id,
        org_id,
        operation_id,
        success: outcome.is_ok(),
        error: outcome.err(),
        reason,
    });
}

#[cfg(test)]
mod tests;
