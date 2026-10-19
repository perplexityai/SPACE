//! Native authorization is applied to the whole mounted resource, before its
//! handler can read, mutate, mint a data-plane capability, or replay a response.
//! ECW's legacy-org bridge is intentionally not part of this contract.

use pplx_space_common_core::types::{OrgId, UserId};

use axum::{
    extract::{FromRequestParts, Path, Request, State},
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_common_core::types::{IdempotencyKey, SandboxOwnerSource};
use pplx_space_public_api::PublicSandboxId;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::error::PublicApiError;
use crate::{
    controllers::sandbox::{SandboxAccessScope, fetch_sandbox},
    federated_identity::{
        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, FEDERATED_SERVICE_GRANT_HEADER,
        FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER, FederatedOwner, FederatedServiceGrant,
        federated_service_grant_from_headers,
    },
    middleware::auth::{AuthFederatedOwner, AuthOrg, AuthUser},
    models::Sandbox,
    public_operations::PublicOperationStore,
    routes::AppState,
};

/// The sandbox row [`authorize`] loaded and admitted for this request.
///
/// Sandbox-scoped handlers reuse it through [`authorized_sandbox`] instead of
/// loading the same row again microseconds later. Only the authorization read
/// is shared: handlers still apply their own org check to it, and controller
/// paths that need authoritative routing or lifecycle state keep re-reading it.
#[derive(Debug, Clone)]
pub(crate) struct AuthorizedSandbox(Sandbox);

/// The handler-side owner check for a sandbox-scoped route.
///
/// Uses the row [`authorize`] already admitted when it is for the same sandbox;
/// otherwise (a handler reached without that layer) loads the row exactly as
/// the handlers did before. Either way the row must belong to `org_id`, and a
/// missing or foreign row is the same 404.
pub(super) async fn authorized_sandbox(
    authorized: Option<axum::Extension<AuthorizedSandbox>>,
    pool: &sqlx::PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<Sandbox, PublicApiError> {
    let sandbox = match authorized {
        Some(axum::Extension(AuthorizedSandbox(sandbox)))
            if PublicSandboxId::from(sandbox.id) == sandbox_id =>
        {
            sandbox
        }
        _ => fetch_sandbox(pool, sandbox_id.into())
            .await?
            .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?,
    };
    if sandbox.org_id != org_id {
        return Err(PublicApiError::not_found("sandbox not found"));
    }
    Ok(sandbox)
}

pub(super) async fn authorize(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    match authorize_request(&state, request).await {
        Ok(request) => next.run(request).await,
        Err(error) => error.into_response(),
    }
}

async fn authorize_request(state: &AppState, request: Request) -> Result<Request, PublicApiError> {
    let (mut parts, body) = request.into_parts();
    if parts
        .extensions
        .get::<axum::extract::MatchedPath>()
        .is_none()
    {
        return Err(PublicApiError::not_found("route not found"));
    }
    let org_id = parts
        .extensions
        .get::<AuthOrg>()
        .ok_or_else(PublicApiError::internal)?
        .0
        .id;
    let owner = parts
        .extensions
        .get::<AuthFederatedOwner>()
        .map(|owner| owner.0.clone());
    let subject_id = owner
        .as_ref()
        .map(|_| {
            parts
                .extensions
                .get::<AuthUser>()
                .map(|actor| actor.0.id)
                .ok_or_else(PublicApiError::internal)
        })
        .transpose()?;
    if owner.is_none()
        && parts.method == axum::http::Method::POST
        && parts
            .extensions
            .get::<axum::extract::MatchedPath>()
            .is_some_and(|path| path.as_str().ends_with("/sandboxes"))
        && !crate::auth::org_has_local_owner(&state.pool, org_id).await?
    {
        return Err(PublicApiError::forbidden(
            "federated sandbox creation requires an asserted owner and grant",
        ));
    }
    let Path(params) = Path::<HashMap<String, String>>::from_request_parts(&mut parts, state)
        .await
        .map_err(|_| PublicApiError::bad_request("invalid resource path"))?;
    if let Some(raw) = params.get("sandbox_id") {
        // Lifecycle actions share the {sandbox_id} mount with point reads.
        let raw = raw.split_once(':').map_or(raw.as_str(), |(id, _)| id);
        // A malformed id names no sandbox, so rejecting it as the handlers do
        // reveals nothing; only a well-formed id is withheld as not found.
        let sandbox_id = super::sandboxes::parse_public_sandbox_id(raw)?;
        let scope = match &owner {
            Some(owner) => SandboxAccessScope::FederatedSubject {
                owner: owner.clone(),
                // The scope's legacy bridge is filtered out below, even if a
                // local row were ever attached to this materialized org.
                legacy_org_id: org_id,
            },
            None => SandboxAccessScope::LocalOrg { org_id },
        };
        let sandbox = state
            .sandbox_controller
            .scoped_sandbox(&state.pool, &scope, sandbox_id.into())
            .await
            .map_err(super::sandboxes::controller_error)?;
        let Some(sandbox) = sandbox else {
            let matched = parts
                .extensions
                .get::<axum::extract::MatchedPath>()
                .ok_or_else(PublicApiError::internal)?
                .as_str();
            let relative = matched
                .strip_prefix(crate::routes::PUBLIC_API_SHARED_ROOT)
                .or_else(|| matched.strip_prefix("/v1"));
            if parts.method == axum::http::Method::DELETE
                && relative
                    == Some(pplx_space_public_api::routes::DELETE_SANDBOX.relative_runtime_path())
            {
                // Deleted targets have no live owner row. Only an exact replay
                // of an operation in the caller's owner scope may pass; the
                // handler rechecks idempotency under its admission lock.
                if let Some(subject_id) = subject_id {
                    namespace_idempotency_key(&mut parts.headers, subject_id)?;
                }
                if super::sandboxes::delete_replay_in_owner_scope(
                    &state.pool,
                    org_id,
                    subject_id,
                    &parts.headers,
                    sandbox_id,
                )
                .await?
                {
                    return Ok(Request::from_parts(parts, body));
                }
            }
            return Err(PublicApiError::not_found("sandbox not found"));
        };
        if owner.is_some() && sandbox.owner_source != SandboxOwnerSource::FederatedIdentity {
            return Err(PublicApiError::not_found("sandbox not found"));
        }
        if let Some(owner) = &owner
            && let Some(grant) = optional_grant(&parts.headers)?
        {
            // This precedes wake/restore enqueue and command/file access that
            // may wake a suspended sandbox. Never renew another subject's grant.
            state
                .sandbox_controller
                .upsert_federated_service_grant(&state.pool, sandbox.id, owner, &grant)
                .await
                .map_err(super::sandboxes::controller_error)?;
        }
        // The grant lives in its own table, so the row is unchanged by the upsert.
        parts.extensions.insert(AuthorizedSandbox(sandbox));
    } else if let Some(raw) = params.get("operation_id") {
        // As for sandboxes: malformed is the handler's 400, unseen is 404.
        let operation_id = super::operations::parse_operation_id(raw)?;
        if !PublicOperationStore::new(state.pool.clone())
            .operation_in_owner_scope(org_id, subject_id, operation_id)
            .await?
        {
            return Err(PublicApiError::not_found("operation not found"));
        }
    } else if owner.is_some() {
        let matched = parts
            .extensions
            .get::<axum::extract::MatchedPath>()
            .ok_or_else(PublicApiError::internal)?
            .as_str();
        let relative = matched
            .strip_prefix(crate::routes::PUBLIC_API_SHARED_ROOT)
            .or_else(|| matched.strip_prefix("/v1"))
            .ok_or_else(PublicApiError::internal)?;
        // Do not implicitly grant org-wide credential/admin surfaces to an
        // asserted subject. All sandbox-scoped routes were checked above.
        if !matches!(
            relative,
            "/sandboxes" | "/templates" | "/templates/{template_name}"
        ) {
            return Err(PublicApiError::forbidden(
                "operation does not support an asserted owner",
            ));
        }
    }
    if let Some(subject_id) = subject_id {
        namespace_idempotency_key(&mut parts.headers, subject_id)?;
    }
    Ok(Request::from_parts(parts, body))
}

pub(super) fn required_grant(
    headers: &HeaderMap,
    owner: Option<&FederatedOwner>,
) -> Result<Option<FederatedServiceGrant>, PublicApiError> {
    owner
        .map(|_| {
            federated_service_grant_from_headers(headers)
                .map_err(|_| PublicApiError::forbidden("invalid federated service grant"))
        })
        .transpose()
}

fn optional_grant(headers: &HeaderMap) -> Result<Option<FederatedServiceGrant>, PublicApiError> {
    if [
        FEDERATED_SERVICE_GRANT_HEADER,
        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
        FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
    ]
    .iter()
    .any(|name| headers.contains_key(*name))
    {
        federated_service_grant_from_headers(headers)
            .map(Some)
            .map_err(|_| PublicApiError::forbidden("invalid federated service grant"))
    } else {
        Ok(None)
    }
}

fn namespace_idempotency_key(
    headers: &mut HeaderMap,
    subject_id: UserId,
) -> Result<(), PublicApiError> {
    let Some(key) = headers.get("idempotency-key") else {
        return Ok(());
    };
    let key = key
        .to_str()
        .map_err(|_| PublicApiError::bad_request("invalid Idempotency-Key"))?;
    IdempotencyKey::parse(key)
        .map_err(|_| PublicApiError::bad_request("invalid Idempotency-Key"))?;
    // The existing store namespace is org + route + key. Add the durable
    // materialized subject without a schema change; never hash mutable flags.
    let mut hash = Sha256::new();
    hash.update(subject_id.as_uuid().as_bytes());
    hash.update(key.as_bytes());
    let value = format!("native-owner-{}", hex::encode(hash.finalize()));
    headers.insert(
        "idempotency-key",
        HeaderValue::from_str(&value).map_err(|_| PublicApiError::internal())?,
    );
    Ok(())
}

#[cfg(test)]
mod tests;
