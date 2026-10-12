//! Read-only public (sandbox-owner) network policy API:
//! `GET /v1/sandboxes/{sandbox_id}/network-policy`.
//!
//! `GET` returns the sandbox's effective desired V1 policy. Network-policy
//! mutations use sandbox create/update instead of a separate resource-specific
//! write contract.

use pplx_space_common_core::types::OrgId;

use pplx_space_common_core::sql;
use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use pplx_space_net_common::NetworkPolicy;
use pplx_space_public_api::PublicSandboxId;
use sqlx::PgPool;

use crate::middleware::auth::AuthOrg;
use crate::routes::AppState;

use super::error::PublicApiError;

pub async fn get_sandbox_network_policy(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(sandbox_id): Path<String>,
) -> Result<Response, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(&sandbox_id)?;
    ensure_sandbox_owner(&state.pool, org.id, sandbox_id).await?;
    document::get(&state, sandbox_id)
        .await?
        .ok_or_else(|| PublicApiError::not_found("sandbox has no network policy"))
}

/// Read the effective desired policy and return an opaque `ETag`.
mod document {
    use super::*;
    use sha2::{Digest, Sha256};

    struct DesiredPolicy {
        sandbox: crate::models::Sandbox,
        policy: NetworkPolicy,
        inherited: bool,
    }

    /// The sandbox's effective desired policy, or `None` when it has neither an
    /// override nor a template policy — i.e. no document to serve. Returning
    /// `None` (rather than fabricating a deny-all document) keeps this in step
    /// with the list/detail projection's `effective_desired_network_policy`,
    /// which is also null for that sandbox.
    async fn load_desired(
        state: &AppState,
        sandbox_id: PublicSandboxId,
    ) -> Result<Option<DesiredPolicy>, PublicApiError> {
        // `From<sqlx::Error>` rather than a bare `internal()`: this read takes
        // no lock, but it still waits on DDL holding `ACCESS EXCLUSIVE` on
        // `sandboxes`, so under the role-level `lock_timeout` it can abort with
        // `55P03` and the caller can just retry. The `From` impl classifies
        // that and logs the rest. This `GET` is the one survivor of the
        // NetworkPolicyV2 removal, which took with it the classified
        // translator this handler used to reach.
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&state.pool, sandbox_id.into())
            .await
            .map_err(|error| {
                // Adds the `sandbox_id`, which `From<sqlx::Error>`'s generic
                // "public api database error" does not carry. At `debug`,
                // deliberately: the `From` already logs a real failure at
                // `error` and a contention abort at `warn`, so this is context
                // for someone who turned debug on, not the production record.
                // Do not read it as replacing that log.
                tracing::debug!(
                    %error,
                    sandbox_id = %pplx_space_common_core::types::SandboxId::from(sandbox_id),
                    "failed to load sandbox for network policy"
                );
                PublicApiError::from(error)
            })?
            .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
        let (policy, inherited) = match (
            sandbox.network_policy.as_ref(),
            sandbox.template_network_policy.as_ref(),
        ) {
            (Some(policy), _) => (policy.0.clone(), false),
            // Template writes are V1-only.
            (None, Some(template)) => (template.0.clone(), true),
            // No override and no template policy: no desired document.
            (None, None) => return Ok(None),
        };
        Ok(Some(DesiredPolicy {
            sandbox,
            policy,
            inherited,
        }))
    }

    /// Opaque revision of the effective desired policy. Covers the CAS
    /// generation, the canonical serialized document (version included), and
    /// the override-vs-inherited distinction; clients compare and echo it,
    /// never parse it. Any input change — including a later template or
    /// organization baseline change that bumps the generation — changes it.
    fn etag(desired: &DesiredPolicy) -> String {
        revision_etag(
            desired.sandbox.network_policy_generation,
            desired.inherited,
            &desired.policy,
        )
    }

    pub(super) fn revision_etag(
        generation: i64,
        inherited: bool,
        policy: &NetworkPolicy,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(generation.to_be_bytes());
        hasher.update([u8::from(inherited)]);
        hasher.update(serde_json::to_vec(policy).expect("network policy should serialize"));
        let digest = hex::encode(hasher.finalize());
        let (short, _) = digest.split_at(32);
        format!("\"{short}\"")
    }

    fn policy_response(desired: &DesiredPolicy) -> Response {
        let etag_value = etag(desired);
        let mut response = Json(desired.policy.clone()).into_response();
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag_value).expect("etag is ascii"),
        );
        response
    }

    pub(super) async fn get(
        state: &AppState,
        sandbox_id: PublicSandboxId,
    ) -> Result<Option<Response>, PublicApiError> {
        Ok(load_desired(state, sandbox_id)
            .await?
            .map(|desired| policy_response(&desired)))
    }
}

fn parse_public_sandbox_id(value: &str) -> Result<PublicSandboxId, PublicApiError> {
    PublicSandboxId::from_str(value).map_err(|_| PublicApiError::bad_request("invalid sandbox id"))
}

async fn ensure_sandbox_owner(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: PublicSandboxId,
) -> Result<(), PublicApiError> {
    let found = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1 AND org_id = $2)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(org_id)
    .fetch_one(pool)
    .await?;
    if !found {
        return Err(PublicApiError::not_found("sandbox not found"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
