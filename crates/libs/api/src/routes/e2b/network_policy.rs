//! SPACE extension: replace desired policy, then observe live enforcement.
use super::*;
use crate::middleware::auth::AuthFederatedOwner;
use pplx_space_common_core::types::IdempotencyKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ReplacePolicy {
    expected_revision: String,
    network: Value,
    #[serde(default)]
    allow_internet_access: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AcceptedPolicy {
    target: String,
}

pub(super) async fn replace(
    State(state): State<AppState>,
    MatchedEcwHost(_host): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<AuthFederatedOwner>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    crate::e2b::E2bJson(request): crate::e2b::E2bJson<ReplacePolicy>,
) -> Result<Response, E2bApiError> {
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &id).await?;
    let key = headers
        .get("idempotency-key")
        .ok_or_else(|| E2bApiError::bad_request("Idempotency-Key header is required"))?;
    let key = IdempotencyKey::parse_header(key.as_bytes())
        .map_err(|err| E2bApiError::bad_request(err.to_string()))?;
    let policy = resolve_e2b_network_policy(
        Some(request.allow_internet_access),
        parse_e2b_network_config(Some(&request.network))?,
    )?
    .ok_or_else(|| E2bApiError::bad_request("explicit network policy is required"))?;
    let request_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&request)
            .map_err(|_| E2bApiError::internal("failed to serialize policy request"))?,
    ));
    let idempotency_digest = hex::encode(Sha256::digest(key.as_str().as_bytes()));
    let target = state
        .sandbox_controller
        .replace_network_policy(
            &state.pool,
            sandbox.id,
            policy,
            &request.expected_revision,
            &idempotency_digest,
            &request_hash,
        )
        .await
        .map_err(controller_error)?;
    Ok((StatusCode::ACCEPTED, Json(AcceptedPolicy { target })).into_response())
}

pub(super) async fn get(
    State(state): State<AppState>,
    MatchedEcwHost(_host): MatchedEcwHost,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    federated: Option<Extension<AuthFederatedOwner>>,
    Path(id): Path<String>,
) -> Result<Response, E2bApiError> {
    let scope = ecw_access_scope(&org, &federated);
    let sandbox = fetch_scoped_sandbox(&state, &scope, &id).await?;
    let observation = state
        .sandbox_controller
        .observe_network_policy(&state.pool, &sandbox)
        .await
        .map_err(controller_error)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(observation)).into_response())
}
