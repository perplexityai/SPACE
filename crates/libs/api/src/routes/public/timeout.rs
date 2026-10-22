use axum::{Json, http::HeaderMap};
use pplx_space_common_core::types::OrgId;
use pplx_space_public_api::{
    RequestFingerprint, SandboxLifecycleState, SetSandboxTimeoutRequest, SetSandboxTimeoutResponse,
    routes::SET_SANDBOX_TIMEOUT,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use super::{
    error::PublicApiError,
    sandboxes::{controller_error, read_public_sandbox},
};
use crate::{
    controllers::{
        sandbox::{AccessIdleTimeoutOutcome, fetch_sandbox},
        substrate::NODE_RPC_TIMEOUT,
    },
    public_operations::{
        CreatePublicOperationIdempotency, HttpOperationMethod, OperationPathTemplate,
        PublicOperationStore,
    },
    routes::AppState,
};

pub(super) async fn set_timeout(
    state: &AppState,
    org_id: OrgId,
    sandbox_id: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Json<SetSandboxTimeoutResponse>, PublicApiError> {
    let request: SetSandboxTimeoutRequest = serde_json::from_slice(body)
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    let sandbox_id = super::sandboxes::parse_public_sandbox_id(sandbox_id)?;
    let key = super::sandboxes::idempotency_key(headers)?;
    let timeout_secs = request
        .timeout_secs()
        .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
    let fingerprint = RequestFingerprint::parse(hex::encode(Sha256::digest(
        format!("{sandbox_id}:{timeout_secs}").as_bytes(),
    )))
    .map_err(|_| PublicApiError::internal())?;
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new(SET_SANDBOX_TIMEOUT.path()),
        key: &key,
        request_fingerprint: &fingerprint,
    };
    let admission = PublicOperationStore::new(state.pool.clone())
        .begin_response_admission(org_id, &idempotency)
        .await?;
    let sandbox = fetch_sandbox(&state.pool, sandbox_id.into())
        .await?
        .filter(|sandbox| sandbox.org_id == org_id)
        .ok_or_else(|| PublicApiError::not_found("sandbox not found"))?;
    if let Some(response) = admission.response() {
        return serde_json::from_value(response.clone())
            .map(Json)
            .map_err(|_| PublicApiError::internal());
    }
    if sandbox.lifecycle == "deleted" {
        return Err(PublicApiError::failed_precondition("sandbox is terminal"));
    }
    let public = read_public_sandbox(&state.pool, org_id, sandbox_id).await?;
    if matches!(
        public.state,
        SandboxLifecycleState::Deleting
            | SandboxLifecycleState::Deleted
            | SandboxLifecycleState::Failed
            | SandboxLifecycleState::Stopped
            | SandboxLifecycleState::Stopping
    ) {
        return Err(PublicApiError::failed_precondition("sandbox is terminal"));
    }
    let tracked = state
        .sandbox_controller
        .tracked_response(&state.pool, sandbox.clone())
        .await
        .map_err(controller_error)?;
    if crate::routes::e2b::translate_sandbox_state(&tracked).is_none() {
        return Err(PublicApiError::failed_precondition("sandbox is terminal"));
    }
    if public.idle_timeout.is_none() || public.idle_action.is_none() {
        return Err(PublicApiError::failed_precondition(
            "sandbox must have a single access-idle delete or suspend rule",
        ));
    }
    let current_policy = sandbox.auto_lifecycle.0.clone().normalized();
    let (Some(config), [stage]) = (
        crate::e2b::lifecycle_config_from_policy(&current_policy),
        current_policy.stages.as_slice(),
    ) else {
        return Err(PublicApiError::failed_precondition(
            "unsupported idle timeout policy",
        ));
    };
    if crate::e2b::access_idle_policy(stage.at, config.auto_pause, config.auto_resume).normalized()
        != current_policy
    {
        return Err(PublicApiError::failed_precondition(
            "custom lifecycle policy cannot be preserved",
        ));
    }
    let observed_at = OffsetDateTime::now_utc();
    let outcome = state
        .sandbox_controller
        .set_access_idle_timeout(
            &state.pool,
            &sandbox,
            timeout_secs,
            prost_types::Timestamp {
                seconds: observed_at.unix_timestamp(),
                nanos: observed_at.nanosecond() as i32,
            },
            tokio::time::Instant::now() + NODE_RPC_TIMEOUT,
            Some(&current_policy),
        )
        .await
        .map_err(controller_error)?;
    let after = read_public_sandbox(&state.pool, org_id, sandbox_id).await?;
    // The acknowledgement has no timestamp, and a stored deadline can predate this reset.
    // Estimate before the call assuming node and API clocks agree within five seconds.
    // Dormant sandboxes only persist intent, so report their actual nullable stored deadline.
    let expires_at = match outcome {
        AccessIdleTimeoutOutcome::StoredOnly => after.expires_at,
        AccessIdleTimeoutOutcome::Applied => {
            Some(conservative_deadline(observed_at, timeout_secs)?)
        }
        // The expected-policy check already pinned an access-idle policy.
        AccessIdleTimeoutOutcome::Unmanaged => {
            return Err(PublicApiError::failed_precondition(
                "unsupported idle timeout policy",
            ));
        }
    };
    let response = SetSandboxTimeoutResponse {
        sandbox_id,
        idle_timeout: format!("{timeout_secs}s"),
        expires_at,
    };
    admission
        .complete(
            &idempotency,
            serde_json::to_value(&response).map_err(|_| PublicApiError::internal())?,
        )
        .await?;
    Ok(Json(response))
}

const DEADLINE_CLOCK_SKEW_ALLOWANCE_SECS: u32 = 5;

fn conservative_deadline(
    observed_at: OffsetDateTime,
    timeout_secs: u32,
) -> Result<OffsetDateTime, PublicApiError> {
    observed_at
        .checked_add(time::Duration::seconds(i64::from(
            timeout_secs.saturating_sub(DEADLINE_CLOCK_SKEW_ALLOWANCE_SECS),
        )))
        .ok_or_else(PublicApiError::internal)
}

#[cfg(test)]
mod tests;
