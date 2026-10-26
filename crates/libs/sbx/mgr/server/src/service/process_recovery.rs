//! Rearm only a settled resident VM, under the same lock as snapshot capture.

use super::auth_lookup::SpaceApiBridgeRouteAuthorization;
use super::spaced_bridge::SpacedEndpointProofPolicy;
use super::*;
use pplx_space_common_core::types::ProcessRecoveryToken;
use pplx_space_spaced_host_client::{SpacedHostClient, proto::WaitReadyResponse};

struct RecoveryActivation {
    sandbox: crate::db::SandboxRow,
    _capture: crate::auto_lifecycle::LifecycleMutexGuard,
}

impl SandboxManagerService {
    async fn recovery_activation_scope(
        &self,
        sandbox_id: &str,
        authorization: Option<&SpaceApiBridgeRouteAuthorization>,
    ) -> Result<RecoveryActivation, Status> {
        let capture = self
            .auto_lifecycle
            .artifact_lock(sandbox_id)
            .lock_owned()
            .await;
        let sandbox = self.lookup_started_sandbox(sandbox_id)?;
        if let Some(authorization) = authorization {
            authorization.authorize_sandbox(&sandbox)?;
        }
        Ok(RecoveryActivation {
            sandbox,
            _capture: capture,
        })
    }

    /// Readiness remains observational on older hosts. New hosts are rearmed only
    /// after re-reading and authorizing Started under the artifact lock. Bound the
    /// complete lock/attestation/handshake by the caller's readiness budget.
    pub(super) async fn recovery_ready(
        &self,
        sandbox: &crate::db::SandboxRow,
        authorization: Option<&SpaceApiBridgeRouteAuthorization>,
        policy: SpacedEndpointProofPolicy,
        timeout: Duration,
    ) -> Result<(SpacedHostClient, WaitReadyResponse), Status> {
        tokio::time::timeout(timeout, async {
            let client = self
                .connect_spaced_host_with_policy(sandbox, policy)
                .await?;
            let response = client.wait_ready(timeout).await?;
            if response.process_recovery_barrier_id.is_none() {
                return Ok((client, response));
            }
            let activation = self
                .recovery_activation_scope(&sandbox.id, authorization)
                .await?;
            let current = &activation.sandbox;
            let client = self
                .connect_spaced_host_with_policy(current, policy)
                .await?;
            let response = client.wait_ready(timeout).await?;
            if let Some(barrier) = response.process_recovery_barrier_id {
                let barrier = ProcessRecoveryToken::parse(barrier).map_err(|error| {
                    Status::failed_precondition(format!("invalid host recovery barrier: {error}"))
                })?;
                client.resume_process_recovery(barrier).await?;
            }
            let response = client.wait_ready(timeout).await?;
            let expected = current.node_execution_id.ok_or_else(|| {
                Status::failed_precondition("sandbox execution identity is unavailable")
            })?;
            if response.process_recovery_barrier_id.is_some()
                || response
                    .process_execution
                    .as_ref()
                    .and_then(|info| info.activation_id.as_deref())
                    != Some(expected.to_string().as_str())
            {
                return Err(Status::failed_precondition(
                    "host did not confirm the current recovery activation",
                ));
            }
            Ok((client, response))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("process recovery readiness timed out"))?
    }

    pub(super) async fn connect_recovery_host(
        &self,
        sandbox: &crate::db::SandboxRow,
        authorization: &SpaceApiBridgeRouteAuthorization,
    ) -> Result<SpacedHostClient, Status> {
        self.recovery_ready(
            sandbox,
            Some(authorization),
            SpacedEndpointProofPolicy::Required,
            Duration::from_secs(30),
        )
        .await
        .map(|(client, _)| client)
    }
}

#[cfg(test)]
mod tests;
