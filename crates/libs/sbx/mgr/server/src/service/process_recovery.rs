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

/// Recovery is optional on the legacy readiness path. A failed or stalled rearm
/// must not invalidate the readiness observation already obtained from the host.
async fn best_effort_recovery_ready(
    mut observed: WaitReadyResponse,
    budget: Duration,
    rearm: impl std::future::Future<Output = Result<WaitReadyResponse, Status>>,
) -> WaitReadyResponse {
    if observed.process_recovery_barrier_id.is_none() {
        return observed;
    }
    if let Ok(Ok(ready)) = tokio::time::timeout(budget, rearm).await {
        return ready;
    }
    if let Some(info) = observed.process_execution.as_mut() {
        info.activation_id = None;
        info.capabilities.retain(|capability| {
            capability == pplx_space_common_grpc::GuestCapability::ProcessOutputGapResume.as_str()
        });
    }
    observed
}

impl SandboxManagerService {
    pub(super) async fn legacy_recovery_ready(
        &self,
        sandbox: &crate::db::SandboxRow,
        authorization: Option<&SpaceApiBridgeRouteAuthorization>,
        policy: SpacedEndpointProofPolicy,
        timeout: Duration,
    ) -> Result<WaitReadyResponse, Status> {
        let started = tokio::time::Instant::now();
        let client = self
            .connect_spaced_host_with_policy(sandbox, policy)
            .await?;
        let observed = client.wait_ready(timeout).await?;
        let budget = timeout
            .saturating_sub(started.elapsed())
            .min(Duration::from_millis(100));
        Ok(best_effort_recovery_ready(observed, budget, async {
            self.recovery_ready(sandbox, authorization, policy, budget)
                .await
                .map(|(_, ready)| ready)
        })
        .await)
    }

    pub(super) fn require_resident_recovery(
        &self,
        sandbox_id: &str,
        authorization: &SpaceApiBridgeRouteAuthorization,
        fence: Option<pplx_space_spaced_common::recovery::RecoveryFence>,
    ) -> Result<(), Status> {
        let Some(fence) = fence else {
            return Ok(());
        };
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        authorization.authorize_sandbox(&sandbox)?;
        resident_recovery_fence(&sandbox, fence)
    }

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

fn resident_recovery_fence(
    sandbox: &crate::db::SandboxRow,
    fence: pplx_space_spaced_common::recovery::RecoveryFence,
) -> Result<(), Status> {
    use pplx_space_spaced_common::recovery::RecoveryReason;
    match sandbox.state {
        SandboxState::Started | SandboxState::Paused => {}
        SandboxState::Suspended | SandboxState::Stopped => {
            return Err(RecoveryReason::ActivationChanged.into_status());
        }
        _ => return Err(RecoveryReason::TemporarilyUnavailable.into_status()),
    }
    let current = sandbox
        .node_execution_id
        .ok_or_else(|| RecoveryReason::TemporarilyUnavailable.into_status())?;
    if current.to_string() != fence.activation.to_string() {
        return Err(RecoveryReason::ActivationChanged.into_status());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
