//! Rearm only a settled resident VM, under the same lock as snapshot capture.

use super::activation_support::{GuestActivityKind, WakeStartConfig};
use super::auth_lookup::SpaceApiBridgeRouteAuthorization;
use super::spaced_bridge::SpacedEndpointProofPolicy;
use super::*;
use pplx_space_common_core::types::ProcessRecoveryToken;
use pplx_space_spaced_common::recovery::RecoveryReason;
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
        let (client, observed) = tokio::time::timeout(timeout, async {
            let client = self
                .connect_spaced_host_with_policy(sandbox, policy)
                .await?;
            let observed = client.wait_ready(timeout).await?;
            Ok::<_, Status>((client, observed))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("guest readiness deadline elapsed"))??;
        let budget = legacy_rearm_budget(timeout, started.elapsed());
        let rearm = self.rearm_observed(sandbox, authorization, client, observed.clone(), budget);
        Ok(best_effort_recovery_ready(observed, budget, rearm).await)
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

    pub(super) async fn ensure_recovery_active(
        &self,
        sandbox_id: &str,
        authorization: &SpaceApiBridgeRouteAuthorization,
        fence: pplx_space_spaced_common::recovery::RecoveryFence,
        budget: &pplx_space_common_grpc::GrpcRetryContext,
        activity: GuestActivityKind,
    ) -> Result<crate::db::SandboxRow, Status> {
        let remaining = recovery_remaining(budget)?;
        let state_writer = self.state_writer_lease("process recovery")?;
        let service = self.clone();
        let authorization = authorization.clone();
        let sandbox_id = sandbox_id.to_owned();
        self.task_tracker
            .spawn(super::drain::with_state_writer(state_writer, async move {
                tokio::time::timeout(
                    remaining,
                    service.ensure_active_for_guest_activity_with_start(
                        Some(&authorization),
                        &sandbox_id,
                        true,
                        Some(remaining),
                        activity,
                        WakeStartConfig {
                            recovery_fence: Some(fence),
                            ..Default::default()
                        },
                    ),
                )
                .await
                .map_err(|_| RecoveryReason::TemporarilyUnavailable.into_status())?
            }))
            .await
            .map_err(|_| Status::internal("recovery wake task failed"))?
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
        let budget = pplx_space_common_grpc::GrpcRetryContext::with_timeout(timeout);
        recovery_remaining(&budget)?;
        tokio::time::timeout(timeout, async {
            let client = self
                .connect_spaced_host_with_policy(sandbox, policy)
                .await?;
            let response = client.wait_ready(recovery_remaining(&budget)?).await?;
            let response = self
                .rearm_observed(
                    sandbox,
                    authorization,
                    client.clone(),
                    response,
                    recovery_remaining(&budget)?,
                )
                .await?;
            Ok((client, response))
        })
        .await
        .map_err(|_| RecoveryReason::TemporarilyUnavailable.into_status())?
        .map_err(rearm_status)
    }

    async fn rearm_observed(
        &self,
        sandbox: &crate::db::SandboxRow,
        authorization: Option<&SpaceApiBridgeRouteAuthorization>,
        client: SpacedHostClient,
        response: WaitReadyResponse,
        timeout: Duration,
    ) -> Result<WaitReadyResponse, Status> {
        let budget = pplx_space_common_grpc::GrpcRetryContext::with_timeout(timeout);
        let Some(barrier) = response.process_recovery_barrier_id else {
            return Ok(response);
        };
        let activation = self
            .recovery_activation_scope(&sandbox.id, authorization)
            .await?;
        let current = &activation.sandbox;
        // Reuse the attested connection only while its runtime and route remain
        // current. A changed row requires a fresh observation on the next call.
        if current.node_execution_id != sandbox.node_execution_id
            || current.spaced_host_pid != sandbox.spaced_host_pid
            || current.pid != sandbox.pid
            || current.space_api_route_identity != sandbox.space_api_route_identity
        {
            return Err(RecoveryReason::TemporarilyUnavailable.into_status());
        }
        let expected = current
            .node_execution_id
            .ok_or_else(|| RecoveryReason::TemporarilyUnavailable.into_status())?;
        let barrier = ProcessRecoveryToken::parse(barrier)
            .map_err(|_| RecoveryReason::TemporarilyUnavailable.into_status())?;
        client
            .resume_process_recovery(barrier, recovery_remaining(&budget)?)
            .await?;
        let response = client.wait_ready(recovery_remaining(&budget)?).await?;
        if response.process_recovery_barrier_id.is_some()
            || response
                .process_execution
                .as_ref()
                .and_then(|info| info.activation_id.as_deref())
                != Some(expected.to_string().as_str())
        {
            return Err(RecoveryReason::TemporarilyUnavailable.into_status());
        }
        Ok(response)
    }

    pub(super) async fn connect_recovery_host(
        &self,
        sandbox: &crate::db::SandboxRow,
        authorization: &SpaceApiBridgeRouteAuthorization,
        budget: &pplx_space_common_grpc::GrpcRetryContext,
    ) -> Result<SpacedHostClient, Status> {
        self.recovery_ready(
            sandbox,
            Some(authorization),
            SpacedEndpointProofPolicy::Required,
            recovery_remaining(budget)?,
        )
        .await
        .map(|(client, _)| client)
    }
}

fn recovery_remaining(
    budget: &pplx_space_common_grpc::GrpcRetryContext,
) -> Result<Duration, Status> {
    budget
        .remaining()
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| RecoveryReason::TemporarilyUnavailable.into_status())
}

fn legacy_rearm_budget(timeout: Duration, elapsed: Duration) -> Duration {
    timeout.saturating_sub(elapsed) / 2
}

fn rearm_status(status: Status) -> Status {
    if RecoveryReason::from_status(&status).is_none()
        && pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&status).is_none()
        && matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::ResourceExhausted
                | tonic::Code::DeadlineExceeded
                | tonic::Code::FailedPrecondition
        )
    {
        RecoveryReason::TemporarilyUnavailable.into_status()
    } else {
        status
    }
}

pub(super) fn resident_recovery_fence(
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
