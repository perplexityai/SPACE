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
        client: SpacedHostClient,
        observed: WaitReadyResponse,
        remaining: Duration,
    ) -> WaitReadyResponse {
        let budget = legacy_rearm_budget(remaining);
        let rearm = self.rearm_observed(sandbox, authorization, client, observed.clone(), budget);
        best_effort_recovery_ready(observed, budget, rearm).await
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
        authorize_resident_recovery(&sandbox, Some(authorization), fence)
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
        let state_writer = match self.state_writer_lease("process recovery") {
            Ok(writer) => writer,
            Err(status) => {
                let sandbox = self.lookup_sandbox(sandbox_id)?;
                authorize_resident_recovery(&sandbox, Some(authorization), fence)?;
                if sandbox.state == SandboxState::Started {
                    return Ok(sandbox);
                }
                return Err(rearm_status(status));
            }
        };
        let service = self.clone();
        let authorization = authorization.clone();
        let sandbox_id = sandbox_id.to_owned();
        let wake =
            self.task_tracker
                .spawn(super::drain::with_state_writer(state_writer, async move {
                    service
                        .ensure_active_for_guest_activity_with_start(
                            Some(&authorization),
                            &sandbox_id,
                            true,
                            Some(remaining),
                            activity,
                            WakeStartConfig {
                                recovery_fence: Some(fence),
                                ..Default::default()
                            },
                        )
                        .await
                }));
        // A resume owns its finalization after publishing Resuming. Expiring
        // the caller detaches this tracked task; it must not drop the wake.
        tokio::time::timeout(remaining, wake)
            .await
            .map_err(|_| RecoveryReason::TemporarilyUnavailable.into_status())?
            .map_err(|_| Status::internal("recovery wake task failed"))?
            .map_err(rearm_status)
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
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let refusal = match sandbox.state {
            SandboxState::Started => None,
            SandboxState::Suspended | SandboxState::Stopped => {
                Some(RecoveryReason::ActivationChanged)
            }
            _ => Some(RecoveryReason::TemporarilyUnavailable),
        };
        if let Some(reason) = refusal {
            let status = reason.into_status();
            return Err(match authorization {
                Some(authorization) => authorization.refuse_sandbox(&sandbox, status),
                None => status,
            });
        }
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
        let activation = tokio::time::timeout(
            recovery_remaining(&budget)?,
            self.recovery_activation_scope(&sandbox.id, authorization),
        )
        .await
        .map_err(|_| RecoveryReason::TemporarilyUnavailable.into_status())??;
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

fn legacy_rearm_budget(remaining: Duration) -> Duration {
    (remaining / 2).min(Duration::from_secs(3))
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
        let temporary = RecoveryReason::TemporarilyUnavailable.into_status();
        let mut mapped = Status::with_metadata(
            temporary.code(),
            temporary.message(),
            status.metadata().clone(),
        );
        mapped.metadata_mut().insert(
            pplx_space_spaced_common::recovery::RECOVERY_REASON_METADATA_KEY,
            tonic::metadata::MetadataValue::from_static(
                RecoveryReason::TemporarilyUnavailable.as_str(),
            ),
        );
        mapped
    } else {
        status
    }
}

pub(super) fn authorize_resident_recovery(
    sandbox: &crate::db::SandboxRow,
    authorization: Option<&SpaceApiBridgeRouteAuthorization>,
    fence: pplx_space_spaced_common::recovery::RecoveryFence,
) -> Result<(), Status> {
    if let Err(status) = resident_recovery_fence(sandbox, fence) {
        return Err(match authorization {
            Some(authorization) => authorization.refuse_sandbox(sandbox, status),
            None => status,
        });
    }
    if let Some(authorization) = authorization {
        authorization.authorize_sandbox(sandbox)?;
    }
    Ok(())
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
