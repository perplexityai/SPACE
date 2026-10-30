//! Persist image semantics before allocation; never infer them from a missing agent.
use super::*;
const FIRMWARE_SHUTDOWN_BUDGET: Duration = Duration::from_secs(120);
use pplx_space_common_core::types::guest_image::GuestImageContract;

pub(super) fn require_agent(sandbox: &crate::db::SandboxRow) -> Result<(), Status> {
    if !sandbox.guest_image.expects_agent() {
        return Err(Status::unimplemented(
            "guest image has no SPACE guest agent",
        ));
    }
    Ok(())
}

pub(super) fn validate_policy(
    image: GuestImageContract,
    policy: &AutoLifecyclePolicy,
) -> Result<(), Status> {
    if !image.supports_auto_memory_lifecycle() && policy.requires_memory_lifecycle() {
        return Err(Status::failed_precondition(
            "agentless firmware guests do not support automatic pause or suspend",
        ));
    }
    Ok(())
}

impl SandboxManagerService {
    pub(super) async fn admit_guest_image(
        &self,
        image: GuestImageContract,
        execution: Option<&pplx_space_common_core::types::ExecutionMetadata>,
    ) -> Result<(), Status> {
        let Some(execution) = execution else {
            return if image.legacy_transport() {
                Ok(())
            } else {
                Err(Status::failed_precondition(
                    "guest image requires bound execution metadata",
                ))
            };
        };
        pplx_space_node_runtime_core::boot::GuestProfile::resolve(execution, image)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if !image.legacy_transport() && self.node_managed_by.is_some() {
            return Err(Status::failed_precondition(
                "guest image transport requires a standalone node",
            ));
        }

        Ok(())
    }
}

/// Connect and all probes share one activation budget; a stale execution is terminal.
pub(super) async fn wait_service(
    connector: &crate::node_manager_transport::NodeManagerConnector,
    id: &str,
    execution: pplx_space_common_core::types::SandboxActivationAttemptId,
    port: std::num::NonZeroU16,
    budget: Duration,
) -> Result<(), Status> {
    tokio::time::timeout(budget, async {
        let mut node = super::tail_support::connect_node(connector)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        loop {
            let response = node
                .probe_guest_service(
                    pplx_space_node_mgr_client::proto::ProbeGuestServiceRequest {
                        sandbox_id: id.to_owned(),
                        execution_fence: Some(pplx_space_node_mgr_client::proto::ExecutionFence {
                            execution_id: execution.to_string(),
                            ..Default::default()
                        }),
                        port: u32::from(port.get()),
                    },
                )
                .await;
            match response {
                Err(error)
                    if matches!(
                        error.code(),
                        tonic::Code::Unavailable | tonic::Code::ResourceExhausted
                    ) => {}
                Err(error) => return Err(error),
                Ok(response) if response.get_ref().reachable => return Ok(()),
                Ok(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .map_err(|_| Status::deadline_exceeded("guest service readiness timed out"))?
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ShutdownOutcome {
    GuestExitObserved,
    ForcedAfterDeadline,
    ForceRequested,
    Unclassified,
}
impl ShutdownOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::GuestExitObserved => "guest_exit_observed",
            Self::ForcedAfterDeadline => "forced_after_deadline",
            Self::ForceRequested => "force_requested",
            Self::Unclassified => "unclassified",
        }
    }
}
impl std::fmt::Display for ShutdownOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Inspection is evidence only when both sandbox and execution still match.
/// Missing PID/fence and transport failures remain unknown, never a clean exit.
async fn inspect_shutdown_process(
    connector: &crate::node_manager_transport::NodeManagerConnector,
    sandbox: &str,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    deadline: tokio::time::Instant,
) -> Result<Option<(u32, bool)>, Status> {
    let response = tokio::time::timeout_at(
        deadline.min(tokio::time::Instant::now() + Duration::from_secs(2)),
        connector.lazy().inspect_sandbox(
            pplx_space_node_mgr_client::proto::InspectSandboxRequest {
                sandbox_id: sandbox.to_owned(),
                ..Default::default()
            },
        ),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("shutdown process inspection timed out"))??
    .into_inner();
    if response.sandbox_id != sandbox {
        return Err(Status::aborted("shutdown sandbox changed"));
    }
    let Some(observed) = response.execution_fence.as_ref() else {
        return Ok(None);
    };
    let observed = pplx_space_node_mgr_client::execution_fence::ExecutionFence::try_from(observed)?;
    if observed.current() != fence.current() {
        return Err(Status::aborted("shutdown execution changed"));
    }
    Ok(response
        .runtime_pid
        .filter(|pid| *pid > 0)
        .map(|pid| (pid, response.runtime_alive)))
}

impl SandboxManagerService {
    /// The owning cleanup episode holds activation exclusion throughout this wait.
    /// Process absence is not an exit-status assertion: crashes and power-off
    /// can both produce GuestExitObserved. RPC/socket errors prove neither.
    pub(super) async fn stop_firmware_guest(
        &self,
        id: &str,
        job_id: Option<&pplx_space_common_core::types::JobId>,
        force: bool,
    ) -> Result<ShutdownOutcome, Status> {
        if force {
            return Ok(ShutdownOutcome::ForceRequested);
        }
        let Some(job_id) = job_id else {
            return Ok(ShutdownOutcome::Unclassified);
        };
        let (fence, remaining) = {
            let db = self.db_conn()?;
            let created = crate::db::read::cleanup_job_created_at(&db, job_id)
                .map_err(|error| Status::internal(error.to_string()))?
                .ok_or_else(|| Status::aborted("cleanup job disappeared"))?;
            let elapsed = jiff::Timestamp::now()
                .as_second()
                .saturating_sub(created.get())
                .max(0);
            let remaining = FIRMWARE_SHUTDOWN_BUDGET
                .as_secs()
                .saturating_sub(elapsed.cast_unsigned());
            let fence = crate::db::read::sandbox_view(&db, id)
                .node_execution_fence()
                .map_err(|error| Status::internal(error.to_string()))?;
            (fence, remaining)
        };
        if remaining == 0 {
            return Ok(ShutdownOutcome::ForcedAfterDeadline);
        }
        let Some(fence) = fence else {
            return Ok(ShutdownOutcome::Unclassified);
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(remaining);
        let expected_pid =
            match inspect_shutdown_process(&self.node_manager, id, fence, deadline).await {
                Ok(Some((_, false))) => return Ok(ShutdownOutcome::GuestExitObserved),
                Ok(Some((pid, true))) => Some(pid),
                Err(error) if error.code() == tonic::Code::Aborted => return Err(error),
                Ok(None) | Err(_) => None,
            };
        let mut node = self.node_manager.lazy();
        let request = node.request_guest_shutdown(
            pplx_space_node_mgr_client::proto::RequestGuestShutdownRequest {
                sandbox_id: id.to_owned(),
                execution_fence: Some(fence.into()),
            },
        );
        match tokio::time::timeout_at(deadline, request).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if error.code() == tonic::Code::Aborted => return Err(error),
            Ok(Err(error)) => {
                tracing::warn!(sandbox_id = id, %error, "ACPI request not acknowledged");
                return Ok(ShutdownOutcome::Unclassified);
            }
            Err(_) => return Ok(ShutdownOutcome::ForcedAfterDeadline),
        }
        wait_firmware_shutdown(
            id,
            &self.node_manager,
            fence,
            expected_pid,
            deadline,
            || {
                let db = self.db_conn()?;
                let job = crate::db::read::cleanup_job(&db, job_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| Status::aborted("cleanup job disappeared"))?;
                let work = crate::db::cleanup_jobs::decode(&job.payload)
                    .map_err(|error| Status::internal(error.to_string()))?;
                Ok(work.immediate_kill)
            },
        )
        .await
    }
}

async fn wait_firmware_shutdown<F>(
    id: &str,
    connector: &crate::node_manager_transport::NodeManagerConnector,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    expected_pid: Option<u32>,
    deadline: tokio::time::Instant,
    mut force_requested: F,
) -> Result<ShutdownOutcome, Status>
where
    F: FnMut() -> Result<bool, Status>,
{
    let sandbox = id
        .parse::<SandboxId>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Ok(ShutdownOutcome::ForcedAfterDeadline);
        }
        if force_requested()? {
            return Ok(ShutdownOutcome::ForceRequested);
        }
        if let Some(expected_pid) = expected_pid {
            match inspect_shutdown_process(connector, id, fence, deadline).await {
                Ok(Some((pid, _))) if pid != expected_pid => {
                    return Err(Status::aborted("shutdown runtime process changed"));
                }
                Ok(Some((_, false))) => return Ok(ShutdownOutcome::GuestExitObserved),
                Err(error) if error.code() == tonic::Code::Aborted => return Err(error),
                Ok(_) | Err(_) => {}
            }
        }
        match tokio::time::timeout_at(
            deadline,
            crate::runtime_via_nodemgr::get_vm_state(sandbox, connector),
        )
        .await
        {
            Ok(Ok(pplx_space_node_runtime_core::vm_state::VmState::Shutdown)) => {
                return Ok(ShutdownOutcome::GuestExitObserved);
            }
            Err(_) => return Ok(ShutdownOutcome::ForcedAfterDeadline),
            Ok(_) => {}
        }
        tokio::time::sleep_until(
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(250)),
        )
        .await;
    }
}

#[cfg(test)]
mod tests;
