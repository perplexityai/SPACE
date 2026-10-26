//! Persist image semantics before allocation; never infer them from a missing agent.
use super::*;
use pplx_space_common_core::types::guest_image::{
    GuestDiskLayout, GuestImageContract, GuestNetwork, GuestReadiness,
};

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
    if !image.expects_agent()
        && policy.stages.iter().any(|stage| {
            matches!(
                stage.action,
                pplx_space_common_core::types::AutoLifecycleIdleAction::Pause
                    | pplx_space_common_core::types::AutoLifecycleIdleAction::Suspend
            )
        })
    {
        return Err(Status::failed_precondition(
            "agentless firmware guests do not support automatic pause or suspend",
        ));
    }
    Ok(())
}

pub(super) fn read_contract(path: &std::path::Path) -> Result<GuestImageContract, Status> {
    let bytes = match std::fs::read(path.join("metadata.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(error) => {
            return Err(Status::failed_precondition(format!(
                "read guest image contract: {error}"
            )));
        }
    };
    let metadata: pplx_space_common_core::types::TemplateMetadata = serde_json::from_slice(&bytes)
        .map_err(|error| {
            Status::failed_precondition(format!("decode guest image contract: {error}"))
        })?;
    metadata
        .execution_metadata()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(metadata.guest_image.unwrap_or_default())
}

impl SandboxManagerService {
    pub(super) async fn admit_guest_image(
        &self,
        image: GuestImageContract,
        execution: Option<&pplx_space_common_core::types::ExecutionMetadata>,
    ) -> Result<(), Status> {
        if image == GuestImageContract::default() {
            if execution.is_some_and(|execution| {
                execution.guest().os == pplx_space_common_core::types::GuestOperatingSystem::Windows
            }) {
                return Err(Status::failed_precondition(
                    "Windows requires an explicit guest image contract",
                ));
            }
            return Ok(());
        }
        let execution = execution.ok_or_else(|| {
            Status::failed_precondition("firmware guest requires bound execution metadata")
        })?;
        if self.node_managed_by.is_some() {
            return Err(Status::failed_precondition(
                "firmware guests require a standalone node",
            ));
        }
        if execution.environment().is_none()
            || execution.guest().os != pplx_space_common_core::types::GuestOperatingSystem::Windows
            || image.disk() != GuestDiskLayout::OpaqueRawDisk
            || image.network() != GuestNetwork::SlotDhcpV1
            || !matches!(image.readiness(), GuestReadiness::TcpService { .. })
        {
            return Err(Status::failed_precondition(
                "unqualified guest image contract",
            ));
        }
        let capabilities = tokio::time::timeout(Duration::from_secs(5), async {
            let mut node = super::tail_support::connect_node(&self.node_manager)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            node.get_execution_capabilities(
                pplx_space_node_mgr_client::proto::GetExecutionCapabilitiesRequest {},
            )
            .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("firmware capability check timed out"))??
        .into_inner();
        if !capabilities.firmware_guests_v1 {
            return Err(Status::failed_precondition(
                "node-manager does not support firmware guests",
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
                .await?;
            if response.into_inner().reachable {
                return Ok(());
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

impl SandboxManagerService {
    /// The owning cleanup episode holds activation exclusion throughout this wait.
    /// Failed or unacknowledged ACPI delivery never becomes evidence of a clean exit.
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
            let job = crate::db::read::cleanup_job(&db, job_id)
                .map_err(|error| Status::internal(error.to_string()))?
                .ok_or_else(|| Status::aborted("cleanup job disappeared"))?;
            let created = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", &job.created_at)
                .map_err(|error| {
                    Status::internal(format!("invalid cleanup creation time: {error}"))
                })?
                .to_zoned(jiff::tz::TimeZone::UTC)
                .map_err(|error| Status::internal(error.to_string()))?
                .timestamp();
            let elapsed = jiff::Timestamp::now()
                .as_second()
                .saturating_sub(created.as_second())
                .max(0);
            let remaining = 120_u64.saturating_sub(elapsed.cast_unsigned());
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
        let sandbox = id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Ok(ShutdownOutcome::ForcedAfterDeadline);
            }
            // A promoted kill interrupts the grace period, without waiting out the deadline.
            {
                let db = self.db_conn()?;
                let job = crate::db::read::cleanup_job(&db, job_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| Status::aborted("cleanup job disappeared"))?;
                let work = crate::db::cleanup_jobs::decode(&job.payload)
                    .map_err(|error| Status::internal(error.to_string()))?;
                if work.immediate_kill {
                    return Ok(ShutdownOutcome::ForceRequested);
                }
            }
            match tokio::time::timeout_at(
                deadline,
                crate::runtime_via_nodemgr::get_vm_state(sandbox, &self.node_manager),
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
}

#[cfg(test)]
mod tests;
