//! Device intent survives activation retries. Replies are checked before the
//! next effectful hop; a missing acknowledgement is never a CPU fallback.
use pplx_space_execution_common::{devices::validate_acknowledgement, proto};
use pplx_space_execution_spec::devices::DeviceRequirements;
use tonic::Status;

pub(super) fn reserve_device_memory(
    execution: &pplx_space_common_core::types::ExecutionMetadata,
    requirements: &DeviceRequirements,
    policy: &mut super::start::ResolvedSandboxMemoryPolicy,
) -> Result<(), Status> {
    let reference = execution.environment().ok_or_else(|| {
        Status::failed_precondition("device execution requires a compiled environment")
    })?;
    let architecture = pplx_space_node_runtime_core::runtime_inventory::host_arch()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let catalog =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::cached_for_architecture(
            architecture,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let environment = catalog
        .iter()
        .find(|environment| environment.definition().reference() == reference)
        .ok_or_else(|| Status::failed_precondition("unknown device execution environment"))?;
    environment
        .definition()
        .check_execution(execution)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let resources = environment
        .exclusive_device_requirement_resources(requirements)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let required_mib = resources
        .required_host_memory_mib(u64::from(policy.effective_memory_mib))
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    reserve_host_memory(policy, required_mib)
}

fn reserve_host_memory(
    policy: &mut super::start::ResolvedSandboxMemoryPolicy,
    required_mib: u64,
) -> Result<(), Status> {
    let reservation = policy.cgroup_memory_mib.max(required_mib);
    if reservation > policy.cgroup_memory_max_mib {
        return Err(Status::resource_exhausted(format!(
            "device execution needs {reservation} MiB host memory; node sandbox limit is {} MiB",
            policy.cgroup_memory_max_mib,
        )));
    }
    policy.cgroup_memory_mib = reservation;
    policy.memory_headroom_mib = reservation - u64::from(policy.effective_memory_mib);
    Ok(())
}

pub(super) async fn require_node_support(
    node: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> Result<(), Status> {
    let response = node
        .get_execution_capabilities(
            pplx_space_node_mgr_client::proto::GetExecutionCapabilitiesRequest {},
        )
        .await?
        .into_inner();
    if !response.exclusive_devices_v1 {
        return Err(Status::failed_precondition(
            "node-manager does not support exclusive devices",
        ));
    }
    Ok(())
}

pub(super) fn allocation_acknowledgement(
    sandbox: pplx_space_common_core::types::SandboxId,
    requirements: Option<&DeviceRequirements>,
    acknowledgement: Option<&proto::DeviceAcknowledgement>,
) -> Result<Option<proto::DeviceExecutionInput>, Status> {
    let Some(requirements) = requirements else {
        if acknowledgement.is_some() {
            return Err(Status::data_loss(
                "peer assigned devices to a CPU-only execution",
            ));
        }
        return Ok(None);
    };
    let assignments = validate_acknowledgement(requirements, acknowledgement, None)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    Ok(Some(proto::DeviceExecutionInput {
        sandbox_id: sandbox.to_string(),
        requirements_digest: pplx_space_execution_common::devices::requirements_digest(
            requirements,
        )
        .to_string(),
        assignments: assignments.iter().map(Into::into).collect(),
    }))
}

pub(super) fn execution_acknowledgement(
    expected: Option<&proto::DeviceExecutionInput>,
    acknowledgement: Option<&proto::DeviceAcknowledgement>,
) -> Result<(), Status> {
    let expected = expected
        .map(|expected| {
            pplx_space_execution_common::devices::decode_execution("", Some(expected))
                .map(|(_, execution)| execution)
                .map_err(|error| Status::data_loss(error.to_string()))
        })
        .transpose()?
        .flatten();
    pplx_space_execution_common::devices::validate_execution_acknowledgement(
        expected.as_ref(),
        acknowledgement,
    )
    .map_err(|error| Status::data_loss(error.to_string()))
}

#[cfg(test)]
mod tests;
