//! Explicit environment creation across rolling service upgrades.
use crate::{SandboxManagerGrpcClient, proto};
use pplx_space_common_core::types::EnvironmentReference;
use tonic::Status;

/// Move both legacy inputs: old servers reject the missing required runtime.
pub fn bind(request: &mut proto::CreateRequest, reference: EnvironmentReference) {
    request.runtime.clear();
    request.environment = Some(proto::EnvironmentCreateInput {
        reference: reference.to_string(),
        template: std::mem::take(&mut request.template),
    });
}

/// Bind a device request without leaving a legacy input an older peer could
/// execute. The selected environment and requirements must both be acknowledged.
pub fn bind_devices(
    request: &mut proto::CreateRequest,
    reference: EnvironmentReference,
    requirements: &pplx_space_execution_spec::devices::DeviceRequirements,
) -> Result<(), Status> {
    if request.environment.is_some() || request.device_create.is_some() {
        return Err(Status::invalid_argument("create input is already bound"));
    }
    request.runtime.clear();
    request.device_create = Some(proto::DeviceCreateInput {
        environment: Some(proto::EnvironmentCreateInput {
            reference: reference.to_string(),
            template: std::mem::take(&mut request.template),
        }),
        requirements: Some(requirements.into()),
    });
    Ok(())
}

/// Unselected requests retain the legacy path. Selected requests require a
/// bounded capability preflight and exact admission acknowledgement. Create's
/// existing channel deadline still owns long blocking guest readiness waits.
pub async fn create(
    client: &mut SandboxManagerGrpcClient,
    request: proto::CreateRequest,
) -> Result<proto::CreateResponse, Status> {
    let (expected, device_digest) = if let Some(input) = &request.device_create {
        if request.environment.is_some()
            || !request.template.is_empty()
            || !request.runtime.is_empty()
        {
            return Err(Status::invalid_argument("provide exactly one create input"));
        }
        let environment = input
            .environment
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("device create requires an environment"))?;
        let requirements = input.requirements.as_ref().ok_or_else(|| {
            Status::invalid_argument("device create requires device requirements")
        })?;
        let requirements =
            pplx_space_execution_spec::devices::DeviceRequirements::try_from(requirements)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        (
            Some(environment.reference.clone()),
            Some(
                pplx_space_execution_common::devices::requirements_digest(&requirements)
                    .to_string(),
            ),
        )
    } else {
        (
            request
                .environment
                .as_ref()
                .map(|input| input.reference.clone()),
            None,
        )
    };
    if expected.is_some() {
        let capabilities = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            client.get_execution_capabilities(proto::GetExecutionCapabilitiesRequest {}),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("environment capability preflight timed out"))?
        .map_err(|error| match error.code() {
            tonic::Code::Unimplemented => Status::unavailable(
                "sandbox manager does not yet support environment selection; retry after rollout",
            ),
            _ => error,
        })?
        .into_inner();
        if device_digest.is_some() && !capabilities.exclusive_devices_v1 {
            return Err(Status::failed_precondition(
                "sandbox manager does not support exclusive devices",
            ));
        }
        if !capabilities.environment_selection {
            return Err(Status::unavailable(
                "sandbox manager environment selection is not ready",
            ));
        }
    }
    let response = client.create(request).await?.into_inner();
    if expected.is_some() && response.environment != expected {
        return Err(Status::failed_precondition(format!(
            "sandbox {} did not acknowledge the requested environment; inspect before retrying",
            response.sandbox_id
        )));
    }
    if response.accepted_device_requirements_digest != device_digest {
        return Err(Status::failed_precondition(format!(
            "sandbox {} did not acknowledge the requested devices; inspect before retrying",
            response.sandbox_id
        )));
    }
    Ok(response)
}
