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

/// Unselected requests retain the legacy path. Selected requests require a
/// bounded capability preflight and exact admission acknowledgement. Create's
/// existing channel deadline still owns long blocking guest readiness waits.
pub async fn create(
    client: &mut SandboxManagerGrpcClient,
    request: proto::CreateRequest,
) -> Result<proto::CreateResponse, Status> {
    let expected = request
        .environment
        .as_ref()
        .map(|input| input.reference.clone());
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
    Ok(response)
}
