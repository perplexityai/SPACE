//! Explicit environment submission across rolling service upgrades.
use crate::{BuildManagerGrpcClient, proto};
use pplx_space_common_core::types::EnvironmentReference;
use tonic::Status;

/// Move the recipe into the selected input. Old servers reject missing recipe.
pub fn bind(request: &mut proto::StartBuildRequest, reference: EnvironmentReference) {
    request.environment = Some(proto::EnvironmentBuildInput {
        reference: reference.to_string(),
        recipe: request.recipe.take(),
    });
}

/// Capability discovery is advisory; the request encoding is the safety guard.
/// The preflight and enqueue operation together are bounded to 30 seconds.
pub async fn start_build(
    client: &mut BuildManagerGrpcClient,
    request: proto::StartBuildRequest,
    reference: EnvironmentReference,
) -> Result<proto::StartBuildResponse, Status> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let capabilities = client
            .get_execution_capabilities(proto::GetExecutionCapabilitiesRequest {})
            .await
            .map_err(|error| match error.code() {
                tonic::Code::Unimplemented => Status::unavailable(
                    "builder does not yet support environment selection; retry after rollout",
                ),
                _ => error,
            })?
            .into_inner();
        if !capabilities.environment_selection {
            return Err(Status::unavailable(
                "builder environment selection is not ready",
            ));
        }
        let response = client.start_build(request).await?.into_inner();
        if response.environment.as_deref() != Some(reference.to_string().as_str()) {
            return Err(Status::failed_precondition(format!(
                "build job {} did not acknowledge the requested environment; do not retry blindly",
                response.job_id
            )));
        }
        Ok(response)
    })
    .await
    .map_err(|_| Status::deadline_exceeded("environment build submission timed out"))?
}
