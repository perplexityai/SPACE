//! Cold creates persist selection before activation. Restore attempts select
//! from immutable checkpoint provenance; retained cold starts use the saved consumer.
use super::{SandboxManagerService, tail_support::connect_node};
use pplx_space_common_core::types::{EnvironmentReference, ExecutionMetadata};
use pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION as DEFINITION_SCHEMA;
use pplx_space_node_mgr_client::{
    NodeManagerGrpcClient,
    environment_selection::{SelectedEnvironment, SelectionResponseError, decode},
    proto::{
        ExecutionEnvironmentSelection, ResolveExecutionEnvironmentRequest,
        resolve_execution_environment_request::Compatible,
    },
};
use tonic::Status;

impl SandboxManagerService {
    pub(super) async fn select_compatible_execution(
        &self,
        source: &ExecutionMetadata,
        intent: ExecutionEnvironmentSelection,
    ) -> Result<ExecutionMetadata, Status> {
        if source.environment().is_none() {
            return Ok(source.clone());
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut node = connect_node(&self.node_manager)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            select_with_client(&mut node, source, intent).await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("compatible environment selection timed out"))?
    }
}

/// The caller owns the deadline, including any exact fallback on this connection.
pub(super) async fn select_with_client(
    node: &mut NodeManagerGrpcClient,
    source: &ExecutionMetadata,
    intent: ExecutionEnvironmentSelection,
) -> Result<ExecutionMetadata, Status> {
    let Some(reference) = source.environment() else {
        return Ok(source.clone());
    };
    let request = match intent {
        ExecutionEnvironmentSelection::ColdBoot => Compatible::ColdBootSource(source.into()),
        ExecutionEnvironmentSelection::Restore => Compatible::RestoreSource(source.into()),
        _ => {
            return Err(Status::invalid_argument(
                "sandbox selection requires a source operation",
            ));
        }
    };
    let response = node
        .resolve_execution_environment(ResolveExecutionEnvironmentRequest {
            definition_schema_version: DEFINITION_SCHEMA,
            reference: None,
            compatible: Some(request.clone()),
        })
        .await?
        .into_inner();
    match decode(&request, response) {
        Ok(selected) => Ok(selected.execution),
        Err(SelectionResponseError::Unsupported)
            if intent == ExecutionEnvironmentSelection::ColdBoot =>
        {
            let selected = resolve_exact(node, reference).await?;
            selected
                .definition
                .check_execution(source)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            Ok(selected.execution)
        }
        // Restore admission remains operation-specific in Prepare/Launch. The
        // exact resolver is a cold-boot RPC and would require unused boot files.
        Err(SelectionResponseError::Unsupported) => Ok(source.clone()),
        Err(error) => Err(Status::failed_precondition(error.to_string())),
    }
}

pub(super) async fn resolve_exact(
    node: &mut NodeManagerGrpcClient,
    reference: EnvironmentReference,
) -> Result<SelectedEnvironment, Status> {
    let response = node
        .resolve_execution_environment(ResolveExecutionEnvironmentRequest {
            definition_schema_version: DEFINITION_SCHEMA,
            compatible: None,
            reference: Some(reference.to_string()),
        })
        .await?
        .into_inner();
    let definition = pplx_space_execution_common::environment::EnvironmentDefinition::try_from(
        response.definition.as_ref().ok_or_else(|| {
            Status::failed_precondition("node did not return environment definition")
        })?,
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let actual = pplx_space_common_core::types::ExecutionMetadata::try_from(
        response.execution.as_ref().ok_or_else(|| {
            Status::failed_precondition("node did not acknowledge execution environment")
        })?,
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if definition.reference() != reference || actual.environment() != Some(reference) {
        return Err(Status::failed_precondition(
            "node returned a different or missing environment binding",
        ));
    }
    definition
        .check_execution(&actual)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(
        pplx_space_node_mgr_client::environment_selection::SelectedEnvironment {
            definition,
            execution: actual,
        },
    )
}

#[cfg(test)]
mod tests;
