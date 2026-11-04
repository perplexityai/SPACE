//! Validate compatible-resolution acknowledgement before starting any work.
use pplx_space_common_core::types::ExecutionMetadata;
use pplx_space_execution_common::{
    ExecutionProtoError,
    environment::{EnvironmentBindingError, EnvironmentDecodeError, EnvironmentDefinition},
};

use crate::proto::{
    ExecutionEnvironmentSelection, ResolveExecutionEnvironmentResponse,
    resolve_execution_environment_request::Compatible,
};

#[derive(Debug)]
pub struct SelectedEnvironment {
    pub definition: EnvironmentDefinition,
    pub execution: ExecutionMetadata,
}

#[derive(Debug, thiserror::Error)]
pub enum SelectionResponseError {
    #[error("node does not acknowledge compatible environment selection")]
    Unsupported,
    #[error("node acknowledged a different environment selection intent")]
    Intent,
    #[error("environment resolution response lacks {0}")]
    Missing(&'static str),
    #[error("invalid environment definition: {0}")]
    Definition(#[from] EnvironmentDecodeError),
    #[error("invalid execution metadata: {0}")]
    Execution(#[from] ExecutionProtoError),
    #[error("environment binding mismatch: {0}")]
    Binding(#[from] EnvironmentBindingError),
    #[error("selected execution does not satisfy the source requirements")]
    Requirements,
}

/// An old reader may return a perfectly valid default binding while ignoring
/// the new request. It is not a compatible-selection acknowledgement. Callers
/// may explicitly retain their old exact contract on `Unsupported` only.
pub fn decode(
    request: &Compatible,
    response: ResolveExecutionEnvironmentResponse,
) -> Result<SelectedEnvironment, SelectionResponseError> {
    let expected = match request {
        Compatible::ColdBootSource(_) => ExecutionEnvironmentSelection::ColdBoot,
        Compatible::RestoreSource(_) => ExecutionEnvironmentSelection::Restore,
    };
    if response.accepted_selection == 0 {
        return Err(SelectionResponseError::Unsupported);
    }
    if response.accepted_selection != expected as i32 {
        return Err(SelectionResponseError::Intent);
    }
    let definition = EnvironmentDefinition::try_from(
        response
            .definition
            .as_ref()
            .ok_or(SelectionResponseError::Missing("definition"))?,
    )?;
    let execution = ExecutionMetadata::try_from(
        response
            .execution
            .as_ref()
            .ok_or(SelectionResponseError::Missing("execution"))?,
    )?;
    if execution.environment() != Some(definition.reference()) {
        return Err(SelectionResponseError::Requirements);
    }
    definition.check_execution(&execution)?;
    let (Compatible::ColdBootSource(source) | Compatible::RestoreSource(source)) = request;
    let source = ExecutionMetadata::try_from(source)?;
    definition.check_cold_boot_source(&source)?;
    if matches!(request, Compatible::RestoreSource(_))
        && source.state_format() != execution.state_format()
    {
        return Err(SelectionResponseError::Requirements);
    }
    Ok(SelectedEnvironment {
        definition,
        execution,
    })
}

#[cfg(test)]
mod tests;
