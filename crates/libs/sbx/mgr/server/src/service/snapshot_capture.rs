//! Environment-bound capture uses an identity old nodes cannot execute.

use pplx_space_common_core::types::ExecutionMetadata;
use pplx_space_node_mgr_client::proto::{CreateSnapshotRequest, EnvironmentSnapshotInput};

pub(crate) fn bind(
    mut request: CreateSnapshotRequest,
    execution: Option<&ExecutionMetadata>,
) -> CreateSnapshotRequest {
    if let Some(execution) = execution
        && super::validation::is_non_default_execution(execution)
        && let Some(reference) = execution.environment()
    {
        request.environment = Some(EnvironmentSnapshotInput {
            sandbox_id: std::mem::take(&mut request.sandbox_id),
            reference: reference.to_string(),
        });
    }
    request
}

#[cfg(test)]
mod tests;
