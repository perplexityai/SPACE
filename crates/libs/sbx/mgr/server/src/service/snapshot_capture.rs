//! Capture preserves the producer binding through generic and transitional transports.

use pplx_space_common_core::types::ExecutionMetadata;
use pplx_space_node_mgr_client::proto::{CaptureVmStateRequest, VmStateArtifact};

pub(crate) fn bind(
    mut request: CaptureVmStateRequest,
    execution: Option<&ExecutionMetadata>,
) -> CaptureVmStateRequest {
    if let Some(execution) = execution
        && super::validation::is_non_default_execution(execution)
        && let Some(reference) = execution.environment()
    {
        request.environment_reference = Some(reference.to_string());
    }
    request
}

/// Unbound historical snapshots predate runtime metadata and belong to FC.
/// Bound snapshots must use their recorded runtime for the bridge fallback gate.
pub(crate) fn runtime(
    execution: Option<&ExecutionMetadata>,
) -> pplx_space_execution_common::proto::RuntimeKind {
    execution.map_or(
        pplx_space_execution_common::proto::RuntimeKind::Firecracker,
        |execution| execution.runtime().kind.into(),
    )
}

#[cfg(test)]
mod tests;

/// The existing state profile names; file paths are checked by the node adapter.
pub(crate) fn artifacts() -> Vec<VmStateArtifact> {
    vec![
        VmStateArtifact {
            name: "machine-state".into(),
            relative_path: "vmstate".into(),
        },
        VmStateArtifact {
            name: "memory".into(),
            relative_path: "memory".into(),
        },
    ]
}
