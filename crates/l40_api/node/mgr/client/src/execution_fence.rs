#[allow(deprecated)]
pub use pplx_space_node_mgr_protocol::execution_fence::EXECUTION_AUTHORITY_CAPABILITY;
pub use pplx_space_node_mgr_protocol::execution_fence::{
    EXECUTION_PREFLIGHT_LIMIT, ExecutionFence, InstanceTarget, execution_preflight_result,
};

pub async fn inspect_execution(
    client: &mut crate::NodeManagerGrpcClient,
    sandbox: pplx_space_common_core::types::SandboxId,
) -> Result<Option<ExecutionFence>, tonic::Status> {
    // Store-only read: bound a wedged receiver instead of pinning startup.
    // Process supervision retries startup without changing persisted authority.
    let mut request = tonic::Request::new(crate::proto::InspectSandboxRequest {
        sandbox_id: sandbox.to_string(),
        execution_only: true,
        ..Default::default()
    });
    request.set_timeout(std::time::Duration::from_secs(10));
    client
        .inspect_sandbox(request)
        .await?
        .into_inner()
        .execution_fence
        .as_ref()
        .map(ExecutionFence::try_from)
        .transpose()
}
