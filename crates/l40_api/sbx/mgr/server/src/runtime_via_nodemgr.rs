use pplx_space_common_core::types::SandboxId;
use pplx_space_node_mgr_client::proto::{GetVmStateRequest, VmState as WireVmState};

use pplx_space_node_runtime_core::vm_state::VmState;
use std::time::Duration;

use crate::node_manager_transport::NodeManagerConnector;

const VM_STATE_RPC_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeStateReadError {
    // Peer messages may include untrusted VM responses. Keep diagnostics finite.
    #[error("node-manager runtime state read failed (status {:?})", .0.code())]
    Rpc(#[from] tonic::Status),
    #[error("node-manager returned unsupported VM state {0}")]
    UnsupportedState(i32),
    #[error("node-manager runtime state read timed out after {timeout_ms} ms")]
    Timeout { timeout_ms: u64 },
}

async fn get_vm_state_inner(
    sandbox_id: SandboxId,
    node_manager: &NodeManagerConnector,
) -> Result<VmState, RuntimeStateReadError> {
    // Reuse the reconnecting channel; the outer read deadline also covers
    // channel readiness after a node-manager restart.
    let mut client = node_manager.lazy();
    let response = client
        .get_vm_state(GetVmStateRequest {
            sandbox_id: sandbox_id.to_string(),
        })
        .await?
        .into_inner();
    match response.state() {
        WireVmState::NotStarted => Ok(VmState::NotStarted),
        WireVmState::Running => Ok(VmState::Running),
        WireVmState::Paused => Ok(VmState::Paused),
        WireVmState::Shutdown => Ok(VmState::Shutdown),
        WireVmState::Unspecified => Err(RuntimeStateReadError::UnsupportedState(response.state)),
    }
}

async fn bounded_vm_state_read<F>(
    read: F,
    timeout: Duration,
) -> Result<VmState, RuntimeStateReadError>
where
    F: std::future::Future<Output = Result<VmState, RuntimeStateReadError>>,
{
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| RuntimeStateReadError::Timeout {
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        })?
}

pub(crate) async fn get_vm_state(
    sandbox_id: SandboxId,
    node_manager: &NodeManagerConnector,
) -> Result<VmState, RuntimeStateReadError> {
    bounded_vm_state_read(
        get_vm_state_inner(sandbox_id, node_manager),
        VM_STATE_RPC_TIMEOUT,
    )
    .await
}

#[cfg(test)]
mod tests;
