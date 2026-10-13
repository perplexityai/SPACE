use pplx_space_common_core::types::SandboxId;
use pplx_space_node_mgr_client::proto::{
    FirecrackerVmState as WireFirecrackerVmState, GetFirecrackerVmStateRequest,
};
use pplx_space_node_runtime_core::fc_api::FirecrackerVmState;
use std::time::Duration;

use crate::node_manager_transport::NodeManagerConnector;

const FIRECRACKER_VM_STATE_RPC_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug, thiserror::Error)]
pub(crate) enum FirecrackerStateReadError {
    // Peer messages may include untrusted VM responses. Keep diagnostics finite.
    #[error("node-manager Firecracker state read failed (status {:?})", .0.code())]
    Rpc(#[from] tonic::Status),
    #[error("node-manager returned an unspecified Firecracker VM state")]
    UnspecifiedState,
    #[error("node-manager Firecracker state read timed out after {timeout_ms} ms")]
    Timeout { timeout_ms: u64 },
}

async fn get_firecracker_vm_state_inner(
    sandbox_id: SandboxId,
    node_manager: &NodeManagerConnector,
) -> Result<FirecrackerVmState, FirecrackerStateReadError> {
    // Reuse the reconnecting channel; the outer read deadline also covers
    // channel readiness after a node-manager restart.
    let mut client = node_manager.lazy();
    let response = client
        .get_firecracker_vm_state(GetFirecrackerVmStateRequest {
            sandbox_id: sandbox_id.to_string(),
        })
        .await?
        .into_inner();
    match response.state() {
        WireFirecrackerVmState::NotStarted => Ok(FirecrackerVmState::NotStarted),
        WireFirecrackerVmState::Running => Ok(FirecrackerVmState::Running),
        WireFirecrackerVmState::Paused => Ok(FirecrackerVmState::Paused),
        WireFirecrackerVmState::Unspecified => Err(FirecrackerStateReadError::UnspecifiedState),
    }
}

async fn bounded_firecracker_vm_state_read<F>(
    read: F,
    timeout: Duration,
) -> Result<FirecrackerVmState, FirecrackerStateReadError>
where
    F: std::future::Future<Output = Result<FirecrackerVmState, FirecrackerStateReadError>>,
{
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| FirecrackerStateReadError::Timeout {
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        })?
}

pub(crate) async fn get_firecracker_vm_state(
    sandbox_id: SandboxId,
    node_manager: &NodeManagerConnector,
) -> Result<FirecrackerVmState, FirecrackerStateReadError> {
    bounded_firecracker_vm_state_read(
        get_firecracker_vm_state_inner(sandbox_id, node_manager),
        FIRECRACKER_VM_STATE_RPC_TIMEOUT,
    )
    .await
}

#[cfg(test)]
mod tests;
