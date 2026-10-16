use std::path::PathBuf;

use pplx_space_common_core::types::{
    ExecutionGeneration, LinuxPid, LinuxUid, SandboxActivationAttemptId, SandboxId, VsockCid,
};
use pplx_space_node_mgr_client::proto::{
    ProcessEndpointIdentity as WireProcessEndpointIdentity,
    SandboxEndpointProof as WireSandboxEndpointProof,
    VsockEndpointIdentity as WireVsockEndpointIdentity,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProcessActivationIdentity {
    pub pid: LinuxPid,
    pub start_time_ticks: u64,
    pub executable_device: u64,
    pub executable_inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VsockActivationIdentity {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub owner_uid: LinuxUid,
    pub owner_gid: u32,
    pub guest_cid: VsockCid,
    pub runtime_fd: u32,
    pub runtime_socket_inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpacedEndpointProof {
    pub sandbox_id: SandboxId,
    pub execution_generation: ExecutionGeneration,
    pub activation_attempt_id: SandboxActivationAttemptId,
    pub spaced_host: ProcessActivationIdentity,
    pub runtime_process: ProcessActivationIdentity,
    pub vsock: VsockActivationIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpacedEndpointRuntimeIdentity {
    pub sandbox_id: SandboxId,
    pub execution_generation: ExecutionGeneration,
    pub activation_attempt_id: SandboxActivationAttemptId,
    pub spaced_host_pid: LinuxPid,
    pub runtime_pid: LinuxPid,
    pub sandbox_uid: LinuxUid,
    pub guest_cid: VsockCid,
    pub vsock_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum SpacedEndpointProofError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Mismatch(String),
}

impl SpacedEndpointProof {
    pub fn from_node_proof(
        runtime: &SpacedEndpointRuntimeIdentity,
        proof: WireSandboxEndpointProof,
    ) -> Result<Self, SpacedEndpointProofError> {
        let spaced_host =
            process_from_wire(proof.spaced_host, runtime.spaced_host_pid, "spaced-host")?;
        let runtime_process = process_from_wire(
            proof.runtime_process,
            runtime.runtime_pid,
            "sandbox runtime",
        )?;
        let vsock = proof.vsock.ok_or_else(|| {
            SpacedEndpointProofError::Invalid("vsock proof is missing".to_string())
        })?;
        if proof.sandbox_id != runtime.sandbox_id.to_string()
            || vsock.guest_cid != runtime.guest_cid.get().get()
        {
            return Err(SpacedEndpointProofError::Mismatch(
                "node endpoint proof does not match the active sandbox runtime".to_string(),
            ));
        }
        let proof = Self {
            sandbox_id: runtime.sandbox_id,
            execution_generation: runtime.execution_generation,
            activation_attempt_id: runtime.activation_attempt_id,
            spaced_host,
            runtime_process,
            vsock: VsockActivationIdentity {
                path: vsock.path.into(),
                device: vsock.device,
                inode: vsock.inode,
                owner_uid: LinuxUid::new(vsock.owner_uid),
                owner_gid: vsock.owner_gid,
                guest_cid: VsockCid::try_from_u32(vsock.guest_cid).map_err(|error| {
                    SpacedEndpointProofError::Invalid(format!("invalid guest CID: {error}"))
                })?,
                runtime_fd: vsock.runtime_fd,
                runtime_socket_inode: vsock.runtime_socket_inode,
            },
        };
        proof.validate_runtime(runtime)?;
        Ok(proof)
    }

    pub fn validate_runtime(
        &self,
        runtime: &SpacedEndpointRuntimeIdentity,
    ) -> Result<(), SpacedEndpointProofError> {
        if self.sandbox_id != runtime.sandbox_id
            || self.execution_generation != runtime.execution_generation
            || self.activation_attempt_id != runtime.activation_attempt_id
            || self.spaced_host.pid != runtime.spaced_host_pid
            || self.runtime_process.pid != runtime.runtime_pid
            || self.vsock.path != runtime.vsock_path
            || self.vsock.owner_uid != runtime.sandbox_uid
            || self.vsock.guest_cid != runtime.guest_cid
        {
            return Err(SpacedEndpointProofError::Mismatch(
                "persisted endpoint proof does not match the active sandbox execution".to_string(),
            ));
        }
        Ok(())
    }

    pub fn node_proof(&self) -> WireSandboxEndpointProof {
        WireSandboxEndpointProof {
            sandbox_id: self.sandbox_id.to_string(),
            spaced_host: Some(process_to_wire(&self.spaced_host)),
            runtime_process: Some(process_to_wire(&self.runtime_process)),
            vsock: Some(WireVsockEndpointIdentity {
                path: self.vsock.path.to_string_lossy().into_owned(),
                device: self.vsock.device,
                inode: self.vsock.inode,
                owner_uid: self.vsock.owner_uid.get(),
                owner_gid: self.vsock.owner_gid,
                guest_cid: self.vsock.guest_cid.get().get(),
                runtime_fd: self.vsock.runtime_fd,
                runtime_socket_inode: self.vsock.runtime_socket_inode,
            }),
        }
    }
}

fn process_from_wire(
    proof: Option<WireProcessEndpointIdentity>,
    expected_pid: LinuxPid,
    name: &str,
) -> Result<ProcessActivationIdentity, SpacedEndpointProofError> {
    let proof = proof.ok_or_else(|| {
        SpacedEndpointProofError::Invalid(format!("{name} process proof is missing"))
    })?;
    let pid = LinuxPid::try_from_u32(proof.pid).map_err(|error| {
        SpacedEndpointProofError::Invalid(format!("invalid {name} pid: {error}"))
    })?;
    if pid != expected_pid {
        return Err(SpacedEndpointProofError::Mismatch(format!(
            "{name} pid does not match the active sandbox runtime"
        )));
    }
    Ok(ProcessActivationIdentity {
        pid,
        start_time_ticks: proof.start_time_ticks,
        executable_device: proof.executable_device,
        executable_inode: proof.executable_inode,
    })
}

fn process_to_wire(proof: &ProcessActivationIdentity) -> WireProcessEndpointIdentity {
    WireProcessEndpointIdentity {
        pid: proof.pid.get().get(),
        start_time_ticks: proof.start_time_ticks,
        executable_device: proof.executable_device,
        executable_inode: proof.executable_inode,
    }
}

#[cfg(test)]
mod tests;
