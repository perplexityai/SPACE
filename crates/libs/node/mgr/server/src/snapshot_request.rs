//! Snapshot profile validation at the node transport boundary. Artifact names
//! are profile-owned; the Firecracker adapter consumes memory and machine-state.

use crate::proto::*;
use pplx_space_common_core::types::{
    DigestParseError, EnvironmentReference, SandboxId, SandboxIdParseError,
};

#[derive(Debug, thiserror::Error)]
pub enum StateContractError {
    #[error("invalid snapshot sandbox identity: {0}")]
    InvalidSandboxId(#[from] SandboxIdParseError),
    #[error("invalid snapshot environment reference: {0}")]
    InvalidEnvironmentReference(#[from] DigestParseError),
    #[error("state profile requires artifact {0}")]
    Missing(&'static str),
    #[error("duplicate state artifact {0}")]
    Duplicate(String),
    #[error("state profile does not accept artifact {0}")]
    Unknown(String),
}

fn firecracker_files(
    artifacts: Vec<VmStateArtifact>,
) -> Result<(String, String), StateContractError> {
    let mut state = None;
    let mut memory = None;
    for artifact in artifacts {
        let slot = match artifact.name.as_str() {
            "machine-state" => &mut state,
            "memory" => &mut memory,
            _ => return Err(StateContractError::Unknown(artifact.name)),
        };
        if slot.replace(artifact.relative_path).is_some() {
            return Err(StateContractError::Duplicate(artifact.name));
        }
    }
    Ok((
        state.ok_or(StateContractError::Missing("machine-state"))?,
        memory.ok_or(StateContractError::Missing("memory"))?,
    ))
}

impl TryFrom<CaptureVmStateRequest> for FirecrackerCapture {
    type Error = StateContractError;
    fn try_from(request: CaptureVmStateRequest) -> Result<Self, Self::Error> {
        let (snapshot_path, mem_file_path) = firecracker_files(request.artifacts)?;
        let storage = request.storage.unwrap_or_default();
        let environment_reference = request
            .environment_reference
            .map(|reference| reference.parse())
            .transpose()?;
        // Bound capture has always required a full, canonical sandbox identity.
        let sandbox_id = if environment_reference.is_some() {
            request.sandbox_id.parse::<SandboxId>()?.to_string()
        } else {
            request.sandbox_id
        };
        Ok(Self {
            sandbox_id,
            environment_reference,
            snapshot_path,
            mem_file_path,
            dest_dir: storage.directory,
            snapshot_volume_mount_dir: storage.encrypted_volume_mount,
            execution_fence: request.execution_fence,
        })
    }
}

impl TryFrom<RestoreVmStateRequest> for FirecrackerRestore {
    type Error = StateContractError;
    fn try_from(request: RestoreVmStateRequest) -> Result<Self, Self::Error> {
        let (snapshot_path, mem_file_path) = firecracker_files(request.artifacts)?;
        let storage = request.storage.unwrap_or_default();
        Ok(Self {
            sandbox_id: request.sandbox_id,
            snapshot_path,
            mem_file_path,
            source_dir: storage.directory,
            snapshot_volume_mount_dir: storage.encrypted_volume_mount,
            execution_fence: request.execution_fence,
            rootfs_rate_limit: request.rootfs_rate_limit,
        })
    }
}

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FirecrackerCapture {
    pub sandbox_id: String,
    pub environment_reference: Option<EnvironmentReference>,
    pub snapshot_path: String,
    pub mem_file_path: String,
    pub dest_dir: Option<String>,
    pub snapshot_volume_mount_dir: Option<String>,
    pub execution_fence: Option<ExecutionFence>,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FirecrackerRestore {
    pub sandbox_id: String,
    pub snapshot_path: String,
    pub mem_file_path: String,
    pub source_dir: Option<String>,
    pub snapshot_volume_mount_dir: Option<String>,
    pub execution_fence: Option<ExecutionFence>,
    pub rootfs_rate_limit: Option<DriveRateLimit>,
}
