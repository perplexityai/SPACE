//! Snapshot profile validation at the node transport boundary. Artifact names
//! are profile-owned; the Firecracker adapter consumes memory and machine-state.

use crate::proto::*;

#[derive(Debug, thiserror::Error)]
pub enum StateContractError {
    #[error("snapshot identities are mutually exclusive")]
    DualSandboxIdentities,
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

fn artifacts(state: String, memory: String) -> Vec<VmStateArtifact> {
    vec![
        VmStateArtifact {
            name: "machine-state".into(),
            relative_path: state,
        },
        VmStateArtifact {
            name: "memory".into(),
            relative_path: memory,
        },
    ]
}

impl TryFrom<CaptureVmStateRequest> for CreateSnapshotRequest {
    type Error = StateContractError;
    fn try_from(request: CaptureVmStateRequest) -> Result<Self, Self::Error> {
        let (snapshot_path, mem_file_path) = firecracker_files(request.artifacts)?;
        let storage = request.storage.unwrap_or_default();
        let (sandbox_id, environment) = match request.environment_reference {
            Some(reference) => (
                String::new(),
                Some(EnvironmentSnapshotInput {
                    sandbox_id: request.sandbox_id,
                    reference,
                }),
            ),
            None => (request.sandbox_id, None),
        };
        Ok(Self {
            sandbox_id,
            environment,
            snapshot_path,
            mem_file_path,
            dest_dir: storage.directory,
            snapshot_volume_mount_dir: storage.encrypted_volume_mount,
            execution_fence: request.execution_fence,
        })
    }
}
impl TryFrom<CreateSnapshotRequest> for CaptureVmStateRequest {
    type Error = StateContractError;

    fn try_from(request: CreateSnapshotRequest) -> Result<Self, Self::Error> {
        if request.environment.is_some() && !request.sandbox_id.is_empty() {
            return Err(StateContractError::DualSandboxIdentities);
        }
        let (sandbox_id, reference) = match request.environment {
            Some(input) => (input.sandbox_id, Some(input.reference)),
            None => (request.sandbox_id, None),
        };
        Ok(Self {
            sandbox_id,
            environment_reference: reference,
            artifacts: artifacts(request.snapshot_path, request.mem_file_path),
            storage: Some(VmStateStorage {
                directory: request.dest_dir,
                encrypted_volume_mount: request.snapshot_volume_mount_dir,
            }),
            execution_fence: request.execution_fence,
        })
    }
}
impl TryFrom<RestoreVmStateRequest> for LoadSnapshotRequest {
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
impl From<LoadSnapshotRequest> for RestoreVmStateRequest {
    fn from(request: LoadSnapshotRequest) -> Self {
        Self {
            sandbox_id: request.sandbox_id,
            artifacts: artifacts(request.snapshot_path, request.mem_file_path),
            storage: Some(VmStateStorage {
                directory: request.source_dir,
                encrypted_volume_mount: request.snapshot_volume_mount_dir,
            }),
            execution_fence: request.execution_fence,
            rootfs_rate_limit: request.rootfs_rate_limit,
        }
    }
}

#[cfg(test)]
mod tests;
