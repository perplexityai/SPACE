//! Snapshot profile validation at the node transport boundary. Artifact names
//! are profile-owned; the Firecracker adapter consumes memory and machine-state.

use crate::proto::*;
use crate::runtime_paths::SnapshotFiles;
use pplx_space_common_core::types::NodeSpacePath;
use pplx_space_common_core::types::{EnvironmentReference, SandboxId};
use pplx_space_node_mgr_client::execution_fence::ExecutionFence as ParsedExecutionFence;
use tonic::Status;

#[derive(Debug, thiserror::Error)]
pub enum StateContractError {
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
    type Error = Status;
    fn try_from(request: CaptureVmStateRequest) -> Result<Self, Self::Error> {
        let (snapshot_path, mem_file_path) = firecracker_files(request.artifacts)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let storage = request.storage.unwrap_or_default();
        let environment_reference = request
            .environment_reference
            .map(|reference| reference.parse::<EnvironmentReference>())
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let execution_fence =
            ParsedExecutionFence::try_from(request.execution_fence.as_ref().ok_or_else(|| {
                Status::failed_precondition("snapshot execution fence is required")
            })?)?;
        let sandbox_id =
            SnapshotSandbox::parse(request.sandbox_id, environment_reference.is_some())?;
        Ok(Self {
            sandbox_id,
            environment_reference,
            storage: SnapshotStorage::parse(storage, snapshot_path, mem_file_path)?,
            execution_fence,
        })
    }
}

impl TryFrom<RestoreVmStateRequest> for FirecrackerRestore {
    type Error = Status;
    fn try_from(request: RestoreVmStateRequest) -> Result<Self, Self::Error> {
        let (snapshot_path, mem_file_path) = firecracker_files(request.artifacts)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let storage = request.storage.unwrap_or_default();
        let execution_fence =
            ParsedExecutionFence::try_from(request.execution_fence.as_ref().ok_or_else(|| {
                Status::failed_precondition("snapshot execution fence is required")
            })?)?;
        let sandbox_id = SnapshotSandbox::parse(request.sandbox_id, false)?;
        Ok(Self {
            sandbox_id,
            storage: SnapshotStorage::parse(storage, snapshot_path, mem_file_path)?,
            execution_fence,
            rootfs_rate_limit: request.rootfs_rate_limit,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[derive(Clone, Debug)]
pub(crate) struct FirecrackerCapture {
    pub sandbox_id: SnapshotSandbox,
    pub environment_reference: Option<EnvironmentReference>,
    pub storage: SnapshotStorage,
    pub execution_fence: ParsedExecutionFence,
}
#[derive(Clone, Debug)]
pub(crate) struct FirecrackerRestore {
    pub sandbox_id: SnapshotSandbox,
    pub storage: SnapshotStorage,
    pub execution_fence: ParsedExecutionFence,
    pub rootfs_rate_limit: Option<DriveRateLimit>,
}

/// The parsed identity and its canonical spelling for existing path/store APIs.
#[derive(Clone, Debug)]
pub(crate) struct SnapshotSandbox {
    id: SandboxId,
    key: String,
}

impl SnapshotSandbox {
    fn parse(key: String, bound: bool) -> Result<Self, Status> {
        let (id, key) = if bound {
            let id = key
                .parse::<SandboxId>()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            (id, id.to_string())
        } else {
            let id = crate::service::trusted_paths::canonical_sandbox_id(&key)?;
            (id, key)
        };
        Ok(Self { id, key })
    }

    pub fn id(&self) -> SandboxId {
        self.id
    }
    pub fn as_str(&self) -> &str {
        &self.key
    }
}

/// Encrypted storage ignores plaintext locations. Choose the mode once before
/// ownership admission; filesystem authority is still checked under the fence.
#[derive(Clone, Debug)]
pub(crate) enum SnapshotStorage {
    Plain {
        files: SnapshotFiles,
        directory: Option<NodeSpacePath>,
    },
    Encrypted(NodeSpacePath),
}

impl SnapshotStorage {
    fn parse(storage: VmStateStorage, state: String, memory: String) -> Result<Self, Status> {
        if let Some(mount) = parse_directory(storage.encrypted_volume_mount)? {
            return Ok(Self::Encrypted(mount));
        }
        Ok(Self::Plain {
            files: SnapshotFiles::parse(state, memory)?,
            directory: parse_directory(storage.directory)?,
        })
    }
}

fn parse_directory(value: Option<String>) -> Result<Option<NodeSpacePath>, Status> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            NodeSpacePath::parse(value).map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .transpose()
}

impl std::fmt::Display for SnapshotSandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.key)
    }
}

pub(crate) fn path_str(path: &NodeSpacePath) -> Result<&str, Status> {
    path.as_path()
        .to_str()
        .ok_or_else(|| Status::invalid_argument("snapshot paths must be UTF-8"))
}
