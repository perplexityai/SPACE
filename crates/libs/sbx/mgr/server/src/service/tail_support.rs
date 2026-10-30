use super::*;
pub(super) use crate::sidecar::sidecar_alive;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeArtifactReadError {
    #[error("read runtime metadata {path}: {source}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse runtime metadata {path}: {source}")]
    Parse {
        path: std::path::PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid snapshot runtime metadata {path}: {source}")]
    Snapshot {
        path: std::path::PathBuf,
        #[source]
        source: crate::snapshot_upload::SnapshotRuntimeMetadataError,
    },
    #[error("unsupported host architecture: {0}")]
    Architecture(String),
    #[error(transparent)]
    Execution(#[from] pplx_space_common_core::types::ExecutionMetadataError),
}

pub(super) fn host_architecture()
-> Result<pplx_space_common_core::types::Arch, RuntimeArtifactReadError> {
    std::env::consts::ARCH.parse().map_err(
        |error: pplx_space_common_core::types::ArchParseError| {
            RuntimeArtifactReadError::Architecture(error.to_string())
        },
    )
}

fn sandbox_artifact_metadata(
    sandbox_id: &str,
) -> Result<Option<pplx_space_common_core::types::SandboxArtifactMetadata>, RuntimeArtifactReadError>
{
    sandbox_artifact_metadata_in(std::path::Path::new(CACHE_DIR), sandbox_id)
}

fn sandbox_artifact_metadata_in(
    cache_dir: &std::path::Path,
    sandbox_id: &str,
) -> Result<Option<pplx_space_common_core::types::SandboxArtifactMetadata>, RuntimeArtifactReadError>
{
    let path = cache_dir.join(sandbox_id).join("metadata.json");
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| RuntimeArtifactReadError::Parse { path, source }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(RuntimeArtifactReadError::Read { path, source }),
    }
}

pub(super) fn sandbox_execution_metadata(
    sandbox_id: &str,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, RuntimeArtifactReadError> {
    sandbox_execution_metadata_in(std::path::Path::new(CACHE_DIR), sandbox_id)
}

pub(super) fn sandbox_execution_metadata_in(
    cache_dir: &std::path::Path,
    sandbox_id: &str,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, RuntimeArtifactReadError> {
    match sandbox_artifact_metadata_in(cache_dir, sandbox_id)? {
        Some(metadata) => Ok(metadata.execution),
        None => initial_firecracker_execution_metadata(),
    }
}

pub(super) fn sandbox_snapshot_execution_metadata(
    sandbox_id: &str,
) -> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, RuntimeArtifactReadError> {
    use pplx_space_common_core::types::SnapshotExecutionMetadata;
    match sandbox_artifact_metadata(sandbox_id)? {
        Some(metadata) => Ok(SnapshotExecutionMetadata::new(metadata.execution)?
            .with_cpu_budget(metadata.cpu_budget)
            .with_guest_memory(metadata.guest_memory)),
        None => Ok(SnapshotExecutionMetadata::new(
            initial_firecracker_execution_metadata()?,
        )?),
    }
}

pub(super) fn activation_metadata_json(
    sandbox: &crate::db::SandboxRow,
    cpu_budget: Option<pplx_space_common_core::types::VcpuCount>,
    guest_memory: Option<pplx_space_common_core::types::MemorySize>,
) -> Result<String, Status> {
    use pplx_space_common_core::types::SandboxArtifactMetadata;
    let invalid = |error: String| Status::failed_precondition(error);
    let execution = match sandbox.execution_metadata.clone() {
        Some(execution) => execution,
        None => {
            initial_firecracker_execution_metadata().map_err(|error| invalid(error.to_string()))?
        }
    };
    let mut metadata = SandboxArtifactMetadata::new(
        sandbox
            .id
            .parse::<SandboxId>()
            .map_err(|error| invalid(error.to_string()))?,
        execution,
        sandbox
            .template
            .parse::<pplx_space_common_core::types::TemplateName>()
            .map_err(|error| invalid(error.to_string()))?,
        sandbox
            .template_hash
            .as_deref()
            .map(str::parse::<pplx_space_common_core::types::ContentHash>)
            .transpose()
            .map_err(|error| invalid(error.to_string()))?,
    );
    if !sandbox.guest_image.legacy_transport() {
        metadata = metadata.with_guest_image(sandbox.guest_image);
    }
    metadata.cpu_budget = cpu_budget;
    metadata.guest_memory = guest_memory;
    serde_json::to_string(&metadata)
        .map_err(|error| Status::internal(format!("serialize activation metadata: {error}")))
}

fn initial_firecracker_execution_metadata()
-> Result<pplx_space_common_core::types::ExecutionMetadata, RuntimeArtifactReadError> {
    Ok(
        pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            host_architecture()?,
            vec![],
        ),
    )
}

fn legacy_snapshot_execution_metadata()
-> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, RuntimeArtifactReadError> {
    Ok(
        pplx_space_common_core::types::SnapshotExecutionMetadata::new(
            initial_firecracker_execution_metadata()?,
        )?,
    )
}

pub(super) fn checkpoint_execution_metadata(
    snapshot_dir: &str,
) -> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, RuntimeArtifactReadError> {
    let runtime_path = std::path::PathBuf::from(snapshot_dir).join("runtime.json");
    match std::fs::read(&runtime_path) {
        Ok(bytes) => {
            return serde_json::from_slice::<
                pplx_space_common_core::types::SnapshotExecutionMetadata,
            >(&bytes)
            .map_err(|source| RuntimeArtifactReadError::Parse {
                path: runtime_path,
                source,
            });
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(RuntimeArtifactReadError::Read {
                path: runtime_path,
                source,
            });
        }
    }

    let snapshot_path = std::path::PathBuf::from(snapshot_dir).join("snapshot.json");
    match std::fs::read(&snapshot_path) {
        Ok(bytes) => match serde_json::from_slice::<crate::snapshot_upload::SnapshotMeta>(&bytes) {
            Ok(metadata) => metadata
                .execution_metadata(host_architecture()?)
                .map_err(|source| RuntimeArtifactReadError::Snapshot {
                    path: snapshot_path,
                    source,
                }),
            // Scheduled legacy sidecars contain only name/type/timestamp. A
            // versioned snapshot that fails validation is not such a sidecar.
            Err(source)
                if source.is_data()
                    && serde_json::from_slice::<serde_json::Value>(&bytes)
                        .is_ok_and(|value| value.get("version").is_none()) =>
            {
                legacy_snapshot_execution_metadata()
            }
            Err(source) => Err(RuntimeArtifactReadError::Parse {
                path: snapshot_path,
                source,
            }),
        },
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            legacy_snapshot_execution_metadata()
        }
        Err(source) => Err(RuntimeArtifactReadError::Read {
            path: snapshot_path,
            source,
        }),
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NodeManagerConnectError {
    #[error("node-manager client: {0}")]
    Client(String),
}

pub(super) async fn connect_node(
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<pplx_space_node_mgr_client::NodeManagerGrpcClient, NodeManagerConnectError> {
    node_manager
        .connect()
        .await
        .map_err(|e| NodeManagerConnectError::Client(e.to_string()))
}

pub(super) fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

pub(super) fn system_time_to_proto(time: &SystemTime) -> Option<prost_types::Timestamp> {
    let duration = time.duration_since(std::time::UNIX_EPOCH).ok()?;
    Some(prost_types::Timestamp {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

pub(super) fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests;
