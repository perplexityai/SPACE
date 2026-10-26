//! Derive runtime control from recorded execution, never a caller-selected socket.
use super::validation::validate_sandbox_id;
use super::*;
use pplx_space_common_core::types::{ExecutionMetadata, SandboxArtifactMetadata};
use std::io::Read;

impl NodeManagerService {
    pub(super) async fn recorded_execution(
        &self,
        sandbox_id: String,
    ) -> Result<Option<ExecutionMetadata>, Status> {
        validate_sandbox_id(&sandbox_id)?;
        self.off_runtime("read sandbox execution", move || {
            let directory_path = PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
            match std::fs::symlink_metadata(directory_path.join("metadata.json")) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => {
                    return Err(Status::failed_precondition(format!(
                        "inspect sandbox execution: {error}"
                    )));
                }
                Ok(_) => {}
            }
            let directory = trusted_paths::open_directory(&directory_path)?;
            let file = trusted_paths::open_regular_file(&directory, "metadata.json".as_ref())?;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| {
                    Status::failed_precondition(format!("read sandbox execution: {error}"))
                })?;
            if bytes.len() > 1024 * 1024 {
                return Err(Status::failed_precondition(
                    "sandbox execution metadata exceeds 1 MiB",
                ));
            }
            let metadata: SandboxArtifactMetadata =
                serde_json::from_slice(&bytes).map_err(|error| {
                    Status::failed_precondition(format!("invalid sandbox execution: {error}"))
                })?;
            if metadata.sandbox_id.to_string() != sandbox_id {
                return Err(Status::failed_precondition(
                    "sandbox execution metadata identity mismatch",
                ));
            }
            Ok(Some(metadata.execution))
        })
        .await
    }
}

impl NodeManagerService {
    pub(super) async fn standalone_configuration(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
        path: PathBuf,
        source: pplx_space_node_runtime_core::boot::BootSource,
        devices: Option<pplx_space_execution_common::devices::DeviceExecution>,
        device_budget: Option<(
            pplx_space_node_runtime_core::catalog::DeviceResourceLimits,
            u64,
        )>,
    ) -> Result<String, Status> {
        self.off_runtime("read runtime boot configuration", move || {
            let (parent, name) = super::artifact_fs::parent(&path)?;
            let file = trusted_paths::open_regular_file(&parent, name)?;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            if bytes.len() > 1024 * 1024 {
                return Err(Status::invalid_argument(
                    "runtime configuration exceeds 1 MiB",
                ));
            }
            let text =
                String::from_utf8(bytes).map_err(|e| Status::invalid_argument(e.to_string()))?;
            match (devices, device_budget) {
                (Some(devices), Some((resources, reserved_memory_mib))) => {
                    pplx_space_node_runtime_core::boot::with_exclusive_devices(
                        runtime,
                        &text,
                        source,
                        &devices,
                        resources,
                        reserved_memory_mib,
                    )
                    .map_err(|e| Status::invalid_argument(e.to_string()))
                }
                (None, None) => {
                    pplx_space_node_runtime_core::boot::validate_standalone(runtime, &text, source)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(text)
                }
                _ => Err(Status::internal(
                    "device assignment and resource qualification disagree",
                )),
            }
        })
        .await
    }

    pub(super) async fn boot_standalone(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
        socket: PathBuf,
        configuration: &str,
    ) -> Result<(), Status> {
        // Socket creation is readiness, not a replayable boot mutation.
        let start = Instant::now();
        loop {
            match tokio::fs::symlink_metadata(&socket).await {
                Ok(metadata) => {
                    use std::os::unix::fs::FileTypeExt;
                    if !metadata.file_type().is_socket() {
                        return Err(Status::failed_precondition(
                            "runtime control endpoint is not a socket",
                        ));
                    }
                    break;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && start.elapsed() < Duration::from_secs(5) =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "runtime control endpoint: {error}"
                    )));
                }
            }
        }
        let client = pplx_space_node_runtime_core::control::connect(runtime, socket)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        client
            .boot(configuration)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))
    }
}
