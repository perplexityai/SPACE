//! Derive runtime control from recorded execution, never a caller-selected socket.
use super::validation::validate_sandbox_id;
use super::*;
use pplx_space_common_core::types::{ExecutionMetadata, SandboxArtifactMetadata};
use std::io::Read;

pub(super) fn recorded_execution_blocking(
    sandbox_id: &str,
) -> Result<Option<ExecutionMetadata>, Status> {
    recorded_execution_in(Path::new(CACHE_DIR), sandbox_id)
}

fn recorded_execution_in(
    cache: &Path,
    sandbox_id: &str,
) -> Result<Option<ExecutionMetadata>, Status> {
    Ok(recorded_artifact_in(cache, sandbox_id)?.map(|metadata| metadata.execution))
}

fn recorded_artifact_in(
    cache: &Path,
    sandbox_id: &str,
) -> Result<Option<SandboxArtifactMetadata>, Status> {
    validate_sandbox_id(sandbox_id)?;
    let directory_path = cache.join(sandbox_id);
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
        .map_err(|error| Status::failed_precondition(format!("read sandbox execution: {error}")))?;
    if bytes.len() > 1024 * 1024 {
        return Err(Status::failed_precondition(
            "sandbox execution metadata exceeds 1 MiB",
        ));
    }
    let metadata: SandboxArtifactMetadata = serde_json::from_slice(&bytes).map_err(|error| {
        Status::failed_precondition(format!("invalid sandbox execution: {error}"))
    })?;
    if metadata.sandbox_id.to_string() != sandbox_id {
        return Err(Status::failed_precondition(
            "sandbox execution metadata identity mismatch",
        ));
    }
    Ok(Some(metadata))
}

fn recorded_guest_network_in(
    cache: &Path,
    sandbox_id: &str,
) -> Result<pplx_space_common_core::types::guest_image::GuestNetwork, Status> {
    // Existing network intent survives catalog retirement. Parsing, file and
    // sandbox identity checks remain mandatory; only absence means legacy.
    Ok(recorded_artifact_in(cache, sandbox_id)?
        .map(|metadata| metadata.guest_image().network())
        .unwrap_or_default())
}

impl NodeManagerService {
    pub(super) async fn recorded_guest_network(
        &self,
        sandbox_id: String,
    ) -> Result<pplx_space_common_core::types::guest_image::GuestNetwork, Status> {
        self.off_runtime("read recorded guest network", move || {
            recorded_guest_network_in(Path::new(CACHE_DIR), &sandbox_id)
        })
        .await
    }
    pub(super) async fn recorded_guest_profile(
        &self,
        sandbox_id: String,
    ) -> Result<Option<pplx_space_node_runtime_core::boot::GuestProfile>, Status> {
        self.off_runtime("read guest profile", move || {
            recorded_artifact_in(Path::new(CACHE_DIR), &sandbox_id)?
                .map(|metadata| {
                    pplx_space_node_runtime_core::boot::GuestProfile::resolve(
                        &metadata.execution,
                        metadata.guest_image(),
                    )
                    .map_err(|error| Status::failed_precondition(error.to_string()))
                })
                .transpose()
        })
        .await
    }
    pub(super) async fn recorded_execution(
        &self,
        sandbox_id: String,
    ) -> Result<Option<ExecutionMetadata>, Status> {
        validate_sandbox_id(&sandbox_id)?;
        self.off_runtime("read sandbox execution", move || {
            recorded_execution_blocking(&sandbox_id)
        })
        .await
    }
}

impl NodeManagerService {
    pub(super) async fn pin_runtime_owner(
        &self,
        sandbox_id: &str,
    ) -> Result<pplx_space_node_runtime_core::launch::ProcessObserver, Status> {
        let slot = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::failed_precondition("runtime has no node lease"))?;
        let id = sandbox_id.to_owned();
        self.off_runtime("pin runtime control owner", move || {
            let pid = super::runtime::registered_firecracker_pid(Path::new(RUN_DIR), &id)?;
            let handle = crate::launch::ProcessHandle::open(pid.get().get())
                .map_err(|e| Status::unavailable(e.to_string()))?;
            // Re-observe cgroup registration after pinning the numeric PID.
            let registered = super::runtime::registered_firecracker_pid(Path::new(RUN_DIR), &id)?;
            if registered != pid
                || handle
                    .credentials_match(slot.sandbox_uid, slot.sandbox_uid)
                    .map_err(|e| Status::unavailable(e.to_string()))?
                    != Some(true)
                || handle
                    .program_matches("cloud-hypervisor")
                    .map_err(|e| Status::unavailable(e.to_string()))?
                    != Some(true)
            {
                return Err(Status::failed_precondition("runtime control owner changed"));
            }
            Ok(handle.into())
        })
        .await
    }

    pub(super) async fn runtime_control(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
        sandbox_id: &str,
    ) -> Result<Box<dyn pplx_space_node_runtime_core::control::RuntimeControl>, Status> {
        let socket = pplx_space_node_runtime_core::control::socket_path(
            runtime,
            sandbox_id
                .parse()
                .map_err(|e| Status::invalid_argument(format!("sandbox id: {e}")))?,
        )
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let peer = if runtime == pplx_space_common_core::types::RuntimeKind::CloudHypervisor {
            Some(self.pin_runtime_owner(sandbox_id).await?)
        } else {
            None
        };
        pplx_space_node_runtime_core::control::connect(runtime, socket, peer)
            .map_err(|e| Status::failed_precondition(e.to_string()))
    }

    pub(super) async fn standalone_configuration(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
        path: PathBuf,
        source: pplx_space_node_runtime_core::boot::BootSource,
    ) -> Result<pplx_space_node_runtime_core::boot::StandaloneBoot, Status> {
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
            pplx_space_node_runtime_core::boot::validate_standalone(runtime, &text, source)
                .map_err(|e| Status::invalid_argument(e.to_string()))
        })
        .await
    }

    pub(super) async fn boot_standalone(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
        sandbox_id: &str,
        socket: PathBuf,
        configuration: &pplx_space_node_runtime_core::boot::StandaloneBoot,
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
        let client = self.runtime_control(runtime, sandbox_id).await?;
        client
            .boot(configuration)
            .await
            .map_err(|e| operation_error("runtime boot", e))
    }
}

#[cfg(test)]
mod tests;

/// A timed-out mutation is uncertain, not an admission refusal.
pub(super) fn operation_error(
    operation: &str,
    error: pplx_space_node_runtime_core::control::ControlError,
) -> Status {
    let message = format!("{operation}: {error}");
    if error.is_timeout() {
        Status::deadline_exceeded(format!("{message}; outcome unknown, reconcile execution"))
    } else {
        Status::internal(message)
    }
}
