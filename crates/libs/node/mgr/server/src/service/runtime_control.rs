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
