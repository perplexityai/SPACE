//! Capture admission keeps an environment's restore requirement on the node.

use super::super::*;
use pplx_space_common_core::types::{EnvironmentReference, SandboxArtifactMetadata};
use pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode;
use std::io::Read;

fn validate_binding(
    requested: Option<EnvironmentReference>,
    actual: EnvironmentReference,
    initial: EnvironmentReference,
) -> Result<(), Status> {
    match requested {
        Some(expected) if expected != actual => Err(Status::failed_precondition(
            "snapshot environment differs from the running sandbox",
        )),
        None if actual != initial => Err(Status::failed_precondition(
            "non-default capture requires an environment-bound identity",
        )),
        _ => Ok(()),
    }
}

impl NodeManagerService {
    pub(super) async fn verify_snapshot_environment(
        &self,
        sandbox_id: String,
        reference: Option<EnvironmentReference>,
    ) -> Result<(), Status> {
        if let Some(reference) = reference {
            self.allowed_definition(
                Some(reference),
                ExecutionSelectionMode::PersistedStateRestore,
            )?;
        }
        let execution = self
            .off_runtime("inspect snapshot execution", move || {
                let directory_path = PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
                match std::fs::symlink_metadata(directory_path.join("metadata.json")) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(error) => {
                        return Err(Status::failed_precondition(format!(
                            "inspect snapshot execution: {error}"
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
                        Status::failed_precondition(format!("read snapshot execution: {error}"))
                    })?;
                if bytes.len() > 1024 * 1024 {
                    return Err(Status::failed_precondition(
                        "snapshot execution metadata exceeds 1 MiB",
                    ));
                }
                let metadata: SandboxArtifactMetadata =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        Status::failed_precondition(format!("invalid snapshot execution: {error}"))
                    })?;
                if metadata.sandbox_id.to_string() != sandbox_id {
                    return Err(Status::failed_precondition(
                        "snapshot environment differs from the running sandbox",
                    ));
                }
                Ok(Some(metadata.execution))
            })
            .await?;
        let Some(execution) = execution else {
            return if reference.is_some() {
                Err(Status::failed_precondition(
                    "bound capture requires sandbox execution metadata",
                ))
            } else {
                Ok(())
            };
        };
        if let Some(actual) = execution.environment() {
            let initial = super::super::node_artifacts::initial_environment()?
                .definition()
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            validate_binding(reference, actual, initial.reference())?;
        } else {
            return if reference.is_some() {
                Err(Status::failed_precondition(
                    "bound capture requires recorded environment identity",
                ))
            } else {
                Ok(())
            };
        }
        self.resolve_bound_execution(
            Some(&execution),
            execution.runtime().kind,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
