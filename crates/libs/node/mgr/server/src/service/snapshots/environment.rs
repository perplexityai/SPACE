//! Capture admission keeps an environment's restore requirement on the node.

use super::super::*;
use pplx_space_common_core::types::EnvironmentReference;
use pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode;

pub(super) fn normalize(
    request: &mut crate::snapshot_request::FirecrackerCapture,
) -> Result<Option<EnvironmentReference>, Status> {
    let Some(input) = request.environment.as_ref() else {
        return Ok(None);
    };
    if !request.sandbox_id.is_empty() {
        return Err(Status::invalid_argument(
            "snapshot identities are mutually exclusive",
        ));
    }
    let sandbox_id = input
        .sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let reference = input
        .reference
        .parse::<EnvironmentReference>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    request.sandbox_id = sandbox_id.to_string();
    Ok(Some(reference))
}

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
        let execution = self.recorded_execution(sandbox_id.clone()).await?;
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
