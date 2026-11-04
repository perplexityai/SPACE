//! Capture admission keeps an environment's restore requirement on the node.

use super::super::*;
use pplx_space_common_core::types::EnvironmentReference;
use pplx_space_node_runtime_core::runtime_inventory::ExecutionSelectionMode;

use super::super::node_artifacts::PolicyScope;

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
        // A capture executes nothing, and restore admission is narrowed, so
        // captures obey the startup settings, not the cluster policy: a blocked
        // environment's VMs can still be suspended and resumed elsewhere.
        if let Some(reference) = reference {
            self.allowed_definition_in(
                PolicyScope::Startup,
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
        self.resolve_bound_execution_in(
            PolicyScope::Startup,
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
