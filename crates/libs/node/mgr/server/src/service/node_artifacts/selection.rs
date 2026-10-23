//! Read-only compatible selection; launches continue to admit exact identities.
use pplx_space_common_core::types::ExecutionMetadata;
use pplx_space_node_runtime_core::selection::{self, CompatibleSelection};
use tonic::Status;

use super::NodeManagerService;
use crate::proto::{
    ExecutionEnvironmentSelection, ResolveExecutionEnvironmentResponse,
    resolve_execution_environment_request::Compatible,
};

impl NodeManagerService {
    pub(super) async fn resolve_compatible_environment(
        &self,
        request: Compatible,
    ) -> Result<ResolveExecutionEnvironmentResponse, Status> {
        let (Compatible::ColdBootSource(execution) | Compatible::RestoreSource(execution)) =
            &request;
        let source = ExecutionMetadata::try_from(execution)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (intent, acknowledgement) = match request {
            Compatible::ColdBootSource(_) => (
                CompatibleSelection::ColdBoot { source: &source },
                ExecutionEnvironmentSelection::ColdBoot,
            ),
            Compatible::RestoreSource(_) => (
                CompatibleSelection::Restore { source: &source },
                ExecutionEnvironmentSelection::Restore,
            ),
        };
        let inventory = self.runtime_inventory().await?;
        let (definition, selected) = selection::select(&inventory, &self.execution_policy, intent)
            .map_err(|error| match error {
                selection::SelectionError::Unavailable(error) => error.into_status(),
                error => Status::failed_precondition(error.to_string()),
            })?;
        let source = intent.source();
        if source.environment() != Some(definition.reference()) {
            pplx_space_metrics::node_manager::execution_consumer_fallback();
            tracing::warn!(
                source_environment = ?source.environment(),
                consumer_environment = %definition.reference(),
                mode = ?intent.mode(),
                source_allowed = source.environment().is_some_and(|reference| self.execution_policy.allows(reference)),
                "selected compatible consumer after exact source was absent, revoked or permanently unavailable"
            );
        }
        Ok(ResolveExecutionEnvironmentResponse {
            definition: Some(definition.into()),
            execution: Some((&selected.execution).into()),
            accepted_selection: acknowledgement as i32,
        })
    }
}
