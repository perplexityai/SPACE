//! Pure conversions of node-supplied publication evidence.
//!
//! Rejections retain their input-validation order. No database row, clock, or
//! generated identifier participates in deciding what the node has claimed.

use std::time::Duration;

use pplx_space_node_api_client::node_status_freshness::reported_freshness;
use tonic::Status;

use crate::OriginNode;
use crate::proto::PublishNodeStateRequest;

use super::diagnostics::InvalidCensusIdentity;
use census::{SandboxCensus, TemplateCensus};
use delete_completions::ValidatedDeleteCompletions;
use execution_observation::ValidatedReport;
use node_role::PublicationRoleIntent;
use node_status::ValidatedNodeStatus;
use pending_backup::PendingBackupReport;
use sandbox_inventory::ValidatedSandboxInventory;
use templates::ValidatedTemplateInventory;

pub(crate) mod census;
pub(crate) mod delete_completions;
pub(crate) mod execution_inventory;
pub(crate) mod execution_observation;
pub(crate) mod failure_episode;
pub(crate) mod fields;
pub(crate) mod node_role;
pub(crate) mod node_status;
pub(crate) mod pending_backup;
pub(crate) mod quarantine;
pub(crate) mod quarantine_claim;
pub(crate) mod receipts;
pub(crate) mod sandbox_identity;
pub(crate) mod sandbox_inventory;
pub(crate) mod sandbox_metadata;
pub(crate) mod sandbox_observation;
pub(crate) mod sandbox_status;
pub(crate) mod snapshots;
pub(crate) mod templates;

/// Parsed publication evidence, not a prediction of guarded database changes.
#[derive(Debug)]
pub(crate) struct ValidatedPublish {
    pub(crate) origin: OriginNode,
    pub(crate) reported_window: Option<Duration>,
    pub(crate) node_status: Option<ValidatedNodeStatus>,
    pub(crate) sandbox_census: SandboxCensus,
    pub(crate) census_diagnostics: Vec<InvalidCensusIdentity>,
    pub(crate) template_census: Option<TemplateCensus>,
    pub(crate) templates: ValidatedTemplateInventory,
    pub(crate) sandboxes: ValidatedSandboxInventory,
    pub(crate) execution_observation: Option<ValidatedReport>,
    pub(crate) pending_local_backups: PendingBackupReport,
    pub(crate) delete_completions: ValidatedDeleteCompletions,
    pub(crate) delete_completions_received: usize,
}

impl ValidatedPublish {
    pub(crate) fn try_from_proto(
        origin: OriginNode,
        request: PublishNodeStateRequest,
    ) -> Result<Self, Status> {
        PublicationRoleIntent::try_from_proto(&request)?
            .validate_authenticated_role(origin.role)?;
        let reported_window = reported_freshness(
            request
                .node_status
                .as_ref()
                .and_then(|status| status.node_ctl.as_ref()),
        );
        let node_status = request
            .node_status
            .map(ValidatedNodeStatus::try_from_proto)
            .transpose()?;
        let (sandbox_census, census_diagnostics) =
            SandboxCensus::from_proto(&request.sandboxes, request.sandbox_inventory_complete);
        let template_census = request
            .template_inventory_complete
            .then(|| TemplateCensus::from_proto(&request.templates));
        // Envelope rejection precedes template rejection; inventory-content
        // rejection follows it. Sandbox rows degrade rather than reject the RPC.
        let report = execution_observation::decode(
            request.execution_observation.as_ref(),
            request.execution_inventory.as_ref(),
        )?;
        let templates =
            ValidatedTemplateInventory::try_from_proto(origin.role, &request.templates)?;
        let sandboxes = ValidatedSandboxInventory::from_proto(&request.sandboxes);
        let execution_observation = report.map(ValidatedReport::try_from_report).transpose()?;
        let pending_local_backups =
            PendingBackupReport::from_proto(request.pending_local_backups.as_ref());
        let delete_completions_received = request.auto_lifecycle_delete_completions.len();
        let delete_completions =
            ValidatedDeleteCompletions::from_proto(&request.auto_lifecycle_delete_completions);
        Ok(Self {
            origin,
            reported_window,
            node_status,
            sandbox_census,
            census_diagnostics,
            template_census,
            templates,
            sandboxes,
            execution_observation,
            pending_local_backups,
            delete_completions,
            delete_completions_received,
        })
    }
}

#[cfg(test)]
mod tests;
