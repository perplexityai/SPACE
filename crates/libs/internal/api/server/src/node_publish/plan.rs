//! Pure publication planning. Claims describe node evidence, never final rows.

use std::time::Duration;

use crate::OriginNode;

use super::diagnostics::InvalidCensusIdentity;
use super::validate::{
    ValidatedPublish,
    census::{SandboxCensus, TemplateCensus},
    delete_completions::ValidatedDeleteCompletions,
    execution_observation::{ObservationError, ValidatedReport},
    node_status::ValidatedNodeStatus,
    pending_backup::PendingBackupReport,
    templates::ValidatedTemplateInventory,
};
use sandboxes::SandboxPlan;

pub(crate) mod sandboxes;

/// Ownership of the checked claims moves into the plan without payload clones.
/// Lifecycle guards, row locks and generated identifiers belong to the store.
#[derive(Debug)]
pub(crate) struct PublishPlan {
    pub(crate) origin: OriginNode,
    pub(crate) reported_window: Option<Duration>,
    pub(crate) node_status: Option<ValidatedNodeStatus>,
    pub(crate) sandbox_census: SandboxCensus,
    pub(crate) census_diagnostics: Vec<InvalidCensusIdentity>,
    pub(crate) template_census: Option<TemplateCensus>,
    pub(crate) templates: Result<ValidatedTemplateInventory, tonic::Status>,
    pub(crate) sandboxes: SandboxPlan,
    pub(crate) execution_observation: Result<Option<ValidatedReport>, ObservationError>,
    pub(crate) pending_local_backups: PendingBackupReport,
    pub(crate) delete_completions: ValidatedDeleteCompletions,
    pub(crate) delete_completions_received: usize,
}

impl From<ValidatedPublish> for PublishPlan {
    fn from(publish: ValidatedPublish) -> Self {
        Self {
            origin: publish.origin,
            reported_window: publish.reported_window,
            node_status: publish.node_status,
            sandbox_census: publish.sandbox_census,
            census_diagnostics: publish.census_diagnostics,
            template_census: publish.template_census,
            templates: publish.templates,
            sandboxes: publish.sandboxes.into(),
            execution_observation: publish.execution_observation,
            pending_local_backups: publish.pending_local_backups,
            delete_completions: publish.delete_completions,
            delete_completions_received: publish.delete_completions_received,
        }
    }
}
