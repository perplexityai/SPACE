use pplx_space_common_core::types::NodeRole;
use tonic::Status;

use crate::proto::PublishedTemplateState;
use claim::{PublishedTemplateName, TemplateClaim};

mod claim;

/// Checked template publication input. Labels retain the legacy inventory
/// grammar, which is intentionally broader than template-creation path names.
#[derive(Debug)]
pub(crate) struct ValidatedTemplateInventory {
    absent_templates: Vec<PublishedTemplateName>,
    claims: Vec<TemplateClaim>,
}

impl ValidatedTemplateInventory {
    pub(crate) fn try_from_proto(
        publisher_role: NodeRole,
        states: &[PublishedTemplateState],
    ) -> Result<Self, Status> {
        let include_template_metadata = publisher_role == NodeRole::Builder;
        let mut absent_templates = Vec::new();
        let mut claims = Vec::new();
        for state in states {
            // Identity rejection precedes optional metadata rejection, even for
            // an absence claim. Do not deduplicate conflicting declarations.
            let template = PublishedTemplateName::parse(&state.template)?;
            if !state.present {
                absent_templates.push(template);
                continue;
            }
            claims.push(TemplateClaim::try_from_proto(
                state,
                include_template_metadata,
            )?);
        }
        Ok(Self {
            absent_templates,
            claims,
        })
    }

    pub(crate) fn into_legacy_rows(self) -> (Vec<String>, Vec<serde_json::Value>) {
        (
            self.absent_templates
                .into_iter()
                .map(PublishedTemplateName::into_string)
                .collect(),
            self.claims.iter().map(TemplateClaim::legacy_row).collect(),
        )
    }
}

#[cfg(test)]
pub(crate) fn template_state_row(
    state: &PublishedTemplateState,
    include_template_metadata: bool,
) -> Result<serde_json::Value, Status> {
    TemplateClaim::try_from_proto(state, include_template_metadata).map(|claim| claim.legacy_row())
}

#[cfg(test)]
mod tests;
