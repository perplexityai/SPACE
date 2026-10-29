use pplx_space_common_core::types::NodeRole;
use tonic::Status;

use crate::proto::PublishedTemplateState;
use claim::TemplateClaim;
pub(in crate::node_publish) use claim::TemplateComponentSet;
pub(crate) use claim::{PublishedTemplateName, TemplateInventoryClaim, TemplatePolicyClaim};

mod claim;

/// Checked template publication input. Labels retain the legacy inventory
/// grammar, which is intentionally broader than template-creation path names.
#[derive(Debug)]
pub(crate) struct ValidatedTemplateInventory {
    absent_templates: Vec<PublishedTemplateName>,
    claims: Vec<TemplateClaim>,
}

impl ValidatedTemplateInventory {
    pub(in crate::node_publish) fn into_inventory(
        self,
    ) -> (Vec<PublishedTemplateName>, Vec<TemplateInventoryClaim>) {
        (
            self.absent_templates,
            self.claims.into_iter().map(Into::into).collect(),
        )
    }

    pub(in crate::node_publish) fn take_policies(
        &mut self,
    ) -> impl Iterator<Item = TemplatePolicyClaim> + '_ {
        self.claims
            .iter_mut()
            .filter_map(TemplateClaim::take_policy)
    }

    pub(in crate::node_publish) fn take_components(
        &mut self,
    ) -> impl Iterator<Item = TemplateComponentSet> + '_ {
        self.claims.iter_mut().map(TemplateClaim::take_components)
    }

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

    #[cfg(test)]
    pub(crate) fn legacy_rows(&self) -> (Vec<&str>, Vec<serde_json::Value>) {
        (
            self.absent_templates
                .iter()
                .map(PublishedTemplateName::as_str)
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
