use pplx_space_common_core::types::SandboxId;

use crate::metadata::parse_sandbox_id;
use crate::node_publish::diagnostics::InvalidCensusIdentity;
use crate::optional_trimmed;
use crate::proto::{PublishedSandboxState, PublishedTemplateState};

/// Pruning keys use the inventory's open legacy labels, not the stricter
/// creation-name or digest grammars. Storage alone consumes their SQL encoding.
#[derive(Debug)]
pub(crate) struct TemplateCensus {
    present_keys: Vec<(String, String)>,
}

impl TemplateCensus {
    pub(crate) fn from_proto(states: &[PublishedTemplateState]) -> Self {
        let present_keys = states
            .iter()
            .filter(|template| template.present)
            .filter_map(|template| {
                let name = optional_trimmed(Some(&template.template))?;
                let content_hash =
                    optional_trimmed(template.content_hash.as_deref()).unwrap_or_default();
                Some((name, content_hash))
            })
            .collect();
        Self { present_keys }
    }

    pub(crate) fn legacy_keys(&self) -> &[(String, String)] {
        &self.present_keys
    }
}

/// The node's completeness claim is distinct from permission to prune. An
/// invalid complete report must still withdraw backup-census certification.
#[derive(Debug)]
pub(crate) enum SandboxCensus {
    Partial,
    InvalidComplete,
    Complete(Vec<SandboxId>),
}

impl SandboxCensus {
    pub(crate) fn from_proto(
        states: &[PublishedSandboxState],
        complete: bool,
    ) -> (Self, Vec<InvalidCensusIdentity>) {
        let mut present_ids = Vec::new();
        let mut diagnostics = Vec::new();
        for state in states {
            match parse_sandbox_id(&state.sandbox_id) {
                Ok(sandbox_id) => {
                    if state.present {
                        present_ids.push(sandbox_id);
                    }
                }
                Err(error) => diagnostics.push(InvalidCensusIdentity {
                    raw_id: state.sandbox_id.clone(),
                    error,
                }),
            }
        }
        let census = if !complete {
            Self::Partial
        } else if diagnostics.is_empty() {
            Self::Complete(present_ids)
        } else {
            Self::InvalidComplete
        };
        (census, diagnostics)
    }

    pub(crate) fn claimed_complete(&self) -> bool {
        match self {
            Self::Partial => false,
            Self::InvalidComplete | Self::Complete(_) => true,
        }
    }

    /// An empty slice certifies an empty census; `None` never permits pruning.
    pub(crate) fn pruning_ids(&self) -> Option<&[SandboxId]> {
        match self {
            Self::Complete(ids) => Some(ids),
            Self::Partial | Self::InvalidComplete => None,
        }
    }
}

#[cfg(test)]
mod tests;
