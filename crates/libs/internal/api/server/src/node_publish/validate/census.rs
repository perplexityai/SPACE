use pplx_space_common_core::types::SandboxId;

use crate::metadata::parse_sandbox_id;
use crate::node_publish::diagnostics::InvalidCensusIdentity;
use crate::proto::PublishedSandboxState;

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
