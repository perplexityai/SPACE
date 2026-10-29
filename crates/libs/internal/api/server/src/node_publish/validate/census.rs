use pplx_space_common_core::types::SandboxId;

/// Pruning keys use the inventory's open legacy labels, not the stricter
/// creation-name or digest grammars. Storage alone consumes their SQL encoding.
#[derive(Debug)]
pub(crate) struct TemplateCensus {
    present_keys: Vec<(String, String)>,
}

impl TemplateCensus {
    pub(super) fn from_keys(present_keys: Vec<(String, String)>) -> Self {
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
