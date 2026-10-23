//! Validate portable policy observations without assigning approval authority.
use pplx_space_common_core::types::{DigestParseError, ExecutionPolicyObservation};

#[derive(Debug, thiserror::Error)]
pub enum PolicyObservationError {
    #[error("invalid execution policy reference: {0}")]
    Reference(#[from] DigestParseError),
    #[error(transparent)]
    Policy(#[from] pplx_space_common_core::types::ExecutionPolicyObservationError),
}

impl TryFrom<&crate::proto::ExecutionPolicyObservation> for ExecutionPolicyObservation {
    type Error = PolicyObservationError;
    fn try_from(value: &crate::proto::ExecutionPolicyObservation) -> Result<Self, Self::Error> {
        let allowed = value
            .allowed_environment_references
            .iter()
            .map(|reference| reference.parse())
            .collect::<Result<Vec<_>, _>>()?;
        let default = value
            .default_environment_reference
            .as_deref()
            .map(str::parse)
            .transpose()?;
        Ok(Self::new(
            value.schema_version,
            allowed,
            default,
            value.legacy_unbound_allowed,
        )?)
    }
}

impl From<&ExecutionPolicyObservation> for crate::proto::ExecutionPolicyObservation {
    fn from(value: &ExecutionPolicyObservation) -> Self {
        Self {
            schema_version: value.schema_version(),
            legacy_unbound_allowed: value.legacy_unbound_allowed(),
            allowed_environment_references: value
                .allowed_environment_references()
                .iter()
                .map(ToString::to_string)
                .collect(),
            default_environment_reference: value
                .default_environment_reference()
                .map(|reference| reference.to_string()),
        }
    }
}

#[cfg(test)]
mod tests;
