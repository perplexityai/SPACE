//! Validated policy observations shared by protobuf and stored JSON readers.
use super::EnvironmentReference;

pub const EXECUTION_POLICY_SCHEMA_VERSION: u32 = 1;

/// Observed eligibility is not proof that the artifacts are installed or usable.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "PolicyObservationDocument"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPolicyObservation {
    schema_version: u32,
    allowed_environment_references: Vec<EnvironmentReference>,
    default_environment_reference: Option<EnvironmentReference>,
    legacy_unbound_allowed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionPolicyObservationError {
    #[error("unsupported execution policy schema {0}")]
    Schema(u32),
    #[error("execution policy repeats environment {0}")]
    Duplicate(EnvironmentReference),
    #[error("execution policy default {0} is outside its allowlist")]
    DefaultNotAllowed(EnvironmentReference),
    #[error("execution policy with allowed environments requires a default")]
    MissingDefault,
}

impl ExecutionPolicyObservation {
    pub fn new(
        schema_version: u32,
        allowed: Vec<EnvironmentReference>,
        default: Option<EnvironmentReference>,
        legacy_unbound_allowed: bool,
    ) -> Result<Self, ExecutionPolicyObservationError> {
        if schema_version != EXECUTION_POLICY_SCHEMA_VERSION {
            return Err(ExecutionPolicyObservationError::Schema(schema_version));
        }
        let mut unique = std::collections::BTreeSet::new();
        for reference in allowed {
            if !unique.insert(reference) {
                return Err(ExecutionPolicyObservationError::Duplicate(reference));
            }
        }
        match default {
            Some(reference) if !unique.contains(&reference) => {
                return Err(ExecutionPolicyObservationError::DefaultNotAllowed(
                    reference,
                ));
            }
            None if !unique.is_empty() => {
                return Err(ExecutionPolicyObservationError::MissingDefault);
            }
            _ => {}
        }
        Ok(Self {
            schema_version,
            allowed_environment_references: unique.into_iter().collect(),
            default_environment_reference: default,
            legacy_unbound_allowed,
        })
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }
    pub fn allowed_environment_references(&self) -> &[EnvironmentReference] {
        &self.allowed_environment_references
    }
    pub fn default_environment_reference(&self) -> Option<EnvironmentReference> {
        self.default_environment_reference
    }
    pub fn legacy_unbound_allowed(&self) -> bool {
        self.legacy_unbound_allowed
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct PolicyObservationDocument {
    schema_version: u32,
    allowed_environment_references: Vec<EnvironmentReference>,
    default_environment_reference: Option<EnvironmentReference>,
    #[serde(default)]
    legacy_unbound_allowed: bool,
}

#[cfg(feature = "serde")]
impl TryFrom<PolicyObservationDocument> for ExecutionPolicyObservation {
    type Error = ExecutionPolicyObservationError;
    fn try_from(value: PolicyObservationDocument) -> Result<Self, Self::Error> {
        Self::new(
            value.schema_version,
            value.allowed_environment_references,
            value.default_environment_reference,
            value.legacy_unbound_allowed,
        )
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
