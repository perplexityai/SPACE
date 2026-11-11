//! Validated policy observations shared by protobuf and stored JSON readers.
use super::EnvironmentReference;

/// A node's startup policy, with no cluster policy applied.
pub const EXECUTION_POLICY_SCHEMA_VERSION: u32 = 1;
/// A cluster-governed policy: the startup settings narrowed by the cluster
/// policy of `applied_generation`, or, with `cluster_policy_unreadable`, the
/// refuse-everything policy a node enforces when it can't read the cluster
/// policy it persisted. The node may have no default: its configured one can
/// fall outside the cluster's executing set while the cluster default is
/// refused.
pub const EXECUTION_POLICY_APPLIED_SCHEMA_VERSION: u32 = 2;

/// Observed eligibility is not proof that the artifacts are installed or usable.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "PolicyObservationDocument"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPolicyObservation {
    schema_version: u32,
    allowed_environment_references: Vec<EnvironmentReference>,
    default_environment_reference: Option<EnvironmentReference>,
    legacy_unbound_allowed: bool,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    applied_generation: Option<u64>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "std::ops::Not::not"))]
    cluster_policy_unreadable: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionPolicyObservationError {
    #[error("unsupported execution policy schema {0}")]
    Schema(u32),
    #[error("execution policy repeats environment {0}")]
    Duplicate(EnvironmentReference),
    #[error("execution policy default {0} is outside its allowlist")]
    DefaultNotAllowed(EnvironmentReference),
    #[error(
        "execution policy with allowed environments requires a compiled or permitted legacy default"
    )]
    MissingDefault,
    #[error(
        "execution policy schema {schema_version} needs exactly one of an applied generation or an unreadable cluster policy, and schema 1 neither"
    )]
    AppliedGeneration { schema_version: u32 },
    #[error("an unreadable cluster policy admits nothing, but this one admits work")]
    UnreadableAdmits,
}

impl ExecutionPolicyObservation {
    pub fn new(
        schema_version: u32,
        allowed: Vec<EnvironmentReference>,
        default: Option<EnvironmentReference>,
        legacy_unbound_allowed: bool,
        applied_generation: Option<u64>,
        cluster_policy_unreadable: bool,
    ) -> Result<Self, ExecutionPolicyObservationError> {
        let applied = match schema_version {
            EXECUTION_POLICY_SCHEMA_VERSION => false,
            EXECUTION_POLICY_APPLIED_SCHEMA_VERSION => true,
            _ => return Err(ExecutionPolicyObservationError::Schema(schema_version)),
        };
        let governed = applied_generation.is_some() || cluster_policy_unreadable;
        if applied != governed || (applied_generation.is_some() && cluster_policy_unreadable) {
            return Err(ExecutionPolicyObservationError::AppliedGeneration { schema_version });
        }
        if cluster_policy_unreadable
            && (!allowed.is_empty() || default.is_some() || legacy_unbound_allowed)
        {
            return Err(ExecutionPolicyObservationError::UnreadableAdmits);
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
            None if !unique.is_empty() && !applied && !legacy_unbound_allowed => {
                return Err(ExecutionPolicyObservationError::MissingDefault);
            }
            _ => {}
        }
        Ok(Self {
            schema_version,
            allowed_environment_references: unique.into_iter().collect(),
            default_environment_reference: default,
            legacy_unbound_allowed,
            applied_generation,
            cluster_policy_unreadable,
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
    /// The cluster policy generation this node enforces. `None` means the node
    /// runs its startup policy only.
    pub fn applied_generation(&self) -> Option<u64> {
        self.applied_generation
    }
    /// The node can't read the cluster policy it persisted, so it admits no new
    /// work until the policy is reapplied. Treat the node as having no usable
    /// policy evidence.
    pub fn cluster_policy_unreadable(&self) -> bool {
        self.cluster_policy_unreadable
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
    #[serde(default)]
    applied_generation: Option<u64>,
    #[serde(default)]
    cluster_policy_unreadable: bool,
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
            value.applied_generation,
            value.cluster_policy_unreadable,
        )
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
