use std::collections::BTreeSet;

use pplx_space_common_core::types::{Digest, DigestParseError, SandboxId, SandboxIdParseError};
use pplx_space_execution_spec::devices::{
    DeviceContractError, DeviceRequirements, ExclusiveDeviceAssignment,
};

use crate::proto;

#[derive(Debug, thiserror::Error)]
pub enum DeviceAdmissionError {
    #[error("device input and legacy sandbox identity are mutually exclusive")]
    MixedIdentity,
    #[error("invalid sandbox identity: {0}")]
    Sandbox(#[from] SandboxIdParseError),
    #[error("device allocation is missing its requirements")]
    MissingRequirements,
    #[error(transparent)]
    Requirements(#[from] DeviceContractError),
    #[error("invalid device requirements digest: {0}")]
    Digest(#[from] DigestParseError),
    #[error("device execution must contain between one and sixteen assignments")]
    AssignmentCount,
    #[error("device execution repeats a lease, parent or host device")]
    DuplicateAssignment,
}

/// Parsed wire evidence. Admission must additionally compare these assignments
/// with the owning store under the execution fence; callers cannot grant leases.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "DeviceExecutionWire", into = "DeviceExecutionWire")
)]
pub struct DeviceExecution {
    requirements_digest: Digest,
    assignments: Vec<ExclusiveDeviceAssignment>,
}

impl DeviceExecution {
    pub fn new(
        requirements_digest: Digest,
        mut assignments: Vec<ExclusiveDeviceAssignment>,
    ) -> Result<Self, DeviceAdmissionError> {
        if assignments.is_empty() || assignments.len() > 16 {
            return Err(DeviceAdmissionError::AssignmentCount);
        }
        let mut leases = BTreeSet::new();
        let mut parents = BTreeSet::new();
        let mut members = BTreeSet::new();
        for assignment in &assignments {
            if !leases.insert(assignment.lease_id())
                || !parents.insert(assignment.parent_id())
                || assignment
                    .host_device_ids()
                    .iter()
                    .any(|member| !members.insert(member))
            {
                return Err(DeviceAdmissionError::DuplicateAssignment);
            }
        }
        assignments.sort_by_key(ExclusiveDeviceAssignment::lease_id);
        Ok(Self {
            requirements_digest,
            assignments,
        })
    }

    pub fn requirements_digest(&self) -> Digest {
        self.requirements_digest
    }

    pub fn assignments(&self) -> &[ExclusiveDeviceAssignment] {
        &self.assignments
    }

    pub fn acknowledgement(&self) -> proto::DeviceAcknowledgement {
        proto::DeviceAcknowledgement {
            requirements_digest: self.requirements_digest.to_string(),
            assignments: self.assignments.iter().map(Into::into).collect(),
        }
    }

    pub fn input(&self, sandbox_id: SandboxId) -> proto::DeviceExecutionInput {
        proto::DeviceExecutionInput {
            sandbox_id: sandbox_id.to_string(),
            requirements_digest: self.requirements_digest.to_string(),
            assignments: self.assignments.iter().map(Into::into).collect(),
        }
    }
}

fn identity(legacy: &str, device: Option<&str>) -> Result<SandboxId, DeviceAdmissionError> {
    match device {
        Some(_) if !legacy.is_empty() => Err(DeviceAdmissionError::MixedIdentity),
        Some(id) => Ok(id.parse()?),
        None => Ok(legacy.parse()?),
    }
}

pub fn decode_allocation(
    legacy_sandbox_id: &str,
    device: Option<&proto::DeviceAllocationInput>,
) -> Result<(SandboxId, Option<DeviceRequirements>), DeviceAdmissionError> {
    let sandbox = identity(
        legacy_sandbox_id,
        device.map(|value| value.sandbox_id.as_str()),
    )?;
    let requirements = device
        .map(|value| {
            value
                .requirements
                .as_ref()
                .ok_or(DeviceAdmissionError::MissingRequirements)
                .and_then(|requirements| {
                    DeviceRequirements::try_from(requirements).map_err(Into::into)
                })
        })
        .transpose()?;
    Ok((sandbox, requirements))
}

pub fn decode_execution(
    legacy_sandbox_id: &str,
    device: Option<&proto::DeviceExecutionInput>,
) -> Result<(SandboxId, Option<DeviceExecution>), DeviceAdmissionError> {
    let sandbox = identity(
        legacy_sandbox_id,
        device.map(|value| value.sandbox_id.as_str()),
    )?;
    let Some(device) = device else {
        return Ok((sandbox, None));
    };
    if device.assignments.is_empty() || device.assignments.len() > 16 {
        return Err(DeviceAdmissionError::AssignmentCount);
    }
    let requirements_digest = device.requirements_digest.parse()?;
    let assignments = device
        .assignments
        .iter()
        .map(ExclusiveDeviceAssignment::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        sandbox,
        Some(DeviceExecution::new(requirements_digest, assignments)?),
    ))
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceExecutionWire {
    requirements_digest: Digest,
    assignments: Vec<ExclusiveDeviceAssignment>,
}
#[cfg(feature = "serde")]
impl TryFrom<DeviceExecutionWire> for DeviceExecution {
    type Error = DeviceAdmissionError;
    fn try_from(value: DeviceExecutionWire) -> Result<Self, Self::Error> {
        Self::new(value.requirements_digest, value.assignments)
    }
}
#[cfg(feature = "serde")]
impl From<DeviceExecution> for DeviceExecutionWire {
    fn from(value: DeviceExecution) -> Self {
        Self {
            requirements_digest: value.requirements_digest,
            assignments: value.assignments,
        }
    }
}
