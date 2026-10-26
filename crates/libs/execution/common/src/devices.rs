//! Validation and canonical acknowledgements for exclusive device requests.

use aws_lc_rs::digest::{SHA256, digest};
use pplx_space_common_core::types::Digest;
use pplx_space_execution_spec::devices::{
    DeviceContractError, DeviceLeaseId, DeviceParentId, DeviceProfile, DeviceRequirement,
    DeviceRequirements, ExclusiveDeviceAssignment, HostDeviceId,
};

use crate::proto;

mod admission;
pub use admission::{DeviceAdmissionError, DeviceExecution, decode_allocation, decode_execution};

impl TryFrom<&proto::DeviceRequirements> for DeviceRequirements {
    type Error = DeviceContractError;
    fn try_from(value: &proto::DeviceRequirements) -> Result<Self, Self::Error> {
        if value.entries.len() > 16 {
            return Err(DeviceContractError::RequirementsSize);
        }
        Self::new(
            value
                .entries
                .iter()
                .map(|entry| {
                    if entry.compatible_profiles.len() > 16 {
                        return Err(DeviceContractError::Alternatives);
                    }
                    if entry.profile.is_empty() {
                        DeviceRequirement::compatible(
                            entry
                                .compatible_profiles
                                .iter()
                                .map(DeviceProfile::parse)
                                .collect::<Result<Vec<_>, _>>()?,
                            entry.count,
                        )
                    } else if entry.compatible_profiles.is_empty() {
                        DeviceRequirement::new(DeviceProfile::parse(&entry.profile)?, entry.count)
                    } else {
                        Err(DeviceContractError::ProfileChoice)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}
impl From<&DeviceRequirements> for proto::DeviceRequirements {
    fn from(value: &DeviceRequirements) -> Self {
        Self {
            entries: value
                .entries()
                .iter()
                .map(|entry| match entry.profiles() {
                    [profile] => proto::DeviceRequirement {
                        profile: profile.to_string(),
                        count: entry.count(),
                        compatible_profiles: vec![],
                    },
                    profiles => proto::DeviceRequirement {
                        profile: String::new(),
                        count: entry.count(),
                        compatible_profiles: profiles.iter().map(ToString::to_string).collect(),
                    },
                })
                .collect(),
        }
    }
}
impl TryFrom<&proto::ExclusiveDeviceAssignment> for ExclusiveDeviceAssignment {
    type Error = DeviceContractError;
    fn try_from(value: &proto::ExclusiveDeviceAssignment) -> Result<Self, Self::Error> {
        if value.host_device_ids.len() > 32 {
            return Err(DeviceContractError::Members);
        }
        Self::new(
            value.lease_id.parse()?,
            value.parent_id.parse()?,
            value.profile.parse()?,
            value
                .host_device_ids
                .iter()
                .map(HostDeviceId::parse)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}
impl From<&ExclusiveDeviceAssignment> for proto::ExclusiveDeviceAssignment {
    fn from(value: &ExclusiveDeviceAssignment) -> Self {
        Self {
            lease_id: value.lease_id().to_string(),
            parent_id: value.parent_id().to_string(),
            profile: value.profile().to_string(),
            host_device_ids: value
                .host_device_ids()
                .iter()
                .map(ToString::to_string)
                .collect(),
        }
    }
}

/// A domain-separated SHA-256 of the persisted canonical requirements JSON.
/// Profile tokens cannot contain JSON metacharacters; ordering is validated by
/// DeviceRequirements. This encoding does not depend on protobuf field order.
pub fn requirements_digest(requirements: &DeviceRequirements) -> Digest {
    let mut canonical =
        String::from("space.exclusive-device-requirements.v1\n{\"schema_version\":1,\"entries\":[");
    for (index, entry) in requirements.entries().iter().enumerate() {
        if index != 0 {
            canonical.push(',');
        }
        match entry.profiles() {
            [profile] => {
                canonical.push_str("{\"profile\":\"");
                canonical.push_str(profile.as_str());
                canonical.push_str("\",\"count\":");
                canonical.push_str(&entry.count().to_string());
            }
            profiles => {
                canonical.push_str("{\"count\":");
                canonical.push_str(&entry.count().to_string());
                canonical.push_str(",\"compatible_profiles\":[");
                for (index, profile) in profiles.iter().enumerate() {
                    if index != 0 {
                        canonical.push(',');
                    }
                    canonical.push('"');
                    canonical.push_str(profile.as_str());
                    canonical.push('"');
                }
                canonical.push(']');
            }
        }
        canonical.push('}');
    }
    canonical.push_str("]}");
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(digest(&SHA256, canonical.as_bytes()).as_ref());
    Digest::sha256(bytes)
}

#[derive(Debug, thiserror::Error)]
pub enum DeviceAcknowledgementError {
    #[error("peer omitted the required device acknowledgement")]
    Missing,
    #[error("peer assigned devices to a CPU-only execution")]
    Unexpected,
    #[error("peer acknowledged different device requirements")]
    Requirements,
    #[error("invalid device assignment: {0}")]
    Assignment(#[from] DeviceContractError),
    #[error("peer's assignments do not cover exactly the requested device units")]
    Coverage,
    #[error(transparent)]
    Selection(#[from] pplx_space_execution_spec::devices::DeviceSelectionError),
    #[error("peer reused a lease, parent or host device across assignments")]
    Duplicate,
    #[error("peer acknowledged different device leases")]
    Leases,
}

/// Preparation and launch must acknowledge the allocation's exact identity.
/// Wire order is not significant; omission, duplicates and substitutions are.
pub fn validate_execution_acknowledgement(
    expected: Option<&DeviceExecution>,
    acknowledgement: Option<&proto::DeviceAcknowledgement>,
) -> Result<(), DeviceAcknowledgementError> {
    let Some(expected) = expected else {
        return if acknowledgement.is_none() {
            Ok(())
        } else {
            Err(DeviceAcknowledgementError::Unexpected)
        };
    };
    let actual = acknowledgement.ok_or(DeviceAcknowledgementError::Missing)?;
    if actual.requirements_digest != expected.requirements_digest().to_string() {
        return Err(DeviceAcknowledgementError::Requirements);
    }
    if actual.assignments.len() != expected.assignments().len() {
        return Err(DeviceAcknowledgementError::Leases);
    }
    let mut assignments = actual
        .assignments
        .iter()
        .map(ExclusiveDeviceAssignment::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    assignments.sort_by_key(ExclusiveDeviceAssignment::lease_id);
    if assignments != expected.assignments() {
        return Err(DeviceAcknowledgementError::Leases);
    }
    Ok(())
}

/// Check coverage as well as the digest. An echo of the digest alone is not
/// proof that the peer allocated every required unit.
pub fn validate_acknowledgement(
    requirements: &DeviceRequirements,
    acknowledgement: Option<&proto::DeviceAcknowledgement>,
    expected_assignments: Option<&[ExclusiveDeviceAssignment]>,
) -> Result<Vec<ExclusiveDeviceAssignment>, DeviceAcknowledgementError> {
    use std::collections::{BTreeMap, BTreeSet};
    let ack = acknowledgement.ok_or(DeviceAcknowledgementError::Missing)?;
    if ack.requirements_digest != requirements_digest(requirements).to_string() {
        return Err(DeviceAcknowledgementError::Requirements);
    }
    if ack.assignments.len() > 16 {
        return Err(DeviceAcknowledgementError::Coverage);
    }
    let mut coverage = BTreeMap::<DeviceProfile, u32>::new();
    let mut leases = BTreeSet::<DeviceLeaseId>::new();
    let mut members = BTreeSet::<HostDeviceId>::new();
    let mut parents = BTreeSet::<DeviceParentId>::new();
    let assignments = ack
        .assignments
        .iter()
        .map(ExclusiveDeviceAssignment::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    for assignment in &assignments {
        if !leases.insert(assignment.lease_id())
            || !parents.insert(assignment.parent_id())
            || assignment
                .host_device_ids()
                .iter()
                .any(|id| !members.insert(id.clone()))
        {
            return Err(DeviceAcknowledgementError::Duplicate);
        }
        *coverage.entry(assignment.profile().clone()).or_default() += 1;
    }
    if !requirements.is_satisfied_by(&coverage)? {
        return Err(DeviceAcknowledgementError::Coverage);
    }
    if let Some(expected) = expected_assignments {
        let expected = expected
            .iter()
            .map(|value| (value.lease_id(), value))
            .collect::<BTreeMap<_, _>>();
        let actual = assignments
            .iter()
            .map(|value| (value.lease_id(), value))
            .collect::<BTreeMap<_, _>>();
        if actual != expected {
            return Err(DeviceAcknowledgementError::Leases);
        }
    }
    Ok(assignments)
}

#[cfg(test)]
mod tests;
