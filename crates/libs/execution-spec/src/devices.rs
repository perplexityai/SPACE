//! Portable requirements for exclusive whole-device allocation.
//!
//! Profiles describe qualified capabilities, not host paths. The host adapter
//! validates enrollment; neither a caller-supplied profile nor a device ID grants
//! permission to access a host resource.

use std::{fmt, str::FromStr};

/// Bound work and acknowledgement size independently of host inventory size.
pub const MAX_EXCLUSIVE_DEVICE_UNITS: u32 = 16;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeviceContractError {
    #[error("invalid device profile {0:?}; expected 1..=64 lowercase token characters")]
    Profile(String),
    #[error("invalid host device identifier {0:?}")]
    HostDevice(String),
    #[error("device count {0} must be in 1..=16")]
    Count(u32),
    #[error("device requirements must contain 1..=16 units")]
    RequirementsSize,
    #[error("unsupported device requirements schema version {0}")]
    SchemaVersion(u32),
    #[error("profile alternatives must contain 1..=16 distinct profiles")]
    Alternatives,
    #[error("exact profile and compatible alternatives are mutually exclusive")]
    ProfileChoice,
    #[error("duplicate device requirement clause")]
    DuplicateClause,
    #[error("device assignment must contain 1..=32 distinct host members")]
    Members,
    #[error("device lease identifier must be a UUIDv7: {0}")]
    Lease(String),
    #[error("device parent identifier must be a UUIDv7: {0}")]
    Parent(String),
}

macro_rules! text_identity {
    ($name:ident, $error:ident, $validate:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        pub struct $name(String);
        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, DeviceContractError> {
                let value = value.into();
                if !($validate)(&value) {
                    return Err(DeviceContractError::$error(value));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl FromStr for $name {
            type Err = DeviceContractError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }
        impl TryFrom<String> for $name {
            type Error = DeviceContractError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

text_identity!(DeviceProfile, Profile, |value: &str| {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'-' | b'_' | b'.')
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
});
text_identity!(HostDeviceId, HostDevice, |value: &str| {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b':'))
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
});

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct DeviceLeaseId(uuid::Uuid);

impl DeviceLeaseId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}
impl Default for DeviceLeaseId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for DeviceLeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for DeviceLeaseId {
    type Err = DeviceContractError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = uuid::Uuid::parse_str(value)
            .map_err(|_| DeviceContractError::Lease(value.to_owned()))?;
        if parsed.get_version_num() != 7 {
            return Err(DeviceContractError::Lease(value.to_owned()));
        }
        Ok(Self(parsed))
    }
}
impl TryFrom<String> for DeviceLeaseId {
    type Error = DeviceContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl From<DeviceLeaseId> for String {
    fn from(value: DeviceLeaseId) -> Self {
        value.to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
pub struct DeviceParentId(uuid::Uuid);

impl DeviceParentId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}
impl Default for DeviceParentId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for DeviceParentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for DeviceParentId {
    type Err = DeviceContractError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = uuid::Uuid::parse_str(value)
            .map_err(|_| DeviceContractError::Parent(value.to_owned()))?;
        if parsed.get_version_num() != 7 {
            return Err(DeviceContractError::Parent(value.to_owned()));
        }
        Ok(Self(parsed))
    }
}
impl TryFrom<String> for DeviceParentId {
    type Error = DeviceContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl From<DeviceParentId> for String {
    fn from(value: DeviceParentId) -> Self {
        value.to_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "RequirementWire", into = "RequirementWire")
)]
pub struct DeviceRequirement {
    profiles: Vec<DeviceProfile>,
    count: u32,
}
impl DeviceRequirement {
    pub fn new(profile: DeviceProfile, count: u32) -> Result<Self, DeviceContractError> {
        Self::compatible(vec![profile], count)
    }
    pub fn compatible(
        mut profiles: Vec<DeviceProfile>,
        count: u32,
    ) -> Result<Self, DeviceContractError> {
        if !(1..=MAX_EXCLUSIVE_DEVICE_UNITS).contains(&count) {
            return Err(DeviceContractError::Count(count));
        }
        if profiles.is_empty() || profiles.len() > 16 {
            return Err(DeviceContractError::Alternatives);
        }
        profiles.sort();
        if profiles
            .iter()
            .zip(profiles.iter().skip(1))
            .any(|(a, b)| a == b)
        {
            return Err(DeviceContractError::Alternatives);
        }
        Ok(Self { profiles, count })
    }
    pub fn profiles(&self) -> &[DeviceProfile] {
        &self.profiles
    }
    pub fn count(&self) -> u32 {
        self.count
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RequirementWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    profile: Option<DeviceProfile>,
    count: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    compatible_profiles: Vec<DeviceProfile>,
}
#[cfg(feature = "serde")]
impl TryFrom<RequirementWire> for DeviceRequirement {
    type Error = DeviceContractError;
    fn try_from(value: RequirementWire) -> Result<Self, Self::Error> {
        match value.profile {
            Some(profile) if value.compatible_profiles.is_empty() => {
                Self::new(profile, value.count)
            }
            None => Self::compatible(value.compatible_profiles, value.count),
            Some(_) => Err(DeviceContractError::ProfileChoice),
        }
    }
}
#[cfg(feature = "serde")]
impl From<DeviceRequirement> for RequirementWire {
    fn from(value: DeviceRequirement) -> Self {
        let mut profiles = value.profiles;
        if profiles.len() == 1 {
            Self {
                profile: profiles.pop(),
                count: value.count,
                compatible_profiles: vec![],
            }
        } else {
            Self {
                profile: None,
                count: value.count,
                compatible_profiles: profiles,
            }
        }
    }
}

/// Canonical profile ordering; an empty set is represented by absence, not by a
/// device-bearing request with no devices. Deserialization enforces the same rules.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "RequirementsWire", into = "RequirementsWire")
)]
pub struct DeviceRequirements(Vec<DeviceRequirement>);
impl DeviceRequirements {
    pub fn new(mut values: Vec<DeviceRequirement>) -> Result<Self, DeviceContractError> {
        if values.is_empty()
            || values.len() > MAX_EXCLUSIVE_DEVICE_UNITS as usize
            || values.iter().map(DeviceRequirement::count).sum::<u32>() > MAX_EXCLUSIVE_DEVICE_UNITS
        {
            return Err(DeviceContractError::RequirementsSize);
        }
        values.sort_by(|a, b| a.profiles.cmp(&b.profiles));
        for (left, right) in values.iter().zip(values.iter().skip(1)) {
            if left.profiles == right.profiles {
                return Err(DeviceContractError::DuplicateClause);
            }
        }
        Ok(Self(values))
    }
    pub fn entries(&self) -> &[DeviceRequirement] {
        &self.0
    }
}
impl TryFrom<Vec<DeviceRequirement>> for DeviceRequirements {
    type Error = DeviceContractError;
    fn try_from(value: Vec<DeviceRequirement>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<DeviceRequirements> for Vec<DeviceRequirement> {
    fn from(value: DeviceRequirements) -> Self {
        value.0
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RequirementsWire {
    schema_version: u32,
    entries: Vec<DeviceRequirement>,
}
#[cfg(feature = "serde")]
impl TryFrom<RequirementsWire> for DeviceRequirements {
    type Error = DeviceContractError;
    fn try_from(value: RequirementsWire) -> Result<Self, Self::Error> {
        if value.schema_version != 1 {
            return Err(DeviceContractError::SchemaVersion(value.schema_version));
        }
        Self::new(value.entries)
    }
}
#[cfg(feature = "serde")]
impl From<DeviceRequirements> for RequirementsWire {
    fn from(value: DeviceRequirements) -> Self {
        Self {
            schema_version: 1,
            entries: value.0,
        }
    }
}

/// A lease covers an indivisible reset/isolation unit, possibly several functions.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "AssignmentWire"))]
pub struct ExclusiveDeviceAssignment {
    parent_id: DeviceParentId,
    lease_id: DeviceLeaseId,
    profile: DeviceProfile,
    host_device_ids: Vec<HostDeviceId>,
}
impl ExclusiveDeviceAssignment {
    pub fn new(
        lease_id: DeviceLeaseId,
        parent_id: DeviceParentId,
        profile: DeviceProfile,
        mut host_device_ids: Vec<HostDeviceId>,
    ) -> Result<Self, DeviceContractError> {
        if host_device_ids.is_empty() || host_device_ids.len() > 32 {
            return Err(DeviceContractError::Members);
        }
        host_device_ids.sort();
        if host_device_ids
            .iter()
            .zip(host_device_ids.iter().skip(1))
            .any(|(left, right)| left == right)
        {
            return Err(DeviceContractError::Members);
        }
        Ok(Self {
            parent_id,
            lease_id,
            profile,
            host_device_ids,
        })
    }
    pub fn parent_id(&self) -> DeviceParentId {
        self.parent_id
    }
    pub fn lease_id(&self) -> DeviceLeaseId {
        self.lease_id
    }
    pub fn profile(&self) -> &DeviceProfile {
        &self.profile
    }
    pub fn host_device_ids(&self) -> &[HostDeviceId] {
        &self.host_device_ids
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentWire {
    parent_id: DeviceParentId,
    lease_id: DeviceLeaseId,
    profile: DeviceProfile,
    host_device_ids: Vec<HostDeviceId>,
}
#[cfg(feature = "serde")]
impl TryFrom<AssignmentWire> for ExclusiveDeviceAssignment {
    type Error = DeviceContractError;
    fn try_from(value: AssignmentWire) -> Result<Self, Self::Error> {
        Self::new(
            value.lease_id,
            value.parent_id,
            value.profile,
            value.host_device_ids,
        )
    }
}

#[cfg(test)]
mod tests;

mod selection;
pub use selection::DeviceSelectionError;
