//! Image handling and readiness are independent of the VMM provider and guest OS.
//! An opaque disk is never mounted or repaired by a host filesystem tool.
use std::num::NonZeroU16;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GuestDiskLayout {
    #[default]
    Ext4Filesystem,
    OpaqueRawDisk,
}

impl GuestDiskLayout {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ext4Filesystem => "ext4_filesystem",
            Self::OpaqueRawDisk => "opaque_raw_disk",
        }
    }
}
impl std::fmt::Display for GuestDiskLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown guest disk layout {0:?}")]
pub struct GuestDiskLayoutParseError(String);
impl std::str::FromStr for GuestDiskLayout {
    type Err = GuestDiskLayoutParseError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ext4_filesystem" => Ok(Self::Ext4Filesystem),
            "opaque_raw_disk" => Ok(Self::OpaqueRawDisk),
            _ => Err(GuestDiskLayoutParseError(value.into())),
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GuestReadiness {
    #[default]
    Agent,
    TcpService {
        port: NonZeroU16,
    },
}

/// The network topology is a versioned image requirement, not a VMM kind.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GuestNetwork {
    #[default]
    KernelSlotIpv4V1,
    SlotDhcpV1,
}

impl GuestNetwork {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KernelSlotIpv4V1 => "kernel_slot_ipv4_v1",
            Self::SlotDhcpV1 => "slot_dhcp_v1",
        }
    }
}
impl std::fmt::Display for GuestNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[derive(Debug, thiserror::Error)]
#[error("unknown guest network contract {0:?}")]
pub struct GuestNetworkParseError(String);
impl std::str::FromStr for GuestNetwork {
    type Err = GuestNetworkParseError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "kernel_slot_ipv4_v1" => Ok(Self::KernelSlotIpv4V1),
            "slot_dhcp_v1" => Ok(Self::SlotDhcpV1),
            _ => Err(GuestNetworkParseError(value.into())),
        }
    }
}

/// Agent readiness requires an agent; TCP readiness makes no such claim. A
/// future firmware image with an agent uses Agent without changing its OS kind.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(try_from = "ImageDocument", into = "ImageDocument")
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GuestImageContract {
    disk: GuestDiskLayout,
    readiness: GuestReadiness,
    network: GuestNetwork,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GuestImageContractError {
    #[error("unsupported guest image contract version {0}")]
    Version(u32),
}

impl GuestImageContract {
    pub fn new(disk: GuestDiskLayout, readiness: GuestReadiness, network: GuestNetwork) -> Self {
        Self {
            disk,
            readiness,
            network,
        }
    }
    pub fn disk(self) -> GuestDiskLayout {
        self.disk
    }
    pub fn readiness(self) -> GuestReadiness {
        self.readiness
    }
    pub fn network(self) -> GuestNetwork {
        self.network
    }
    pub fn expects_agent(self) -> bool {
        matches!(self.readiness, GuestReadiness::Agent)
    }
}

#[cfg(feature = "serde")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageDocument {
    version: u32,
    disk: GuestDiskLayout,
    readiness: GuestReadiness,
    network: GuestNetwork,
}

#[cfg(feature = "serde")]
impl TryFrom<ImageDocument> for GuestImageContract {
    type Error = GuestImageContractError;
    fn try_from(value: ImageDocument) -> Result<Self, Self::Error> {
        if value.version != 1 {
            return Err(GuestImageContractError::Version(value.version));
        }
        Ok(Self::new(value.disk, value.readiness, value.network))
    }
}
#[cfg(feature = "serde")]
impl From<GuestImageContract> for ImageDocument {
    fn from(value: GuestImageContract) -> Self {
        Self {
            version: 1,
            disk: value.disk,
            readiness: value.readiness,
            network: value.network,
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
