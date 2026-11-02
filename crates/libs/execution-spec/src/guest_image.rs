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

/// Agent availability is independent of the signal used to declare readiness.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GuestAgent {
    #[default]
    Spaced,
    Absent,
}

/// Persisted image requirements, validated against the selected boot contract.
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
    agent: GuestAgent,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GuestImageContractError {
    #[error("unsupported guest image contract version {0}")]
    Version(u32),
    #[error("agent readiness requires a guest agent")]
    MissingAgent,
}

impl GuestImageContract {
    pub fn new(
        disk: GuestDiskLayout,
        readiness: GuestReadiness,
        network: GuestNetwork,
        agent: GuestAgent,
    ) -> Result<Self, GuestImageContractError> {
        Self {
            disk,
            readiness,
            network,
            agent,
        }
        .validate()
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
    pub fn agent(self) -> GuestAgent {
        self.agent
    }
    pub fn expects_agent(self) -> bool {
        self.agent == GuestAgent::Spaced
    }
    /// Automatic pause/suspend requires the agent activity contract. Runtime
    /// memory-state qualification remains an independent admission check.
    pub fn supports_auto_memory_lifecycle(self) -> bool {
        self.expects_agent()
    }

    /// The legacy transport cannot represent other image semantics.
    pub fn legacy_transport(self) -> bool {
        self.disk == GuestDiskLayout::Ext4Filesystem
            && self.network == GuestNetwork::KernelSlotIpv4V1
            && self.agent == GuestAgent::Spaced
            && self.readiness == GuestReadiness::Agent
    }
    pub fn validate(self) -> Result<Self, GuestImageContractError> {
        if self.readiness == GuestReadiness::Agent && !self.expects_agent() {
            return Err(GuestImageContractError::MissingAgent);
        }
        Ok(self)
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
    agent: GuestAgent,
}

#[cfg(feature = "serde")]
impl TryFrom<ImageDocument> for GuestImageContract {
    type Error = GuestImageContractError;
    fn try_from(value: ImageDocument) -> Result<Self, Self::Error> {
        if value.version != 1 {
            return Err(GuestImageContractError::Version(value.version));
        }
        Self::new(value.disk, value.readiness, value.network, value.agent)
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
            agent: value.agent,
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
