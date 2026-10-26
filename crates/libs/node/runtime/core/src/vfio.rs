//! Linux VFIO locators and the bounded host view of an admitted assignment.
//! Durable ownership and reset-domain qualification remain node-manager's job.
use std::{fmt, path::PathBuf, str::FromStr};

#[derive(Debug, thiserror::Error)]
pub enum VfioError {
    #[error("device hardware qualification: {0}")]
    Profile(#[from] crate::catalog::DeviceProfileError),
    #[error("invalid canonical PCI device locator {0:?}")]
    Address(String),
    #[error("VFIO assignment: {0}")]
    Invalid(&'static str),
    #[error("inspect VFIO path {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[cfg(feature = "artifact-store")]
    #[error("VFIO path protection: {0}")]
    Protection(#[from] crate::artifact_store::VerifyError),
}

/// Domain:bus:device.function, with PCI's device/function bounds. The wire
/// locator uses `pci:`; neither form accepts arbitrary paths or normalizes aliases.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PciAddress {
    domain: u16,
    bus: u8,
    device: u8,
    function: u8,
}
impl FromStr for PciAddress {
    type Err = VfioError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let b = value.as_bytes();
        if b.len() != 12
            || b.get(4) != Some(&b':')
            || b.get(7) != Some(&b':')
            || b.get(10) != Some(&b'.')
            || b.iter().enumerate().any(|(i, b)| {
                !matches!(i, 4 | 7 | 10) && !(b.is_ascii_digit() || (b'a'..=b'f').contains(b))
            })
        {
            return Err(VfioError::Address(value.into()));
        }
        let hex =
            |s: &str| u16::from_str_radix(s, 16).map_err(|_| VfioError::Address(value.into()));
        let address = Self {
            domain: hex(value
                .get(..4)
                .ok_or_else(|| VfioError::Address(value.into()))?)?,
            bus: hex(value
                .get(5..7)
                .ok_or_else(|| VfioError::Address(value.into()))?)? as u8,
            device: hex(value
                .get(8..10)
                .ok_or_else(|| VfioError::Address(value.into()))?)? as u8,
            function: hex(value
                .get(11..)
                .ok_or_else(|| VfioError::Address(value.into()))?)? as u8,
        };
        if address.device > 31 || address.function > 7 {
            return Err(VfioError::Address(value.into()));
        }
        Ok(address)
    }
}
impl fmt::Display for PciAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04x}:{:02x}:{:02x}.{:x}",
            self.domain, self.bus, self.device, self.function
        )
    }
}
impl PciAddress {
    pub fn from_locator(value: &str) -> Result<Self, VfioError> {
        value
            .strip_prefix("pci:")
            .ok_or_else(|| VfioError::Address(value.into()))?
            .parse()
    }
    pub fn sysfs_path(self) -> PathBuf {
        PathBuf::from("/sys/bus/pci/devices").join(self.to_string())
    }
}

#[cfg(all(
    target_os = "linux",
    feature = "host-runtime",
    feature = "artifact-store"
))]
mod linux;
#[cfg(all(
    target_os = "linux",
    feature = "host-runtime",
    feature = "artifact-store"
))]
pub use linux::{PciDeviceObservation, PinnedVfio, PinnedVfioDevice, VfioHolderExclusion};

#[cfg(any(
    test,
    all(
        target_os = "linux",
        feature = "host-runtime",
        feature = "artifact-store"
    )
))]
fn check_group_members(
    assigned: &std::collections::BTreeMap<PciAddress, (u64, u64)>,
    observed: &std::collections::BTreeMap<PciAddress, (u64, u64)>,
) -> Result<(), VfioError> {
    if assigned.is_empty() || assigned != observed {
        return Err(VfioError::Invalid(
            "IOMMU group contains an unassigned or replaced device",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
