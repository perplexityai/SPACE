//! Same-boot PCI observations detect replacement and topology changes. They are
//! not a hardware serial number, firmware attestation or reset qualification.
use super::*;
use aws_lc_rs::digest::{Context, SHA256};
use pplx_space_common_core::types::Digest;
use std::io::Read;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PciDeviceObservation {
    identifiers: [u16; 4],
    sysfs_identity: (u64, u64),
    topology_digest: Digest,
}

impl PciDeviceObservation {
    /// Vendor, device, subsystem vendor and subsystem device, in that order.
    pub fn identifiers(&self) -> [u16; 4] {
        self.identifiers
    }
    pub fn sysfs_identity(&self) -> (u64, u64) {
        self.sysfs_identity
    }
    pub fn topology_digest(&self) -> Digest {
        self.topology_digest
    }
}

fn identifier(directory: &Path, name: &str) -> Result<u16, VfioError> {
    let path = directory.join(name);
    let mut bytes = String::new();
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .map_err(|error| io(&path, error))?
        .take(65)
        .read_to_string(&mut bytes)
        .map_err(|error| io(&path, error))?;
    if bytes.len() > 64 {
        return Err(VfioError::Invalid("oversized PCI identifier"));
    }
    bytes
        .trim()
        .strip_prefix("0x")
        .and_then(|hex| u16::from_str_radix(hex, 16).ok())
        .ok_or(VfioError::Invalid("malformed PCI identifier"))
}

pub(super) fn observe(device: &PinnedVfioDevice) -> Result<PciDeviceObservation, VfioError> {
    let pinned = PathBuf::from(format!("/proc/self/fd/{}", device.directory.as_raw_fd()));
    let meta = device
        .directory
        .metadata()
        .map_err(|error| io(&pinned, error))?;
    let canonical = std::fs::read_link(&pinned).map_err(|error| io(&pinned, error))?;
    let parent = canonical
        .parent()
        .ok_or(VfioError::Invalid("PCI device has no bus"))?;
    let mut siblings = BTreeMap::new();
    // Record immediate bus membership as well as the device's kernel path and
    // group. Profiles still have to qualify reset domains; this does not infer
    // that a bus reset is safe or that sibling bridges contain no devices.
    for (index, entry) in std::fs::read_dir(parent)
        .map_err(|error| io(parent, error))?
        .enumerate()
    {
        if index >= 512 {
            return Err(VfioError::Invalid("PCI bus observation exceeds its bound"));
        }
        let entry = entry.map_err(|error| io(parent, error))?;
        let Some(address) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<PciAddress>().ok())
        else {
            continue;
        };
        let identity =
            std::fs::symlink_metadata(entry.path()).map_err(|error| io(&entry.path(), error))?;
        if !identity.is_dir() {
            return Err(VfioError::Invalid(
                "PCI bus member is not a device directory",
            ));
        }
        siblings.insert(address, (identity.dev(), identity.ino()));
    }
    if siblings.get(&device.address) != Some(&(meta.dev(), meta.ino())) {
        return Err(VfioError::Invalid("PCI device changed during observation"));
    }
    let mut digest = Context::new(&SHA256);
    digest.update(b"space.pci-topology-observation.v1\0");
    let path = canonical.as_os_str().as_encoded_bytes();
    digest.update(&(path.len() as u64).to_le_bytes());
    digest.update(path);
    digest.update(&device.group.to_le_bytes());
    for (address, (dev, ino)) in siblings {
        digest.update(address.to_string().as_bytes());
        digest.update(&dev.to_le_bytes());
        digest.update(&ino.to_le_bytes());
    }
    let mut hash = [0; 32];
    hash.copy_from_slice(digest.finish().as_ref());
    Ok(PciDeviceObservation {
        identifiers: [
            identifier(&pinned, "vendor")?,
            identifier(&pinned, "device")?,
            identifier(&pinned, "subsystem_vendor")?,
            identifier(&pinned, "subsystem_device")?,
        ],
        sysfs_identity: (meta.dev(), meta.ino()),
        topology_digest: Digest::sha256(hash),
    })
}
