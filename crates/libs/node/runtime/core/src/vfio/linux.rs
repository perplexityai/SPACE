use super::*;
use pplx_space_execution_common::devices::DeviceExecution;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
    },
    path::Path,
};

mod identity;
#[cfg(feature = "gpu-development")]
mod reset;
#[cfg(test)]
mod tests;
pub use identity::PciDeviceObservation;

#[derive(Debug)]
pub struct PinnedVfioDevice {
    address: PciAddress,
    directory: File,
    group: u32,
}
impl PinnedVfioDevice {
    pub fn address(&self) -> PciAddress {
        self.address
    }
    pub fn directory(&self) -> &File {
        &self.directory
    }
    pub fn group(&self) -> u32 {
        self.group
    }
    pub fn observe(&self) -> Result<PciDeviceObservation, VfioError> {
        identity::observe(self)
    }
}
#[derive(Debug)]
pub struct PinnedVfio {
    devices: Vec<PinnedVfioDevice>,
    nodes: BTreeMap<PathBuf, File>,
}
fn io(path: &Path, source: std::io::Error) -> VfioError {
    VfioError::Io {
        path: path.to_owned(),
        source,
    }
}
fn open_directory(path: &Path) -> Result<File, VfioError> {
    crate::artifact_store::verify_execution_directory(path, 0)?;
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|e| io(path, e))
}
fn pin_node(path: &Path, group: bool) -> Result<File, VfioError> {
    let parent = path
        .parent()
        .ok_or(VfioError::Invalid("device node has no parent"))?;
    crate::artifact_store::verify_execution_directory(parent, 0)?;
    // O_PATH pins the node without opening a VFIO group. Opening it here would
    // own the group and prevent the contained VMM's subsequent exclusive open.
    let file = File::from(
        nix::fcntl::open(
            path,
            nix::fcntl::OFlag::O_PATH
                | nix::fcntl::OFlag::O_NOFOLLOW
                | nix::fcntl::OFlag::O_CLOEXEC,
            nix::sys::stat::Mode::empty(),
        )
        .map_err(|e| io(path, e.into()))?,
    );
    let meta = file.metadata().map_err(|e| io(path, e))?;
    if !meta.file_type().is_char_device() || meta.uid() != 0 || (group && meta.mode() & 0o077 != 0)
    {
        return Err(VfioError::Invalid(
            "VFIO group must be a root-owned private character device",
        ));
    }
    Ok(file)
}
impl PinnedVfio {
    /// Check actual pinned hardware against the selected environment/profile,
    /// independently of the enrollment name supplied by an operator.
    pub fn check_profile_assignments(
        &self,
        execution: &DeviceExecution,
        profiles: &[crate::catalog::ExclusiveDeviceProfile],
    ) -> Result<(), VfioError> {
        for assignment in execution.assignments() {
            let profile = profiles
                .iter()
                .find(|profile| profile.profile() == assignment.profile())
                .ok_or_else(|| {
                    crate::catalog::DeviceProfileError::Unqualified(assignment.profile().clone())
                })?;
            let mut observations = Vec::new();
            for locator in assignment.host_device_ids() {
                let address = PciAddress::from_locator(locator.as_str())?;
                let device = self
                    .devices
                    .iter()
                    .find(|device| device.address() == address)
                    .ok_or(VfioError::Invalid(
                        "assigned device missing from pinned view",
                    ))?;
                observations.push(device.observe()?.identifiers());
            }
            profile.check_pci_hardware(&observations)?;
        }
        Ok(())
    }
    /// Inspect only the admitted devices, before entering the mount namespace.
    /// This checks the IOMMU boundary; it does not qualify a bus-reset domain.
    pub fn inspect(execution: &DeviceExecution) -> Result<Self, VfioError> {
        Self::inspect_units(
            execution
                .assignments()
                .iter()
                .map(|assignment| assignment.host_device_ids()),
        )
    }
    /// Operator enrollment has no lease yet and never creates one merely to
    /// inspect hardware. The complete proposed parent is one isolation unit.
    pub fn inspect_parent(
        members: &[pplx_space_execution_spec::devices::HostDeviceId],
    ) -> Result<Self, VfioError> {
        if members.is_empty() || members.len() > 32 {
            return Err(VfioError::Invalid("invalid enrollment member count"));
        }
        Self::inspect_units(std::iter::once(members))
    }
    fn inspect_units<'a>(
        units: impl IntoIterator<Item = &'a [pplx_space_execution_spec::devices::HostDeviceId]>,
    ) -> Result<Self, VfioError> {
        let mut devices = Vec::new();
        let mut groups = BTreeMap::new();
        let mut by_address = BTreeMap::new();
        for (unit, members) in units.into_iter().enumerate() {
            for locator in members {
                let address = PciAddress::from_locator(locator.as_str())?;
                let path = address.sysfs_path();
                let canonical = std::fs::canonicalize(&path).map_err(|e| io(&path, e))?;
                if !canonical.starts_with("/sys/devices") {
                    return Err(VfioError::Invalid("PCI path escaped kernel device tree"));
                }
                let directory = open_directory(&canonical)?;
                let pinned = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
                let driver_path = pinned.join("driver");
                let driver = std::fs::read_link(&driver_path).map_err(|e| io(&driver_path, e))?;
                if driver.file_name() != Some(std::ffi::OsStr::new("vfio-pci")) {
                    return Err(VfioError::Invalid(
                        "assigned PCI function is not bound to vfio-pci",
                    ));
                }
                let group_path = pinned.join("iommu_group");
                let group_link = std::fs::read_link(&group_path).map_err(|e| io(&group_path, e))?;
                let group = group_link
                    .file_name()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u32>().ok())
                    .ok_or(VfioError::Invalid("invalid IOMMU group"))?;
                if groups.insert(group, unit).is_some_and(|p| p != unit) {
                    return Err(VfioError::Invalid("IOMMU group crosses allocation parents"));
                }
                let meta = directory.metadata().map_err(|e| io(&path, e))?;
                if by_address
                    .insert(address, (meta.dev(), meta.ino()))
                    .is_some()
                {
                    return Err(VfioError::Invalid("duplicate PCI address"));
                }
                devices.push(PinnedVfioDevice {
                    address,
                    directory,
                    group,
                });
            }
        }
        let mut nodes = BTreeMap::new();
        let container = PathBuf::from("/dev/vfio/vfio");
        nodes.insert(container.clone(), pin_node(&container, false)?);
        for group in groups.keys() {
            let path = PathBuf::from(format!("/sys/kernel/iommu_groups/{group}/devices"));
            let mut observed = BTreeMap::new();
            for (index, entry) in std::fs::read_dir(&path)
                .map_err(|e| io(&path, e))?
                .enumerate()
            {
                if index >= 32 {
                    return Err(VfioError::Invalid("IOMMU group exceeds assignment bound"));
                }
                let entry = entry.map_err(|e| io(&path, e))?;
                let address = entry
                    .file_name()
                    .to_str()
                    .ok_or(VfioError::Invalid("non-UTF8 PCI address"))?
                    .parse::<PciAddress>()?;
                let meta = std::fs::metadata(entry.path()).map_err(|e| io(&entry.path(), e))?;
                observed.insert(address, (meta.dev(), meta.ino()));
            }
            let assigned = devices
                .iter()
                .filter(|d| d.group == *group)
                .map(|d| {
                    by_address
                        .get(&d.address)
                        .copied()
                        .map(|identity| (d.address, identity))
                        .ok_or(VfioError::Invalid("assigned device identity disappeared"))
                })
                .collect::<Result<_, _>>()?;
            check_group_members(&assigned, &observed)?;
            let node = PathBuf::from(format!("/dev/vfio/{group}"));
            nodes.insert(node.clone(), pin_node(&node, true)?);
        }
        Ok(Self { devices, nodes })
    }
    /// Exclude every kernel VFIO holder until the returned guard is dropped.
    /// Group open rejects another group owner and device-cdev users; device FDs
    /// retain their group-file reference. No container/device ioctl is issued,
    /// so this establishes exclusion without assigning or resetting hardware.
    pub fn exclude_holders(&self) -> Result<VfioHolderExclusion, VfioError> {
        let mut groups = BTreeMap::new();
        for device in &self.devices {
            if groups.contains_key(&device.group) {
                continue;
            }
            let path = PathBuf::from(format!("/dev/vfio/{}", device.group));
            let pinned = self
                .nodes
                .get(&path)
                .ok_or(VfioError::Invalid("VFIO group node disappeared"))?;
            let descriptor = PathBuf::from(format!("/proc/self/fd/{}", pinned.as_raw_fd()));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_CLOEXEC)
                .open(&descriptor)
                .map_err(|e| io(&path, e))?;
            let before = pinned.metadata().map_err(|e| io(&path, e))?;
            let after = file.metadata().map_err(|e| io(&path, e))?;
            if (before.dev(), before.ino(), before.rdev())
                != (after.dev(), after.ino(), after.rdev())
            {
                return Err(VfioError::Invalid(
                    "VFIO group changed while excluding holders",
                ));
            }
            groups.insert(device.group, file);
        }
        Ok(VfioHolderExclusion { _groups: groups })
    }

    pub fn devices(&self) -> &[PinnedVfioDevice] {
        &self.devices
    }
    pub fn nodes(&self) -> &BTreeMap<PathBuf, File> {
        &self.nodes
    }
}

/// Live exclusive group descriptors; dropping this guard releases exclusion.
#[derive(Debug)]
pub struct VfioHolderExclusion {
    _groups: BTreeMap<u32, File>,
}
