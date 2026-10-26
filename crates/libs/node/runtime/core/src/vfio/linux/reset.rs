//! Narrow reset procedure for a single-function, single-device bus. Never infer
//! that an IOMMU group alone is a safe bus-reset domain.
use super::*;
use std::io::{Read, Write};

impl PinnedVfio {
    /// The guard must belong to this exact inspection and remain held until the
    /// reset/check completes. Callers keep durable admission fenced until drop.
    pub fn reset_single_device_bus(&self, guard: &VfioHolderExclusion) -> Result<(), VfioError> {
        let [device] = self.devices.as_slice() else {
            return Err(VfioError::Invalid("reset requires one PCI function"));
        };
        if guard._groups.len() != 1 || !guard._groups.contains_key(&device.group) {
            return Err(VfioError::Invalid("reset lacks its VFIO exclusion"));
        }
        let pinned = PathBuf::from(format!("/proc/self/fd/{}", device.directory.as_raw_fd()));
        let before = device.observe()?;
        let canonical = std::fs::read_link(&pinned).map_err(|e| io(&pinned, e))?;
        let parent = canonical
            .parent()
            .ok_or(VfioError::Invalid("missing PCI bus"))?;
        let mut members = Vec::new();
        for (index, entry) in std::fs::read_dir(parent)
            .map_err(|e| io(parent, e))?
            .enumerate()
        {
            if index >= 512 {
                return Err(VfioError::Invalid("reset bus exceeds bound"));
            }
            let entry = entry.map_err(|e| io(parent, e))?;
            if let Some(address) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<PciAddress>().ok())
            {
                members.push(address);
            }
        }
        if members != [device.address] {
            return Err(VfioError::Invalid("bus reset would affect other devices"));
        }
        let method_path = pinned.join("reset_method");
        let mut methods = String::new();
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&method_path)
            .map_err(|e| io(&method_path, e))?
            .take(65)
            .read_to_string(&mut methods)
            .map_err(|e| io(&method_path, e))?;
        if methods.trim() != "bus" {
            return Err(VfioError::Invalid(
                "reset procedure requires the sole bus method",
            ));
        }
        let path = pinned.join("reset");
        let mut reset = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        reset.write_all(b"1\n").map_err(|e| io(&path, e))?;
        if device.observe()? != before {
            return Err(VfioError::Invalid("device identity changed during reset"));
        }
        Ok(())
    }
}
