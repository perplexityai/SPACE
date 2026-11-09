//! NTFS storage primitives. Selectors are bounded files, never reparse points;
//! the launcher resolves their slot enum to an allowlisted executable path.
use super::*;
use std::fs::OpenOptions;
use std::os::windows::{
    ffi::OsStrExt,
    fs::{MetadataExt, OpenOptionsExt},
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
    GetDiskFreeSpaceExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
};

fn wide(path: &Path) -> Result<Vec<u16>, UpdateError> {
    let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
    if value.contains(&0) {
        return Err(UpdateError::Layout);
    }
    value.push(0);
    Ok(value)
}

pub(crate) fn open_regular(path: &Path) -> Result<File, UpdateError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(UpdateError::Layout);
    }
    Ok(file)
}

pub(crate) fn create(path: &Path, _: bool) -> Result<File, UpdateError> {
    // Inherit the installer-owned SYSTEM/Administrators-only directory ACL.
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?)
}

pub(crate) fn replace(from: &Path, to: &Path) -> Result<(), UpdateError> {
    let from = wide(from)?;
    let to = wide(to)?;
    // SAFETY: both paths are terminated buffers alive for this synchronous call.
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), UpdateError> {
    // Windows has no portable directory fsync. The privileged service flushes
    // the containing local volume, including NTFS namespace metadata. A flush
    // failure prevents acknowledgement of the durable transition.
    let Some(std::path::Component::Prefix(prefix)) = path.components().next() else {
        return Err(UpdateError::Layout);
    };
    let drive = match prefix.kind() {
        std::path::Prefix::Disk(drive) | std::path::Prefix::VerbatimDisk(drive) => drive,
        _ => return Err(UpdateError::Layout),
    };
    let volume = format!(r"\\.\{}:", char::from(drive));
    OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(volume)?
        .sync_all()?;
    Ok(())
}

pub(crate) fn ensure_capacity(root: &Path, bytes: u64) -> Result<(), UpdateError> {
    let root = wide(root)?;
    let mut available = 0u64;
    // SAFETY: root is terminated and the available-byte result is writable.
    if unsafe {
        GetDiskFreeSpaceExW(
            root.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    if available < bytes.saturating_add(16 * 1024 * 1024) {
        return Err(io::Error::from_raw_os_error(112).into());
    }
    Ok(())
}

pub(crate) fn ensure_unreferenced(slot: &Path) -> Result<(), UpdateError> {
    for binary in Binary::ALL {
        let path = slot.join(files::binary_name(binary));
        match open_regular(&path) {
            Ok(file) => drop(file),
            Err(UpdateError::Io(e)) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
        // Executable mappings cannot be opened for writing. Deny all sharing
        // while probing; Windows also rejects removal if a new mapping races
        // this check. No active slot is ever a staging destination.
        match OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(0)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&path)
        {
            Ok(file) => drop(file),
            Err(e) if matches!(e.raw_os_error(), Some(5 | 32 | 33)) => {
                return Err(UpdateError::Referenced);
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

impl SlotStore {
    pub(super) fn link_slot(&self, name: &str) -> Result<Slot, UpdateError> {
        match files::read_bounded(&self.root.join(name))?.as_slice() {
            b"a" => Ok(Slot::A),
            b"b" => Ok(Slot::B),
            _ => Err(UpdateError::Layout),
        }
    }

    pub(super) fn set_link(&self, name: &str, slot: Slot) -> Result<(), UpdateError> {
        if self.link_slot(name).is_ok_and(|existing| existing == slot) {
            return Ok(());
        }
        let temp = self.root.join(format!(".{name}.next"));
        files::remove_file_if_present(&temp)?;
        let mut file = create(&temp, false)?;
        file.write_all(slot.name().as_bytes())?;
        file.sync_all()?;
        drop(file);
        replace(&temp, &self.root.join(name))?;
        sync_directory(&self.root)
    }
}
