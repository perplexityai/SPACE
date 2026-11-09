//! NTFS storage primitives. Selectors are bounded files, never reparse points;
//! recovery resolves their slot enum to an allowlisted executable path.
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

const BOOT_KEY: &str = r"SOFTWARE\SPACE\GuestUpdateBoot";

/// Volatile registry state survives service restarts and memory snapshots but
/// disappears on a real reboot. Only recovery initializes it under SlotStore's
/// writer lock, before it starts the daemon or consumes an activation attempt.
impl SlotStore {
    pub fn initialize_boot_epoch(&self) -> Result<(), UpdateError> {
        use windows_sys::Win32::System::Registry::*;
        let path = wide(Path::new(BOOT_KEY))?;
        let name = wide(Path::new("Epoch"))?;
        let mut raw = std::ptr::null_mut();
        let mut disposition = 0;
        // SAFETY: terminated key path and writable output handles/disposition.
        let code = unsafe {
            RegCreateKeyExW(
                HKEY_LOCAL_MACHINE,
                path.as_ptr(),
                0,
                std::ptr::null(),
                REG_OPTION_VOLATILE,
                KEY_QUERY_VALUE | KEY_SET_VALUE,
                std::ptr::null(),
                &mut raw,
                &mut disposition,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32).into());
        }
        let key = RegistryKey(raw);
        match boot_clock() {
            Ok(_) => return Ok(()),
            Err(UpdateError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let value = wide(Path::new(&Uuid::now_v7().to_string()))?;
        // SAFETY: live key, terminated value name, and UTF-16 data including NUL.
        let code = unsafe {
            RegSetValueExW(
                key.0,
                name.as_ptr(),
                0,
                REG_SZ,
                value.as_ptr().cast(),
                (value.len() * 2) as u32,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32).into());
        }
        Ok(())
    }
}
struct RegistryKey(windows_sys::Win32::System::Registry::HKEY);
impl Drop for RegistryKey {
    fn drop(&mut self) {
        // SAFETY: uniquely owned registry handle, not a generic kernel handle.
        unsafe { windows_sys::Win32::System::Registry::RegCloseKey(self.0) };
    }
}

pub fn boot_clock() -> Result<(Uuid, u64), UpdateError> {
    use windows_sys::Win32::System::Registry::*;
    let path = wide(Path::new(BOOT_KEY))?;
    let name = wide(Path::new("Epoch"))?;
    let mut value = [0u16; 37];
    let mut bytes = std::mem::size_of_val(&value) as u32;
    // SAFETY: terminated paths and bounded writable UTF-16 output buffer.
    let code = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            path.as_ptr(),
            name.as_ptr(),
            RRF_RT_REG_SZ,
            std::ptr::null_mut(),
            value.as_mut_ptr().cast(),
            &mut bytes,
        )
    };
    if code != 0 {
        return Err(io::Error::from_raw_os_error(code as i32).into());
    }
    if bytes != 74 || value[36] != 0 {
        return Err(UpdateError::Layout);
    }
    let boot = String::from_utf16(&value[..36])
        .ok()
        .and_then(|s| s.parse::<Uuid>().ok())
        .filter(|v| !v.is_nil())
        .ok_or(UpdateError::Layout)?;
    // SAFETY: no pointers; the monotonic boot counter includes suspended time.
    let millis = unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() };
    Ok((boot, millis))
}

pub(super) fn verify_running_binary(expected: &Path) -> Result<(), UpdateError> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    fn identity(path: &Path) -> Result<(u32, u32, u32), UpdateError> {
        let file = open_regular(path)?;
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: live regular-file handle and writable fixed-size metadata.
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    }
    if identity(&std::env::current_exe()?)? != identity(expected)? {
        return Err(UpdateError::Identity);
    }
    Ok(())
}

/// Recovery-owned startup bound, kept in the volatile per-boot key. A daemon
/// cannot extend it by sending SCM checkpoint/wait-hint progress reports.
pub fn record_start_deadline(deadline_ms: u64) -> Result<(), UpdateError> {
    use windows_sys::Win32::System::Registry::*;
    let path = wide(Path::new(BOOT_KEY))?;
    let name = wide(Path::new("StartDeadline"))?;
    // SAFETY: existing per-boot key, terminated names and a fixed-size QWORD.
    let code = unsafe {
        RegSetKeyValueW(
            HKEY_LOCAL_MACHINE,
            path.as_ptr(),
            name.as_ptr(),
            REG_QWORD,
            (&deadline_ms as *const u64).cast(),
            std::mem::size_of::<u64>() as u32,
        )
    };
    if code != 0 {
        return Err(io::Error::from_raw_os_error(code as i32).into());
    }
    Ok(())
}

pub fn start_deadline() -> Result<u64, UpdateError> {
    use windows_sys::Win32::System::Registry::*;
    let path = wide(Path::new(BOOT_KEY))?;
    let name = wide(Path::new("StartDeadline"))?;
    let mut deadline = 0u64;
    let mut size = std::mem::size_of::<u64>() as u32;
    // SAFETY: terminated names and writable, sized QWORD output.
    let code = unsafe {
        RegGetValueW(
            HKEY_LOCAL_MACHINE,
            path.as_ptr(),
            name.as_ptr(),
            RRF_RT_REG_QWORD,
            std::ptr::null_mut(),
            (&mut deadline as *mut u64).cast(),
            &mut size,
        )
    };
    if code != 0 {
        return Err(io::Error::from_raw_os_error(code as i32).into());
    }
    if size != std::mem::size_of::<u64>() as u32 {
        return Err(UpdateError::Layout);
    }
    Ok(deadline)
}
