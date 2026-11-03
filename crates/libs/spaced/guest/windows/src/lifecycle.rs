//! Native flush and clock primitives. These do not qualify VMM memory restore.
use std::{
    ffi::OsString,
    io,
    os::windows::{ffi::OsStringExt, fs::OpenOptionsExt},
};
use windows_sys::Win32::{
    Foundation::{ERROR_NO_MORE_FILES, FILETIME, HANDLE, INVALID_HANDLE_VALUE, SYSTEMTIME},
    Storage::FileSystem::{
        FILE_SHARE_READ, FILE_SHARE_WRITE, FindFirstVolumeW, FindNextVolumeW, FindVolumeClose,
    },
    System::{SystemInformation::SetSystemTime, Time::FileTimeToSystemTime},
};

#[derive(Debug, thiserror::Error)]
pub(super) enum LifecycleError {
    #[error("host timestamp is outside the supported Windows UTC range")]
    Timestamp,
    #[error("enumerating guest volumes: {0}")]
    Enumerate(io::Error),
    #[error("guest volume enumeration exceeds its bound")]
    VolumeLimit,
    #[error("opening guest volume for flush: {0}")]
    Open(io::Error),
    #[error("flushing guest volume: {0}")]
    Flush(io::Error),
    #[error("setting guest UTC clock: {0}")]
    Clock(io::Error),
}

struct VolumeSearch(HANDLE);
impl Drop for VolumeSearch {
    fn drop(&mut self) {
        // SAFETY: owns the unique successful FindFirstVolumeW search handle.
        if unsafe { FindVolumeClose(self.0) } == 0 {
            tracing::warn!(error = %io::Error::last_os_error(), "closing volume enumeration failed");
        }
    }
}

pub(super) fn flush_volumes() -> Result<(), LifecycleError> {
    let mut name = [0_u16; 1024];
    // SAFETY: writable initialized buffer with the exact UTF-16 element count.
    let handle = unsafe { FindFirstVolumeW(name.as_mut_ptr(), name.len() as u32) };
    if handle == INVALID_HANDLE_VALUE {
        return Err(LifecycleError::Enumerate(io::Error::last_os_error()));
    }
    let search = VolumeSearch(handle);
    for _ in 0..64 {
        let end = name.iter().position(|&c| c == 0).ok_or_else(|| {
            LifecycleError::Enumerate(io::Error::new(
                io::ErrorKind::InvalidData,
                "unterminated volume name",
            ))
        })?;
        let path = name
            .get(..end)
            .and_then(|name| name.strip_suffix(&[b'\\' as u16]))
            .ok_or_else(|| {
                LifecycleError::Enumerate(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid volume name",
                ))
            })?;
        // Opening the GUID without its trailing slash addresses the volume,
        // not its root directory. File::sync_all uses FlushFileBuffers on Windows.
        let volume = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(OsString::from_wide(path))
            .map_err(LifecycleError::Open)?;
        volume.sync_all().map_err(LifecycleError::Flush)?;
        name.fill(0);
        // SAFETY: search remains live and the output buffer has the stated size.
        if unsafe { FindNextVolumeW(search.0, name.as_mut_ptr(), name.len() as u32) } == 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                Ok(())
            } else {
                Err(LifecycleError::Enumerate(error))
            };
        }
    }
    Err(LifecycleError::VolumeLimit)
}

fn system_time(timestamp: prost_types::Timestamp) -> Result<SYSTEMTIME, LifecycleError> {
    // FILETIME counts 100 ns since 1601. Keep protobuf's upper bound (9999),
    // validate before arithmetic, and let Windows convert to calendar fields.
    if !(-11_644_473_600..=253_402_300_799).contains(&timestamp.seconds)
        || !(0..1_000_000_000).contains(&timestamp.nanos)
    {
        return Err(LifecycleError::Timestamp);
    }
    let seconds =
        u64::try_from(timestamp.seconds + 11_644_473_600).map_err(|_| LifecycleError::Timestamp)?;
    let nanos = u64::try_from(timestamp.nanos).map_err(|_| LifecycleError::Timestamp)?;
    let ticks = seconds * 10_000_000 + nanos / 100;
    let file_time = FILETIME {
        dwLowDateTime: ticks as u32,
        dwHighDateTime: (ticks >> 32) as u32,
    };
    let mut result = SYSTEMTIME::default();
    // SAFETY: both pointers address initialized, live ABI structs for this call.
    if unsafe { FileTimeToSystemTime(&file_time, &mut result) } == 0 {
        return Err(LifecycleError::Timestamp);
    }
    Ok(result)
}

pub(super) fn sync_clock(timestamp: prost_types::Timestamp) -> Result<(), LifecycleError> {
    let time = system_time(timestamp)?;
    // SAFETY: valid UTC SYSTEMTIME retained for the synchronous call. Windows
    // enables SeSystemtimePrivilege for the call; SCM runs this backend as SYSTEM.
    if unsafe { SetSystemTime(&time) } == 0 {
        return Err(LifecycleError::Clock(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
