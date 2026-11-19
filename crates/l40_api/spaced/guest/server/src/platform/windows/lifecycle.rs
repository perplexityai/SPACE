//! Native volume flush and UTC synchronization for guest memory lifecycle.
//! Environment admission separately validates the VMM snapshot contract.
use pplx_space_spaced_guest_protocol::proto::bridge_guest::ClockSyncStatus;
use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    sync::Arc,
};
use tokio::sync::Semaphore;
use tonic::Status;
use windows_sys::Win32::{
    Foundation::{ERROR_NO_MORE_FILES, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, SYSTEMTIME},
    Storage::FileSystem::{
        CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, FindFirstVolumeW, FindNextVolumeW,
        FindVolumeClose, FlushFileBuffers, GetDriveTypeW, OPEN_EXISTING,
    },
    System::{SystemInformation::SetSystemTime, WindowsProgramming::DRIVE_FIXED},
};

pub(super) async fn flush(slots: Arc<Semaphore>) -> Result<(), Status> {
    run_flush(slots, flush_fixed_volumes).await
}

async fn run_flush(
    slots: Arc<Semaphore>,
    operation: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> Result<(), Status> {
    let permit = slots.try_acquire_owned().map_err(|_| {
        Status::resource_exhausted("guest filesystem flush already in progress; retry with backoff")
    })?;
    // Cancelling the RPC cannot cancel FlushFileBuffers. Keep admission in the
    // blocking worker so a timeout and retry cannot accumulate stuck flushes.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
            .map_err(|error| Status::internal(format!("Windows volume flush failed: {error}")))
    })
    .await
    .map_err(|error| Status::internal(format!("Windows volume flush task failed: {error}")))?
}

struct VolumeSearch(HANDLE);
impl Drop for VolumeSearch {
    fn drop(&mut self) {
        // SAFETY: this is the uniquely owned search handle from FindFirstVolumeW.
        unsafe {
            FindVolumeClose(self.0);
        }
    }
}

fn flush_fixed_volumes() -> io::Result<()> {
    // Enumerate GUID paths, including volumes without a drive letter. Never
    // flush network shares or accept a caller-supplied privileged device path.
    let mut name = [0_u16; 1024];
    // SAFETY: writable buffer and its element count remain live for this call.
    let search = unsafe { FindFirstVolumeW(name.as_mut_ptr(), name.len() as u32) };
    if search == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let search = VolumeSearch(search);
    let mut flushed = false;
    for _ in 0..256 {
        // SAFETY: successful enumeration returns a terminated volume root path.
        if unsafe { GetDriveTypeW(name.as_ptr()) } == DRIVE_FIXED {
            let end = name
                .iter()
                .position(|unit| *unit == 0)
                .ok_or_else(|| io::Error::other("unterminated volume path"))?;
            let slash = end
                .checked_sub(1)
                .and_then(|index| name.get_mut(index))
                .ok_or_else(|| io::Error::other("invalid volume root path"))?;
            if *slash != u16::from(b'\\') {
                return Err(io::Error::other("invalid volume root path"));
            }
            // A volume handle, unlike a root-directory handle, omits the slash.
            *slash = 0;
            // SAFETY: terminated OS-supplied path, no optional pointer arguments;
            // OPEN_EXISTING cannot create or truncate the volume.
            let handle = unsafe {
                CreateFileW(
                    name.as_ptr(),
                    GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    std::ptr::null(),
                    OPEN_EXISTING,
                    0,
                    std::ptr::null_mut(),
                )
            };
            if handle == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: CreateFileW transferred ownership of a valid handle.
            let volume = unsafe { OwnedHandle::from_raw_handle(handle) };
            // SAFETY: live volume handle opened with write access by the service.
            if unsafe { FlushFileBuffers(volume.as_raw_handle()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            flushed = true;
        }
        // SAFETY: live search handle and writable output buffer.
        if unsafe { FindNextVolumeW(search.0, name.as_mut_ptr(), name.len() as u32) } == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_NO_MORE_FILES as i32) {
                return Err(error);
            }
            return if flushed {
                Ok(())
            } else {
                Err(io::Error::other("no fixed volumes were available to flush"))
            };
        }
    }
    Err(io::Error::other("guest volume count exceeds flush bound"))
}

fn utc_system_time(time: Option<prost_types::Timestamp>) -> Result<SYSTEMTIME, Status> {
    let time = time.ok_or_else(|| Status::invalid_argument("host_time is required"))?;
    if !(0..1_000_000_000).contains(&time.nanos) {
        return Err(Status::invalid_argument("host_time nanos out of range"));
    }
    let time = jiff::Timestamp::new(time.seconds, time.nanos)
        .map_err(|_| Status::invalid_argument("host_time out of range"))?
        .to_zoned(jiff::tz::TimeZone::UTC);
    if !(1601..=9999).contains(&time.year()) {
        return Err(Status::invalid_argument(
            "host_time outside Windows clock range",
        ));
    }
    Ok(SYSTEMTIME {
        wYear: time.year().cast_unsigned(),
        wMonth: u16::from(time.month().cast_unsigned()),
        wDay: u16::from(time.day().cast_unsigned()),
        wHour: u16::from(time.hour().cast_unsigned()),
        wMinute: u16::from(time.minute().cast_unsigned()),
        wSecond: u16::from(time.second().cast_unsigned()),
        wMilliseconds: time.millisecond().cast_unsigned(),
        wDayOfWeek: 0,
    })
}

pub(super) fn sync_clock(time: Option<prost_types::Timestamp>) -> Result<ClockSyncStatus, Status> {
    let time = utc_system_time(time)?;
    // SAFETY: a fully initialized UTC SYSTEMTIME. SetSystemTime manages the
    // service token's system-time privilege; no tenant impersonation is active.
    if unsafe { SetSystemTime(&time) } == 0 {
        tracing::warn!(error = %io::Error::last_os_error(), "Windows guest clock sync failed");
        Ok(ClockSyncStatus::Failed)
    } else {
        Ok(ClockSyncStatus::Synced)
    }
}

#[cfg(test)]
mod tests;
