//! Guest-only, opt-in reporting policy and explicit setup-cache release.
use std::{path::Path, str::FromStr};

const ORDER_PATH: &str = "/sys/module/page_reporting/parameters/page_reporting_order";
pub const ORDER_ENV: &str = "SPACE_FREE_PAGE_REPORTING_ORDER";

/// Minimum buddy order eligible for reporting on supported 4 KiB-page guests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReportingOrder(u8);

impl std::fmt::Display for ReportingOrder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ReportingOrder {
    type Err = MemoryReportingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse::<u8>() {
            Ok(order @ 0..=9) => Ok(Self(order)),
            _ => Err(MemoryReportingError::InvalidOrder),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryReportingError {
    #[error("reporting order must be an integer from 0 through 9")]
    InvalidOrder,
    #[error("memory reporting maintenance must run inside a VSOCK guest")]
    NotGuest,
    #[error("memory reporting maintenance is only supported on Linux")]
    Unsupported,
    #[error("could not {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("kernel did not retain requested reporting order {requested}")]
    Readback { requested: ReportingOrder },
}

fn io_error(operation: &'static str, source: std::io::Error) -> MemoryReportingError {
    MemoryReportingError::Io { operation, source }
}

#[cfg(target_os = "linux")]
fn require_guest_cid(cid: u32) -> Result<(), MemoryReportingError> {
    if cid <= tokio_vsock::VMADDR_CID_HOST || cid == u32::MAX {
        return Err(MemoryReportingError::NotGuest);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn require_guest() -> Result<(), MemoryReportingError> {
    use std::os::fd::AsRawFd;
    let file = std::fs::File::open("/dev/vsock")
        .map_err(|err| io_error("open local VSOCK device", err))?;
    let mut cid: u32 = 0;
    // SAFETY: the Linux VSOCK CID ioctl writes one u32 into the live, correctly
    // sized local variable. No guest-controlled pointers cross this boundary.
    if unsafe { libc::ioctl(file.as_raw_fd(), 0x7b9, &mut cid) } < 0 {
        return Err(io_error(
            "read local VSOCK CID",
            std::io::Error::last_os_error(),
        ));
    }
    require_guest_cid(cid)
}

#[cfg(not(target_os = "linux"))]
fn require_guest() -> Result<(), MemoryReportingError> {
    Err(MemoryReportingError::Unsupported)
}

fn apply_order(path: &Path, order: ReportingOrder) -> Result<(), MemoryReportingError> {
    std::fs::write(path, format!("{order}\n"))
        .map_err(|err| io_error("write reporting order", err))?;
    let actual =
        std::fs::read_to_string(path).map_err(|err| io_error("read back reporting order", err))?;
    if actual.trim().parse::<ReportingOrder>()? != order {
        return Err(MemoryReportingError::Readback { requested: order });
    }
    Ok(())
}

/// Unset preserves the kernel/snapshot setting. Explicit invalid or unsupported
/// settings fail readiness rather than silently keeping a different order.
pub fn configure_from_env() -> Result<Option<ReportingOrder>, MemoryReportingError> {
    let Some(value) = std::env::var_os(ORDER_ENV) else {
        return Ok(None);
    };
    let order = value
        .to_str()
        .ok_or(MemoryReportingError::InvalidOrder)?
        .parse()?;
    require_guest()?;
    apply_order(Path::new(ORDER_PATH), order)?;
    Ok(Some(order))
}

/// An explicit post-setup operation, never a periodic cache-flushing policy.
/// The CID guard prevents accidental host invocation from dropping host cache.
pub fn release_setup_cache() -> Result<(), MemoryReportingError> {
    require_guest()?;
    #[cfg(target_os = "linux")]
    {
        // SAFETY: sync takes no arguments and flushes only this guest kernel's
        // filesystems. The VSOCK CID guard above excludes the host kernel.
        unsafe { libc::sync() };
        std::fs::write("/proc/sys/vm/drop_caches", b"3\n")
            .map_err(|err| io_error("release clean guest setup cache", err))?;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    Err(MemoryReportingError::Unsupported)
}

#[cfg(test)]
#[path = "memory_reporting/tests.rs"]
mod tests;
