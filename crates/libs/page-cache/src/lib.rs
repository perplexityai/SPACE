//! Page-cache advice and range writeback (start and wait) for files streamed
//! through a service: `posix_fadvise` and `sync_file_range` wrappers that are
//! no-ops off Linux.
//!
//! It is its own crate so file-copying code can use it without the rest of the
//! service host.

use std::path::Path;

/// Ask Linux to reclaim clean page-cache entries for a file we just streamed.
///
/// Many node-local SPACE services stream large rootfs, snapshot, and volume
/// files through hostPath volumes. Those pages can remain charged to the
/// service cgroup after the service is done with them. This is a best-effort
/// cache hint rather than a durability boundary: dirty pages may stay cached
/// until normal writeback completes.
pub fn advise_dontneed(file: &std::fs::File, path: &Path, operation: &'static str) {
    if let Err(error) = advise_dontneed_inner(file) {
        tracing::warn!(
            error = %error,
            path = %path.display(),
            operation,
            "failed to advise file cache as no-longer-needed"
        );
    }
}

#[cfg(target_os = "linux")]
fn sync_file_range(
    file: &std::fs::File,
    offset: u64,
    length: u64,
    flags: libc::c_uint,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = libc::off64_t::try_from(offset)
        .map_err(|_| std::io::Error::other("writeback offset exceeds off64_t::MAX"))?;
    let length = libc::off64_t::try_from(length)
        .map_err(|_| std::io::Error::other("writeback length exceeds off64_t::MAX"))?;
    // SAFETY: `file.as_raw_fd()` is valid for this synchronous call. The
    // integer range contains no Rust-managed pointers.
    let result = unsafe { libc::sync_file_range(file.as_raw_fd(), offset, length, flags) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Start asynchronous writeback for a file range.
///
/// This is not a durability boundary: metadata and storage device caches are
/// outside this operation's contract.
#[cfg(target_os = "linux")]
pub fn start_writeback_range(
    file: &std::fs::File,
    offset: u64,
    length: u64,
) -> std::io::Result<()> {
    sync_file_range(file, offset, length, libc::SYNC_FILE_RANGE_WRITE)
}

#[cfg(not(target_os = "linux"))]
pub fn start_writeback_range(
    _file: &std::fs::File,
    _offset: u64,
    _length: u64,
) -> std::io::Result<()> {
    Ok(())
}

/// Wait for previously submitted writeback in a file range.
#[cfg(target_os = "linux")]
pub fn wait_writeback_range(file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
    sync_file_range(
        file,
        offset,
        length,
        libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
    )
}

#[cfg(not(target_os = "linux"))]
pub fn wait_writeback_range(
    _file: &std::fs::File,
    _offset: u64,
    _length: u64,
) -> std::io::Result<()> {
    Ok(())
}

/// Ask the kernel to reclaim clean page-cache entries in one file range.
#[cfg(target_os = "linux")]
pub fn advise_range_dontneed(
    file: &std::fs::File,
    offset: u64,
    length: u64,
) -> std::io::Result<()> {
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| std::io::Error::other("advice offset exceeds off_t::MAX"))?;
    let length = libc::off_t::try_from(length)
        .map_err(|_| std::io::Error::other("advice length exceeds off_t::MAX"))?;
    nix::fcntl::posix_fadvise(
        file,
        offset,
        length,
        nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
    )
    .map_err(Into::into)
}

#[cfg(not(target_os = "linux"))]
pub fn advise_range_dontneed(
    _file: &std::fs::File,
    _offset: u64,
    _length: u64,
) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn advise_path_dontneed(path: &Path, operation: &'static str) {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) => {
            tracing::debug!(
                error = %error,
                path = %path.display(),
                operation,
                "failed to open file for page-cache advice"
            );
            return;
        }
    };
    advise_dontneed(&file, path, operation);
}

#[cfg(not(target_os = "linux"))]
pub fn advise_path_dontneed(_path: &Path, _operation: &'static str) {}

#[cfg(target_os = "linux")]
fn advise_dontneed_inner(file: &std::fs::File) -> std::io::Result<()> {
    advise_range_dontneed(file, 0, 0)
}

#[cfg(not(target_os = "linux"))]
fn advise_dontneed_inner(_file: &std::fs::File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests;
