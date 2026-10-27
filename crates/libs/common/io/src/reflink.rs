//! Preserve btrfs NoCoW when preparing an ordinary file copy. This is source
//! inheritance, not a policy for converting populated or published images.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Reject a filesystem that cannot honor an explicit NoCoW request. Callers
/// may pass a directory descriptor to validate their cache before accepting work.
pub fn validate_nocow_filesystem(file: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if rustix::fs::fstatfs(file)?.f_type == libc::BTRFS_SUPER_MAGIC {
        return Ok(());
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "nocow backing requires Linux btrfs",
    ))
}

/// Set and verify NoCoW before populating a btrfs inode. Never use this to
/// convert populated files: their data checksum policy cannot change in place.
pub fn set_nocow_on_empty(file: &File) -> io::Result<()> {
    validate_nocow_filesystem(file)?;
    if file.metadata()?.len() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NoCoW destination must be empty",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{IFlags, ioctl_getflags, ioctl_setflags};

        let flags = ioctl_getflags(file)?;
        // Btrfs requires clearing both compression flags in a separate ioctl.
        // FS_NOCOMP_FL is not named by rustix.
        let compatible = flags & !(IFlags::COMPRESSED | IFlags::from_bits_retain(0x400));
        if compatible != flags {
            ioctl_setflags(file, compatible)?;
        }
        ioctl_setflags(file, compatible | IFlags::NOCOW)?;
        if !ioctl_getflags(file)?.contains(IFlags::NOCOW) {
            return Err(io::Error::other("btrfs did not retain the nocow flag"));
        }
    }
    Ok(())
}

/// Match a NoCoW source on an empty btrfs destination before attempting a
/// reflink. Other source/filesystem combinations retain existing behavior.
/// Attribute failures are errors, never permission to fall back into CoW.
pub fn preserve_nocow(source: &File, destination: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{IFlags, fstatfs, ioctl_getflags};

        if fstatfs(source)?.f_type != libc::BTRFS_SUPER_MAGIC
            || fstatfs(destination)?.f_type != libc::BTRFS_SUPER_MAGIC
            || !ioctl_getflags(source)?.contains(IFlags::NOCOW)
        {
            return Ok(());
        }
        set_nocow_on_empty(destination)?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (source, destination);
    Ok(())
}

/// Atomically publish a reflink or sparse byte copy in a caller-owned directory.
/// Returns true for a reflink. A failed copy leaves the previous output intact.
/// Blocking: use a blocking-I/O executor from async code. Privileged callers
/// with untrusted parents must pin them and use [`copy_on_descriptors`] instead.
pub fn copy_file(source: &Path, destination: &Path) -> io::Result<bool> {
    copy_path(source, destination, false)
}

/// Atomically publish only a real reflink. Unsupported cloning is an error;
/// no byte-copy fallback is allowed on latency-sensitive shadow paths.
pub fn reflink_file(source: &Path, destination: &Path) -> io::Result<()> {
    copy_path(source, destination, true).map(|_| ())
}

fn copy_path(source: &Path, destination: &Path, require_reflink: bool) -> io::Result<bool> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let source = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(source)?;
    let source_metadata = source.metadata()?;
    if !source_metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy source must be regular",
        ));
    }
    let existing = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => {
            if !metadata.is_file()
                || (source_metadata.dev() == metadata.dev()
                    && source_metadata.ino() == metadata.ino())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "copy destination must be a distinct regular file",
                ));
            }
            Some(metadata.permissions())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Creation applies the umask. Keeping this descriptor open also permits
    // cloning a read-only source without reopening a read-only destination.
    let temporary = tempfile::Builder::new()
        .prefix(".reflink-")
        .permissions(std::fs::Permissions::from_mode(
            source_metadata.mode() & 0o777,
        ))
        .tempfile_in(parent)?;
    let cloned = copy_contents(
        &source,
        temporary.as_file(),
        source_metadata.len(),
        require_reflink,
    )?;
    if let Some(permissions) = existing {
        temporary.as_file().set_permissions(permissions)?;
    }
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    Ok(cloned)
}

/// Copy pinned regular files into an empty destination, preserving NoCoW and
/// holes. Returns true when the kernel cloned extents instead of copying bytes.
pub fn copy_on_descriptors(source: &File, destination: &File, len: u64) -> io::Result<bool> {
    copy_contents(source, destination, len, false)
}

/// Clone a bounded extent between pinned files, falling back to an exact copy
/// when this filesystem or extent layout cannot reflink. The caller owns the
/// destination range and keeps the source immutable for the whole operation.
pub fn copy_range_on_descriptors(
    source: &File,
    source_offset: u64,
    destination: &File,
    destination_offset: u64,
    length: u64,
) -> io::Result<bool> {
    let source_length = source.metadata()?.len();
    let destination_length = destination.metadata()?.len();
    if length == 0
        || source_offset
            .checked_add(length)
            .is_none_or(|end| end > source_length)
        || destination_offset
            .checked_add(length)
            .is_none_or(|end| end > destination_length)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "extent exceeds its pinned file bounds",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let range = libc::file_clone_range {
            src_fd: i64::from(source.as_raw_fd()),
            src_offset: source_offset,
            src_length: length,
            dest_offset: destination_offset,
        };
        // SAFETY: the UAPI struct and both borrowed descriptors remain live
        // through the synchronous kernel copy. Only destination data changes.
        if unsafe { libc::ioctl(destination.as_raw_fd(), libc::FICLONERANGE, &range) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if !matches!(
            error.raw_os_error(),
            Some(libc::EXDEV | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOTTY | libc::ENOSYS)
        ) {
            return Err(error);
        }
    }
    let mut buffer = vec![0_u8; length.min(1 << 20) as usize];
    let mut copied = 0;
    while copied < length {
        let count = (length - copied).min(buffer.len() as u64) as usize;
        let span = buffer
            .get_mut(..count)
            .ok_or_else(|| io::Error::other("copy buffer capacity was exceeded"))?;
        source.read_exact_at(span, source_offset + copied)?;
        destination.write_all_at(span, destination_offset + copied)?;
        copied += count as u64;
    }
    Ok(false)
}

fn copy_contents(
    source: &File,
    destination: &File,
    len: u64,
    require_reflink: bool,
) -> io::Result<bool> {
    preserve_nocow(source, destination)?;
    #[cfg(target_os = "linux")]
    match rustix::fs::ioctl_ficlone(destination, source) {
        Ok(()) => return Ok(true),
        Err(error) if require_reflink => return Err(error.into()),
        Err(_) => {}
    }
    #[cfg(not(target_os = "linux"))]
    if require_reflink {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "reflink-only copy requires Linux",
        ));
    }
    sparse_copy(source, destination, len)?;
    Ok(false)
}

/// Byte copy that reproduces the source's holes via `SEEK_DATA`/`SEEK_HOLE`,
/// so a mostly-hole sparse backing file stays sparse on the destination. On
/// a filesystem without data/hole seeking the whole range is treated as one
/// data segment.
fn sparse_copy(src: &File, dst: &File, len: u64) -> io::Result<()> {
    // Trailing holes (and the final size) come from the truncate, not writes.
    dst.set_len(len)?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut offset: u64 = 0;
    while offset < len {
        let data_start = match rustix::fs::seek(src, rustix::fs::SeekFrom::Data(offset))
            .map_err(io::Error::from)
        {
            Ok(data_start) => data_start,
            // No data at or after `offset`: the rest of the file is a hole.
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => break,
            // Filesystem can't seek holes: copy the remainder as data.
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::EINVAL) | Some(libc::ENOTSUP) | Some(libc::ENOTTY)
                ) =>
            {
                copy_segment(src, dst, offset, len, &mut buf)?;
                break;
            }
            Err(e) => return Err(e),
        };
        // A hole always follows data: filesystems report an implicit hole at
        // EOF, so this cannot fail with ENXIO after a successful SEEK_DATA.
        let hole_start = rustix::fs::seek(src, rustix::fs::SeekFrom::Hole(data_start))?;
        copy_segment(src, dst, data_start, hole_start, &mut buf)?;
        offset = hole_start;
    }
    Ok(())
}

fn copy_segment(src: &File, dst: &File, start: u64, end: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut offset = start;
    while offset < end {
        let want = usize::try_from((end - offset).min(buf.len() as u64))
            .map_err(|_| io::Error::other("segment length exceeds usize"))?;
        let chunk = buf
            .get_mut(..want)
            .ok_or_else(|| io::Error::other("copy chunk exceeds buffer"))?;
        let got = src.read_at(chunk, offset)?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source shrank during copy",
            ));
        }
        let filled = chunk
            .get(..got)
            .ok_or_else(|| io::Error::other("read returned more than requested"))?;
        dst.write_all_at(filled, offset)?;
        offset += got as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
