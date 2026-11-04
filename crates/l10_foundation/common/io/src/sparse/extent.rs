//! Shared extent boundaries and unsupported-filesystem fallback policy.

use std::fs::File;
use std::io;

#[derive(Debug)]
pub(crate) enum Extent {
    Data(u64),
    Hole(u64),
    Unsupported,
}

#[derive(Clone, Copy)]
pub(super) enum Boundary {
    Data,
    Hole,
}

/// Probe one extent within an immutable file's logical length. Probing may
/// move the descriptor cursor, so consumers must use positional data reads.
#[cfg(all(unix, feature = "reflink"))]
pub(crate) fn next_extent(file: &File, offset: u64, length: u64) -> io::Result<Extent> {
    next_extent_with(file, offset, length, &mut seek_boundary)
}

pub(super) fn next_extent_with(
    file: &File,
    offset: u64,
    length: u64,
    seek: &mut impl FnMut(&File, u64, Boundary) -> io::Result<u64>,
) -> io::Result<Extent> {
    match probe(file, offset, length, seek) {
        Err(error) if unsupported(&error) => Ok(Extent::Unsupported),
        result => result,
    }
}

fn probe(
    file: &File,
    offset: u64,
    length: u64,
    seek: &mut impl FnMut(&File, u64, Boundary) -> io::Result<u64>,
) -> io::Result<Extent> {
    let data = match seek(file, offset, Boundary::Data) {
        Ok(data) => data,
        // ENXIO means there is no more data, not an unsupported operation.
        Err(error) if error.raw_os_error() == Some(libc::ENXIO) => length,
        Err(error) => return Err(error),
    };
    if data < offset || data > length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data extent outside the remaining file",
        ));
    }
    if data > offset {
        return Ok(Extent::Hole(data));
    }
    let hole = seek(file, offset, Boundary::Hole)?;
    if hole <= offset || hole > length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "hole extent does not advance within the file",
        ));
    }
    Ok(Extent::Data(hole))
}

fn unsupported(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Unsupported
        || error.raw_os_error().is_some_and(|errno| {
            // ENOTSUP and EOPNOTSUPP are aliases on Linux, but differ on macOS.
            [
                libc::EINVAL,
                libc::ENOTSUP,
                libc::EOPNOTSUPP,
                libc::ENOSYS,
                libc::ENOTTY,
            ]
            .contains(&errno)
        })
}

#[cfg(all(feature = "sparse-read", any(target_os = "linux", target_os = "macos")))]
pub(super) fn seek_boundary(file: &File, offset: u64, boundary: Boundary) -> io::Result<u64> {
    let whence = match boundary {
        Boundary::Data => rustix::fs::SeekFrom::Data(offset),
        Boundary::Hole => rustix::fs::SeekFrom::Hole(offset),
    };
    rustix::fs::seek(file, whence).map_err(io::Error::from)
}

#[cfg(not(all(feature = "sparse-read", any(target_os = "linux", target_os = "macos"))))]
pub(super) fn seek_boundary(_file: &File, _offset: u64, _boundary: Boundary) -> io::Result<u64> {
    Err(io::ErrorKind::Unsupported.into())
}
