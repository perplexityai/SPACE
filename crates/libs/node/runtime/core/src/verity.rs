//! Linux file integrity for immutable execution artifacts. Expected digests
//! belong to the trusted catalog, never to downloader-supplied metadata.

use std::fs::File;
use std::path::Path;

pub use pplx_space_execution_catalog::verity::VerityDigest;

#[derive(Debug, thiserror::Error)]
pub enum VerityError {
    #[error("fs-verity is unavailable on this host or filesystem")]
    Unsupported,
    #[error("file does not have fs-verity enabled")]
    Unsealed,
    #[error("fs-verity digest {actual} differs from catalog digest {expected}")]
    Mismatch {
        expected: VerityDigest,
        actual: VerityDigest,
    },
    #[error("fs-verity returned an unsupported digest algorithm or length")]
    DigestFormat,
    #[error("{operation} execution artifact: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

/// Owns the same authenticated inode the caller must consume or execute.
#[derive(Debug)]
pub struct SealedFile(File);

impl SealedFile {
    pub fn open(path: &Path, expected: VerityDigest) -> Result<Self, VerityError> {
        let file = open_readonly(path)?;
        check(&file, expected)?;
        Ok(Self(file))
    }

    pub fn file(&self) -> &File {
        &self.0
    }

    /// Link the authenticated inode, rather than reopening its former path.
    pub fn link_to(&self, destination: &Path) -> std::io::Result<()> {
        link_file(&self.0, destination)
    }
}

/// Linux requires CAP_DAC_READ_SEARCH for linkat(AT_EMPTY_PATH); retain it
/// when restricting node-manager's capabilities. See linkat(2).
#[cfg(target_os = "linux")]
pub(crate) fn link_file(file: &File, destination: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;
    let parent = destination
        .parent()
        .ok_or(std::io::ErrorKind::InvalidInput)?;
    let name = destination
        .file_name()
        .ok_or(std::io::ErrorKind::InvalidInput)?;
    let parent = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent)?;
    let name = std::ffi::CString::new(name.as_bytes())?;
    // SAFETY: both descriptors and the destination basename remain valid
    // for linkat. AT_EMPTY_PATH selects the measured source inode itself.
    let result = unsafe {
        libc::linkat(
            file.as_raw_fd(),
            c"".as_ptr(),
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::AT_EMPTY_PATH,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn link_file(_file: &File, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Read-only availability query on an opened execution-filesystem directory.
/// ENODATA proves the ioctl exists, not that sealing a particular file with
/// our parameters will succeed. Sealing failures still fail closed.
#[cfg(any(feature = "artifact-store", test))]
pub(crate) fn filesystem_has_verity_api(directory: &File) -> Result<bool, VerityError> {
    match measure(directory) {
        Ok(_) | Err(VerityError::Unsealed) => Ok(true),
        Err(VerityError::Unsupported) => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn check(file: &File, expected: VerityDigest) -> Result<(), VerityError> {
    let actual = measure(file)?;
    if actual != expected {
        return Err(VerityError::Mismatch { expected, actual });
    }
    Ok(())
}

/// Seal in place. Existing readers and hard links retain the same inode.
/// The caller must first verify mutable content through this descriptor, or
/// have just published it through the verified-copy path under the namespace
/// lock with root-only mutation authority. A post-seal mismatch cannot make
/// this inode writable again.
#[cfg(any(feature = "artifact-store", test))]
pub(crate) fn seal(file: &File, expected: VerityDigest) -> Result<(), VerityError> {
    match measure(file) {
        Ok(_) => return check(file, expected),
        Err(VerityError::Unsealed) => {}
        Err(error) => return Err(error),
    }
    enable(file)?;
    check(file, expected)
}

pub fn open_readonly(path: &Path) -> Result<File, VerityError> {
    let mut options = File::options();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    options.open(path).map_err(|source| VerityError::Io {
        operation: "open",
        source,
    })
}

#[cfg(target_os = "linux")]
pub fn measure(file: &File) -> Result<VerityDigest, VerityError> {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    struct Measurement {
        algorithm: u16,
        size: u16,
        digest: [u8; 32],
    }
    const _: () = assert!(std::mem::size_of::<Measurement>() == 36);
    let mut measurement = Measurement {
        algorithm: 0,
        size: 32,
        digest: [0; 32],
    };
    // FS_IOC_MEASURE_VERITY encodes the four-byte flexible-array header,
    // not the size of the userspace digest buffer following that header.
    // SAFETY: the kernel receives a writable header and its declared buffer.
    let result = unsafe { libc::ioctl(file.as_raw_fd(), 0xc004_6686_u32 as _, &mut measurement) };
    if result < 0 {
        return Err(ioctl_error("measure"));
    }
    if measurement.algorithm != 1 || measurement.size != 32 {
        return Err(VerityError::DigestFormat);
    }
    Ok(VerityDigest::new(
        pplx_space_common_core::types::Digest::sha256(measurement.digest),
    ))
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "artifact-store", test))]
pub(crate) fn enable(file: &File) -> Result<(), VerityError> {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    struct Enable {
        version: u32,
        algorithm: u32,
        block_size: u32,
        salt_size: u32,
        salt_ptr: u64,
        signature_size: u32,
        reserved: u32,
        signature_ptr: u64,
        reserved2: [u64; 11],
    }
    const _: () = assert!(std::mem::size_of::<Enable>() == 128);
    let arguments = Enable {
        version: 1,
        algorithm: 1,
        block_size: 4096,
        salt_size: 0,
        salt_ptr: 0,
        signature_size: 0,
        reserved: 0,
        signature_ptr: 0,
        reserved2: [0; 11],
    };
    // SAFETY: arguments has the kernel UAPI layout, with all reserved fields
    // and absent user pointers zeroed. The borrowed descriptor remains open.
    let result = unsafe { libc::ioctl(file.as_raw_fd(), 0x4080_6685_u32 as _, &arguments) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EEXIST) {
            return Err(classify_io("enable", error));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn ioctl_error(operation: &'static str) -> VerityError {
    classify_io(operation, std::io::Error::last_os_error())
}

#[cfg(target_os = "linux")]
fn classify_io(operation: &'static str, source: std::io::Error) -> VerityError {
    match source.raw_os_error() {
        Some(libc::ENOTTY | libc::EOPNOTSUPP | libc::ENOSYS) => VerityError::Unsupported,
        Some(libc::ENODATA) => VerityError::Unsealed,
        _ => VerityError::Io { operation, source },
    }
}

#[cfg(not(target_os = "linux"))]
pub fn measure(_file: &File) -> Result<VerityDigest, VerityError> {
    Err(VerityError::Unsupported)
}

#[cfg(not(target_os = "linux"))]
#[cfg(any(feature = "artifact-store", test))]
pub(crate) fn enable(_file: &File) -> Result<(), VerityError> {
    Err(VerityError::Unsupported)
}

#[cfg(test)]
mod tests;
