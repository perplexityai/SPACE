//! Selection of a trusted worker artifact, independent of the hosting application.

use std::path::Path;
#[cfg(target_os = "linux")]
use std::{fs::File, sync::Arc};

/// An explicitly selected Linux worker executable, pinned to its opened inode.
///
/// The caller must trust the executable, its loader and libraries, and protect
/// them from modification. Opening validates the file's shape, not its provenance
/// or protocol. Replacing the path later does not change this selection; create
/// a new service to use a replacement. In-place modification remains unsupported.
/// Only native ELF executables are accepted; script interpreters are unsupported.
#[derive(Clone, Debug)]
pub struct WorkerExecutable {
    #[cfg(target_os = "linux")]
    file: Arc<File>,
    #[cfg(not(target_os = "linux"))]
    _private: (),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenWorkerExecutableError {
    #[error("worker executable path must be absolute")]
    RelativePath,
    #[error("cannot open or inspect worker executable: {0}")]
    Io(#[source] std::io::Error),
    #[error("worker executable must be a regular executable ELF file")]
    InvalidExecutable,
    #[error("identity workers require Linux")]
    UnsupportedPlatform,
}

impl WorkerExecutable {
    /// Open a caller-selected artifact. This does not launch it or authenticate it.
    ///
    /// The caller needs read access to inspect the ELF header, in addition to the
    /// execute permission needed at launch. An execute-only artifact is insufficient
    /// when the caller cannot bypass its read permissions.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenWorkerExecutableError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(OpenWorkerExecutableError::RelativePath);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
            let file = std::fs::OpenOptions::new()
                .read(true)
                // Do not wait for a writer if a misconfigured path names a FIFO.
                .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
                .open(path)
                .map_err(OpenWorkerExecutableError::Io)?;
            let metadata = file.metadata().map_err(OpenWorkerExecutableError::Io)?;
            if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
                return Err(OpenWorkerExecutableError::InvalidExecutable);
            }
            let mut magic = [0; 4];
            if file
                .read_at(&mut magic, 0)
                .map_err(OpenWorkerExecutableError::Io)?
                != 4
                || magic != *b"\x7fELF"
            {
                return Err(OpenWorkerExecutableError::InvalidExecutable);
            }
            Ok(Self {
                file: Arc::new(file),
            })
        }
        #[cfg(not(target_os = "linux"))]
        Err(OpenWorkerExecutableError::UnsupportedPlatform)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn path(&self) -> std::path::PathBuf {
        use std::os::fd::AsRawFd;
        // The file stays CLOEXEC: ELF loading resolves this path before the
        // kernel closes descriptors, and the worker must not inherit the pin.
        format!("/proc/self/fd/{}", self.file.as_raw_fd()).into()
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
