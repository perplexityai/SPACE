//! Pin cache files before device setup. A tool must never reopen an untrusted
//! cache pathname, even after that pathname passed a no-follow check.
use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;

use crate::rootfs_dmcrypt::DmCryptError;
use crate::service::trusted_paths;

pub(crate) fn error(source: io::Error) -> DmCryptError {
    if source.kind() == io::ErrorKind::StorageFull {
        return DmCryptError::InsufficientDisk(source.to_string());
    }
    DmCryptError::Io {
        cmd: "pinned device backing".into(),
        source,
    }
}

pub(crate) fn unlink(path: &Path) -> io::Result<()> {
    let parent = trusted_paths::open_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?)
        .map_err(path_error)?;
    match nix::unistd::unlinkat(
        &parent,
        path.file_name().ok_or(io::ErrorKind::InvalidInput)?,
        nix::unistd::UnlinkatFlags::NoRemoveDir,
    ) {
        Ok(()) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn remove_empty_directory(path: &Path) -> io::Result<()> {
    let parent =
        match trusted_paths::open_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?) {
            Ok(parent) => parent,
            Err(e) if e.code() == tonic::Code::NotFound => return Ok(()),
            Err(e) => return Err(path_error(e)),
        };
    match nix::unistd::unlinkat(
        &parent,
        path.file_name().ok_or(io::ErrorKind::InvalidInput)?,
        nix::unistd::UnlinkatFlags::RemoveDir,
    ) {
        Ok(()) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn path_error(error: tonic::Status) -> io::Error {
    io::Error::new(
        if error.code() == tonic::Code::NotFound {
            io::ErrorKind::NotFound
        } else {
            io::ErrorKind::PermissionDenied
        },
        error,
    )
}

pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, DmCryptError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| error(io::Error::other(e)))?
        .map_err(error)
}

pub(crate) struct Backing {
    pub(crate) file: File,
    parent: File,
}

impl Backing {
    /// Existing files are never truncated by opening. Only the final leaf may
    /// be absent; every parent must already be a pinned, no-follow directory.
    pub(crate) fn open(path: &Path, writable: bool) -> io::Result<Option<Self>> {
        Self::open_with(
            path,
            if writable {
                OFlag::O_RDWR
            } else {
                OFlag::O_RDONLY
            },
        )
    }

    pub(crate) fn create(path: &Path) -> io::Result<Self> {
        Self::open_with(path, OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL)?
            .ok_or_else(|| io::Error::other("new backing disappeared"))
    }

    /// Keep a fresh encrypted COW unnamed until initialization succeeds. A
    /// rejected or interrupted build then cannot masquerade as persisted data
    /// on retry, and dropping it never unlinks a caller-replaced pathname.
    pub(crate) fn create_unpublished(path: &Path) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let parent =
                trusted_paths::open_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?)
                    .map_err(path_error)?;
            let file = File::from(openat(
                &parent,
                ".",
                OFlag::O_RDWR | OFlag::O_TMPFILE | OFlag::O_CLOEXEC,
                Mode::from_bits_truncate(0o600),
            )?);
            Ok(Self { file, parent })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    /// Publish the initialized inode into its pinned parent without replacing
    /// an existing entry. The caller must do this before activating guest I/O.
    pub(crate) fn publish(&self, name: &std::ffi::OsStr) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.file.sync_all()?;
            nix::unistd::linkat(
                &self.file,
                "",
                &self.parent,
                name,
                nix::fcntl::AtFlags::AT_EMPTY_PATH,
            )?;
            self.parent.sync_all()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = name;
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    /// Fresh snapshot volumes intentionally replace the old leaf, never its
    /// referent. A competing creator makes O_EXCL fail rather than clobbering it.
    pub(crate) fn replace(path: &Path) -> io::Result<Self> {
        let parent =
            trusted_paths::open_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?)
                .map_err(path_error)?;
        let name = path.file_name().ok_or(io::ErrorKind::InvalidInput)?;
        match nix::unistd::unlinkat(&parent, name, nix::unistd::UnlinkatFlags::NoRemoveDir) {
            Ok(()) | Err(nix::errno::Errno::ENOENT) => {}
            Err(e) => return Err(e.into()),
        }
        Self::open_at(parent, name, OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL)
    }

    fn open_with(path: &Path, flags: OFlag) -> io::Result<Option<Self>> {
        let parent =
            trusted_paths::open_directory(path.parent().ok_or(io::ErrorKind::InvalidInput)?)
                .map_err(path_error)?;
        let name = path.file_name().ok_or(io::ErrorKind::InvalidInput)?;
        match Self::open_at(parent, name, flags) {
            Ok(file) => Ok(Some(file)),
            Err(e) if e.kind() == io::ErrorKind::NotFound && !flags.contains(OFlag::O_CREAT) => {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn open_at(parent: File, name: &std::ffi::OsStr, flags: OFlag) -> io::Result<Self> {
        let file = File::from(openat(
            &parent,
            name,
            flags | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK,
            Mode::from_bits_truncate(0o600),
        )?);
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Err(io::Error::other(
                "device backing must be a single-link regular file",
            ));
        }
        Ok(Self { file, parent })
    }

    pub(crate) fn resize(&self, length: u64) -> io::Result<()> {
        let stat = nix::sys::statvfs::fstatvfs(&self.parent)?;
        let free = u128::from(stat.blocks_available()) * u128::from(stat.fragment_size());
        if free < u128::from(crate::rootfs_dmcrypt::MIN_FREE_BYTES_FOR_SETUP) {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "insufficient free space for device setup",
            ));
        }
        self.file.set_len(length)?;
        self.file.sync_all()?;
        self.parent.sync_all()
    }

    pub(crate) fn require(file: Option<Self>) -> io::Result<Self> {
        file.ok_or_else(|| io::ErrorKind::NotFound.into())
    }
}

/// The held block fd prevents index reuse during setup. AUTOCLEAR releases a
/// failed attachment on drop; a successful dm table owns it after handoff.
pub(crate) struct LoopDevice {
    #[cfg(target_os = "linux")]
    _device: File,
    path: PathBuf,
}

impl LoopDevice {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn matching(device: &Path, backing: &File) -> io::Result<Option<Self>> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let file = File::open(device)?;
            let mut info = Info::default();
            // SAFETY: Info is the LOOP_GET_STATUS64 buffer, kept live with the fd.
            if unsafe { libc::ioctl(file.as_raw_fd(), 0x4c05, &mut info) } != 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(libc::ENXIO) {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
            let metadata = backing.metadata()?;
            Ok(
                (info.device == metadata.dev() && info.inode == metadata.ino()).then(|| Self {
                    _device: file,
                    path: device.to_owned(),
                }),
            )
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (device, backing);
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    pub(crate) fn attach(backing: &Backing, readonly: bool) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let control = File::options()
                .read(true)
                .write(true)
                .open("/dev/loop-control")?;
            for _ in 0..16 {
                // SAFETY: GET_FREE takes no pointer; control is a live loop-control fd.
                let index = unsafe { libc::ioctl(control.as_raw_fd(), 0x4c82) };
                if index < 0 {
                    return Err(io::Error::last_os_error());
                }
                let path = PathBuf::from(format!("/dev/loop{index}"));
                let device = File::options().read(true).write(true).open(&path)?;
                let mut config = Config {
                    fd: backing.file.as_raw_fd().cast_unsigned(),
                    block_size: 0,
                    info: Info {
                        flags: 4 | u32::from(readonly),
                        ..Info::default()
                    },
                    reserved: [0; 8],
                };
                // SAFETY: Config is Linux loop_config; both descriptors and its
                // buffer remain alive for the atomic LOOP_CONFIGURE ioctl.
                if unsafe { libc::ioctl(device.as_raw_fd(), 0x4c0a, &mut config) } == 0 {
                    return Ok(Self {
                        _device: device,
                        path,
                    });
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EBUSY) {
                    return Err(error);
                }
            }
            Err(io::Error::other(
                "loop attachment remained busy after 16 attempts",
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (backing, readonly);
            Err(io::ErrorKind::Unsupported.into())
        }
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Default)]
struct Info {
    device: u64,
    inode: u64,
    rdevice: u64,
    offset: u64,
    sizelimit: u64,
    number: u32,
    encrypt_type: u32,
    key_size: u32,
    flags: u32,
    names_and_key: [u64; 20],
    init: [u64; 2],
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct Config {
    fd: u32,
    block_size: u32,
    info: Info,
    reserved: [u64; 8],
}

pub(crate) fn matches_loop(device: &Path, backing: &File) -> io::Result<bool> {
    Ok(LoopDevice::matching(device, backing)?.is_some())
}

pub(crate) fn mount(device: &Path, target: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let target = trusted_paths::create_directory(target).map_err(path_error)?;
        let target_path = format!("/proc/self/fd/{}", target.as_raw_fd());
        nix::mount::mount(
            Some(device),
            target_path.as_str(),
            Some("btrfs"),
            nix::mount::MsFlags::MS_NOSUID | nix::mount::MsFlags::MS_NODEV,
            Some("compress-force=zstd"),
        )?;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (device, target);
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(target_os = "linux")]
const _: () = {
    assert!(std::mem::size_of::<Info>() == 232);
    assert!(std::mem::size_of::<Config>() == 304);
};

#[cfg(test)]
mod tests;
