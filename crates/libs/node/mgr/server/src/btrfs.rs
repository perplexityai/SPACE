//! Btrfs UAPI operations on pinned objects. The CLI canonicalizes operands and
//! therefore cannot preserve an open descriptor's identity through a mutation.
use std::ffi::OsStr;
use std::fs::File;
use std::io;

use pplx_space_common_core::host::btrfs::SubvolumeInfo;

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

pub(crate) fn is_subvolume(directory: &File) -> io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(directory.metadata()?.ino() == 256
            && nix::sys::statfs::fstatfs(directory)?.filesystem_type()
                == nix::sys::statfs::BTRFS_SUPER_MAGIC)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        Ok(false)
    }
}

pub(crate) fn create(parent: &File, name: &OsStr) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if nix::sys::statfs::fstatfs(parent)?.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC {
        return create_subvolume(parent, name);
    }

    nix::sys::stat::mkdirat(
        parent,
        name,
        nix::sys::stat::Mode::from_bits_truncate(0o700),
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn create_subvolume(parent: &File, name: &OsStr) -> io::Result<()> {
    let mut args = VolumeArgs::new(name)?;
    // SAFETY: VolumeArgs has the SUBVOL_CREATE_V2 layout; parent and args
    // remain live for the synchronous call.
    ioctl_result(unsafe { libc::ioctl(parent.as_raw_fd(), 0x5000_9418, &mut args) })
}

pub(crate) fn snapshot(
    source: &File,
    parent: &File,
    name: &OsStr,
    readonly: bool,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let mut args = VolumeArgs::new(name)?;
        args.fd = i64::from(source.as_raw_fd());
        args.flags = if readonly { 2 } else { 0 };
        // SAFETY: VolumeArgs has the SNAP_CREATE_V2 layout; both descriptors
        // remain borrowed until the kernel finishes the snapshot.
        ioctl_result(unsafe { libc::ioctl(parent.as_raw_fd(), 0x5000_9417, &mut args) })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (source, parent, name, readonly);
        Err(io::ErrorKind::Unsupported.into())
    }
}

pub(crate) fn info(directory: &File) -> io::Result<(u64, SubvolumeInfo)> {
    #[cfg(target_os = "linux")]
    {
        if !is_subvolume(directory)? {
            return Err(io::Error::other("not a btrfs subvolume root"));
        }
        let mut args = Info::default();
        // SAFETY: Info is the complete writable GET_SUBVOL_INFO buffer.
        ioctl_result(unsafe {
            libc::ioctl(directory.as_raw_fd(), 0x81f8_943cu32 as _, &mut args)
        })?;
        let uuid = |bytes| {
            let id = uuid::Uuid::from_bytes(bytes);
            (!id.is_nil()).then_some(id)
        };
        Ok((
            args.treeid,
            SubvolumeInfo {
                uuid: uuid(args.uuid),
                parent_uuid: uuid(args.parent_uuid),
                received_uuid: uuid(args.received_uuid),
            },
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        Err(io::ErrorKind::Unsupported.into())
    }
}

pub(crate) fn is_readonly(directory: &File) -> io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        let mut flags: u64 = 0;
        // SAFETY: GETFLAGS writes one u64 into this live buffer.
        ioctl_result(unsafe {
            libc::ioctl(directory.as_raw_fd(), 0x8008_9419u32 as _, &mut flags)
        })?;
        Ok(flags & 2 != 0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        Err(io::ErrorKind::Unsupported.into())
    }
}

pub(crate) fn set_readonly(directory: &File, readonly: bool) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let mut flags: u64 = 0;
        // SAFETY: GETFLAGS writes one u64 into this live buffer.
        ioctl_result(unsafe {
            libc::ioctl(directory.as_raw_fd(), 0x8008_9419u32 as _, &mut flags)
        })?;
        let next = if readonly { flags | 2 } else { flags & !2 };
        if flags != next {
            flags = next;
            // SAFETY: SETFLAGS reads one u64 from this live buffer.
            ioctl_result(unsafe { libc::ioctl(directory.as_raw_fd(), 0x4008_941a, &flags) })?;
        }
        // Also finish an interrupted transition whose flag changed but whose
        // UUID clear failed. Stamping requires the subvolume to be writable.
        if !readonly && info(directory)?.1.received_uuid.is_some() {
            stamp(directory, uuid::Uuid::nil())?;
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (directory, readonly);
        Err(io::ErrorKind::Unsupported.into())
    }
}

pub(crate) fn stamp(directory: &File, uuid: uuid::Uuid) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        pplx_space_common_core::host::btrfs::set_received_uuid_on_fd(directory, uuid)
            .map_err(Into::into)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (directory, uuid);
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// Called only by admitted, tracked blocking work. Kernel I/O owns the
/// descriptors until completion, including when the RPC waiter disappears.
pub(crate) fn send(parent: &File, source: &File, output: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let parent_id = info(parent)?.0;
        #[repr(C)]
        struct SendArgs {
            send_fd: i64,
            clone_count: u64,
            clones: *const u64,
            parent: u64,
            flags: u64,
            version: u32,
            reserved: [u8; 28],
        }
        let args = SendArgs {
            send_fd: i64::from(output.as_raw_fd()),
            clone_count: 1,
            clones: &parent_id,
            parent: parent_id,
            flags: 0,
            version: 0,
            reserved: [0; 28],
        };
        // Protocol v1 is the existing portable stream, with a complete header
        // and end marker. The sole clone parent remains pinned throughout.
        // SAFETY: SendArgs matches SEND's UAPI; its clone pointer references
        // parent_id and all three descriptors stay borrowed through the call.
        ioctl_result(unsafe { libc::ioctl(source.as_raw_fd(), 0x4048_9426, &args) })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (parent, source, output);
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(target_os = "linux")]
fn ioctl_result(result: libc::c_int) -> io::Result<()> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct VolumeArgs {
    fd: i64,
    transid: u64,
    flags: u64,
    unused: [u64; 4],
    name: [u8; 4040],
}

#[cfg(target_os = "linux")]
impl VolumeArgs {
    fn new(name: &OsStr) -> io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 255
            || bytes.contains(&b'/')
            || bytes.contains(&0)
            || matches!(bytes, b"." | b"..")
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut args = Self {
            fd: 0,
            transid: 0,
            flags: 0,
            unused: [0; 4],
            name: [0; 4040],
        };
        args.name
            .get_mut(..bytes.len())
            .ok_or(io::ErrorKind::InvalidInput)?
            .copy_from_slice(bytes);
        Ok(args)
    }
}

#[cfg(target_os = "linux")]
#[derive(Default)]
#[repr(C)]
struct Info {
    treeid: u64,
    name: [u64; 32],
    parent_id: u64,
    dirid: u64,
    generation: u64,
    flags: u64,
    uuid: [u8; 16],
    parent_uuid: [u8; 16],
    received_uuid: [u8; 16],
    transids: [u64; 4],
    times: [u64; 8],
    reserved: [u64; 8],
}

#[cfg(target_os = "linux")]
const _: () = {
    assert!(std::mem::size_of::<VolumeArgs>() == 4096);
    assert!(std::mem::size_of::<Info>() == 504);
};
