//! Read-only inspection on an already pinned descriptor. These ioctls neither
//! change filesystem features nor modify a subvolume or its contents.

use std::{fs::File, io};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadonlySubvolumeIdentity {
    pub uuid: uuid::Uuid,
    pub tree_id: u64,
    pub generation: u64,
}

pub fn readonly_subvolume_identity(directory: &File) -> io::Result<ReadonlySubvolumeIdentity> {
    #[cfg(target_os = "linux")]
    {
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        let metadata = directory.metadata()?;
        if !metadata.is_dir()
            || metadata.ino() != 256
            || nix::sys::statfs::fstatfs(directory)?.filesystem_type()
                != nix::sys::statfs::BTRFS_SUPER_MAGIC
        {
            return Err(io::Error::other("source is not a btrfs subvolume root"));
        }
        let mut info = Info::default();
        // SAFETY: Info is the complete 504-byte GET_SUBVOL_INFO output buffer;
        // directory remains borrowed and the kernel only reads its descriptor.
        if unsafe { nix::libc::ioctl(directory.as_raw_fd(), 0x81f8_943cu32 as _, &mut info) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let uuid = uuid::Uuid::from_bytes(info.uuid);
        // GET_SUBVOL_INFO exposes root-item flags (BTRFS_ROOT_SUBVOL_RDONLY
        // = bit 0), unlike SUBVOL_GETFLAGS (BTRFS_SUBVOL_RDONLY = bit 1).
        const ROOT_SUBVOL_RDONLY: u64 = 1;
        if info.flags & ROOT_SUBVOL_RDONLY == 0 || uuid.is_nil() {
            return Err(io::Error::other(
                "source subvolume is writable or has no stable UUID",
            ));
        }
        Ok(ReadonlySubvolumeIdentity {
            uuid,
            tree_id: info.treeid,
            generation: info.generation,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        Err(io::ErrorKind::Unsupported.into())
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
const _: () = assert!(std::mem::size_of::<Info>() == 504);
