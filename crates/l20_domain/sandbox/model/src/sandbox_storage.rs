//! Rootfs entitlement and local-disk completeness, shared by admission and
//! node-side materialization.
//!
//! The entitlement limits the writable guest disk, not snapshot amplification
//! or aggregate node occupancy. Allocation admission must still account for
//! backing storage.
//!
//! Completeness ([`restorable_disk`], [`bootable_encrypted_disk`]) is the one
//! definition of a sandbox disk a rollback can put back and a cold boot can
//! open. node-manager's instance rollback applies the first, and a runtime
//! upgrade's admission applies the second, which implies the first, so an
//! admitted disk is one its rollback restores.

use pplx_space_byte_size::DiskSize;

pub const MAX_SANDBOX_ROOTFS_BYTES: u64 = 128 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RootfsSizeError {
    #[error("rootfs size must be greater than zero")]
    Zero,
    #[error("rootfs size {requested_bytes} bytes exceeds the maximum {maximum_bytes} bytes")]
    TooLarge {
        requested_bytes: u64,
        maximum_bytes: u64,
    },
}

pub fn validate_rootfs_bytes(requested_bytes: u64) -> Result<DiskSize, RootfsSizeError> {
    if requested_bytes > MAX_SANDBOX_ROOTFS_BYTES {
        return Err(RootfsSizeError::TooLarge {
            requested_bytes,
            maximum_bytes: MAX_SANDBOX_ROOTFS_BYTES,
        });
    }
    DiskSize::b(requested_bytes).map_err(|_| RootfsSizeError::Zero)
}

pub fn validate_rootfs_mib(requested_mib: u32) -> Result<DiskSize, RootfsSizeError> {
    validate_rootfs_bytes(u64::from(requested_mib) * 1024 * 1024)
}

/// One file of a sandbox's local disk, as a reader found it without
/// following a final symlink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskFile {
    Absent,
    /// Present, but empty or not a regular file.
    Empty,
    /// A non-empty regular file.
    Present,
}

/// What a disk may consist of to replace an instance's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskForm {
    /// A non-empty `rootfs.ext4`.
    Rootfs,
    /// A non-empty `rootfs.ext4`, or no rootfs and a non-empty encrypted COW:
    /// an encrypted instance whose read-only origin is the template clone a
    /// cold start makes when absent. A runtime upgrade preserves such an
    /// instance for a checkpoint staged from its backup, whose guest writes
    /// are all in the COW.
    RootfsOrEncryptedCow,
}

/// Why a disk isn't complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IncompleteDisk {
    #[error("rootfs.ext4 is empty or not a regular file")]
    EmptyRootfs,
    #[error("there's no rootfs.ext4")]
    NoRootfs,
    #[error("the encrypted rootfs COW is empty or not a regular file")]
    EmptyCow,
    #[error("there's no encrypted rootfs COW")]
    NoCow,
    #[error(
        "there's no rootfs.ext4 and no template digest names the bytes the COW was written over"
    )]
    NoOrigin,
}

/// Whether a disk of `rootfs` and encrypted `cow` can replace an instance's,
/// as node-manager's rollback of a preserved copy requires.
pub fn restorable_disk(
    form: DiskForm,
    rootfs: DiskFile,
    cow: DiskFile,
) -> Result<(), IncompleteDisk> {
    match (rootfs, form, cow) {
        (DiskFile::Present, _, _) => Ok(()),
        (DiskFile::Empty, _, _) => Err(IncompleteDisk::EmptyRootfs),
        (DiskFile::Absent, DiskForm::RootfsOrEncryptedCow, DiskFile::Present) => Ok(()),
        (DiskFile::Absent, _, _) => Err(IncompleteDisk::NoRootfs),
    }
}

/// Whether an encrypted sandbox's disk can be cold-booted and, if the boot
/// fails, rolled back. The cold boot reopens the COW, so it must be there;
/// an absent rootfs is the template's, so a digest must name it; and the
/// whole must be [`restorable_disk`] as a COW-only copy.
pub fn bootable_encrypted_disk(
    rootfs: DiskFile,
    cow: DiskFile,
    origin_named: bool,
) -> Result<(), IncompleteDisk> {
    match cow {
        DiskFile::Present => {}
        DiskFile::Empty => return Err(IncompleteDisk::EmptyCow),
        DiskFile::Absent => return Err(IncompleteDisk::NoCow),
    }
    if rootfs == DiskFile::Absent && !origin_named {
        return Err(IncompleteDisk::NoOrigin);
    }
    restorable_disk(DiskForm::RootfsOrEncryptedCow, rootfs, cow)
}

#[cfg(test)]
mod tests;
