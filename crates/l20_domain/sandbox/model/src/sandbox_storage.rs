//! Rootfs entitlement shared by admission and node-side materialization.
//!
//! This limits the writable guest disk, not snapshot amplification or aggregate
//! node occupancy. Allocation admission must still account for backing storage.

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

#[cfg(test)]
mod tests;
