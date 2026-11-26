//! Cluster-controller entry point for the S3-backed memory drop.

use pplx_space_api::controllers::sandbox::memory_drop::MemoryDrop;

/// The memory drop, when `SPACE_MEMORY_DROP_ENABLED` is set.
pub(crate) fn from_env() -> Option<MemoryDrop> {
    MemoryDrop::from_env()
}
