#![doc = include_str!("../README.md")]

use pplx_space_common_core::types::StorageUri;

#[cfg(feature = "s3")]
pub mod s3;

/// Outcome of deleting one snapshot prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeletePrefixOutcome {
    Deleted { count: u64 },
    AlreadyGone,
}

#[derive(Debug, thiserror::Error)]
pub enum DeletePrefixError {
    #[error("object store delete failed: {0}")]
    Backend(String),
}

/// Backend-neutral operation for deleting the bytes below a parsed snapshot
/// prefix. Partial backend deletion must return an error.
pub trait StorageBackend: Send + Sync + 'static {
    fn delete_prefix(
        &self,
        uri: &StorageUri,
    ) -> impl std::future::Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send;
}
