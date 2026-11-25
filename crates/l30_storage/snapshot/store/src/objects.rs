//! The S3 objects under a snapshot's storage prefix.
//!
//! A node uploads each suspend attempt's files under its own prefix, and a
//! snapshot row's storage location names that prefix. This reads and writes
//! single files there on [`S3ObjectStore`], with credentials resolved from
//! the default chain per call: the pod's workload identity on EKS. The
//! object-store methods take static credentials, so resolving per call keeps
//! a long-lived process from holding an expired session.

use std::io::Read;

use pplx_space_artifact_model::StorageUri;
use pplx_space_object_store::{ObjectCredentials, ObjectStore, ObjectTarget, S3ObjectStore};

/// The largest decoded object [`SnapshotObjects::read_small`] accepts. A
/// real `metadata.json` is a few KiB; the cap keeps a corrupt or hostile
/// compressed object from expanding without bound in this process.
pub const MAX_SMALL_OBJECT_BYTES: u64 = 1024 * 1024;

/// The zstd frame magic. A node's volume manager may zstd-compress every
/// file it uploads, `metadata.json` included, and readers sniff for this.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// A snapshot object operation failed: credentials, S3 or decoding.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SnapshotObjectsError(String);

impl SnapshotObjectsError {
    // `impl Display` takes any error by value; `to_string` copies its
    // message out, so the error type doesn't carry the AWS error types.
    fn from_display(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

/// Files under snapshot prefixes in S3.
pub struct SnapshotObjects {
    store: S3ObjectStore,
}

impl Default for SnapshotObjects {
    fn default() -> Self {
        Self::new()
    }
}

fn target<'a>(prefix: &'a StorageUri, key: &'a str) -> ObjectTarget<'a> {
    ObjectTarget {
        bucket: &prefix.bucket,
        key,
        region: &prefix.region,
        endpoint: prefix.endpoint.as_deref(),
        path_style: prefix.path_style,
    }
}

async fn credentials() -> Result<ObjectCredentials, SnapshotObjectsError> {
    ObjectCredentials::from_default_chain()
        .await
        .map_err(SnapshotObjectsError::from_display)
}

fn over_the_cap() -> SnapshotObjectsError {
    SnapshotObjectsError(format!("object exceeds {MAX_SMALL_OBJECT_BYTES} bytes"))
}

/// `body` with one zstd frame undone, or as-is when it isn't compressed;
/// either way, refused past [`MAX_SMALL_OBJECT_BYTES`].
pub(crate) fn decode(body: Vec<u8>) -> Result<Vec<u8>, SnapshotObjectsError> {
    if !body.starts_with(&ZSTD_MAGIC) {
        if body.len() as u64 > MAX_SMALL_OBJECT_BYTES {
            return Err(over_the_cap());
        }
        return Ok(body);
    }
    let decoder = zstd::Decoder::new(body.as_slice())
        .map_err(|error| SnapshotObjectsError(format!("zstd: {error}")))?;
    let mut decoded = Vec::new();
    // One byte past the cap tells an object at the cap from one over it.
    decoder
        .take(MAX_SMALL_OBJECT_BYTES + 1)
        .read_to_end(&mut decoded)
        .map_err(|error| SnapshotObjectsError(format!("zstd: {error}")))?;
    if decoded.len() as u64 > MAX_SMALL_OBJECT_BYTES {
        return Err(over_the_cap());
    }
    Ok(decoded)
}

impl SnapshotObjects {
    pub fn new() -> Self {
        Self {
            store: S3ObjectStore::new(),
        }
    }

    /// The size of the object at `key` under `prefix`, or `None` when it's
    /// absent.
    pub async fn size(
        &self,
        prefix: &StorageUri,
        key: &str,
    ) -> Result<Option<u64>, SnapshotObjectsError> {
        let creds = credentials().await?;
        let meta = self
            .store
            .head(&creds, target(prefix, key))
            .await
            .map_err(SnapshotObjectsError::from_display)?;
        Ok(meta.map(|meta| meta.size))
    }

    /// A small object's bytes, zstd-decoded when the node compressed it and
    /// capped at [`MAX_SMALL_OBJECT_BYTES`] both stored and decoded.
    pub async fn read_small(
        &self,
        prefix: &StorageUri,
        key: &str,
    ) -> Result<Vec<u8>, SnapshotObjectsError> {
        let creds = credentials().await?;
        // `get_bytes` buffers the whole body, so the stored size is checked
        // first. That bounds the read unless the object is replaced between
        // the HEAD and the GET.
        let stored = self
            .store
            .head(&creds, target(prefix, key))
            .await
            .map_err(SnapshotObjectsError::from_display)?
            .ok_or_else(|| SnapshotObjectsError(format!("no {key} object")))?
            .size;
        if stored > MAX_SMALL_OBJECT_BYTES {
            return Err(over_the_cap());
        }
        let body = self
            .store
            .get_bytes(&creds, target(prefix, key))
            .await
            .map_err(SnapshotObjectsError::from_display)?;
        decode(body)
    }

    /// A server-side copy of `size` bytes from `source_key` under `source`
    /// to `dest_key` under `dest`.
    pub async fn copy(
        &self,
        source: &StorageUri,
        source_key: &str,
        dest: &StorageUri,
        dest_key: &str,
        size: u64,
    ) -> Result<(), SnapshotObjectsError> {
        let creds = credentials().await?;
        self.store
            .copy_object(
                &creds,
                target(source, source_key),
                target(dest, dest_key),
                size,
            )
            .await
            .map_err(SnapshotObjectsError::from_display)
    }

    /// Writes `body` to `key` under `prefix`, uncompressed.
    pub async fn write(
        &self,
        prefix: &StorageUri,
        key: &str,
        body: Vec<u8>,
    ) -> Result<(), SnapshotObjectsError> {
        let creds = credentials().await?;
        self.store
            .put_bytes(&creds, target(prefix, key), body.into())
            .await
            .map_err(SnapshotObjectsError::from_display)
    }
}

#[cfg(test)]
mod tests;
