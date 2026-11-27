//! An in-memory [`ReleaseObjects`], for this crate's tests and the crates
//! that drive twins. Compiled for this crate's tests and behind the
//! `test-support` feature; enable it only from `[dev-dependencies]`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pplx_space_execution_spec::EnvironmentReference;

use crate::{ObjectAccessError, ReleaseObjects, TwinMetadataTarget};

/// Objects by `(bucket, key)`, with every write's key recorded in order.
#[derive(Default)]
pub struct MemoryReleaseObjects {
    objects: Mutex<BTreeMap<(String, String), Bytes>>,
    writes: Mutex<Vec<String>>,
    refused: Mutex<BTreeSet<String>>,
}

impl MemoryReleaseObjects {
    /// Store `body` at `key` in `bucket`, as a build's publication would.
    pub fn put(&self, bucket: &str, key: &str, body: impl Into<Bytes>) {
        self.lock_objects()
            .insert((bucket.to_owned(), key.to_owned()), body.into());
    }

    /// The object at `key` in `bucket`.
    pub fn get(&self, bucket: &str, key: &str) -> Option<Bytes> {
        self.lock_objects()
            .get(&(bucket.to_owned(), key.to_owned()))
            .cloned()
    }

    /// Fail every write to `bucket`, as an unreachable replica would.
    pub fn refuse_writes_to(&self, bucket: &str) {
        self.refused
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(bucket.to_owned());
    }

    /// The keys written through [`ReleaseObjects`], in order.
    pub fn writes(&self) -> Vec<String> {
        self.writes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn lock_objects(&self) -> std::sync::MutexGuard<'_, BTreeMap<(String, String), Bytes>> {
        self.objects
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

#[async_trait]
impl ReleaseObjects for MemoryReleaseObjects {
    async fn read_metadata(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError> {
        let key = target.shared_metadata_key();
        self.get(target.bucket(), &key)
            .map(|body| body.to_vec())
            .ok_or(ObjectAccessError::Store(
                pplx_space_object_store::ObjectStoreError::NotFound { key },
            ))
    }

    async fn read_manifest(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError> {
        let key = target.manifest_key();
        self.get(target.bucket(), &key)
            .map(|body| body.to_vec())
            .ok_or(ObjectAccessError::Store(
                pplx_space_object_store::ObjectStoreError::NotFound { key },
            ))
    }

    async fn write_twin_metadata(
        &self,
        target: &TwinMetadataTarget,
        environment: EnvironmentReference,
        document: Bytes,
    ) -> Result<(), ObjectAccessError> {
        let key = target.twin_metadata_key(environment)?;
        if self
            .refused
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains(target.bucket())
        {
            return Err(ObjectAccessError::Store(
                pplx_space_object_store::ObjectStoreError::Io(std::io::Error::other(
                    "the bucket refuses writes",
                )),
            ));
        }
        self.writes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(key.clone());
        self.put(target.bucket(), &key, document);
        Ok(())
    }
}
