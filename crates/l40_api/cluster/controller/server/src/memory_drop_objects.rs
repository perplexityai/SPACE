//! The S3 objects the memory drop copies through.
//!
//! The api crate owns the memory drop and stays free of AWS dependencies;
//! it asks for four operations on one snapshot prefix through
//! [`MemoryDropObjects`]. The snapshot store owns those objects, so this
//! only adapts its [`SnapshotObjects`] to that trait: it names each file's
//! key under the prefix and carries the store's error message across.
//!
//! Off by default; `SPACE_MEMORY_DROP_ENABLED=true` turns it on. The API reads
//! the same variable to decide whether its route accepts a drop.

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use pplx_space_api::controllers::sandbox::memory_drop::{
    MEMORY_DROP_ENABLED_VAR, MemoryDrop, MemoryDropObjects, MemoryDropObjectsError, object_key,
};
use pplx_space_artifact_model::{SnapshotArtifact as SnapshotFile, StorageUri};
use pplx_space_snapshot_store::objects::{SnapshotObjects, SnapshotObjectsError};

/// The memory drop, when `SPACE_MEMORY_DROP_ENABLED` is set.
pub(crate) fn from_env() -> Option<MemoryDrop> {
    pplx_space_env_vars::env_flag_var(MEMORY_DROP_ENABLED_VAR, false).then(|| {
        // `Arc::new` turns the concrete adapter into the shared trait object
        // the executor holds: `Arc<S3MemoryDropObjects>` coerces to
        // `Arc<dyn MemoryDropObjects>` at this call.
        MemoryDrop::new(std::sync::Arc::new(S3MemoryDropObjects(
            SnapshotObjects::new(),
        )))
    })
}

/// A newtype: Rust's orphan rule forbids implementing the api crate's trait
/// for the snapshot store's type here, since neither is local, so the
/// wrapper is the local type the impl hangs on.
struct S3MemoryDropObjects(SnapshotObjects);

fn objects_error(error: SnapshotObjectsError) -> MemoryDropObjectsError {
    MemoryDropObjectsError(error.to_string())
}

impl MemoryDropObjects for S3MemoryDropObjects {
    fn size<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Option<u64>, MemoryDropObjectsError>> {
        async move {
            let key = object_key(prefix, file);
            self.0.size(prefix, &key).await.map_err(objects_error)
        }
        .boxed()
    }

    fn read<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
    ) -> BoxFuture<'a, Result<Vec<u8>, MemoryDropObjectsError>> {
        async move {
            let key = object_key(prefix, file);
            self.0.read_small(prefix, &key).await.map_err(objects_error)
        }
        .boxed()
    }

    fn copy<'a>(
        &'a self,
        source: &'a StorageUri,
        dest: &'a StorageUri,
        file: SnapshotFile,
        size: u64,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>> {
        async move {
            let source_key = object_key(source, file);
            let dest_key = object_key(dest, file);
            self.0
                .copy(source, &source_key, dest, &dest_key, size)
                .await
                .map_err(objects_error)
        }
        .boxed()
    }

    fn write<'a>(
        &'a self,
        prefix: &'a StorageUri,
        file: SnapshotFile,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), MemoryDropObjectsError>> {
        async move {
            let key = object_key(prefix, file);
            self.0
                .write(prefix, &key, body)
                .await
                .map_err(objects_error)
        }
        .boxed()
    }
}
