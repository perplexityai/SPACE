//! S3-backed [`MemoryDropObjects`] adapter shared by the API server (for
//! restore-time memory drops) and the cluster controller (for off-node drops).

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use pplx_space_artifact_model::{SnapshotArtifact as SnapshotFile, StorageUri};
use pplx_space_snapshot_store::objects::{SnapshotObjects, SnapshotObjectsError};

use super::{
    MEMORY_DROP_ENABLED_VAR, MemoryDrop, MemoryDropObjects, MemoryDropObjectsError, object_key,
};

impl MemoryDrop {
    /// Construct a [`MemoryDrop`] backed by the snapshot object store when
    /// `SPACE_MEMORY_DROP_ENABLED=true`.
    pub fn from_env() -> Option<Self> {
        pplx_space_env_vars::env_flag_var(MEMORY_DROP_ENABLED_VAR, false)
            .then(|| Self::new(Arc::new(S3MemoryDropObjects(SnapshotObjects::new()))))
    }
}

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
