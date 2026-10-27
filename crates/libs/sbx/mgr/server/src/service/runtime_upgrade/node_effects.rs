//! The production [`RuntimeUpgradeEffects`]: node-manager RPCs for the
//! preserved copy, the service's own Start and Stop for the lifecycle, and the
//! local-artifact catalog for checkpoint retirement.

use std::os::unix::fs::MetadataExt;
use std::path::Path;

use pplx_space_common_core::types::{
    EnvironmentReference, ExecutionMetadata, LocalArtifactId, SandboxActivationAttemptId,
};
use pplx_space_node_mgr_client::proto::{
    CreateBtrfsSnapshotRequest, DeleteBtrfsSnapshotRequest, RestoreInstanceSnapshotRequest,
};
use tonic::metadata::MetadataMap;
use tonic::{Extensions, Request, Status};

use super::{LocalDisk, RuntimeUpgradeEffects};
use crate::db::{RuntimeUpgradeOperationId, RuntimeUpgradeRecord, SandboxRow};
use crate::local_artifacts::CheckoutError;
use crate::proto::{StartRequest, StartResponse};
use crate::service::SandboxManagerService;
use crate::service::cleanup_support::read_node_execution_fence;
use crate::service::suspend_snapshot_publication::{
    canonical_suspend_snapshot_dir, garbage_suspend_snapshot_dir,
    suspend_snapshot_staging_parent_dir,
};
use crate::service::tail_support::{checkpoint_execution_metadata, sandbox_execution_metadata_in};
use crate::state::SandboxState;

/// How long retirement waits for a restore, export or publication holding the
/// sandbox's artifacts, so one busy sandbox can't stall the recovery loop.
const RETIRE_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

pub(in crate::service) struct NodeRuntimeUpgradeEffects {
    service: SandboxManagerService,
    /// The `UpgradeRuntime` caller's metadata and peer extensions. The
    /// upgrade's Start runs as that caller, through Start's authorization; its
    /// guarded stop needs no more than the caller already proved, since the
    /// upgrade's principals are a subset of Stop's. The recovery loop has no
    /// caller and never boots or stops anything.
    caller: Option<(MetadataMap, Extensions)>,
}

impl NodeRuntimeUpgradeEffects {
    pub(in crate::service) fn for_caller(
        service: SandboxManagerService,
        metadata: MetadataMap,
        extensions: Extensions,
    ) -> Self {
        Self {
            service,
            caller: Some((metadata, extensions)),
        }
    }

    pub(in crate::service) fn for_recovery(service: SandboxManagerService) -> Self {
        Self {
            service,
            caller: None,
        }
    }

    fn caller_request<T>(&self, message: T) -> Result<Request<T>, Status> {
        let (metadata, extensions) = self.caller.clone().ok_or_else(|| {
            Status::failed_precondition("runtime upgrade recovery never starts or stops a sandbox")
        })?;
        Ok(Request::from_parts(metadata, extensions, message))
    }

    fn node(&self) -> pplx_space_node_mgr_client::NodeManagerGrpcClient {
        self.service.node_manager.lazy()
    }

    /// Hand retired bytes to the bounded immediate reclaim, which removes
    /// them locally or through node-manager. Whatever it can't remove stays
    /// where a sweep finds it.
    fn reclaim(&self, sandbox_id: &str, retired: std::path::PathBuf) {
        drop(
            crate::service::recovery::staging_cleanup::reclaim_garbage_now(
                sandbox_id.to_string(),
                retired,
                self.service.node_manager.clone(),
            ),
        );
    }
}

/// Device, inode and birth time: a subvolume root's inode is always 256 and
/// a freed device number is reused, so those two alone can match a newer
/// directory. Filesystems without a birth time fall back to the change time.
fn directory_identity(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            let born = metadata
                .created()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or_else(
                    || format!("c{}.{}", metadata.ctime(), metadata.ctime_nsec()),
                    |since| format!("b{}", since.as_nanos()),
                );
            Ok(Some(format!(
                "{}:{}:{born}",
                metadata.dev(),
                metadata.ino()
            )))
        }
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn required_file(path: &Path, what: &str) -> Result<(), Status> {
    match path.try_exists() {
        Ok(true) => Ok(()),
        Ok(false) => Err(Status::failed_precondition(format!(
            "a runtime upgrade needs the sandbox's local {what}; {} is absent",
            path.display()
        ))),
        Err(error) => Err(Status::failed_precondition(format!(
            "inspect {}: {error}",
            path.display()
        ))),
    }
}

/// The superseded checkpoint can't be told apart from bytes another catalog
/// row owns. Nothing is deleted, and the upgrade stays verified, refusing
/// warm start and restore, until the conflict clears: the sandbox's next
/// suspend replaces every older suspend row, or an operator resolves it.
fn catalog_conflict(sandbox_id: &str, detail: &str) -> Status {
    tracing::warn!(
        sandbox_id,
        detail,
        "superseded checkpoint has conflicting catalog rows; leaving it unretired"
    );
    Status::failed_precondition(format!(
        "sandbox {sandbox_id}'s superseded checkpoint has conflicting catalog rows ({detail}); \
         it stays in place and unretired"
    ))
}

impl NodeRuntimeUpgradeEffects {
    /// Where the sandbox's `metadata.json` is kept while the instance is
    /// preserved under `name`. The boot rewrites that record of the disk's
    /// execution, and it lives outside the instance subvolume the preserved
    /// copy holds; backup and export read it.
    fn preserved_metadata(&self, sandbox_id: &str, name: &str) -> std::path::PathBuf {
        self.service
            .cache_root()
            .join(sandbox_id)
            .join(format!(".metadata.json.{name}"))
    }

    /// Retire one catalogued checkpoint through a delete lease, as the reaper
    /// does, returning where its bytes went. The caller holds the artifact
    /// lock.
    fn retire_catalogued(
        &self,
        sandbox_id: &str,
        artifact: LocalArtifactId,
    ) -> Result<Option<std::path::PathBuf>, Status> {
        let mut lease = match self.service.local_artifacts.check_out_for_delete(artifact) {
            Ok(lease) => lease,
            // Superseded by a newer suspend, or already retired.
            Err(CheckoutError::NotFound(_)) => return Ok(None),
            // Another row claims the same bytes, so they aren't identifiably
            // this checkpoint's. Leave them, as the reaper does, and stay
            // unretired: a warm start or restore would otherwise load them.
            Err(error @ CheckoutError::SharedLocalPath { .. }) => {
                return Err(catalog_conflict(sandbox_id, &error.to_string()));
            }
            Err(error) if error.is_in_use() => {
                return Err(Status::unavailable(format!(
                    "retire checkpoint {artifact}: {error}"
                )));
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "retire checkpoint {artifact}: {error}"
                )));
            }
        };
        // A rename error drops the lease, which returns the row to `ready`.
        let staged = lease
            .vacate_to_gc_staging()
            .map_err(|error| Status::internal(format!("retire checkpoint {artifact}: {error}")))?;
        lease
            .commit()
            .map_err(|error| Status::internal(format!("retire checkpoint {artifact}: {error}")))?;
        Ok(staged.map(std::path::PathBuf::from))
    }
}

#[tonic::async_trait]
impl RuntimeUpgradeEffects for NodeRuntimeUpgradeEffects {
    async fn inspect_local_disk(&self, sandbox: &SandboxRow) -> Result<LocalDisk, Status> {
        let cache_root = self.service.cache_root();
        crate::service::create_support::repair_retained_instance_traversal(
            &self.service.node_manager,
            cache_root,
            &sandbox.id,
        )
        .await?;
        let sandbox_dir = cache_root.join(&sandbox.id);
        let instance = sandbox_dir.join("instance");
        let rootfs = instance.join("rootfs.ext4");
        required_file(&rootfs, "rootfs")?;
        if sandbox.volume_encryption {
            // Without the COW the encrypted cold start has nothing to reopen.
            required_file(&instance.join("cow.luks"), "encrypted rootfs COW")?;
        }
        let execution = sandbox_execution_metadata_in(cache_root, &sandbox.id)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        // A stopped sandbox can keep the checkpoint of a suspend before its
        // stop, which a warm start would load over the disk the boot writes,
        // so its identity is recorded for retirement too.
        let checkpoint = canonical_suspend_snapshot_dir(&sandbox_dir);
        let checkpoint_dir_identity = directory_identity(&checkpoint).map_err(|error| {
            Status::failed_precondition(format!("inspect {}: {error}", checkpoint.display()))
        })?;
        let checkpoint_execution = if sandbox.state == SandboxState::Suspended {
            Some(
                checkpoint_execution_metadata(&checkpoint.to_string_lossy())
                    .map_err(|error| Status::failed_precondition(error.to_string()))?
                    .into_execution(),
            )
        } else {
            None
        };
        Ok(LocalDisk {
            execution,
            checkpoint_execution,
            checkpoint_dir_identity,
        })
    }

    async fn preserve_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status> {
        // A copy left by an interrupted attempt of this operation predates no
        // boot, but replacing it keeps the copy exactly the current instance.
        self.discard_disk(sandbox_id, name).await?;
        let metadata = self
            .service
            .cache_root()
            .join(sandbox_id)
            .join("metadata.json");
        let bytes = std::fs::read(&metadata).map_err(|error| {
            Status::failed_precondition(format!("read {}: {error}", metadata.display()))
        })?;
        crate::snapshot_scheduler::write_file_atomic(
            self.preserved_metadata(sandbox_id, name),
            &bytes,
        )
        .map_err(|error| Status::internal(format!("preserve {}: {error}", metadata.display())))?;
        self.node()
            .create_btrfs_snapshot(CreateBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: name.to_string(),
                writable: false,
                execution_fence: read_node_execution_fence(&self.service.db, sandbox_id)?
                    .map(Into::into),
            })
            .await?;
        Ok(())
    }

    async fn restore_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status> {
        self.node()
            .restore_instance_snapshot(RestoreInstanceSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: name.to_string(),
                execution_fence: read_node_execution_fence(&self.service.db, sandbox_id)?
                    .map(Into::into),
            })
            .await?;
        let preserved = self.preserved_metadata(sandbox_id, name);
        match std::fs::read(&preserved) {
            Ok(bytes) => crate::snapshot_scheduler::write_file_atomic(
                self.service
                    .cache_root()
                    .join(sandbox_id)
                    .join("metadata.json"),
                &bytes,
            )
            .map_err(|error| Status::internal(format!("restore metadata.json: {error}"))),
            // Kept before the instance copy, so without it nothing booted.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Status::internal(format!(
                "read {}: {error}",
                preserved.display()
            ))),
        }
    }

    async fn discard_disk(&self, sandbox_id: &str, name: &str) -> Result<(), Status> {
        match self
            .node()
            .delete_btrfs_snapshot(DeleteBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: name.to_string(),
            })
            .await
        {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::NotFound => {}
            Err(status) => return Err(status),
        }
        let preserved = self.preserved_metadata(sandbox_id, name);
        match std::fs::remove_file(&preserved) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Status::internal(format!(
                "discard {}: {error}",
                preserved.display()
            ))),
        }
    }

    async fn resolve_target(
        &self,
        target: EnvironmentReference,
    ) -> Result<ExecutionMetadata, Status> {
        let mut node = self.node();
        crate::service::environment_selection::resolve_exact(&mut node, target)
            .await
            .map(|selected| selected.execution)
    }

    async fn cold_boot(
        &self,
        operation_id: RuntimeUpgradeOperationId,
        start: StartRequest,
    ) -> Result<StartResponse, Status> {
        self.service
            .start_with_runtime_upgrade(self.caller_request(start)?, Some(operation_id))
            .await
            .map(tonic::Response::into_inner)
    }

    async fn stop(
        &self,
        sandbox_id: &str,
        execution: SandboxActivationAttemptId,
    ) -> Result<(), Status> {
        // The upgrade's caller holds Stop's authority (its principals are a
        // subset of Stop's), which the recovery loop has none of.
        if self.caller.is_none() {
            return Err(Status::failed_precondition(
                "runtime upgrade recovery never starts or stops a sandbox",
            ));
        }
        self.service
            .stop_activation(sandbox_id, execution)
            .await
            .map(drop)
    }

    /// Snapshot rollback, export and checkpoint publication take the artifact
    /// lock before they touch the instance or the checkpoint.
    async fn exclude_disk_mutations(&self, sandbox_id: &str) -> Box<dyn Send> {
        Box::new(
            self.service
                .auto_lifecycle
                .artifact_lock(sandbox_id)
                .lock_owned()
                .await,
        )
    }

    /// Retire the checkpoint the upgrade superseded, by identity only: the
    /// catalog row it pinned when fenced, or for an uncatalogued legacy
    /// checkpoint the directory it saw. A newer checkpoint at the same path
    /// has another identity and is left alone. Moving the bytes out of the
    /// checkpoint path is the durable retirement: a catalogued checkpoint goes
    /// to the reaper's GC staging, an uncatalogued one to the suspend-garbage
    /// lane, and each lane's sweep deletes whatever the immediate reclaim
    /// leaves.
    async fn retire_checkpoint(&self, record: &RuntimeUpgradeRecord) -> Result<(), Status> {
        if record.checkpoint_artifact_id.is_none() && record.checkpoint_dir_identity.is_none() {
            return Ok(());
        }
        // Excludes suspend publication, restore staging and the reaper, as the
        // reaper's own deletions do.
        let _artifact = tokio::time::timeout(
            RETIRE_LOCK_WAIT,
            self.service
                .auto_lifecycle
                .artifact_lock(&record.sandbox_id)
                .lock_owned(),
        )
        .await
        .map_err(|_| {
            Status::unavailable("the sandbox's artifacts are busy; retirement retries later")
        })?;
        let retired = match record.checkpoint_artifact_id {
            Some(artifact) => self.retire_catalogued(&record.sandbox_id, artifact)?,
            None => {
                let sandbox_dir = self.service.cache_root().join(&record.sandbox_id);
                let checkpoint = canonical_suspend_snapshot_dir(&sandbox_dir);
                let current = directory_identity(&checkpoint).map_err(|error| {
                    Status::internal(format!("inspect {}: {error}", checkpoint.display()))
                })?;
                if current.is_none() || current != record.checkpoint_dir_identity {
                    return Ok(());
                }
                // The directory the upgrade saw. Catalogued since, it is
                // retired through its catalog row, never behind it.
                let catalogued = crate::db::read::local_artifacts_at_path(
                    &*self.service.db_conn()?,
                    &record.sandbox_id,
                    &checkpoint.to_string_lossy(),
                )
                .map_err(|error| Status::internal(format!("read checkpoint catalog: {error}")))?;
                match catalogued.as_slice() {
                    [] => {
                        let garbage = garbage_suspend_snapshot_dir(&sandbox_dir);
                        std::fs::create_dir_all(suspend_snapshot_staging_parent_dir(&sandbox_dir))
                            .and_then(|()| std::fs::rename(&checkpoint, &garbage))
                            .map_err(|error| {
                                Status::internal(format!(
                                    "retire {}: {error}",
                                    checkpoint.display()
                                ))
                            })?;
                        Some(garbage)
                    }
                    [artifact] => self.retire_catalogued(&record.sandbox_id, *artifact)?,
                    _ => {
                        return Err(catalog_conflict(
                            &record.sandbox_id,
                            &format!("{} catalog rows claim it", catalogued.len()),
                        ));
                    }
                }
            }
        };
        if let Some(retired) = retired {
            self.reclaim(&record.sandbox_id, retired);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
