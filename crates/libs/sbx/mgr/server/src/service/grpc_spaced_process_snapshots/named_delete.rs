//! DeleteSnapshots: named snapshots by name or all at once, through
//! [`crate::local_artifacts::LocalArtifacts::delete_named_snapshot`].

use super::*;
use crate::local_artifacts::{CheckoutError, NamedSnapshotDelete};
use crate::snapshot_gc::delete_gc_staging;
use pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir_in;

/// [`crate::local_artifacts::LocalArtifacts::delete_named_snapshot`] with
/// node-manager's DeleteCacheArtifact of `local_path` as the in-place delete of
/// a snapshot without a row.
async fn delete_cached_snapshot(
    service: &SandboxManagerService,
    (sandbox_id, snapshot_name): (&str, &str),
    local_path: String,
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> NamedSnapshotDelete {
    let request = pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
        local_path: local_path.clone(),
    };
    let delete_in_place = async { node_client.delete_cache_artifact(request).await.map(drop) };
    service
        .local_artifacts
        .delete_named_snapshot(sandbox_id, snapshot_name, &local_path, delete_in_place)
        .await
}

fn snapshot_delete_refusal(error: CheckoutError) -> Status {
    if error.is_in_use() {
        Status::failed_precondition(format!("snapshot is in use: {error}"))
    } else {
        Status::internal(format!("inspect snapshot publication: {error}"))
    }
}

impl SandboxManagerService {
    pub(in crate::service) async fn delete_snapshots(
        &self,
        request: Request<DeleteSnapshotsRequest>,
    ) -> Result<Response<DeleteSnapshotsResponse>, Status> {
        self.authorize_peer(&request, "DeleteSnapshots", ALLOW_SANDBOX_OPERATOR)?;
        let mut node_client = None;
        let mut staged = Vec::new();
        let result = self
            .delete_snapshots_locked(request.get_ref(), &mut node_client, &mut staged)
            .await;
        // The deleted rows owned this staging, so nothing does now and its
        // delete waits for no lock. Under the artifact lock it would hold the
        // sandbox's publication, rollback, export and backup enqueue behind
        // node-manager's deletion lane. A failure is the orphan sweep's.
        if let Some(client) = node_client.as_mut() {
            for (snapshot_name, staging) in &staged {
                let _ = delete_gc_staging(client, staging, "DeleteSnapshots", snapshot_name).await;
            }
        }
        result
    }

    /// The deletes under the sandbox's artifact lock. Each catalogued
    /// snapshot's staging, with its name, goes to `staged` for the caller to
    /// delete once the lock is released, on every return path.
    async fn delete_snapshots_locked(
        &self,
        req: &DeleteSnapshotsRequest,
        node_client: &mut Option<pplx_space_node_mgr_client::NodeManagerGrpcClient>,
        staged: &mut Vec<(String, String)>,
    ) -> Result<Response<DeleteSnapshotsResponse>, Status> {
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _guard = artifact_lock.lock().await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.require_snapshot_mutation_ownership(&sandbox.id)?;
        let protected_instance_path = sandbox.current_local_artifact_id.and_then(|artifact_id| {
            self.local_artifacts
                .get(artifact_id)
                .ok()
                .flatten()
                .filter(|artifact| {
                    artifact.artifact_origin
                        != pplx_space_common_core::types::ArtifactOrigin::Snapshot
                })
                .map(|artifact| artifact.local_path)
        });
        // A test's own root, or the production cache.
        let cache_root = self.cache_root().to_string_lossy();

        let node_client = node_client.insert(
            connect_node(&self.node_manager)
                .await
                .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?,
        );

        if req.all {
            let mut deleted = Vec::new();

            // Delete btrfs snapshots (d-* subvolumes) via node-manager
            let sandbox_id = sandbox
                .id
                .parse::<SandboxId>()
                .map_err(|error| Status::internal(format!("invalid stored sandbox id: {error}")))?;
            let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox_id, false);
            while let Some(page) = pages.next(node_client).await? {
                for snap in page {
                    if is_internal_snapshot_name(&snap.name) {
                        continue;
                    }
                    let local_path = sandbox_snapshot_dir_in(&cache_root, &sandbox.id, &snap.name);
                    if protected_instance_path.as_deref() == Some(local_path.as_str()) {
                        continue;
                    }
                    if deleted.len() >= 4096 {
                        return Err(Status::resource_exhausted(
                            "deleted 4096 snapshots; retry delete-all to continue",
                        ));
                    }
                    let names = (sandbox.id.as_str(), snap.name.as_str());
                    match delete_cached_snapshot(self, names, local_path, node_client).await {
                        NamedSnapshotDelete::Deleted { staging } => {
                            staged.extend(staging.map(|staging| (snap.name.clone(), staging)));
                            deleted.push(snap.name);
                        }
                        NamedSnapshotDelete::BytesNotDeleted(status) => return Err(status),
                        // A refused row or a differing path: kept and left out of
                        // `deleted`, as delete-all did before rows were checked out.
                        kept => tracing::warn!(
                            target: super::LOG_TARGET,
                            sandbox_id = %sandbox.id,
                            snapshot_name = %snap.name,
                            outcome = ?kept,
                            "delete-all retains a snapshot it cannot delete",
                        ),
                    }
                }
            }
            // Note: list_btrfs_snapshots returns both d-* and f-* entries, so the loop
            // above already covers full snapshots. No separate f-* scan needed.

            Ok(Response::new(DeleteSnapshotsResponse { deleted }))
        } else if let Some(snapshot_name) = req.snapshot_name.as_deref().filter(|s| !s.is_empty()) {
            validate_snapshot_name(snapshot_name)?;
            if is_internal_snapshot_name(snapshot_name) {
                return Err(Status::failed_precondition(
                    "cannot delete an internal snapshot artifact",
                ));
            }
            let local_path = sandbox_snapshot_dir_in(&cache_root, &sandbox.id, snapshot_name);
            if protected_instance_path.as_deref() == Some(local_path.as_str()) {
                return Err(Status::failed_precondition(
                    "cannot delete the current instance artifact",
                ));
            }

            // Both disk and full snapshots may be btrfs subvolumes —
            // delete via node-manager (root). DeleteCacheArtifact
            // handles both the subvolume case (privileged
            // `btrfs subvolume delete`) and the plain-directory case
            // (of GC staging, for a catalogued snapshot, after the lock is
            // released), and is idempotent on a missing path.
            let names = (sandbox.id.as_str(), snapshot_name);
            match delete_cached_snapshot(self, names, local_path, node_client).await {
                NamedSnapshotDelete::Deleted { staging } => {
                    staged.extend(staging.map(|staging| (snapshot_name.to_owned(), staging)));
                }
                NamedSnapshotDelete::Refused(error) => return Err(snapshot_delete_refusal(error)),
                NamedSnapshotDelete::PathDiffers { .. } => {
                    return Err(Status::failed_precondition(
                        "delete snapshot publication path differs",
                    ));
                }
                NamedSnapshotDelete::BytesNotDeleted(e) => {
                    return Err(Status::internal(format!("delete snapshot failed: {e}")));
                }
            }

            Ok(Response::new(DeleteSnapshotsResponse {
                deleted: vec![snapshot_name.to_string()],
            }))
        } else {
            Err(Status::invalid_argument(
                "provide snapshot_name or set all=true",
            ))
        }
    }
}
