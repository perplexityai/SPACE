//! Completing a staged checkpoint's disk for the upgrade that boots it.
//!
//! Staging lays a suspend checkpoint down from its backup and leaves the
//! sandbox suspended without launching it. A restore would finish the disk at
//! launch; an upgrade boots it cold instead, so it reads the disk first
//! ([`super::RuntimeUpgradeEffects::inspect_local_disk`]), keeps a read-only
//! copy of the instance and its `metadata.json`, and rolls back to that copy.
//! Node-manager's `MaterializeStagedCheckpointDisk` finishes the parts a
//! staged checkpoint lacks:
//!
//! - **`metadata.json`**, the disk's record of its execution, which a launch
//!   writes. Staging writes none, so it's written from the checkpoint's
//!   producer, the execution that wrote the disk.
//! - **An encrypted sandbox's `instance/cow.luks`.** Its guest writes are in
//!   the checkpoint's `cow.luks`, which a restore hands node-manager to lay
//!   into the instance before opening. Node-manager lays it into a subvolume
//!   instance on every call, over whatever the instance holds, which may be
//!   an earlier boot's writes, so the copy the upgrade preserves and rolls
//!   back to is the pinned checkpoint's disk. The rootfs under it is the template's, which the cold
//!   start clones when absent, as a restore's launch does.
//!
//! Node-manager does the writes because an instance a launch already handed
//! to the VM owner isn't sandbox-manager's to write into. It all runs only
//! with [`crate::service::STAGED_CHECKPOINT_DISK_ENV`] on, and only against
//! a node-manager that advertises the RPC, checked on every call and never
//! cached across a reconnect: a sandbox-manager rolled out ahead of its
//! node-manager refuses the upgrade here, before anything on the node
//! changes and before the upgrade fences the sandbox.
//!
//! The upgrade does this under its exclusion of disk mutations, after its
//! own refusals and right before it reads the disk, so every attempt
//! finishes whatever a staging, or an earlier attempt, left undone,
//! whichever caller staged it. A `metadata.json` already there is kept. Whatever is already there, the checkpoint
//! must be the one the row pins: the upgrade's cold boot of a suspended
//! sandbox lays that checkpoint's COW again
//! (`rootfs_encryption::prepare_encrypted_rootfs`), so the guest boots the
//! checkpoint the upgrade was decided from.

use std::path::Path;

use pplx_space_node_mgr_client::proto::{
    CloneCapabilitiesRequest, MaterializeStagedCheckpointDiskRequest, StagedDiskPart,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use tonic::Status;

use crate::db::SandboxRow;
use crate::rootfs_encryption::path_present;
use crate::service::suspend_snapshot_publication::{
    ValidatedSuspendSnapshotMetadata, canonical_suspend_snapshot_dir,
    validate_suspend_snapshot_metadata,
};
use crate::state::SandboxState;

#[cfg(test)]
mod tests;

const RPC: &str = "MaterializeStagedCheckpointDisk";

/// Whether the node-manager behind `client` serves
/// `MaterializeStagedCheckpointDisk` and restores a preserved encrypted copy
/// holding a COW and no rootfs. One that predates `GetCloneCapabilities`
/// does neither.
pub(in crate::service) async fn supports_staged_checkpoint_disk(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> Result<bool, Status> {
    match client
        .get_clone_capabilities(CloneCapabilitiesRequest {})
        .await
    {
        Ok(capabilities) => Ok(capabilities.into_inner().staged_checkpoint_disk),
        Err(status) if status.code() == tonic::Code::Unimplemented => Ok(false),
        Err(status) => Err(Status::new(
            status.code(),
            format!("node-manager capability lookup: {}", status.message()),
        )),
    }
}

fn present(path: &Path) -> Result<bool, Status> {
    path_present(path).map_err(|error| {
        Status::failed_precondition(format!("inspect {}: {error}", path.display()))
    })
}

impl crate::service::SandboxManagerService {
    /// Finish the disk of the suspended checkpoint staged for `sandbox`'s
    /// upgrade, so the upgrade boots it as it would a copy suspended here.
    /// A sandbox that isn't suspended and unowned, or that an upgrade still
    /// fences or owes a retirement, is left to the upgrade, which settles or
    /// refuses it before it reads the disk.
    ///
    /// The caller read `sandbox` under the sandbox's artifact lock and still
    /// holds it; restore staging, publication, eviction and retirement take
    /// it too ([`super::RuntimeUpgradeEffects::exclude_disk_mutations`]).
    pub(in crate::service) async fn complete_staged_upgrade_disk(
        &self,
        sandbox: &SandboxRow,
    ) -> Result<(), super::Refusal> {
        if !self.staged_checkpoint_disk || !sandbox.state.is_unowned(SandboxState::Suspended) {
            return Ok(());
        }
        // Unretired covers every phase that fences the sandbox, and a
        // verified one still owes its checkpoint's retirement.
        let unretired = crate::db::read::unretired_runtime_upgrade(&*self.db_conn()?, &sandbox.id)
            .map_err(|error| Status::unavailable(format!("read runtime upgrade: {error}")))?;
        if unretired.is_some() {
            return Ok(());
        }
        let sandbox_dir = self.cache_root().join(&sandbox.id);
        let checkpoint = canonical_suspend_snapshot_dir(&sandbox_dir);
        if !present(&checkpoint)? {
            return Ok(());
        }
        let pinned = require_pinned_suspend_checkpoint(sandbox, &checkpoint)?;
        // An unencrypted disk is the rootfs staging laid, refused before the
        // metadata is written if the upgrade's rollback couldn't restore it.
        // An older node-manager left `instance/` private to the VM owner, so
        // node-manager repairs its traversal first, as the inspection does.
        if !sandbox.volume_encryption {
            crate::service::create_support::repair_retained_instance_traversal(
                &self.node_manager,
                self.cache_root(),
                &sandbox.id,
            )
            .await?;
            super::node_effects::require_plaintext_rootfs(
                &sandbox_dir.join("instance").join("rootfs.ext4"),
            )?;
        }
        let metadata_json = if present(&sandbox_dir.join("metadata.json"))? {
            String::new()
        } else {
            staged_disk_metadata_json(sandbox, &checkpoint)?
        };
        if metadata_json.is_empty() && !sandbox.volume_encryption {
            return Ok(());
        }
        Ok(self
            .materialize_staged_checkpoint_disk(sandbox, pinned, metadata_json)
            .await?)
    }

    async fn materialize_staged_checkpoint_disk(
        &self,
        sandbox: &SandboxRow,
        pinned: PinnedCheckpoint,
        metadata_json: String,
    ) -> Result<(), Status> {
        let local_artifact_id = pinned.local_artifact_id;
        let sandbox_id = sandbox.id.as_str();
        let fence = crate::service::cleanup_support::read_node_execution_fence(
            &self.db, sandbox_id,
        )?
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "sandbox {sandbox_id} has no node execution to finish its checkpoint's disk under"
            ))
        })?;
        let mut client =
            self.node_manager.connect().await.map_err(|error| {
                Status::unavailable(format!("connect to node-manager: {error}"))
            })?;
        // Per call, on the channel the RPC then uses: a node-manager
        // replaced since the last call may be older.
        let supported = supports_staged_checkpoint_disk(&mut client).await?;
        if !supported {
            return Err(Status::failed_precondition(format!(
                "sandbox {sandbox_id}'s checkpoint was staged without a launch, and this \
                 node-manager can't finish its disk for a runtime upgrade ({RPC}); roll \
                 node-manager out first"
            )));
        }
        let encrypted = sandbox.volume_encryption;
        let metadata_requested = !metadata_json.is_empty();
        let response = client
            .materialize_staged_checkpoint_disk(MaterializeStagedCheckpointDiskRequest {
                sandbox_id: sandbox_id.to_owned(),
                target: Some(
                    pplx_space_node_mgr_client::execution_fence::InstanceTarget::Execution(fence)
                        .into(),
                ),
                local_artifact_id: local_artifact_id.to_string(),
                metadata_json,
                encrypted,
                template_digest: pinned.template_digest.clone().unwrap_or_default(),
            })
            .await
            // node-manager's code says whether a retry can help.
            .map_err(|status| {
                Status::new(
                    status.code(),
                    format!(
                        "finish sandbox {sandbox_id}'s staged checkpoint disk: {}",
                        status.message()
                    ),
                )
            })?
            .into_inner();
        let finished = |part: i32, requested: bool, keep: bool| match StagedDiskPart::try_from(part)
        {
            Ok(StagedDiskPart::Written) => requested,
            Ok(StagedDiskPart::Kept) => requested && keep,
            Ok(StagedDiskPart::NotRequested) => !requested,
            _ => false,
        };
        // Only the metadata may be kept: the COW must be the pinned
        // checkpoint's, laid by this call.
        // And the disk it prepared must be the pinned checkpoint's, as the
        // node read it from the staged sidecar.
        let laid_template =
            (!response.template_digest.is_empty()).then_some(response.template_digest.as_str());
        let same_template = match (laid_template, pinned.template_digest.as_deref()) {
            (None, None) => true,
            (laid, pinned) => crate::rootfs_encryption::same_template_digest(laid, pinned),
        };
        if !finished(response.metadata, metadata_requested, true)
            || !finished(response.cow, encrypted, false)
            || response.local_artifact_id != local_artifact_id.to_string()
            || !same_template
        {
            return Err(Status::internal(format!(
                "{RPC} for sandbox {sandbox_id} answered metadata {:?}, cow {:?}, checkpoint {:?} \
                 over template {:?}; expected checkpoint {local_artifact_id} over {:?}",
                response.metadata(),
                response.cow(),
                response.local_artifact_id,
                response.template_digest,
                pinned.template_digest,
            )));
        }
        tracing::info!(
            sandbox_id,
            %local_artifact_id,
            metadata = ?response.metadata(),
            cow = ?response.cow(),
            "finished a staged checkpoint's disk for its runtime upgrade"
        );
        Ok(())
    }
}

/// `metadata.json` from the checkpoint's producer, the execution that wrote
/// the disk. The record names the disk's execution from now on, so it comes
/// only from the checkpoint the upgrade was decided from, which the caller
/// validated. A producer's metadata that can't be read may be readable on a
/// retry, but one that reads and doesn't parse or validate answers every
/// retry the same way, so it's final for the attempt, as the inspection's is
/// ([`super::node_effects::artifact_read_refusal`]).
fn staged_disk_metadata_json(
    sandbox: &SandboxRow,
    checkpoint: &Path,
) -> Result<String, super::Refusal> {
    let producer =
        crate::service::tail_support::checkpoint_execution_metadata(&checkpoint.to_string_lossy())
            .map_err(super::node_effects::artifact_read_refusal)?;
    let (cpu_budget, guest_memory) = (producer.cpu_budget(), producer.guest_memory());
    Ok(
        crate::service::tail_support::sandbox_artifact_metadata_json(
            sandbox,
            producer.into_execution(),
            cpu_budget,
            guest_memory,
        )?,
    )
}

/// Refuse `checkpoint` unless its sidecar names `sandbox` and the artifact the
/// row pins, the checkpoint the upgrade was decided from
/// ([`super::require_decided_checkpoint`]). Publication validates the same
/// sidecar before it publishes the checkpoint
/// (`suspend_snapshot_publication::validate_suspend_snapshot_metadata`).
pub(super) fn require_pinned_checkpoint_identity(
    sandbox: &SandboxRow,
    checkpoint: &Path,
) -> Result<ValidatedSuspendSnapshotMetadata, Status> {
    let sandbox_id = sandbox
        .id
        .parse::<SandboxId>()
        .map_err(|error| not_the_pinned_checkpoint(sandbox, error.to_string()))?;
    let validated = validate_suspend_snapshot_metadata(checkpoint, Some(sandbox_id))
        .map_err(|error| not_the_pinned_checkpoint(sandbox, error.to_string()))?;
    if sandbox.current_local_artifact_id != Some(validated.local_artifact_id) {
        return Err(not_the_pinned_checkpoint(
            sandbox,
            format!("its sidecar names {}", validated.local_artifact_id),
        ));
    }
    Ok(validated)
}

fn not_the_pinned_checkpoint(sandbox: &SandboxRow, why: String) -> Status {
    Status::failed_precondition(format!(
        "sandbox {}'s checkpoint isn't the suspend checkpoint it pins ({why}); refusing to \
         upgrade from it",
        sandbox.id
    ))
}

/// Finish a disk only from the suspend checkpoint the row pins
/// ([`require_pinned_checkpoint_identity`]), with the row's encryption and,
/// for an encrypted one, the row's template. A disk artifact forked from
/// another sandbox carries that sandbox's COW, which the encrypted rootfs
/// preparation re-encrypts, never lays.
fn require_pinned_suspend_checkpoint(
    sandbox: &SandboxRow,
    checkpoint: &Path,
) -> Result<PinnedCheckpoint, Status> {
    let refuse = |why: String| not_the_pinned_checkpoint(sandbox, why);
    let validated = require_pinned_checkpoint_identity(sandbox, checkpoint)?;
    if validated.row.volume_encryption != sandbox.volume_encryption {
        return Err(refuse(format!(
            "its sidecar's encryption is {}, the row's {}",
            validated.row.volume_encryption, sandbox.volume_encryption
        )));
    }
    // An encrypted sandbox's cold start clones the origin from the row's
    // template digest, so its checkpoint's COW must have been written over the
    // same one. An unencrypted checkpoint's rootfs is its whole disk.
    let checkpoint_template = validated.row.template_hash.map(|hash| hash.to_string());
    if sandbox.volume_encryption
        && !crate::rootfs_encryption::same_template_digest(
            checkpoint_template.as_deref(),
            sandbox.template_hash.as_deref(),
        )
    {
        return Err(refuse(format!(
            "it was written over template {}, the row names {}",
            checkpoint_template.as_deref().unwrap_or("<none>"),
            sandbox.template_hash.as_deref().unwrap_or("<none>")
        )));
    }
    Ok(PinnedCheckpoint {
        local_artifact_id: validated.local_artifact_id,
        template_digest: checkpoint_template,
    })
}

/// The checkpoint the row pins, as its sidecar names it: the artifact and
/// the template digest it was written over, if it names one.
struct PinnedCheckpoint {
    local_artifact_id: LocalArtifactId,
    template_digest: Option<String>,
}
