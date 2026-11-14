//! Completing a staged checkpoint's disk for the upgrade that boots it.
//!
//! Staging lays a suspend checkpoint down from its backup and leaves the
//! sandbox suspended without launching it. A restore would finish the disk at
//! launch; an upgrade boots it cold instead, so it reads the disk first
//! ([`super::RuntimeUpgradeEffects::inspect_local_disk`]), keeps a read-only
//! copy of the instance and its `metadata.json`, and rolls back to that copy.
//! This finishes the parts a staged checkpoint lacks:
//!
//! - **`metadata.json`**, the disk's record of its execution, which a launch
//!   writes. Staging writes none, so it's written from the checkpoint's
//!   producer, the execution that wrote the disk.
//! - **An encrypted sandbox's `instance/cow.luks`.** Its guest writes are in
//!   the checkpoint's `cow.luks`, which a restore hands node-manager to lay
//!   into the instance before opening. The upgrade's cold start hands it no
//!   source, and node-manager formats a fresh COW wherever
//!   `instance/cow.luks` is absent, so the COW is laid into the instance here,
//!   before the upgrade looks. The rootfs under it is the template's, which
//!   the cold start clones when absent, as a restore's launch does.
//!
//! The upgrade does this under its exclusion of disk mutations, right before
//! it reads the disk, so every attempt finishes whatever a staging, or an
//! earlier attempt, left undone, whichever caller staged it. What a sandbox
//! already has is left alone: a copy suspended on this node keeps its own
//! instance and record. The upgrade's cold boot of a suspended sandbox lays
//! the pinned checkpoint's COW again regardless, through node-manager
//! (`rootfs_encryption::prepare_encrypted_rootfs`), so the guest boots the
//! checkpoint the upgrade was decided from.

use std::path::Path;

use pplx_space_common_core::types::SandboxId;
use tonic::Status;

use crate::db::SandboxRow;
use crate::rootfs_encryption::{ROOTFS_COW_FILE, path_present};
use crate::service::suspend_snapshot_publication::{
    canonical_suspend_snapshot_dir, validate_suspend_snapshot_metadata,
};
use crate::state::SandboxState;

#[cfg(test)]
mod tests;

fn present(path: &Path) -> Result<bool, Status> {
    path_present(path)
        .map_err(|error| Status::unavailable(format!("inspect {}: {error}", path.display())))
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
    ) -> Result<(), Status> {
        if !sandbox.state.is_unowned(SandboxState::Suspended) {
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
        self.record_staged_disk_execution(sandbox, &sandbox_dir, &checkpoint)?;
        if sandbox.volume_encryption {
            self.lay_staged_checkpoint_cow(sandbox, &sandbox_dir, &checkpoint)
                .await?;
        }
        Ok(())
    }

    /// Write `metadata.json` from the checkpoint's producer when the sandbox
    /// has none.
    fn record_staged_disk_execution(
        &self,
        sandbox: &SandboxRow,
        sandbox_dir: &Path,
        checkpoint: &Path,
    ) -> Result<(), Status> {
        let metadata = sandbox_dir.join("metadata.json");
        if present(&metadata)? {
            return Ok(());
        }
        // The record names the disk's execution from now on, so it comes
        // only from the checkpoint the upgrade was decided from.
        require_pinned_suspend_checkpoint(sandbox, checkpoint)?;
        let producer = crate::service::tail_support::checkpoint_execution_metadata(
            &checkpoint.to_string_lossy(),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let (cpu_budget, guest_memory) = (producer.cpu_budget(), producer.guest_memory());
        let json = crate::service::tail_support::sandbox_artifact_metadata_json(
            sandbox,
            producer.into_execution(),
            cpu_budget,
            guest_memory,
        )?;
        crate::snapshot_scheduler::write_file_atomic(&metadata, json.as_bytes())
            .map_err(|error| Status::internal(format!("write {}: {error}", metadata.display())))?;
        tracing::info!(
            sandbox_id = %sandbox.id,
            "recorded a staged checkpoint's disk execution for its runtime upgrade"
        );
        Ok(())
    }

    /// Lay the encrypted checkpoint's `cow.luks` into `instance/` when the
    /// instance has none, as a restore's launch would have node-manager do.
    async fn lay_staged_checkpoint_cow(
        &self,
        sandbox: &SandboxRow,
        sandbox_dir: &Path,
        checkpoint: &Path,
    ) -> Result<(), Status> {
        let sandbox_id = sandbox.id.as_str();
        // Older node-managers left a retained instance private to the VM
        // owner, which an unprivileged look into it fails on.
        crate::service::create_support::repair_retained_instance_traversal(
            &self.node_manager,
            self.cache_root(),
            sandbox_id,
        )
        .await?;
        let instance = sandbox_dir.join("instance");
        let laid = instance.join(ROOTFS_COW_FILE);
        if present(&laid)? {
            return Ok(());
        }
        let staged = checkpoint.join(ROOTFS_COW_FILE);
        match crate::rootfs_encryption::non_empty_regular_file(&staged).map_err(|error| {
            Status::unavailable(format!("inspect {}: {error}", staged.display()))
        })? {
            // Without one the upgrade's inspection refuses the sandbox.
            None => return Ok(()),
            // Laid, it would reach the fenced upgrade, whose rollback of the
            // preserved copy refuses an empty COW.
            Some(false) => {
                return Err(Status::failed_precondition(format!(
                    "sandbox {sandbox_id}'s staged checkpoint COW {} is empty or not a regular \
                     file",
                    staged.display()
                )));
            }
            Some(true) => {}
        }
        require_pinned_suspend_checkpoint(sandbox, checkpoint)?;
        let fence = crate::service::cleanup_support::read_node_execution_fence(
            &self.db, sandbox_id,
        )?
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "sandbox {sandbox_id} has no node execution to lay its checkpoint's COW under"
            ))
        })?;
        // A subvolume, so the upgrade can keep a read-only copy of it.
        let prepared = crate::btrfs_via_nodemgr::create_sandbox_instance(
            sandbox_id,
            pplx_space_node_mgr_client::execution_fence::InstanceTarget::Execution(fence),
            &self.node_manager,
        )
        .await
        .map_err(|error| Status::unavailable(format!("create {}: {error}", instance.display())))?;
        if prepared != crate::btrfs_via_nodemgr::PreparedInstance::Subvolume {
            return Err(Status::failed_precondition(format!(
                "sandbox {sandbox_id}'s instance isn't a btrfs subvolume ({prepared:?}), so a \
                 runtime upgrade can't keep a copy of it"
            )));
        }
        // Pinned under the lock. The copy outlives a cancelled request, and
        // with it the lock, so it publishes only into the instance it pinned
        // and only where nothing is laid yet: a later attempt's COW, maybe
        // open under a running guest, is never replaced, and an instance an
        // eviction put at this path since never gets this checkpoint's COW.
        let pinned = std::fs::File::open(&staged)
            .and_then(|source| std::fs::File::open(&instance).map(|directory| (source, directory)));
        let published = tokio::task::spawn_blocking(move || {
            let (source, directory) = pinned?;
            publish_new_copy(&source, &directory, ROOTFS_COW_FILE)
        })
        .await
        .map_err(|error| Status::internal(format!("lay {}: {error}", laid.display())))?
        .map_err(|error| {
            Status::unavailable(format!(
                "lay {} into {}: {error}",
                staged.display(),
                laid.display()
            ))
        })?;
        match published {
            Some(reflinked) => tracing::info!(
                sandbox_id,
                reflinked,
                "laid a staged encrypted checkpoint's COW into its instance for its runtime upgrade"
            ),
            None => tracing::info!(
                sandbox_id,
                "another attempt laid the staged encrypted checkpoint's COW first"
            ),
        }
        Ok(())
    }
}

/// Copy `source` into `directory` as `name` through a private temporary,
/// then publish it only if `name` doesn't exist yet, all relative to the
/// pinned directory. Returns whether the copy was a reflink, or `None` when
/// `name` already existed; the temporary is removed either way. Temporaries
/// an interrupted process left are removed first: the caller holds the lock
/// every publisher takes, and a late copy whose temporary goes only fails to
/// publish.
fn publish_new_copy(
    source: &std::fs::File,
    directory: &std::fs::File,
    name: &str,
) -> std::io::Result<Option<bool>> {
    use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags};
    let prefix = format!(".{name}.");
    for entry in rustix::fs::Dir::read_from(directory)? {
        let entry = entry?;
        let leftover = entry.file_name();
        if leftover.to_bytes().starts_with(prefix.as_bytes()) {
            match rustix::fs::unlinkat(directory, leftover, AtFlags::empty()) {
                Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let temporary = format!("{prefix}{}", uuid::Uuid::now_v7());
    let copy = std::fs::File::from(rustix::fs::openat(
        directory,
        temporary.as_str(),
        OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )?);
    let published = (|| {
        let reflinked = pplx_space_file_io::reflink::copy_on_descriptors(
            source,
            &copy,
            source.metadata()?.len(),
        )?;
        copy.sync_all()?;
        match rustix::fs::renameat_with(
            directory,
            temporary.as_str(),
            directory,
            name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        directory.sync_all()?;
        Ok(Some(reflinked))
    })();
    if !matches!(published, Ok(Some(_))) {
        match rustix::fs::unlinkat(directory, temporary.as_str(), AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(error) => tracing::warn!(
                temporary,
                %error,
                "couldn't remove an unpublished COW copy"
            ),
        }
    }
    published
}

/// Finish a disk only from the suspend checkpoint the row pins, the one the
/// upgrade was decided from, with the row's encryption and, for an encrypted
/// one, the row's template. A disk artifact forked
/// from another sandbox carries that sandbox's COW, which the encrypted rootfs
/// preparation re-encrypts, never lays.
fn require_pinned_suspend_checkpoint(
    sandbox: &SandboxRow,
    checkpoint: &Path,
) -> Result<(), Status> {
    let refuse = |why: String| {
        Status::failed_precondition(format!(
            "sandbox {}'s checkpoint isn't the suspend checkpoint it pins ({why}); refusing to \
             finish its disk from it",
            sandbox.id
        ))
    };
    let sandbox_id = sandbox
        .id
        .parse::<SandboxId>()
        .map_err(|error| refuse(error.to_string()))?;
    let validated = validate_suspend_snapshot_metadata(checkpoint, Some(sandbox_id))
        .map_err(|error| refuse(error.to_string()))?;
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
    if sandbox.current_local_artifact_id != Some(validated.local_artifact_id) {
        return Err(refuse(format!(
            "its sidecar names {}",
            validated.local_artifact_id
        )));
    }
    Ok(())
}
