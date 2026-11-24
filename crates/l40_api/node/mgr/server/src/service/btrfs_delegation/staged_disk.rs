//! Finishing the disk of a suspend checkpoint staged on this node without a
//! launch, so a runtime upgrade can cold-boot it as it would a copy suspended
//! here.
//!
//! A staged restore leaves `snapshot/` and no `metadata.json`, and for an
//! encrypted sandbox no `instance/cow.luks`. Sandbox-manager validates the
//! checkpoint against the row's pin and holds its artifact lock, so
//! `snapshot/` can't change under this call; node-manager does the writes,
//! because an instance a launch already handed to the VM owner isn't
//! sandbox-manager's to write into. The checkpoint's COW is laid over the
//! instance's on every call, as a restore lays it, because a suspended
//! sandbox's disk is its checkpoint and whatever the instance holds may be
//! an earlier boot's writes. A `metadata.json` already there is kept. Either
//! way a retry after any failure converges.

use std::path::Path;

use pplx_space_content_digest::ContentHash;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_model::sandbox_storage::DiskFile;
use pplx_space_snapshot_model::LocalArtifactId;
use tonic::{Request, Response, Status};

use super::{
    ALLOW_NODEMGR_ADMIN_OR_SBXMGR, InstanceOwners, NodeManagerService, artifact_fs,
    create_sandbox_instance_subvolume, trusted_paths, validate_sandbox_id,
};
use crate::proto::{
    MaterializeStagedCheckpointDiskRequest, MaterializeStagedCheckpointDiskResponse, StagedDiskPart,
};
use crate::service::encrypted_volumes::ROOTFS_COW_FILE;

const RPC: &str = "MaterializeStagedCheckpointDisk";

impl NodeManagerService {
    pub(in crate::service) async fn materialize_staged_checkpoint_disk(
        &self,
        request: Request<MaterializeStagedCheckpointDiskRequest>,
    ) -> Result<Response<MaterializeStagedCheckpointDiskResponse>, Status> {
        self.authorize_peer(&request, RPC, ALLOW_NODEMGR_ADMIN_OR_SBXMGR)?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        // An owned task: a caller that disconnects mid-copy doesn't drop the
        // artifact lock before the copy finishes.
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service
                .materialize_staged_checkpoint_disk_inner(request)
                .await
        })
        .await
    }

    async fn materialize_staged_checkpoint_disk_inner(
        &self,
        request: Request<MaterializeStagedCheckpointDiskRequest>,
    ) -> Result<Response<MaterializeStagedCheckpointDiskResponse>, Status> {
        use pplx_space_node_mgr_client::execution_fence::InstanceTarget;

        let req = request.into_inner();
        validate_sandbox_id(&req.sandbox_id)?;
        let checkpoint = CheckpointIdentity::requested(
            &req.sandbox_id,
            &req.local_artifact_id,
            &req.template_digest,
        )?;
        let metadata = validated_metadata(&req.sandbox_id, &req.metadata_json)?;
        let target: InstanceTarget = req
            .target
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("instance target is required"))?
            .try_into()?;
        if !matches!(target, InstanceTarget::Execution(_)) {
            return Err(Status::invalid_argument(
                "a staged checkpoint's disk is finished under an execution fence",
            ));
        }
        let sandbox_id = req.sandbox_id;
        let gate = self.lock_sandbox_cgroups(&sandbox_id).await;
        let sandbox_root = self.instance_target_root(&sandbox_id, target).await?;
        let encrypted = req.encrypted;
        let ((metadata, cow), checkpoint) = self
            .off_runtime("materialize staged checkpoint disk", move || {
                let _gate = gate;
                materialize(
                    &sandbox_root,
                    &StagedDisk {
                        owners: InstanceOwners::node(),
                        metadata_gid: pplx_space_host_layout::identities::gids::SPACE_CACHE_RW,
                        require_subvolume: true,
                    },
                    &checkpoint,
                    metadata.as_deref(),
                    encrypted,
                )
                .map(|parts| (parts, checkpoint))
            })
            .await?;
        tracing::info!(
            sandbox_id,
            checkpoint = %checkpoint.local_artifact_id,
            template_digest = ?checkpoint.template,
            metadata = ?metadata,
            cow = ?cow,
            "materialized a staged checkpoint's disk"
        );
        Ok(Response::new(MaterializeStagedCheckpointDiskResponse {
            metadata: metadata.into(),
            cow: cow.into(),
            local_artifact_id: checkpoint.local_artifact_id.to_string(),
            template_digest: checkpoint
                .template
                .map(|digest| digest.to_string())
                .unwrap_or_default(),
        }))
    }
}

/// The request's `metadata.json`, when it names this sandbox. Empty asks for
/// none.
fn validated_metadata(sandbox_id: &str, json: &str) -> Result<Option<Vec<u8>>, Status> {
    use std::str::FromStr;

    use pplx_space_artifact_model::SandboxArtifactMetadata;
    use pplx_space_sandbox_model::SandboxId;

    if json.is_empty() {
        return Ok(None);
    }
    let metadata: SandboxArtifactMetadata = serde_json::from_str(json)
        .map_err(|error| Status::invalid_argument(format!("invalid sandbox metadata: {error}")))?;
    let sandbox_id = SandboxId::from_str(sandbox_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if metadata.sandbox_id != sandbox_id {
        return Err(Status::invalid_argument(
            "sandbox metadata id does not match request",
        ));
    }
    Ok(Some(json.as_bytes().to_vec()))
}

/// Who owns what this writes. On a node, root creates the instance for
/// sandbox-manager, and `metadata.json` is the cache group's; a test passes
/// its own ids.
pub(super) struct StagedDisk {
    pub(super) owners: InstanceOwners,
    pub(super) metadata_gid: u32,
    /// Refuse an `instance/` that isn't a btrfs subvolume, which no upgrade
    /// could snapshot for its rollback. A test on another filesystem turns
    /// it off.
    pub(super) require_subvolume: bool,
}

/// The checkpoint a call prepares the disk of: the sandbox, the artifact
/// and the template digest its writes are over, which an unencrypted
/// sandbox's sidecar may not name.
#[derive(Debug, PartialEq)]
pub(super) struct CheckpointIdentity {
    pub(super) sandbox_id: SandboxId,
    pub(super) local_artifact_id: LocalArtifactId,
    pub(super) template: Option<ContentHash>,
}

impl CheckpointIdentity {
    fn requested(
        sandbox_id: &str,
        local_artifact_id: &str,
        template: &str,
    ) -> Result<Self, Status> {
        Ok(Self {
            sandbox_id: sandbox_id.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid sandbox id: {error}"))
            })?,
            local_artifact_id: local_artifact_id.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid checkpoint artifact id: {error}"))
            })?,
            template: (!template.is_empty())
                .then(|| template.parse())
                .transpose()
                .map_err(|error| {
                    Status::invalid_argument(format!("invalid template digest: {error}"))
                })?,
        })
    }

    /// What the staged checkpoint's `snapshot.json` names. A sidecar
    /// without an artifact id names no checkpoint this call can prepare.
    fn staged(checkpoint: &std::fs::File) -> Result<Self, Status> {
        #[derive(serde::Deserialize)]
        struct Source {
            content_hash: Option<String>,
        }
        #[derive(serde::Deserialize)]
        struct Sidecar {
            sandbox_id: String,
            local_artifact_id: Option<String>,
            source: Source,
        }
        let file =
            trusted_paths::open_regular_file(checkpoint, std::ffi::OsStr::new("snapshot.json"))?;
        let sidecar: Sidecar =
            serde_json::from_reader(std::io::BufReader::new(file)).map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid staged checkpoint snapshot.json: {error}"
                ))
            })?;
        let artifact = sidecar.local_artifact_id.ok_or_else(|| {
            Status::failed_precondition("the staged checkpoint's snapshot.json names no artifact")
        })?;
        Self::requested(
            &sidecar.sandbox_id,
            &artifact,
            sidecar.source.content_hash.as_deref().unwrap_or_default(),
        )
        .map_err(|status| Status::failed_precondition(status.message().to_owned()))
    }
}

/// Check the staged checkpoint is the requested one, then lay the encrypted
/// COW, then write `metadata.json`: a sandbox whose metadata is there but
/// whose COW isn't is what main's refusal already guards, and a retry
/// finishes either part.
pub(super) fn materialize(
    sandbox_root: &Path,
    staged: &StagedDisk,
    requested: &CheckpointIdentity,
    metadata: Option<&[u8]>,
    encrypted: bool,
) -> Result<(StagedDiskPart, StagedDiskPart), Status> {
    let sandbox = trusted_paths::open_directory(sandbox_root)?;
    let found = CheckpointIdentity::staged(&staged_checkpoint(&sandbox)?)?;
    if &found != requested {
        return Err(Status::failed_precondition(format!(
            "the staged checkpoint is {found:?}, not the requested {requested:?}"
        )));
    }
    // An encrypted sandbox's COW is over an origin the cold start clones
    // from the template this names.
    if encrypted && found.template.is_none() {
        return Err(Status::failed_precondition(
            "the staged encrypted checkpoint's snapshot.json names no template digest",
        ));
    }
    let cow = if encrypted {
        lay_cow(sandbox_root, staged)?
    } else {
        StagedDiskPart::NotRequested
    };
    let metadata = match metadata {
        None => StagedDiskPart::NotRequested,
        Some(contents) => write_metadata(sandbox_root, contents, staged.metadata_gid)?,
    };
    Ok((metadata, cow))
}

fn lay_cow(sandbox_root: &Path, staged: &StagedDisk) -> Result<StagedDiskPart, Status> {
    let subvolume = create_sandbox_instance_subvolume(sandbox_root, staged.owners)?;
    if staged.require_subvolume && !subvolume {
        return Err(Status::failed_precondition(
            "the sandbox instance isn't a btrfs subvolume, so a runtime upgrade can't keep a copy of its disk",
        ));
    }
    let sandbox = trusted_paths::open_directory(sandbox_root)?;
    // The instance must be a directory, not a link to one elsewhere.
    artifact_fs::directory(&sandbox, std::ffi::OsStr::new("instance"))
        .map_err(artifact_fs::error)?;
    // The instance's COW, whatever it holds, isn't known to be the pinned
    // checkpoint's: an earlier activation may have booted and written to it.
    // A suspended sandbox's disk is its checkpoint, so the checkpoint's COW
    // is laid over it every time, as a restore lays it, and the copy the
    // upgrade preserves next is that disk.
    let checkpoint = staged_checkpoint(&sandbox)?;
    if artifact_fs::disk_file_at(&checkpoint, ROOTFS_COW_FILE)? != DiskFile::Present {
        return Err(Status::failed_precondition(format!(
            "the staged checkpoint holds no non-empty {ROOTFS_COW_FILE}"
        )));
    }
    // What a restore does with the same file: a contained copy into a temp
    // name renamed over the destination, so a crash leaves no partial COW.
    crate::contained_copy::stage_disk_file_contained(
        sandbox_root,
        &sandbox_root.join("snapshot").join(ROOTFS_COW_FILE),
        &sandbox_root.join("instance").join(ROOTFS_COW_FILE),
    )
    .map_err(|error| match error {
        error @ crate::contained_copy::ContainedCopyError::Escape { .. } => {
            Status::failed_precondition(error.to_string())
        }
        error => Status::unavailable(error.to_string()),
    })?;
    Ok(StagedDiskPart::Written)
}

/// The sandbox's `snapshot/`, which must be a directory, not a link.
fn staged_checkpoint(sandbox: &std::fs::File) -> Result<std::fs::File, Status> {
    match artifact_fs::directory(sandbox, std::ffi::OsStr::new("snapshot")) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            Status::failed_precondition("no checkpoint is staged in snapshot/"),
        ),
        Err(error) => Err(artifact_fs::error(error)),
    }
}

fn write_metadata(
    sandbox_root: &Path,
    contents: &[u8],
    gid: u32,
) -> Result<StagedDiskPart, Status> {
    let sandbox = trusted_paths::open_directory(sandbox_root)?;
    match nix::sys::stat::fstatat(
        &sandbox,
        "metadata.json",
        nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
    ) {
        Ok(_) => Ok(StagedDiskPart::Kept),
        Err(nix::errno::Errno::ENOENT) => {
            crate::service::launch_prepare::write_sandbox_metadata_with_group(
                &sandbox_root.join("metadata.json"),
                contents,
                gid,
            )?;
            Ok(StagedDiskPart::Written)
        }
        Err(errno) => Err(artifact_fs::error(std::io::Error::from(errno))),
    }
}

#[cfg(test)]
#[path = "staged_disk_tests.rs"]
mod tests;
