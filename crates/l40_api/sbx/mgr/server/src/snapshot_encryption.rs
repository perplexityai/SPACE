//! BYOK encrypted snapshot-volume orchestration for the suspend/restore path.
//!
//! Turns the node-mgr encrypted-snapshot-volume primitive into a place
//! Firecracker can write (suspend) and read (restore) its vmstate + memory
//! while they stay compressed-then-encrypted on disk. Callers gate on the
//! sandbox's immutable `volume_encryption` flag (see [`crate::byok`]); this
//! module is just the snapshot-volume-specific node-mgr calls. node-mgr
//! resolves the sandbox's single volume DEK from credmgr by sandbox_id — the
//! same per-sandbox key the encrypted rootfs already uses — so this path
//! handles no key material.

use crate::byok::CACHE_DIR;

/// Volume name for the suspend snapshot capture (one of potentially
/// several per-sandbox encrypted volumes).
const SNAPSHOT_VOLUME_NAME: &str = "snapshot";

/// Logical size of the encrypted snapshot volume. The backing file is sparse
/// (`truncate -s`) and btrfs(zstd) only allocates what Firecracker actually
/// writes (the guest memory dump + vmstate, compressed), so this is a generous
/// ceiling that covers a suspend of any current sandbox shape rather than a
/// tight fit — undersizing would ENOSPC the memory dump mid-flush.
const SNAPSHOT_VOLUME_SIZE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// On-disk LUKS2 backing file for a sandbox's encrypted snapshot volume on the
/// suspend (capture) path. Lives next to the rootfs `cow.luks`.
pub(crate) fn snapshot_backing_path(sandbox_id: &str) -> String {
    format!("{CACHE_DIR}/{sandbox_id}/instance/snapshot.luks")
}

/// Whether this runtime may create a new encrypted memory volume.
///
/// This decision is made from the paused row before suspend admission. A
/// successful cold start clears the published-artifact tuple; a restore retains
/// it. Admission then moves that tuple into durable rollback-only predecessor
/// storage and clears the publication pin until a successor is committed.
pub(crate) fn capture_volume_is_fresh(sandbox: &crate::db::SandboxRow) -> bool {
    !(sandbox.current_local_artifact_id.is_some()
        && sandbox.current_local_artifact_kind
            == Some(pplx_space_common_core::types::ArtifactKind::Full)
        && sandbox.current_local_artifact_origin
            == Some(pplx_space_common_core::types::ArtifactOrigin::Suspend))
}

/// Rootfs COW backing file (the encrypted rootfs delta), staged alongside the
/// sealed snapshot volume on suspend.
pub(crate) fn rootfs_cow_path(sandbox_id: &str) -> String {
    format!("{CACHE_DIR}/{sandbox_id}/instance/cow.luks")
}

/// A prepared (mounted) encrypted snapshot volume.
pub(crate) struct PreparedEncryptedSnapshot {
    /// Mounted btrfs(zstd) dir to bind into the jail for FC's vmstate+memory.
    pub mount_dir: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EncryptedSnapshotPrepareError {
    #[error("node-manager connect for encrypted snapshot volume: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("node-manager PrepareEncryptedVolume ({code:?}): {message}", code = .0.code(), message = .0.message())]
    Rpc(#[source] tonic::Status),
}

impl EncryptedSnapshotPrepareError {
    pub(crate) fn into_status(self, message: String) -> tonic::Status {
        match self {
            Self::Connect(_) => tonic::Status::internal(message),
            Self::Rpc(status) => {
                let details = tonic::codegen::Bytes::copy_from_slice(status.details());
                let metadata = status.metadata().clone();
                tonic::Status::with_details_and_metadata(status.code(), message, details, metadata)
            }
        }
    }
}

/// Prepare the encrypted snapshot volume for an encrypted sandbox.
///
/// `fresh` = `true` only for the first suspend after a cold boot; `false` on
/// restore and on every re-suspend (open + mount the published restore volume).
/// `restore_source` is the downloaded `snapshot.luks` on a cross-node
/// restore: node-mgr stages it to the canonical backing next to the rootfs
/// COW before opening (mirroring the rootfs `restore_cow_source`), so a
/// later re-suspend seals and stages the canonical path. `None` opens the
/// canonical backing in place (suspend, or same-node restore).
///
/// The sandbox's single volume DEK (resolved by node-mgr from credmgr) covers
/// this volume too, so the sandbox MUST already be provisioned for encryption
/// (it is, from the encrypted-rootfs prepare at start).
pub(crate) async fn prepare_encrypted_snapshot_volume(
    sandbox_id: &str,
    execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    fresh: bool,
    restore_source: Option<&str>,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<PreparedEncryptedSnapshot, EncryptedSnapshotPrepareError> {
    let mut node = node_manager.connect().await?;
    let prepared = node
        .prepare_encrypted_volume(
            pplx_space_node_mgr_client::proto::PrepareEncryptedVolumeRequest {
                execution_fence,
                sandbox_id: sandbox_id.to_string(),
                volume_name: SNAPSHOT_VOLUME_NAME.to_string(),
                size_bytes: SNAPSHOT_VOLUME_SIZE_BYTES,
                fresh,
                restore_backing_source: restore_source.map(str::to_string),
            },
        )
        .await
        .map_err(EncryptedSnapshotPrepareError::Rpc)?
        .into_inner();
    Ok(PreparedEncryptedSnapshot {
        mount_dir: prepared.mount_dir,
    })
}

/// Unmount + close the encrypted snapshot volume, sealing its LUKS2 backing
/// file, then (if `stage_dir` is set) reflink-copy the sealed `snapshot.luks`
/// and the rootfs `cow.luks` into `stage_dir` for publication — done in
/// node-mgr because it can read the sandbox-private `instance/` dir. Call
/// after Firecracker has finished writing vmstate+memory.
pub(crate) async fn release_encrypted_snapshot_volume(
    sandbox_id: &str,
    execution_fence: Option<pplx_space_node_mgr_client::proto::ExecutionFence>,
    stage_dir: &str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<(), String> {
    let mut node = node_manager
        .connect()
        .await
        .map_err(|e| format!("node-manager connect for snapshot-volume release: {e}"))?;
    // The two sealed ciphertext artifacts a suspend publishes: the volume's own
    // backing (vmstate+memory) and the rootfs delta. node-mgr copies them out
    // of the sandbox-private instance dir for us.
    let stage_artifacts = if stage_dir.is_empty() {
        Vec::new()
    } else {
        vec![
            pplx_space_node_mgr_client::proto::EncryptedVolumeStagedArtifact {
                src_path: snapshot_backing_path(sandbox_id),
                dst_name: "snapshot.luks".to_string(),
            },
            pplx_space_node_mgr_client::proto::EncryptedVolumeStagedArtifact {
                src_path: rootfs_cow_path(sandbox_id),
                dst_name: "cow.luks".to_string(),
            },
        ]
    };
    node.release_encrypted_volume(
        pplx_space_node_mgr_client::proto::ReleaseEncryptedVolumeRequest {
            execution_fence,
            sandbox_id: sandbox_id.to_string(),
            volume_name: SNAPSHOT_VOLUME_NAME.to_string(),
            stage_dir: stage_dir.to_string(),
            stage_artifacts,
        },
    )
    .await
    .map_err(|s| {
        format!(
            "node-manager ReleaseEncryptedVolume ({:?}): {}",
            s.code(),
            s.message()
        )
    })?;
    Ok(())
}
