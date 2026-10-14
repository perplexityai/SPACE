//! BYOK encrypted-rootfs orchestration for the sandbox start/restore paths.
//!
//! The sbx-mgr seam that turns the node-mgr + cluster-api primitives into a
//! live encrypted rootfs; this module is just the rootfs-specific node-mgr
//! call. Callers gate on the sandbox's immutable `volume_encryption` flag (see
//! [`crate::byok`]) and invoke this only for encrypted sandboxes. Any
//! encrypted-path failure is a HARD error: silently booting a BYOK-mandated
//! sandbox on a plaintext rootfs would be an at-rest breach.
//!
//! Warm-pool members retain a pre-boot reflink but keep writing their ordinary
//! rootfs file. BYOK adoption captures the difference and switches the live
//! Firecracker drive to its encrypted device before guest access is enabled.

/// Prepare the encrypted rootfs device for an encrypted sandbox. Returns the
/// mapper path to thread into `LaunchSandboxRequest.rootfs_block_device_path`.
///
/// node-mgr derives the canonical origin/COW paths from the sandbox id and
/// decides format-vs-open of the COW from on-disk state (whether `cow.luks`
/// is already a LUKS container), so first-start formats and a restart/restore
/// opens — no `fresh` flag crosses the wire. On the snapshot-restore path the
/// caller passes `restore_cow_source` — the downloaded `cow.luks` that
/// node-mgr lays down at the COW path before opening it — so the
/// reconstructed rootfs carries the suspended guest's writes.
///
/// node-mgr self-serves the DEK from credmgr by sandbox_id; sbx-mgr handles no
/// key material and persists no wrap.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EncryptedRootfsPrepareError {
    #[error("node-manager connect for encrypted rootfs: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("node-manager PrepareEncryptedRootfs ({code:?}): {message}", code = .0.code(), message = .0.message())]
    Rpc(#[source] tonic::Status),
}

impl EncryptedRootfsPrepareError {
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

pub(crate) async fn prepare_encrypted_rootfs(
    sandbox_id: &str,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    restore_cow_source: Option<&str>,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<String, EncryptedRootfsPrepareError> {
    let mut node = node_manager.connect().await?;
    let prepared = node
        .prepare_encrypted_rootfs(
            pplx_space_node_mgr_client::proto::PrepareEncryptedRootfsRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: sandbox_id.to_string(),
                restore_cow_source: restore_cow_source.map(str::to_string),
                pool_adoption: None,
            },
        )
        .await
        .map_err(EncryptedRootfsPrepareError::Rpc)?
        .into_inner();
    Ok(prepared.mapper_path)
}

/// Earlier pool layouts kept writes outside the ordinary rootfs. Their
/// plaintext COW must still prevent a capture that would lose those writes.
const POOL_COW_FILE: &str = "cow.plain";

/// Name of the encrypted rootfs COW inside `instance/`. Owned by node-manager
/// (its `ROOTFS_COW_FILE`); duplicated here because sbx-mgr only ever stats
/// it. On a sandbox whose row says `volume_encryption=false`, its presence
/// means a claim-time re-home committed but the row's stamp has not landed —
/// the guest's writes live in that encrypted COW.
const REHOMED_COW_FILE: &str = "cow.luks";

fn instance_file_exists(sandbox_id: &str, name: &str) -> bool {
    std::path::Path::new(crate::byok::CACHE_DIR)
        .join(sandbox_id)
        .join("instance")
        .join(name)
        .exists()
}

/// Reject plaintext capture of members left by the earlier pool disk layout.
pub(crate) fn pool_plain_cow_exists(sandbox_id: &str) -> bool {
    instance_file_exists(sandbox_id, POOL_COW_FILE)
}

/// Whether the sandbox carries an encrypted rootfs COW on disk. Only
/// meaningful on a plaintext-flagged row (encrypted sandboxes always have
/// one): there it means a committed re-home whose `volume_encryption` stamp
/// is missing, and every plaintext handling path must fail closed until the
/// stamp lands.
pub(crate) fn rehomed_encrypted_cow_exists(sandbox_id: &str) -> bool {
    instance_file_exists(sandbox_id, REHOMED_COW_FILE)
}

/// A partial drive switch retains the old file under pool-rootfs.ext4. Capturing
/// only the new canonical origin would silently drop the warm VM's writes.
pub(crate) fn committed_cow_state_exists(sandbox_id: &str) -> bool {
    pool_plain_cow_exists(sandbox_id)
        || rehomed_encrypted_cow_exists(sandbox_id)
        || instance_file_exists(sandbox_id, "pool-rootfs.ext4")
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PoolRootfsPrepareError {
    #[error("node-manager connect for snapshot rootfs: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("node-manager PrepareSnapshotRootfs ({code:?}): {message}", code = .0.code(), message = .0.message())]
    Rpc(tonic::Status),
}

/// Retain the pre-boot reflink used to reconstruct a warm VM's disk changes.
pub(crate) async fn prepare_snapshot_rootfs(
    sandbox_id: &str,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<String, PoolRootfsPrepareError> {
    let mut node = node_manager.connect().await?;
    let prepared = node
        .prepare_snapshot_rootfs(
            pplx_space_node_mgr_client::proto::PrepareSnapshotRootfsRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: sandbox_id.to_string(),
            },
        )
        .await
        .map_err(PoolRootfsPrepareError::Rpc)?
        .into_inner();
    Ok(prepared.mapper_path)
}

#[cfg(test)]
mod tests;
