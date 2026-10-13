//! BYOK encrypted-rootfs orchestration for the sandbox start/restore paths.
//!
//! The sbx-mgr seam that turns the node-mgr + cluster-api primitives into a
//! live encrypted rootfs; this module is just the rootfs-specific node-mgr
//! call. Callers gate on the sandbox's immutable `volume_encryption` flag (see
//! [`crate::byok`]) and invoke this only for encrypted sandboxes. Any
//! encrypted-path failure is a HARD error: silently booting a BYOK-mandated
//! sandbox on a plaintext rootfs would be an at-rest breach.
//!
//! The warm-pool member shape below is different: it is a *device-shape*
//! choice for plaintext sandboxes (a composite whose COW a claim can later
//! re-home), not an encryption decision, so its failures degrade to the
//! plaintext rootfs file — but only when [`PoolRootfsPrepareError`] proves the
//! file is safe to boot (see `degrade_to_rootfs_file_is_safe`) and the
//! sandbox has no committed COW state to abandon.

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
            },
        )
        .await
        .map_err(EncryptedRootfsPrepareError::Rpc)?
        .into_inner();
    Ok(prepared.mapper_path)
}

/// Backing file name of a member's plaintext COW inside `instance/`. Owned by
/// node-manager (`rootfs_dmcrypt::POOL_COW_FILE`); duplicated here because
/// sbx-mgr only ever stats it. Presence contract: node-manager guarantees the
/// file survives on disk only once its exception store has been activated, so
/// its presence means guest writes live in the COW and ONLY in the COW.
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

/// Whether the sandbox has a committed plaintext pool COW on disk. Per the
/// [`POOL_COW_FILE`] presence contract, a surviving `cow.plain` means guest
/// writes live in the COW and only there. Besides guarding against plaintext
/// capture and file boots, this re-selects the composite shape on restart
/// regardless of pool_template: a claimed member (pool_template cleared)
/// must keep riding the COW that holds its writes.
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

/// Committed COW state that plaintext handling would silently abandon:
/// booting the bare rootfs file, or capturing a plaintext-format snapshot,
/// reads only the origin — the guest's writes live in whichever COW exists
/// (`cow.plain` pre-claim, `cow.luks` after an unstamped re-home) and would
/// be rolled back.
pub(crate) fn committed_cow_state_exists(sandbox_id: &str) -> bool {
    pool_plain_cow_exists(sandbox_id) || rehomed_encrypted_cow_exists(sandbox_id)
}

/// A pool-rootfs prepare failure, split by what it implies about node-side
/// state: the degrade decision in the start path must not boot the bare
/// rootfs file while a device stack may exist underneath it.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PoolRootfsPrepareError {
    /// node-manager was never reached; nothing can have been built.
    #[error("node-manager connect for snapshot rootfs: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    /// node-manager answered the RPC (or the transport failed after it was
    /// sent — see `degrade_to_rootfs_file_is_safe`).
    #[error("node-manager PrepareSnapshotRootfs ({code:?}): {message}", code = .0.code(), message = .0.message())]
    Rpc(tonic::Status),
}

impl PoolRootfsPrepareError {
    /// Whether booting the sandbox on the bare rootfs file is safe after this
    /// failure. Safe only when node-manager was never reached or answered
    /// with a verdict it reaches BEFORE touching devices (or after a
    /// guaranteed unwind of them). Two classes stay unsafe:
    /// - transport-class failures (deadline, connection reset, cancellation)
    ///   may hide a prepare that SUCCEEDED node-side;
    /// - `Internal` means a command/IO failure whose best-effort unwind may
    ///   itself have failed, or a panicked preparation task with no unwind
    ///   at all — devices may survive.
    ///
    /// Booting the file over a surviving composite mutates its read-only
    /// origin, and the next start reuses an exception store that no longer
    /// matches it — silent rootfs corruption. Fail the start instead; the
    /// fill retries and the prepare is idempotent. (The caller additionally
    /// refuses to degrade whenever committed COW state is on disk; this
    /// classification is the belt for states that check cannot see.)
    pub(crate) fn degrade_to_rootfs_file_is_safe(&self) -> bool {
        match self {
            Self::Connect(_) => true,
            Self::Rpc(status) => matches!(
                status.code(),
                tonic::Code::InvalidArgument
                    | tonic::Code::FailedPrecondition
                    | tonic::Code::ResourceExhausted
                    | tonic::Code::Unimplemented
                    | tonic::Code::PermissionDenied
                    | tonic::Code::Unauthenticated
                    | tonic::Code::NotFound
            ),
        }
    }
}

/// Prepare the warm-pool member rootfs: the same dm-snapshot composite the
/// encrypted path builds, but over a plaintext COW.
///
/// No DEK is involved — a member has no owner yet, so there is no org key to
/// format one with. The point of the shape is that every guest write lands in
/// the COW, which is what lets a claim swap the COW for a LUKS one under the
/// running guest instead of having to cold-create an encrypted sandbox.
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
