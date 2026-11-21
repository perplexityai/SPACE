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
///
/// A cold start passes no `restore_cow_source`. If the sandbox then has no
/// `instance/cow.luks` but its checkpoint has one, node-mgr would format a
/// fresh COW and the guest would boot without its writes, so that start is
/// refused ([`EncryptedRootfsPrepareError::CheckpointCowNotLaid`]). With
/// [`PinnedCheckpoint::lay_checkpoint_cow`], a suspended sandbox's start
/// lays the COW of the encrypted suspend checkpoint the row pins, written
/// over the template the row names, as a restore does, and refuses any
/// other checkpoint.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EncryptedRootfsPrepareError {
    #[error("read encrypted disk restore metadata: {0}")]
    MetadataIo(#[source] std::io::Error),
    #[error("parse encrypted disk restore metadata: {0}")]
    MetadataJson(#[from] serde_json::Error),
    #[error("encrypted disk restore metadata does not match the target sandbox")]
    MetadataIdentity,
    #[error(
        "the sandbox's instance has no cow.luks and its checkpoint holds one that isn't its own \
         encrypted suspend checkpoint's; refusing to format a fresh COW in its place"
    )]
    CheckpointCowNotLaid,
    #[error("node-manager connect for encrypted rootfs: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("node-manager PrepareEncryptedRootfs ({code:?}): {message}", code = .0.code(), message = .0.message())]
    Rpc(#[source] tonic::Status),
}

impl EncryptedRootfsPrepareError {
    pub(crate) fn into_status(self, message: String) -> tonic::Status {
        match self {
            Self::Connect(_) => tonic::Status::internal(message),
            Self::MetadataIo(_) => tonic::Status::unavailable(message),
            Self::MetadataJson(_) | Self::MetadataIdentity | Self::CheckpointCowNotLaid => {
                tonic::Status::failed_precondition(message)
            }
            Self::Rpc(status) => {
                let details = tonic::codegen::Bytes::copy_from_slice(status.details());
                let metadata = status.metadata().clone();
                tonic::Status::with_details_and_metadata(status.code(), message, details, metadata)
            }
        }
    }
}

/// The suspend checkpoint a sandbox's row pins, if it still pins one (a stop
/// after a suspend drops the pin and keeps the checkpoint), and the template
/// its disk was written over.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PinnedCheckpoint<'a> {
    pub(crate) template_hash: Option<&'a str>,
    pub(crate) local_artifact_id: Option<pplx_space_snapshot_model::LocalArtifactId>,
    /// The row is suspended, so its checkpoint, not the instance, is its
    /// disk, as a restore treats it by laying the checkpoint's COW whenever
    /// the checkpoint holds one.
    pub(crate) suspended: bool,
    /// Lay the pinned suspend checkpoint's COW at a cold start. Off, a
    /// checkpoint COW the instance lacks is refused, never laid, and the
    /// instance's own COW is opened as before.
    pub(crate) lay_checkpoint_cow: bool,
}

impl<'a> PinnedCheckpoint<'a> {
    pub(crate) fn of(sandbox: &'a crate::db::SandboxRow, lay_checkpoint_cow: bool) -> Self {
        Self {
            lay_checkpoint_cow,
            template_hash: sandbox.template_hash.as_deref(),
            local_artifact_id: sandbox.current_local_artifact_id,
            suspended: sandbox
                .state
                .is_unowned(crate::state::SandboxState::Suspended),
        }
    }
}

/// Whether two recorded template digests name the same template build. A
/// missing or unreadable one names nothing, so it matches nothing.
pub(crate) fn same_template_digest(a: Option<&str>, b: Option<&str>) -> bool {
    let parse = |digest: Option<&str>| {
        digest.and_then(|digest| {
            digest
                .parse::<pplx_space_content_digest::ContentHash>()
                .ok()
        })
    };
    matches!((parse(a), parse(b)), (Some(a), Some(b)) if a == b)
}

pub(crate) async fn prepare_encrypted_rootfs(
    cache_root: &std::path::Path,
    sandbox_id: &str,
    pinned: PinnedCheckpoint<'_>,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    restore_cow_source: Option<&str>,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<String, EncryptedRootfsPrepareError> {
    let mut fork_source = None;
    let mut cow_source = restore_cow_source.map(str::to_owned);
    let sandbox_dir = cache_root.join(sandbox_id);
    let mut own_suspend_checkpoint = false;
    if cow_source.is_none() {
        let snapshot_dir = sandbox_dir.join("snapshot");
        match tokio::fs::read(snapshot_dir.join("snapshot.json")).await {
            Ok(bytes) => {
                let meta: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(&bytes)?;
                if meta.encrypted
                    && crate::snapshot_upload::artifact_kind_from_meta(&meta)
                        == pplx_space_snapshot_model::ArtifactKind::Disk
                {
                    if meta.sandbox_id != sandbox_id {
                        return Err(EncryptedRootfsPrepareError::MetadataIdentity);
                    }
                    let source = meta
                        .rootfs_encryption_source
                        .ok_or(EncryptedRootfsPrepareError::MetadataIdentity)?;
                    if source.to_string() == sandbox_id {
                        return Err(EncryptedRootfsPrepareError::MetadataIdentity);
                    }
                    cow_source = Some(snapshot_dir.join("cow.luks").to_string_lossy().into_owned());
                    fork_source = Some(
                        pplx_space_node_mgr_client::proto::EncryptedRootfsForkSource {
                            sandbox_id: source.to_string(),
                        },
                    );
                } else {
                    // The checkpoint the row pins, written over the template
                    // the start clones the origin from. A row that pins
                    // none, stopped since its suspend, has no checkpoint
                    // that's known to be its disk.
                    own_suspend_checkpoint = meta.encrypted
                        && meta.sandbox_id == sandbox_id
                        // Named Full, as staging requires
                        // (`validate_suspend_snapshot_metadata`): a sidecar
                        // that names no kind isn't counted as one.
                        && meta.artifact_kind == Some(pplx_space_snapshot_model::ArtifactKind::Full)
                        && meta.artifact_origin
                            == Some(pplx_space_snapshot_model::ArtifactOrigin::Suspend)
                        && pinned
                            .local_artifact_id
                            .is_some_and(|pin| meta.local_artifact_id == Some(pin))
                        && same_template_digest(
                            meta.source.content_hash.as_deref(),
                            pinned.template_hash,
                        );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(EncryptedRootfsPrepareError::MetadataIo(error)),
        }
    }
    let staged = sandbox_dir.join("snapshot").join(ROOTFS_COW_FILE);
    if cow_source.is_none()
        && pinned.lay_checkpoint_cow
        && pinned.suspended
        && path_present(&staged).map_err(EncryptedRootfsPrepareError::MetadataIo)?
    {
        // A suspended sandbox's disk is its pinned checkpoint's COW, laid on
        // every activation as a restore lays it, whatever an earlier attempt
        // left in the instance. A checkpoint that isn't the pinned one isn't
        // that disk, and the instance may not be either, so neither boots.
        if !own_suspend_checkpoint {
            return Err(EncryptedRootfsPrepareError::CheckpointCowNotLaid);
        }
        cow_source = Some(staged.to_string_lossy().into_owned());
    }
    if cow_source.is_none() && checkpoint_cow_unlaid(&sandbox_dir)? {
        // node-manager would format a fresh COW over a disk whose writes may
        // be only in the checkpoint. Laying it is the case above; anything
        // else, the switch off included, is refused.
        return Err(EncryptedRootfsPrepareError::CheckpointCowNotLaid);
    }
    let mut node = node_manager.connect().await?;
    let prepared = node
        .prepare_encrypted_rootfs(
            pplx_space_node_mgr_client::proto::PrepareEncryptedRootfsRequest {
                execution_fence: Some(execution_fence.into()),
                sandbox_id: sandbox_id.to_string(),
                restore_cow_source: cow_source,
                fork_source,
            },
        )
        .await
        .map_err(EncryptedRootfsPrepareError::Rpc)?
        .into_inner();
    Ok(prepared.mapper_path)
}

/// Name of the encrypted rootfs COW, in `instance/` and in a suspend
/// checkpoint. Owned by node-manager (its `ROOTFS_COW_FILE`) and by the
/// suspend's sealed artifact set; duplicated here because sbx-mgr only stats
/// it.
pub(crate) const ROOTFS_COW_FILE: &str = "cow.luks";

/// Whether the checkpoint holds a `cow.luks` and the instance none. node-mgr formats a fresh COW exactly when `instance/cow.luks` is
/// absent, so a start that hands it no source would boot the guest without
/// the writes a checkpoint staged away from the node that ran the sandbox
/// holds there.
fn checkpoint_cow_unlaid(
    sandbox_dir: &std::path::Path,
) -> Result<bool, EncryptedRootfsPrepareError> {
    let present = |path: &std::path::Path| {
        path_present(path).map_err(EncryptedRootfsPrepareError::MetadataIo)
    };
    let staged = sandbox_dir.join("snapshot").join(ROOTFS_COW_FILE);
    Ok(!present(&sandbox_dir.join("instance").join(ROOTFS_COW_FILE))? && present(&staged)?)
}

/// Whether `path` names anything, without following a final symlink. Absent
/// is `false`; any other failure to look is an error, never an answer.
pub(crate) fn path_present(path: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// What `path` holds as a disk file, without following a final symlink. Any
/// failure to look other than absence is an error, never an answer.
pub(crate) fn disk_file(
    path: &std::path::Path,
) -> std::io::Result<pplx_space_sandbox_model::sandbox_storage::DiskFile> {
    use pplx_space_sandbox_model::sandbox_storage::DiskFile;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() > 0 => {
            Ok(DiskFile::Present)
        }
        Ok(_) => Ok(DiskFile::Empty),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DiskFile::Absent),
        Err(error) => Err(error),
    }
}

/// Backing file name of a member's plaintext COW inside `instance/`. Owned by
/// node-manager (`rootfs_dmcrypt::POOL_COW_FILE`); duplicated here because
/// sbx-mgr only ever stats it. Presence contract: node-manager guarantees the
/// file survives on disk only once its exception store has been activated, so
/// its presence means guest writes live in the COW and ONLY in the COW.
const POOL_COW_FILE: &str = "cow.plain";

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
    instance_file_exists(sandbox_id, ROOTFS_COW_FILE)
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
