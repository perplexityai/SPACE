//! BYOK encrypted-volume delegation handlers.
//!
//! These RPCs let the unprivileged sandbox-manager delegate the
//! device-mapper work behind BYOK sandboxes — losetup / cryptsetup /
//! dmsetup / mount — to node-manager, the privileged daemon (root +
//! `privileged: true` in the K8s daemonset; `User=root` in the systemd
//! direct-deploy unit). They mirror the btrfs delegation RPCs: requests
//! carry domain identifiers (`sandbox_id`, `volume_name`) and the
//! canonical host paths are derived here; the few caller-supplied paths
//! (restore sources staged by the snapshot pull, the suspend publication
//! staging dir) are validated lexically to live under the calling
//! sandbox's cache dir, and every copy across this boundary then resolves
//! both endpoints descriptor-relative with symlinks rejected at each
//! component ([`crate::contained_copy`]) — so a compromised caller can't
//! point node-manager at filesystem locations outside the sandbox's own
//! on-disk footprint, not even via a symlink planted inside it.
//!
//! Key material never crosses the node-manager socket: the sandbox's
//! single volume DEK is resolved from credmgr by `sandbox_id`
//! (`GetOrIssueSandboxVolumeDek`) — issued and persisted on the first
//! volume, loaded on every later call — and dropped (via `Zeroizing`)
//! as soon as cryptsetup has consumed it.
//!
//! There is no `ReleaseEncryptedRootfs` RPC: the rootfs device stack is
//! torn down by node-manager's own sandbox cleanup
//! (`cleanup_runtime_artifacts`), which sweeps the `sbxroot-*` /
//! `sbxvol-*` dm devices without needing a caller.

use pplx_space_common_core::types::OrgId;

use std::path::{Component, Path, PathBuf};

use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::SandboxId;
use tonic::{Request, Response, Status};
use zeroize::Zeroizing;

use crate::proto::{
    PrepareEncryptedRootfsRequest, PrepareEncryptedRootfsResponse, PrepareEncryptedVolumeRequest,
    PrepareEncryptedVolumeResponse, PrepareSnapshotRootfsRequest, PrepareSnapshotRootfsResponse,
    ReleaseEncryptedVolumeRequest, ReleaseEncryptedVolumeResponse,
};
use crate::{encrypted_volume, rootfs_dmcrypt};

use super::{ALLOW_NODE_ADMIN_OR_SBXMGR, NodeManagerService};

/// Plaintext, reflinked-from-template rootfs file inside `instance/` — the
/// dm-snapshot origin of the encrypted rootfs.
const ROOTFS_ORIGIN_FILE: &str = "rootfs.ext4";

/// Per-sandbox encrypted rootfs COW backing file inside `instance/`.
pub(super) const ROOTFS_COW_FILE: &str = "cow.luks";

/// Upper bound on `volume_name`. The name lands in a dm mapper name
/// (`sbxvol-<uuid>-<name>`, kernel limit 127 chars) and in derived paths, so
/// keep it short.
const MAX_VOLUME_NAME_LEN: usize = 32;

impl NodeManagerService {
    pub(super) async fn run_owned_device_operation<T: Send + 'static>(
        &self,
        sandbox_id: &str,
        method: crate::lifecycle_limiter::LifecycleMethod,
        work: impl std::future::Future<Output = Result<T, Status>> + Send + 'static,
    ) -> Result<T, Status> {
        let typed_sandbox_id = parse_sandbox_id(sandbox_id)?;
        let permit = self.lifecycle_limiter.acquire(method).await?;
        let mutation_claim = self.claim_sandbox_work(sandbox_id)?;
        let (gate, device) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let gate = self.lock_sandbox_vm_cgroup_launch(sandbox_id).await;
            let device = self
                .sandbox_device_stack_lock(&typed_sandbox_id)
                .await
                .lock_owned()
                .await;
            (gate, device)
        })
        .await
        .map_err(|_| Status::unavailable("device mutation ownership is busy"))?;
        self.run_owned_operation("device mutation", async move {
            let _ownership = (permit, gate, device, mutation_claim);
            work.await
        })
        .await
    }

    pub(super) async fn prepare_encrypted_rootfs(
        &self,
        request: Request<PrepareEncryptedRootfsRequest>,
    ) -> Result<Response<PrepareEncryptedRootfsResponse>, Status> {
        self.authorize_peer(
            &request,
            "PrepareEncryptedRootfs",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox_id.to_string(),
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                service
                    .prepare_encrypted_rootfs_inner(sandbox_id, request.into_inner())
                    .await
            },
        )
        .await
    }

    async fn prepare_encrypted_rootfs_inner(
        &self,
        sandbox_id: SandboxId,
        req: PrepareEncryptedRootfsRequest,
    ) -> Result<Response<PrepareEncryptedRootfsResponse>, Status> {
        // Counted before any request validation so the attempted/failed rate
        // covers parameter rejections too; the guard's drop arm records them.
        let metric = DeviceOpMetric::prepare(pplx_space_metrics::byok::OP_ROOTFS);
        self.check_execution(&sandbox_id.to_string(), req.execution_fence.as_ref(), false)?;

        let instance_dir = PathBuf::from(paths::sandbox_instance_dir(&sandbox_id.to_string()));
        let origin_path = instance_dir.join(ROOTFS_ORIGIN_FILE);
        let cow_path = instance_dir.join(ROOTFS_COW_FILE);

        // Snapshot-restore: the downloaded encrypted COW to lay down at the
        // canonical COW path before opening. Validated (lexically) here at the
        // wire boundary; the staging itself happens inside
        // `rootfs_dmcrypt::prepare`, ordered behind its live-composite re-entry
        // check so a retry against a running rootfs can't clobber the live
        // backing — the same layer the data-volume primitive enforces it at.
        let restore_cow_source = match req.restore_cow_source.as_deref().filter(|s| !s.is_empty()) {
            Some(src) => Some(validate_sandbox_scoped_path(
                &sandbox_id,
                src,
                "restore_cow_source",
            )?),
            None => None,
        };

        // node-manager self-serves the sandbox's single volume DEK from
        // credmgr (issued+persisted on the first volume, loaded thereafter).
        // The caller never handles a KEK or wrap; credmgr owns org→KEK
        // resolution, the wrap storage, and the AAD.
        let plaintext_dek = fetch_sandbox_volume_dek(&sandbox_id, None)
            .await
            .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_DEK))?;

        let prepared = rootfs_dmcrypt::prepare(rootfs_dmcrypt::PrepareParams {
            sandbox_id,
            origin_path,
            cow_path,
            restore_cow_source,
            // The COW is sized from the measured origin device; there is no
            // caller-supplied size to cross-check.
            rootfs_size_bytes: 0,
            dek: &plaintext_dek,
        })
        .await;
        let prepared = record_outcome(&metric, prepared)?;
        // plaintext_dek drops here → Zeroizing scrubs it; cryptsetup already
        // loaded the key into the kernel keyring.

        Ok(Response::new(PrepareEncryptedRootfsResponse {
            mapper_path: prepared.mapper_path.to_string_lossy().into_owned(),
        }))
    }

    /// Warm-pool member shape: the same composite, over a plaintext COW.
    /// No DEK is fetched — at fill time the member has no owner, so there is
    /// no org whose key could be used.
    pub(super) async fn prepare_snapshot_rootfs(
        &self,
        request: Request<PrepareSnapshotRootfsRequest>,
    ) -> Result<Response<PrepareSnapshotRootfsResponse>, Status> {
        self.authorize_peer(
            &request,
            "PrepareSnapshotRootfs",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox_id.to_string(),
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                service.check_execution(
                    &sandbox_id.to_string(),
                    request.get_ref().execution_fence.as_ref(),
                    false,
                )?;
                service.prepare_snapshot_rootfs_inner(sandbox_id).await
            },
        )
        .await
    }

    async fn prepare_snapshot_rootfs_inner(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Response<PrepareSnapshotRootfsResponse>, Status> {
        // Counted before any request validation so the attempted/failed rate
        // covers parameter rejections too; the guard's drop arm records them.
        // The caller degrades a failure here to the plaintext rootfs file
        // with only a warn log, so this series is the ONLY place a node whose
        // pool fills all fail is visible.
        let metric = DeviceOpMetric::prepare(pplx_space_metrics::byok::OP_POOL_ROOTFS);
        let instance_dir = PathBuf::from(paths::sandbox_instance_dir(&sandbox_id.to_string()));

        let origin_path = instance_dir.join(ROOTFS_ORIGIN_FILE);
        let cow_path = instance_dir.join(rootfs_dmcrypt::POOL_COW_FILE);
        let encrypted_cow_path = instance_dir.join(ROOTFS_COW_FILE);
        let result = rootfs_dmcrypt::prepare_plaintext_snapshot(
            &sandbox_id,
            &origin_path,
            &cow_path,
            &encrypted_cow_path,
        )
        .await;
        let prepared = record_outcome(&metric, result)?;

        Ok(Response::new(PrepareSnapshotRootfsResponse {
            mapper_path: prepared.mapper_path.to_string_lossy().into_owned(),
        }))
    }

    pub(super) async fn prepare_encrypted_volume(
        &self,
        request: Request<PrepareEncryptedVolumeRequest>,
    ) -> Result<Response<PrepareEncryptedVolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "PrepareEncryptedVolume",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox_id.to_string(),
            crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
            async move {
                service
                    .prepare_encrypted_volume_inner(sandbox_id, request.into_inner())
                    .await
            },
        )
        .await
    }

    async fn prepare_encrypted_volume_inner(
        &self,
        sandbox_id: SandboxId,
        req: PrepareEncryptedVolumeRequest,
    ) -> Result<Response<PrepareEncryptedVolumeResponse>, Status> {
        let metric = DeviceOpMetric::prepare(pplx_space_metrics::byok::OP_SNAPSHOT_VOLUME);
        self.check_execution(&sandbox_id.to_string(), req.execution_fence.as_ref(), false)?;
        let volume_name = validate_volume_name(&req.volume_name)?;

        // The backing always lives at the canonical instance path. A restore
        // source (the downloaded `<volume_name>.luks`) is *staged to* that
        // path before opening — mirroring `restore_cow_source` on the rootfs
        // — so a later re-suspend seals and stages the same canonical file a
        // fresh suspend would, even when the restore landed on a new node.
        let backing_path = volume_backing_path(&sandbox_id, volume_name);
        let restore_source = match req
            .restore_backing_source
            .as_deref()
            .filter(|s| !s.is_empty())
        {
            Some(raw) => Some(validate_sandbox_scoped_path(
                &sandbox_id,
                raw,
                "restore_backing_source",
            )?),
            None => None,
        };
        // Reject the fresh+restore contradiction here, before the side-effectful
        // DEK fetch below issues and persists key material for a request that
        // will only be rejected.
        if req.fresh && restore_source.is_some() {
            return Err(Status::invalid_argument(
                "restore_backing_source is only valid when opening (fresh = false)",
            ));
        }
        let mount_dir = volume_mount_dir(&sandbox_id, volume_name);

        // Same single per-sandbox DEK as the rootfs COW — credmgr returns it
        // keyed by sandbox_id (already issued by the time any non-rootfs
        // volume is prepared). The caller never handles a KEK or wrap.
        let plaintext_dek = fetch_sandbox_volume_dek(&sandbox_id, None)
            .await
            .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_DEK))?;

        let volume_name = volume_name.to_string();
        let operation_sandbox_id = sandbox_id;
        let operation = async move {
            let prepared = encrypted_volume::prepare(encrypted_volume::PrepareParams {
                sandbox_id: operation_sandbox_id,
                volume_name,
                backing_path,
                restore_source,
                mount_dir,
                size_bytes: req.size_bytes,
                dek: &plaintext_dek,
                fresh: req.fresh,
            })
            .await;
            let prepared = record_outcome(&metric, prepared)?;

            Ok(Response::new(PrepareEncryptedVolumeResponse {
                mount_dir: prepared.mount_dir.to_string_lossy().into_owned(),
            }))
        };
        operation.await
    }

    pub(super) async fn release_encrypted_volume(
        &self,
        request: Request<ReleaseEncryptedVolumeRequest>,
    ) -> Result<Response<ReleaseEncryptedVolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "ReleaseEncryptedVolume",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = parse_sandbox_id(&request.get_ref().sandbox_id)?;
        let service = self.clone();
        self.run_owned_device_operation(
            &sandbox_id.to_string(),
            crate::lifecycle_limiter::LifecycleMethod::Release,
            async move {
                service
                    .release_encrypted_volume_inner(sandbox_id, request.into_inner())
                    .await
            },
        )
        .await
    }

    async fn release_encrypted_volume_inner(
        &self,
        sandbox_id: SandboxId,
        req: ReleaseEncryptedVolumeRequest,
    ) -> Result<Response<ReleaseEncryptedVolumeResponse>, Status> {
        let metric = DeviceOpMetric::release(pplx_space_metrics::byok::OP_SNAPSHOT_VOLUME);
        self.check_execution(&sandbox_id.to_string(), req.execution_fence.as_ref(), false)?;
        let volume_name = validate_volume_name(&req.volume_name)?.to_string();
        let mount_dir = volume_mount_dir(&sandbox_id, &volume_name);

        // Keep teardown and publication staging in one cancellation-shielded,
        // serialized operation. Once release has positively sealed the
        // backing, no retry or cleanup may reformat/delete it before staging
        // finishes reading the ciphertext.
        let operation_sandbox_id = sandbox_id;
        let operation = async move {
            // Not resolved here: staging below is part of the same RPC, so the
            // metric settles once the whole release-and-stage has either
            // finished or failed.
            if let Err(e) =
                encrypted_volume::release(&operation_sandbox_id, &volume_name, &mount_dir).await
            {
                metric.failed(dmcrypt_error_phase(&e));
                return Err(dmcrypt_error_to_status(e));
            }

            // Optionally stage sealed (ciphertext) artifacts into the caller's
            // publication staging dir. node-manager is privileged and can read
            // the sandbox-private `instance/` dir; the artifacts are LUKS2
            // containers, so the reflink copies carry only ciphertext.
            if !req.stage_dir.trim().is_empty() && !req.stage_artifacts.is_empty() {
                let stage_dir = validate_sandbox_scoped_path(
                    &operation_sandbox_id,
                    req.stage_dir.trim(),
                    "stage_dir",
                )
                .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_STAGE))?;
                // `sync` first so any in-flight writes are durable before the
                // copy. Best-effort: a failed sync degrades durability of the
                // staged copy, not correctness of the sealed volume.
                match rootfs_dmcrypt::run("sync", &[]).await {
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(sandbox_id = %operation_sandbox_id, %error, "failed to run sync before artifact staging");
                    }
                }
                for artifact in &req.stage_artifacts {
                    let src = validate_sandbox_scoped_path(
                        &operation_sandbox_id,
                        &artifact.src_path,
                        "stage_artifacts.src_path",
                    )
                    .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_STAGE))?;
                    let dst_name = validate_stage_dst_name(&artifact.dst_name)
                        .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_STAGE))?;
                    stage_contained(&operation_sandbox_id, src, stage_dir.join(dst_name))
                        .await
                        .inspect_err(|_| metric.failed(pplx_space_metrics::byok::PHASE_STAGE))?;
                }
            }
            metric.succeeded();
            Ok(Response::new(ReleaseEncryptedVolumeResponse {}))
        };
        operation.await
    }
}

/// Parse the wire sandbox id into the typed [`SandboxId`] every derived path
/// and dm name is keyed on.
fn parse_sandbox_id(raw: &str) -> Result<SandboxId, Status> {
    raw.trim()
        .parse()
        .map_err(|e| Status::invalid_argument(format!("sandbox_id: {e}")))
}

/// Canonical LUKS2 backing file for a named per-sandbox encrypted volume.
fn volume_backing_path(sandbox_id: &SandboxId, volume_name: &str) -> PathBuf {
    PathBuf::from(paths::sandbox_instance_dir(&sandbox_id.to_string()))
        .join(format!("{volume_name}.luks"))
}

/// Canonical mount point of a named per-sandbox encrypted volume.
fn volume_mount_dir(sandbox_id: &SandboxId, volume_name: &str) -> PathBuf {
    PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()))
        .join(format!("{volume_name}-vol"))
}

/// Validate a logical volume name. It lands in a dm mapper name and in
/// derived filesystem paths, so restrict it to a short, path- and
/// dm-safe alphabet.
fn validate_volume_name(raw: &str) -> Result<&str, Status> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(Status::invalid_argument("volume_name must not be empty"));
    }
    if name.len() > MAX_VOLUME_NAME_LEN {
        return Err(Status::invalid_argument(format!(
            "volume_name must be at most {MAX_VOLUME_NAME_LEN} characters"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(Status::invalid_argument(
            "volume_name must contain only lowercase ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(name)
}

/// Validate a caller-supplied absolute path and require it to live under the
/// calling sandbox's cache dir. Purely lexical — rejects relative paths and
/// `.`/`..` components rather than canonicalizing — so the check can't be
/// confused by traversal sequences; the cache dir's contents themselves are
/// only writable by node services, not by guests.
fn validate_sandbox_scoped_path(
    sandbox_id: &SandboxId,
    raw: &str,
    field: &'static str,
) -> Result<PathBuf, Status> {
    let path = Path::new(raw);
    if !path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "{field} must be an absolute path"
        )));
    }
    if path
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(Status::invalid_argument(format!(
            "{field} must not contain '.' or '..' components"
        )));
    }
    let sandbox_dir = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    if !path.starts_with(&sandbox_dir) {
        return Err(Status::invalid_argument(format!(
            "{field} must live under the sandbox cache dir {}",
            sandbox_dir.display()
        )));
    }
    Ok(path.to_path_buf())
}

/// Validate a staged artifact's destination name: a bare file name inside
/// the (already-validated) staging dir.
fn validate_stage_dst_name(raw: &str) -> Result<&str, Status> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(Status::invalid_argument(
            "stage_artifacts.dst_name must not be empty",
        ));
    }
    if name == "." || name == ".." || name.contains('/') {
        return Err(Status::invalid_argument(
            "stage_artifacts.dst_name must be a bare file name",
        ));
    }
    Ok(name)
}

/// Copy a boundary-crossing artifact with both endpoints resolved
/// descriptor-relative beneath the sandbox's cache dir and symlinks rejected
/// at every component ([`crate::contained_copy`]) — the lexical
/// [`validate_sandbox_scoped_path`] check alone cannot stop a symlink the
/// caller plants inside the sandbox dir. Reflink (O(1), sparse) on btrfs,
/// hole-preserving byte copy on non-CoW filesystems (dev/test hosts). Runs
/// on the blocking pool.
async fn stage_contained(sandbox_id: &SandboxId, src: PathBuf, dst: PathBuf) -> Result<(), Status> {
    let root = PathBuf::from(paths::sandbox_cache_dir(&sandbox_id.to_string()));
    tokio::task::spawn_blocking(move || {
        stage_ciphertext(
            &root,
            &src,
            &dst,
            pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("artifact staging task failed: {error}")))?
}

fn stage_ciphertext(root: &Path, src: &Path, dst: &Path, reader_gid: u32) -> Result<(), Status> {
    let copied =
        crate::contained_copy::stage_file_contained(root, src, dst).map_err(
            |error| match error {
                e @ crate::contained_copy::ContainedCopyError::Escape { .. } => {
                    Status::invalid_argument(e.to_string())
                }
                e @ crate::contained_copy::ContainedCopyError::Io { .. } => {
                    Status::internal(e.to_string())
                }
            },
        )?;
    // Backings remain private. Only the published ciphertext copy is shared
    // with the unprivileged uploader, using the returned inode rather than a
    // second lookup through the caller-writable staging directory.
    super::trusted_paths::set_permissions(&copied, Some(reader_gid), 0o660)?;
    copied
        .sync_all()
        .map_err(|error| Status::internal(format!("sync staged ciphertext permissions: {error}")))
}

/// Resolve the sandbox's single volume DEK from credmgr. The plaintext lives
/// in [`Zeroizing`] so it scrubs on drop; keep the binding short-lived.
/// Connect failures surface as `Unavailable` (retryable by the orchestrator);
/// credmgr's own status code passes through otherwise, preserving its
/// permanent-vs-transient taxonomy.
///
/// `expected_org` makes credmgr verify the sandbox's resolved owner before
/// issuing or unwrapping anything — callers that assert ownership (the
/// claim-time re-home) set it so a stale ownership view fails fast instead of
/// minting a wrap the real owner can never unwrap. Callers that follow
/// ownership (restart/restore of an already-encrypted sandbox) pass `None`.
async fn fetch_sandbox_volume_dek(
    sandbox_id: &SandboxId,
    expected_org: Option<OrgId>,
) -> Result<Zeroizing<Vec<u8>>, Status> {
    use pplx_space_metrics::byok;

    byok::dek_fetch_attempted();
    let started = std::time::Instant::now();
    let mut client = pplx_space_credmgr_client::connect_from_env()
        .await
        .map_err(|e| {
            // Never reached credmgr at all: the node-local socket is missing or
            // its peer is down. Distinct from a refusal, and distinct from an
            // authority-side error, because the remedy is on this node.
            byok::dek_fetch_failed(byok::DEK_UNREACHABLE);
            Status::unavailable(format!("credmgr connect: {e}"))
        })?;
    let mut resp = client
        .get_or_issue_sandbox_volume_dek(
            pplx_space_credmgr_client::proto::GetOrIssueSandboxVolumeDekRequest {
                sandbox_id: sandbox_id.to_string(),
                expected_org_id: expected_org.map(|org| org.to_string()).unwrap_or_default(),
            },
        )
        .await
        .map_err(|status| {
            byok::dek_fetch_failed(dek_failure_reason(status.code()));
            credmgr_status_with_context(status)
        })?
        .into_inner();
    byok::dek_fetch_duration(started);
    Ok(Zeroizing::new(std::mem::take(&mut resp.plaintext_dek)))
}

fn credmgr_status_with_context(status: Status) -> Status {
    let message = format!("credmgr GetOrIssueSandboxVolumeDek: {}", status.message());
    let details = tonic::codegen::Bytes::copy_from_slice(status.details());
    let metadata = status.metadata().clone();
    Status::with_details_and_metadata(status.code(), message, details, metadata)
}

/// Bucket a credmgr DEK-fetch status into the metric `reason` vocabulary.
///
/// The split that matters operationally is "credmgr decided no" versus
/// "credmgr could not answer": a refusal means an org is unenrolled, or its
/// KEK is revoked, and no amount of retrying helps, while the rest are
/// availability problems that resolve on their own.
fn dek_failure_reason(code: tonic::Code) -> &'static str {
    use pplx_space_metrics::byok;
    match code {
        tonic::Code::FailedPrecondition
        | tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::NotFound
        | tonic::Code::InvalidArgument => byok::DEK_REFUSED,
        tonic::Code::Unavailable => byok::DEK_UNREACHABLE,
        _ => byok::DEK_ERROR,
    }
}

/// Map a device-layer failure onto a gRPC status. Permanent caller mistakes
/// (bad params, kernel can't do it) become `InvalidArgument` /
/// `FailedPrecondition`; a full disk is `ResourceExhausted`; the command/io
/// rest is `Internal` so the orchestrator retries.
fn dmcrypt_error_to_status(e: rootfs_dmcrypt::DmCryptError) -> Status {
    use rootfs_dmcrypt::DmCryptError;
    match e {
        DmCryptError::InvalidParams(msg) => Status::invalid_argument(msg),
        DmCryptError::KernelUnsupported(msg) => Status::failed_precondition(msg),
        DmCryptError::InsufficientDisk(msg) => Status::resource_exhausted(msg),
        e
        @ (DmCryptError::Command { .. } | DmCryptError::Busy { .. } | DmCryptError::Io { .. }) => {
            Status::internal(e.to_string())
        }
    }
}

/// The metric `phase` for a device-primitive failure.
///
/// Exhaustive over [`rootfs_dmcrypt::DmCryptError`] on purpose: callers see
/// every failure flattened into `Status::internal` plus a message, so this tag
/// is the only place the typed distinction survives. A new variant fails to
/// compile here rather than landing silently in a catch-all.
fn dmcrypt_error_phase(e: &rootfs_dmcrypt::DmCryptError) -> &'static str {
    use pplx_space_metrics::byok;
    use rootfs_dmcrypt::DmCryptError;
    match e {
        DmCryptError::KernelUnsupported(_) => byok::PHASE_KERNEL,
        DmCryptError::InvalidParams(_) => byok::PHASE_PARAMS,
        DmCryptError::InsufficientDisk(_) => byok::PHASE_DISK,
        DmCryptError::Command { .. } | DmCryptError::Busy { .. } | DmCryptError::Io { .. } => {
            byok::PHASE_DEVICE
        }
    }
}

/// Which counter pair a [`DeviceOpMetric`] reports into.
#[derive(Clone, Copy)]
enum DeviceOpKind {
    Prepare,
    Release,
}

/// Records `attempted` when constructed and a terminal outcome exactly once:
/// [`Self::succeeded`], [`Self::failed`], or — if the handler returns without
/// calling either — a `phase:params` failure on drop.
///
/// The drop arm is the point of the type. These handlers validate several
/// request fields after the attempt is counted, and every one of those early
/// returns has to land in `failed` or the attempted/failed rate the catalog
/// tells people to compute silently overstates success. Recording it at each
/// exit worked until someone added the next exit; recording it on drop cannot
/// be forgotten. A future early return that is genuinely not a parameter
/// problem is then merely mis-phased rather than uncounted, which is the
/// better failure mode.
struct DeviceOpMetric {
    op: &'static str,
    kind: DeviceOpKind,
    started: std::time::Instant,
    resolved: std::cell::Cell<bool>,
}

impl DeviceOpMetric {
    fn prepare(op: &'static str) -> Self {
        pplx_space_metrics::byok::device_prepare_attempted(op);
        Self::new(op, DeviceOpKind::Prepare)
    }

    fn release(op: &'static str) -> Self {
        pplx_space_metrics::byok::device_release_attempted(op);
        Self::new(op, DeviceOpKind::Release)
    }

    fn new(op: &'static str, kind: DeviceOpKind) -> Self {
        Self {
            op,
            kind,
            started: std::time::Instant::now(),
            resolved: std::cell::Cell::new(false),
        }
    }

    fn failed(&self, phase: &'static str) {
        if self.resolved.replace(true) {
            return;
        }
        match self.kind {
            DeviceOpKind::Prepare => {
                pplx_space_metrics::byok::device_prepare_failed(self.op, phase)
            }
            DeviceOpKind::Release => {
                pplx_space_metrics::byok::device_release_failed(self.op, phase)
            }
        }
    }

    fn succeeded(&self) {
        if self.resolved.replace(true) {
            return;
        }
        if matches!(self.kind, DeviceOpKind::Prepare) {
            pplx_space_metrics::byok::device_prepare_duration(self.op, self.started);
        }
    }
}

impl Drop for DeviceOpMetric {
    fn drop(&mut self) {
        self.failed(pplx_space_metrics::byok::PHASE_PARAMS);
    }
}

/// Record a device primitive's outcome against `metric`, then convert the
/// error for the wire.
fn record_outcome<T>(
    metric: &DeviceOpMetric,
    result: Result<T, rootfs_dmcrypt::DmCryptError>,
) -> Result<T, Status> {
    match result {
        Ok(value) => {
            metric.succeeded();
            Ok(value)
        }
        Err(e) => {
            metric.failed(dmcrypt_error_phase(&e));
            Err(dmcrypt_error_to_status(e))
        }
    }
}

#[cfg(test)]
mod tests;
