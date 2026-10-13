//! Privileged btrfs delegation handlers.
//!
//! These six RPCs let the unprivileged template-manager and sandbox-
//! manager services delegate the btrfs operations that need
//! `CAP_SYS_ADMIN` to node-manager — which runs as the privileged
//! daemon in production (root + `privileged: true` in the K8s
//! daemonset; `User=root` in the systemd direct-deploy unit). In
//! the retired compose harness all three services ran as root too, but
//! tmplmgr and sbxmgr had `CAP_SYS_ADMIN` dropped from their bounding set
//! via `setpriv` to mirror the production capability boundary.
//!
//! The six RPCs split into two clusters:
//!
//! - **Template volume lifecycle** (caller: tmplmgr) —
//!   `BtrfsCreateTemplateSubvolume`,
//!   `BtrfsFinalizeTemplateSubvolume`,
//!   `BtrfsDeleteTemplateSubvolume`.
//! - **Sandbox suspend/restore codec** (caller: sbxmgr) —
//!   `BtrfsEncodeRootfsSend`,
//!   `BtrfsReceiveInstance`,
//!   `BtrfsDeleteSandboxInstance`.
//!
//! Each RPC accepts domain identifiers (`template_name + content_hash`,
//! `sandbox_id`) and derives the canonical host paths internally.
//! Callers never pass arbitrary paths, so a compromised caller can't
//! ask nodemgr to act on filesystem locations outside the SPACE cache.

use std::path::{Path, PathBuf};

use pplx_space_common_svc::UdsPeerPrincipal;
use pplx_space_common_svc::core::host::linux::identities::user_names;
use tonic::{Request, Response, Status};

use crate::proto::{
    BtrfsCreateTemplateSubvolumeRequest, BtrfsCreateTemplateSubvolumeResponse,
    BtrfsDeleteSandboxInstanceRequest, BtrfsDeleteSandboxInstanceResponse,
    BtrfsDeleteTemplateSubvolumeRequest, BtrfsDeleteTemplateSubvolumeResponse,
    BtrfsEncodeRootfsSendRequest, BtrfsEncodeRootfsSendResponse,
    BtrfsEncodeSnapshotRootfsSendRequest, BtrfsEncodeSnapshotRootfsSendResponse,
    BtrfsFinalizeTemplateSubvolumeRequest, BtrfsFinalizeTemplateSubvolumeResponse,
    BtrfsReceiveInstanceRequest, BtrfsReceiveInstanceResponse,
};

use pplx_space_common_core::host::linux::paths::TEMPLATE_CACHE_DIR_MODE;
use pplx_space_common_svc::core::host::linux::identities::{gids, users};

use super::validation::validate_sandbox_id;
use super::{CACHE_DIR, NodeManagerService, TEMPLATE_DIR};
use super::{artifact_fs, trusted_paths};

const ALLOW_NODEMGR_ADMIN_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(pplx_space_common_svc::LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];

const ALLOW_NODEMGR_ADMIN_OR_SBXMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(pplx_space_common_svc::LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];

fn log_best_effort_cleanup<T, E: std::fmt::Display>(
    operation: &str,
    path: &Path,
    result: Result<T, E>,
) {
    if let Err(error) = result {
        tracing::debug!(
            operation,
            path = %path.display(),
            %error,
            "best-effort btrfs cleanup failed"
        );
    }
}

// Template-identifier validators. These keep callers from smuggling
// path-traversal sequences via `template_name` or `content_hash`. The
// callers' own typed wrappers (`TemplateName`, `ContentHash`) enforce
// the same rules, but nodemgr re-validates because it can't trust
// peers — this is the privilege boundary.

fn validate_template_name(name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("template_name must not be empty"));
    }
    // Match the allowed character set the cluster-Postgres
    // `templates.name` column accepts: ASCII alphanumeric + `-`, `_`,
    // `.`. Crucially excludes `/`, `..`, NUL, anything that would
    // escape the template_root.
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(Status::invalid_argument(format!(
            "template_name must be ASCII alphanumeric or [-_.]: got '{name}'"
        )));
    }
    if name == "." || name == ".." || name.starts_with('.') {
        return Err(Status::invalid_argument(format!(
            "template_name must not start with '.': got '{name}'"
        )));
    }
    Ok(())
}

fn validate_content_hash(hash: &str) -> Result<(), Status> {
    let Some(rest) = hash.strip_prefix("sha256:") else {
        return Err(Status::invalid_argument(format!(
            "content_hash must start with 'sha256:': got '{hash}'"
        )));
    };
    if rest.len() != 64 || !rest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Status::invalid_argument(format!(
            "content_hash must be 'sha256:' followed by 64 lowercase hex digits: got '{hash}'"
        )));
    }
    if rest.bytes().any(|b| b.is_ascii_uppercase()) {
        return Err(Status::invalid_argument(format!(
            "content_hash hex must be lowercase: got '{hash}'"
        )));
    }
    Ok(())
}

fn template_digest_dir(name: &str, content_hash: &str) -> PathBuf {
    PathBuf::from(format!("{TEMPLATE_DIR}/{name}/{content_hash}"))
}

fn sandbox_dir(sandbox_id: &str) -> PathBuf {
    PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"))
}

fn instance_dir(sandbox_id: &str) -> PathBuf {
    sandbox_dir(sandbox_id).join("instance")
}

fn snapshot_dir(sandbox_id: &str) -> PathBuf {
    sandbox_dir(sandbox_id).join("snapshot")
}

fn named_snapshot_send_source(sandbox_id: &str, snapshot_name: &str) -> Result<PathBuf, Status> {
    validate_sandbox_id(sandbox_id)?;
    super::validation::validate_snapshot_name(snapshot_name)?;
    Ok(sandbox_dir(sandbox_id)
        .join("snapshots")
        .join(snapshot_name))
}

fn named_snapshot_wire_source(artifact_dir: &Path) -> PathBuf {
    instance_ro_in_snapshot_dir(artifact_dir)
}

/// Path the suspend codec writes its RO snapshot to (next to the
/// other snapshot artifacts the FC `create_snapshot` step wrote).
fn instance_ro_in_snapshot_dir(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join("instance.ro")
}

/// Where the suspend codec writes its btrfs-send stream (consumed
/// later by the upload tail) and where the restore codec reads
/// it from (placed by sbxmgr's S3 pull).
fn rootfs_btrfs_send_path(snapshot_dir: &Path) -> PathBuf {
    // Wire-format filename for the btrfs-send rootfs codec. Mirrors
    // `crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME` in sbxmgr;
    // not promoted to common_core yet so we duplicate the literal
    // here. Both sides MUST agree.
    snapshot_dir.join("rootfs.btrfs-send")
}

/// Where the Full-codec rootfs lives — superseded by btrfs-send
/// stream on the suspend side, removed after a successful encode.
fn rootfs_ext4_in_snapshot_dir(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(pplx_space_common_core::types::TEMPLATE_ROOTFS_FILE)
}

struct ValidatedSendParent {
    path: PathBuf,
    uuid: uuid::Uuid,
}

fn validated_send_parent(
    template_name: &str,
    content_hash: &str,
) -> Result<ValidatedSendParent, Status> {
    validate_template_name(template_name)?;
    validate_content_hash(content_hash)?;
    let path = template_digest_dir(template_name, content_hash);
    if !artifact_fs::is_subvolume(&path)? {
        return Err(lineage_precondition(
            LineageFallbackCause::TemplateNotSubvolume,
            format!(
                "btrfs-send codec requires template {template_name}@{content_hash} to be a local \
                 btrfs subvolume; this node still has the legacy directory-form template. \
                 Retry with format=full or wait for tmplmgr to re-install this build."
            ),
        ));
    }
    let info = artifact_fs::info(&path).map_err(|error| {
        Status::internal(format!(
            "read template subvolume info {}: {error}",
            path.display()
        ))
    })?;
    let uuid = info.uuid.ok_or_else(|| {
        Status::internal(format!(
            "template subvolume at {} reports no UUID",
            path.display()
        ))
    })?;
    let expected_received =
        pplx_space_common_core::host::btrfs::deterministic_template_received_uuid(
            template_name,
            content_hash,
        );
    if info.received_uuid != Some(expected_received) {
        return Err(lineage_precondition(
            LineageFallbackCause::TemplateReceivedUuidMismatch,
            format!(
                "btrfs-send codec requires template received_uuid={expected_received} for \
                 cross-node stream portability; got {:?}. The template was likely installed \
                 before received_uuid stamping shipped; force a re-install via tmplmgr or \
                 retry with format=full.",
                info.received_uuid,
            ),
        ));
    }
    Ok(ValidatedSendParent { path, uuid })
}

fn write_parented_btrfs_send(
    parent: &Path,
    source: &Path,
    stream_path: &Path,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<u64, Status> {
    let parent = trusted_paths::open_directory(parent)?;
    let source = trusted_paths::open_directory(source)?;
    let output = artifact_fs::write_atomic(stream_path, |output| {
        artifact_fs::btrfs::send(&parent, &source, output).map_err(artifact_fs::error)?;
        // The ioctl cannot be interrupted by dropping a future. Keep ownership
        // through its return, then refuse publication for a cancelled caller.
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Err(Status::cancelled("btrfs stream transfer cancelled"));
        }
        trusted_paths::set_permissions(output, Some(gids::SPACE_CACHE_RW), 0o660)
    })?;
    output
        .metadata()
        .map(|m| m.len())
        .map_err(artifact_fs::error)
}

struct ArtifactDirectoryGuard {
    path: PathBuf,
    armed: bool,
}

impl ArtifactDirectoryGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

fn create_snapshot_artifact_directory(
    path: PathBuf,
    reader_gid: u32,
) -> Result<ArtifactDirectoryGuard, Status> {
    let (parent, name) = artifact_fs::parent(&path)?;
    nix::sys::stat::mkdirat(
        &parent,
        name,
        nix::sys::stat::Mode::from_bits_truncate(0o700),
    )
    .map_err(|error| {
        if error == nix::errno::Errno::EEXIST {
            Status::already_exists("snapshot artifact destination exists")
        } else {
            artifact_fs::error(error)
        }
    })?;
    let guard = ArtifactDirectoryGuard::new(path.clone());
    let directory = artifact_fs::directory(&parent, name).map_err(artifact_fs::error)?;
    trusted_paths::set_permissions(&directory, Some(reader_gid), 0o2770)?;
    Ok(guard)
}

impl Drop for ArtifactDirectoryGuard {
    fn drop(&mut self) {
        if self.armed {
            log_best_effort_cleanup(
                "remove incomplete snapshot artifact directory",
                &self.path,
                artifact_fs::delete(&self.path),
            );
        }
    }
}

const LINEAGE_CAUSE_METADATA: &str = "x-space-btrfs-lineage-cause";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineageFallbackCause {
    InstanceNotSubvolume,
    InstanceParentMismatch,
    SnapshotNotSubvolume,
    SnapshotParentMismatch,
    TemplateNotSubvolume,
    TemplateReceivedUuidMismatch,
}

impl LineageFallbackCause {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InstanceNotSubvolume => "instance_not_subvolume",
            Self::InstanceParentMismatch => "instance_parent_mismatch",
            Self::SnapshotNotSubvolume => "snapshot_not_subvolume",
            Self::SnapshotParentMismatch => "snapshot_parent_mismatch",
            Self::TemplateNotSubvolume => "template_not_subvolume",
            Self::TemplateReceivedUuidMismatch => "template_received_uuid_mismatch",
        }
    }
}

fn validate_named_snapshot_lineage(
    template_uuid: uuid::Uuid,
    instance: &pplx_space_common_core::host::btrfs::SubvolumeInfo,
    snapshot: &pplx_space_common_core::host::btrfs::SubvolumeInfo,
) -> Result<(), Status> {
    if instance.parent_uuid != Some(template_uuid) {
        return Err(lineage_precondition(
            LineageFallbackCause::InstanceParentMismatch,
            format!(
                "instance.parent_uuid is {:?}, expected {template_uuid}",
                instance.parent_uuid
            ),
        ));
    }
    let instance_uuid = instance.uuid.ok_or_else(|| {
        lineage_precondition(
            LineageFallbackCause::InstanceNotSubvolume,
            "instance subvolume reports no UUID".to_string(),
        )
    })?;
    if snapshot.parent_uuid != Some(instance_uuid) {
        return Err(lineage_precondition(
            LineageFallbackCause::SnapshotParentMismatch,
            format!(
                "snapshot.parent_uuid is {:?}, expected {instance_uuid}",
                snapshot.parent_uuid
            ),
        ));
    }
    Ok(())
}

fn lineage_precondition(cause: LineageFallbackCause, message: String) -> Status {
    let mut status = Status::failed_precondition(message);
    status.metadata_mut().insert(
        LINEAGE_CAUSE_METADATA,
        tonic::metadata::MetadataValue::from_static(cause.as_str()),
    );
    status
}

/// Deletes `path` on drop unless disarmed. The drop path runs two `btrfs`
/// subprocesses on the calling thread, so non-cancelled exits should prefer
/// [`disarm`](Self::disarm) plus an offloaded delete; the blocking drop
/// remains the backstop for RPC cancellation and early error returns.
struct SubvolumeCleanupGuard {
    path: PathBuf,
    operation: &'static str,
    armed: bool,
}

impl SubvolumeCleanupGuard {
    fn new(path: PathBuf, operation: &'static str) -> Self {
        Self {
            path,
            operation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SubvolumeCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            log_best_effort_cleanup(self.operation, &self.path, artifact_fs::delete(&self.path));
        }
    }
}

impl NodeManagerService {
    /// Returns the retained source identity and, for a deferred send, its
    /// rootfs image length, read back through the same verification later
    /// inspections use. A materialized send is sized by its stream file, so
    /// it adds no read-back that could fail it.
    async fn retain_send_source(
        &self,
        retain: bool,
        defer: bool,
        artifact: &Path,
        parent: &Path,
        source: &Path,
    ) -> Result<Option<(uuid::Uuid, Option<u64>)>, Status> {
        if !retain {
            return Ok(None);
        }
        let (artifact, parent, source) =
            (artifact.to_owned(), parent.to_owned(), source.to_owned());
        self.off_runtime("retain immutable send source", move || {
            let id = super::btrfs_send_source::capture(&artifact, &parent, &source)?;
            if !defer {
                return Ok(Some((id, None)));
            }
            // Not FailedPrecondition, which callers read as an ineligible
            // source and answer with a Full fallback beside this bundle.
            let rootfs_bytes =
                super::btrfs_send_source::inspect(&artifact, id).map_err(|status| {
                    Status::internal(format!("verify retained send source: {}", status.message()))
                })?;
            Ok(Some((id, Some(rootfs_bytes))))
        })
        .await
    }

    async fn encode_pinned_stream(
        &self,
        parent: &Path,
        source: &Path,
        destination: &Path,
    ) -> Result<u64, Status> {
        let (parent, source, destination) =
            (parent.to_owned(), source.to_owned(), destination.to_owned());
        let owner = self.artifact_work.clone();
        self.off_runtime("encode pinned btrfs stream", move || {
            write_parented_btrfs_send(
                &parent,
                &source,
                &destination,
                owner.as_ref().map(|owner| &owner.cancel),
            )
        })
        .await
    }

    pub(super) async fn btrfs_create_template_subvolume(
        &self,
        request: Request<BtrfsCreateTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsCreateTemplateSubvolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsCreateTemplateSubvolume",
            ALLOW_NODEMGR_ADMIN_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        validate_template_name(&req.template_name)?;
        validate_content_hash(&req.content_hash)?;

        let digest_dir = template_digest_dir(&req.template_name, &req.content_hash);
        let digest_dir_for_section = digest_dir.clone();
        self.off_runtime("create template subvolume", move || {
            let digest_dir = digest_dir_for_section;
            let parent = digest_dir
                .parent()
                .ok_or_else(|| artifact_fs::error("missing template parent"))?;
            let parent_fd = trusted_paths::create_directory(parent)?;
            trusted_paths::set_permissions(
                &parent_fd,
                Some(gids::SPACE_CACHE_RW),
                TEMPLATE_CACHE_DIR_MODE,
            )?;
            let name = digest_dir
                .file_name()
                .ok_or_else(|| artifact_fs::error("missing template digest"))?;
            artifact_fs::btrfs::create(&parent_fd, name).map_err(artifact_fs::error)?;
            let digest = artifact_fs::directory(&parent_fd, name).map_err(artifact_fs::error)?;
            nix::unistd::fchown(
                &digest,
                Some(nix::unistd::Uid::from_raw(users::SPACE_TMPLMGR.uid)),
                Some(nix::unistd::Gid::from_raw(gids::SPACE_CACHE_RW)),
            )
            .map_err(artifact_fs::error)?;
            trusted_paths::set_permissions(&digest, None, TEMPLATE_CACHE_DIR_MODE)?;
            Ok(())
        })
        .await?;

        Ok(Response::new(BtrfsCreateTemplateSubvolumeResponse {
            digest_dir: digest_dir.to_string_lossy().into_owned(),
        }))
    }

    pub(super) async fn btrfs_finalize_template_subvolume(
        &self,
        request: Request<BtrfsFinalizeTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsFinalizeTemplateSubvolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsFinalizeTemplateSubvolume",
            ALLOW_NODEMGR_ADMIN_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        validate_template_name(&req.template_name)?;
        validate_content_hash(&req.content_hash)?;

        let digest_dir = template_digest_dir(&req.template_name, &req.content_hash);
        let received_uuid =
            pplx_space_common_core::host::btrfs::deterministic_template_received_uuid(
                &req.template_name,
                &req.content_hash,
            );
        self.off_runtime("finalize template subvolume", move || {
            let digest = trusted_paths::open_directory(&digest_dir)?;
            let (_, info) = artifact_fs::btrfs::info(&digest).map_err(artifact_fs::error)?;
            if info.received_uuid != Some(received_uuid) {
                artifact_fs::btrfs::stamp(&digest, received_uuid).map_err(artifact_fs::error)?;
            }
            artifact_fs::btrfs::set_readonly(&digest, true).map_err(artifact_fs::error)
        })
        .await?;

        Ok(Response::new(BtrfsFinalizeTemplateSubvolumeResponse {
            received_uuid: received_uuid.to_string(),
        }))
    }

    pub(super) async fn btrfs_delete_template_subvolume(
        &self,
        request: Request<BtrfsDeleteTemplateSubvolumeRequest>,
    ) -> Result<Response<BtrfsDeleteTemplateSubvolumeResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsDeleteTemplateSubvolume",
            ALLOW_NODEMGR_ADMIN_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        validate_template_name(&req.template_name)?;
        validate_content_hash(&req.content_hash)?;

        let digest_dir = template_digest_dir(&req.template_name, &req.content_hash);
        self.off_runtime("delete template subvolume", move || {
            delete_subvolume_or_dir(&digest_dir)
        })
        .await?;
        Ok(Response::new(BtrfsDeleteTemplateSubvolumeResponse {}))
    }

    pub(super) async fn btrfs_encode_rootfs_send(
        &self,
        request: Request<BtrfsEncodeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsEncodeRootfsSend",
            ALLOW_NODEMGR_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service.btrfs_encode_rootfs_send_inner(request).await
        })
        .await
    }

    async fn btrfs_encode_rootfs_send_inner(
        &self,
        request: Request<BtrfsEncodeRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeRootfsSendResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let _gate = self.lock_sandbox_cgroups(&req.sandbox_id).await;
        self.check_filesystem_execution(&req.sandbox_id, req.execution_fence.as_ref())
            .await?;
        let snapshot_dir = match req.dest_dir.as_deref().filter(|path| !path.is_empty()) {
            Some(path) => {
                crate::runtime_paths::validate_create_snapshot_dest(&req.sandbox_id, path)?
            }
            None => snapshot_dir(&req.sandbox_id),
        };
        let send_parent = {
            let snapshot_dir = snapshot_dir.clone();
            let template_name = req.template_name.clone();
            let content_hash = req.content_hash.clone();
            self.off_runtime("validate suspend send parent", move || {
                trusted_paths::create_directory(&snapshot_dir).map_err(|error| {
                    Status::internal(format!(
                        "create snapshot destination {}: {error}",
                        snapshot_dir.display()
                    ))
                })?;
                validated_send_parent(&template_name, &content_hash)
            })
            .await?
        };
        let template_uuid = send_parent.uuid;

        let instance_path = instance_dir(&req.sandbox_id);
        let instance_probe = {
            let instance_path = instance_path.clone();
            self.off_runtime("probe suspend instance lineage", move || {
                Ok(artifact_fs::info(&instance_path))
            })
            .await?
        };
        let lineage_failure = match instance_probe {
            Ok(info) if info.parent_uuid == Some(template_uuid) => None,
            Ok(info) => Some((
                LineageFallbackCause::InstanceParentMismatch,
                format!(
                    "instance.parent_uuid is {:?}, expected {template_uuid}",
                    info.parent_uuid
                ),
            )),
            Err(error) => Some((
                LineageFallbackCause::InstanceNotSubvolume,
                format!("read instance subvolume info failed: {error}"),
            )),
        };

        if let Some((cause, detail)) = lineage_failure {
            return Err(lineage_precondition(
                cause,
                format!(
                    "btrfs-send codec cannot use {}: {detail}; retry with format=full",
                    instance_path.display()
                ),
            ));
        }

        let instance_ro_path = instance_ro_in_snapshot_dir(&snapshot_dir);
        {
            let instance_ro_path = instance_ro_path.clone();
            let send_source = instance_path.clone();
            self.off_runtime("snapshot suspend send source", move || {
                // Defensive: a half-finished prior suspend might have left
                // this around. Use the force variant so a stale RO subvolume
                // doesn't block the snapshot below.
                artifact_fs::delete(&instance_ro_path).map_err(|e| {
                    Status::internal(format!("stale instance.ro cleanup failed: {e}"))
                })?;
                artifact_fs::snapshot(&send_source, &instance_ro_path, true)
                    .map_err(|e| Status::internal(format!("btrfs ro snapshot of instance: {e}")))
            })
            .await?;
        }

        let retained = self
            .retain_send_source(
                req.retain_source,
                req.defer_send,
                &snapshot_dir,
                &send_parent.path,
                &instance_ro_path,
            )
            .await?;
        let stream_path = rootfs_btrfs_send_path(&snapshot_dir);
        let encode_result = if req.defer_send && retained.is_some() {
            Ok(0)
        } else {
            self.encode_pinned_stream(&send_parent.path, &instance_ro_path, &stream_path)
                .await
        };
        let stream_bytes = match encode_result {
            Ok(stream_bytes) => stream_bytes,
            Err(error) => {
                let instance_ro_path = instance_ro_path.clone();
                self.off_runtime_best_effort(move || {
                    log_best_effort_cleanup(
                        "delete temporary instance.ro subvolume",
                        &instance_ro_path,
                        artifact_fs::delete(&instance_ro_path),
                    );
                })
                .await;
                return Err(error);
            }
        };

        {
            let rootfs_ext4 = rootfs_ext4_in_snapshot_dir(&snapshot_dir);
            let instance_ro_path = instance_ro_path.clone();
            self.off_runtime_best_effort(move || {
                // Drop rootfs.ext4 — the stream replaces it. Leaving both in
                // the snapshot dir would confuse the per-format upload set
                // and the restore-side codec dispatch.
                log_best_effort_cleanup(
                    "remove encoded ext4 rootfs",
                    &rootfs_ext4,
                    artifact_fs::unlink(&rootfs_ext4),
                );
                // Tear down the RO `instance.ro` snapshot now that the send
                // has finished. Without this, the snapshot dir keeps a btrfs
                // subvolume alive until the next suspend's stale-purge runs.
                log_best_effort_cleanup(
                    "delete encoded instance.ro subvolume",
                    &instance_ro_path,
                    artifact_fs::delete(&instance_ro_path),
                );
            })
            .await;
        }

        tracing::info!(
            sandbox_id = %req.sandbox_id,
            template = %req.template_name,
            content_hash = %req.content_hash,
            stream = %stream_path.display(),
            stream_bytes,
            "btrfs-send codec: rootfs encoded as btrfs send -p stream"
        );
        Ok(Response::new(BtrfsEncodeRootfsSendResponse {
            stream_bytes,
            source_id: retained.map(|(id, _)| id.to_string()),
            rootfs_bytes: retained.and_then(|(_, bytes)| bytes),
        }))
    }

    pub(super) async fn btrfs_encode_snapshot_rootfs_send(
        &self,
        request: Request<BtrfsEncodeSnapshotRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeSnapshotRootfsSendResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsEncodeSnapshotRootfsSend",
            ALLOW_NODEMGR_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service
                .btrfs_encode_snapshot_rootfs_send_inner(request)
                .await
        })
        .await
    }

    async fn btrfs_encode_snapshot_rootfs_send_inner(
        &self,
        request: Request<BtrfsEncodeSnapshotRootfsSendRequest>,
    ) -> Result<Response<BtrfsEncodeSnapshotRootfsSendResponse>, Status> {
        let req = request.get_ref();
        let source = named_snapshot_send_source(&req.sandbox_id, &req.snapshot_name)?;
        let send_parent = {
            let source = source.clone();
            let template_name = req.template_name.clone();
            let content_hash = req.content_hash.clone();
            let instance_path = instance_dir(&req.sandbox_id);
            self.off_runtime("validate named-snapshot send lineage", move || {
                if !artifact_fs::is_subvolume(&source)? {
                    return Err(lineage_precondition(
                        LineageFallbackCause::SnapshotNotSubvolume,
                        format!(
                            "named snapshot {} is not a btrfs subvolume",
                            source.display()
                        ),
                    ));
                }
                let send_parent = validated_send_parent(&template_name, &content_hash)?;
                let instance_info = artifact_fs::info(&instance_path).map_err(|error| {
                    lineage_precondition(
                        LineageFallbackCause::InstanceNotSubvolume,
                        format!("read instance subvolume info failed: {error}"),
                    )
                })?;
                let snapshot_info = artifact_fs::info(&source).map_err(|error| {
                    lineage_precondition(
                        LineageFallbackCause::SnapshotNotSubvolume,
                        format!("read named snapshot subvolume info failed: {error}"),
                    )
                })?;
                validate_named_snapshot_lineage(send_parent.uuid, &instance_info, &snapshot_info)?;
                Ok(send_parent)
            })
            .await?
        };
        let destination =
            crate::runtime_paths::validate_create_snapshot_dest(&req.sandbox_id, &req.dest_dir)?;
        if destination == source
            || destination.starts_with(&source)
            || source.starts_with(&destination)
        {
            return Err(Status::invalid_argument(
                "snapshot artifact destination must be separate from its immutable source",
            ));
        }
        let mut destination_guard =
            create_snapshot_artifact_directory(destination.clone(), gids::SPACE_CACHE_RW)?;
        // Do not put the operation-specific named snapshot directly on the
        // wire. Restore expects the received subvolume to be `instance.ro`,
        // and a same-node receive cannot import a stream whose source identity
        // is still present on that filesystem. A short-lived RO clone gives
        // the stream its stable wire basename and a fresh identity while
        // preserving the exact frozen bytes.
        let wire_source = named_snapshot_wire_source(&destination);
        {
            let source = source.clone();
            let wire_source = wire_source.clone();
            self.off_runtime("clone named snapshot for send", move || {
                artifact_fs::snapshot(&source, &wire_source, true).map_err(|error| {
                    Status::internal(format!(
                        "create named snapshot wire clone {}: {error}",
                        wire_source.display()
                    ))
                })
            })
            .await?;
        }
        let mut wire_source_cleanup =
            SubvolumeCleanupGuard::new(wire_source.clone(), "delete named snapshot wire clone");
        let retained = self
            .retain_send_source(
                req.retain_source,
                req.defer_send,
                &destination,
                &send_parent.path,
                &wire_source,
            )
            .await?;
        let stream_path = rootfs_btrfs_send_path(&destination);
        let send_result = if req.defer_send && retained.is_some() {
            Ok(0)
        } else {
            self.encode_pinned_stream(&send_parent.path, &wire_source, &stream_path)
                .await
        };
        // Even on cancellation the send ioctl has returned before cleanup.
        // The wire clone is dead weight on success and failure alike.
        wire_source_cleanup.disarm();
        self.off_runtime_best_effort(move || {
            log_best_effort_cleanup(
                "delete named snapshot wire clone",
                &wire_source,
                artifact_fs::delete(&wire_source),
            );
        })
        .await;
        let stream_bytes = send_result?;
        destination_guard.disarm();

        tracing::info!(
            sandbox_id = %req.sandbox_id,
            snapshot_name = %req.snapshot_name,
            template = %req.template_name,
            content_hash = %req.content_hash,
            stream = %stream_path.display(),
            stream_bytes,
            "encoded immutable named snapshot as parent-relative btrfs stream"
        );
        Ok(Response::new(BtrfsEncodeSnapshotRootfsSendResponse {
            stream_bytes,
            source_id: retained.map(|(id, _)| id.to_string()),
            rootfs_bytes: retained.and_then(|(_, bytes)| bytes),
        }))
    }

    pub(super) async fn btrfs_receive_instance(
        &self,
        request: Request<BtrfsReceiveInstanceRequest>,
    ) -> Result<Response<BtrfsReceiveInstanceResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsReceiveInstance",
            ALLOW_NODEMGR_ADMIN_OR_SBXMGR,
        )?;
        let sandbox_id = request.get_ref().sandbox_id.clone();
        self.run_artifact_mutation(&sandbox_id, |service| async move {
            service.btrfs_receive_instance_inner(request).await
        })
        .await
    }

    async fn btrfs_receive_instance_inner(
        &self,
        request: Request<BtrfsReceiveInstanceRequest>,
    ) -> Result<Response<BtrfsReceiveInstanceResponse>, Status> {
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        validate_template_name(&req.template_name)?;
        validate_content_hash(&req.content_hash)?;
        let target = req
            .target
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("instance target is required"))?
            .try_into()?;
        let _gate = self.lock_sandbox_cgroups(&req.sandbox_id).await;
        let sandbox_root = self.instance_target_root(&req.sandbox_id, target).await?;
        self.btrfs_receive_instance_at(
            &req.sandbox_id,
            &req.template_name,
            &req.content_hash,
            sandbox_root,
        )
        .await
    }

    async fn btrfs_receive_instance_at(
        &self,
        sandbox_id: &str,
        template_name: &str,
        content_hash: &str,
        sandbox_root: PathBuf,
    ) -> Result<Response<BtrfsReceiveInstanceResponse>, Status> {
        let template_dir = template_digest_dir(template_name, content_hash);
        let instance_path = sandbox_root.join("instance");
        let stream_path = rootfs_btrfs_send_path(&sandbox_root.join("snapshot"));
        let stream_len = {
            let template_dir = template_dir.clone();
            let template_name = template_name.to_string();
            let content_hash = content_hash.to_string();
            let stream_path = stream_path.clone();
            self.off_runtime("validate restore template", move || {
                let template_path = template_dir.as_path();
                // These two precondition checks (template-is-a-subvolume and
                // template-has-the-expected-`received_uuid`) match the encode
                // side's checks in `BtrfsEncodeRootfsSend`. They surface as
                // `FailedPrecondition` so the caller can detect "this codec
                // can't proceed against this node's template" on the wire and
                // fall back to the Full codec — distinct from `Internal`,
                // which the caller treats as a real codec failure.
                let template_info = artifact_fs::info(template_path).map_err(|e| {
                    Status::failed_precondition(format!(
                        "btrfs-send restore needs template {}@{} as a btrfs subvolume on \
                                 this node; read_subvolume_info({}) failed: {e}",
                        template_name,
                        content_hash,
                        template_path.display(),
                    ))
                })?;
                let expected_received =
                    pplx_space_common_core::host::btrfs::deterministic_template_received_uuid(
                        &template_name,
                        &content_hash,
                    );
                if template_info.received_uuid != Some(expected_received) {
                    return Err(Status::failed_precondition(format!(
                        "btrfs-send restore needs template {}@{} \
                         received_uuid={expected_received} on this node; got {:?}. The template \
                         was installed before the stamp landed; force a re-install via tmplmgr \
                         to recover.",
                        template_name, content_hash, template_info.received_uuid,
                    )));
                }
                Ok(std::fs::metadata(&stream_path)
                    .ok()
                    .map(|metadata| metadata.len()))
            })
            .await?
        };
        let restore_reservation_bytes = stream_len.map(|len| {
            len.saturating_add(self.disk_admission.config().allocation_reservation_bytes)
        });
        let disk_reservation = self.disk_admission.reserve(
            crate::disk_admission::DiskAllocationOperation::BtrfsReceiveInstance,
            restore_reservation_bytes,
        )?;

        let owner = self.artifact_work.clone();
        self.off_runtime("receive and publish btrfs stream", move || {
            let receive = artifact_fs::receive::ReceiveRoot::open(&sandbox_root)?;
            let stream = receive.stream()?;
            receive.receive(stream, owner.as_ref().map(|owner| &owner.cancel))
        })
        .await?;

        tracing::info!(
            sandbox_id,
            template = template_name,
            content_hash,
            instance = %instance_path.display(),
            "btrfs-send codec: instance reconstructed from btrfs receive"
        );
        self.disk_admission
            .release_after_success(disk_reservation)
            .await;
        Ok(Response::new(BtrfsReceiveInstanceResponse {}))
    }

    pub(super) async fn btrfs_delete_sandbox_instance(
        &self,
        request: Request<BtrfsDeleteSandboxInstanceRequest>,
    ) -> Result<Response<BtrfsDeleteSandboxInstanceResponse>, Status> {
        self.authorize_peer(
            &request,
            "BtrfsDeleteSandboxInstance",
            ALLOW_NODEMGR_ADMIN_OR_SBXMGR,
        )?;
        let req = request.get_ref();
        validate_sandbox_id(&req.sandbox_id)?;
        let sandbox_id = req.sandbox_id.clone();
        let target = req
            .target
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("instance target is required"))?
            .try_into()?;
        let guard = self.lock_sandbox_artifacts(&sandbox_id).await?;
        let claim = self.claim_sandbox_mutation(&sandbox_id)?;
        let gate = self.lock_sandbox_cgroups(&sandbox_id).await;
        let sandbox_root = self.instance_target_root(&sandbox_id, target).await?;
        self.off_runtime("delete instance artifacts", move || {
            let _ownership = (guard, claim, gate);
            delete_sandbox_instance_artifacts(&sandbox_id, sandbox_root)
        })
        .await
    }
}

fn delete_sandbox_instance_artifacts(
    sandbox_id: &str,
    sandbox_root: PathBuf,
) -> Result<Response<BtrfsDeleteSandboxInstanceResponse>, Status> {
    delete_sandbox_instance_artifacts_with(sandbox_id, sandbox_root, artifact_fs::delete)
}

fn delete_sandbox_instance_artifacts_with(
    sandbox_id: &str,
    sandbox_root: PathBuf,
    mut delete: impl FnMut(&Path) -> Result<(), Status>,
) -> Result<Response<BtrfsDeleteSandboxInstanceResponse>, Status> {
    // Acknowledgement covers these instance targets only. Retained snapshots
    // and other sandbox artifacts remain owned by their lifecycle operations.
    let mut errors = Vec::new();
    for path in [
        sandbox_root.join("instance"),
        sandbox_root.join("instance.ro"),
        instance_ro_in_snapshot_dir(&sandbox_root.join("snapshot")),
    ] {
        // Independent targets can be reclaimed even while one remains busy.
        if let Err(error) = delete(&path) {
            errors.push((path, error));
        }
    }
    if let Some((_, first)) = errors.first() {
        return Err(Status::new(
            first.code(),
            errors
                .iter()
                .map(|(path, error)| format!("{}: {}", path.display(), error.message()))
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }
    tracing::info!(
        sandbox_id,
        sandbox_root = %sandbox_root.display(),
        "removed sandbox instance btrfs artifacts"
    );
    Ok(Response::new(BtrfsDeleteSandboxInstanceResponse {}))
}

#[cfg(test)]
#[path = "btrfs_delegation/snapshot_send_tests.rs"]
mod snapshot_send_tests;

#[cfg(test)]
mod validator_tests;

/// Subvolume-aware delete with plain-dir fallback. Used by both
/// `BtrfsDeleteTemplateSubvolume` and the pre-receive scrub in
/// `BtrfsReceiveInstance`. Idempotent: returns Ok if the path
/// doesn't exist.
fn delete_subvolume_or_dir(path: &Path) -> Result<(), Status> {
    artifact_fs::delete(path)
}
