use std::{
    collections::{HashMap, HashSet},
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use pplx_space_common_core::host::linux::identities::{user_names, users};
use pplx_space_common_core::host::linux::paths::{
    TEMPLATE_CACHE_DIR_MODE, normalize_template_cache_tree as normalize_template_cache_tree_io,
    set_template_cache_permissions as set_template_cache_permissions_io,
};
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, DEFAULT_BASE_SNAPSHOT_MEMORY_MIB, DEFAULT_BASE_SNAPSHOT_VCPU, Digest,
    HashAlg, MIN_BASE_SNAPSHOT_MEMORY_MIB, MIN_BASE_SNAPSHOT_VCPU, RUNTIME_FIRECRACKER_ALIAS,
    TEMPLATE_ROOTFS_FILE, TEMPLATE_ROOTFS_ZSTD_CODEC, TEMPLATE_ROOTFS_ZSTD_FILE,
    TEMPLATE_ROOTFS_ZSTD_LEVEL,
};
use pplx_space_common_svc::{
    AuthorizedUdsPeer, LocalRole, PeerIdentity, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal,
};
use pplx_space_feature_flags::FeatureFlagClient;
use pplx_space_net_common::{
    NetworkPolicyV1,
    proto::{NetworkPolicyApplyDecision, NetworkPolicyV1 as NetworkPolicyProto},
};
use pplx_space_sbx_mgr_client::proto as sbx_proto;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::file_reader::TemplateReadMode;
use crate::proto::template_manager_server::TemplateManager;
use crate::proto::{
    CreatePoolRequest, CreatePoolResponse, DeletePoolRequest, DeletePoolResponse,
    DeleteTemplateRequest, DeleteTemplateResponse, ExportTemplateRequest, ListTemplatesRequest,
    ListTemplatesResponse, PoolStatusRequest, PoolStatusResponse, RegisterTemplateRequest,
    RegisterTemplateResponse, UpdateTemplateSettingsRequest, UpdateTemplateSettingsResponse,
    UpdateTemplateSnapshotPolicyRequest, UpdateTemplateSnapshotPolicyResponse,
};
use uuid::Uuid;

const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;
const SPACE_NODEAPI_ACCOUNT: &str = user_names::SPACE_NODEAPI;
const SPACE_SBXMGR_ACCOUNT: &str = user_names::SPACE_SBXMGR;
const SPACE_K8S_NODE_CONTROLLER_ACCOUNT: &str = user_names::SPACE_K8S_NODE_CONTROLLER;
const SPACE_NODECTL_ACCOUNT: &str = user_names::SPACE_NODECTL;
const BASE_SNAPSHOT_BUILD_READY_TIMEOUT_SECS: u32 = 120;
const BASE_SNAPSHOT_BUILD_TIMEOUT_GRACE_SECS: u64 = 180;
const BASE_SNAPSHOT_BUILD_CLEANUP_TIMEOUT_SECS: u64 = 30;
const IMPORTING_MARKER: &str = ".importing";
const REPUBLISHING_MARKER: &str = ".republishing.";
const REPLACED_MARKER: &str = ".replaced.";
/// Prefix of build-manager's per-job staging directories under a template's
/// name dir (`build.importing.{job_id}`). Build-manager owns their lifetime;
/// template-manager consumes one on a `RegisterTemplate` with `staging_dir`
/// set and reaps stale leftovers at startup.
const BUILD_IMPORTING_PREFIX: &str = "build.importing.";

#[derive(Clone, Debug)]
struct TemplateArtifactStore {
    root: PathBuf,
}

#[derive(Debug, thiserror::Error)]
enum TemplateArtifactDeleteError {
    #[error("remove template build snapshots {path}: {source}")]
    BuildSnapshots {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("remove template build dir {path}: {source}")]
    BuildDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("remove template dir {path}: {source}")]
    TemplateDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl TemplateArtifactStore {
    fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn template_exists(&self, name: &str) -> bool {
        template_artifacts_dir(&self.root, name).exists()
    }

    fn build_artifacts_missing(&self, name: &str, content_hash: &str) -> bool {
        let build_dir = build_artifacts_dir(&self.root, name, content_hash);
        !build_dir.exists() || !build_dir.join(TEMPLATE_ROOTFS_FILE).exists()
    }

    fn snapshot_artifacts_missing(
        &self,
        name: &str,
        content_hash: &str,
        vcpu: u32,
        memory_mib: u32,
    ) -> bool {
        let snapshot_dir = snapshot_artifacts_dir(&self.root, name, content_hash, vcpu, memory_mib);
        !snapshot_dir.join("snapshot.base").exists() || !snapshot_dir.join("vmstate.base").exists()
    }

    fn remove_build_artifacts(
        &self,
        name: &str,
        content_hash: &str,
    ) -> Result<(), TemplateArtifactDeleteError> {
        self.remove_snapshot_artifacts(name, content_hash)?;

        let build_dir = build_artifacts_dir(&self.root, name, content_hash);
        remove_dir_all_if_exists(&build_dir).map_err(|source| {
            TemplateArtifactDeleteError::BuildDir {
                path: build_dir,
                source,
            }
        })?;

        Ok(())
    }

    /// Remove only the per-`(vcpu, memory_mib)` snapshot dirs for a
    /// build — does not touch the build's digest dir (subvol on
    /// btrfs, plain dir on legacy). Used by the eviction loop's
    /// btrfs-aware delete path which removes the build subvol via
    /// nodemgr and the snapshots locally.
    fn remove_snapshot_artifacts(
        &self,
        name: &str,
        content_hash: &str,
    ) -> Result<(), TemplateArtifactDeleteError> {
        let snapshots_dir = build_snapshots_artifacts_dir(&self.root, name, content_hash);
        remove_dir_all_if_exists(&snapshots_dir).map_err(|source| {
            TemplateArtifactDeleteError::BuildSnapshots {
                path: snapshots_dir,
                source,
            }
        })
    }

    fn remove_template_artifacts(&self, name: &str) -> Result<(), TemplateArtifactDeleteError> {
        let template_dir = template_artifacts_dir(&self.root, name);
        remove_dir_all_if_exists(&template_dir).map_err(|source| {
            TemplateArtifactDeleteError::TemplateDir {
                path: template_dir,
                source,
            }
        })
    }
}

impl Default for TemplateArtifactStore {
    fn default() -> Self {
        Self::new(TEMPLATE_DIR)
    }
}

fn template_artifacts_dir(root: &Path, name: &str) -> PathBuf {
    root.join(name)
}

fn build_artifacts_dir(root: &Path, name: &str, content_hash: &str) -> PathBuf {
    template_artifacts_dir(root, name).join(content_hash)
}

fn build_snapshots_artifacts_dir(root: &Path, name: &str, content_hash: &str) -> PathBuf {
    template_artifacts_dir(root, name)
        .join("snapshots")
        .join(content_hash)
}

fn snapshot_artifacts_dir(
    root: &Path,
    name: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u32,
) -> PathBuf {
    build_snapshots_artifacts_dir(root, name, content_hash)
        .join(format!("{vcpu}vcpu-{memory_mib}mib"))
}

fn remove_dir_all_if_exists(path: &Path) -> Result<(), io::Error> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn lock_status<'a, T>(mutex: &'a Mutex<T>, context: &str) -> Result<MutexGuard<'a, T>, Status> {
    mutex
        .lock()
        .map_err(|_| Status::internal(format!("{context} mutex poisoned")))
}

fn is_import_staging_dir(name: &str) -> bool {
    name.split_once(':')
        .is_some_and(|(algorithm, _)| algorithm.parse::<HashAlg>().is_ok())
        && name.contains(IMPORTING_MARKER)
}

fn is_build_staging_dir(name: &str) -> bool {
    name.strip_prefix(BUILD_IMPORTING_PREFIX)
        .is_some_and(|suffix| !suffix.is_empty())
}

fn is_republish_staging_dir(name: &str) -> bool {
    transient_digest_base(name, REPUBLISHING_MARKER).is_some()
}

fn replaced_digest_base(name: &str) -> Option<&str> {
    transient_digest_base(name, REPLACED_MARKER)
}

fn transient_digest_base<'a>(name: &'a str, marker: &str) -> Option<&'a str> {
    let (base, suffix) = name.split_once(marker)?;
    base.parse::<Digest>().ok()?;
    if base.is_empty() || suffix.is_empty() {
        return None;
    }
    Some(base)
}

struct ImportStagingGuard {
    path: PathBuf,
    active: bool,
}

impl ImportStagingGuard {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for ImportStagingGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Err(error) = remove_staging_path(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove template import staging dir"
            );
        }
    }
}

/// Remove a template import staging path. The staging dir may be a
/// btrfs subvolume — the digest dir lives as a subvolume so it can act
/// as a `btrfs send -p` parent for the suspend codec — or a plain
/// directory (legacy / non-btrfs hosts).
///
/// `std::fs::remove_dir_all` recurses contents fine but cannot
/// `rmdir` a subvolume root on default-mounted btrfs, so a Drop on a
/// subvolume-form staging dir used to leak an empty undeletable
/// subvolume per failed import — with the staging name carrying a
/// fresh `Uuid::now_v7()` suffix, those leaks accumulate one-per-
/// failed-import for the life of the node. Subvolume-aware deletion
/// makes Drop robust against the import pipeline failing at any step
/// between subvolume creation and the atomic rename.
fn remove_staging_path(path: &Path) -> std::io::Result<()> {
    if pplx_space_common_core::host::btrfs::is_subvolume(path) {
        // Clear RO first if some future import path stamps it pre-
        // rename — today nothing does, but `btrfs subvolume delete`
        // rejects RO targets and we want this helper to stay correct
        // if that ever changes.
        let _ = pplx_space_common_core::host::btrfs::set_readonly(path, false);
        match pplx_space_common_core::host::btrfs::delete_subvolume_if_present(path) {
            Ok(()) => return Ok(()),
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    %err,
                    "btrfs subvolume delete failed on staging dir; falling back to remove_dir_all",
                );
                // Fall through to remove_dir_all — useful for the
                // unusual case where the dir reports as a subvolume
                // but the delete still rejects (kernel bug, mount-
                // option mismatch, …). On the common path the
                // subvolume delete already succeeded above.
            }
        }
    }
    std::fs::remove_dir_all(path)
}

const ALLOW_TEMPLATE_OBSERVER: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_K8S_NODE_CONTROLLER_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_NODECTL_ACCOUNT),
];
const ALLOW_TEMPLATE_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
];
const ALLOW_TEMPLATE_NETWORK_POLICY_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_NODECTL_ACCOUNT),
];
const ALLOW_TEMPLATE_DESIRED_STATE_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_NODECTL_ACCOUNT),
];
const ALLOW_TEMPLATE_POOL_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_NODECTL_ACCOUNT),
];
const ALLOW_TEMPLATE_SNAPSHOT_POLICY_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(SPACE_NODEAPI_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_NODECTL_ACCOUNT),
];

#[derive(Clone)]
pub struct TemplateManagerService {
    db: Arc<Mutex<rusqlite::Connection>>,
    artifact_store: TemplateArtifactStore,
    production_hash_algorithm: HashAlg,
    feature_flags: FeatureFlagClient,
    pool_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    snapshot_reconcile_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    snapshot_retry_backoff: Arc<Mutex<snapshots::RetryBackoff>>,
    normalized_template_dirs: Arc<Mutex<HashSet<String>>>,
    /// Prevents concurrent imports of the same (name, content_hash).
    importing_templates: Arc<Mutex<std::collections::HashSet<String>>>,
    peer_authorizer: UdsPeerAuthorizer,
    /// Auto-pulls missing builds via `ClusterApi.FindTemplateRelease` plus an
    /// S3 download on `GetTemplate` cache miss; coalesces concurrent pulls of
    /// the same `(name, content_hash)`.
    volume_puller: crate::volume::VolumePuller,
    /// Shared durable worker for tmplmgr's background jobs. Dispatches
    /// by `job.kind`; today the only kind is `template_stamp` (retry
    /// the privileged `received_uuid` finalize when the inline call
    /// failed transiently). New job kinds add a `match` arm in
    /// [`crate::background_jobs::TemplateBackgroundExecutor`] — no
    /// new worker.
    background_worker: pplx_space_common_jobs::BackgroundWorker<
        crate::background_jobs::TemplateBackgroundExecutor,
    >,
    /// Connect budget this service spends reaching sandbox-manager.
    ///
    /// Every production path uses [`pplx_space_sbx_mgr_client::DEFAULT_CONNECT_RETRY_POLICY`],
    /// the shared default. It is a field rather than a constant so a test that
    /// deliberately runs without a sandbox-manager socket can decline the wait
    /// instead of buying the full default budget on every enrichment call.
    sbx_connect_retry_policy: pplx_space_sbx_mgr_client::GrpcRetryPolicy,
    /// Shared by the GC loop, startup recovery and `DeleteIdleTemplate`.
    eviction_gate: crate::eviction::EvictionGate,
}

// ── Path helpers ──────────────────────────────────────────────────────────────

/// Path to a build's digest directory: `{TEMPLATE_DIR}/{name}/{content_hash}/`
fn build_dir(name: &str, content_hash: &str) -> String {
    format!("{TEMPLATE_DIR}/{name}/{content_hash}")
}

/// Path to the snapshots directory for a build: `{TEMPLATE_DIR}/{name}/snapshots/{content_hash}/{vcpu}vcpu-{memory_mib}mib/`
fn snapshot_dir(name: &str, content_hash: &str, vcpu: u32, memory_mib: u32) -> String {
    pplx_space_common_core::host::linux::paths::template_snapshot_dir(
        name,
        content_hash,
        vcpu,
        memory_mib,
    )
}

#[derive(Clone, Copy, Debug, Default)]
struct TemplateRuntimeDefaults {
    vcpu: Option<u32>,
    memory_mib: Option<u32>,
    disk_size_mb: Option<u32>,
}

fn template_runtime_defaults(name: &str, content_hash: &str) -> TemplateRuntimeDefaults {
    let metadata_path = Path::new(&build_dir(name, content_hash)).join("metadata.json");
    let Ok(metadata_json) = std::fs::read_to_string(metadata_path) else {
        return TemplateRuntimeDefaults::default();
    };
    let Ok(metadata) =
        serde_json::from_str::<pplx_space_common_core::types::TemplateMetadata>(&metadata_json)
    else {
        return TemplateRuntimeDefaults::default();
    };
    let (vcpu, memory_mib, disk_size_mb) = metadata.runtime_defaults();
    TemplateRuntimeDefaults {
        vcpu,
        memory_mib,
        disk_size_mb,
    }
}

/// Parse a snapshot config dir name like "2vcpu-1024mib" into (vcpu, memory_mib).
fn parse_snapshot_config_dir(name: &str) -> Option<(u32, u32)> {
    let (vcpu_part, memory_part) = name.split_once('-')?;
    let vcpu = vcpu_part.strip_suffix("vcpu")?.parse().ok()?;
    let memory_mib = memory_part.strip_suffix("mib")?.parse().ok()?;
    Some((vcpu, memory_mib))
}

fn set_template_cache_permissions(path: &Path, mode: u32) -> Result<(), Status> {
    set_template_cache_permissions_io(path, mode).map_err(|e| {
        Status::internal(format!(
            "set template cache permissions on {}: {e}",
            path.display()
        ))
    })
}

fn normalize_template_cache_tree(path: &Path) -> Result<(), Status> {
    normalize_template_cache_tree_io(path).map_err(|e| {
        Status::internal(format!(
            "normalize template cache tree {}: {e}",
            path.display()
        ))
    })
}

fn copy_template_file(src: &Path, dst: &Path) -> Result<(), Status> {
    #[cfg(target_os = "linux")]
    {
        let copy = || -> std::io::Result<()> {
            let metadata = std::fs::metadata(src)?;
            let times = std::fs::FileTimes::new()
                .set_accessed(metadata.accessed()?)
                .set_modified(metadata.modified()?);
            pplx_space_common_io::reflink::copy_file(src, dst)?;
            std::fs::File::open(dst)?.set_times(times)
        };
        copy().map_err(|error| {
            Status::internal(format!(
                "copy template {} -> {}: {error}",
                src.display(),
                dst.display()
            ))
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        match std::process::Command::new("cp")
            .arg("--reflink=auto")
            .arg("--preserve=timestamps")
            .arg(src)
            .arg(dst)
            .status()
        {
            Ok(status) if status.success() => Ok(()),
            Ok(status) => std::fs::copy(src, dst).map(|_| ()).map_err(|error| {
                Status::internal(format!(
                    "copy {} -> {} after cp exited {status}: {error}",
                    src.display(),
                    dst.display()
                ))
            }),
            Err(command_error) => std::fs::copy(src, dst).map(|_| ()).map_err(|error| {
                Status::internal(format!(
                    "copy {} -> {} after cp failed ({command_error}): {error}",
                    src.display(),
                    dst.display()
                ))
            }),
        }
    }
}

fn replace_template_build_dir(staging_path: &Path, digest_path: &Path) -> Result<PathBuf, Status> {
    #[cfg(target_os = "linux")]
    {
        exchange_template_build_dirs(staging_path, digest_path)?;
        Ok(staging_path.to_path_buf())
    }

    #[cfg(not(target_os = "linux"))]
    {
        replace_template_build_dir_non_atomic(staging_path, digest_path)
    }
}

#[cfg(target_os = "linux")]
fn exchange_template_build_dirs(staging_path: &Path, digest_path: &Path) -> Result<(), Status> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging_path,
        rustix::fs::CWD,
        digest_path,
        rustix::fs::RenameFlags::EXCHANGE,
    )
    .map_err(|error| {
        Status::internal(format!(
            "exchange template build {} <-> {}: {error}",
            staging_path.display(),
            digest_path.display()
        ))
    })
}

#[cfg(not(target_os = "linux"))]
fn replace_template_build_dir_non_atomic(
    staging_path: &Path,
    digest_path: &Path,
) -> Result<PathBuf, Status> {
    let digest_name = digest_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Status::internal(format!(
                "template digest path has no valid file name: {}",
                digest_path.display()
            ))
        })?;
    let backup_path =
        digest_path.with_file_name(format!("{digest_name}{REPLACED_MARKER}{}", Uuid::now_v7()));

    std::fs::rename(digest_path, &backup_path).map_err(|error| {
        Status::internal(format!(
            "move current template build {} -> {}: {error}",
            digest_path.display(),
            backup_path.display()
        ))
    })?;

    if let Err(error) = std::fs::rename(staging_path, digest_path) {
        if let Err(restore_error) = std::fs::rename(&backup_path, digest_path) {
            tracing::error!(
                backup = %backup_path.display(),
                digest = %digest_path.display(),
                %restore_error,
                "failed to restore previous template build after replacement failure"
            );
        }
        return Err(Status::internal(format!(
            "replace template build {} -> {}: {error}",
            staging_path.display(),
            digest_path.display()
        )));
    }

    Ok(backup_path)
}

fn write_or_copy_template_file(
    src: &Path,
    dst: &Path,
    replacement: Option<&[u8]>,
    required: bool,
) -> Result<(), Status> {
    if let Some(bytes) = replacement {
        std::fs::write(dst, bytes)
            .map_err(|error| Status::internal(format!("write {}: {error}", dst.display())))?;
        return Ok(());
    }
    if src.exists() {
        copy_template_file(src, dst)?;
        return Ok(());
    }
    if required {
        return Err(Status::internal(format!(
            "missing required template file {}",
            src.display()
        )));
    }
    Ok(())
}

fn template_file_replacement_is_unchanged(
    path: &Path,
    replacement: Option<&[u8]>,
) -> Result<bool, Status> {
    let Some(replacement) = replacement else {
        return Ok(true);
    };
    match std::fs::read(path) {
        Ok(existing) => Ok(existing == replacement),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Status::internal(format!(
            "read existing template file {}: {error}",
            path.display()
        ))),
    }
}

fn allocated_file_size(path: impl AsRef<Path>) -> Option<u64> {
    std::fs::metadata(path)
        .ok()
        .map(|metadata| metadata.blocks().saturating_mul(512))
}

fn template_cache_size_bytes(name: &str, content_hash: &str) -> Option<u64> {
    allocated_file_size(Path::new(&build_dir(name, content_hash)).join("rootfs.ext4"))
}

fn snapshot_cache_size_bytes(
    name: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u32,
) -> Option<u64> {
    let dir = PathBuf::from(snapshot_dir(name, content_hash, vcpu, memory_mib));
    let snapshot = allocated_file_size(dir.join("snapshot.base"))?;
    let vmstate = allocated_file_size(dir.join("vmstate.base")).unwrap_or(0);
    Some(snapshot.saturating_add(vmstate))
}

fn rootfs_artifact_size_from_manifest(
    manifest: &pplx_space_common_core::types::TemplateManifest,
) -> Option<u64> {
    // Pre-typing this read returned `None` when `size` was absent from
    // the JSON and `Some(0)` when it was an explicit zero. Callers use
    // `None` as the "fall back to fs::metadata().len()" signal.
    // `TemplateManifestFile::size` is `Option<u64>` to preserve that.
    manifest
        .files
        .get(TEMPLATE_ROOTFS_FILE)
        .and_then(|e| e.size)
}

fn write_template_manifest_atomic(
    manifest_path: &Path,
    contents: impl AsRef<[u8]>,
) -> Result<(), Status> {
    let parent = manifest_path.parent().ok_or_else(|| {
        Status::internal(format!(
            "manifest path has no parent: {}",
            manifest_path.display()
        ))
    })?;
    let file_name = manifest_path.file_name().ok_or_else(|| {
        Status::internal(format!(
            "manifest path has no file name: {}",
            manifest_path.display()
        ))
    })?;
    let tmp_path = parent.join(format!(
        "{}.tmp.{}",
        file_name.to_string_lossy(),
        Uuid::now_v7()
    ));

    if let Err(error) = std::fs::write(&tmp_path, contents) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(Status::internal(format!(
            "write manifest temp {}: {error}",
            tmp_path.display()
        )));
    }

    if let Err(error) = std::fs::rename(&tmp_path, manifest_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(Status::internal(format!(
            "rename manifest temp {} -> {}: {error}",
            tmp_path.display(),
            manifest_path.display()
        )));
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NormalizeTemplateManifestError {
    #[error("invalid template metadata execution schema: {0}")]
    Metadata(#[from] pplx_space_common_core::types::TemplateMetadataSchemaError),
    #[error("invalid template manifest execution schema: {0}")]
    Manifest(#[from] pplx_space_common_core::types::TemplateManifestSchemaError),
    #[error("template metadata and manifest execution metadata disagree")]
    ExecutionMismatch,
}

/// Register keeps the digest-addressed manifest. Metadata refresh cannot change
/// its environment binding, including an upgrade from an unbound release.
fn validate_retained_environment_binding(
    manifest: &pplx_space_common_core::types::TemplateManifest,
    metadata: &pplx_space_common_core::types::TemplateMetadata,
) -> Result<(), Status> {
    let retained = manifest
        .execution_metadata()
        .map_err(|error| Status::data_loss(error.to_string()))?
        .and_then(|execution| execution.environment());
    let incoming = metadata
        .execution_metadata()
        .map_err(|error| Status::invalid_argument(error.to_string()))?
        .and_then(|execution| execution.environment());
    if retained != incoming {
        return Err(Status::already_exists(
            "rootfs digest already has a different environment binding; retain the release unchanged",
        ));
    }
    Ok(())
}

pub(crate) fn normalize_template_manifest(
    mut manifest: pplx_space_common_core::types::TemplateManifest,
    name: &str,
    content_hash: &str,
    metadata: &pplx_space_common_core::types::TemplateMetadata,
    rootfs_size: u64,
    compressed_rootfs: Option<(&str, u64)>,
) -> Result<pplx_space_common_core::types::TemplateManifest, NormalizeTemplateManifestError> {
    use pplx_space_common_core::types::{TemplateManifestCompression, TemplateManifestFile};

    manifest.name = name.to_string();
    manifest.content_hash = content_hash.to_string();
    let metadata_execution = metadata.execution_metadata()?;
    let manifest_execution = manifest.execution_metadata()?;
    if let (Some(metadata_execution), Some(manifest_execution)) =
        (metadata_execution, manifest_execution)
        && metadata_execution != manifest_execution
    {
        return Err(NormalizeTemplateManifestError::ExecutionMismatch);
    }
    if let Some(execution) = metadata_execution {
        if manifest.runtime.is_empty() {
            manifest.runtime = execution.runtime().kind.alias().to_string();
        }
        if manifest.execution.is_none() {
            manifest.set_execution_metadata(execution.clone());
        }
    } else if manifest.runtime.is_empty() {
        manifest.runtime = RUNTIME_FIRECRACKER_ALIAS.to_string();
    }
    if manifest.arch.is_empty() {
        manifest.arch = template_arch(metadata, &manifest);
    }
    // `TemplateManifest`'s derived `Default` gives `config: Value::Null`
    // (serde_json::Value's own Default is Null), and an explicit
    // `"config": null` on the wire deserializes the same way. The
    // pre-typing helper patched both cases to `{}` so the on-disk
    // manifest converged across writers. Preserve that.
    if matches!(manifest.config, serde_json::Value::Null) {
        manifest.config = serde_json::Value::Object(serde_json::Map::new());
    }
    manifest.bootable_artifact = TEMPLATE_ROOTFS_FILE.to_string();

    manifest.files.insert(
        TEMPLATE_ROOTFS_FILE.to_string(),
        TemplateManifestFile {
            digest: content_hash.to_string(),
            size: Some(rootfs_size),
            compression: None,
        },
    );
    if let Some((compressed_rootfs_hash, compressed_rootfs_size)) = compressed_rootfs {
        manifest.files.insert(
            TEMPLATE_ROOTFS_ZSTD_FILE.to_string(),
            TemplateManifestFile {
                digest: compressed_rootfs_hash.to_string(),
                size: Some(compressed_rootfs_size),
                compression: Some(TemplateManifestCompression {
                    codec: TEMPLATE_ROOTFS_ZSTD_CODEC.to_string(),
                    level: TEMPLATE_ROOTFS_ZSTD_LEVEL,
                }),
            },
        );
    }

    Ok(manifest)
}

fn manifest_has_compressed_rootfs_entry(
    manifest: &pplx_space_common_core::types::TemplateManifest,
) -> bool {
    // Pre-typing this required `size` to be a present u64 (any value,
    // including 0). Match exactly: `entry.size.is_some()` not `> 0`.
    manifest
        .files
        .get(TEMPLATE_ROOTFS_ZSTD_FILE)
        .is_some_and(|entry| {
            !entry.digest.is_empty()
                && entry.size.is_some()
                && entry.compression.as_ref().is_some_and(|c| {
                    c.codec == TEMPLATE_ROOTFS_ZSTD_CODEC && c.level == TEMPLATE_ROOTFS_ZSTD_LEVEL
                })
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootfsHashVerification {
    Verify,
    AlreadyVerified,
}

static ROOTFS_ZSTD_GENERATION_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static ROOTFS_ZSTD_GENERATION_INFLIGHT: OnceLock<Arc<Mutex<HashSet<PathBuf>>>> = OnceLock::new();

fn rootfs_zstd_generation_semaphore() -> Arc<Semaphore> {
    Arc::clone(ROOTFS_ZSTD_GENERATION_SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(1))))
}

fn rootfs_zstd_generation_inflight() -> Arc<Mutex<HashSet<PathBuf>>> {
    Arc::clone(ROOTFS_ZSTD_GENERATION_INFLIGHT.get_or_init(|| Arc::new(Mutex::new(HashSet::new()))))
}

struct RootfsZstdGenerationInflightGuard {
    digest_path: PathBuf,
}

impl Drop for RootfsZstdGenerationInflightGuard {
    fn drop(&mut self) {
        match rootfs_zstd_generation_inflight().lock() {
            Ok(mut inflight) => {
                inflight.remove(&self.digest_path);
            }
            Err(error) => {
                tracing::warn!(
                    digest_dir = %self.digest_path.display(),
                    %error,
                    "compressed rootfs generation inflight set mutex poisoned during cleanup"
                );
            }
        }
    }
}

async fn generate_template_compressed_rootfs(
    template_id: &str,
    content_hash: &str,
    digest_dir: &str,
    trigger: &'static str,
    read_mode: TemplateReadMode,
) -> Result<crate::rootfs_zstd::RootfsZstdStats, Status> {
    let rootfs_path = crate::rootfs_zstd::rootfs_path(Path::new(digest_dir));
    let compressed_path = crate::rootfs_zstd::compressed_rootfs_path(Path::new(digest_dir));
    let _permit = rootfs_zstd_generation_semaphore()
        .acquire_owned()
        .await
        .map_err(|_| Status::internal("rootfs zstd generation semaphore closed"))?;

    if compressed_path.exists() {
        let rootfs_size = std::fs::metadata(&rootfs_path)
            .map(|metadata| metadata.len())
            .map_err(|error| {
                Status::internal(format!("stat rootfs {}: {error}", rootfs_path.display()))
            })?;
        let compressed_size = std::fs::metadata(&compressed_path)
            .map(|metadata| metadata.len())
            .map_err(|error| {
                Status::internal(format!(
                    "stat compressed rootfs {}: {error}",
                    compressed_path.display()
                ))
            })?;
        let compressed_hash = compute_template_file_hash(
            compressed_path,
            HashAlg::Sha256,
            "hash compressed rootfs",
            read_mode,
        )
        .await?;
        return Ok(crate::rootfs_zstd::RootfsZstdStats {
            rootfs_size,
            compressed_size,
            compressed_hash,
        });
    }

    let stats = tokio::task::spawn_blocking(move || {
        crate::rootfs_zstd::compress_rootfs(&rootfs_path, &compressed_path, read_mode)
    })
    .await
    .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
    .map_err(|error| {
        Status::internal(format!(
            "compress template rootfs for {template_id}@{content_hash} during {trigger}: {error}"
        ))
    })?;
    tracing::info!(
        template = %template_id,
        content_hash,
        rootfs_size = stats.rootfs_size,
        compressed_size = stats.compressed_size,
        "generated compressed template rootfs"
    );
    Ok(stats)
}

fn optional_positive_or_default(
    field: &'static str,
    requested: Option<u32>,
    default: u32,
) -> Result<u32, Status> {
    match requested {
        Some(0) => Err(Status::invalid_argument(format!(
            "{field} must be greater than 0 when set"
        ))),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

fn optional_snapshot_dimensions(
    vcpu: Option<u32>,
    memory_mib: Option<u32>,
) -> Result<Option<(u32, u32)>, Status> {
    match (vcpu, memory_mib) {
        (None, None) => Ok(None),
        (Some(0), _) => Err(Status::invalid_argument(
            "snapshot_vcpu must be greater than 0 when set",
        )),
        (_, Some(0)) => Err(Status::invalid_argument(
            "snapshot_memory_mib must be greater than 0 when set",
        )),
        (Some(vcpu), Some(memory_mib)) => Ok(Some((vcpu, memory_mib))),
        _ => Err(Status::invalid_argument(
            "snapshot_vcpu and snapshot_memory_mib must be set together",
        )),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ResolvedSandboxMemory {
    pub(crate) requested_memory_mib: u32,
    pub(crate) effective_memory_mib: u32,
    pub(crate) cgroup_memory_mib: u64,
    pub(crate) guest_memory_max_mib: u32,
    pub(crate) cgroup_memory_max_mib: u64,
    pub(crate) memory_headroom_mib: u64,
}

pub(crate) async fn resolve_sandbox_memory(
    requested_memory_mib: u32,
    sbx_connect_retry_policy: pplx_space_sbx_mgr_client::GrpcRetryPolicy,
) -> Result<ResolvedSandboxMemory, Status> {
    if requested_memory_mib == 0 {
        return Err(Status::invalid_argument(
            "requested_memory_mib must be greater than 0",
        ));
    }
    let mut client = pplx_space_sbx_mgr_client::connect_with_retry_policy(sbx_connect_retry_policy)
        .await
        .map_err(|error| Status::unavailable(format!("connect sandbox-manager: {error}")))?;
    let response = client
        .resolve_sandbox_memory(sbx_proto::ResolveSandboxMemoryRequest {
            requested_memory_mib,
        })
        .await
        .map_err(|error| {
            Status::new(
                error.code(),
                format!("resolve sandbox memory policy: {error}"),
            )
        })?
        .into_inner();
    Ok(ResolvedSandboxMemory {
        requested_memory_mib: response.requested_memory_mib,
        effective_memory_mib: response.effective_memory_mib,
        cgroup_memory_mib: response.cgroup_memory_mib,
        guest_memory_max_mib: response.guest_memory_max_mib,
        cgroup_memory_max_mib: response.cgroup_memory_max_mib,
        memory_headroom_mib: response.memory_headroom_mib,
    })
}

fn proto_policy_to_common(
    policy: &sbx_proto::AutoLifecyclePolicy,
) -> Result<AutoLifecyclePolicy, Status> {
    let mut policy = AutoLifecyclePolicy::try_from(policy).map_err(Status::from)?;
    policy.normalize_in_place();
    policy.validate_basic().map_err(Status::invalid_argument)?;
    Ok(policy)
}

fn common_policy_to_proto(policy: &AutoLifecyclePolicy) -> sbx_proto::AutoLifecyclePolicy {
    sbx_proto::AutoLifecyclePolicy::from(policy)
}

fn proto_network_policy_to_common(policy: &NetworkPolicyProto) -> Result<NetworkPolicyV1, Status> {
    NetworkPolicyV1::try_from(policy).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(crate) fn common_network_policy_to_proto(policy: &NetworkPolicyV1) -> NetworkPolicyProto {
    NetworkPolicyProto::from(policy)
}

fn update_template_settings_principals(
    request: &UpdateTemplateSettingsRequest,
) -> &'static [UdsPeerPrincipal] {
    if request.default_auto_lifecycle.is_some() {
        ALLOW_TEMPLATE_DESIRED_STATE_OPERATOR
    } else {
        ALLOW_TEMPLATE_NETWORK_POLICY_OPERATOR
    }
}

fn template_peer_authorizer() -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new_with_default_local_roles(
        "space-template-manager",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_NODEAPI),
            UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
            UdsPeerAccount::with_linux_user(users::SPACE_K8S_NODE_CONTROLLER),
            UdsPeerAccount::with_linux_user(users::SPACE_NODECTL),
        ],
    )
}

fn internal_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, None));
    request
}

fn dependent_sandbox_ids_for_template_delete_target(
    sandboxes: &[sbx_proto::SandboxListElement],
    target: TemplateBuildDeleteTarget<'_>,
    scope: DependentSandboxScope,
) -> Vec<(String, String, Vec<String>)> {
    match target {
        TemplateBuildDeleteTarget::Builds(builds) => builds
            .iter()
            .filter_map(|build| {
                let sandbox_ids = sandboxes
                    .iter()
                    .filter(|sandbox| {
                        sandbox_matches_template_build(sandbox, &build.name, &build.content_hash)
                    })
                    .map(|sandbox| sandbox.sandbox_id.clone())
                    .collect::<Vec<_>>();
                (!sandbox_ids.is_empty())
                    .then(|| (build.name.clone(), build.content_hash.clone(), sandbox_ids))
            })
            .collect(),
        TemplateBuildDeleteTarget::Template { name } => {
            let sandbox_ids = sandboxes
                .iter()
                .filter(|sandbox| {
                    if scope == DependentSandboxScope::ExcludePoolMembers
                        && sandbox.pool_template.as_deref() == Some(name)
                    {
                        return false;
                    }
                    sandbox.template.as_deref() == Some(name)
                })
                .map(|sandbox| sandbox.sandbox_id.clone())
                .collect::<Vec<_>>();
            if sandbox_ids.is_empty() {
                Vec::new()
            } else {
                vec![(name.to_string(), "<any>".to_string(), sandbox_ids)]
            }
        }
    }
}

fn sandbox_matches_template_build(
    sandbox: &sbx_proto::SandboxListElement,
    template: &str,
    content_hash: &str,
) -> bool {
    sandbox.template.as_deref() == Some(template)
        && sandbox.image_hash.as_deref() == Some(content_hash)
}

#[derive(Clone, Copy)]
enum TemplateBuildDeleteTarget<'a> {
    Builds(&'a [crate::db::BuildRow]),
    Template { name: &'a str },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DependentSandboxScope {
    All,
    ExcludePoolMembers,
}

#[derive(Clone, Copy)]
enum TemplateBuildDeleteOperation {
    Build,
    Template,
}

fn sandbox_id_preview(sandbox_ids: &[String]) -> String {
    let preview = sandbox_ids
        .iter()
        .take(5)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if sandbox_ids.len() > 5 {
        format!(" and {} more", sandbox_ids.len() - 5)
    } else {
        String::new()
    };
    format!("{preview}{suffix}")
}

fn dependent_template_build_delete_error(
    operation: TemplateBuildDeleteOperation,
    dependencies: &[(String, String, Vec<String>)],
) -> Status {
    let total_sandboxes = dependencies
        .iter()
        .map(|(_, _, sandbox_ids)| sandbox_ids.len())
        .sum::<usize>();
    let template = dependencies
        .first()
        .map(|(template, _, _)| template.as_str())
        .unwrap_or("<unknown>");
    let details = dependencies
        .iter()
        .take(3)
        .map(|(_, content_hash, sandbox_ids)| {
            format!(
                "{content_hash}: {} sandbox(es) ({})",
                sandbox_ids.len(),
                sandbox_id_preview(sandbox_ids)
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let suffix = if dependencies.len() > 3 {
        format!(" and {} more build(s)", dependencies.len() - 3)
    } else {
        String::new()
    };
    match operation {
        TemplateBuildDeleteOperation::Build => {
            let (template, content_hash, sandbox_ids) =
                dependencies.first().expect("dependencies is non-empty");
            let preview = sandbox_id_preview(sandbox_ids);
            Status::failed_precondition(format!(
                "cannot delete build {template}@{content_hash}: {} sandbox(es) were created from it ({preview})",
                sandbox_ids.len()
            ))
        }
        TemplateBuildDeleteOperation::Template => Status::failed_precondition(format!(
            "cannot delete template {template}: {total_sandboxes} sandbox(es) were created from {} build(s): {details}{suffix}",
            dependencies.len()
        )),
    }
}

/// Fetch the set of `(template, content_hash)` build keys referenced by a
/// live instance subvolume on this node — across **all** sandbox owners.
/// Used by the GC loop to filter the eviction candidate list before deleting
/// builds.
///
/// This calls `ListBuildReferencingSandboxes`, not the plain `List`, on
/// purpose: `List` is filtered to template-manager-owned sandboxes, so an
/// adopted pool sandbox (now user-owned, `pool_template` cleared) is invisible
/// to it even though its instance subvolume still references the parent build
/// for incremental suspend/restore. sbx-mgr applies the state scope
/// (`SandboxState::holds_parent_referencing_instance_subvolume`), so a row
/// whose instance subvolume is being torn down does not pin its parent.
///
/// A failed sbx-mgr call short-circuits the tick (`Err(_)`) so we never evict
/// on a partial reference view; the grace period is the correctness backstop.
async fn fetch_build_referencing_sandbox_keys() -> Result<HashSet<(String, String)>, String> {
    let mut client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|err| format!("sbx-mgr connect: {err}"))?;
    let response = client
        .list_build_referencing_sandboxes(sbx_proto::ListBuildReferencingSandboxesRequest {})
        .await
        .map_err(|err| format!("sbx-mgr list_build_referencing_sandboxes: {err}"))?
        .into_inner();
    let mut active = HashSet::new();
    for s in response.sandboxes {
        if let (Some(template), Some(hash)) = (s.template.clone(), s.image_hash.clone())
            && !template.is_empty()
            && !hash.is_empty()
        {
            active.insert((template, hash));
        }
    }
    Ok(active)
}

/// Fetch the `(template, content_hash)` releases warm pools currently
/// target, from the control plane. Local prefetch pins only cover the
/// hardcoded-tag and latest-N releases; a warm-pool selected release
/// can sit outside both, and evicting it converts the next pool refill
/// into a full re-pull. Same fail-closed contract as
/// [`fetch_build_referencing_sandbox_keys`]: any error short-circuits the tick.
async fn fetch_warm_pool_protected_keys() -> Result<HashSet<(String, String)>, String> {
    let mut client = pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(|err| format!("cluster-api connect: {err}"))?;
    let response = client
        .list_warm_pool_template_releases(
            pplx_space_node_api_client::proto::ListWarmPoolTemplateReleasesRequest {},
        )
        .await
        .map_err(|err| format!("cluster-api list warm pool releases: {err}"))?
        .into_inner();
    Ok(response
        .releases
        .into_iter()
        .filter(|r| !r.template_name.is_empty() && !r.hash.is_empty())
        .map(|r| (r.template_name, r.hash))
        .collect())
}

/// Disk delete routes through `btrfs_via_nodemgr` so a build
/// installed as a btrfs subvolume actually gets its
/// subvolume removed — `std::fs::remove_dir_all` recurses
/// contents but cannot `rmdir` a subvolume root, leaving an
/// empty undeletable subvolume per evicted build. The fallback
/// path mirrors `volume::cleanup_digest_dir_after_hash_mismatch`:
/// when the nodemgr RPC is unreachable we still wipe the
/// snapshot dir (plain dir) and the subvol contents
/// (`remove_dir_all` succeeds on file inodes inside the subvol
/// even when the root is undeletable). That at least disarms
/// the recovery path; the empty subvol root is a known leak
/// until nodemgr comes back.
///
/// The closure is invoked from `execute_one_async` between
/// the pre-check and finalize txns, with the DB lock
/// released — so we can `await` the nodemgr RPC directly
/// instead of going through `Handle::block_on` from inside a
/// `spawn_blocking` thread.
fn gc_delete_disk_fn(artifact_store: TemplateArtifactStore) -> crate::eviction::DeleteDiskFn {
    let store = artifact_store;
    Arc::new(move |name: &str, content_hash: &str| {
        let n = name.to_string();
        let h = content_hash.to_string();
        let store = store.clone();
        Box::pin(async move {
            let outcome: Result<(), String> =
                match crate::btrfs_via_nodemgr::delete_template_subvolume(&n, &h).await {
                    Ok(()) => {
                        // Snapshot dirs are plain dirs under
                        // `<root>/<name>/snapshots/<hash>/`,
                        // separate from the digest subvol nodemgr
                        // just deleted. Run only the snapshot
                        // half locally — the combined helper
                        // would re-try a now-gone build dir.
                        store
                            .remove_snapshot_artifacts(&n, &h)
                            .map_err(|err| format!("snapshot cleanup: {err}"))
                    }
                    Err(rpc_err) => {
                        tracing::warn!(
                            template = %n,
                            content_hash = %h,
                            err = %rpc_err,
                            "btrfs_via_nodemgr::delete_template_subvolume failed; falling back to subvolume-aware local cleanup"
                        );
                        // Subvolume-aware local fallback. Plain
                        // `remove_dir_all` EROFSes on the first
                        // unlink inside a read-only digest
                        // subvolume, surfacing Err to the caller
                        // and leaving the row stuck in
                        // `state='deleting'`. The shared
                        // `cleanup_digest_dir_sync` helper from
                        // volume.rs clears the RO flag first and
                        // handles the subvol-vs-plain-dir
                        // disambiguation the same way
                        // `cleanup_digest_dir_after_hash_mismatch`
                        // does on the pull-failure path.
                        fallback_delete_build_artifacts(&store, &n, &h)
                            .map_err(|err| err.to_string())
                    }
                };
            outcome.map_err(|err| {
                // Surface the disk error as a SQLite error
                // so it bubbles out of `execute_one_async`.
                // The wrapping `Code::SQLITE_IOERR` is a
                // synthetic carrier; the underlying error
                // is logged by `run_loop`.
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                    Some(err),
                )
            })
        })
    })
}

/// The stuck-`deleting` repair's disk probe: a build whose rootfs is gone
/// cannot be restored to `ready`.
fn gc_reaper_probe_fn(artifact_store: TemplateArtifactStore) -> crate::eviction::ReaperDiskProbeFn {
    Arc::new(move |name: &str, content_hash: &str| {
        let store = artifact_store.clone();
        let n = name.to_string();
        let h = content_hash.to_string();
        Box::pin(async move {
            if store.build_artifacts_missing(&n, &h) {
                crate::eviction::ReaperDiskState::Missing
            } else {
                crate::eviction::ReaperDiskState::Intact
            }
        })
    })
}

/// Total on-disk footprint for a build: every regular file in the
/// digest dir (rootfs.ext4, rootfs.ext4.zst, manifest.json,
/// metadata.json, plus any future entries) summed by allocated
/// blocks, plus the per-`(vcpu, memory_mib)` snapshot pair files
/// recorded in `template_snapshots`. `snapshot_dims` is provided by
/// the caller (read once from the DB inside `read_plan_db_snapshot`)
/// so this function does NOT need a DB connection — that's the
/// structural property the deadlock fix depends on.
///
/// Falls back to `fallback_artifact_size` (the value stored in
/// `template_builds.artifact_size_bytes`) when reading the digest
/// dir fails — typically a partial / pre-deletion state where the
/// planner still wants a non-zero size to charge.
fn build_disk_usage_with_snapshots(
    name: &str,
    content_hash: &str,
    fallback_artifact_size: u64,
    snapshot_dims: &[(u32, u32)],
) -> u64 {
    let digest_dir = build_dir(name, content_hash);
    let build_bytes =
        digest_dir_allocated_bytes(Path::new(&digest_dir)).unwrap_or(fallback_artifact_size);
    let snapshot_bytes: u64 = snapshot_dims
        .iter()
        .map(|(vcpu, memory_mib)| {
            snapshot_cache_size_bytes(name, content_hash, *vcpu, *memory_mib).unwrap_or(0)
        })
        .sum();
    build_bytes.saturating_add(snapshot_bytes)
}

/// Sum the allocated size of every regular file directly inside
/// `path` (non-recursive — the digest dir is flat after the
/// btrfs-subvolume install path). Returns `None` if the
/// dir can't be read; the caller falls back to the stored
/// `artifact_size_bytes`. Subdirs are skipped (none expected today,
/// and recursing would double-count the snapshot dirs which live
/// elsewhere).
/// Subvolume-aware fallback delete for the eviction path when the
/// privileged `BtrfsDeleteTemplateSubvolume` RPC against node-mgr
/// fails (transient connect blip, RPC deadline exceeded, etc.).
///
/// `TemplateArtifactStore::remove_build_artifacts` uses plain
/// `std::fs::remove_dir_all`, which walks the contents of a btrfs
/// subvolume root fine but cannot `rmdir` the root itself on
/// default-mounted btrfs (the kernel reserves rmdir for the subvolume
/// delete ioctl). Before this helper, the eviction fallback path
/// would return Err on the `rmdir` step and skip Phase 3, leaving the
/// row stuck in `state='deleting'` AND an empty subvol root on disk.
///
/// This helper mirrors `volume::cleanup_digest_dir_after_hash_mismatch`
/// — try the privileged delete via node-mgr; on failure use the
/// subvol-aware [`crate::volume::cleanup_digest_dir_sync`] helper.
///
/// The local helper fully removes the subvolume even without
/// `CAP_SYS_ADMIN` (the production posture): node-mgr chowns digest
/// subvolumes to the tmplmgr user at create time, clearing the RO flag
/// is owner-permitted, subvolume deletion needs only rmdir-equivalent
/// permissions on current kernels, and where that ioctl is refused,
/// `remove_dir_all` still completes because rmdir of an emptied
/// subvolume root behaves like a plain directory. A cleanup failure
/// past all of that (e.g. a write-protected parent dir) leaves at most
/// a wiped-out digest dir; the helper still returns Ok so Phase 3
/// finalizes the row, and the next `do_pull`'s `digest_dir_existed`
/// adoption path re-uses any leftover subvol root, writing fresh bytes
/// into it.
fn fallback_delete_build_artifacts(
    store: &TemplateArtifactStore,
    name: &str,
    content_hash: &str,
) -> Result<(), io::Error> {
    store
        .remove_snapshot_artifacts(name, content_hash)
        .map_err(|err| match err {
            TemplateArtifactDeleteError::BuildSnapshots { source, .. } => source,
            // remove_snapshot_artifacts only produces BuildSnapshots
            // errors; the other variants are unreachable here.
            other => io::Error::other(format!("unexpected delete error: {other}")),
        })?;
    let build_dir = build_artifacts_dir(&store.root, name, content_hash);
    match crate::volume::cleanup_digest_dir_sync(&build_dir) {
        Ok(()) => Ok(()),
        Err(err) => {
            // Soft success: the RO flag was already cleared and the
            // recursive file-removal ran, so what remains is at most
            // an empty btrfs subvolume root, which `do_pull`'s
            // `digest_dir_existed` adoption path re-uses on the next
            // access. The alternative — surfacing Err from here —
            // would skip Phase 3 finalize and leave the row in
            // `state='deleting'`, which is the bug this helper exists
            // to prevent. The stuck-deleting reaper in `gc_tick`
            // would still catch it on the next tick, but routing
            // through that path on every nodemgr blip would
            // double-count the eviction's recovery cost. The
            // start-of-tick reaper exists as the SECOND line of
            // defense (daemon crash mid-eviction, total local
            // fallback failure, etc.), not the primary recovery for
            // an expected production failure mode.
            tracing::warn!(
                template = name,
                content_hash,
                error = %err,
                dir = %build_dir.display(),
                "fallback cleanup: local subvolume removal incomplete; leaving any \
                 leftover subvol root for the next do_pull's digest_dir_existed path \
                 to adopt",
            );
            Ok(())
        }
    }
}

/// `JoinError` → `Status` for the blocking sections of the delete
/// paths, carrying the identifiers the surrounding logs use so a
/// panicked blocking task is attributable.
fn blocking_join_status(op: &'static str, target: &str, e: tokio::task::JoinError) -> Status {
    Status::internal(format!("{op} spawn_blocking for {target}: {e}"))
}

fn digest_dir_allocated_bytes(path: &Path) -> Option<u64> {
    let mut total: u64 = 0;
    let entries = std::fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if metadata.is_file() {
            total = total.saturating_add(metadata.blocks().saturating_mul(512));
        }
    }
    Some(total)
}

async fn ensure_no_dependent_sandboxes_for_template_builds(
    operation: TemplateBuildDeleteOperation,
    scope: DependentSandboxScope,
    target: TemplateBuildDeleteTarget<'_>,
) -> Result<(), Status> {
    if let TemplateBuildDeleteTarget::Builds([]) = target {
        return Ok(());
    }
    let check_target = match target {
        TemplateBuildDeleteTarget::Builds([build]) => {
            format!("build {}@{}", build.name, build.content_hash)
        }
        TemplateBuildDeleteTarget::Builds(builds) => format!("{} build(s)", builds.len()),
        TemplateBuildDeleteTarget::Template { name } => format!("template {name}"),
    };

    let mut sbx_client = pplx_space_sbx_mgr_client::connect()
        .await
        .map_err(|error| {
            Status::unavailable(format!(
                "cannot check sandbox dependencies before deleting {check_target}: {error}"
            ))
        })?;
    // Use `ListBuildReferencingSandboxes`, not `List`: `List` hides
    // user-owned (and adopted-pool) sandboxes behind its template-manager
    // owner filter, so a manual delete would otherwise dangle a live user
    // sandbox's parent build the same way on-node GC did. The state scope is
    // applied server-side.
    let response = sbx_client
        .list_build_referencing_sandboxes(sbx_proto::ListBuildReferencingSandboxesRequest {})
        .await
        .map_err(|error| {
            Status::new(
                error.code(),
                format!(
                    "cannot check sandbox dependencies before deleting {check_target}: {error}"
                ),
            )
        })?
        .into_inner();

    let dependencies =
        dependent_sandbox_ids_for_template_delete_target(&response.sandboxes, target, scope);
    if dependencies.is_empty() {
        Ok(())
    } else {
        Err(dependent_template_build_delete_error(
            operation,
            &dependencies,
        ))
    }
}

#[derive(Default)]
struct SnapshotReconcileOutcome {
    reconciled: bool,
    snapshot_built: bool,
    snapshot_deleted: bool,
    in_progress: bool,
    content_hash: Option<String>,
    error: Option<String>,
}

type TemplateExportStream = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<crate::proto::ExportTemplateChunk, Status>> + Send>,
>;

/// Detach a long-lived loop while retaining an attributable panic signal.
///
/// Tokio's default panic-hook record names the panic origin, which may be a
/// shared callee rather than the loop that awaited it. The outer task observes
/// the inner task's `JoinError` and attaches the loop identity without changing
/// the existing no-restart lifecycle.
fn spawn_observed_background_loop(
    loop_name: &'static str,
    future: impl std::future::Future<Output = ()> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match tokio::spawn(future).await {
            Err(error) if error.is_panic() => {
                tracing::error!(
                    loop_name,
                    %error,
                    "template-manager background loop task panicked"
                );
            }
            Err(error) => {
                tracing::warn!(
                    loop_name,
                    %error,
                    "template-manager background loop task cancelled"
                );
            }
            Ok(()) => {}
        }
    })
}

impl TemplateManagerService {
    pub fn new(conn: rusqlite::Connection) -> Result<Self, rusqlite::Error> {
        crate::db::ensure_template_settings_for_builds(&conn)?;
        let db = Arc::new(Mutex::new(conn));
        let background_worker = crate::background_jobs::build_worker(Arc::clone(&db));
        Ok(Self {
            db,
            artifact_store: TemplateArtifactStore::default(),
            production_hash_algorithm: crate::content_hash::DEFAULT_PRODUCTION_ALGORITHM,
            feature_flags: FeatureFlagClient::disabled(),
            pool_handle: Arc::new(Mutex::new(None)),
            snapshot_reconcile_locks: Arc::new(Mutex::new(HashMap::new())),
            snapshot_retry_backoff: Arc::new(Mutex::new(snapshots::RetryBackoff::default())),
            normalized_template_dirs: Arc::new(Mutex::new(HashSet::new())),
            importing_templates: Arc::new(Mutex::new(std::collections::HashSet::new())),
            volume_puller: crate::volume::VolumePuller::new_with_background_worker(
                background_worker.clone(),
            ),
            peer_authorizer: template_peer_authorizer(),
            background_worker,
            sbx_connect_retry_policy: pplx_space_sbx_mgr_client::DEFAULT_CONNECT_RETRY_POLICY,
            eviction_gate: crate::eviction::EvictionGate::default(),
        })
    }

    /// Install the service-owned runtime flag client before accepting work.
    pub fn with_feature_flags(mut self, flags: FeatureFlagClient) -> Self {
        self.feature_flags = flags;
        self
    }

    fn template_read_mode(&self, template: &str) -> TemplateReadMode {
        TemplateReadMode::from_flags(&self.feature_flags, template)
    }

    pub(crate) fn with_production_hash_algorithm(mut self, algorithm: HashAlg) -> Self {
        self.production_hash_algorithm = algorithm;
        self
    }

    pub(crate) fn with_rootfs_backing_policy(
        mut self,
        policy: crate::rootfs_backing::RootfsBackingPolicy,
    ) -> Self {
        self.volume_puller.backing_policy = policy;
        self
    }

    /// Handle to the shared background worker, for the service binary
    /// to call `recover_and_log` at startup and `shutdown` at exit.
    pub fn background_worker(
        &self,
    ) -> pplx_space_common_jobs::BackgroundWorker<crate::background_jobs::TemplateBackgroundExecutor>
    {
        self.background_worker.clone()
    }

    /// Decline the sandbox-manager connect budget.
    ///
    /// Tests that exercise handlers whose sandbox-manager enrichment is
    /// best-effort still run the real connect and the real fallback; they
    /// just do not wait out the shared default budget for a socket that a
    /// unit test never creates.
    #[cfg(test)]
    fn without_sbx_connect_retries(mut self) -> Self {
        self.sbx_connect_retry_policy = pplx_space_sbx_mgr_client::GrpcRetryPolicy::none();
        self
    }

    #[cfg(test)]
    pub(crate) fn new_with_template_root(
        conn: rusqlite::Connection,
        root: impl Into<PathBuf>,
    ) -> Result<Self, rusqlite::Error> {
        // These protocol/cache fixtures use the host's temporary filesystem;
        // explicit NoCoW materialization has its own disposable btrfs coverage.
        let mut service = Self::new(conn)?
            .with_rootfs_backing_policy(crate::rootfs_backing::RootfsBackingPolicy::Inherit);
        service.artifact_store = TemplateArtifactStore::new(root);
        Ok(service)
    }

    fn clone_for_snapshot_reconcile(&self) -> Self {
        self.clone()
    }

    fn spawn_desired_base_snapshot_reconcile(&self, template: String, reason: &'static str) {
        let service = self.clone_for_snapshot_reconcile();
        tokio::spawn(async move {
            let outcome = service.reconcile_desired_base_snapshot(&template).await;
            if outcome.in_progress {
                tracing::debug!(
                    template = %template,
                    reason,
                    "desired base snapshot reconcile skipped because one is already in progress"
                );
            }
            if let Some(error) = outcome.error {
                tracing::warn!(
                    template = %template,
                    error = %error,
                    reason,
                    "desired base snapshot reconcile failed"
                );
            }
        });
    }

    fn normalize_template_name_dir_once(&self, name: &str, path: &Path) {
        {
            let Ok(mut normalized) = self.normalized_template_dirs.lock() else {
                tracing::warn!(
                    template = %name,
                    "normalized template dir set mutex poisoned; skipping permission repair"
                );
                return;
            };
            if !normalized.insert(name.to_string()) {
                return;
            }
        }

        if let Err(error) = normalize_template_cache_tree(path) {
            tracing::warn!(
                template = %name,
                path = %path.display(),
                %error,
                "failed to repair template cache permissions"
            );
        }
    }

    /// Spawn the background template prefetch loop. The loop is gated
    /// internally by `SPACE_TEMPLATE_PREFETCH_ENABLED`; calling this
    /// when the env var is unset is cheap (the task logs once and
    /// returns).
    ///
    /// The handle is detached today: there's no shutdown signal in
    /// template-manager, so the task lives until the runtime is
    /// dropped. Cooperative shutdown via `CancellationToken` is a
    /// follow-up.
    pub fn spawn_prefetch_loop(&self) {
        let pins = crate::pinned_digests::PinnedDigestsStore::new(Arc::clone(&self.db));
        let puller = self.volume_puller.clone();
        let cfg = crate::prefetch::PrefetchConfig::from_env();
        spawn_observed_background_loop("prefetch", crate::prefetch::run_loop(pins, puller, cfg));
    }

    pub fn spawn_node_state_dirty_syncer(&self) {
        crate::node_state_sync::spawn_dirty_outbox_worker(Arc::clone(&self.db));
    }

    /// Spawn the LRU eviction loop. Budgeted by
    /// `SPACE_TEMPLATE_CACHE_BUDGET_BYTES` (default 100 GiB): when it is
    /// explicitly zero, [`crate::eviction::run_loop`] logs and returns
    /// immediately, so calling this unconditionally is safe.
    ///
    /// The loop owns:
    /// - The shared `Arc<Mutex<Connection>>` (same instance as the
    ///   prefetcher and the volume puller).
    /// - A clone of [`TemplateArtifactStore`] for disk deletes.
    /// - A closure that lazily connects to sbx-mgr per tick. A
    ///   failed `List` short-circuits the tick — without an active
    ///   set we cannot safely evict; grace-period age is the backstop
    ///   for any digest the snapshot might have missed.
    pub fn spawn_eviction_loop(&self, shutdown: CancellationToken) {
        let cfg = crate::eviction::GcConfig::from_env();
        let db = Arc::clone(&self.db);
        let artifact_store = self.artifact_store.clone();

        let fetch_active: crate::eviction::FetchActiveFn =
            Arc::new(|| Box::pin(fetch_build_referencing_sandbox_keys()));

        let fetch_warm_pool: crate::eviction::FetchWarmPoolFn =
            Arc::new(|| Box::pin(fetch_warm_pool_protected_keys()));

        let disk_size_for: crate::eviction::DiskSizeFn = Arc::new(
            move |name: &str, content_hash: &str, fallback: u64, snapshot_dims: &[(u32, u32)]| {
                build_disk_usage_with_snapshots(name, content_hash, fallback, snapshot_dims)
            },
        );

        let delete_disk_for = gc_delete_disk_fn(artifact_store.clone());

        // Stuck-deleting reaper probe: queried by `gc_tick`'s
        // start-of-tick recovery pass for any row left in
        // `state='deleting'` from a prior tick whose Phase 2 errored.
        // Mirrors `TemplateArtifactStore::build_artifacts_missing` —
        // missing rootfs.ext4 means the build is unrecoverable and
        // the row should be dropped; otherwise reset to 'ready' and
        // let the planner replan it.
        let reaper_probe = gc_reaper_probe_fn(artifact_store.clone());

        spawn_observed_background_loop(
            "eviction",
            crate::eviction::run_loop(
                db,
                cfg,
                shutdown,
                crate::eviction::GcHooks {
                    fetch_active,
                    fetch_warm_pool,
                    disk_size_for,
                    delete_disk_for,
                    reaper_probe,
                },
                self.eviction_gate.clone(),
            ),
        );
    }

    pub fn spawn_network_policy_notification_syncer(&self) {
        let db = Arc::clone(&self.db);
        tokio::spawn(async move {
            let mut delay = std::time::Duration::from_secs(1);
            loop {
                tokio::time::sleep(delay).await;
                let rows = {
                    let conn = match db.lock() {
                        Ok(conn) => conn,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "template DB mutex poisoned while reading network policy notification outbox"
                            );
                            delay =
                                (delay.saturating_mul(2)).min(std::time::Duration::from_secs(300));
                            continue;
                        }
                    };
                    match crate::db::list_template_network_policy_notifications(&conn, 100) {
                        Ok(rows) => rows,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "failed to read template network policy notification outbox"
                            );
                            delay =
                                (delay.saturating_mul(2)).min(std::time::Duration::from_secs(300));
                            continue;
                        }
                    }
                };
                if rows.is_empty() {
                    delay = std::time::Duration::from_secs(1);
                    continue;
                }

                let mut sent = Vec::new();
                for row in rows {
                    let mut sbx_client = pplx_space_sbx_mgr_client::lazy();
                    let result = sbx_client
                        .template_network_policy_updated(
                            pplx_space_sbx_mgr_client::proto::TemplateNetworkPolicyUpdatedRequest {
                                template: row.template.clone(),
                                policy: Some(common_network_policy_to_proto(&row.policy)),
                                generation: row.generation,
                                authority: row.authority.as_proto_i32(),
                            },
                        )
                        .await;
                    match result {
                        Ok(_) => sent.push(row),
                        Err(error) => {
                            tracing::warn!(
                                template = %row.template,
                                generation = row.generation,
                                %error,
                                "failed to notify sbx-manager of template network policy; retrying"
                            );
                        }
                    }
                }

                if !sent.is_empty() {
                    let conn = match db.lock() {
                        Ok(conn) => conn,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "template DB mutex poisoned while deleting network policy notifications"
                            );
                            delay =
                                (delay.saturating_mul(2)).min(std::time::Duration::from_secs(300));
                            continue;
                        }
                    };
                    if let Err(error) =
                        crate::db::delete_template_network_policy_notifications(&conn, &sent)
                    {
                        tracing::warn!(
                            %error,
                            "failed to delete sent template network policy notifications"
                        );
                    }
                }
                delay = if sent.is_empty() {
                    (delay.saturating_mul(2)).min(std::time::Duration::from_secs(300))
                } else {
                    std::time::Duration::from_secs(1)
                };
            }
        });
    }

    pub fn spawn_startup_base_snapshot_reconcile(&self) {
        let service = self.clone_for_snapshot_reconcile();
        tokio::spawn(async move {
            let snapshot_policies = {
                let db = Arc::clone(&service.db);
                tokio::task::spawn_blocking(move || {
                    let Ok(conn) = db.lock() else {
                        tracing::warn!(
                            "template DB mutex poisoned while listing snapshot policies"
                        );
                        return Vec::new();
                    };
                    crate::db::list_template_snapshot_policies(&conn).unwrap_or_default()
                })
                .await
                .unwrap_or_default()
            };

            if snapshot_policies.is_empty() {
                return;
            }

            tracing::debug!(
                policy_count = snapshot_policies.len(),
                "starting base snapshot policy reconciliation in background"
            );
            for settings in snapshot_policies {
                tracing::debug!(
                    template = %settings.template,
                    ensure_base_snapshot = settings.ensure_base_snapshot,
                    "reconciling base snapshot policy"
                );
                let outcome = service
                    .reconcile_desired_base_snapshot(&settings.template)
                    .await;
                if let Some(error) = outcome.error {
                    tracing::warn!(
                        template = %settings.template,
                        error = %error,
                        "desired base snapshot reconcile failed"
                    );
                }
            }
        });
    }

    fn spawn_template_compressed_rootfs_generation(
        &self,
        template_id: String,
        content_hash: String,
        digest_dir: String,
        trigger: &'static str,
    ) {
        let service = self.clone();
        let digest_path = PathBuf::from(&digest_dir);
        {
            let inflight = rootfs_zstd_generation_inflight();
            let Ok(mut inflight) = inflight.lock() else {
                tracing::warn!(
                    template = %template_id,
                    content_hash,
                    digest_dir,
                    "compressed rootfs generation inflight set mutex poisoned; skipping schedule"
                );
                return;
            };
            if !inflight.insert(digest_path.clone()) {
                tracing::debug!(
                    template = %template_id,
                    content_hash,
                    digest_dir,
                    "compressed template rootfs generation already scheduled"
                );
                return;
            }
        }

        tokio::spawn(async move {
            let _inflight_guard = RootfsZstdGenerationInflightGuard { digest_path };
            if let Err(error) = service
                .ensure_template_compressed_rootfs_metadata(
                    &template_id,
                    &content_hash,
                    &digest_dir,
                    trigger,
                )
                .await
            {
                tracing::warn!(
                    template = %template_id,
                    content_hash,
                    digest_dir,
                    error = %error,
                    "background compressed template rootfs generation failed"
                );
            }
        });
    }

    async fn ensure_template_compressed_rootfs_metadata(
        &self,
        template_id: &str,
        content_hash: &str,
        digest_dir: &str,
        trigger: &'static str,
    ) -> Result<(), Status> {
        let digest_path = Path::new(digest_dir);
        let manifest_path = digest_path.join("manifest.json");
        let compressed_rootfs_path = digest_path.join(TEMPLATE_ROOTFS_ZSTD_FILE);
        let manifest_json = std::fs::read_to_string(&manifest_path).unwrap_or_default();
        let manifest: pplx_space_common_core::types::TemplateManifest =
            serde_json::from_str(&manifest_json).unwrap_or_default();

        let generated_stats = if !compressed_rootfs_path.exists() {
            Some(
                generate_template_compressed_rootfs(
                    template_id,
                    content_hash,
                    digest_dir,
                    trigger,
                    self.template_read_mode(template_id),
                )
                .await?,
            )
        } else {
            None
        };

        let rootfs_path = digest_path.join(TEMPLATE_ROOTFS_FILE);
        let rootfs_size = std::fs::metadata(&rootfs_path)
            .map(|metadata| metadata.len())
            .map_err(|error| {
                Status::internal(format!("stat rootfs {}: {error}", rootfs_path.display()))
            })?;
        let (compressed_rootfs_hash, compressed_rootfs_size) = if let Some(stats) = generated_stats
        {
            (stats.compressed_hash, stats.compressed_size)
        } else {
            let hash = compute_template_file_hash(
                compressed_rootfs_path.clone(),
                HashAlg::Sha256,
                "hash compressed rootfs",
                self.template_read_mode(template_id),
            )
            .await?;
            let size = std::fs::metadata(&compressed_rootfs_path)
                .map(|metadata| metadata.len())
                .map_err(|error| {
                    Status::internal(format!(
                        "stat compressed rootfs {}: {error}",
                        compressed_rootfs_path.display()
                    ))
                })?;
            (hash, size)
        };
        let metadata_json =
            std::fs::read_to_string(digest_path.join("metadata.json")).unwrap_or_default();
        let metadata: pplx_space_common_core::types::TemplateMetadata =
            serde_json::from_str(&metadata_json).unwrap_or_default();
        let manifest = normalize_template_manifest(
            manifest,
            template_id,
            content_hash,
            &metadata,
            rootfs_size,
            Some((&compressed_rootfs_hash, compressed_rootfs_size)),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|error| Status::internal(format!("serialize manifest: {error}")))?;
        write_template_manifest_atomic(&manifest_path, manifest_json)?;
        normalize_template_cache_tree(digest_path)?;

        self.upsert_build_from_digest_dir(
            template_id,
            content_hash,
            digest_dir,
            RootfsHashVerification::AlreadyVerified,
        )
        .await
    }

    fn authorize_peer<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
        allowed: &[UdsPeerPrincipal],
    ) -> Result<AuthorizedUdsPeer, Status> {
        self.peer_authorizer
            .authorize_request(request, operation, allowed)
    }

    async fn upsert_build_from_digest_dir(
        &self,
        template_id: &str,
        content_hash: &str,
        digest_dir: &str,
        rootfs_hash_verification: RootfsHashVerification,
    ) -> Result<(), Status> {
        let manifest_path = Path::new(digest_dir).join("manifest.json");
        let metadata_path = Path::new(digest_dir).join("metadata.json");
        let rootfs_path = Path::new(digest_dir).join("rootfs.ext4");

        let manifest_json = std::fs::read_to_string(&manifest_path).map_err(|e| {
            Status::internal(format!("read manifest {}: {e}", manifest_path.display()))
        })?;
        let metadata_json = std::fs::read_to_string(&metadata_path).unwrap_or_default();
        let manifest: pplx_space_common_core::types::TemplateManifest =
            serde_json::from_str(&manifest_json).unwrap_or_default();
        let metadata: pplx_space_common_core::types::TemplateMetadata =
            serde_json::from_str(&metadata_json).unwrap_or_default();
        let build_date = manifest.build_date.clone();
        let runtime = if manifest.runtime.is_empty() {
            RUNTIME_FIRECRACKER_ALIAS.to_string()
        } else {
            manifest.runtime.clone()
        };
        let arch = template_arch(&metadata, &manifest);
        crate::rootfs_cache::verify_rootfs_from_metadata(digest_dir).map_err(|error| {
            Status::failed_precondition(format!(
                "template rootfs is unavailable for {template_id}@{content_hash}: {error}"
            ))
        })?;
        if rootfs_hash_verification == RootfsHashVerification::Verify {
            let observed_hash = compute_template_file_hash(
                rootfs_path.clone(),
                content_hash_algorithm(content_hash)?,
                "hash digest rootfs",
                self.template_read_mode(template_id),
            )
            .await?;
            if observed_hash != content_hash {
                return Err(Status::failed_precondition(format!(
                    "rootfs at {} has hash {observed_hash} but expected {content_hash}",
                    rootfs_path.display()
                )));
            }
        }
        let artifact_size = match rootfs_artifact_size_from_manifest(&manifest) {
            Some(size) => size,
            None => std::fs::metadata(&rootfs_path)
                .map(|m| m.len())
                .map_err(|e| {
                    Status::internal(format!("stat rootfs {}: {e}", rootfs_path.display()))
                })?,
        };

        let db = Arc::clone(&self.db);
        let n = template_id.to_string();
        let h = content_hash.to_string();
        tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let conn = lock_status(&db, "template DB")?;
            crate::db::upsert_build(
                &conn,
                &crate::db::BuildUpsert {
                    name: &n,
                    content_hash: &h,
                    build_date: &build_date,
                    manifest_json: &manifest_json,
                    runtime: &runtime,
                    arch: &arch,
                    artifact_size_bytes: artifact_size,
                    state: "ready",
                },
            )
            .map_err(|e| Status::internal(format!("db upsert_build: {e}")))
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))??;

        crate::node_state_sync::mark_template_dirty(&self.db, template_id);
        Ok(())
    }

    /// Imports may refresh description/build timestamps and recompute the
    /// compressed artifact. All other JSON fields must match the local build,
    /// including execution provenance, configuration, lineage and resource
    /// defaults. Omitted metadata preserves the existing document. Changing
    /// execution metadata requires the separate authorized registration path;
    /// an import's rootfs digest does not authenticate those fields.
    async fn prepare_existing_build_refresh(
        &self,
        template_id: &str,
        content_hash: &str,
        digest_dir: &str,
        manifest_json: Option<&[u8]>,
        metadata_json: Option<&[u8]>,
    ) -> Result<Option<ImportStagingGuard>, Status> {
        let digest_path = Path::new(digest_dir);
        let unchanged = template_file_replacement_is_unchanged(
            &digest_path.join("manifest.json"),
            manifest_json,
        )? && template_file_replacement_is_unchanged(
            &digest_path.join("metadata.json"),
            metadata_json,
        )?;
        let staging = if unchanged {
            None
        } else {
            let path = PathBuf::from(format!("{digest_dir}.republishing.{}", Uuid::now_v7()));
            std::fs::create_dir(&path).map_err(|error| {
                Status::internal(format!("create republish staging dir: {error}"))
            })?;
            let guard = ImportStagingGuard::new(path);
            set_template_cache_permissions(&guard.path, TEMPLATE_CACHE_DIR_MODE)?;
            for (file, replacement, required) in [
                (TEMPLATE_ROOTFS_FILE, None, true),
                ("manifest.json", manifest_json, true),
                ("metadata.json", metadata_json, false),
            ] {
                write_or_copy_template_file(
                    &digest_path.join(file),
                    &guard.path.join(file),
                    replacement,
                    required,
                )?;
            }
            import_helpers::validate_existing_refresh_metadata(digest_path, &guard.path)?;
            Some(guard)
        };
        let prepared_path = staging
            .as_ref()
            .map_or(digest_path, |guard| guard.path.as_path());
        crate::rootfs_cache::verify_rootfs_from_metadata(prepared_path).map_err(|error| {
            Status::failed_precondition(format!("verify existing rootfs: {error}"))
        })?;
        let observed_hash = compute_template_file_hash(
            prepared_path.join(TEMPLATE_ROOTFS_FILE),
            content_hash_algorithm(content_hash)?,
            "hash existing rootfs",
            self.template_read_mode(template_id),
        )
        .await?;
        if observed_hash != content_hash {
            return Err(Status::failed_precondition(format!(
                "existing rootfs has hash {observed_hash} but expected {content_hash}"
            )));
        }
        let manifest_bytes = std::fs::read(prepared_path.join("manifest.json"))
            .map_err(|error| Status::internal(format!("read prepared manifest: {error}")))?;
        let manifest =
            import_helpers::parse_template_manifest("prepared manifest", &manifest_bytes)?;
        let metadata = match std::fs::read(prepared_path.join("metadata.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                Status::invalid_argument(format!("parse prepared metadata: {error}"))
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => return Err(Status::internal(format!("read prepared metadata: {error}"))),
        };
        // Validate the typed schema without serializing its defaults over the raw manifest.
        let rootfs_size = std::fs::metadata(prepared_path.join(TEMPLATE_ROOTFS_FILE))
            .map_err(|error| Status::internal(format!("stat existing rootfs: {error}")))?
            .len();
        normalize_template_manifest(
            manifest,
            template_id,
            content_hash,
            &metadata,
            rootfs_size,
            None,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if staging.is_some() {
            let stats = generate_template_compressed_rootfs(
                template_id,
                content_hash,
                &prepared_path.to_string_lossy(),
                "local_import",
                self.template_read_mode(template_id),
            )
            .await?;
            let object = |value: Option<serde_json::Value>| {
                serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                    value
                        .filter(|value| !value.is_null())
                        .unwrap_or_else(|| serde_json::json!({})),
                )
                .map_err(|error| {
                    Status::invalid_argument(format!("parse manifest object: {error}"))
                })
            };
            let mut manifest: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&manifest_bytes).map_err(|error| {
                    Status::invalid_argument(format!("parse prepared manifest: {error}"))
                })?;
            let mut files = object(manifest.remove("files"))?;
            let mut compressed = object(files.remove(TEMPLATE_ROOTFS_ZSTD_FILE))?;
            let mut compression = object(compressed.remove("compression"))?;
            compression.insert(
                "codec".into(),
                serde_json::json!(TEMPLATE_ROOTFS_ZSTD_CODEC),
            );
            compression.insert(
                "level".into(),
                serde_json::json!(TEMPLATE_ROOTFS_ZSTD_LEVEL),
            );
            compressed.insert("digest".into(), serde_json::json!(stats.compressed_hash));
            compressed.insert("size".into(), serde_json::json!(stats.compressed_size));
            compressed.insert("compression".into(), compression.into());
            files.insert(TEMPLATE_ROOTFS_ZSTD_FILE.into(), compressed.into());
            manifest.insert("files".into(), files.into());
            write_template_manifest_atomic(
                &prepared_path.join("manifest.json"),
                serde_json::to_vec_pretty(&manifest).map_err(|error| {
                    Status::internal(format!("serialize prepared manifest: {error}"))
                })?,
            )?;
            normalize_template_cache_tree(prepared_path)?;
        }
        Ok(staging)
    }

    async fn publish_existing_build_refresh(
        &self,
        template_id: &str,
        content_hash: &str,
        digest_dir: &str,
        prepared: Option<ImportStagingGuard>,
    ) -> Result<(), Status> {
        let Some(mut staging_guard) = prepared else {
            return self
                .upsert_build_from_digest_dir(
                    template_id,
                    content_hash,
                    digest_dir,
                    RootfsHashVerification::AlreadyVerified,
                )
                .await;
        };
        let digest_path = Path::new(digest_dir);
        let replaced_path = replace_template_build_dir(&staging_guard.path, digest_path)?;
        staging_guard.disarm();
        if let Err(error) = self
            .upsert_build_from_digest_dir(
                template_id,
                content_hash,
                digest_dir,
                RootfsHashVerification::AlreadyVerified,
            )
            .await
        {
            let rejected_path = replace_template_build_dir(&replaced_path, digest_path)?;
            drop(ImportStagingGuard::new(rejected_path));
            return Err(error);
        }
        if let Err(error) = remove_staging_path(&replaced_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %replaced_path.display(),
                %error,
                "failed to remove replaced template build dir"
            );
        }

        Ok(())
    }

    /// Register-time refresh for a digest_dir that already exists.
    ///
    /// Verifies the local-cache invariant — `sha256(digest_dir/rootfs.ext4)`
    /// must equal the canonical expanded `content_hash` — and refuses to
    /// mutate the rootfs. Optionally updates `metadata.json` if the incoming
    /// bytes differ; `manifest.json` is left alone because it was written from
    /// this hash by a prior register.
    async fn refresh_register_existing_expanded_build(
        &self,
        template_id: &str,
        content_hash: &str,
        digest_dir: &str,
        metadata_json: Option<&[u8]>,
    ) -> Result<(), Status> {
        let digest_path = Path::new(digest_dir);
        let rootfs_path = digest_path.join(TEMPLATE_ROOTFS_FILE);

        if !rootfs_path.exists() {
            return Err(Status::failed_precondition(format!(
                "template rootfs missing at {}",
                rootfs_path.display()
            )));
        }

        let observed_hash = compute_template_file_hash(
            rootfs_path.clone(),
            content_hash_algorithm(content_hash)?,
            "hash digest rootfs",
            self.template_read_mode(template_id),
        )
        .await?;
        if observed_hash != content_hash {
            return Err(Status::failed_precondition(format!(
                "rootfs at {} has hash {observed_hash} but expected {content_hash}",
                rootfs_path.display()
            )));
        }

        if let Some(bytes) = metadata_json {
            let manifest_bytes = std::fs::read(digest_path.join("manifest.json"))
                .map_err(|error| Status::data_loss(format!("read retained manifest: {error}")))?;
            let manifest = serde_json::from_slice(&manifest_bytes)
                .map_err(|error| Status::data_loss(format!("parse retained manifest: {error}")))?;
            let incoming = serde_json::from_slice(bytes).map_err(|error| {
                Status::invalid_argument(format!("parse incoming metadata: {error}"))
            })?;
            validate_retained_environment_binding(&manifest, &incoming)?;
            let metadata_path = digest_path.join("metadata.json");
            if !template_file_replacement_is_unchanged(&metadata_path, Some(bytes))? {
                std::fs::write(&metadata_path, bytes).map_err(|error| {
                    Status::internal(format!("write {}: {error}", metadata_path.display()))
                })?;
                normalize_template_cache_tree(digest_path)?;
            }
        }

        let manifest_json =
            std::fs::read_to_string(digest_path.join("manifest.json")).unwrap_or_default();
        let manifest: pplx_space_common_core::types::TemplateManifest =
            serde_json::from_str(&manifest_json).unwrap_or_default();
        let compressed_rootfs_needs_refresh = !digest_path.join(TEMPLATE_ROOTFS_ZSTD_FILE).exists()
            || !manifest_has_compressed_rootfs_entry(&manifest);

        self.upsert_build_from_digest_dir(
            template_id,
            content_hash,
            digest_dir,
            RootfsHashVerification::AlreadyVerified,
        )
        .await?;

        if compressed_rootfs_needs_refresh {
            self.spawn_template_compressed_rootfs_generation(
                template_id.to_string(),
                content_hash.to_string(),
                digest_dir.to_string(),
                "register_existing",
            );
        }

        Ok(())
    }
}

async fn expand_template_rootfs(
    template_id: &str,
    content_hash: &str,
    digest_dir: &str,
    trigger: &'static str,
) -> Result<(), Status> {
    let dir = digest_dir.to_string();
    match tokio::task::spawn_blocking(move || {
        crate::rootfs_cache::expand_rootfs_from_metadata(Path::new(&dir))
    })
    .await
    {
        Ok(Ok(Some((path, size_bytes)))) => {
            pplx_space_metrics::template::rootfs_expansion(template_id, trigger, size_bytes);
            tracing::info!(
                template = %template_id,
                content_hash,
                rootfs = %path.display(),
                size_bytes,
                "expanded template rootfs"
            );
            Ok(())
        }
        Ok(Ok(None)) => Ok(()),
        Ok(Err(error)) => {
            pplx_space_metrics::template::rootfs_expansion_error(
                template_id,
                trigger,
                error.reason(),
            );
            tracing::warn!(
                template = %template_id,
                content_hash,
                %error,
                "template rootfs expansion failed; template build will not be published"
            );
            Err(Status::failed_precondition(format!(
                "template rootfs expansion failed for {template_id}@{content_hash}: {error}"
            )))
        }
        Err(error) => {
            pplx_space_metrics::template::rootfs_expansion_error(template_id, trigger, "task_join");
            tracing::warn!(
                template = %template_id,
                content_hash,
                %error,
                "template rootfs expansion task failed; template build will not be published"
            );
            Err(Status::internal(format!(
                "template rootfs expansion task failed for {template_id}@{content_hash}: {error}"
            )))
        }
    }
}

mod adapter;
mod import_helpers;
mod lookup;
mod pools;
mod snapshots;
mod startup_recovery;
mod support;
mod templates;
#[cfg(test)]
mod tests;
mod transfer;

use import_helpers::*;
use support::*;
