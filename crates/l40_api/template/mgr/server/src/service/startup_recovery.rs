//! Startup recovery reconciles the on-disk template cache with the
//! local SQLite catalog after a process restart.
//!
//! Design:
//!
//! - A row in `template_builds` is treated as a validity signal. When
//!   a digest dir has a matching DB row, recovery checks only that the
//!   files the manifest declares are on disk at their recorded sizes —
//!   no hashing.
//! - When a digest dir has no DB row, recovery has no prior validity
//!   signal. It computes the declared digest of the bootable artifact and
//!   confirms it matches the dir name. On match the build row is
//!   inserted; on mismatch the digest dir is removed and no row is
//!   written. The manifest provides the file list and recorded sizes.
//! - `manifest.json` is the source of truth for which files should
//!   exist in the digest dir. Entries whose `compression` descriptor
//!   is set are stored only in object storage and are not required
//!   locally; entries without compression are required locally at the
//!   declared size.
//! - A read that fails in a way that can clear (an I/O error reading the
//!   manifest or `metadata.json`, stating a file or hashing the bootable
//!   artifact) never deletes the dir: recovery drops the build's row and
//!   keeps the dir, which the GC tick's rowless check, or the next startup,
//!   verifies from scratch. Until then pulls refuse the dir retryably,
//!   since a pull never adopts or writes into a build dir that has no row.
//! - `metadata.json` that's absent or empty means the build has none; one
//!   that reads but doesn't parse is a permanent rejection.
//! - Leftover transient dirs (`.old` template dirs, import and republish
//!   staging, interrupted replacements) are reaped before the socket
//!   binds, by [`TemplateManagerService::clean_transient_dirs_before_serving`],
//!   since the per-dir pass runs once requests flow and a live import's
//!   staging dir looks the same.

use super::*;

use pplx_space_template_manifest::{TemplateManifest, TemplateMetadata};

fn lock_recovery_db<'a>(
    db: &'a Mutex<rusqlite::Connection>,
) -> Result<std::sync::MutexGuard<'a, rusqlite::Connection>, String> {
    db.lock()
        .map_err(|_| "template DB mutex poisoned".to_string())
}

/// Reasons a digest dir fails reconciliation.
///
/// A permanent rejection removes the dir. A transient one, an I/O error
/// reading the manifest or metadata, stating a file or hashing the bootable
/// artifact that can clear without any change to the dir, drops only the
/// build's row and keeps the dir, so the next GC tick, or the next startup,
/// verifies it from scratch; see [`DigestDirRejection::is_transient`].
#[derive(Debug, thiserror::Error)]
enum DigestDirRejection {
    #[error("manifest.json read failed: {error}")]
    ManifestUnreadable { error: String, transient: bool },
    #[error("manifest.json parse failed: {0}")]
    ManifestUnparseable(String),
    #[error("metadata.json read failed: {error}")]
    MetadataUnreadable { error: String, transient: bool },
    #[error("metadata.json parse failed: {0}")]
    MetadataUnparseable(String),
    #[error("manifest filename {filename:?} is unsafe: {reason}")]
    ManifestFilenamesUnsafe {
        filename: String,
        reason: &'static str,
    },
    #[error("manifest content_hash {found:?} does not match digest dir name {expected:?}")]
    ContentHashDisagreesWithDirName { found: String, expected: String },
    #[error("manifest binds the {found} cache partition but the digest dir holds {expected}")]
    PartitionDisagreesWithDirName { found: String, expected: String },
    /// `transient` when the schema is one this release doesn't support yet,
    /// which a later release may read; otherwise the binding is invalid.
    #[error("manifest execution metadata is invalid: {reason}")]
    ExecutionMetadataInvalid { reason: String, transient: bool },
    #[error("manifest does not declare a bootable_artifact")]
    BootableArtifactNotDeclared,
    #[error("manifest.files map has no entry for bootable artifact {0:?}")]
    BootableEntryAbsentFromFilesMap(String),
    #[error("bootable artifact entry digest {found:?} does not match digest dir name {expected:?}")]
    BootableEntryDigestDisagreesWithDirName { found: String, expected: String },
    #[error("manifest entry for {filename:?} does not declare a size")]
    FileSizeNotDeclared { filename: String },
    #[error("file {filename:?} is declared by manifest but missing on disk")]
    DeclaredFileMissing { filename: String },
    #[error("file {filename:?} has size {actual} on disk, manifest declares {recorded}")]
    DeclaredFileSizeMismatch {
        filename: String,
        actual: u64,
        recorded: u64,
    },
    #[error("could not stat {filename:?}: {error}")]
    StatFailed {
        filename: String,
        error: String,
        transient: bool,
    },
    #[error("could not hash bootable artifact: {error}")]
    BootableArtifactUnreadable { error: String, transient: bool },
    #[error("bootable artifact sha256 {computed:?} does not match digest dir name {expected:?}")]
    BootableArtifactHashMismatch { computed: String, expected: String },
}

impl DigestDirRejection {
    /// Whether reading the dir failed in a way that can clear without any
    /// change to it. Deleting such a dir would lose an installed build a
    /// later startup could verify, and a twin can't be pulled back.
    fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::ManifestUnreadable {
                transient: true,
                ..
            } | Self::MetadataUnreadable {
                transient: true,
                ..
            } | Self::StatFailed {
                transient: true,
                ..
            } | Self::BootableArtifactUnreadable {
                transient: true,
                ..
            } | Self::ExecutionMetadataInvalid {
                transient: true,
                ..
            }
        )
    }
}

/// Whether an I/O error reading a build's files can clear without any change
/// to them, as an I/O error from the device can. A missing file, one that
/// isn't valid UTF-8, and a path that's a directory, runs through a file,
/// loops or is invalid stay that way.
///
/// A permission error counts as transient, by decision: a deploy with the
/// wrong user or security profile fails every read with EACCES, and treating
/// that as permanent would delete every node's cache with no way back short
/// of re-pulling it once the deploy is fixed. As transient, an EACCES that
/// never clears keeps the dir without a row, so every pull of its digest is
/// refused until it clears or `DeleteTemplateBuild` removes the dir, with a
/// warning each time the GC tick examines it. That's where this differs from
/// `host_path_io_error_is_transient` in `pplx-space-host-linux`'s
/// `path_errors`, whose permanent answer fails a sandbox create on the node's
/// launch path rather than deleting a build.
fn io_error_is_transient(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    let permanent = matches!(
        error.kind(),
        ErrorKind::NotFound
            | ErrorKind::InvalidData
            | ErrorKind::InvalidInput
            | ErrorKind::InvalidFilename
            | ErrorKind::IsADirectory
            | ErrorKind::NotADirectory
    ) || error.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error());
    !permanent
}

fn parse_manifest_from_digest_dir(
    digest_dir: &Path,
) -> Result<(TemplateManifest, String), DigestDirRejection> {
    let manifest_path = digest_dir.join("manifest.json");
    let manifest_json = std::fs::read_to_string(&manifest_path).map_err(|error| {
        DigestDirRejection::ManifestUnreadable {
            transient: io_error_is_transient(&error),
            error: error.to_string(),
        }
    })?;
    let manifest: TemplateManifest = serde_json::from_str(&manifest_json)
        .map_err(|error| DigestDirRejection::ManifestUnparseable(error.to_string()))?;
    // The on-disk manifest is whatever the last writer left, which in
    // crash-recovery scenarios may be the raw object from an interrupted
    // pull. Sanitize filenames before they reach `Path::join` — without
    // this, `bootable_artifact: "../escape"` or an absolute path slips
    // straight into `fs::metadata`.
    manifest
        .validate_filenames()
        .map_err(|error| DigestDirRejection::ManifestFilenamesUnsafe {
            filename: error.name,
            reason: error.reason,
        })?;
    Ok((manifest, manifest_json))
}

/// A build's metadata.json, or the default when it has none: registration
/// copies it only when the source has one, and install treats an empty file
/// as none (`validate_guest_documents`). It carries the build's arch and
/// execution metadata, so a file that's there but can't be read or parsed is
/// a rejection, never the default, which would register the build with the
/// wrong ones.
fn parse_metadata_from_digest_dir(
    digest_dir: &Path,
) -> Result<TemplateMetadata, DigestDirRejection> {
    let metadata_json = match std::fs::read_to_string(digest_dir.join("metadata.json")) {
        Ok(json) => json,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(TemplateMetadata::default());
        }
        Err(error) => {
            return Err(DigestDirRejection::MetadataUnreadable {
                transient: io_error_is_transient(&error),
                error: error.to_string(),
            });
        }
    };
    if metadata_json.is_empty() {
        return Ok(TemplateMetadata::default());
    }
    serde_json::from_str(&metadata_json)
        .map_err(|error| DigestDirRejection::MetadataUnparseable(error.to_string()))
}

/// Confirm manifest invariants that don't require I/O: content_hash
/// matches the dir name and the bootable artifact entry is present and
/// pinned to the dir name.
fn validate_manifest_self_consistency(
    hash_name: &str,
    manifest: &TemplateManifest,
) -> Result<(), DigestDirRejection> {
    if manifest.content_hash != hash_name {
        return Err(DigestDirRejection::ContentHashDisagreesWithDirName {
            found: manifest.content_hash.clone(),
            expected: hash_name.to_string(),
        });
    }
    if manifest.bootable_artifact.is_empty() {
        return Err(DigestDirRejection::BootableArtifactNotDeclared);
    }
    let bootable_entry = manifest
        .files
        .get(&manifest.bootable_artifact)
        .ok_or_else(|| {
            DigestDirRejection::BootableEntryAbsentFromFilesMap(manifest.bootable_artifact.clone())
        })?;
    if bootable_entry.digest != hash_name {
        return Err(
            DigestDirRejection::BootableEntryDigestDisagreesWithDirName {
                found: bootable_entry.digest.clone(),
                expected: hash_name.to_string(),
            },
        );
    }
    Ok(())
}

/// Checks a build dir against the partition its binding selects.
///
/// A legacy-layout `{hash}` dir is adopted in place whatever environment it
/// names: nodes that ran another environment before partitions existed hold
/// such builds, often under running and suspended sandboxes. Dropping one
/// would delete a restore parent and force a re-pull, and moving it would
/// change its received UUID and hide it from an older release after a
/// rollback. So it stays filed in the legacy partition at its path, an
/// environment's lookup still finds it there through its legacy fallback,
/// and a rollback serves it unchanged. Only a `{hash}@{env}` dir, which this
/// release created, must name its own partition. A binding under a schema
/// this release doesn't support yet is a transient rejection: the dir is kept
/// and reported, so a later release can read it. Any other invalid binding is
/// a permanent rejection, removed only when no sandbox depends on the digest.
fn validate_partition_binding(
    partition: crate::db::TemplateCachePartition,
    manifest: &TemplateManifest,
    metadata: &TemplateMetadata,
) -> Result<(), DigestDirRejection> {
    if partition == crate::db::TemplateCachePartition::Legacy {
        return Ok(());
    }
    let environment = binding_environment(manifest, metadata)?;
    let bound = pplx_space_node_runtime_core::catalog::template_cache_partition(environment);
    if bound != partition {
        return Err(DigestDirRejection::PartitionDisagreesWithDirName {
            found: bound.to_string(),
            expected: partition.to_string(),
        });
    }
    Ok(())
}

/// The environment a build dir's manifest binds, else its metadata's, as
/// [`crate::service::effective_environment`] reads it, with an unsupported
/// schema told apart from an invalid binding.
fn binding_environment(
    manifest: &TemplateManifest,
    metadata: &TemplateMetadata,
) -> Result<Option<pplx_space_execution_spec::EnvironmentReference>, DigestDirRejection> {
    use pplx_space_template_manifest::{TemplateManifestSchemaError, TemplateMetadataSchemaError};
    let invalid = |reason: String, transient: bool| DigestDirRejection::ExecutionMetadataInvalid {
        reason,
        transient,
    };
    match manifest.execution_metadata() {
        Ok(Some(execution)) => Ok(execution.environment()),
        Ok(None) => metadata.environment().map_err(|error| {
            let transient = matches!(error, TemplateMetadataSchemaError::UnsupportedVersion(_));
            invalid(error.to_string(), transient)
        }),
        Err(error) => {
            let transient = matches!(error, TemplateManifestSchemaError::UnsupportedVersion(_));
            Err(invalid(error.to_string(), transient))
        }
    }
}

/// Verify that every locally-required file the manifest declares is on
/// disk at the recorded size. Manifest entries with a compression
/// descriptor live in object storage only and are skipped here — that's
/// the compressed-pair edge case where the worker keeps only the
/// decompressed sibling.
fn verify_local_files_against_manifest(
    digest_dir: &Path,
    manifest: &TemplateManifest,
) -> Result<(), DigestDirRejection> {
    for (filename, entry) in &manifest.files {
        if entry.compression.is_some() {
            continue;
        }
        let recorded = entry
            .size
            .ok_or_else(|| DigestDirRejection::FileSizeNotDeclared {
                filename: filename.clone(),
            })?;
        let path = digest_dir.join(filename);
        let actual = match std::fs::metadata(&path) {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DigestDirRejection::DeclaredFileMissing {
                    filename: filename.clone(),
                });
            }
            Err(error) => {
                return Err(DigestDirRejection::StatFailed {
                    filename: filename.clone(),
                    transient: io_error_is_transient(&error),
                    error: error.to_string(),
                });
            }
        };
        if actual != recorded {
            return Err(DigestDirRejection::DeclaredFileSizeMismatch {
                filename: filename.clone(),
                actual,
                recorded,
            });
        }
    }
    Ok(())
}

/// Hash the bootable artifact and confirm it matches `expected_hash`.
/// Used only when the DB has no prior validity signal for this digest
/// dir; the multi-GiB read is wrapped in `spawn_blocking`.
async fn verify_bootable_artifact_hash(
    digest_dir: &Path,
    bootable_filename: &str,
    expected_hash: &str,
    read_mode: TemplateReadMode,
) -> Result<(), DigestDirRejection> {
    let path = digest_dir.join(bootable_filename);
    let path_str = path.display().to_string();
    let algorithm = content_hash_algorithm(expected_hash).map_err(|error| {
        DigestDirRejection::BootableArtifactUnreadable {
            error: error.to_string(),
            transient: false,
        }
    })?;
    let computed =
        tokio::task::spawn_blocking(move || compute_content_hash(&path_str, algorithm, read_mode))
            .await
            .map_err(|error| DigestDirRejection::BootableArtifactUnreadable {
                error: format!("hash task failed: {error}"),
                transient: false,
            })?
            .map_err(|error| DigestDirRejection::BootableArtifactUnreadable {
                transient: io_error_is_transient(&error),
                error: error.to_string(),
            })?;
    if computed != expected_hash {
        return Err(DigestDirRejection::BootableArtifactHashMismatch {
            computed,
            expected: expected_hash.to_string(),
        });
    }
    Ok(())
}

async fn lookup_build_row(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
    partition: crate::db::TemplateCachePartition,
) -> Result<Option<crate::db::BuildRow>, String> {
    let db = Arc::clone(db);
    let n = name.to_string();
    let h = content_hash.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = lock_recovery_db(&db)?;
        crate::db::get_build(&conn, &n, &h, partition).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("spawn_blocking: {error}"))?
    .map_err(|error| format!("db::get_build: {error}"))
}

struct ValidatedBuildRow {
    name: String,
    content_hash: String,
    partition: crate::db::TemplateCachePartition,
    manifest_json: String,
    build_date: String,
    runtime: String,
    arch: String,
    artifact_size_bytes: u64,
}

async fn insert_validated_build_row(
    db: &Arc<Mutex<rusqlite::Connection>>,
    row: ValidatedBuildRow,
) -> Result<(), String> {
    let db = Arc::clone(db);
    tokio::task::spawn_blocking(move || {
        let conn = lock_recovery_db(&db)?;
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: &row.name,
                content_hash: &row.content_hash,
                cache_partition: row.partition,
                build_date: &row.build_date,
                manifest_json: &row.manifest_json,
                runtime: &row.runtime,
                arch: &row.arch,
                artifact_size_bytes: row.artifact_size_bytes,
                state: "ready",
            },
        )
        .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("spawn_blocking: {error}"))?
    .map_err(|error| format!("db::upsert_build: {error}"))
}

async fn delete_stale_build_row(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    content_hash: &str,
    partition: crate::db::TemplateCachePartition,
) -> Result<(), String> {
    let db = Arc::clone(db);
    let n = name.to_string();
    let h = content_hash.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = lock_recovery_db(&db)?;
        crate::db::delete_build(&conn, &n, &h, partition).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("spawn_blocking: {error}"))?
    .map_err(|error| format!("db::delete_build: {error}"))
}

/// Remove an invalid build dir under `template_root`. Installed builds are
/// read-only btrfs subvolumes that only node-manager can delete; the local
/// subvolume-aware cleanup is the fallback, as on the pull path. A dir that
/// survives both stays without a row, and the next GC tick rejects it and
/// tries again.
///
/// Node-manager resolves `(name, hash, partition)` under `TEMPLATE_DIR`, so
/// the RPC is sent only when that's the root being recovered; under any
/// other root it would delete the canonical cache's build of the same name,
/// hash and partition, and only the local cleanup runs.
async fn remove_digest_dir_from_disk(template_root: &Path, build: DigestDirBuild<'_>) {
    let DigestDirBuild {
        name,
        hash_name,
        partition,
        digest_dir,
    } = build;
    let canonical_root = template_root == Path::new(TEMPLATE_DIR);
    let dir = digest_dir.to_path_buf();
    let take_rpc_leg = canonical_root
        && tokio::task::spawn_blocking(move || pplx_space_btrfs::is_subvolume(&dir))
            .await
            .unwrap_or(false);
    if take_rpc_leg {
        match crate::btrfs_via_nodemgr::delete_template_subvolume(name, hash_name, partition).await
        {
            Ok(()) => return,
            Err(error) => tracing::warn!(
                dir = %digest_dir.display(),
                %error,
                "node-manager could not delete digest dir during startup recovery; trying local cleanup"
            ),
        }
    }
    let dir = digest_dir.to_path_buf();
    let removed = tokio::task::spawn_blocking(move || crate::volume::cleanup_digest_dir_sync(&dir))
        .await
        .map_err(std::io::Error::other)
        .flatten();
    if let Err(error) = removed {
        tracing::warn!(
            dir = %digest_dir.display(),
            %error,
            "failed to remove digest dir during startup recovery"
        );
    }
}

/// One build dir recovery is reconciling.
#[derive(Clone, Copy)]
struct DigestDirBuild<'a> {
    name: &'a str,
    hash_name: &'a str,
    partition: crate::db::TemplateCachePartition,
    digest_dir: &'a Path,
}

/// How long recovery waits for sandbox-manager's list of dependent sandboxes
/// before keeping an invalid build for a later check.
const DEPENDENT_SANDBOX_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Sandbox-manager's answer on which `(template, hash)` builds sandboxes
/// depend on, asked at most once per startup recovery scan or GC tick and
/// shared by every dir that scan or tick finds invalid. An answer it couldn't
/// get, or didn't get within [`DEPENDENT_SANDBOX_CHECK_TIMEOUT`], stands for
/// the rest of the scan or tick too, so a sandbox-manager that's down or hung
/// costs it one wait rather than one per dir, each under its digest's claim.
/// A dir held during the startup scan gets its own answer once its holder
/// lets go, since that wait has no bound and a sandbox may pull the build
/// meanwhile; only such a dir found invalid asks.
#[derive(Clone)]
pub(super) struct DependentSandboxCheck {
    fetch: crate::eviction::FetchActiveFn,
    answer: Arc<tokio::sync::OnceCell<DependentSandboxAnswer>>,
}

/// The `(template, hash)` builds sandboxes depend on, or why sandbox-manager
/// couldn't say.
type DependentSandboxAnswer = Result<HashSet<(String, String)>, String>;

impl DependentSandboxCheck {
    pub(super) fn new(fetch: crate::eviction::FetchActiveFn) -> Self {
        Self {
            fetch,
            answer: Arc::default(),
        }
    }

    /// Why an invalid build of `(name, content_hash)` must stay, or `None`
    /// when nothing depends on it.
    async fn keeps(&self, name: &str, content_hash: &str) -> Option<String> {
        let answer = self
            .answer
            .get_or_init(|| async {
                match tokio::time::timeout(DEPENDENT_SANDBOX_CHECK_TIMEOUT, (self.fetch)()).await {
                    Ok(Ok(builds)) => Ok(builds),
                    Ok(Err(error)) => {
                        Err(format!("could not check for dependent sandboxes: {error}"))
                    }
                    Err(_) => Err("timed out checking for dependent sandboxes".to_string()),
                }
            })
            .await;
        match answer {
            Ok(builds) if !builds.contains(&(name.to_string(), content_hash.to_string())) => None,
            Ok(_) => Some("a sandbox still depends on its build".to_string()),
            Err(reason) => Some(reason.clone()),
        }
    }
}

async fn drop_invalid_digest_dir(
    service: &TemplateManagerService,
    build: DigestDirBuild<'_>,
    rejection: &DigestDirRejection,
    dependents: &DependentSandboxCheck,
) {
    let DigestDirBuild {
        name,
        hash_name,
        partition,
        digest_dir,
    } = build;
    let db = &service.db;
    if rejection.is_transient() {
        // Never delete an installed build over a read that can clear. Its
        // row goes, so the build isn't served unverified, and pulls refuse
        // the dir without a row until the next GC tick verifies it from
        // scratch. If the row can't be dropped, the build stays served
        // as it was, and the next startup checks it against that row.
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            cache_partition = %partition,
            dir = %digest_dir.display(),
            reason = %rejection,
            "could not verify digest dir; dropping its row and keeping it for the next GC tick"
        );
        if let Err(error) = delete_stale_build_row(db, name, hash_name, partition).await {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                %error,
                "could not drop the row of an unverified digest dir"
            );
        }
        // The node no longer serves the build; the cluster hears of it now
        // rather than at the next periodic reconcile.
        crate::node_state_sync::mark_template_dirty(db, name);
        return;
    }
    tracing::warn!(
        template = %name,
        content_hash = %hash_name,
        cache_partition = %partition,
        dir = %digest_dir.display(),
        reason = %rejection,
        "dropping invalid digest dir during startup recovery"
    );
    // Not while a sandbox depends on the build, as eviction and
    // DeleteTemplateBuild won't remove one either: a suspended sandbox's
    // restore can use it as its parent. That covers a partition mismatch
    // too, such as a legacy-layout dir bound to another environment, which
    // a sandbox suspended before partitions still names. Nor when
    // sandbox-manager can't say. Its answer names `(template, hash)`, so a
    // dependent covers every partition of the digest. The row of this dir's
    // partition still goes, so the invalid build isn't served, and the dir,
    // now without a row, is the GC tick's to check again. The answer is
    // asked once per pass; see `DependentSandboxCheck`.
    if let Some(reason) = dependents.keeps(name, hash_name).await {
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            cache_partition = %partition,
            dir = %digest_dir.display(),
            %reason,
            "keeping invalid digest dir; dropping its row and leaving it for the next GC tick"
        );
        if let Err(error) = delete_stale_build_row(db, name, hash_name, partition).await {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                %error,
                "could not drop the row of an invalid digest dir"
            );
        }
        crate::node_state_sync::mark_template_dirty(db, name);
        return;
    }
    // Its base snapshots go first: left behind, they'd be registered as the
    // snapshots of the next build installed under the same hash. If they
    // can't be removed, the dir stays too, so the next pass retries both
    // rather than leaving snapshots no build dir points back to; the row
    // still goes, so the invalid build isn't served, and the dir, now
    // without a row, is the GC tick's to retry.
    let (store, n, h) = (
        service.artifact_store.clone(),
        name.to_string(),
        hash_name.to_string(),
    );
    match off_runtime(name, hash_name, move || {
        store.remove_snapshot_artifacts(&n, &h, partition)
    })
    .await
    {
        Some(Ok(())) => {}
        removal => {
            if let Some(Err(error)) = removal {
                tracing::warn!(
                    template = %name,
                    content_hash = %hash_name,
                    %error,
                    "could not remove base snapshots of an invalid digest dir; dropping its row and keeping it for the next GC tick"
                );
            }
            if let Err(error) = delete_stale_build_row(db, name, hash_name, partition).await {
                tracing::warn!(
                    template = %name,
                    content_hash = %hash_name,
                    %error,
                    "could not drop the row of an invalid digest dir"
                );
            }
            crate::node_state_sync::mark_template_dirty(db, name);
            return;
        }
    }
    remove_digest_dir_from_disk(&service.artifact_store.root, build).await;
    if let Err(error) = delete_stale_build_row(db, name, hash_name, partition).await {
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            %error,
            "could not delete stale build row after removing invalid digest dir"
        );
    }
}

/// Run a step that reads or writes the build dir with blocking calls on the
/// blocking pool, so a read stalled on a hung disk can't take a runtime
/// worker; the GC tick runs these checks over and over, not once. `None`,
/// logged, when the step's task fails.
async fn off_runtime<T: Send + 'static>(
    name: &str,
    hash_name: &str,
    step: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    match tokio::task::spawn_blocking(step).await {
        Ok(output) => Some(output),
        Err(error) => {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                %error,
                "digest dir check task failed; leaving digest dir alone for next pass"
            );
            None
        }
    }
}

/// Reconcile a single digest dir against the local catalog.
///
/// - When `template_builds` already has a row for `(name, hash_name)`,
///   the row is treated as a validity signal. The manifest's declared
///   local files are stat'd; matching sizes leave the row untouched,
///   any mismatch causes the digest dir + row to be removed.
/// - When no row exists, the bootable artifact is hashed and the row
///   is inserted on match. Hash mismatch causes the digest dir to be
///   removed and no row to be written.
async fn reconcile_digest_dir_against_db(
    service: &TemplateManagerService,
    build: DigestDirBuild<'_>,
    dependents: &DependentSandboxCheck,
) {
    let DigestDirBuild {
        name,
        hash_name,
        partition,
        digest_dir,
    } = build;
    let dir = digest_dir.to_path_buf();
    let Some(parsed) = off_runtime(name, hash_name, move || {
        parse_manifest_from_digest_dir(&dir)
    })
    .await
    else {
        return;
    };
    let (manifest, manifest_json) = match parsed {
        Ok(parsed) => parsed,
        Err(rejection) => {
            drop_invalid_digest_dir(service, build, &rejection, dependents).await;
            return;
        }
    };

    if let Err(rejection) = validate_manifest_self_consistency(hash_name, &manifest) {
        drop_invalid_digest_dir(service, build, &rejection, dependents).await;
        return;
    }

    let (dir, declared) = (digest_dir.to_path_buf(), manifest.clone());
    let Some(local_files) = off_runtime(name, hash_name, move || {
        verify_local_files_against_manifest(&dir, &declared)
    })
    .await
    else {
        return;
    };
    if let Err(rejection) = local_files {
        drop_invalid_digest_dir(service, build, &rejection, dependents).await;
        return;
    }

    let existing_row = match lookup_build_row(&service.db, name, hash_name, partition).await {
        Ok(row) => row,
        Err(error) => {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                %error,
                "DB lookup failed during startup recovery; leaving digest dir alone for next pass"
            );
            return;
        }
    };

    if existing_row.is_some() {
        // A row keeps its build whatever metadata.json holds, so only a
        // binding the manifest carries itself is checked against the dir's
        // partition here. Registration and pulls write that binding into
        // the manifest before the row.
        if manifest.execution.is_some()
            && let Err(rejection) =
                validate_partition_binding(partition, &manifest, &TemplateMetadata::default())
        {
            drop_invalid_digest_dir(service, build, &rejection, dependents).await;
            return;
        }
        tracing::info!(
            template = %name,
            content_hash = %hash_name,
            "digest dir confirmed against existing catalog row during startup recovery"
        );
        return;
    }

    // Only a build without a row is registered from its metadata, so only
    // its metadata.json is read.
    let dir = digest_dir.to_path_buf();
    let Some(metadata) = off_runtime(name, hash_name, move || {
        parse_metadata_from_digest_dir(&dir)
    })
    .await
    else {
        return;
    };
    let metadata = match metadata {
        Ok(metadata) => metadata,
        Err(rejection) => {
            drop_invalid_digest_dir(service, build, &rejection, dependents).await;
            return;
        }
    };
    if let Err(rejection) = validate_partition_binding(partition, &manifest, &metadata) {
        drop_invalid_digest_dir(service, build, &rejection, dependents).await;
        return;
    }

    if let Err(rejection) = verify_bootable_artifact_hash(
        digest_dir,
        &manifest.bootable_artifact,
        hash_name,
        service.template_read_mode(name),
    )
    .await
    {
        drop_invalid_digest_dir(service, build, &rejection, dependents).await;
        return;
    }

    let dir = digest_dir.to_path_buf();
    let Some(normalized) =
        off_runtime(name, hash_name, move || normalize_template_cache_tree(&dir)).await
    else {
        return;
    };
    if let Err(error) = normalized {
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            %error,
            "failed to normalize digest dir permissions during startup recovery; skipping registration"
        );
        return;
    }

    let build_date = manifest.build_date.clone();
    // A pull interrupted before normalizing leaves a manifest without the
    // binding its metadata carries; the row stores it the way registration
    // would, so the store files it under the partition validated above.
    let BuildRowManifest {
        json: manifest_json,
        runtime,
    } = build_row_manifest(&manifest, manifest_json, &metadata);
    let arch = template_arch(&metadata, &manifest);
    let Some(bootable_size) = manifest
        .files
        .get(&manifest.bootable_artifact)
        .and_then(|entry| entry.size)
    else {
        // self-consistency already passed, but the size could still be
        // None for non-canonical manifests; treat as invalid.
        let rejection = DigestDirRejection::FileSizeNotDeclared {
            filename: manifest.bootable_artifact.clone(),
        };
        drop_invalid_digest_dir(service, build, &rejection, dependents).await;
        return;
    };

    let row = ValidatedBuildRow {
        name: name.to_string(),
        content_hash: hash_name.to_string(),
        partition,
        manifest_json,
        build_date,
        runtime,
        arch,
        artifact_size_bytes: bootable_size,
    };
    match insert_validated_build_row(&service.db, row).await {
        Ok(()) => {
            tracing::info!(
                template = %name,
                content_hash = %hash_name,
                rootfs_size = bootable_size,
                "digest dir validated via hashing and inserted into catalog during startup recovery"
            );
        }
        Err(error) => {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                %error,
                "catalog insert failed during startup recovery; leaving digest dir alone for next pass"
            );
        }
    }
}

fn cleanup_transient_template_name_dirs(template_root: &Path) {
    let Ok(entries) = std::fs::read_dir(template_root) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".old") {
            continue;
        }
        let Some(base) = name.strip_suffix(".old") else {
            continue;
        };
        let final_dir = template_root.join(base);
        if !final_dir.exists() {
            tracing::info!(
                dir = %entry.path().display(),
                template = %base,
                "restoring leftover .old template dir during startup recovery"
            );
            let _ = std::fs::rename(entry.path(), &final_dir);
        } else {
            tracing::info!(
                dir = %entry.path().display(),
                template = %base,
                "removing leftover .old template dir during startup recovery"
            );
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

fn cleanup_transient_digest_staging_dirs(template_root: &Path) {
    let Ok(entries) = std::fs::read_dir(template_root) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".old") {
            continue;
        }
        let Ok(sub_entries) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for sub in sub_entries.flatten() {
            let sub_name = sub.file_name().to_string_lossy().to_string();
            if is_import_staging_dir(&sub_name) || is_republish_staging_dir(&sub_name) {
                tracing::info!(
                    template = %name,
                    dir = %sub.path().display(),
                    "removing stale import or republish staging dir during startup recovery"
                );
                let _ = std::fs::remove_dir_all(sub.path());
            } else if is_build_staging_dir(&sub_name) {
                // Build staging dirs belong to build-manager, a separate
                // process whose builds keep running across template-manager
                // restarts (every deploy restarts this DaemonSet). Only reap
                // dirs old enough that no live build can still own them.
                if build_staging_dir_is_stale(&sub.path()) {
                    tracing::info!(
                        template = %name,
                        dir = %sub.path().display(),
                        "removing stale build staging dir during startup recovery"
                    );
                    let _ = std::fs::remove_dir_all(sub.path());
                }
            } else if let Some(base_name) = replaced_digest_base(&sub_name) {
                let final_path = entry.path().join(base_name);
                if final_path.exists() {
                    tracing::info!(
                        template = %name,
                        dir = %sub.path().display(),
                        "removing stale replaced digest dir during startup recovery"
                    );
                    let _ = std::fs::remove_dir_all(sub.path());
                } else {
                    tracing::info!(
                        template = %name,
                        dir = %sub.path().display(),
                        content_hash = %base_name,
                        "restoring interrupted template build replacement during startup recovery"
                    );
                    let _ = std::fs::rename(sub.path(), final_path);
                }
            }
        }
    }
}

/// Maximum age before an orphaned build-manager staging dir is reaped at
/// startup. Build jobs time out after 6 hours; anything older than this has
/// no live owner (the owning build-manager crashed without cleanup and its
/// expired-lease reaper missed the dir).
const BUILD_STAGING_DIR_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

pub(super) fn build_staging_dir_is_stale(path: &Path) -> bool {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age > BUILD_STAGING_DIR_MAX_AGE)
}

/// Register the base snapshots of every build of a template. Returns the
/// `(content_hash, partition)` builds whose snapshots didn't all register,
/// for the GC tick to retry.
async fn reconcile_base_snapshots_for_template(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    name_dir: &Path,
) -> Vec<(String, crate::db::TemplateCachePartition)> {
    let mut failed = Vec::new();
    let snapshots_dir = name_dir.join("snapshots");
    if !snapshots_dir.is_dir() {
        return failed;
    }
    let Ok(snap_hash_entries) = std::fs::read_dir(&snapshots_dir) else {
        return failed;
    };
    for snap_hash_entry in snap_hash_entries.flatten() {
        let build_dir_name = snap_hash_entry.file_name().to_string_lossy().to_string();
        let Some((snap_hash, partition)) =
            crate::db::TemplateCachePartition::parse_build_dir_name(&build_dir_name)
        else {
            continue;
        };
        if !snap_hash.starts_with("sha256:") || !snap_hash_entry.path().is_dir() {
            continue;
        }
        if let (_, Some(error)) = reconcile_base_snapshots_for_build(
            db,
            name,
            snap_hash,
            partition,
            &snap_hash_entry.path(),
        )
        .await
        {
            tracing::warn!(
                template = %name,
                content_hash = %snap_hash,
                cache_partition = %partition,
                %error,
                "could not register base snapshots during startup recovery"
            );
            failed.push((snap_hash.to_string(), partition));
        }
    }
    failed
}

/// Register the base snapshots under one build's snapshot dir
/// `{name}/snapshots/{build_dir_name}`. A snapshot row needs its build's row,
/// so this registers nothing for a build without one. Returns how many it
/// registered and the first error, if any snapshot failed to read or record.
async fn reconcile_base_snapshots_for_build(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    snap_hash: &str,
    partition: crate::db::TemplateCachePartition,
    snap_hash_dir: &Path,
) -> (usize, Option<String>) {
    let dir = snap_hash_dir.to_path_buf();
    let Some((snapshots, read_error)) =
        off_runtime(name, snap_hash, move || read_base_snapshot_dirs(&dir)).await
    else {
        return (0, Some("base snapshot read task failed".to_string()));
    };
    let mut first_error = read_error;
    let mut registered = 0;
    for (vcpu, memory_mib, snap_hash_val) in snapshots {
        let db = Arc::clone(db);
        let n = name.to_string();
        let h = snap_hash.to_string();
        let config_dir = snap_hash_dir.join(format!("{vcpu}vcpu-{memory_mib}mib"));
        let upserted = tokio::task::spawn_blocking(move || {
            let conn = lock_recovery_db(&db)?;
            upsert_snapshot_if_present(
                &conn,
                crate::db::SnapshotKey {
                    name: &n,
                    content_hash: &h,
                    cache_partition: partition,
                    vcpu,
                    memory_mib,
                },
                &snap_hash_val,
                &config_dir,
            )
        })
        .await
        .map_err(|error| format!("base snapshot upsert task failed: {error}"))
        .flatten();
        match upserted {
            Err(error) => {
                tracing::warn!(
                    template = %name,
                    content_hash = %snap_hash,
                    cache_partition = %partition,
                    vcpu,
                    memory_mib,
                    %error,
                    "failed to register base snapshot"
                );
                first_error.get_or_insert(error);
            }
            Ok(true) => registered += 1,
            Ok(false) => {}
        }
    }
    (registered, first_error)
}

/// Record a base snapshot read from `config_dir`, if its build has a row in
/// its partition and its files are still there, and say whether it did. The
/// runtime connection doesn't enforce foreign keys, so the build row is
/// checked here: a snapshot dir of a build without one, such as a dir
/// startup recovery left for a later check, gets no row until its build does.
/// Pruning removes a snapshot's dir before its row and takes no digest claim,
/// so a dir read before a prune may be gone by now; checking under the DB
/// lock keeps a row from coming back over files that went.
fn upsert_snapshot_if_present(
    conn: &rusqlite::Connection,
    key: crate::db::SnapshotKey<'_>,
    snapshot_hash: &str,
    config_dir: &Path,
) -> Result<bool, String> {
    let build = crate::db::get_build(conn, key.name, key.content_hash, key.cache_partition)
        .map_err(|error| error.to_string())?;
    if build.is_none() {
        return Ok(false);
    }
    let present = ["snapshot.base", "vmstate.base"]
        .iter()
        .all(|file| config_dir.join(file).exists());
    if !present {
        return Ok(false);
    }
    crate::db::upsert_snapshot(conn, key, snapshot_hash)
        .map(|()| true)
        .map_err(|error| error.to_string())
}

/// The finished base snapshots under `{name}/snapshots/{build_dir_name}` that
/// read, as `(vcpu, memory_mib, snapshot_hash)`, and the first read error, if
/// any. A dir that's missing has none; a `snapshot_hash` that's missing is
/// empty, as startup recovery has always read it. A config that fails to read
/// is left out and its error returned, so the caller retries it, while the
/// configs that read are still registered.
fn read_base_snapshot_dirs(dir: &Path) -> (Vec<(u32, u32, String)>, Option<String>) {
    let read_error =
        |path: &Path, error: std::io::Error| format!("read {}: {error}", path.display());
    let config_entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return (Vec::new(), None),
        Err(error) => return (Vec::new(), Some(read_error(dir, error))),
    };
    let mut snapshots = Vec::new();
    let mut first_error = None;
    for config_entry in config_entries {
        let config_entry = match config_entry {
            Ok(entry) => entry,
            Err(error) => {
                first_error.get_or_insert(read_error(dir, error));
                continue;
            }
        };
        let config_name = config_entry.file_name().to_string_lossy().to_string();
        let Some((vcpu, memory_mib)) = parse_snapshot_config_dir(&config_name) else {
            continue;
        };
        let config_dir = config_entry.path();
        let read_config = || -> Result<Option<String>, String> {
            for file in ["snapshot.base", "vmstate.base"] {
                let path = config_dir.join(file);
                if !path
                    .try_exists()
                    .map_err(|error| read_error(&path, error))?
                {
                    return Ok(None);
                }
            }
            let hash_path = config_dir.join("snapshot_hash");
            match std::fs::read_to_string(&hash_path) {
                Ok(value) => Ok(Some(value)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(Some(String::new()))
                }
                Err(error) => Err(read_error(&hash_path, error)),
            }
        };
        match read_config() {
            Ok(Some(snap_hash_val)) => snapshots.push((vcpu, memory_mib, snap_hash_val)),
            Ok(None) => {}
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    (snapshots, first_error)
}

async fn prune_catalog_rows_missing_on_disk(
    db: &Arc<Mutex<rusqlite::Connection>>,
    artifact_store: &TemplateArtifactStore,
) {
    let db = Arc::clone(db);
    let artifact_store = artifact_store.clone();
    match tokio::task::spawn_blocking(move || {
        let Ok(conn) = db.lock() else {
            tracing::warn!("template DB mutex poisoned while pruning stale startup recovery rows");
            return;
        };

        // Resolve any build that was mid-eviction when tmplmgr died.
        // Phase 1 of `eviction::execute_one_async` flips the row to
        // `state='deleting'` and commits before the async disk
        // delete starts; if we crashed between that commit and the
        // Phase 3 finalize, the row is stuck in `deleting` state.
        // The disk may be intact (Phase 2 hadn't started) or gone
        // (Phase 2 finished but Phase 3 didn't). Decide by
        // inspecting the artifact:
        //   - artifacts present → flip back to `ready` so the
        //     build is usable again.
        //   - artifacts missing → delete the row outright; a partial
        //     dir left on disk has no row, so the GC tick's rowless
        //     check removes it.
        let stuck = crate::db::list_deleting_builds(&conn).unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to enumerate stuck 'deleting' template builds");
            Vec::new()
        });
        for (name, content_hash, partition) in &stuck {
            if artifact_store.build_artifacts_missing(name, content_hash, *partition) {
                tracing::info!(
                    template = %name,
                    content_hash = %content_hash,
                    cache_partition = %partition,
                    "stuck 'deleting' template build: artifacts missing, dropping row"
                );
                let _ = crate::db::delete_build(&conn, name, content_hash, *partition);
            } else {
                tracing::info!(
                    template = %name,
                    content_hash = %content_hash,
                    "stuck 'deleting' template build: artifacts intact, restoring to 'ready'"
                );
                let _ = conn.execute(
                    "UPDATE template_builds SET state = 'ready'
                     WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3
                       AND state = 'deleting'",
                    rusqlite::params![name, content_hash, partition.key()],
                );
            }
        }

        let builds = crate::db::list_builds(&conn).unwrap_or_default();
        for build in &builds {
            if artifact_store.build_artifacts_missing(
                &build.name,
                &build.content_hash,
                build.cache_partition,
            ) {
                tracing::warn!(
                    template = %build.name,
                    content_hash = %build.content_hash,
                    cache_partition = %build.cache_partition,
                    "pruning catalog build row whose artifacts are missing on disk"
                );
                let _ = crate::db::delete_build(
                    &conn,
                    &build.name,
                    &build.content_hash,
                    build.cache_partition,
                );
            }
        }

        let remaining_builds = crate::db::list_builds(&conn).unwrap_or_default();
        for build in &remaining_builds {
            let snaps = crate::db::list_snapshots(
                &conn,
                &build.name,
                &build.content_hash,
                build.cache_partition,
            )
            .unwrap_or_default();
            for snap in &snaps {
                if artifact_store.snapshot_artifacts_missing(
                    &snap.name,
                    &snap.content_hash,
                    snap.cache_partition,
                    snap.vcpu,
                    snap.memory_mib,
                ) {
                    tracing::warn!(
                        template = %snap.name,
                        content_hash = %snap.content_hash,
                        vcpu = snap.vcpu,
                        memory_mib = snap.memory_mib,
                        "pruning catalog snapshot row whose artifacts are missing on disk"
                    );
                    let _ = crate::db::delete_snapshot(
                        &conn,
                        crate::db::SnapshotKey {
                            name: &snap.name,
                            content_hash: &snap.content_hash,
                            cache_partition: snap.cache_partition,
                            vcpu: snap.vcpu,
                            memory_mib: snap.memory_mib,
                        },
                    );
                }
            }
        }

        drop(conn);
        match crate::pinned_digests::PinnedDigestsStore::new(Arc::clone(&db))
            .prune_without_local_builds()
        {
            Ok(pruned) if pruned > 0 => {
                crate::metrics::digest_pins_pruned(pruned as u64);
                tracing::info!(pruned, "pruned template digest pins without local builds");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to prune template digest pins without local builds"
                );
            }
        }
    })
    .await
    {
        Ok(()) => {}
        Err(error) => {
            tracing::warn!(
                %error,
                "stale startup recovery row pruning task failed"
            );
        }
    }
}

/// Whether a dir directly under the template root is a template's dir, not
/// a hidden or transient one.
fn is_template_name_dir(name: &str) -> bool {
    !(name.starts_with('.') || name.ends_with(".old") || name.ends_with(".importing"))
}

/// The content hash and cache partition of a build dir under a template's
/// dir, `{hash}` or `{hash}@{environment}`; `None` for import or republish
/// staging, an interrupted replacement, or anything else.
fn parse_build_dir_name(dir_name: &str) -> Option<(&str, crate::db::TemplateCachePartition)> {
    if is_import_staging_dir(dir_name)
        || is_republish_staging_dir(dir_name)
        || replaced_digest_base(dir_name).is_some()
    {
        return None;
    }
    crate::db::TemplateCachePartition::parse_build_dir_name(dir_name)
}

/// Build dirs `{template}/{build dir}` under `root` that have no row in their
/// partition and no claim of their digest, as `(template, build dir name,
/// dir)` in name order. The scan reads the disk without the DB lock, then
/// filters under it. A row lookup that fails counts as a row, so the dir
/// waits for a later tick.
fn rowless_build_dirs(
    root: &Path,
    db: &Mutex<rusqlite::Connection>,
    puller: &crate::volume::VolumePuller,
) -> Vec<(String, String, PathBuf)> {
    let mut dirs = Vec::new();
    let templates = match std::fs::read_dir(root) {
        Ok(templates) => templates,
        Err(error) => {
            tracing::warn!(
                dir = %root.display(),
                %error,
                "could not list the template root for build dirs without a row"
            );
            return Vec::new();
        }
    };
    for template in templates.flatten() {
        let name = template.file_name().to_string_lossy().to_string();
        if !is_template_name_dir(&name) || !template.path().is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(template.path()) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    template = %name,
                    %error,
                    "could not list a template's build dirs to find ones without a row"
                );
                continue;
            }
        };
        for entry in entries.flatten() {
            let dir_name = entry.file_name().to_string_lossy().to_string();
            if parse_build_dir_name(&dir_name).is_some() && entry.path().is_dir() {
                dirs.push((name.clone(), dir_name, entry.path()));
            }
        }
    }
    dirs.sort();
    let Ok(conn) = db.lock() else {
        tracing::warn!("template DB mutex poisoned; skipping build dirs without a row");
        return Vec::new();
    };
    dirs.into_iter()
        .filter(|(name, dir_name, _)| {
            parse_build_dir_name(dir_name).is_some_and(|(hash, partition)| {
                !puller.is_claimed(name, hash)
                    && crate::db::get_build(&conn, name, hash, partition)
                        .is_ok_and(|row| row.is_none())
            })
        })
        .collect()
}

impl TemplateManagerService {
    /// Prune `template_builds` / `template_snapshots` rows whose on-disk
    /// artifacts are missing. Exposed so tests can drive the recovery
    /// cleanup path without running the full recovery sweep, and so the
    /// delete-template crash window has a single named hook.
    pub(super) async fn prune_stale_template_artifact_db_rows(&self) {
        {
            // Its stuck-`deleting` repair must not meet a live eviction.
            let _evicting = self.eviction_gate.hold().await;
            prune_catalog_rows_missing_on_disk(&self.db, &self.artifact_store).await;
        }
    }

    /// Run startup recovery's check, from the GC tick, on up to `limit` build
    /// dirs under the template root that have no row. A valid one gets its
    /// row back, a permanently invalid one is removed, and one whose read
    /// fails in a way that can clear stays for a later tick. Pulls refuse a
    /// build dir without a row, so this bounds how long one blocks its build.
    /// Dirs a pull or import has in flight are skipped: they have no row
    /// until that work finishes.
    ///
    /// The dirs are taken in `(template, build dir)` order from just after
    /// the last one the previous tick examined, wrapping around, so every one
    /// is reached within `ceil(N / limit)` ticks however many keep failing,
    /// as long as checks and snapshot repairs finish within `wait`; the
    /// repairs share the checks' permits and run first, and while checks or
    /// repairs parked past their tick hold every permit, no tick starts a
    /// check. Returns how many
    /// checks it started that verified their dir or were left running past
    /// `wait`; one that found the digest claimed, its row back or the dir
    /// gone, or whose task failed, isn't counted.
    ///
    /// The tick waits up to `wait` for each check. One that takes longer, as
    /// a read of a hung disk can, keeps running in its own task under its
    /// claim, so the next tick's scan skips it, and the tick moves on to
    /// eviction rather than stalling behind it. The scan for candidates
    /// isn't bounded: it lists the same template dirs eviction's own
    /// planning reads, so a disk that stalls it stalls eviction anyway.
    pub(super) async fn reconcile_rowless_build_dirs(
        &self,
        limit: usize,
        wait: std::time::Duration,
    ) -> usize {
        self.retry_snapshot_repairs(limit, wait).await;
        let root = self.artifact_store.root.clone();
        let cursor = self
            .rowless_cursor
            .lock()
            .map(|cursor| cursor.clone())
            .unwrap_or_default();
        let db = Arc::clone(&self.db);
        let puller = self.volume_puller.clone();
        let scan = tokio::task::spawn_blocking(move || {
            let rowless = rowless_build_dirs(&root, &db, &puller);
            // Start after the cursor and wrap around.
            let start = cursor.as_ref().map_or(0, |cursor| {
                rowless
                    .iter()
                    .position(|(name, dir_name, _)| (name, dir_name) > (&cursor.0, &cursor.1))
                    .unwrap_or(0)
            });
            rowless
                .iter()
                .cycle()
                .skip(start)
                .take(limit.min(rowless.len()))
                .cloned()
                .collect::<Vec<_>>()
        });
        let candidates = scan.await.unwrap_or_else(|error| {
            tracing::warn!(%error, "scan for build dirs without a row failed");
            Vec::new()
        });

        let mut checked = 0;
        let dependents = self.dependent_sandbox_check();
        for (name, dir_name, digest_dir) in candidates {
            let Some((content_hash, partition)) = parse_build_dir_name(&dir_name)
                .map(|(content_hash, partition)| (content_hash.to_string(), partition))
            else {
                continue;
            };
            // A check that outlives its tick keeps its permit, so checks
            // parked on a hung disk can't pile up across ticks: while they
            // hold every permit, no tick starts another.
            let Ok(permit) = Arc::clone(&self.rowless_checks).try_acquire_owned() else {
                tracing::warn!(
                    "earlier checks of build dirs without a row are still running; \
                     skipping the rest of this tick's"
                );
                break;
            };
            // The cursor passes only dirs a check started on, so one skipped
            // for want of a permit comes first next tick.
            if let Ok(mut cursor) = self.rowless_cursor.lock() {
                *cursor = Some((name.clone(), dir_name.clone()));
            }
            // Its own task, so a tick that shutdown cancels can't drop the
            // digest claim while a blocking removal or catalog write it
            // guards keeps running; the task lets go once those finish.
            let (service, dependents) = (self.clone(), dependents.clone());
            let verify = tokio::spawn(async move {
                let _permit = permit;
                service
                    .verify_rowless_build_dir(
                        &name,
                        &content_hash,
                        partition,
                        &digest_dir,
                        &dependents,
                    )
                    .await
            });
            match tokio::time::timeout(wait, verify).await {
                Ok(Ok(verified)) => checked += usize::from(verified),
                Ok(Err(error)) => {
                    tracing::warn!(%error, "a check of a build dir without a row failed");
                }
                Err(_) => {
                    tracing::warn!(
                        ?wait,
                        "a check of a build dir without a row is still running; \
                         the tick moves on and leaves it to finish under its claim"
                    );
                    checked += 1;
                }
            }
        }
        checked
    }

    /// Run startup recovery's check on one build dir the scan found without
    /// a row, under its digest claim. Returns false, and touches nothing,
    /// when someone else holds the claim, or, by the time the claim is taken,
    /// the build has a row or the dir is gone: the scan ran before the claim,
    /// and a registration or import may have finished the build since, or a
    /// delete removed it.
    pub(super) async fn verify_rowless_build_dir(
        &self,
        name: &str,
        content_hash: &str,
        partition: crate::db::TemplateCachePartition,
        digest_dir: &Path,
        dependents: &DependentSandboxCheck,
    ) -> bool {
        // Held for the whole check, so no pull, import, registration or
        // DeleteTemplateBuild of the digest, in any partition, runs
        // alongside it.
        let Some(_claim) = self.volume_puller.try_claim_digest(name, content_hash) else {
            return false;
        };
        self.verify_claimed_rowless_build_dir(name, content_hash, partition, digest_dir, dependents)
            .await
    }

    /// Register the base snapshots of a build whose row the GC tick
    /// restored, read from `{name}/snapshots/{build_dir_name}` of its
    /// partition. On failure the build stays served, without those
    /// snapshots, and goes on the pending list each tick retries. Returns how
    /// many snapshots it registered.
    async fn register_restored_base_snapshots(
        &self,
        name: &str,
        content_hash: &str,
        partition: crate::db::TemplateCachePartition,
    ) -> usize {
        let dir =
            build_snapshots_artifacts_dir(&self.artifact_store.root, name, content_hash, partition);
        let (registered, error) =
            reconcile_base_snapshots_for_build(&self.db, name, content_hash, partition, &dir).await;
        let Ok(mut pending) = self.snapshot_repairs.lock() else {
            return registered;
        };
        let key = (name.to_string(), content_hash.to_string(), partition);
        if let Some(error) = error {
            tracing::warn!(
                template = %name,
                content_hash = %content_hash,
                cache_partition = %partition,
                %error,
                "could not register the base snapshots of a restored build; retrying next tick"
            );
            pending.insert(key);
        } else {
            pending.remove(&key);
        }
        registered
    }

    /// Retry registering the base snapshots of restored builds whose earlier
    /// attempt failed, up to `limit` a tick, taken in order from just after
    /// the last one tried so one that keeps failing doesn't hold the rest
    /// back, and waiting up to `wait` for each. A build whose row in its
    /// partition has gone since leaves the list; one whose digest someone
    /// else holds waits for a later tick.
    async fn retry_snapshot_repairs(&self, limit: usize, wait: std::time::Duration) {
        let all: Vec<_> = self
            .snapshot_repairs
            .lock()
            .map(|pending| pending.iter().cloned().collect())
            .unwrap_or_default();
        let cursor = self
            .snapshot_repair_cursor
            .lock()
            .map(|cursor| cursor.clone())
            .unwrap_or_default();
        let start = cursor.as_ref().map_or(0, |cursor| {
            all.iter().position(|key| key > cursor).unwrap_or(0)
        });
        let pending: Vec<_> = all
            .iter()
            .cycle()
            .skip(start)
            .take(limit.min(all.len()))
            .cloned()
            .collect();
        for (name, content_hash, partition) in pending {
            // Like a check, a repair runs in its own task holding a permit
            // and the digest's claim until it ends, so a repair parked on a
            // hung disk outlives its tick without piling up behind it.
            let Ok(permit) = Arc::clone(&self.rowless_checks).try_acquire_owned() else {
                return;
            };
            if let Ok(mut cursor) = self.snapshot_repair_cursor.lock() {
                *cursor = Some((name.clone(), content_hash.clone(), partition));
            }
            let Some(claim) = self.volume_puller.try_claim_digest(&name, &content_hash) else {
                continue;
            };
            let service = self.clone();
            let (task_name, task_hash) = (name.clone(), content_hash.clone());
            let repair = tokio::spawn(async move {
                let (_permit, _claim) = (permit, claim);
                match lookup_build_row(&service.db, &task_name, &task_hash, partition).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        if let Ok(mut pending) = service.snapshot_repairs.lock() {
                            pending.remove(&(task_name, task_hash, partition));
                        }
                        return;
                    }
                    Err(_) => return,
                }
                if service
                    .register_restored_base_snapshots(&task_name, &task_hash, partition)
                    .await
                    > 0
                {
                    // The node can now serve the build from those snapshots,
                    // even if others still fail; the cluster hears of it now.
                    crate::node_state_sync::mark_template_dirty(&service.db, &task_name);
                }
            });
            if tokio::time::timeout(wait, repair).await.is_err() {
                tracing::warn!(
                    template = %name,
                    content_hash = %content_hash,
                    cache_partition = %partition,
                    ?wait,
                    "base snapshot repair still running; it finishes under its claim"
                );
            }
        }
    }

    /// [`Self::verify_rowless_build_dir`] once the claim is held.
    async fn verify_claimed_rowless_build_dir(
        &self,
        name: &str,
        content_hash: &str,
        partition: crate::db::TemplateCachePartition,
        digest_dir: &Path,
        dependents: &DependentSandboxCheck,
    ) -> bool {
        if !lookup_build_row(&self.db, name, content_hash, partition)
            .await
            .is_ok_and(|row| row.is_none())
        {
            return false;
        }
        // A delete may also have removed the dir since the scan.
        let dir = digest_dir.to_path_buf();
        if !off_runtime(name, content_hash, move || dir.is_dir())
            .await
            .unwrap_or(false)
        {
            return false;
        }
        tracing::info!(
            template = %name,
            content_hash = %content_hash,
            cache_partition = %partition,
            dir = %digest_dir.display(),
            "verifying a build dir without a row"
        );
        reconcile_digest_dir_against_db(
            self,
            DigestDirBuild {
                name,
                hash_name: content_hash,
                partition,
                digest_dir,
            },
            dependents,
        )
        .await;
        // Dropping the row dropped its base-snapshot rows too, and they
        // can't come back without it; startup recovery only registers
        // snapshot dirs once. Register them with the restored row; a read or
        // write that fails is retried on later ticks, since the build, now
        // with a row, no longer comes up as rowless.
        if lookup_build_row(&self.db, name, content_hash, partition)
            .await
            .is_ok_and(|row| row.is_some())
        {
            self.register_restored_base_snapshots(name, content_hash, partition)
                .await;
        }
        // A restored row, or a removed dir, changes what the node can
        // serve; the cluster hears of it now rather than at the next
        // periodic reconcile.
        crate::node_state_sync::mark_template_dirty(&self.db, name);
        true
    }

    /// Spawn startup recovery once the socket is bound. Requests are served
    /// while it runs; only a request for the build recovery is checking at
    /// that moment waits for that check.
    ///
    /// Heavy validation work (hashing unrecognized digest dirs, deleting
    /// invalid ones) runs off the service startup path so local dependents
    /// can connect and surface backend-specific errors while recovery
    /// catches up.
    ///
    /// `shutdown` is propagated to subsystems that already accept
    /// cooperative shutdown (currently: the eviction loop). The
    /// prefetch loop and dirty-outbox / network-policy syncers
    /// don't yet take a token — wiring that through is tracked
    /// separately.
    pub fn spawn_startup_recovery(&self, shutdown: tokio_util::sync::CancellationToken) {
        self.spawn_prefetch_loop();
        self.spawn_node_state_dirty_syncer();
        self.spawn_network_policy_notification_syncer();
        // Budgeted by `SPACE_TEMPLATE_CACHE_BUDGET_BYTES`; an explicit
        // zero turns eviction off and leaves only the rowless-dir check.
        self.spawn_eviction_loop(shutdown);

        let service = self.clone();
        tokio::spawn(async move {
            tracing::info!("template-manager startup recovery started");
            service.run_startup_recovery().await;
            tracing::info!("template-manager startup recovery finished");

            service.spawn_startup_base_snapshot_reconcile();
        });
    }

    /// Reap the transient dirs a previous process left: `.old` template
    /// dirs, import, partition-probe and republish staging, stale build
    /// staging, and interrupted replacements. Call before the socket binds,
    /// while no pull, import or registration can own one; a live pull's
    /// probe or import's staging dir carries the same names, so reaping them
    /// once requests flow would race it.
    pub fn clean_transient_dirs_before_serving(&self) {
        cleanup_transient_template_name_dirs(&self.artifact_store.root);
        cleanup_transient_digest_staging_dirs(&self.artifact_store.root);
    }

    /// Reconcile DB with disk and recover background loops needed after
    /// startup. Runs once requests flow, so it leaves transient dirs alone;
    /// [`Self::clean_transient_dirs_before_serving`] reaped the stale ones.
    pub async fn run_startup_recovery(&self) {
        let mut held_at_scan = Vec::new();
        let dependents = self.dependent_sandbox_check();
        if let Ok(entries) = std::fs::read_dir(&self.artifact_store.root) {
            for entry in entries.flatten() {
                if !entry.path().is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if !is_template_name_dir(&name) {
                    continue;
                }

                let name_dir = entry.path();
                self.normalize_template_name_dir_once(&name, &name_dir);

                if let Ok(digest_entries) = std::fs::read_dir(&name_dir) {
                    for digest_entry in digest_entries.flatten() {
                        let hash_name = digest_entry.file_name().to_string_lossy().to_string();
                        if hash_name == "runtime-rootfs" {
                            tracing::info!(
                                template = %name,
                                dir = %digest_entry.path().display(),
                                "removing obsolete runtime-rootfs cache dir during startup recovery"
                            );
                            let _ = std::fs::remove_dir_all(digest_entry.path());
                            continue;
                        }
                        let Some((content_hash, partition)) = parse_build_dir_name(&hash_name)
                        else {
                            continue;
                        };
                        if !digest_entry.path().is_dir() {
                            continue;
                        }
                        let content_hash = content_hash.to_string();
                        // Requests flow by now: a digest a pull, import,
                        // registration or delete holds isn't verified
                        // alongside them. It's checked once they let go,
                        // after the scan.
                        let digest_dir = digest_entry.path();
                        let Some(_claim) =
                            self.volume_puller.try_claim_digest(&name, &content_hash)
                        else {
                            held_at_scan.push((name.clone(), content_hash, partition, digest_dir));
                            continue;
                        };
                        reconcile_digest_dir_against_db(
                            self,
                            DigestDirBuild {
                                name: &name,
                                hash_name: &content_hash,
                                partition,
                                digest_dir: &digest_dir,
                            },
                            &dependents,
                        )
                        .await;
                    }
                }

                let failed =
                    reconcile_base_snapshots_for_template(&self.db, &name, &name_dir).await;
                // The GC tick retries them; one whose build has no row drops
                // off its list.
                if !failed.is_empty()
                    && let Ok(mut pending) = self.snapshot_repairs.lock()
                {
                    pending.extend(
                        failed
                            .into_iter()
                            .map(|(hash, partition)| (name.clone(), hash, partition)),
                    );
                }
            }
        }

        self.prune_stale_template_artifact_db_rows().await;

        {
            match self.pool_handle.lock() {
                Ok(mut handle) if handle.is_none() => {
                    let db = Arc::clone(&self.db);
                    *handle = Some(tokio::spawn(crate::pool_manager::pool_loop(db)));
                    tracing::debug!("pool manager loop started");
                }
                Ok(_) => {}
                Err(_) => tracing::warn!(
                    "pool manager handle mutex poisoned; startup recovery cannot spawn pool loop"
                ),
            }
        }

        // A pull refuses a dir without a row while holding its digest, so a
        // dir held during the scan may be one only this pass can verify:
        // wait for each holder, then check the dir if it's still there.
        for (name, hash_name, partition, digest_dir) in held_at_scan {
            let _claim = self.volume_puller.claim_digest(&name, &hash_name).await;
            if digest_dir.is_dir() {
                // The wait has no bound, so the scan's answer may predate a
                // sandbox that pulled the build meanwhile: ask again under
                // the claim, which only a dir found invalid spends.
                let dependents = self.dependent_sandbox_check();
                reconcile_digest_dir_against_db(
                    self,
                    DigestDirBuild {
                        name: &name,
                        hash_name: &hash_name,
                        partition,
                        digest_dir: &digest_dir,
                    },
                    &dependents,
                )
                .await;
                // The scan registered no snapshots for a build that had no
                // row then; register them now that it has one, as the GC
                // tick does for a build it restores.
                if lookup_build_row(&self.db, &name, &hash_name, partition)
                    .await
                    .is_ok_and(|row| row.is_some())
                {
                    self.register_restored_base_snapshots(&name, &hash_name, partition)
                        .await;
                }
            }
        }
    }

    /// A fresh [`DependentSandboxCheck`], for one recovery pass or GC tick.
    pub(super) fn dependent_sandbox_check(&self) -> DependentSandboxCheck {
        DependentSandboxCheck::new(Arc::clone(&self.dependent_sandboxes))
    }
}

#[cfg(test)]
mod tests;
