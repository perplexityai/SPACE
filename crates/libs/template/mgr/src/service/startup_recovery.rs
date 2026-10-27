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

use super::*;

use pplx_space_common_core::types::{TemplateManifest, TemplateMetadata};

fn lock_recovery_db<'a>(
    db: &'a Mutex<rusqlite::Connection>,
) -> Result<std::sync::MutexGuard<'a, rusqlite::Connection>, String> {
    db.lock()
        .map_err(|_| "template DB mutex poisoned".to_string())
}

/// Reasons a digest dir fails reconciliation and should be removed.
///
/// Transient errors (DB unavailable, I/O hiccup during stat) are not
/// represented here; those are handled by leaving the digest dir alone
/// for the next reconcile pass.
#[derive(Debug, thiserror::Error)]
enum DigestDirRejection {
    #[error("manifest.json read failed: {0}")]
    ManifestUnreadable(String),
    #[error("manifest.json parse failed: {0}")]
    ManifestUnparseable(String),
    #[error("manifest filename {filename:?} is unsafe: {reason}")]
    ManifestFilenamesUnsafe {
        filename: String,
        reason: &'static str,
    },
    #[error("manifest content_hash {found:?} does not match digest dir name {expected:?}")]
    ContentHashDisagreesWithDirName { found: String, expected: String },
    #[error("manifest binds the {found} cache partition but the digest dir holds {expected}")]
    PartitionDisagreesWithDirName { found: String, expected: String },
    #[error("manifest execution metadata is invalid: {0}")]
    ExecutionMetadataInvalid(String),
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
    StatFailed { filename: String, error: String },
    #[error("could not hash bootable artifact: {0}")]
    BootableArtifactUnreadable(String),
    #[error("bootable artifact sha256 {computed:?} does not match digest dir name {expected:?}")]
    BootableArtifactHashMismatch { computed: String, expected: String },
}

fn parse_manifest_from_digest_dir(
    digest_dir: &Path,
) -> Result<(TemplateManifest, String), DigestDirRejection> {
    let manifest_path = digest_dir.join("manifest.json");
    let manifest_json = std::fs::read_to_string(&manifest_path)
        .map_err(|error| DigestDirRejection::ManifestUnreadable(error.to_string()))?;
    let manifest: TemplateManifest = serde_json::from_str(&manifest_json)
        .map_err(|error| DigestDirRejection::ManifestUnparseable(error.to_string()))?;
    // The on-disk manifest is whatever the last writer left, which in
    // crash-recovery scenarios may be the raw object from an interrupted
    // pull. Sanitize filenames before they reach `Path::join` — without
    // this, `bootable_artifact: "../escape"` or an absolute path slips
    // straight into `fs::metadata`. Matches the defense `volume.rs`
    // applies on its recovery path.
    manifest
        .validate_filenames()
        .map_err(|error| DigestDirRejection::ManifestFilenamesUnsafe {
            filename: error.name,
            reason: error.reason,
        })?;
    Ok((manifest, manifest_json))
}

fn parse_metadata_from_digest_dir(digest_dir: &Path) -> TemplateMetadata {
    // Metadata is informational here (arch fallback); failures degrade
    // gracefully to the manifest-derived defaults.
    let metadata_json =
        std::fs::read_to_string(digest_dir.join("metadata.json")).unwrap_or_default();
    serde_json::from_str(&metadata_json).unwrap_or_default()
}

/// Confirm manifest invariants that don't require I/O: content_hash
/// matches the dir name, the manifest binds the dir's partition, and the
/// bootable artifact entry is present and pinned to the dir name.
///
/// A node that predates partitions can hold a build bound to another
/// environment in the legacy layout. It's dropped like any other mismatch and
/// re-pulled into its own partition on demand, since every reader derives a
/// build's partition from its binding. Its sandboxes keep running; suspend
/// falls back to the Full codec, as when eviction removes a parent build.
fn validate_manifest_self_consistency(
    hash_name: &str,
    partition: crate::db::TemplateCachePartition,
    manifest: &TemplateManifest,
    metadata: &TemplateMetadata,
) -> Result<(), DigestDirRejection> {
    let bound = match crate::service::effective_environment(manifest, metadata) {
        // Without the initial environment's reference, an E0 build can't be
        // told from another environment's, so no dir is dropped for it.
        _ if pplx_space_node_runtime_core::catalog::initial_environment_reference().is_none() => {
            None
        }
        Ok(environment) => {
            Some(pplx_space_node_runtime_core::catalog::template_cache_partition(environment))
        }
        // Unreadable metadata in a legacy dir keeps its pre-partition handling:
        // the row upsert refuses it and the dir stays.
        Err(_) if partition == crate::db::TemplateCachePartition::Legacy => None,
        Err(error) => return Err(DigestDirRejection::ExecutionMetadataInvalid(error)),
    };
    if let Some(bound) = bound
        && bound != partition
    {
        return Err(DigestDirRejection::PartitionDisagreesWithDirName {
            found: bound.to_string(),
            expected: partition.to_string(),
        });
    }
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
) -> Result<(), DigestDirRejection> {
    let path = digest_dir.join(bootable_filename);
    let path_str = path.display().to_string();
    let algorithm = content_hash_algorithm(expected_hash)
        .map_err(|error| DigestDirRejection::BootableArtifactUnreadable(error.to_string()))?;
    let computed = tokio::task::spawn_blocking(move || compute_content_hash(&path_str, algorithm))
        .await
        .map_err(|error| {
            DigestDirRejection::BootableArtifactUnreadable(format!("hash task failed: {error}"))
        })?
        .map_err(|error| DigestDirRejection::BootableArtifactUnreadable(error.to_string()))?;
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

/// Remove an invalid build dir, reporting whether it's gone. Installed
/// builds are read-only btrfs subvolumes that only node-manager can delete;
/// the local subvolume-aware cleanup is the fallback, as on the pull path.
async fn remove_digest_dir_from_disk(build: DigestDirBuild<'_>) -> bool {
    let digest_dir = build.digest_dir;
    let dir = digest_dir.to_path_buf();
    let is_subvolume = tokio::task::spawn_blocking(move || {
        pplx_space_common_core::host::btrfs::is_subvolume(&dir)
    })
    .await
    .unwrap_or(false);
    if is_subvolume {
        match crate::btrfs_via_nodemgr::delete_template_subvolume(
            build.name,
            build.hash_name,
            build.partition,
        )
        .await
        {
            Ok(()) => return true,
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
    match removed {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                dir = %digest_dir.display(),
                %error,
                "failed to remove digest dir during startup recovery"
            );
            false
        }
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

async fn drop_invalid_digest_dir(
    service: &TemplateManagerService,
    build: DigestDirBuild<'_>,
    rejection: &DigestDirRejection,
) {
    let DigestDirBuild {
        name,
        hash_name,
        partition,
        digest_dir,
    } = build;
    tracing::warn!(
        template = %name,
        content_hash = %hash_name,
        cache_partition = %partition,
        dir = %digest_dir.display(),
        reason = %rejection,
        "dropping invalid digest dir during startup recovery"
    );
    // A build in the wrong partition never becomes valid there, so its base
    // snapshots go too, first: startup re-registers any snapshot dir it finds
    // under the dir's partition. Until both are gone its rootfs is intact, so
    // it keeps serving cold boots and a later startup retries.
    // Other rejections can be transient and keep their snapshots.
    if matches!(
        rejection,
        DigestDirRejection::PartitionDisagreesWithDirName { .. }
    ) {
        let store = service.artifact_store.clone();
        let (n, h) = (name.to_string(), hash_name.to_string());
        let snapshots_removed =
            tokio::task::spawn_blocking(move || store.remove_snapshot_artifacts(&n, &h, partition))
                .await;
        if !matches!(snapshots_removed, Ok(Ok(()))) {
            tracing::warn!(
                template = %name,
                content_hash = %hash_name,
                "could not remove base snapshots of a build in the wrong cache partition; keeping it"
            );
            return;
        }
        if !remove_digest_dir_from_disk(build).await {
            return;
        }
    } else {
        remove_digest_dir_from_disk(build).await;
    }
    if let Err(error) = delete_stale_build_row(&service.db, name, hash_name, partition).await {
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            %error,
            "could not delete stale build row after removing invalid digest dir"
        );
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
) {
    let DigestDirBuild {
        name,
        hash_name,
        partition,
        digest_dir,
    } = build;
    let (manifest, manifest_json) = match parse_manifest_from_digest_dir(digest_dir) {
        Ok(parsed) => parsed,
        Err(rejection) => {
            drop_invalid_digest_dir(service, build, &rejection).await;
            return;
        }
    };

    let metadata = parse_metadata_from_digest_dir(digest_dir);
    if let Err(rejection) =
        validate_manifest_self_consistency(hash_name, partition, &manifest, &metadata)
    {
        drop_invalid_digest_dir(service, build, &rejection).await;
        return;
    }

    if let Err(rejection) = verify_local_files_against_manifest(digest_dir, &manifest) {
        drop_invalid_digest_dir(service, build, &rejection).await;
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
        tracing::info!(
            template = %name,
            content_hash = %hash_name,
            "digest dir confirmed against existing catalog row during startup recovery"
        );
        return;
    }

    if let Err(rejection) =
        verify_bootable_artifact_hash(digest_dir, &manifest.bootable_artifact, hash_name).await
    {
        drop_invalid_digest_dir(service, build, &rejection).await;
        return;
    }

    if let Err(error) = normalize_template_cache_tree(digest_dir) {
        tracing::warn!(
            template = %name,
            content_hash = %hash_name,
            %error,
            "failed to normalize digest dir permissions during startup recovery; skipping registration"
        );
        return;
    }

    let build_date = manifest.build_date.clone();
    let runtime = if manifest.runtime.is_empty() {
        RUNTIME_FIRECRACKER_ALIAS.to_string()
    } else {
        manifest.runtime.clone()
    };
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
        drop_invalid_digest_dir(service, build, &rejection).await;
        return;
    };

    // A pull interrupted before normalizing leaves a manifest without the
    // binding its metadata carries; the row stores it the way registration
    // would, so the store files it under the partition validated above.
    let manifest_json = match metadata.execution_metadata() {
        Ok(Some(execution)) if manifest.execution.is_none() => {
            let mut bound = manifest.clone();
            bound.set_execution_metadata(execution.clone());
            serde_json::to_string(&bound).unwrap_or(manifest_json)
        }
        _ => manifest_json,
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

fn cleanup_transient_template_name_dirs() {
    let Ok(entries) = std::fs::read_dir(TEMPLATE_DIR) else {
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
        let final_dir = format!("{TEMPLATE_DIR}/{base}");
        if !Path::new(&final_dir).exists() {
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

fn cleanup_transient_digest_staging_dirs() {
    let Ok(entries) = std::fs::read_dir(TEMPLATE_DIR) else {
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

async fn reconcile_base_snapshots_for_template(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    name_dir: &Path,
) {
    let snapshots_dir = name_dir.join("snapshots");
    if !snapshots_dir.is_dir() {
        return;
    }
    let Ok(snap_hash_entries) = std::fs::read_dir(&snapshots_dir) else {
        return;
    };
    for snap_hash_entry in snap_hash_entries.flatten() {
        let build_dir_name = snap_hash_entry.file_name().to_string_lossy().to_string();
        let Some((snap_hash, partition)) =
            crate::db::TemplateCachePartition::parse_build_dir_name(&build_dir_name)
        else {
            continue;
        };
        let snap_hash = snap_hash.to_string();
        if !snap_hash.starts_with("sha256:") || !snap_hash_entry.path().is_dir() {
            continue;
        }
        let Ok(config_entries) = std::fs::read_dir(snap_hash_entry.path()) else {
            continue;
        };
        for config_entry in config_entries.flatten() {
            let config_name = config_entry.file_name().to_string_lossy().to_string();
            let Some((vcpu, memory_mib)) = parse_snapshot_config_dir(&config_name) else {
                continue;
            };
            let config_dir = config_entry.path();
            if !config_dir.join("snapshot.base").exists()
                || !config_dir.join("vmstate.base").exists()
            {
                continue;
            }
            let snap_hash_val =
                std::fs::read_to_string(config_dir.join("snapshot_hash")).unwrap_or_default();

            let db = Arc::clone(db);
            let n = name.to_string();
            let h = snap_hash.clone();
            match tokio::task::spawn_blocking(move || {
                let conn = lock_recovery_db(&db)?;
                crate::db::upsert_snapshot(
                    &conn,
                    crate::db::SnapshotKey {
                        name: &n,
                        content_hash: &h,
                        cache_partition: partition,
                        vcpu,
                        memory_mib,
                    },
                    &snap_hash_val,
                )
                .map_err(|error| error.to_string())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(
                        template = %name,
                        content_hash = %snap_hash,
                        %error,
                        "failed to upsert base snapshot during startup recovery"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        template = %name,
                        content_hash = %snap_hash,
                        %error,
                        "base snapshot upsert task failed during startup recovery"
                    );
                }
            }
        }
    }
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
        //   - artifacts missing → delete the row outright; any
        //     partial subvolume on disk is a small leak handled by
        //     the next eviction's nodemgr-delete path.
        let stuck = crate::db::list_deleting_builds(&conn).unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to enumerate stuck 'deleting' template builds");
            Vec::new()
        });
        for (name, content_hash, partition) in &stuck {
            if artifact_store.build_artifacts_missing(name, content_hash, *partition) {
                tracing::info!(
                    template = %name,
                    content_hash = %content_hash,
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
                pplx_space_metrics::template::digest_pins_pruned(pruned as u64);
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

impl TemplateManagerService {
    /// Prune `template_builds` / `template_snapshots` rows whose on-disk
    /// artifacts are missing. Exposed so tests can drive the recovery
    /// cleanup path without running the full recovery sweep, and so the
    /// delete-template crash window has a single named hook.
    pub(super) async fn prune_stale_template_artifact_db_rows(&self) {
        prune_catalog_rows_missing_on_disk(&self.db, &self.artifact_store).await;
    }

    /// Spawn startup recovery without delaying the template-manager gRPC bind.
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
        // zero makes the loop log and return immediately.
        self.spawn_eviction_loop(shutdown);

        let service = self.clone();
        tokio::spawn(async move {
            tracing::info!("template-manager startup recovery started");
            service.run_startup_recovery().await;
            tracing::info!("template-manager startup recovery finished");

            service.spawn_startup_base_snapshot_reconcile();
        });
    }

    /// Reconcile DB with disk and recover background loops needed after startup.
    pub async fn run_startup_recovery(&self) {
        cleanup_transient_template_name_dirs();
        cleanup_transient_digest_staging_dirs();

        if let Ok(entries) = std::fs::read_dir(TEMPLATE_DIR) {
            for entry in entries.flatten() {
                if !entry.path().is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with('.') || name.ends_with(".old") || name.ends_with(".importing") {
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
                        if is_import_staging_dir(&hash_name)
                            || is_republish_staging_dir(&hash_name)
                            || replaced_digest_base(&hash_name).is_some()
                            || !digest_entry.path().is_dir()
                        {
                            continue;
                        }
                        let Some((content_hash, partition)) =
                            crate::db::TemplateCachePartition::parse_build_dir_name(&hash_name)
                        else {
                            continue;
                        };
                        let digest_dir = digest_entry.path();
                        reconcile_digest_dir_against_db(
                            self,
                            DigestDirBuild {
                                name: &name,
                                hash_name: content_hash,
                                partition,
                                digest_dir: &digest_dir,
                            },
                        )
                        .await;
                    }
                }

                reconcile_base_snapshots_for_template(&self.db, &name, &name_dir).await;
            }
        }

        self.prune_stale_template_artifact_db_rows().await;

        {
            let Ok(mut handle) = self.pool_handle.lock() else {
                tracing::warn!(
                    "pool manager handle mutex poisoned; startup recovery cannot spawn pool loop"
                );
                return;
            };
            if handle.is_none() {
                let db = Arc::clone(&self.db);
                *handle = Some(tokio::spawn(crate::pool_manager::pool_loop(db)));
                tracing::debug!("pool manager loop started");
            }
        }
    }
}

#[cfg(test)]
mod tests;
