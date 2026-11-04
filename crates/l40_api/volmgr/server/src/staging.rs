//! Reflink-based source staging for push jobs.
//!
//! The volume-manager push pipeline persists job intent in SQLite and runs
//! the actual S3 upload from a worker task — so the wall-clock gap between
//! "caller submitted a push" and "worker opens the file to upload" can be
//! arbitrarily large (queueing, retries, restarts). If the caller's source
//! file is unlinked or overwritten in that window, the upload fails with
//! `entity not found` and the job is marked permanently failed. The
//! recurring symptom is a source file that vanished between submit and
//! execute when the caller re-runs an operation over the same paths.
//!
//! Staging takes a btrfs reflink of the caller's `local_path` into a
//! vol-mgr-owned directory at submit time. The reflink is a fresh inode
//! sharing blocks via copy-on-write: future unlinks of the original or
//! writes through the original's path don't affect the staged copy. The
//! upload worker reads bytes from the staged path, then unlinks it after a
//! terminal outcome. On Linux the fast path requests a reflink explicitly;
//! when the filesystem cannot clone the source, staging falls back to a full
//! data copy and emits a warning so enqueue latency degradation is observable.
//!
//! Process restarts are handled by [`cleanup_orphans`], which runs at
//! startup before the worker recovers any jobs: it removes any staged file
//! whose owning job is no longer in `pending`/`running` state (either
//! because the job completed/failed before crash, or because the file
//! belongs to a dedup-loser submission that crashed before cleaning up).

use std::collections::HashSet;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use pplx_space_common_svc::page_cache;
use uuid::Uuid;

/// Vol-mgr's staging area for in-flight push sources. One file per active
/// push job, named after the staging UUID embedded in the job payload's
/// `staged_path`.
#[derive(Clone, Debug)]
pub struct StagingDir {
    root: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StagingMode {
    #[cfg(target_os = "linux")]
    Reflink,
    #[cfg(target_os = "macos")]
    CloneOrCopy,
    CopyFallback,
}

impl StagingMode {
    const fn as_str(self) -> &'static str {
        match self {
            #[cfg(target_os = "linux")]
            Self::Reflink => "reflink",
            #[cfg(target_os = "macos")]
            Self::CloneOrCopy => "clone_or_copy",
            Self::CopyFallback => "copy_fallback",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error("prepare staging dir {path}: {source}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("stage source {src} -> {dst}: {message}")]
    Stage {
        src: PathBuf,
        dst: PathBuf,
        message: String,
    },
}

impl StagingDir {
    /// Create a `StagingDir` rooted at `root`, ensuring the directory
    /// exists. Idempotent.
    pub fn ensure(root: impl Into<PathBuf>) -> Result<Self, StagingError> {
        let root = root.into();
        // Shared ancestors keep ordinary creation permissions. Provisioning owns
        // mode/group on an existing leaf; only an unprovisioned leaf gets a
        // restrictive default, since upload copies may contain VM memory.
        let ensure = || -> std::io::Result<()> {
            if let Some(parent) = root.parent() {
                std::fs::create_dir_all(parent)?;
            }
            match std::fs::DirBuilder::new().mode(0o750).create(&root) {
                Err(error)
                    if error.kind() == std::io::ErrorKind::AlreadyExists && root.is_dir() =>
                {
                    Ok(())
                }
                result => result,
            }
        };
        ensure().map_err(|source| StagingError::CreateDir {
            path: root.clone(),
            source,
        })?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Reflink `src` into the staging dir under a fresh UUID name and
    /// return the staged path. The caller is responsible for unlinking
    /// the returned path after the upload completes (success or permanent
    /// failure) and on dedup misses.
    ///
    /// Async because the fallback path (non-btrfs filesystems where
    /// explicit reflinking is unavailable) can move arbitrarily
    /// large files: a synchronous wrapper would block a tokio worker
    /// thread for the entire copy duration. The reflink fast-path on
    /// btrfs/APFS completes in microseconds, so awaiting here costs
    /// effectively nothing in production.
    pub async fn stage(&self, src: &Path) -> Result<PathBuf, StagingError> {
        // UUIDv7 is the SPACE-wide default for new ids (see CLAUDE.md). The
        // timestamp prefix gives `ls /space/cache/firecracker/.volmgr-staging`
        // a free chronological sort that mirrors submit order, which is the
        // first thing an operator needs while triaging a stuck push.
        let staged = self.root.join(Uuid::now_v7().to_string());
        let started_at = Instant::now();
        let mode = reflink_copy(src, &staged)
            .await
            .map_err(|message| StagingError::Stage {
                src: src.to_path_buf(),
                dst: staged.clone(),
                message,
            })?;
        if mode == StagingMode::CopyFallback {
            tracing::warn!(
                source_path = %src.display(),
                staged_path = %staged.display(),
                staging_mode = mode.as_str(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "volume push source staged with full-copy fallback"
            );
        } else {
            tracing::debug!(
                source_path = %src.display(),
                staged_path = %staged.display(),
                staging_mode = mode.as_str(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "volume push source staged"
            );
        }
        page_cache::advise_path_dontneed(src, "stage volume upload source");
        page_cache::advise_path_dontneed(&staged, "stage volume upload copy");
        Ok(staged)
    }
}

/// Reflink-or-copy `src` to `dst`. Linux shares descriptor-based copy mechanics
/// with node-manager and restore staging, including verified NoCoW inheritance.
async fn reflink_copy(src: &Path, dst: &Path) -> Result<StagingMode, String> {
    #[cfg(target_os = "linux")]
    {
        let src = src.to_path_buf();
        let dst = dst.to_path_buf();
        tokio::task::spawn_blocking(move || pplx_space_common_io::reflink::copy_file(&src, &dst))
            .await
            .map_err(|error| format!("join staged copy: {error}"))?
            .map(|cloned| {
                if cloned {
                    StagingMode::Reflink
                } else {
                    StagingMode::CopyFallback
                }
            })
            .map_err(|error| format!("stage reflink or sparse copy: {error}"))
    }

    #[cfg(target_os = "macos")]
    {
        // macOS supports clonefile on APFS; cp -c uses it when available
        // and falls back to a regular copy otherwise. Mirroring the
        // node-runtime-core helper so test/dev environments behave the
        // same.
        let status = tokio::process::Command::new("cp")
            .arg("-c")
            .arg(src)
            .arg(dst)
            .status()
            .await
            .map_err(|e| format!("spawn cp -c: {e}"))?;
        if status.success() {
            return Ok(StagingMode::CloneOrCopy);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        tokio::fs::copy(src, dst)
            .await
            .map_err(|error| format!("copy: {error}"))?;
        Ok(StagingMode::CopyFallback)
    }
}

/// Reflink `src` to `dst` without a full-copy fallback.
///
/// Shadow comparisons call this immediately before a pull becomes visible as
/// complete. Falling back to a byte copy there would put large artifacts back
/// on the restore hot path, so unsupported filesystems skip shadow validation
/// instead.
pub(crate) async fn reflink_only(src: &Path, dst: &Path) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        let src = src.to_path_buf();
        let dst = dst.to_path_buf();
        tokio::task::spawn_blocking(move || pplx_space_common_io::reflink::reflink_file(&src, &dst))
            .await
            .map_err(|error| format!("join shadow reflink: {error}"))?
            .map_err(|error| format!("shadow reflink: {error}"))
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (src, dst);
        Err("reflink-only shadow snapshots require Linux".to_string())
    }
}

/// Remove every file under `staging_dir` whose path is not in `keep`.
///
/// Call at startup, before [`BackgroundWorker::recover`], with `keep`
/// populated from the staged paths of every job currently in
/// `pending`/`running`. Files left behind by jobs that completed or
/// permanently failed before the previous process exit are unreferenced
/// and removed. Returns the number of files deleted; per-file errors are
/// logged at WARN and counted, not surfaced (an unremovable staging file
/// is a noisy disk-leak, not a correctness bug).
///
/// [`BackgroundWorker::recover`]: pplx_space_common_jobs::BackgroundWorker::recover
pub fn cleanup_orphans(staging_dir: &Path, keep: &HashSet<PathBuf>) -> usize {
    let Ok(entries) = std::fs::read_dir(staging_dir) else {
        return 0;
    };
    let mut removed = 0_usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if keep.contains(&path) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                removed += 1;
                tracing::debug!(
                    path = %path.display(),
                    "removed orphan staging file"
                );
            }
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "failed to remove orphan staging file"
                );
            }
        }
    }
    if removed > 0 {
        tracing::info!(removed, "cleaned up orphan staging files");
    }
    removed
}

#[cfg(test)]
mod tests;
