//! Abandoned suspend directories are the cleanup record, including after the
//! lifecycle row has returned to a stable state. The inventory loop owns retry.
use pplx_space_common_core::types::LocalArtifactId;

use super::*;
use std::collections::VecDeque;
use std::fs::ReadDir;
use std::path::{Path, PathBuf};

const SCAN_ENTRIES: usize = 128;
const DELETE_BATCH: usize = 8;
/// Entries under a sandbox's `snapshots/` with this prefix are logically
/// deleted. The periodic sweep removes them through node-manager, which can
/// delete nested subvolumes this service cannot.
pub(in crate::service) const RETIRED_PREFIX: &str = ".suspend-gc-pending-";

/// Two directory handles, independent of sandbox count. Keep enumeration across
/// ticks so retained/busy entries and large snapshot inventories cannot hide the
/// tail forever. Process restart simply begins another sweep.
#[derive(Default)]
pub(super) struct StagingCleanupScan {
    sandboxes: Option<ReadDir>,
    staging: Option<(SandboxId, ReadDir)>,
}

impl SandboxManagerService {
    pub(super) fn abandoned_suspend_staging_batch(
        &self,
        cache_dir: &Path,
        scan: &mut StagingCleanupScan,
    ) -> std::io::Result<Vec<PathBuf>> {
        if scan.sandboxes.is_none() {
            scan.sandboxes = match std::fs::read_dir(cache_dir) {
                Ok(entries) => Some(entries),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(error),
            };
        }
        let mut paths = Vec::new();
        let mut remaining = SCAN_ENTRIES;
        while remaining > 0 && paths.len() < DELETE_BATCH {
            if scan.staging.is_none() {
                remaining -= 1;
                let Some(entry) = scan.sandboxes.as_mut().and_then(Iterator::next) else {
                    scan.sandboxes = None;
                    break;
                };
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name();
                let Some(id) = name
                    .to_str()
                    .and_then(|name| name.parse::<SandboxId>().ok())
                else {
                    continue;
                };
                if name != id.to_string().as_str() {
                    continue;
                }
                let parent = suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(
                    &entry.path(),
                );
                match std::fs::read_dir(&parent) {
                    Ok(entries) => scan.staging = Some((id, entries)),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        tracing::warn!(sandbox_id = %id, %error, "cannot enumerate abandoned suspend staging; will retry");
                        continue;
                    }
                }
            }
            let (id, entries) = scan.staging.as_mut().ok_or_else(|| {
                std::io::Error::other("suspend staging scan lost its open directory")
            })?;
            let id = id.to_string();
            // Retire names while no publication/cleanup can begin. Once outside
            // the publication namespace, a later recovery cannot adopt files
            // whose deletion is pending. No unlink occurs until node-manager
            // confirms exclusion from surviving captures and transfers.
            let wake = self.auto_lifecycle.wake_lock(&id);
            let artifact = self.auto_lifecycle.artifact_lock(&id);
            let guards = match (
                wake.try_lock(),
                artifact.try_lock(),
                self.try_cleanup_fence(&id),
            ) {
                (Ok(wake), Ok(artifact), CleanupLockTryResult::Acquired(cleanup)) => {
                    Some((wake, artifact, cleanup))
                }
                _ => None,
            };
            let eligible = if guards.is_some() {
                self.startup_recovery_db().and_then(|db| {
                    crate::db::get_sandbox(&db, &id).map_err(StartupRecoveryError::from)
                })
            } else {
                Ok(None)
            };
            let may_retire = match eligible {
                Ok(Some(row))
                    if matches!(
                        row.state,
                        SandboxState::Started
                            | SandboxState::Paused
                            | SandboxState::Stopped
                            | SandboxState::Suspended
                    ) =>
                {
                    true
                }
                Ok(_) => false,
                Err(error) => {
                    tracing::warn!(sandbox_id = %id, %error, "cannot establish suspend staging owner; retaining files");
                    false
                }
            };
            while remaining > 0 && paths.len() < DELETE_BATCH {
                remaining -= 1;
                let Some(entry) = entries.next() else {
                    scan.staging = None;
                    break;
                };
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if name
                    .strip_prefix(RETIRED_PREFIX)
                    .is_some_and(|id| id.parse::<LocalArtifactId>().is_ok())
                {
                    paths.push(entry.path());
                } else if guards.is_some()
                    && name
                        .strip_prefix(suspend_snapshot_publication::RETIRED_HOLD_PREFIX)
                        .and_then(|id| id.parse::<LocalArtifactId>().ok())
                        .is_some()
                {
                    // A hold is the canonical snapshot a publication moved
                    // aside. Only the lifecycle guards make any observation
                    // about it current: a publication retry restores the hold
                    // to the canonical path and moves it aside again before
                    // replacing it.
                    let sandbox_dir = entry
                        .path()
                        .parent()
                        .and_then(|p| p.parent().map(Path::to_path_buf));
                    let Some(sandbox_dir) = sandbox_dir else {
                        continue;
                    };
                    let canonical =
                        suspend_snapshot_publication::canonical_suspend_snapshot_dir(&sandbox_dir);
                    match self.hold_disposition(&id, &entry.path(), &canonical, may_retire) {
                        Ok(HoldDisposition::Release) => {
                            match suspend_snapshot_publication::release_retired_suspend_snapshot(
                                &sandbox_dir,
                                &entry.path(),
                            ) {
                                Ok(Some(released)) => paths.push(released),
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::warn!(sandbox_id = %id, %error, "cannot release retired snapshot; will retry")
                                }
                            }
                        }
                        Ok(HoldDisposition::Restore) => {
                            match std::fs::rename(entry.path(), &canonical) {
                                Ok(()) => {
                                    tracing::warn!(sandbox_id = %id, "restored the held snapshot of a rolled-back publication")
                                }
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                                Err(error) => {
                                    tracing::warn!(sandbox_id = %id, %error, "cannot restore held snapshot; will retry")
                                }
                            }
                        }
                        Ok(HoldDisposition::Retain(reason)) => {
                            tracing::warn!(sandbox_id = %id, hold = %entry.path().display(), reason, "retaining held snapshot")
                        }
                        Ok(HoldDisposition::InFlight) => {}
                        Err(error) => {
                            tracing::warn!(sandbox_id = %id, %error, "cannot establish held snapshot owner; retaining it")
                        }
                    }
                } else if may_retire
                    && let Some(artifact) = name
                        .strip_prefix(suspend_snapshot_publication::SUSPEND_STAGING_PREFIX)
                        .and_then(|id| id.parse::<LocalArtifactId>().ok())
                {
                    let retired = entry
                        .path()
                        .with_file_name(format!("{RETIRED_PREFIX}{artifact}"));
                    match std::fs::rename(entry.path(), &retired) {
                        Ok(()) => paths.push(retired),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            tracing::warn!(sandbox_id = %id, %error, "cannot retire abandoned suspend staging; will retry")
                        }
                    }
                }
            }
        }
        Ok(paths)
    }
}

/// The artifact a snapshot directory's sidecar names, read without validating
/// the artifact. `None` is an absent, unreadable or unparsable sidecar:
/// unknown identity, never evidence about ownership.
fn sidecar_artifact_id(snapshot_dir: &Path) -> Option<LocalArtifactId> {
    #[derive(serde::Deserialize)]
    struct Identity {
        local_artifact_id: Option<LocalArtifactId>,
    }
    let path = snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    serde_json::from_slice::<Identity>(&std::fs::read(path).ok()?)
        .ok()?
        .local_artifact_id
}

#[derive(Debug, PartialEq, Eq)]
enum HoldDisposition {
    /// The suspended row pins the held artifact and nothing stands at the
    /// canonical path: the hold is the snapshot a wake needs, moved aside by a
    /// publication that never committed.
    Restore,
    /// Durably superseded: the canonical snapshot names a different artifact
    /// that the suspended row pins, or the sandbox left the suspended state
    /// with no canonical snapshot at all.
    Release,
    /// Not enough evidence either way. A readable canonical identity alone is
    /// not: a restore writes metadata before the VM files, so the canonical
    /// path may hold an incomplete copy of the very artifact in the hold.
    Retain(&'static str),
    /// No canonical snapshot and a lifecycle state that recovery still owns.
    InFlight,
}

impl SandboxManagerService {
    fn hold_disposition(
        &self,
        sandbox_id: &str,
        hold: &Path,
        canonical: &Path,
        stable: bool,
    ) -> Result<HoldDisposition, StartupRecoveryError> {
        let held = sidecar_artifact_id(hold);
        if let Some(canonical_id) = sidecar_artifact_id(canonical) {
            let Some(held) = held else {
                return Ok(HoldDisposition::Retain("hold identity unreadable"));
            };
            if held == canonical_id {
                return Ok(HoldDisposition::Retain(
                    "canonical snapshot names the held artifact",
                ));
            }
            return Ok(if self.suspended_pinned_to(sandbox_id, canonical_id)? {
                HoldDisposition::Release
            } else {
                HoldDisposition::Retain("canonical artifact is not the pinned one")
            });
        }
        if canonical.exists() {
            return Ok(HoldDisposition::Retain(
                "canonical snapshot has no readable identity",
            ));
        }
        if !stable {
            return Ok(HoldDisposition::InFlight);
        }
        let Some(held) = held else {
            return Ok(HoldDisposition::Retain("hold identity unreadable"));
        };
        Ok(if self.suspended_pinned_to(sandbox_id, held)? {
            HoldDisposition::Restore
        } else {
            HoldDisposition::Release
        })
    }

    fn suspended_pinned_to(
        &self,
        sandbox_id: &str,
        artifact: LocalArtifactId,
    ) -> Result<bool, StartupRecoveryError> {
        let db = self.startup_recovery_db()?;
        Ok(crate::db::sandbox_suspended_with_pinned_artifact(
            &db,
            sandbox_id,
            &artifact.to_string(),
        )?)
    }
}

/// Reclaim a lane entry now instead of on a later sweep. Publication deleted
/// an ordinary retired snapshot inline before the lane existed, and the sweep
/// handles at most `DELETE_BATCH` entries per pass, so a sustained retirement
/// rate must not wait on it. One local removal covers an ordinary snapshot;
/// one this service cannot delete, because it holds retained read-only
/// subvolumes, is deleted through node-manager. Either failure leaves the
/// entry, already in the lane, for the sweep.
///
/// Admission is bounded: a slot is taken before spawning and held through the
/// local removal and the node-manager call, so a retirement burst or slow I/O
/// cannot pile blocking deletions onto the runtime. Without a free slot the
/// entry joins a bounded in-memory backlog that the running reclaims drain
/// as their slots free up; beyond that backlog, and across a restart, the
/// entry waits durably in the lane for the sweep.
pub(in crate::service) fn reclaim_garbage_now(
    sandbox_id: String,
    garbage: PathBuf,
    node_manager: crate::node_manager_transport::NodeManagerConnector,
) -> Option<tokio::task::JoinHandle<()>> {
    // Admission and enqueue happen under the backlog lock, and a worker gives
    // its slot back under the same lock only after seeing the backlog empty,
    // so an entry can never be appended between a worker's last look and its
    // exit: either the producer finds the slot free, or the worker finds the
    // entry.
    let permit = {
        let mut backlog = immediate_reclaim_backlog();
        match IMMEDIATE_RECLAIM_SLOTS.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                if backlog.len() < IMMEDIATE_RECLAIM_BACKLOG {
                    backlog.push_back((sandbox_id, garbage, node_manager));
                } else {
                    tracing::debug!(sandbox_id, path = %garbage.display(), "immediate reclaim backlog full; the staging sweep collects the entry");
                }
                return None;
            }
        }
    };
    Some(tokio::spawn(async move {
        let mut permit = Some(permit);
        let mut next = Some((sandbox_id, garbage, node_manager));
        // Each entry is deleted through the connector that submitted it.
        while let Some((sandbox_id, garbage, node_manager)) = next {
            reclaim_one(&sandbox_id, &garbage, &node_manager).await;
            let mut backlog = immediate_reclaim_backlog();
            next = backlog.pop_front();
            if next.is_none() {
                // Release the slot while still holding the lock.
                drop(permit.take());
            }
        }
    }))
}

type ReclaimEntry = (
    String,
    PathBuf,
    crate::node_manager_transport::NodeManagerConnector,
);

fn immediate_reclaim_backlog() -> std::sync::MutexGuard<'static, VecDeque<ReclaimEntry>> {
    static BACKLOG: std::sync::Mutex<VecDeque<ReclaimEntry>> =
        std::sync::Mutex::new(VecDeque::new());
    BACKLOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn reclaim_one(
    sandbox_id: &str,
    garbage: &Path,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) {
    let local = {
        let garbage = garbage.to_path_buf();
        tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&garbage)).await
    };
    let error = match local {
        Ok(Ok(())) => {
            tracing::debug!(sandbox_id, path = %garbage.display(), "retired snapshot removed");
            return;
        }
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => return,
        Ok(Err(error)) => error,
        Err(join) => {
            tracing::warn!(sandbox_id, path = %garbage.display(), %join, "retired snapshot removal task failed; the staging sweep retries");
            return;
        }
    };
    let mut client = node_manager.lazy();
    let request = pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
        local_path: garbage.to_string_lossy().into_owned(),
    };
    match tokio::time::timeout(
        IMMEDIATE_DELETE_TIMEOUT,
        client.delete_cache_artifact(request),
    )
    .await
    {
        Ok(Ok(_)) => tracing::info!(
            sandbox_id,
            path = %garbage.display(),
            local_error = %error,
            "retired snapshot deleted through node-manager"
        ),
        Ok(Err(status)) => tracing::info!(
            sandbox_id,
            path = %garbage.display(),
            local_error = %error,
            %status,
            "retired snapshot deletion deferred to the staging sweep"
        ),
        Err(_) => tracing::info!(
            sandbox_id,
            path = %garbage.display(),
            local_error = %error,
            "retired snapshot deletion timed out; node-manager keeps ownership and the staging sweep retries"
        ),
    }
}

const IMMEDIATE_DELETE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Concurrent immediate reclaims; the rest queue up to the backlog bound.
const IMMEDIATE_RECLAIM_CONCURRENCY: usize = 2;
const IMMEDIATE_RECLAIM_BACKLOG: usize = 256;
static IMMEDIATE_RECLAIM_SLOTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(IMMEDIATE_RECLAIM_CONCURRENCY);

pub(super) async fn delete_batch(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    paths: Vec<PathBuf>,
    shutdown: &CancellationToken,
) {
    // Bound how long this hygiene pass delays lifecycle recovery. Expiry drops
    // only the RPC waiter: node-manager retains deletion ownership. Unconfirmed
    // paths stay on disk and are rediscovered on the next complete sweep.
    let work = async {
        for path in paths {
            if shutdown.is_cancelled() {
                break;
            }
            if let Err(error) = client
                .delete_cache_artifact(
                    pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                        local_path: path.to_string_lossy().into_owned(),
                    },
                )
                .await
            {
                tracing::warn!(staging_dir = %path.display(), %error, "abandoned suspend staging deletion failed; will retry");
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .is_err()
    {
        tracing::warn!(
            "suspend staging cleanup budget reached; retained paths will be rediscovered"
        );
    }
}

#[cfg(test)]
mod tests;
