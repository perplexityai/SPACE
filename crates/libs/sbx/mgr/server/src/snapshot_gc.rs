//! LRU reaper for on-node snapshot artifacts under `/space/cache`.
//!
//! Sandboxes that have been suspended (or stopped with a persisted
//! disk artifact, or have a named user snapshot) leave per-sandbox
//! directories under `/space/cache/firecracker/sandboxes/<sandbox_id>/`
//! (`snapshot` and `snapshots/<name>`).
//! These accumulate until the sandbox is deleted; nodes regularly
//! run out of disk before that happens.
//!
//! This module runs a per-node LRU loop that frees disk by deleting
//! local artifacts that meet **all** of these criteria:
//!
//! 1. **Backup completed.** `local_artifacts.backup_completed_at IS
//!    NOT NULL` — written by the snapshot-upload worker after
//!    `finalize_artifact_backup` returns `ready`. Per-artifact (not
//!    per-sandbox) — the `jobs.subject_id` column is `sandbox.id`,
//!    not `local_artifact_id`, so a cross-table join on subject_id
//!    would be ambiguous when a sandbox has multiple artifacts at
//!    different backup states (e.g. A1 uploaded, A2 mid-upload).
//! 2. **Not pinned to a live instance.** No row in `sandboxes` points
//!    at the artifact as either its committed current artifact or the
//!    rollback predecessor of an in-progress suspend.
//! 3. **Idle.** `now - last_touched_at >= idle`
//!    (1 day). `last_touched_at` is bumped on every
//!    lifecycle transition that opens the artifact's `local_path`,
//!    so an artifact that hasn't been touched for a day is genuinely
//!    cold.
//! 4. **Outside the grace period.** Per [`pplx_space_common_lru_reaper`]'s
//!    grace rule: `now - max(created_at, last_touched_at) >= grace`.
//!    The grace period (default 1800 s, floor 60 s) defends against
//!    the create/touch race where a sandbox has just produced an
//!    artifact but no consumer has touched it yet.
//!
//! The trigger is the shared disk-fraction gate ([`crate::disk_usage`]):
//! the loop reaps only while `/space/cache` filesystem usage is at or
//! above `engage_fraction`. Each tick examines at most 128 raw artifact rows
//! from a finite rowid sweep, then frees that page in LRU order on
//! `last_touched_at` (tiebroken by `local_artifact_id`). This is not a global
//! oldest-first ordering. Fresh rows wait for a later sweep; no recovery
//! deadline is promised. Sizing runs without a state-writer lease and checks
//! shutdown between artifacts, but each filesystem size walk is indivisible.
//!
//! ## Race protection
//!
//! Per-sandbox artifact and staging locks exclude publication and restore.
//! One IMMEDIATE transaction rechecks backup, pin, idle/grace, and shared-path
//! evidence before Ready→Deleting. The resulting delete lease and operation
//! locks remain held across the filesystem rename and the separate row-delete
//! commit; a state-writer lease keeps this work owned through shutdown.
//!
//! The rmdir of the staged directory runs after commit and lock release. If it fails, the staging
//! directory persists under `<local_path>.gc-pending-*` with no row
//! referencing it — invisible to the planner. The orphan sweep
//! covers that gap: it runs once at startup (even when the reaper
//! is disabled — the bytes are logically deleted either way) and
//! again at the start of every tick, so a failed cleanup is retried
//! each interval rather than waiting for a process restart.

use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_common_lru_reaper::{self as lru_reaper, EvictionInput};
use rusqlite::Connection;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use self::orphans::{OrphanKind, OrphanScan};
use crate::auto_lifecycle::now_secs;
use crate::db::snapshot_gc::ArtifactScan;

mod orphans;
use crate::local_artifacts::{CheckoutError, GcDeleteCheckout, GcEligibility, LocalArtifacts};

pub(crate) struct GcOwnership {
    pub coordinator: crate::auto_lifecycle::AutoLifecycleCoordinator,
    pub drain: Arc<crate::service::drain::DrainState>,
    pub shutdown: CancellationToken,
    pub node_manager: crate::node_manager_transport::NodeManagerConnector,
}

#[derive(Debug, thiserror::Error)]
enum GcDatabaseReadError {
    #[error(transparent)]
    Poisoned(#[from] crate::node_state_sync::DatabaseMutexPoisoned),
    #[error(transparent)]
    Inventory(#[from] crate::db::inventory_read::InventoryReadError),
}

/// Runtime configuration for the snapshot reaper loop. Loaded from
/// env on startup; disabled unless `SPACE_SBX_GC_ENABLED` is
/// `1`/`true`.
///
/// The trigger is the shared disk-fraction gate (see
/// [`crate::disk_usage`]): the loop reaps only while `/space/cache`
/// usage is at or above `engage_fraction`, freeing in LRU order until
/// usage falls below `target_fraction`; the engage→target gap is the
/// hysteresis. `engage_fraction = 0` makes the loop always-on.
#[derive(Debug, Clone, Copy)]
pub struct GcConfig {
    pub enabled: bool,
    pub engage_fraction: f64,
    pub target_fraction: f64,
    pub interval: Duration,
    pub grace: Duration,
    pub idle: Duration,
}

impl GcConfig {
    const ENV_ENABLED: &'static str = "SPACE_SBX_GC_ENABLED";
    const ENV_ENGAGE_FRACTION: &'static str = "SPACE_SBX_GC_ENGAGE_FRACTION";
    /// Test hooks (`pplx_space_common_core::env::test_hook`): compiled in only
    /// with the `test-hooks` feature, so release binaries cannot shrink these
    /// timers. Smoke benches use them to make the loop observable.
    const ENV_TARGET_FRACTION: &'static str = "SPACE_SBX_GC_TARGET_FRACTION";
    const ENV_INTERVAL_SECS: &'static str = "SPACE_SBX_GC_INTERVAL_SECS";
    const ENV_GRACE_SECS: &'static str = "SPACE_SBX_GC_GRACE_SECS";
    const ENV_IDLE_SECS: &'static str = "SPACE_SBX_GC_IDLE_SECS";

    /// Artifact caches engage above the cold-sandbox eviction loop
    /// (0.40) and below template GC: their reclaim cost (S3 re-pull
    /// on a named-snapshot restore) sits between the two.
    const DEFAULT_ENGAGE_FRACTION: f64 = 0.50;
    const DEFAULT_TARGET_FRACTION: f64 = 0.40;
    const DEFAULT_INTERVAL_SECS: u64 = 300;
    const DEFAULT_GRACE_SECS: u64 = 1800;
    const DEFAULT_IDLE_SECS: u64 = 86_400;

    /// Floor below which startup warns about premature cold-cache eviction.
    /// Grace complements, but never replaces, transactional pin/lease checks
    /// and artifact/staging exclusion during publication and restore.
    const SAFETY_GRACE_FLOOR_SECS: u64 = 60;

    pub fn from_env() -> Self {
        let enabled = std::env::var(Self::ENV_ENABLED)
            .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let engage = std::env::var(Self::ENV_ENGAGE_FRACTION)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(Self::DEFAULT_ENGAGE_FRACTION);
        let target = pplx_space_common_core::env::test_hook_var(Self::ENV_TARGET_FRACTION)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(Self::DEFAULT_TARGET_FRACTION);
        let (engage_fraction, target_fraction) = crate::disk_usage::validate_fraction_pair(
            "snapshot_gc",
            engage,
            target,
            Self::ENV_ENGAGE_FRACTION,
            Self::ENV_TARGET_FRACTION,
        );
        let interval_secs = pplx_space_common_core::env::test_hook_var(Self::ENV_INTERVAL_SECS)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(Self::DEFAULT_INTERVAL_SECS);
        let grace_secs = pplx_space_common_core::env::test_hook_var(Self::ENV_GRACE_SECS)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(Self::DEFAULT_GRACE_SECS);
        let idle_secs = pplx_space_common_core::env::test_hook_var(Self::ENV_IDLE_SECS)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(Self::DEFAULT_IDLE_SECS);
        if enabled && grace_secs < Self::SAFETY_GRACE_FLOOR_SECS {
            warn!(
                grace_secs,
                floor_secs = Self::SAFETY_GRACE_FLOOR_SECS,
                env_var = Self::ENV_GRACE_SECS,
                "sbx-mgr snapshot reaper grace period is below the safety floor — \
                 artifacts may be reaped before consumers can touch them. \
                 Use this only in tests; production should leave it at the default (1800 s).",
            );
        }
        Self {
            enabled,
            engage_fraction,
            target_fraction,
            interval: Duration::from_secs(interval_secs),
            grace: Duration::from_secs(grace_secs),
            idle: Duration::from_secs(idle_secs),
        }
    }
}

/// Why a per-candidate delete did or didn't happen. Each variant
/// corresponds 1:1 to a `snapshot.gc.skipped{reason}` tag (or a
/// successful `snapshot.gc.deleted{...}` for `Deleted`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionOutcome {
    Deleted,
    SkippedPinnedCurrentAtExecute,
    SkippedNotIdleAtExecute,
    SkippedSharedLocalPath,
    /// State or durable backup evidence no longer permits GC.
    SkippedNotReady,
    SkippedRowMissing,
}

impl EvictionOutcome {
    fn metric_label(self) -> Option<&'static str> {
        match self {
            EvictionOutcome::Deleted => None,
            EvictionOutcome::SkippedPinnedCurrentAtExecute => Some("pinned_current_at_execute"),
            EvictionOutcome::SkippedNotIdleAtExecute => Some("not_idle_at_execute"),
            EvictionOutcome::SkippedSharedLocalPath => Some("shared_local_path"),
            EvictionOutcome::SkippedNotReady => Some("not_ready"),
            EvictionOutcome::SkippedRowMissing => Some("row_missing"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct GcTickReport {
    engaged: bool,
    raw_rows_examined: usize,
    candidates_planned: usize,
    deleted: usize,
    skipped_pinned: usize,
    skipped_not_idle: usize,
    skipped_shared_local_path: usize,
    skipped_not_ready: usize,
    skipped_row_missing: usize,
    errors: usize,
    sampled_bytes_before: u64,
    bytes_freed: u64,
}

/// Recursively sum allocated file bytes under `path`. Symlinks are followed
/// only at the top level — within the directory tree we use
/// metadata's reported block count, which for symlinks is the symlink
/// target's allocation (so a snapshot directory full of symlinks does
/// not over-count). Physical allocation must match the statvfs-derived
/// reclaim target even when snapshot artifacts are sparse. Errors at any
/// node return 0 for that node and keep walking; an unreadable directory
/// yields a partial total. Shared with the cold-sandbox eviction loop's
/// freed-bytes estimate.
pub(crate) fn dir_size_bytes(path: &Path) -> u64 {
    let Ok(meta) = std::fs::metadata(path) else {
        return 0;
    };
    if meta.is_file() {
        return meta.blocks().saturating_mul(512);
    }
    if !meta.is_dir() {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total: u64 = 0;
    for entry in entries.flatten() {
        let Ok(child_meta) = entry.metadata() else {
            continue;
        };
        if child_meta.is_file() {
            total = total.saturating_add(child_meta.blocks().saturating_mul(512));
        } else if child_meta.is_dir() {
            total = total.saturating_add(dir_size_bytes(&entry.path()));
        }
    }
    total
}

/// Result of a per-candidate eviction attempt. The reaper drives
/// this in three phases:
///
///   1. `check_out_for_gc` (in LocalArtifacts) — CAS the row from
///      Ready → Deleting under the db lock. The check_out also
///      validates backup, pin, idle/grace, and shared-path evidence. On
///      success the row is reserved.
///   2. atomic `rename(local_path → .gc-pending-<id>)` outside the
///      lock. If it fails for a non-ENOENT reason the lease drops
///      and the row rolls back to Ready.
///   3. `lease.commit()` — DELETEs the row under the lock again.
///      The on-disk cleanup of the staging dir runs **after** this
///      function returns, via node-mgr's `DeleteCacheArtifact` RPC
///      (which can `btrfs subvolume delete` a subvolume root that
///      sbx-mgr's UID cannot rmdir on its own).
///
/// On Deleted, returns `Some(staging_path)` so the caller can pass
/// it to the RPC; returns `None` when the on-disk dir was already
/// gone at rename time.
///
fn execute_one(
    artifacts: &LocalArtifacts,
    candidate_id_str: &str,
    sandbox_id: &str,
    eligibility: GcEligibility,
) -> Result<(EvictionOutcome, Option<String>), String> {
    let candidate_id: LocalArtifactId = candidate_id_str
        .parse()
        .map_err(|e| format!("invalid local_artifact_id: {e}"))?;
    let lease = match artifacts.check_out_for_gc(candidate_id, sandbox_id, eligibility) {
        Ok(GcDeleteCheckout::Leased(lease)) => lease,
        Ok(GcDeleteCheckout::Pinned) | Err(CheckoutError::Pinned(_)) => {
            return Ok((EvictionOutcome::SkippedPinnedCurrentAtExecute, None));
        }
        Ok(GcDeleteCheckout::Recent) => {
            return Ok((EvictionOutcome::SkippedNotIdleAtExecute, None));
        }
        Ok(GcDeleteCheckout::Unbacked) => return Ok((EvictionOutcome::SkippedNotReady, None)),
        Err(CheckoutError::NotFound(_)) => return Ok((EvictionOutcome::SkippedRowMissing, None)),
        Err(CheckoutError::NotReady { state, .. }) => {
            warn!(
                local_artifact_id = %candidate_id,
                row_state = %state,
                "snapshot reaper: candidate is in non-ready state; another op holds it",
            );
            return Ok((EvictionOutcome::SkippedNotReady, None));
        }
        Err(CheckoutError::SharedLocalPath { .. }) => {
            return Ok((EvictionOutcome::SkippedSharedLocalPath, None));
        }
        Err(CheckoutError::CorruptState { raw_state, .. }) => {
            warn!(
                local_artifact_id = %candidate_id,
                raw_state = %raw_state,
                "snapshot reaper: candidate has corrupt state value; refusing to reap and surfacing as error",
            );
            return Err(format!("corrupt state value: {raw_state:?}"));
        }
        Err(CheckoutError::Db(e)) => return Err(format!("check_out_for_gc: {e}")),
        Err(error @ CheckoutError::NamedNotFound { .. }) => {
            // Only the named-lookup checkout returns this variant;
            // the id-keyed checkout used here never does.
            return Err(format!("unexpected checkout error: {error}"));
        }
    };

    // Atomic rename into a `.gc-pending-<id>` sibling. Linux's
    // open-file semantics keep any in-flight reader's fd valid; new
    // opens of the original path fail with ENOENT. The lease holds
    // the row in 'deleting' across the rename.
    let candidate_local_path = lease.local_path();
    let staging = format!("{candidate_local_path}.gc-pending-{candidate_id_str}");
    let disk_present = match std::fs::rename(candidate_local_path, &staging) {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            warn!(
                local_artifact_id = %candidate_id,
                local_path = %candidate_local_path,
                error = %err,
                "snapshot reaper: rename to staging path failed; lease will roll state back",
            );
            // Drop the lease without commit → DeleteLease::drop
            // rolls back to 'ready'. Next tick retries.
            return Err(format!("rename failed: {err}"));
        }
    };

    // Finalize: DELETE the row. The DELETE is gated by `state =
    // 'deleting'` so a concurrent rollback (impossible while we
    // hold the lease, but defensive) cannot turn this into a
    // spurious deletion.
    lease.commit().map_err(|e| format!("delete commit: {e}"))?;

    // The staging directory cleanup runs after this function
    // returns, via node-mgr's `DeleteCacheArtifact` RPC. Returning
    // the path lets the async caller invoke that without making
    // the per-candidate blocking phase itself dial gRPC.
    let staging_to_clean = disk_present.then_some(staging);
    Ok((EvictionOutcome::Deleted, staging_to_clean))
}

async fn gc_tick(
    artifacts: LocalArtifacts,
    db: Arc<Mutex<Connection>>,
    cfg: GcConfig,
    now: u64,
    disk: crate::disk_usage::DiskUsage,
    ownership: &GcOwnership,
    scan: &mut ArtifactScan,
) -> Result<GcTickReport, String> {
    // Disk-fraction gate: below the engage threshold the tick is a
    // cheap no-op — in particular the per-artifact directory-size
    // walk (the expensive part of planning) is skipped entirely.
    if ownership.shutdown.is_cancelled()
        || !crate::disk_usage::gate_engaged(disk.fraction(), cfg.engage_fraction)
    {
        return Ok(GcTickReport::default());
    }
    let need_to_free = disk.bytes_over_target(cfg.target_fraction);
    let idle_secs = cfg.idle.as_secs();

    let db_for_snap = Arc::clone(&db);
    let mut next_scan = *scan;
    let (next_scan, page) = tokio::task::spawn_blocking(move || {
        let conn = crate::node_state_sync::connection_or_signal_failure(&db_for_snap)?;
        let page = next_scan.next_page(&conn)?;
        Ok::<_, GcDatabaseReadError>((next_scan, page))
    })
    .await
    .map_err(|e| format!("snapshot read spawn_blocking: {e}"))?
    .map_err(|e| format!("read GC artifact page: {e}"))?;
    *scan = next_scan;

    // Disk-size walk and protected-set assembly happen outside the
    // DB lock. Items not yet idle are layered into `pinned` so the
    // planner skips them via its existing protected-set path; no
    // need to extend the planner's vocabulary.
    let raw_rows_examined = page.examined;
    let artifact_rows = page.artifacts;
    let shutdown = ownership.shutdown.clone();
    // Disk-size walk per artifact runs in spawn_blocking so a node
    // with many cold snapshot directories doesn't starve the async
    // executor — `dir_size_bytes` is recursive blocking
    // `std::fs::{metadata,read_dir}` and would otherwise pin a
    // runtime worker for the full tick.
    let plan_inputs: Vec<(EvictionInput<String>, String, String, String, bool, bool)> =
        tokio::task::spawn_blocking(move || {
            artifact_rows
                .into_iter()
                .take_while(|_| !shutdown.is_cancelled())
                .map(|row| {
                    let size = dir_size_bytes(Path::new(&row.local_path));
                    let input = EvictionInput {
                        key: row.local_artifact_id.clone(),
                        size_bytes: size,
                        created_at_secs: row.created_at_secs,
                        last_used_at_secs: row.last_touched_at_secs,
                    };
                    (
                        input,
                        row.sandbox_id,
                        row.artifact_origin,
                        row.artifact_kind,
                        row.backup_completed,
                        row.is_ready_state && !row.pinned_current,
                    )
                })
                .collect()
        })
        .await
        .map_err(|e| format!("plan_inputs spawn_blocking: {e}"))?;

    let sampled_bytes_before: u64 = plan_inputs.iter().map(|(i, ..)| i.size_bytes).sum();

    let mut pinned = HashSet::new();
    for (input, _, _, _, backup_completed, unowned_ready) in &plan_inputs {
        if !*backup_completed {
            pinned.insert(input.key.clone());
        }
        if !*unowned_ready {
            // Current pins and non-ready leases protect their artifact.
            pinned.insert(input.key.clone());
        }
        if now.saturating_sub(input.last_used_at_secs) < idle_secs {
            pinned.insert(input.key.clone());
        }
    }

    let inputs: Vec<EvictionInput<String>> = plan_inputs.iter().map(|(i, ..)| i.clone()).collect();
    let plan = lru_reaper::plan_evictions_to_free(
        inputs,
        &pinned,
        &HashSet::new(),
        need_to_free,
        cfg.grace.as_secs(),
        now,
    );

    let mut report = GcTickReport {
        engaged: true,
        raw_rows_examined,
        candidates_planned: plan.len(),
        sampled_bytes_before,
        ..Default::default()
    };

    for candidate in plan {
        if ownership.shutdown.is_cancelled() {
            break;
        }
        let (_, sandbox_id, origin, kind, _, _) = plan_inputs
            .iter()
            .find(|(i, ..)| i.key == candidate.key)
            .map(|(i, p, o, k, b, r)| (i.clone(), p.clone(), o.clone(), k.clone(), *b, *r))
            .ok_or_else(|| format!("plan_inputs missing key {}", candidate.key))?;
        let candidate_id_str = candidate.key.clone();
        let candidate_size = candidate.size_bytes;
        let artifact_lock = ownership.coordinator.artifact_lock(&sandbox_id);
        let staging_lock = ownership.coordinator.staging_lock(&sandbox_id);
        let guards = artifact_lock
            .try_lock_owned()
            .ok()
            .zip(staging_lock.try_lock_owned().ok());
        let Some(guards) = guards else {
            report.skipped_not_ready += 1;
            pplx_space_metrics::snapshot::gc_skipped("not_ready");
            continue;
        };
        let Ok(writer) =
            crate::service::drain::acquire_state_writer(&ownership.drain, "SnapshotGc")
        else {
            break;
        };
        let artifacts_for_blocking = artifacts.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let _writer = writer;
            let _guards = guards;
            execute_one(
                &artifacts_for_blocking,
                &candidate_id_str,
                &sandbox_id,
                GcEligibility {
                    now_secs: now,
                    idle_secs,
                    grace_secs: cfg.grace.as_secs(),
                },
            )
        })
        .await
        .map_err(|e| format!("execute_one spawn_blocking: {e}"))?;

        match outcome {
            Ok((EvictionOutcome::Deleted, staging_to_clean)) => {
                report.deleted += 1;
                report.bytes_freed = report.bytes_freed.saturating_add(candidate_size);
                pplx_space_metrics::snapshot::gc_deleted(&origin, &kind);
                // Clean up the staging directory via node-mgr.
                // The RPC handles both btrfs subvolumes (created by
                // node-mgr's `create_btrfs_snapshot` for d-*/f-*
                // named snapshots and d-stop-* stopped artifacts —
                // sbx-mgr's UID can't `rmdir` the subvol root) and
                // plain directories (suspend snapshots written file-
                // by-file). On RPC failure the staging dir leaks
                // until the next sbx-mgr startup sweep, which now
                // also goes through this RPC.
                if let Some(staging) = staging_to_clean {
                    delete_cache_artifact_via_node_mgr(
                        &staging,
                        &candidate.key,
                        &ownership.node_manager,
                    )
                    .await;
                }
            }
            Ok((skip, _)) => {
                match skip {
                    EvictionOutcome::SkippedPinnedCurrentAtExecute => report.skipped_pinned += 1,
                    EvictionOutcome::SkippedNotIdleAtExecute => report.skipped_not_idle += 1,
                    EvictionOutcome::SkippedSharedLocalPath => {
                        report.skipped_shared_local_path += 1
                    }
                    EvictionOutcome::SkippedNotReady => report.skipped_not_ready += 1,
                    EvictionOutcome::SkippedRowMissing => report.skipped_row_missing += 1,
                    EvictionOutcome::Deleted => unreachable!(),
                }
                if let Some(label) = skip.metric_label() {
                    pplx_space_metrics::snapshot::gc_skipped(label);
                }
            }
            Err(err) => {
                warn!(
                    local_artifact_id = %candidate.key,
                    error = %err,
                    "snapshot reaper: candidate delete failed",
                );
                report.errors += 1;
                pplx_space_metrics::snapshot::gc_skipped("error");
            }
        }
    }

    if report.bytes_freed < need_to_free {
        // A zero target is always-on reclaim: it asks to free every idle
        // artifact, so no page meets it and the shortfall is routine.
        if cfg.target_fraction > 0.0 {
            warn!(
                disk_usage_fraction = disk.fraction(),
                target_fraction = cfg.target_fraction,
                need_to_free,
                bytes_freed = report.bytes_freed,
                raw_rows_examined,
                "snapshot reaper: sampled page did not meet reclaim target; later ticks continue the bounded sweep",
            );
        } else {
            debug!(
                disk_usage_fraction = disk.fraction(),
                need_to_free,
                bytes_freed = report.bytes_freed,
                raw_rows_examined,
                "snapshot reaper: sampled page did not meet reclaim target; later ticks continue the bounded sweep",
            );
        }
    }
    Ok(report)
}

/// Root the reaper sweeps for `.gc-pending-*` orphans. Every staging
/// directory it produces is a sibling of a sandbox's snapshot artifact, and
/// node-mgr's `DeleteCacheArtifact` refuses any path outside this root, so the
/// rest of `/space/cache` holds nothing it could reap. Walking it anyway read
/// other services' private directories, such as node-mgr's root-only
/// `.btrfs-send-staging`, and warned about each on every pass. Tests drive
/// bounded discovery through a tempdir.
const CACHE_ROOT_FOR_SWEEP: &str = pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR;

/// Maximum directory-walk depth below [`CACHE_ROOT_FOR_SWEEP`]. Staging
/// directories sit at `<sandbox_id>/snapshot.gc-pending-<id>` and
/// `<sandbox_id>/{snapshots,template-snapshot-staging}/<name>.gc-pending-<id>`,
/// the shapes node-mgr accepts. The walk reads no deeper, and inside a
/// sandbox's directory it enters only those snapshot directories
/// (`OrphanKind::descends_into`): the rest are artifact contents and the jail
/// `instance`, which keep the sandbox's own owner and mode.
const SWEEP_MAX_DEPTH: usize = 3;
const FORK_COPY_ORPHAN_MIN_AGE_SECS: u64 = 60 * 60;

/// Issue a `DeleteCacheArtifact` RPC against node-mgr. Best-effort;
/// callers (per-candidate reap finalize, startup sweep) only log on
/// failure rather than abort, because the parent operation has
/// already committed (the row is gone, or this was orphan cleanup
/// to begin with).
async fn delete_cache_artifact_via_node_mgr(
    staging_path: &str,
    candidate_label: &str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) {
    let mut client = match node_manager.connect().await {
        Ok(c) => c,
        Err(err) => {
            warn!(
                candidate = %candidate_label,
                staging_path,
                error = %err,
                "snapshot reaper: node-mgr connect for DeleteCacheArtifact failed; staging directory will be retried at next sbx-mgr start",
            );
            return;
        }
    };
    let req = pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
        local_path: staging_path.to_string(),
    };
    match client.delete_cache_artifact(req).await {
        Ok(_) => {
            info!(
                candidate = %candidate_label,
                staging_path,
                "snapshot reaper: deleted cache artifact via node-mgr",
            );
        }
        Err(status) => {
            warn!(
                candidate = %candidate_label,
                staging_path,
                code = %status.code(),
                message = %status.message(),
                "snapshot reaper: DeleteCacheArtifact RPC failed; staging directory will be retried at next sbx-mgr start",
            );
        }
    }
}

/// Visit one bounded namespace page. Failed deletions stay on disk and are
/// rediscovered on a later sweep, including when disk-pressure GC is disabled.
async fn sweep_orphans(
    scan: &mut OrphanScan,
    kind: OrphanKind,
    artifacts: &LocalArtifacts,
    active_copy_operations: Arc<crate::service::fork_artifact::CopyOperationReservations>,
    ownership: &GcOwnership,
) {
    if ownership.shutdown.is_cancelled() {
        return;
    }
    let root = match kind {
        OrphanKind::GcPending => PathBuf::from(CACHE_ROOT_FOR_SWEEP),
        OrphanKind::ForkCopy => crate::service::fork_artifact::copy_staging_root_under(Path::new(
            pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR,
        )),
    };
    let mut next_scan = std::mem::take(scan);
    let discovery_active_operations = Arc::clone(&active_copy_operations);
    let discovered = tokio::task::spawn_blocking(move || {
        let result = next_scan.next_page(&root, kind, &discovery_active_operations, now_secs());
        (next_scan, result)
    })
    .await;
    let paths = match discovered {
        Ok((next, result)) => {
            *scan = next;
            match result {
                Ok(paths) => paths,
                Err(error) => {
                    warn!(%error, "snapshot reaper: orphan discovery failed; retrying on a later tick");
                    return;
                }
            }
        }
        Err(error) => {
            warn!(%error, "snapshot reaper: orphan discovery task failed; restarting sweep");
            return;
        }
    };
    for path in paths {
        if ownership.shutdown.is_cancelled() {
            break;
        }
        if matches!(kind, OrphanKind::GcPending) {
            let artifacts = artifacts.clone();
            let candidate = path.clone();
            match tokio::task::spawn_blocking(move || gc_pending_is_unowned(&artifacts, &candidate))
                .await
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    warn!(path = %path.display(), %error,
                        "retaining GC staging after ownership lookup task failed");
                    continue;
                }
            }
        }
        let _copy_delete = match kind {
            OrphanKind::GcPending => None,
            OrphanKind::ForkCopy => {
                let Some(lease) = crate::service::fork_artifact::CopyOperationDeleteLease::acquire(
                    Arc::clone(&active_copy_operations),
                    path.clone(),
                ) else {
                    continue;
                };
                Some(lease)
            }
        };
        let Ok(_writer) =
            crate::service::drain::acquire_state_writer(&ownership.drain, "SnapshotGcOrphan")
        else {
            break;
        };
        // Await the admitted mutation; caller cancellation stops the next
        // admission instead of dropping this mutation and its reservation.
        delete_cache_artifact_via_node_mgr(
            &path.to_string_lossy(),
            "orphan-sweep",
            &ownership.node_manager,
        )
        .await;
    }
}

fn gc_pending_is_unowned(artifacts: &LocalArtifacts, path: &Path) -> bool {
    let Some(id) = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.rsplit_once(".gc-pending-"))
        .and_then(|(_, id)| id.parse::<LocalArtifactId>().ok())
    else {
        warn!(path = %path.display(), "retaining GC staging with invalid artifact identity");
        return false;
    };
    match artifacts.gc_staging_is_unowned(id) {
        Ok(unowned) => unowned,
        Err(error) => {
            warn!(path = %path.display(), %error,
                "retaining GC staging after ownership lookup failed");
            false
        }
    }
}

/// Drive the reaper on a recurring interval until `shutdown` fires.
///
/// Orphan sweeps run even when disk-pressure reaping is **disabled**: this is
/// crash/rollout recovery, not GC policy. `.gc-pending-*`
/// directories are logically deleted bytes that would otherwise leak
/// indefinitely if the reaper were turned off after a failed cleanup,
/// while backup-marker repair runs independently in upload maintenance.
///
/// Recovery for `local_artifacts` rows stranded in `restoring` or
/// `deleting` runs synchronously in the service startup path before
/// any background task or RPC can acquire a fresh lease. It must not
/// run from this spawned task because a live restore lease also uses
/// `state = 'restoring'`.
pub(crate) async fn run_loop(
    artifacts: LocalArtifacts,
    db: Arc<Mutex<Connection>>,
    active_fork_copy_operations: Arc<crate::service::fork_artifact::CopyOperationReservations>,
    cfg: GcConfig,
    ownership: GcOwnership,
) {
    let shutdown = &ownership.shutdown;
    if !cfg.enabled {
        info!(
            env_var = GcConfig::ENV_ENABLED,
            "disk-pressure snapshot reaping disabled; bounded orphan recovery remains active",
        );
    }

    info!(
        engage_fraction = cfg.engage_fraction,
        target_fraction = cfg.target_fraction,
        interval_secs = cfg.interval.as_secs(),
        grace_secs = cfg.grace.as_secs(),
        idle_secs = cfg.idle.as_secs(),
        "snapshot reaper loop starting",
    );

    let mut scan = ArtifactScan::default();
    let mut gc_orphans = OrphanScan::default();
    let mut fork_orphans = OrphanScan::default();
    loop {
        // Re-sweep `.gc-pending-*` orphans every tick: a staging
        // directory whose cleanup RPC failed after the row was
        // already deleted is invisible to the planner (no row → not
        // in sampled artifact bytes), so without this retry the bytes would
        // leak until the next process restart.
        if shutdown.is_cancelled() {
            return;
        }
        sweep_orphans(
            &mut gc_orphans,
            OrphanKind::GcPending,
            &artifacts,
            Arc::clone(&active_fork_copy_operations),
            &ownership,
        )
        .await;
        sweep_orphans(
            &mut fork_orphans,
            OrphanKind::ForkCopy,
            &artifacts,
            Arc::clone(&active_fork_copy_operations),
            &ownership,
        )
        .await;
        if shutdown.is_cancelled() {
            return;
        }

        if !cfg.enabled {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(cfg.interval) => {}
            }
            continue;
        }

        let tick_started = Instant::now();
        let tick_result = match crate::disk_usage::read_cache_disk_usage() {
            Some(disk) => {
                pplx_space_metrics::snapshot::gc_disk_usage_fraction(disk.fraction());
                // Cancellation stops candidate admission, not an already-owned
                // blocking rename/commit. Its writer lease also survives panic
                // or cancellation of this supervising future.
                gc_tick(
                    artifacts.clone(),
                    Arc::clone(&db),
                    cfg,
                    now_secs(),
                    disk,
                    &ownership,
                    &mut scan,
                )
                .await
            }
            None => {
                Err("statvfs on the cache filesystem failed; cannot evaluate disk gate".to_string())
            }
        };

        match tick_result {
            Ok(report) => {
                info!(
                    engaged = report.engaged,
                    candidates = report.candidates_planned,
                    deleted = report.deleted,
                    skipped_pinned = report.skipped_pinned,
                    skipped_not_idle = report.skipped_not_idle,
                    skipped_shared_local_path = report.skipped_shared_local_path,
                    skipped_row_missing = report.skipped_row_missing,
                    errors = report.errors,
                    raw_rows_examined = report.raw_rows_examined,
                    sampled_bytes_before = report.sampled_bytes_before,
                    bytes_freed = report.bytes_freed,
                    "snapshot reaper tick complete",
                );
                pplx_space_metrics::snapshot::gc_tick("ok", tick_started);
                if report.engaged {
                    pplx_space_metrics::snapshot::gc_tick_work(
                        report.sampled_bytes_before,
                        report.bytes_freed,
                        report.candidates_planned as u64,
                    );
                }
            }
            Err(err) => {
                warn!(error = %err, "snapshot reaper tick failed");
                pplx_space_metrics::snapshot::gc_tick("error", tick_started);
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(cfg.interval) => {}
        }
    }
}

#[cfg(test)]
mod tests;
