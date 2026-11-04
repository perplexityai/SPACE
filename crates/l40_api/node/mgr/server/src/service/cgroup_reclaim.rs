//! Periodic node-local reclaim of orphaned (leaked) sandbox cgroups.
//!
//! Per-sandbox cleanup retries cgroup removal until the sandbox's Firecracker
//! threads exit, so a transient `EBUSY` self-heals while the sandbox is still
//! tracked. But once a sandbox's network lease/record is gone there is no
//! per-sandbox retry target: an `EBUSY` at that point leaves the cgroup
//! directory behind permanently. This loop sweeps the cgroup base and reclaims
//! directories that are no longer tracked *and* are unpopulated (no live
//! tasks), so it can never disturb a live or racing sandbox.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

use crate::cgroup::{self, OrphanReclaimOutcome};
use crate::slot_pool::NetworkPool;

/// Cadence of the orphan-cgroup sweep.
pub(crate) const RECLAIM_INTERVAL: Duration = Duration::from_secs(120);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ReclaimStats {
    /// Untracked, unpopulated cgroups found this sweep (reclaimed or failed).
    leaked: usize,
    reclaimed: usize,
    failed: usize,
}

/// Spawn the node-local orphan-cgroup reclaim loop.
pub fn start_orphan_cgroup_reclaim(
    pool: Arc<NetworkPool>,
    interval: Duration,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
) {
    task_tracker.spawn(async move {
        run_loop(pool, interval, shutdown_token).await;
    });
}

async fn run_loop(pool: Arc<NetworkPool>, interval: Duration, shutdown: CancellationToken) {
    info!(
        interval_secs = interval.as_secs(),
        "orphan cgroup reclaim loop starting"
    );
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {
                let pool = pool.clone();
                let stats = match tokio::task::spawn_blocking(move || reclaim_pass(&pool)).await {
                    Ok(Ok(stats)) => stats,
                    Ok(Err(error)) => {
                        warn!(%error, "skipping orphan cgroup reclamation: slot inventory unavailable");
                        continue;
                    }
                    Err(error) => {
                        warn!(%error, "orphan cgroup reclaim tick task failed");
                        continue;
                    }
                };
                pplx_space_metrics::sandbox::worker::leaked_cgroups(stats.leaked);
                if stats.reclaimed > 0 || stats.failed > 0 {
                    pplx_space_metrics::sandbox::worker::cgroups_reclaimed(stats.reclaimed as u64);
                    info!(
                        reclaimed = stats.reclaimed,
                        failed = stats.failed,
                        "orphan cgroup reclaim sweep completed"
                    );
                }
            }
        }
    }
}

fn reclaim_pass(pool: &NetworkPool) -> Result<ReclaimStats, crate::slot_pool::SlotStoreError> {
    let records = pool.all_records()?;
    let tracked: HashSet<String> = records
        .into_iter()
        .map(|record| record.info.sandbox_id)
        .filter(|id| !id.is_empty())
        .collect();
    Ok(reclaim_listed(cgroup::list_sandbox_cgroup_dirs(), &tracked))
}

fn reclaim_listed(dirs: Vec<(String, PathBuf)>, tracked: &HashSet<String>) -> ReclaimStats {
    let mut stats = ReclaimStats::default();
    for (id, path) in dirs {
        if tracked.contains(&id) {
            continue;
        }
        match cgroup::reclaim_unpopulated_cgroup(&path) {
            Ok(OrphanReclaimOutcome::Reclaimed) => {
                stats.leaked += 1;
                stats.reclaimed += 1;
            }
            Ok(OrphanReclaimOutcome::Populated) => {
                // Untracked but live: a racing create, or a sandbox whose lease
                // record was lost. Never killed here — the per-sandbox cleanup
                // path owns live teardown. Surfaced for visibility only.
                warn!(
                    sandbox_id = %id,
                    path = %path.display(),
                    "untracked but populated sandbox cgroup; leaving for per-sandbox cleanup"
                );
            }
            Ok(OrphanReclaimOutcome::Absent) => {}
            Err(error) => {
                stats.leaked += 1;
                stats.failed += 1;
                warn!(sandbox_id = %id, %error, "failed to reclaim orphaned sandbox cgroup");
            }
        }
    }
    stats
}

#[cfg(test)]
mod tests;
