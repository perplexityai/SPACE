//! Periodic btrfs unallocated-space reclaim for `/space/cache`.
//!
//! Sandbox and template churn frees space *inside* data block groups without
//! returning the chunks to the unallocated pool, so a node can end up with
//! the whole device allocated to sparsely-used data chunks. Once the
//! unallocated pool is gone, metadata can no longer grow a new chunk and the
//! filesystem wedges: subvolume deletion fails `EROFS` even though data
//! writes still succeed, and the only recovery is draining the node. This
//! loop watches the unallocated pool and, when it drops below a low
//! watermark *while* enough reclaimable slack sits inside data chunks, runs
//! an escalating ladder of bounded `btrfs balance` data stages until a
//! target watermark of unallocated space is restored.
//!
//! The `usage=0` stage runs first because it drops fully-empty chunks
//! without relocating anything — it works even when the unallocated pool is
//! too small for a relocation destination. Metadata chunks are never
//! balanced.
//!
//! The usage gauges are emitted every tick even when balancing is disabled,
//! so fleet monitoring sees unallocated-space pressure regardless of whether
//! remediation is armed.

use std::path::Path;
use std::time::{Duration, Instant};

use pplx_space_common_core::host::btrfs::{
    FilesystemUsage, balance_running, read_filesystem_usage, start_data_balance,
};
use pplx_space_common_core::host::linux::paths::SPACE_CACHE_DIR;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

/// Emergency kill-switch. Balancing is on by default; the loop keeps
/// emitting the usage gauges even when this is set to `false`.
const ENV_ENABLED: &str = "SPACE_NODEMGR_BTRFS_BALANCE_ENABLED";

const GIB: u64 = 1 << 30;
/// Cadence of the read-only usage probe and gauge emission. Each tick is one
/// `btrfs filesystem usage` call bounded at 2s.
const TICK_INTERVAL: Duration = Duration::from_secs(300);
/// Minimum time between balance passes on a node. Bounds relocation IO to at
/// most ~4 ladder passes per day regardless of how fast the node re-crosses
/// the trigger watermark; the gauges keep their tick cadence.
const BALANCE_PASS_COOLDOWN: Duration = Duration::from_secs(6 * 60 * 60);
/// Balancing triggers below this much unallocated space...
const UNALLOCATED_LOW_BYTES: u64 = 100 * GIB;
/// ...and stops once this much is restored. Must not sit below the trigger
/// watermark or every pass would end "ineffective" by construction.
const UNALLOCATED_TARGET_BYTES: u64 = 200 * GIB;
/// Minimum free space trapped inside allocated data chunks for a balance to
/// be worth starting; below this the node's space is held by live data and
/// needs eviction/drain instead.
const MIN_DATA_SLACK_BYTES: u64 = 100 * GIB;
/// Data block groups relocated per ladder stage. A data chunk is 1 GiB per
/// device in the profile (a 4 GiB stripe on the 4x NVMe RAID0 workers), so
/// 50 chunks caps a stage at ~200 GiB reclaimed; relocation IO is bounded
/// by the stage's usage filter times that (~100 GiB at dusage=50).
const STAGE_CHUNK_LIMIT: u32 = 50;

/// Escalating data-chunk usage-percent filters. Later stages relocate fuller
/// chunks and cost proportionally more IO, so each is tried only if the
/// previous stages did not restore the target watermark.
const DUSAGE_LADDER: [u8; 4] = [0, 10, 25, 50];
const BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(10);
/// Upper bound on waiting for one stage. A stage that exceeds it keeps
/// running in the kernel; the next tick observes it via `balance_running`
/// and stays out of the way.
const MAX_STAGE_WAIT: Duration = Duration::from_secs(30 * 60);

pub(crate) fn enabled_from_env() -> bool {
    match std::env::var(ENV_ENABLED) {
        Ok(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no"
        ),
        Err(_) => true,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BalanceDecision {
    /// Unallocated pool at or above the low watermark; nothing to do.
    Healthy,
    /// Unallocated pool is low and data chunks hold enough reclaimable slack
    /// for a balance to restore it.
    BalanceNeeded,
    /// Unallocated pool is low but data chunks are packed: balance cannot
    /// reclaim meaningful space. The node needs sandbox eviction or a drain,
    /// not a balance.
    InsufficientSlack,
}

fn evaluate(usage: FilesystemUsage) -> BalanceDecision {
    if usage.device_unallocated_bytes >= UNALLOCATED_LOW_BYTES {
        return BalanceDecision::Healthy;
    }
    if usage.data_slack_bytes() < MIN_DATA_SLACK_BYTES {
        return BalanceDecision::InsufficientSlack;
    }
    BalanceDecision::BalanceNeeded
}

/// Spawn the btrfs balance reconciler loop.
pub fn start_btrfs_balance_reconciler(
    enabled: bool,
    shutdown_token: CancellationToken,
    task_tracker: &TaskTracker,
) {
    task_tracker.spawn(async move {
        run_loop(enabled, shutdown_token).await;
    });
}

async fn run_loop(enabled: bool, shutdown: CancellationToken) {
    info!(
        enabled,
        interval_secs = TICK_INTERVAL.as_secs(),
        pass_cooldown_secs = BALANCE_PASS_COOLDOWN.as_secs(),
        unallocated_low_bytes = UNALLOCATED_LOW_BYTES,
        unallocated_target_bytes = UNALLOCATED_TARGET_BYTES,
        min_data_slack_bytes = MIN_DATA_SLACK_BYTES,
        chunk_limit = STAGE_CHUNK_LIMIT,
        "btrfs balance reconciler loop starting"
    );
    let mut ticker = tokio::time::interval(TICK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_pass_started_at: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => run_tick(enabled, &mut last_pass_started_at, &shutdown).await,
        }
    }
}

async fn run_tick(
    enabled: bool,
    last_pass_started_at: &mut Option<Instant>,
    shutdown: &CancellationToken,
) {
    let Some(usage) = probe_usage().await else {
        return;
    };
    emit_usage_gauges(usage);

    let decision = evaluate(usage);
    pplx_space_metrics::node_disk::btrfs_balance_needed(decision == BalanceDecision::BalanceNeeded);
    match decision {
        BalanceDecision::Healthy => return,
        BalanceDecision::InsufficientSlack => {
            warn!(
                device_unallocated_bytes = usage.device_unallocated_bytes,
                data_slack_bytes = usage.data_slack_bytes(),
                metadata_available_bytes = usage.metadata_available_bytes(),
                "unallocated pool is low but data chunks are packed; balance cannot help — node needs sandbox eviction"
            );
            return;
        }
        BalanceDecision::BalanceNeeded => {}
    }

    if !enabled {
        info!(
            device_unallocated_bytes = usage.device_unallocated_bytes,
            data_slack_bytes = usage.data_slack_bytes(),
            "balance needed but the reconciler kill-switch is set; unset SPACE_NODEMGR_BTRFS_BALANCE_ENABLED to re-arm"
        );
        return;
    }

    if let Some(started_at) = last_pass_started_at {
        let since_last_pass = started_at.elapsed();
        if since_last_pass < BALANCE_PASS_COOLDOWN {
            debug!(
                since_last_pass_secs = since_last_pass.as_secs(),
                cooldown_secs = BALANCE_PASS_COOLDOWN.as_secs(),
                "balance needed but pass cooldown is active"
            );
            return;
        }
    }

    match tokio::task::spawn_blocking(|| balance_running(Path::new(SPACE_CACHE_DIR))).await {
        Ok(Ok(false)) => {}
        Ok(Ok(true)) => {
            info!("a btrfs balance is already running; skipping this tick");
            return;
        }
        Ok(Err(error)) => {
            warn!(%error, "btrfs balance status probe failed");
            return;
        }
        Err(error) => {
            warn!(%error, "btrfs balance status probe task failed");
            return;
        }
    }

    *last_pass_started_at = Some(Instant::now());
    run_balance_ladder(usage, shutdown).await;
}

async fn run_balance_ladder(initial: FilesystemUsage, shutdown: &CancellationToken) {
    let mut current = initial;
    for dusage in DUSAGE_LADDER {
        if shutdown.is_cancelled() {
            return;
        }
        // usage=0 drops empty chunks without relocation, so it needs no
        // bound; relocating stages are capped to keep per-pass IO bounded.
        let chunk_limit = (dusage > 0).then_some(STAGE_CHUNK_LIMIT);
        let started_at = Instant::now();
        let start = tokio::task::spawn_blocking(move || {
            start_data_balance(Path::new(SPACE_CACHE_DIR), dusage, chunk_limit)
        })
        .await;
        match start {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, dusage, "btrfs balance stage failed to start");
                break;
            }
            Err(error) => {
                warn!(%error, dusage, "btrfs balance stage start task failed");
                break;
            }
        }
        pplx_space_metrics::node_disk::btrfs_balance_stage_runs(dusage);

        if !wait_for_balance_idle(shutdown).await {
            return;
        }
        let Some(usage) = probe_usage().await else {
            break;
        };
        current = usage;
        emit_usage_gauges(current);
        info!(
            dusage,
            elapsed_secs = started_at.elapsed().as_secs(),
            device_unallocated_bytes = current.device_unallocated_bytes,
            "btrfs balance stage complete"
        );
        if current.device_unallocated_bytes >= UNALLOCATED_TARGET_BYTES {
            break;
        }
    }

    let reclaimed_bytes = current
        .device_unallocated_bytes
        .saturating_sub(initial.device_unallocated_bytes);
    pplx_space_metrics::node_disk::btrfs_balance_reclaimed_bytes(reclaimed_bytes);
    if current.device_unallocated_bytes < UNALLOCATED_LOW_BYTES {
        pplx_space_metrics::node_disk::btrfs_balance_ineffective();
        warn!(
            reclaimed_bytes,
            device_unallocated_bytes = current.device_unallocated_bytes,
            data_slack_bytes = current.data_slack_bytes(),
            "btrfs balance ladder exhausted below the low watermark; node likely needs sandbox eviction"
        );
    } else {
        info!(
            reclaimed_bytes,
            device_unallocated_bytes = current.device_unallocated_bytes,
            "btrfs balance pass restored unallocated headroom"
        );
    }
}

/// Poll until no balance is running. Returns `false` when shutdown fired,
/// the stage outlived [`MAX_STAGE_WAIT`], or the status probe failed — in
/// all three cases any kernel-side balance keeps running on its own and a
/// later tick picks the filesystem back up.
///
/// Sleeps before the first probe: `balance start --bg` forks, and the parent
/// can return before the child has issued the balance ioctl, so an immediate
/// status check could see idle and skip ahead of the stage it just started.
async fn wait_for_balance_idle(shutdown: &CancellationToken) -> bool {
    let started_at = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return false,
            _ = tokio::time::sleep(BALANCE_POLL_INTERVAL) => {}
        }
        match tokio::task::spawn_blocking(|| balance_running(Path::new(SPACE_CACHE_DIR))).await {
            Ok(Ok(false)) => return true,
            Ok(Ok(true)) => {}
            Ok(Err(error)) => {
                warn!(%error, "btrfs balance status poll failed");
                return false;
            }
            Err(error) => {
                warn!(%error, "btrfs balance status poll task failed");
                return false;
            }
        }
        if started_at.elapsed() >= MAX_STAGE_WAIT {
            warn!(
                elapsed_secs = started_at.elapsed().as_secs(),
                "btrfs balance stage exceeded its wait bound; leaving it running"
            );
            return false;
        }
    }
}

async fn probe_usage() -> Option<FilesystemUsage> {
    match tokio::task::spawn_blocking(|| read_filesystem_usage(Path::new(SPACE_CACHE_DIR))).await {
        Ok(Ok(usage)) => Some(usage),
        Ok(Err(error)) => {
            warn!(%error, "btrfs filesystem usage probe failed");
            None
        }
        Err(error) => {
            warn!(%error, "btrfs filesystem usage probe task failed");
            None
        }
    }
}

fn emit_usage_gauges(usage: FilesystemUsage) {
    pplx_space_metrics::node_disk::btrfs_unallocated_bytes(usage.device_unallocated_bytes);
    pplx_space_metrics::node_disk::btrfs_data_slack_bytes(usage.data_slack_bytes());
    pplx_space_metrics::node_disk::btrfs_metadata_available_bytes(usage.metadata_available_bytes());
}

#[cfg(test)]
mod tests;
