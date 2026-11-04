//! How many codec workers to run, from the configured CPU request and the cgroup CPU quota.

#[allow(unused_imports)]
use super::*;

/// How many CPU-bound zstd passes (compress or decompress) may run at
/// once. `0` means auto-size to the CPU quota, or to the deployed CPU
/// request when the pod has no CPU limit.
///
/// Each zstd pass is single-threaded, and `spawn_blocking`'s pool is ~512
/// threads, so without admission a pull storm runs every decode
/// concurrently on the pod's CPU share — N jobs each at 1/N speed instead
/// of share-many at full speed. Bounding to the share keeps the same
/// aggregate throughput but finishes jobs in arrival order, which is what
/// per-job completion latency is made of.
pub(super) fn resolved_codec_concurrency(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    // Kubernetes enforces CPU *limits* through the cgroup CFS quota, which
    // `available_parallelism` (scheduler-affinity based) does not observe
    // unless the pod is cpuset-pinned. Read the quota directly. Without a
    // quota (the pod runs limit-less), the deployed whole-core CPU request
    // arrives as WORKER_THREADS (Downward API on Kubernetes, Environment=
    // in the direct systemd units) and is the intended share — affinity
    // would size to every core of a metal host. Affinity remains the last
    // resort for undeployed contexts.
    detect_cgroup_cpu_quota_cores()
        .or_else(cpu_request_cores_from_env)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(std::num::NonZeroUsize::get)
        })
        .unwrap_or(2)
        .max(1)
}

/// The whole-core CPU request the deployment stamped into `WORKER_THREADS`,
/// or `None` when unset or malformed.
pub(super) fn cpu_request_cores_from_env() -> Option<usize> {
    parse_cpu_request_cores(&std::env::var("WORKER_THREADS").ok()?)
}

pub(super) fn parse_cpu_request_cores(raw: &str) -> Option<usize> {
    raw.trim().parse().ok().filter(|cores| *cores > 0)
}

/// Cores implied by a CFS `quota`/`period` pair, rounded to the nearest
/// whole core and floored at 1. Integer arithmetic keeps this free of
/// float sign/precision casts.
pub(super) fn cores_from_cfs_quota(quota: u64, period: u64) -> Option<usize> {
    if period == 0 {
        return None;
    }
    let rounded = (quota + period / 2) / period;
    Some(usize::try_from(rounded).unwrap_or(usize::MAX).max(1))
}

/// Read the cgroup CPU quota (in whole cores) the process is limited to,
/// or `None` when unlimited or unreadable. Handles cgroup v2 (`cpu.max`)
/// and v1 (`cpu.cfs_quota_us` / `cpu.cfs_period_us`). Sync file reads at
/// startup only.
pub(super) fn detect_cgroup_cpu_quota_cores() -> Option<usize> {
    // cgroup v2: "$QUOTA $PERIOD" where QUOTA is "max" or microseconds.
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut fields = contents.split_whitespace();
        let quota = fields.next()?;
        let period: u64 = fields.next()?.parse().ok()?;
        if quota == "max" {
            return None;
        }
        return cores_from_cfs_quota(quota.parse().ok()?, period);
    }
    // cgroup v1.
    let quota: i64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    // -1 (unlimited) and any non-positive value have no quota.
    let quota = u64::try_from(quota).ok().filter(|&q| q > 0)?;
    let period: u64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    cores_from_cfs_quota(quota, period)
}
