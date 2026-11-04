//! Filesystem-usage gates shared by the disk-gated reclaim loops
//! (`crate::snapshot_gc`, `crate::sandbox_eviction`).
//!
//! Both loops trigger on the **fraction of the `/space/cache`
//! filesystem in use** (one cheap `statvfs` call), not on summed
//! directory sizes: the fraction is the actual node-health signal —
//! it sees every consumer, including ones a single loop can't reclaim
//! — and is portable across large and small node filesystems.
//!
//! The measurement itself lives in `pplx-space-common-core` so the
//! node-ctl Kubernetes SSD-headroom tier label reads the identical signal;
//! this module keeps the eviction-specific hysteresis gates.

use std::time::Duration;

pub use pplx_space_common_core::host::disk::{DiskUsage, read_cache_disk_usage};

/// Shared validation for an `(engage, target)` hysteresis pair: both
/// clamped to `[0, 1]`; a `target >= engage` misconfiguration warns
/// loudly and derives `target = engage - 0.10` (floored at 0) rather
/// than disabling the loop — mirroring the snapshot reaper's
/// warn-don't-silently-fix grace-floor convention. Returns
/// `(engage, target)`.
pub fn validate_fraction_pair(
    loop_name: &str,
    engage: f64,
    target: f64,
    engage_env: &str,
    target_env: &str,
) -> (f64, f64) {
    let engage = if engage.is_finite() {
        engage.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let target = if target.is_finite() {
        target.clamp(0.0, 1.0)
    } else {
        0.0
    };
    if target >= engage && engage > 0.0 {
        let derived = (engage - 0.10).max(0.0);
        tracing::warn!(
            loop_name,
            engage_fraction = engage,
            target_fraction = target,
            derived_target = derived,
            engage_env,
            target_env,
            "disk-gate target fraction is not below the engage fraction; deriving target = engage - 0.10 — fix the configuration",
        );
        return (engage, derived);
    }
    (engage, target)
}

/// Hysteresis gate: a loop engages when usage reaches the engage
/// fraction and, once engaged, keeps reclaiming until usage falls
/// below the target fraction. With `engage == 0.0` the gate is
/// always open (always-on reclaim); with `engage == 1.0` it can
/// only open on a literally full filesystem (residue-only mode).
pub fn gate_engaged(usage_fraction: f64, engage_fraction: f64) -> bool {
    usage_fraction >= engage_fraction
}

/// Coarse retry pacing for loops that could not read disk usage —
/// not a config knob; failing statvfs means something is wrong with
/// the mount and hammering it helps nobody.
pub const USAGE_READ_FAILURE_BACKOFF: Duration = Duration::from_secs(60);

#[cfg(test)]
mod tests;
