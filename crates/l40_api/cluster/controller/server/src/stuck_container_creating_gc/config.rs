//! Deploy-time configuration for the stuck-ContainerCreating pod GC. Helm
//! renders these under `clusterController.reconcilers.stuckContainerCreatingGc.*`.

use std::time::Duration;

use crate::env_config;

pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_THRESHOLD: Duration = Duration::from_secs(600);
pub(crate) const DEFAULT_MAX_DELETES_PER_MINUTE: u32 = 60;
pub(crate) const DEFAULT_TICK_BUDGET: Duration = Duration::from_secs(45);

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default everywhere; each environment opts in
    /// via `SPACE_STUCK_CONTAINER_CREATING_GC_ENABLED=true`.
    pub enabled: bool,
    /// Tick cadence.
    pub interval: Duration,
    /// Minimum time a pod must have been stuck in `ContainerCreating` before it
    /// is eligible for deletion.
    pub threshold: Duration,
    /// Ceiling on candidates acted on per rolling minute. Each costs at most
    /// one apiserver GET (verify) plus one DELETE, so this bounds the loop's
    /// total apiserver load, not just successful deletes.
    pub max_deletes_per_minute: u32,
    /// Wall-clock budget for the per-candidate action phase of a tick. Once it
    /// elapses, remaining candidates defer to the next tick. Bounds tick
    /// duration when the apiserver is slow (each GET/DELETE can otherwise take
    /// the kube client's multi-minute timeout), so the loop keeps scanning
    /// during the very incident it exists to relieve. Should stay below
    /// `interval` so ticks don't bunch.
    pub tick_budget: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: DEFAULT_INTERVAL,
            threshold: DEFAULT_THRESHOLD,
            max_deletes_per_minute: DEFAULT_MAX_DELETES_PER_MINUTE,
            tick_budget: DEFAULT_TICK_BUDGET,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars, falling back to defaults for any var that is
    /// missing or unparseable.
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default(
                "SPACE_STUCK_CONTAINER_CREATING_GC_ENABLED",
                default.enabled,
            ),
            interval: default.interval,
            threshold: default.threshold,
            // A safety rate limit that non-production clusters deliberately
            // lower, so it stays configurable.
            max_deletes_per_minute: env_config::positive_u32(
                "SPACE_STUCK_CONTAINER_CREATING_GC_MAX_DELETES_PER_MINUTE",
                default.max_deletes_per_minute,
            ),
            tick_budget: default.tick_budget,
        }
    }
}
