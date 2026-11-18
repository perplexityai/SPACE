//! Deploy-time configuration for the retired-execution pod GC, read from
//! `SPACE_RETIRED_EXECUTION_POD_GC_*` environment variables on the cluster
//! controller.

use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;

use crate::env_config;

pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
/// Minimum time a pod must have been continuously observed as reapable before
/// it is deleted. A retired generation is already final, so this only rides
/// out cache-vs-Postgres skew and lets the owning writer's own pod delete (the
/// API's suspend or cancel cleanup) land first.
pub(crate) const DEFAULT_THRESHOLD: Duration = Duration::from_secs(300);
/// Pods acted on per rolling minute. The steady-state Succeeded backlog on a
/// busy cluster is in the low thousands per day, so this clears a full
/// `terminated-pod-gc-threshold` (12,500) backlog in under two hours without
/// a burst of apiserver DELETEs.
pub(crate) const DEFAULT_MAX_DELETES_PER_MINUTE: u32 = 120;
pub(crate) const DEFAULT_TICK_BUDGET: Duration = Duration::from_secs(45);
/// Upper bound on pod names looked up per DB round-trip (`ANY($1)`).
pub(crate) const DEFAULT_BATCH_SIZE: u32 = 500;

/// Ceilings on operator-set values; a larger value clamps to them.
pub(crate) const MAX_DELETES_PER_MINUTE_CEILING: u32 = 10 * DEFAULT_MAX_DELETES_PER_MINUTE;
pub(crate) const BATCH_SIZE_CEILING: u32 = 10 * DEFAULT_BATCH_SIZE;
pub(crate) const DURATION_CEILING: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default everywhere; each environment opts in
    /// via `SPACE_RETIRED_EXECUTION_POD_GC_ENABLED=true`.
    pub enabled: bool,
    /// When true (the default), reapable pods are classified, verified and
    /// reported, but nothing is deleted.
    pub dry_run: bool,
    /// Tick cadence.
    pub interval: Duration,
    /// Minimum continuous reapable observation before a pod is deleted.
    pub threshold: PositiveDuration,
    /// Ceiling on pods acted on per rolling minute. Each costs at most one
    /// verify GET, one row read and one DELETE.
    pub max_deletes_per_minute: u32,
    /// Wall-clock budget for one tick; remaining candidates defer to the next.
    pub tick_budget: Duration,
    /// Chunk size for the execution-row lookup query (`ANY($1)`).
    pub batch_size: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: true,
            interval: DEFAULT_INTERVAL,
            threshold: PositiveDuration::new_panic(DEFAULT_THRESHOLD),
            max_deletes_per_minute: DEFAULT_MAX_DELETES_PER_MINUTE,
            tick_budget: DEFAULT_TICK_BUDGET,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl ReconcilerConfig {
    /// Read config from env vars, falling back to defaults for any var that is
    /// missing or unparseable. Durations are whole seconds; a zero duration or
    /// count keeps the default too, and a value above its ceiling clamps to it.
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config
                .truthy_default("SPACE_RETIRED_EXECUTION_POD_GC_ENABLED", default.enabled),
            dry_run: config
                .truthy_default("SPACE_RETIRED_EXECUTION_POD_GC_DRY_RUN", default.dry_run),
            interval: config.bounded_duration_secs(
                "SPACE_RETIRED_EXECUTION_POD_GC_INTERVAL_SECS",
                default.interval,
                DURATION_CEILING,
            ),
            threshold: config.bounded_positive_duration_secs(
                "SPACE_RETIRED_EXECUTION_POD_GC_THRESHOLD_SECS",
                default.threshold,
                DURATION_CEILING,
            ),
            max_deletes_per_minute: config.bounded_u32(
                "SPACE_RETIRED_EXECUTION_POD_GC_MAX_DELETES_PER_MINUTE",
                default.max_deletes_per_minute,
                MAX_DELETES_PER_MINUTE_CEILING,
            ),
            tick_budget: config.bounded_duration_secs(
                "SPACE_RETIRED_EXECUTION_POD_GC_TICK_BUDGET_SECS",
                default.tick_budget,
                DURATION_CEILING,
            ),
            batch_size: config.bounded_u32(
                "SPACE_RETIRED_EXECUTION_POD_GC_BATCH_SIZE",
                default.batch_size,
                BATCH_SIZE_CEILING,
            ),
        }
    }
}
