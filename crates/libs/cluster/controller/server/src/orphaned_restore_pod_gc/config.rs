//! Deploy-time configuration for the orphaned-restore-pod GC, read from
//! `SPACE_ORPHANED_RESTORE_POD_GC_*` environment variables on the cluster
//! controller.

use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;

use crate::env_config;

pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
/// Minimum time a restore pod must have been *continuously observed as
/// provably superseded* (row deleted, no active execution, or active execution
/// at a strictly-higher generation) before it is eligible for deletion.
/// Defense in depth: an in-flight restore's execution claim is inserted at the
/// pod's generation before the pod is created, so the generation fence alone
/// already skips it; the threshold additionally rides out any window where
/// this loop's cached pod view and its Postgres read disagree, while still
/// reclaiming a stranded VM within a few minutes.
pub(crate) const DEFAULT_THRESHOLD: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_MAX_DELETES_PER_MINUTE: u32 = 60;
pub(crate) const DEFAULT_TICK_BUDGET: Duration = Duration::from_secs(45);
/// How long a restore pod may stay terminating before it's reported as a stuck
/// teardown. Sandbox pods carry a 10s termination grace period and no
/// finalizers, so on a healthy node the pod object is gone seconds after the
/// DELETE. One still terminating minutes later means the kubelet never
/// confirmed its containers stopped (typically an unreachable node), so its VM
/// may still be running.
pub(crate) const DEFAULT_TEARDOWN_STUCK_AFTER: Duration = Duration::from_secs(5 * 60);
/// Upper bound on sandbox rows classified per DB round-trip. The candidate
/// scan is pod-driven (every restore pod in the reflector cache), so the owning
/// rows are looked up in `ANY($1)` chunks of this size to bound a single query.
pub(crate) const DEFAULT_BATCH_SIZE: u32 = 500;

/// Ceilings on operator-set values; a larger value clamps to them. The counts
/// bound apiserver and Postgres load, so a typo with extra digits mustn't lift
/// them. The duration ceiling keeps deadline arithmetic (`now + tick_budget`)
/// far from overflow.
pub(crate) const MAX_DELETES_PER_MINUTE_CEILING: u32 = 10 * DEFAULT_MAX_DELETES_PER_MINUTE;
pub(crate) const BATCH_SIZE_CEILING: u32 = 10 * DEFAULT_BATCH_SIZE;
pub(crate) const DURATION_CEILING: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    /// Master enable gate. Off by default everywhere; each environment opts in
    /// via `SPACE_ORPHANED_RESTORE_POD_GC_ENABLED=true`.
    pub enabled: bool,
    /// When true (the default), orphans are classified and reported via logs,
    /// metrics, and `cluster_reconciler.outcome` (`mode:dry_run`), but nothing
    /// is deleted and no `space.sandbox` cleanup event is emitted.
    pub dry_run: bool,
    /// Tick cadence.
    pub interval: Duration,
    /// Minimum continuous-orphan observation before a pod is eligible for
    /// deletion. Always positive — a zero threshold would act on the first
    /// cached observation, with no margin for cache-vs-Postgres skew.
    pub threshold: PositiveDuration,
    /// Ceiling on orphans acted on per rolling minute. Each costs at most one
    /// apiserver GET (verify) plus one DELETE, so this bounds the reaping's
    /// apiserver load, not just successful deletes. Stuck-teardown checks are
    /// separately capped per tick and aren't charged here.
    pub max_deletes_per_minute: u32,
    /// Wall-clock budget for one tick: the owning-row classification plus the
    /// per-candidate action phase. Once it elapses, remaining orphans defer to
    /// the next tick (a classification that overruns it acts on nothing).
    /// Should stay below `interval` so ticks don't bunch.
    pub tick_budget: Duration,
    /// Chunk size for the owning-row classification query (`ANY($1)`).
    pub batch_size: u32,
    /// How long a restore pod may stay terminating (after this loop's DELETE,
    /// or since this loop first saw it terminating) before it's reported as
    /// `teardown_stuck`.
    pub teardown_stuck_after: PositiveDuration,
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
            teardown_stuck_after: PositiveDuration::new_panic(DEFAULT_TEARDOWN_STUCK_AFTER),
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
                .truthy_default("SPACE_ORPHANED_RESTORE_POD_GC_ENABLED", default.enabled),
            dry_run: config
                .truthy_default("SPACE_ORPHANED_RESTORE_POD_GC_DRY_RUN", default.dry_run),
            interval: config.bounded_duration_secs(
                "SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS",
                default.interval,
                DURATION_CEILING,
            ),
            threshold: config.bounded_positive_duration_secs(
                "SPACE_ORPHANED_RESTORE_POD_GC_THRESHOLD_SECS",
                default.threshold,
                DURATION_CEILING,
            ),
            max_deletes_per_minute: config.bounded_u32(
                "SPACE_ORPHANED_RESTORE_POD_GC_MAX_DELETES_PER_MINUTE",
                default.max_deletes_per_minute,
                MAX_DELETES_PER_MINUTE_CEILING,
            ),
            tick_budget: config.bounded_duration_secs(
                "SPACE_ORPHANED_RESTORE_POD_GC_TICK_BUDGET_SECS",
                default.tick_budget,
                DURATION_CEILING,
            ),
            batch_size: config.bounded_u32(
                "SPACE_ORPHANED_RESTORE_POD_GC_BATCH_SIZE",
                default.batch_size,
                BATCH_SIZE_CEILING,
            ),
            teardown_stuck_after: config.bounded_positive_duration_secs(
                "SPACE_ORPHANED_RESTORE_POD_GC_TEARDOWN_STUCK_AFTER_SECS",
                default.teardown_stuck_after,
                DURATION_CEILING,
            ),
        }
    }
}
