//! Cluster-controller host for the reusable snapshot-retention state machine.
//!
//! The `pplx-space-snapshot-retention` library owns candidate selection,
//! claim/recovery/finalize ordering, storage routing, and the Postgres workflow.
//! This module owns deployment configuration and the controller process loop.

use std::num::NonZeroU32;
use std::time::Duration;

use pplx_space_common_core::types::{
    ClusterId, SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_ENV, process_cluster_id,
};
use pplx_space_feature_flags::{FeatureFlagClient, JsonFlag};
use pplx_space_snapshot_retention::backend::StorageBackendRegistry;
use tokio_util::sync::CancellationToken;

use crate::env_config;

pub use pplx_space_snapshot_retention::*;

#[cfg(test)]
mod tests;

pub(crate) const DEFAULT_TICK: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_MAX_PER_TICK: u32 = 100;
pub(crate) const MAX_PER_TICK_CEILING: u32 = 10_000;
pub(crate) const DEFAULT_KEEP_N: u32 = 3;
pub(crate) const DEFAULT_MIN_AGE: Duration = Duration::from_secs(
    pplx_space_common_core::types::SNAPSHOT_RETENTION_SWEEPER_DEFAULT_MIN_AGE_SECONDS,
);

/// Declared here rather than in the flags crate because only this reconciler
/// reads it. A JSON flag declares no fallback: the caller owns validation and
/// its own local configuration, which is what `effective_limits` below
/// supplies.
///
/// Per field for an *absent* field, all-or-nothing for an invalid one — an
/// override that fails validation reverts both limits to the environment's
/// rather than salvaging the field that parsed.
const RETENTION_CONFIG_FLAG: JsonFlag = JsonFlag::new("space-snapshot-retention-config");

// A flag provider may retune throughput, but never retention eligibility or loop timing.
fn effective_limits(
    config: &ReconcilerConfig,
    cluster_id: Option<&ClusterId>,
    mut json_value: impl FnMut(JsonFlag, &str) -> Option<serde_json::Value>,
) -> (NonZeroU32, NonZeroU32) {
    let fallback = (
        NonZeroU32::new(config.max_per_tick).expect("validated environment limit is positive"),
        config.max_concurrent_deletes,
    );
    let Some(value) = cluster_id.and_then(|id| json_value(RETENTION_CONFIG_FLAG, id.as_str()))
    else {
        return fallback;
    };
    // Validate the entire cached assignment before applying either field.
    let parsed = (|| {
        let object = value.as_object()?;
        let bounded = |key, default| match object.get(key) {
            None => Some(default),
            Some(value) => {
                let value = value.as_u64()?;
                if !(1..=u64::from(MAX_PER_TICK_CEILING)).contains(&value) {
                    return None;
                }
                NonZeroU32::new(u32::try_from(value).ok()?)
            }
        };
        Some((
            bounded("max_per_tick", fallback.0)?,
            bounded("max_concurrent_deletes", fallback.1)?,
        ))
    })();
    parsed.unwrap_or_else(|| {
        tracing::warn!(
            flag_key = RETENTION_CONFIG_FLAG.key(),
            max_per_tick = fallback.0.get(),
            max_concurrent_deletes = fallback.1.get(),
            "invalid snapshot-retention config; using full environment fallback"
        );
        fallback
    })
}

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub dry_run: bool,
    pub tick: Duration,
    pub keep_n: u32,
    pub max_per_tick: u32,
    pub max_concurrent_deletes: NonZeroU32,
    pub min_age: Option<Duration>,
    pub scoped_bucket: Option<String>,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: false,
            tick: DEFAULT_TICK,
            keep_n: DEFAULT_KEEP_N,
            max_per_tick: DEFAULT_MAX_PER_TICK,
            max_concurrent_deletes: NonZeroU32::MIN,
            min_age: Some(DEFAULT_MIN_AGE),
            scoped_bucket: None,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let config = env_config::EnvConfig(env);
        let default = Self::default();
        Self {
            enabled: config
                .truthy_default("SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED", default.enabled),
            dry_run: config
                .safe_mode_flag("SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN", default.dry_run),
            tick: config
                .test_duration_secs("SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS", default.tick),
            keep_n: config.positive_u32("SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N", default.keep_n),
            max_per_tick: config.bounded_u32(
                "SPACE_SNAPSHOT_RETENTION_SWEEPER_MAX_PER_TICK",
                default.max_per_tick,
                MAX_PER_TICK_CEILING,
            ),
            max_concurrent_deletes: default.max_concurrent_deletes,
            min_age: config.test_optional_duration_secs(
                SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_ENV,
                default.min_age,
            ),
            scoped_bucket: env
                .var("SPACE_SNAPSHOT_BUCKET")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        }
    }
}

impl From<ReconcilerConfig> for RetentionPolicy {
    fn from(config: ReconcilerConfig) -> Self {
        Self {
            dry_run: config.dry_run,
            keep_n: config.keep_n,
            max_per_tick: config.max_per_tick,
            max_concurrent_deletes: config.max_concurrent_deletes,
            min_age: config.min_age,
        }
    }
}

/// Drive the application-owned interval until shutdown.
pub async fn run<S: RetentionSink>(
    config: ReconcilerConfig,
    sink: S,
    registry: StorageBackendRegistry,
    feature_flags: FeatureFlagClient,
    shutdown_token: CancellationToken,
) {
    let cluster_id = process_cluster_id();
    if cluster_id.is_none() {
        tracing::warn!(
            "snapshot-retention flag overrides require SPACE_CLUSTER_ID; using environment limits"
        );
    }
    let limits = limit_source(config.clone(), cluster_id, feature_flags);
    run_with_limits(config, sink, registry, shutdown_token, limits).await;
}

/// The per-tick limit resolution `run` installs: the environment's limits,
/// retuned by a provider override when the env master allows one.
///
/// A named function rather than a closure inline in `run` so the policy is
/// reachable from a test. `run_with_limits` refuses to loop at all when the
/// sweeper is disabled, so on that path the env master below is unreachable
/// today — and a policy that holds only because of where its caller happens
/// to return early is one nobody can check, which is the whole failure this
/// issue exists to stop.
fn limit_source(
    config: ReconcilerConfig,
    cluster_id: Option<&'static ClusterId>,
    feature_flags: FeatureFlagClient,
) -> impl FnMut() -> (NonZeroU32, NonZeroU32) {
    move || {
        effective_limits(&config, cluster_id, |flag, subject| {
            // Env-master-first: `SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED` is
            // the hard off, so a provider-supplied override cannot retune a
            // sweeper the environment turned off.
            //
            // `resolved()` collapses "master off" into "nothing resolved", which
            // is sound here because `run_with_limits` below refuses to loop at
            // all when the sweeper is disabled — a disabled master never reaches
            // a tick that could consult these limits.
            feature_flags
                .json_override(config.enabled, flag, subject)
                .resolved()
        })
    }
}

async fn run_with_limits<S: RetentionSink>(
    config: ReconcilerConfig,
    sink: S,
    registry: StorageBackendRegistry,
    shutdown_token: CancellationToken,
    mut limits: impl FnMut() -> (NonZeroU32, NonZeroU32),
) {
    if !config.enabled {
        tracing::info!("snapshot-retention-sweeper is disabled; waiting for shutdown");
        shutdown_token.cancelled().await;
        tracing::info!("snapshot-retention-sweeper stopping");
        return;
    }
    if registry.is_empty() {
        tracing::error!(
            "snapshot-retention-sweeper enabled but no storage backend is registered; refusing to run"
        );
        shutdown_token.cancelled().await;
        tracing::info!("snapshot-retention-sweeper stopping");
        return;
    }

    tracing::info!(
        dry_run = config.dry_run,
        tick_secs = config.tick.as_secs(),
        keep_n = config.keep_n,
        max_per_tick = config.max_per_tick,
        max_concurrent_deletes = config.max_concurrent_deletes.get(),
        min_age_secs = ?config.min_age.map(|duration| duration.as_secs()),
        backends = ?registry.schemes(),
        "starting snapshot-retention-sweeper"
    );
    let tick = config.tick;
    let mut reconciler = Reconciler::new(config, sink, registry);
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Resolve once before work starts. Keep this reconciler (and its
                // cursors) alive; an in-flight sweep finishes with these limits.
                let (max_per_tick, max_concurrent_deletes) = limits();
                reconciler.set_work_limits(max_per_tick, max_concurrent_deletes);
                tracing::info!(max_per_tick = max_per_tick.get(),
                    max_concurrent_deletes = max_concurrent_deletes.get(),
                    "snapshot-retention-sweeper effective limits");
                let _ = reconciler.tick().await;
            }
            _ = shutdown_token.cancelled() => break,
        }
    }

    tracing::info!("snapshot-retention-sweeper stopping");
}
