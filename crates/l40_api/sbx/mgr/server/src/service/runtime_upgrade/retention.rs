//! Service-layer idle retention and background pruning for
//! `sandbox_runtime_upgrade_attempt_outcomes` and
//! `sandbox_runtime_upgrade_abandoned`.
//!
//! - [`RETENTION_ENV`] (`SPACE_SBXMGR_RUNTIME_UPGRADE_RETENTION_SECS`) defaults
//!   to 7 days (`604800` seconds). Standard falsy values (`0`, `false`, `no`,
//!   `off`, `disabled`) disable pruning (`Option<Duration>::None`), and
//!   unparseable values fail closed (`None` with a warning).
//! - Retries and late mutating RPCs (`UpgradeRuntime`, `Wake` carrying a
//!   runtime upgrade, and `AbandonRuntimeUpgrade`) refresh `last_seen_at`
//!   best-effort when they hit a retained row, without mutating `recorded_at`.
//!   `GetRuntimeUpgrade` stays read-only so `RestartAdmission::ReadOnly` holds.
//! - `start_runtime_upgrade_recovery` waits [`STARTUP_PRUNE_GRACE`] before its
//!   first sweep so in-flight kubelet and control-plane retries after a restart
//!   can check in first, then advances a loop-local
//!   [`RuntimeUpgradeRetentionPruner`] by one bounded batch per recovery tick.

use std::time::Duration;

use crate::db::runtime_upgrade_retention::{
    RuntimeUpgradePruneBatch, RuntimeUpgradeRetentionPruner,
};
use crate::db::{Connection, RuntimeUpgradeOperationId};
use crate::service::SandboxManagerService;

/// Environment variable controlling idle retention of settled runtime-upgrade
/// attempt outcomes and abandonments, in seconds.
pub(crate) const RETENTION_ENV: &str = "SPACE_SBXMGR_RUNTIME_UPGRADE_RETENTION_SECS";

/// Default idle retention window (7 days).
pub(super) const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Values below 1 hour emit a startup warning because delayed caller retries
/// could outlast the window.
pub(super) const RECOMMENDED_MIN_RETENTION: Duration = Duration::from_secs(60 * 60);

/// Grace period after recovery starts before the first prune sweep runs.
pub(super) const STARTUP_PRUNE_GRACE: Duration = Duration::from_secs(15 * 60);

pub(in crate::service) fn retention_from_env() -> Option<Duration> {
    match std::env::var(RETENTION_ENV) {
        Ok(raw) => parse_retention_env(Some(&raw)),
        Err(std::env::VarError::NotPresent) => Some(DEFAULT_RETENTION),
        Err(std::env::VarError::NotUnicode(_)) => {
            tracing::warn!(
                env = RETENTION_ENV,
                "non-UTF-8 runtime-upgrade retention value; disabling retention pruning"
            );
            None
        }
    }
}

pub(super) fn parse_retention_env(raw: Option<&str>) -> Option<Duration> {
    let Some(trimmed) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Some(DEFAULT_RETENTION);
    };
    if pplx_space_env_vars::parse_env_flag(trimmed) == Some(false) {
        tracing::info!(
            env = RETENTION_ENV,
            value = trimmed,
            "runtime-upgrade record retention pruning disabled"
        );
        return None;
    }
    match trimmed.parse::<u64>() {
        Ok(secs) if secs > 0 => {
            let retention = Duration::from_secs(secs);
            if retention < RECOMMENDED_MIN_RETENTION {
                tracing::warn!(
                    env = RETENTION_ENV,
                    retention_secs = secs,
                    recommended_min_secs = RECOMMENDED_MIN_RETENTION.as_secs(),
                    "runtime-upgrade retention is shorter than the recommended 1h minimum"
                );
            }
            Some(retention)
        }
        _ => {
            tracing::warn!(
                env = RETENTION_ENV,
                value = trimmed,
                "invalid runtime-upgrade retention value; disabling retention pruning"
            );
            None
        }
    }
}

pub(super) fn touch_attempt_best_effort(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) {
    if let Err(error) = crate::db::runtime_upgrade_retention::touch_runtime_upgrade_attempt_outcome(
        conn,
        operation_id,
        attempt_id,
    ) {
        tracing::warn!(
            %operation_id,
            attempt_id,
            %error,
            "could not refresh runtime-upgrade attempt outcome last_seen_at"
        );
    }
}

pub(super) fn touch_operation_best_effort(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) {
    if let Err(error) =
        crate::db::runtime_upgrade_retention::touch_retained_runtime_upgrade_operation(
            conn,
            operation_id,
            sandbox_id,
        )
    {
        tracing::warn!(
            %operation_id,
            sandbox_id,
            %error,
            "could not refresh retained runtime-upgrade operation last_seen_at"
        );
    }
}

impl SandboxManagerService {
    /// Run at most one bounded cursor batch of runtime-upgrade retention
    /// pruning when retention is enabled and the service is accepting state
    /// writers.
    pub(in crate::service) async fn prune_retained_runtime_upgrades(
        &self,
        pruner: &mut RuntimeUpgradeRetentionPruner,
    ) -> Option<RuntimeUpgradePruneBatch> {
        let retention = self.runtime_upgrade_retention?;
        let writer = self.state_writer_lease_if_accepting("RuntimeUpgradeRetentionPrune")?;
        let service = self.clone();
        let mut moved_pruner = std::mem::take(pruner);
        let outcome = tokio::task::spawn_blocking(move || {
            let _writer = writer;
            let conn = service.db_conn()?;
            // RPCs lock `runtime_upgrade_lock` before `db`. Acquiring
            // `try_lock_owned` under `db` never waits and stays deadlock-free,
            // and `next_batch_with_clock` samples the wall clock only after the
            // `IMMEDIATE` transaction and candidate idle guards are held.
            let batch = moved_pruner
                .next_batch_with_clock(
                    &conn,
                    retention,
                    |sandbox_id| {
                        service
                            .auto_lifecycle
                            .runtime_upgrade_lock(sandbox_id)
                            .try_lock_owned()
                            .ok()
                    },
                    || match crate::db::now_epoch_i64().and_then(|secs| {
                        u64::try_from(secs).map_err(crate::db::Error::to_sql_conversion)
                    }) {
                        Ok(secs) => Ok(Some(secs)),
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "skipping runtime-upgrade retention prune due to invalid wall clock"
                            );
                            Ok(None)
                        }
                    },
                )
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok::<_, tonic::Status>((moved_pruner, batch))
        })
        .await;
        match outcome {
            Ok(Ok((updated, batch))) => {
                *pruner = updated;
                if let Some(batch) = &batch
                    && batch.total_deleted() > 0
                {
                    tracing::info!(
                        deleted_attempt_outcomes = batch.deleted_attempt_outcomes,
                        deleted_abandoned = batch.deleted_abandoned,
                        retention_secs = retention.as_secs(),
                        "pruned idle runtime-upgrade retention records"
                    );
                }
                batch
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "runtime-upgrade retention prune batch failed");
                None
            }
            Err(join_error) => {
                tracing::warn!(%join_error, "runtime-upgrade retention prune task failed");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests;
