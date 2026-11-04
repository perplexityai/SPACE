use pplx_space_common_core::sql;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::env_config;

const DEFAULT_TICK: Duration = Duration::from_secs(300);
const DEFAULT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_MAX_RECORDS: u32 = 100_000;
const DEFAULT_MAX_DELETE_PER_TICK: u32 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub tick: Duration,
    pub retention: Duration,
    pub max_records: u32,
    pub max_delete_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            retention: DEFAULT_RETENTION,
            max_records: DEFAULT_MAX_RECORDS,
            max_delete_per_tick: DEFAULT_MAX_DELETE_PER_TICK,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            tick: default.tick,
            retention: env_config::duration_secs(
                "SPACE_ADMIN_AUDIT_RETENTION_GRACE_SECS",
                default.retention,
            ),
            max_records: env_config::EnvConfig(&pplx_space_common_core::env::ProcessEnvironment)
                .bounded_u32(
                    "SPACE_ADMIN_AUDIT_RETENTION_MAX_RECORDS",
                    default.max_records,
                    10_000_000,
                ),
            max_delete_per_tick: default.max_delete_per_tick,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    pub aged_deleted: u64,
    pub overflow_deleted: u64,
    pub retained_rows: u64,
}

impl TickReport {
    fn total_deleted(self) -> u64 {
        self.aged_deleted.saturating_add(self.overflow_deleted)
    }
}

pub trait RetentionSink: Send + Sync + 'static {
    fn prune_audit_events(
        &self,
        retention: Duration,
        max_records: u32,
        max_delete_per_tick: u32,
    ) -> impl std::future::Future<Output = Result<TickReport, sqlx::Error>> + Send;
}

#[derive(Clone)]
pub struct AdminAuditRetentionStore {
    pool: PgPool,
}

impl AdminAuditRetentionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RetentionSink for AdminAuditRetentionStore {
    async fn prune_audit_events(
        &self,
        retention: Duration,
        max_records: u32,
        max_delete_per_tick: u32,
    ) -> Result<TickReport, sqlx::Error> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let max_records = i64::from(max_records);
        let max_delete_per_tick = i64::from(max_delete_per_tick);
        let mut tx = self.pool.begin().await?;

        let aged_deleted = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS (
                 SELECT id
                   FROM admin_audit_events
                  WHERE created_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                  ORDER BY created_at ASC, id ASC
                  LIMIT $2
             )
             DELETE FROM admin_audit_events e
              USING candidates c
              WHERE e.id = c.id
              RETURNING e.id::TEXT"
        ))
        .bind(retention_secs)
        .bind(max_delete_per_tick)
        .fetch_all(&mut *tx)
        .await?
        .len();

        let remaining_delete_budget =
            max_delete_per_tick.saturating_sub(i64::try_from(aged_deleted).unwrap_or(i64::MAX));

        let overflow_deleted = if remaining_delete_budget > 0 {
            sqlx::query_scalar::<_, String>(sql!(
                "WITH ranked AS (
                     SELECT id,
                            ROW_NUMBER() OVER (ORDER BY created_at DESC, id DESC) AS row_num
                       FROM admin_audit_events
                 ),
                 candidates AS (
                     SELECT id
                       FROM ranked
                      WHERE row_num > $1
                      ORDER BY row_num DESC
                      LIMIT $2
                 )
                 DELETE FROM admin_audit_events e
                  USING candidates c
                  WHERE e.id = c.id
                  RETURNING e.id::TEXT"
            ))
            .bind(max_records)
            .bind(remaining_delete_budget)
            .fetch_all(&mut *tx)
            .await?
            .len()
        } else {
            0
        };

        let retained_rows_count =
            sqlx::query_scalar::<_, i64>(sql!("SELECT COUNT(*) FROM admin_audit_events"))
                .fetch_one(&mut *tx)
                .await?;
        let retained_rows = count_to_u64(retained_rows_count);

        tx.commit().await?;

        Ok(TickReport {
            aged_deleted: usize_to_u64(aged_deleted),
            overflow_deleted: usize_to_u64(overflow_deleted),
            retained_rows,
        })
    }
}

pub struct Reconciler<S> {
    config: ReconcilerConfig,
    sink: S,
}

impl<S> Reconciler<S>
where
    S: RetentionSink,
{
    pub fn new(config: ReconcilerConfig, sink: S) -> Self {
        Self { config, sink }
    }

    pub async fn tick(&self) -> Result<TickReport, sqlx::Error> {
        let report = self
            .sink
            .prune_audit_events(
                self.config.retention,
                self.config.max_records,
                self.config.max_delete_per_tick,
            )
            .await?;
        pplx_space_metrics::admin::audit_retained_rows(report.retained_rows);
        let total_deleted = report.total_deleted();
        if total_deleted > 0 {
            pplx_space_metrics::admin::audit_retention_deleted(total_deleted);
            tracing::info!(
                aged_deleted = report.aged_deleted,
                overflow_deleted = report.overflow_deleted,
                retained_rows = report.retained_rows,
                "admin audit retention pruned expired rows"
            );
        }
        Ok(report)
    }
}

pub async fn run<S>(config: ReconcilerConfig, sink: S, shutdown_token: CancellationToken)
where
    S: RetentionSink,
{
    tracing::info!(
        tick_secs = config.tick.as_secs(),
        retention_secs = config.retention.as_secs(),
        max_records = config.max_records,
        max_delete_per_tick = config.max_delete_per_tick,
        "starting admin-audit-retention reconciler"
    );

    let reconciler = Reconciler::new(config.clone(), sink);
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            _ = interval.tick() => {
                let started = Instant::now();
                match reconciler.tick().await {
                    Ok(report) => {
                        tracing::debug!(
                            aged_deleted = report.aged_deleted,
                            overflow_deleted = report.overflow_deleted,
                            retained_rows = report.retained_rows,
                            elapsed_ms = started.elapsed().as_millis(),
                            "admin-audit-retention tick completed"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "admin-audit-retention tick failed");
                    }
                }
            }
        }
    }
}

pub fn run_with_pool(
    config: ReconcilerConfig,
    pool: PgPool,
    shutdown_token: CancellationToken,
) -> impl std::future::Future<Output = ()> {
    run(config, AdminAuditRetentionStore::new(pool), shutdown_token)
}

fn count_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
