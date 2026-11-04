use pplx_space_common_core::sql;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

const DEFAULT_TICK: Duration = Duration::from_secs(300);
const DEFAULT_GRACE: Duration = Duration::from_secs(3600);
const DEFAULT_MAX_RECORDS_PER_TICK: u32 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub tick: Duration,
    pub grace: Duration,
    pub max_records_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            grace: DEFAULT_GRACE,
            max_records_per_tick: DEFAULT_MAX_RECORDS_PER_TICK,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    pub sessions_deleted: u64,
}

pub trait RetentionSink: Send + Sync + 'static {
    fn gc_expired_sessions(
        &self,
        grace: Duration,
        max_records: u32,
    ) -> impl std::future::Future<Output = Result<TickReport, sqlx::Error>> + Send;
}

#[derive(Clone)]
pub struct DataPlaneSessionRetentionStore {
    pool: PgPool,
}

impl DataPlaneSessionRetentionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RetentionSink for DataPlaneSessionRetentionStore {
    async fn gc_expired_sessions(
        &self,
        grace: Duration,
        max_records: u32,
    ) -> Result<TickReport, sqlx::Error> {
        let grace_secs = i64::try_from(grace.as_secs()).unwrap_or(i64::MAX);
        let max_records = i64::from(max_records);
        let rows = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS (
                 SELECT session_id
                   FROM data_plane_sessions
                  WHERE (
                            expires_at IS NOT NULL
                        AND expires_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                        )
                     OR (
                            state IN ('closed', 'revoked', 'failed')
                        AND updated_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                        )
                  ORDER BY updated_at ASC, session_id ASC
                  LIMIT $2
             )
             DELETE FROM data_plane_sessions AS d
              USING candidates AS c
              WHERE d.session_id = c.session_id
              RETURNING d.session_id"
        ))
        .bind(grace_secs)
        .bind(max_records)
        .fetch_all(&self.pool)
        .await?;

        Ok(TickReport {
            sessions_deleted: rows.len() as u64,
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
            .gc_expired_sessions(self.config.grace, self.config.max_records_per_tick)
            .await?;
        if report.sessions_deleted > 0 {
            pplx_space_metrics::data_plane::session_retention_deleted(report.sessions_deleted);
            tracing::info!(
                sessions_deleted = report.sessions_deleted,
                "data-plane session retention pruned expired records"
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
        grace_secs = config.grace.as_secs(),
        max_records_per_tick = config.max_records_per_tick,
        "starting data-plane-session-retention reconciler"
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
                            sessions_deleted = report.sessions_deleted,
                            elapsed_ms = started.elapsed().as_millis(),
                            "data-plane-session-retention tick completed"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "data-plane-session-retention tick failed");
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
    run(
        config,
        DataPlaneSessionRetentionStore::new(pool),
        shutdown_token,
    )
}

#[cfg(test)]
mod tests;
