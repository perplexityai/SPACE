use pplx_space_common_core::{
    sql,
    types::{SandboxId, SubstrateKind},
};
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEFAULT_TICK: Duration = Duration::from_secs(300);
const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const DEFAULT_MAX_RECORDS_PER_TICK: u32 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub tick: Duration,
    pub retention: Duration,
    pub max_records_per_tick: u32,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            retention: DEFAULT_RETENTION,
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
    pub direct_nodes_deleted: u64,
    pub templates_deleted: u64,
    pub deleted_sandboxes_deleted: u64,
    pub direct_node_tombstones: u64,
    pub template_tombstones: u64,
    pub deleted_sandbox_rows: u64,
}

impl TickReport {
    fn total(self) -> u64 {
        self.direct_nodes_deleted
            .saturating_add(self.templates_deleted)
            .saturating_add(self.deleted_sandboxes_deleted)
    }
}

pub trait RetentionSink: Send + Sync + 'static {
    fn prune_tombstones(
        &self,
        retention: Duration,
        max_records: u32,
    ) -> impl std::future::Future<Output = Result<TickReport, sqlx::Error>> + Send;
}

#[derive(Clone)]
pub struct TombstoneRetentionStore {
    pool: PgPool,
}

impl TombstoneRetentionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RetentionSink for TombstoneRetentionStore {
    async fn prune_tombstones(
        &self,
        retention: Duration,
        max_records: u32,
    ) -> Result<TickReport, sqlx::Error> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let max_records = i64::from(max_records);
        let mut tx = self.pool.begin().await?;

        // Quarantined sandboxes are not terminal for tombstone retention:
        // they intentionally keep the node association until quarantine-aware
        // cleanup or operator release resolves the preserved state.
        let direct_nodes_deleted = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS (
                 SELECT n.id
                   FROM nodes n
                   JOIN node_direct nd ON nd.node_id = n.id
                  WHERE n.managed_by = $3
                    AND COALESCE(n.state, 'registered') = 'deleted'
                    AND nd.deleted_at IS NOT NULL
                    AND nd.deleted_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandboxes s
                         WHERE s.active_node_id = n.id
                           AND s.lifecycle NOT IN ('deleted', 'failed')
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandboxes s
                         WHERE s.target_node_id = n.id
                           AND s.lifecycle NOT IN ('deleted', 'failed')
                    )
                  ORDER BY nd.deleted_at ASC, n.id ASC
                  LIMIT $2
             )
             DELETE FROM nodes n
              USING candidates c
              WHERE n.id = c.id
              RETURNING n.id::TEXT"
        ))
        .bind(retention_secs)
        .bind(max_records)
        .bind(SubstrateKind::Space)
        .fetch_all(&mut *tx)
        .await?
        .len() as u64;

        let templates_deleted = sqlx::query_scalar::<_, String>(sql!(
            "WITH candidates AS (
                 SELECT t.name
                   FROM templates t
                  WHERE t.desired_state = 'deleted'
                    AND COALESCE(t.delete_requested_at, t.updated_at, t.created_at)
                        < NOW() - ($1::BIGINT * INTERVAL '1 second')
                    AND NOT EXISTS (
                        SELECT 1
                          FROM template_releases r
                         WHERE r.template_name = t.name
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM node_template_inventory i
                         WHERE i.template_name = t.name
                    )
                  ORDER BY COALESCE(t.delete_requested_at, t.updated_at, t.created_at) ASC,
                           t.name ASC
                  LIMIT $2
             )
             DELETE FROM templates t
              USING candidates c
              WHERE t.name = c.name
              RETURNING t.name"
        ))
        .bind(retention_secs)
        .bind(max_records)
        .fetch_all(&mut *tx)
        .await?
        .len() as u64;

        let deleted_sandbox_ids = sqlx::query_scalar::<_, Uuid>(sql!(
            "WITH candidates AS (
                 SELECT s.id
                   FROM sandboxes s
                  WHERE s.lifecycle = 'deleted'
                    AND COALESCE(s.updated_at, s.created_at)
                        < NOW() - ($1::BIGINT * INTERVAL '1 second')
                  ORDER BY COALESCE(s.updated_at, s.created_at) ASC,
                           s.id ASC
                  LIMIT $2
             )
             DELETE FROM sandboxes s
              USING candidates c
              WHERE s.id = c.id
              RETURNING s.id"
        ))
        .bind(retention_secs)
        .bind(max_records)
        .fetch_all(&mut *tx)
        .await?;
        // Same rationale as finalize_sandbox_db_delete_tx (this reaper
        // predates it): receipts are not foreign-keyed to `sandboxes`, and an
        // orphaned receipt is unreachable by the suspended-fossil drain.
        let deleted_reservations = deleted_sandbox_ids
            .iter()
            .map(|id| SandboxId::from_uuid(*id))
            .collect::<Vec<_>>();
        pplx_space_cluster_pool::WarmPoolStore::delete_claimed_slots_for_retired_executions(
            &mut tx,
            &deleted_reservations,
        )
        .await?;
        let deleted_sandboxes_deleted = deleted_sandbox_ids.len() as u64;

        let direct_node_tombstones_count = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*)
               FROM nodes n
               JOIN node_direct nd ON nd.node_id = n.id
              WHERE n.managed_by = $1
                AND COALESCE(n.state, 'registered') = 'deleted'
                AND nd.deleted_at IS NOT NULL"
        ))
        .bind(SubstrateKind::Space)
        .fetch_one(&mut *tx)
        .await?;
        let direct_node_tombstones = count_to_u64(direct_node_tombstones_count);
        let template_tombstones_count = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*)
               FROM templates
              WHERE desired_state = 'deleted'"
        ))
        .fetch_one(&mut *tx)
        .await?;
        let template_tombstones = count_to_u64(template_tombstones_count);
        let deleted_sandbox_rows_count = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*)
               FROM sandboxes
              WHERE lifecycle = 'deleted'"
        ))
        .fetch_one(&mut *tx)
        .await?;
        let deleted_sandbox_rows = count_to_u64(deleted_sandbox_rows_count);

        tx.commit().await?;

        Ok(TickReport {
            direct_nodes_deleted,
            templates_deleted,
            deleted_sandboxes_deleted,
            direct_node_tombstones,
            template_tombstones,
            deleted_sandbox_rows,
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
            .prune_tombstones(self.config.retention, self.config.max_records_per_tick)
            .await?;
        pplx_space_metrics::tombstone::retained_rows("direct_node", report.direct_node_tombstones);
        pplx_space_metrics::tombstone::retained_rows("template", report.template_tombstones);
        pplx_space_metrics::tombstone::retained_rows("sandbox", report.deleted_sandbox_rows);
        if report.total() > 0 {
            pplx_space_metrics::tombstone::retention_deleted(
                "direct_node",
                report.direct_nodes_deleted,
            );
            pplx_space_metrics::tombstone::retention_deleted("template", report.templates_deleted);
            pplx_space_metrics::tombstone::retention_deleted(
                "sandbox",
                report.deleted_sandboxes_deleted,
            );
            tracing::info!(
                direct_nodes_deleted = report.direct_nodes_deleted,
                templates_deleted = report.templates_deleted,
                deleted_sandboxes_deleted = report.deleted_sandboxes_deleted,
                "tombstone retention pruned expired rows"
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
        max_records_per_tick = config.max_records_per_tick,
        "starting tombstone-retention reconciler"
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
                            direct_nodes_deleted = report.direct_nodes_deleted,
                            templates_deleted = report.templates_deleted,
                            deleted_sandboxes_deleted = report.deleted_sandboxes_deleted,
                            direct_node_tombstones = report.direct_node_tombstones,
                            template_tombstones = report.template_tombstones,
                            deleted_sandbox_rows = report.deleted_sandbox_rows,
                            elapsed_ms = started.elapsed().as_millis(),
                            "tombstone-retention tick completed"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "tombstone-retention tick failed");
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
    run(config, TombstoneRetentionStore::new(pool), shutdown_token)
}

fn count_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests;
