use std::time::Duration;

use pplx_space_postgres_pool::Db;
use tokio_util::sync::CancellationToken;

use reconcile::QueueLagStage;

pub mod reconcile;

const DEFAULT_QUEUE_METRICS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct QueueMetricsConfig {
    pub interval: Duration,
}

impl Default for QueueMetricsConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_QUEUE_METRICS_INTERVAL,
        }
    }
}

pub async fn run_queue_metrics(
    db: Db,
    config: QueueMetricsConfig,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        interval_secs = config.interval.as_secs(),
        "starting state-reconciliation queue metrics emitter"
    );

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                emit_node_queue_depth(&db).await;
                emit_node_queue_stale_claims(&db).await;
                emit_node_queue_lag(&db).await;
                emit_trigger_depth(&db).await;
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("state-reconciliation queue metrics emitter stopping");
                return;
            }
        }
    }
}

pub async fn emit_node_queue_depth(db: &Db) {
    let rows = pplx_space_cluster_trigger_queue::reconcile_queue_depth(db).await;
    match rows {
        Ok(rows) => {
            for row in rows {
                pplx_space_reconcile_metrics::queue_depth(
                    &row.facet,
                    &row.state,
                    u64::try_from(row.count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile queue depth metrics");
        }
    }
}

pub async fn emit_node_queue_stale_claims(db: &Db) {
    let rows = pplx_space_cluster_trigger_queue::reconcile_queue_stale_claims(db).await;
    match rows {
        Ok(rows) => {
            for row in rows {
                crate::metrics::reconcile::queue_stale_claims(
                    &row.facet,
                    u64::try_from(row.count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile stale claim metrics");
        }
    }
}

pub async fn emit_node_queue_lag(db: &Db) {
    let rows = pplx_space_cluster_trigger_queue::reconcile_queue_lag(db).await;
    match rows {
        Ok(rows) => {
            for row in rows {
                crate::metrics::reconcile::queue_lag_seconds(
                    &row.facet,
                    QueueLagStage::Claim,
                    row.claim_lag_seconds,
                );
                crate::metrics::reconcile::queue_lag_seconds(
                    &row.facet,
                    QueueLagStage::Retry,
                    row.retry_lag_seconds,
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile queue lag metrics");
        }
    }
}

pub async fn emit_trigger_depth(db: &Db) {
    let rows = pplx_space_cluster_trigger_queue::trigger_queue_depth(db).await;
    match rows {
        Ok(rows) => {
            for row in rows {
                crate::metrics::reconcile::trigger_depth(
                    &row.state,
                    u64::try_from(row.count).unwrap_or(0),
                );
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to emit node reconcile trigger depth metrics");
        }
    }
}

#[cfg(test)]
mod tests;
