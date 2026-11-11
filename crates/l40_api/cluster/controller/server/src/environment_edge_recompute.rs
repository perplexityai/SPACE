//! Periodically recomputes the environment catalog's upgrade and memory edges
//! from every node's certifications.
//!
//! A publication recomputes only the environments its own node's certifications
//! touch, and deleting a node (drain termination, orphan cleanup, tombstone
//! retention, the admin route) cascades its certification rows away without
//! recomputing anything. So an edge vetoed only by deleted nodes would stay out
//! of the catalog until some node that knows its source changed its report,
//! which in a steady fleet may never happen. This loop is the one owner of that
//! correction, whatever deleted the node. It runs for both substrates and needs
//! only Postgres.
//!
//! Each tick only tries the catalog's aggregate lock, as publications do: when
//! a publication holds it, the tick is skipped and the next one recomputes.

use std::time::{Duration, Instant};

use pplx_space_runtime_environment_catalog_store::{self as catalog_store, RecomputeOutcome};
use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// How often the periodic pass recomputes every certified environment's edges.
/// A tick that finds the lock held, or fails, leaves the correction to the next
/// one.
const TICK: Duration = Duration::from_secs(300);

pub trait EdgeRecomputeSink: Send + Sync + 'static {
    fn recompute_all_edges(
        &self,
    ) -> impl std::future::Future<Output = Result<RecomputeOutcome, sqlx::Error>> + Send;
}

pub struct CatalogEdgeStore {
    pool: PgPool,
}

impl EdgeRecomputeSink for CatalogEdgeStore {
    async fn recompute_all_edges(&self) -> Result<RecomputeOutcome, sqlx::Error> {
        catalog_store::recompute_all_edges(&self.pool).await
    }
}

/// One recompute, logged when it changed the catalog.
pub async fn tick<S: EdgeRecomputeSink>(sink: &S) -> Result<RecomputeOutcome, sqlx::Error> {
    let outcome = sink.recompute_all_edges().await?;
    match outcome {
        RecomputeOutcome::Recomputed { changed } if changed > 0 => {
            tracing::info!(changed, "recomputed runtime environment edges");
        }
        RecomputeOutcome::Recomputed { .. } => {}
        RecomputeOutcome::Deferred => {
            tracing::debug!("runtime environment catalog busy; recomputing edges next tick");
        }
    }
    Ok(outcome)
}

pub async fn run<S: EdgeRecomputeSink>(sink: S, shutdown_token: CancellationToken) {
    tracing::info!(
        tick_secs = TICK.as_secs(),
        "starting environment-edge-recompute reconciler"
    );
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            _ = interval.tick() => {
                let started = Instant::now();
                if let Err(error) = tick(&sink).await {
                    tracing::warn!(
                        %error,
                        elapsed_ms = started.elapsed().as_millis(),
                        "environment-edge-recompute tick failed"
                    );
                }
            }
        }
    }
}

pub fn run_with_pool(
    pool: PgPool,
    shutdown_token: CancellationToken,
) -> impl std::future::Future<Output = ()> {
    run(CatalogEdgeStore { pool }, shutdown_token)
}

#[cfg(test)]
mod tests;
