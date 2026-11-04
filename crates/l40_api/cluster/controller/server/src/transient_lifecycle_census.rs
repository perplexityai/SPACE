//! Reports how many sandboxes sit in each transient lifecycle, and for how
//! long, as the `sandbox.transient_lifecycle.*` gauges
//! (`pplx_space_metrics::sandbox::transient_lifecycle`).
//!
//! Read-only, and independent of every loop that acts on such rows: it runs
//! whether or not the transient-lifecycle sweeper or node-drain is enabled,
//! so a wedged lifecycle stays visible with them off. It runs on the elected
//! leader only (`run_reconcilers`), so one replica reads the census at a
//! time. After a leader change both leaders' last values can still be
//! queried, so combine replicas with `max` (see `docs/observability.md`). Its
//! cadence is fixed.

use std::time::{Duration, Instant};

use pplx_space_metrics::sandbox::transient_lifecycle::{self as metrics, LifecycleAge};
use pplx_space_sandbox_store::stuck::{
    StuckSandboxesError, TransientLifecycleAges, transient_lifecycle_ages,
};
use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// One census a minute: the youngest age bucket is a minute wide.
const TICK: Duration = Duration::from_secs(60);

pub(crate) async fn run(pool: PgPool, shutdown_token: CancellationToken) {
    tracing::info!(
        tick_secs = TICK.as_secs(),
        "starting transient-lifecycle census"
    );
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                // The error is logged inside; a failed tick emits no gauge
                // rather than zeros it did not read, and is still timed.
                let _ = tick(&pool).await;
                metrics::loop_latency(started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("transient-lifecycle census stopping");
                return;
            }
        }
    }
}

/// Read the census and emit it. A failed read is logged and emits nothing,
/// so the gauges are not zeroed by a database error.
async fn tick(pool: &PgPool) -> Result<(), StuckSandboxesError> {
    match transient_lifecycle_ages(pool, &LifecycleAge::LOWER_BOUNDS).await {
        Ok(census) => {
            emit(&census);
            Ok(())
        }
        Err(error) => {
            tracing::warn!(error = %error, "transient-lifecycle census read failed");
            Err(error)
        }
    }
}

fn emit(census: &[TransientLifecycleAges]) {
    let entries: Vec<metrics::LifecycleCensus<'_>> = census
        .iter()
        .map(|entry| metrics::LifecycleCensus {
            lifecycle: entry.lifecycle,
            rows_by_age: &entry.rows_by_bucket,
            oldest_age: entry.oldest_age,
        })
        .collect();
    metrics::census(&entries);
}

#[cfg(test)]
mod tests;
