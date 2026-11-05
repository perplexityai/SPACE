//! Client-side database-pool metrics.
//!
//! Successful acquire duration is the one exception to this module's emitter
//! ownership: SQLx exposes it as a tracing event, so the subscriber layer in
//! `pplx-space-observability` must emit it below this crate's dependency edge.
//! Re-exporting its canonical name here keeps that metric discoverable with the
//! rest of the pool telemetry without introducing a dependency cycle.

use pplx_space_common_core::types::DatabaseIdentity;
use pplx_space_metrics_emit::{distribution, tag};

pub use pplx_space_observability::DB_POOL_ACQUIRE_TOTAL_MS_METRIC;

/// In-use fraction of the pool's `max_connections` cap, 0.0–1.0, tagged by
/// the Postgres login backing the pool.
///
/// Emitted as a distribution so `max`/percentile queries aggregate every
/// pod's samples server-side; per-replica gauges from co-scheduled pods
/// would collapse into one last-write-wins series at the node agent.
pub fn utilization_for_identity(ratio: f64, identity: DatabaseIdentity) {
    distribution(
        "db.pool.utilization",
        ratio,
        &[tag("database_identity", identity.as_str())],
    );
}

/// Checked-out connections tagged by the Postgres login backing the pool.
pub fn connections_in_use_for_identity(in_use: u32, identity: DatabaseIdentity) {
    distribution(
        "db.pool.connections_in_use",
        f64::from(in_use),
        &[tag("database_identity", identity.as_str())],
    );
}

/// Idle connections tagged by the Postgres login backing the pool.
pub fn connections_idle_for_identity(idle: usize, identity: DatabaseIdentity) {
    distribution(
        "db.pool.connections_idle",
        idle as f64,
        &[tag("database_identity", identity.as_str())],
    );
}
