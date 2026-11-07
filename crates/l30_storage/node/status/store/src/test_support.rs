//! Seeding `node_status_snapshots` rows for tests.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeId;
use sqlx::PgPool;

/// A node status snapshot to seed, relative to the database clock. The
/// default is reachable, fresh for another minute and observed now, with a
/// complete inventory and environment arrays received now: evidence.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotSeed {
    pub reachable: bool,
    /// `execution_inventory_status`, or `None` for no report.
    pub inventory: Option<&'static str>,
    /// Seconds until `stale_after`; negative is in the past.
    pub stale_in_secs: f64,
    /// Seconds since `observed_at`.
    pub observed_secs_ago: f64,
    /// Seconds since the environment arrays were received, or `None` for no
    /// arrays.
    pub environment_refs_secs_ago: Option<f64>,
}

impl Default for SnapshotSeed {
    fn default() -> Self {
        Self {
            reachable: true,
            inventory: Some("complete"),
            stale_in_secs: 60.0,
            observed_secs_ago: 0.0,
            environment_refs_secs_ago: Some(0.0),
        }
    }
}

/// Insert or replace `node`'s snapshot as `seed` describes.
pub async fn upsert_snapshot(
    pool: &PgPool,
    node: NodeId,
    seed: SnapshotSeed,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services, updated_at,
             execution_inventory_status, execution_inventory_received_at,
             usable_environment_refs, known_environment_refs, environment_refs_received_at
         )
         VALUES (
             $1, now() - make_interval(secs => $2), now() + make_interval(secs => $3), $4,
             '[]'::jsonb, now(),
             $5, CASE WHEN $5::text IS NULL THEN NULL ELSE now() END,
             CASE WHEN $6::float8 IS NULL THEN NULL ELSE '{}'::text[] END,
             CASE WHEN $6::float8 IS NULL THEN NULL ELSE '{}'::text[] END,
             now() - make_interval(secs => $6)
         )
         ON CONFLICT (node_id) DO UPDATE SET
             observed_at = EXCLUDED.observed_at,
             stale_after = EXCLUDED.stale_after,
             reachable = EXCLUDED.reachable,
             execution_inventory_status = EXCLUDED.execution_inventory_status,
             execution_inventory_received_at = EXCLUDED.execution_inventory_received_at,
             usable_environment_refs = EXCLUDED.usable_environment_refs,
             known_environment_refs = EXCLUDED.known_environment_refs,
             environment_refs_received_at = EXCLUDED.environment_refs_received_at"
    ))
    .bind(node)
    .bind(seed.observed_secs_ago)
    .bind(seed.stale_in_secs)
    .bind(seed.reachable)
    .bind(seed.inventory)
    .bind(seed.environment_refs_secs_ago)
    .execute(pool)
    .await?;
    Ok(())
}
