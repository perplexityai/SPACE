//! `sandbox_snapshot_inventory`: the snapshots each sandbox's node last
//! published for it, one row per `(sandbox, snapshot)`.
//!
//! The census prune of rows for sandboxes no longer reported is part of
//! [`crate::ingest`]. [`upsert_published`] is the publish path's upsert, run
//! on the executor (usually the publish transaction) its caller passes; the
//! publish path's prune of snapshots a node no longer lists is not in this
//! crate yet.

use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::types::{Json, JsonValue};

/// Upsert the published snapshots in `snapshots` (an observed projection), a
/// JSON array of objects with the keys `sandbox_id`, `snapshot_id`, `type`,
/// `state`, `timestamp`, `source_node_id`, `label`, `retention_tier`,
/// `vcpu`, `memory_mib`, `size_bytes`, `template_id`, `content_hash` and
/// `locations`. Only entries whose sandbox is in `sandbox_ids` are written,
/// and a row whose published columns are unchanged is not rewritten, so its
/// `observed_at` and `updated_at` keep their values.
pub async fn upsert_published(
    executor: impl PgExecutor<'_>,
    sandbox_ids: &[SandboxId],
    snapshots: JsonValue,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT UNNEST($1::UUID[]) AS sandbox_id
         ),
         input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 sandbox_id UUID,
                 snapshot_id TEXT,
                 type TEXT,
                 state TEXT,
                 timestamp TEXT,
                 source_node_id TEXT,
                 label TEXT,
                 retention_tier TEXT,
                 vcpu INTEGER,
                 memory_mib INTEGER,
                 size_bytes BIGINT,
                 template_id TEXT,
                 content_hash TEXT,
                 locations JSONB
             )
         )
         INSERT INTO sandbox_snapshot_inventory (
                 sandbox_id, snapshot_id, type, state, timestamp, source_node_id,
                 label, retention_tier, vcpu, memory_mib, size_bytes, template_id,
                 content_hash, locations, observed_at, updated_at
             )
         SELECT
             input.sandbox_id,
             input.snapshot_id,
             input.type,
             input.state,
             input.timestamp,
             input.source_node_id,
             input.label,
             input.retention_tier,
             input.vcpu,
             input.memory_mib,
             COALESCE(input.size_bytes, 0),
             input.template_id,
             input.content_hash,
             COALESCE(input.locations, '[]'::jsonb),
             NOW(),
             NOW()
         FROM input
         JOIN scope ON scope.sandbox_id = input.sandbox_id
         ON CONFLICT (sandbox_id, snapshot_id) DO UPDATE
         SET type = EXCLUDED.type,
             state = EXCLUDED.state,
             timestamp = EXCLUDED.timestamp,
             source_node_id = EXCLUDED.source_node_id,
             label = EXCLUDED.label,
             retention_tier = EXCLUDED.retention_tier,
             vcpu = EXCLUDED.vcpu,
             memory_mib = EXCLUDED.memory_mib,
             size_bytes = EXCLUDED.size_bytes,
             template_id = EXCLUDED.template_id,
             content_hash = EXCLUDED.content_hash,
             locations = EXCLUDED.locations,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE (
             sandbox_snapshot_inventory.type,
             sandbox_snapshot_inventory.state,
             sandbox_snapshot_inventory.timestamp,
             sandbox_snapshot_inventory.source_node_id,
             sandbox_snapshot_inventory.label,
             sandbox_snapshot_inventory.retention_tier,
             sandbox_snapshot_inventory.vcpu,
             sandbox_snapshot_inventory.memory_mib,
             sandbox_snapshot_inventory.size_bytes,
             sandbox_snapshot_inventory.template_id,
             sandbox_snapshot_inventory.content_hash,
             sandbox_snapshot_inventory.locations
         ) IS DISTINCT FROM (
             EXCLUDED.type,
             EXCLUDED.state,
             EXCLUDED.timestamp,
             EXCLUDED.source_node_id,
             EXCLUDED.label,
             EXCLUDED.retention_tier,
             EXCLUDED.vcpu,
             EXCLUDED.memory_mib,
             EXCLUDED.size_bytes,
             EXCLUDED.template_id,
             EXCLUDED.content_hash,
             EXCLUDED.locations
         )"
    ))
    .bind(sandbox_ids)
    .bind(Json(snapshots))
    .execute(executor)
    .await?;
    Ok(())
}
