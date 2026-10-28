use pplx_space_common_core::{sql, types::SandboxId};
use sqlx::{PgConnection, types::Json};

use crate::node_publish::validate::snapshots::SnapshotClaim;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplaceSnapshotInventoryError {
    #[error("prune absent published sandbox snapshots: {0}")]
    Prune(#[source] sqlx::Error),
    #[error("upsert published sandbox snapshot: {0}")]
    Upsert(#[source] sqlx::Error),
}

/// Replace snapshot descriptions only for sandboxes accepted by the caller's
/// owner guard. Pruning precedes upserting on the same connection; ordinary
/// writes lock inventory rows, with no additional explicit sandbox locks.
/// An empty accepted scope does nothing; an empty report clears that scope.
pub(crate) async fn replace(
    conn: &mut PgConnection,
    sandbox_ids: &[SandboxId],
    snapshot_rows: &[SnapshotClaim],
) -> Result<(), ReplaceSnapshotInventoryError> {
    if sandbox_ids.is_empty() {
        return Ok(());
    }

    let snapshot_sandbox_ids = snapshot_rows
        .iter()
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    let snapshot_ids = snapshot_rows
        .iter()
        .map(|row| row.snapshot_id.as_str())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "WITH scope AS (
             SELECT UNNEST($1::UUID[]) AS sandbox_id
         ),
         incoming AS (
             SELECT sandbox_id, snapshot_id
             FROM UNNEST($2::UUID[], $3::TEXT[]) AS incoming(sandbox_id, snapshot_id)
         )
         DELETE FROM sandbox_snapshot_inventory ssi
         USING scope
         WHERE ssi.sandbox_id = scope.sandbox_id
           AND NOT EXISTS (
               SELECT 1
               FROM incoming
               WHERE incoming.sandbox_id = ssi.sandbox_id
                 AND incoming.snapshot_id = ssi.snapshot_id
           )"
    ))
    .bind(sandbox_ids)
    .bind(&snapshot_sandbox_ids)
    .bind(&snapshot_ids)
    .execute(&mut *conn)
    .await
    .map_err(ReplaceSnapshotInventoryError::Prune)?;

    upsert(conn, sandbox_ids, snapshot_rows).await
}

/// Upsert only in the accepted sandbox scope, locking conflicting inventory
/// rows. Empty claims issue no SQL; unchanged descriptions preserve both clocks.
async fn upsert(
    conn: &mut PgConnection,
    sandbox_ids: &[SandboxId],
    snapshot_rows: &[SnapshotClaim],
) -> Result<(), ReplaceSnapshotInventoryError> {
    if snapshot_rows.is_empty() || sandbox_ids.is_empty() {
        return Ok(());
    }
    // All columns visit the same claims, preserving nullable array positions.
    macro_rules! column {
        ($value:expr) => {
            snapshot_rows.iter().map($value).collect::<Vec<_>>()
        };
    }
    sqlx::query(UPSERT_SNAPSHOT_INVENTORY_SQL)
        .bind(sandbox_ids)
        .bind(column!(|row| row.sandbox_id))
        .bind(column!(|row| row.snapshot_id.as_str()))
        .bind(column!(SnapshotClaim::kind))
        .bind(column!(|row| row.state.as_str()))
        .bind(column!(|row| row.timestamp.as_deref()))
        .bind(column!(|row| row.source_node_id.as_deref()))
        .bind(column!(|row| row.label.as_deref()))
        .bind(column!(|row| row.retention_tier.as_deref()))
        .bind(column!(|row| row.vcpu))
        .bind(column!(|row| row.memory_mib))
        .bind(column!(|row| row.size_bytes))
        .bind(column!(|row| row.template_id.as_deref()))
        .bind(column!(|row| row.content_hash.as_deref()))
        .bind(column!(|row| Json(&row.locations)))
        .execute(&mut *conn)
        .await
        .map_err(ReplaceSnapshotInventoryError::Upsert)?;
    Ok(())
}

const UPSERT_SNAPSHOT_INVENTORY_SQL: &str = sql!(
    "WITH scope AS (
             SELECT UNNEST($1::UUID[]) AS sandbox_id
         ),
         input AS (
             SELECT sandbox_id, snapshot_id, type, state, timestamp, source_node_id,
                    label, retention_tier, vcpu, memory_mib, size_bytes, template_id,
                    content_hash, locations
             FROM UNNEST(
                 $2::UUID[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[],
                 $7::TEXT[], $8::TEXT[], $9::TEXT[], $10::INTEGER[], $11::INTEGER[],
                 $12::BIGINT[], $13::TEXT[], $14::TEXT[], $15::JSONB[]
             ) AS input(
                 sandbox_id, snapshot_id, type, state, timestamp, source_node_id,
                 label, retention_tier, vcpu, memory_mib, size_bytes, template_id,
                 content_hash, locations
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
);

#[cfg(test)]
mod tests;
