use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use sqlx::PgPool;

use crate::routes::debug::{
    response::{Snapshot, SnapshotInventory, SnapshotStorage, SnapshotsSection},
    sources::begin_read_only,
};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<SnapshotsSection, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let mut snapshots = sqlx::query_as::<_, Snapshot>(sql!(
        "SELECT id,
                sandbox_id,
                type,
                state,
                timestamp,
                source_node_id,
                label,
                retention_tier,
                vcpu,
                memory_mib,
                size_bytes,
                template_id,
                content_hash,
                org_id,
                failure_reason
           FROM snapshots
          WHERE sandbox_id = $1
          ORDER BY timestamp DESC, id DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut *tx)
    .await?;

    let placements = pplx_space_snapshot_store::storage_placements(&mut *tx, sandbox_id).await?;
    for snapshot in &mut snapshots {
        snapshot.storage = placements
            .iter()
            .find(|placement| placement.snapshot_id.as_uuid() == snapshot.id)
            .map(|placement| {
                SnapshotStorage::new(placement.in_object_store, placement.node_local_node_id)
            });
    }

    let inventory = sqlx::query_as::<_, SnapshotInventory>(sql!(
        "SELECT ssi.sandbox_id,
                sos.node_id,
                ssi.snapshot_id,
                ssi.type,
                ssi.state,
                ssi.timestamp,
                ssi.source_node_id,
                ssi.label,
                ssi.retention_tier,
                ssi.vcpu,
                ssi.memory_mib,
                ssi.size_bytes,
                ssi.template_id,
                ssi.content_hash,
                ssi.locations,
                ssi.observed_at,
                ssi.updated_at
           FROM sandbox_snapshot_inventory ssi
           JOIN sandbox_observed_states sos ON sos.sandbox_id = ssi.sandbox_id
          WHERE ssi.sandbox_id = $1
          ORDER BY ssi.observed_at DESC, ssi.snapshot_id DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(SnapshotsSection {
        snapshots,
        inventory,
    })
}
