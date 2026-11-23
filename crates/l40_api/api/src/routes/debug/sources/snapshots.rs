use pplx_space_sandbox_model::SandboxId;

use pplx_space_sql_locator::sql;
use sqlx::PgPool;

use crate::routes::debug::{
    response::{Snapshot, SnapshotInventory, SnapshotStorage, SnapshotsSection},
    sources::{begin_read_only, source_error},
};

pub(in crate::routes::debug) async fn fetch(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<SnapshotsSection, sqlx::Error> {
    let mut tx = begin_read_only(pool).await?;
    let mut snapshots: Vec<Snapshot> =
        pplx_space_snapshot_store::listing::stored_for_sandbox(&mut *tx, sandbox_id)
            .await?
            .into_iter()
            .map(Snapshot::from)
            .collect();

    let inventory = sqlx::query_as::<_, SnapshotInventory>(sql!(
        "SELECT sandbox_id,
                snapshot_id,
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
                locations,
                observed_at,
                updated_at
           FROM sandbox_snapshot_inventory
          WHERE sandbox_id = $1
          ORDER BY observed_at DESC, snapshot_id DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut *tx)
    .await?;

    // Last, because a failed statement aborts the transaction: the catalog
    // and inventory above are already read, and only `storage` is lost.
    let storage_error =
        match pplx_space_snapshot_store::storage_placements(&mut *tx, sandbox_id).await {
            Ok(placements) => {
                for snapshot in &mut snapshots {
                    snapshot.storage = placements
                        .iter()
                        .find(|placement| placement.snapshot_id.as_uuid() == snapshot.id)
                        .map(|placement| {
                            SnapshotStorage::new(
                                placement.in_object_store,
                                placement.node_local_node_id,
                            )
                        });
                }
                tx.commit().await?;
                None
            }
            // Dropping the aborted transaction rolls it back.
            Err(error) => Some(source_error(error)),
        };
    Ok(SnapshotsSection {
        snapshots,
        inventory,
        storage_error,
    })
}
