//! Snapshot locks and guarded writes for sandboxes given up as lost because
//! the node holding their only copy is gone.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use std::time::Duration;

use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};

/// Lock, `FOR UPDATE` in id order, the `creating` full uploads `node_id`
/// started for `ids`, and return their ids. The locks last until the caller's
/// transaction ends.
///
/// The drained-node give-up takes them before its flip evaluates, so a
/// finalize of one of those uploads either committed first, and the flip sees
/// it, or waits until the give-up's transaction ends. If that transaction
/// failed the upload, because the flip took its sandbox, a `ready` finalize
/// then matches no row; otherwise the upload is still `creating`.
pub async fn lock_creating_full_uploads_from_node(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[SandboxId],
    node_id: NodeId,
) -> Result<Vec<SnapshotId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM snapshots
              WHERE sandbox_id = ANY($1)
                AND source_node_id = $2
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
    ))
    .bind(ids)
    .bind(node_id)
    .fetch_all(&mut **tx)
    .await
}

/// Lock, `FOR UPDATE` in id order, every `creating` full upload of
/// `sandbox_id`, from any node, and return their ids. The locks last until
/// the caller's transaction ends.
///
/// The lost-started give-up takes them before its flip evaluates the
/// sandbox's restore source, so a finalize racing it either committed first,
/// and the flip sees it, or conflicts with the give-up's locks.
pub async fn lock_creating_full_uploads(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<Vec<SnapshotId>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_all(&mut **tx)
    .await
}

/// Finalize `failed` the dead uploads `node_id` left for `ids`, which were
/// flipped to `failed` in this transaction because `node_id` is gone. Run it
/// after the flip, in the flip's transaction.
///
/// A `creating` snapshot from the gone node, quiet past `dead_upload_settle`,
/// can never finalize: its upload worker died with the instance. Left
/// `creating`, it reads as a live upload forever, and the drained-node
/// snapshot guard would refuse the reap on it. It's finalized `failed`, as the
/// worker's own failure path does.
pub async fn fail_quiet_full_uploads_from_node(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    ids: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE snapshots
            SET state = 'failed'
          WHERE sandbox_id = ANY($1)
            AND source_node_id = $2
            AND type = 'full'
            AND state = 'creating'
            AND COALESCE(last_attempt_at, timestamp)
                < NOW() - make_interval(secs => $3::BIGINT)"
    ))
    .bind(ids)
    .bind(node_id)
    .bind(dead_upload_settle.as_secs().cast_signed())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
