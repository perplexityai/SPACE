//! Guarded snapshot writes for sandboxes given up as lost because the node
//! holding their only copy is gone.

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::time::Duration;

use pplx_space_common_core::sql;
use sqlx::{Postgres, Transaction};

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
