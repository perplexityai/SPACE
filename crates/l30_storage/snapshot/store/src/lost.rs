//! Snapshot locks and guarded writes for sandboxes given up as lost because
//! the node holding their only copy is gone, and for the uploads a gone node
//! left in flight.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use std::time::Duration;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// Lock, `FOR UPDATE` in id order, the `creating` full uploads `node_id`
/// started for `ids`, and return their ids. The locks last until the caller's
/// transaction ends.
///
/// The drained-node give-up takes them before its flip evaluates, so a
/// finalize of one of those uploads either committed first, and the flip sees
/// it, or waits until the give-up's transaction ends. If that transaction
/// failed the upload, because the flip took its sandbox, a `ready` finalize
/// then matches no row; otherwise the upload is still `creating`.
///
/// The caller must pass its open transaction.
pub async fn lock_creating_full_uploads_from_node<C>(
    tx: &mut C,
    ids: &[SandboxId],
    node_id: NodeId,
) -> Result<Vec<SnapshotId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
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
    .fetch_all(tx.into_sqlx())
    .await?)
}

/// Lock, `FOR UPDATE` in id order, every `creating` full upload of
/// `sandbox_id`, from any node, and return their ids. The locks last until
/// the caller's transaction ends.
///
/// The lost-started give-up takes them before its flip evaluates the
/// sandbox's restore source, so a finalize racing it either committed first,
/// and the flip sees it, or conflicts with the give-up's locks.
///
/// The caller must pass its open transaction.
pub async fn lock_creating_full_uploads<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<Vec<SnapshotId>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT id FROM snapshots
              WHERE sandbox_id = $1
                AND type = 'full'
                AND state = 'creating'
              ORDER BY id
              FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_all(tx.into_sqlx())
    .await?)
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
///
/// The caller must pass its open transaction.
pub async fn fail_quiet_full_uploads_from_node<C>(
    tx: &mut C,
    node_id: NodeId,
    ids: &[SandboxId],
    dead_upload_settle: Duration,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// One upload [`fail_inflight_uploads_from_gone_node`] finalized `failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct FailedUpload {
    #[sqlx(rename = "id")]
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
}

/// Finalize `failed` every in-flight (`creating`) full upload from `node_id`,
/// stamping `failure_reason` where none is recorded yet, and return the rows it
/// finalized. Node drain runs it once the node's Kubernetes Node is gone and
/// its instance is confirmed terminated: the upload worker died with the
/// instance, so no finalize can arrive.
///
/// The identity guards are the transition's: `node_id` is still `draining`,
/// and its `node_k8s.k8s_node_uid` is still `expected_uid` (`IS NOT DISTINCT
/// FROM`, so a `None` the caller observed matches only a NULL). A same-name
/// replacement that registered, or a reset phase, makes it match nothing,
/// which is a normal outcome: an empty result. The caller decides what to do
/// with it and which executor it runs on.
///
/// The two `EXISTS` guards are spelled out as the text
/// `pplx_space_sandbox_store::drain::gone_node_identity_guards("$1", "$2")`
/// renders for node drain's other gone-node writes; a change to one is a
/// change to both.
pub async fn fail_inflight_uploads_from_gone_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    failure_reason: &str,
) -> Result<Vec<FailedUpload>, DbError> {
    Ok(sqlx::query_as(sql!(
        "UPDATE snapshots
            SET state = 'failed',
                failure_reason = COALESCE(failure_reason, $3)
          WHERE state = 'creating'
            AND type = 'full'
            AND source_node_id = $1
            AND EXISTS (
                SELECT 1 FROM nodes
                 WHERE id = $1
                   AND drain_phase = 'draining'
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                 WHERE node_id = $1
                   AND k8s_node_uid IS NOT DISTINCT FROM $2
            )
          RETURNING id, sandbox_id"
    ))
    .bind(node_id)
    .bind(expected_uid)
    .bind(failure_reason)
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
