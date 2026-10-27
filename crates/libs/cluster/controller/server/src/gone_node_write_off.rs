//! Bookkeeping shared by the give-ups that mark sandboxes lost because the
//! node holding their only copy is gone: `drained_node_terminator`'s
//! drained-node give-up and `lost_started_sandboxes`.
//!
//! Each caller owns its own candidate claim and terminal flip. What they share
//! is the identity re-check that keeps a same-name replacement node from
//! inheriting the dead node's verdict, and the rows a lost sandbox leaves
//! behind that nothing else would clean up.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_events::{SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventType};
use sqlx::{Postgres, Transaction};

/// Whether the Kubernetes node row is still in `drain_phase` (`None` = outside
/// any drain) and still carries the kube uid the EC2 cross-check observed.
///
/// Both rows are read `FOR SHARE`. The give-up's flip only reads `nodes` and
/// `node_k8s`, so without the lock a concurrent drain-phase change or a
/// reflector uid bump committing mid-transaction would be invisible to it.
/// The shared lock holds those writers off until commit, or waits for theirs
/// and then reads the committed version, refusing on drift.
pub(crate) async fn node_identity_holds(
    tx: &mut Transaction<'_, Postgres>,
    node_id: NodeId,
    drain_phase: Option<&str>,
    expected_uid: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let node: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT 1 FROM nodes
          WHERE id = $1 AND drain_phase IS NOT DISTINCT FROM $2 AND managed_by = $3
          FOR SHARE"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(&mut **tx)
    .await?;
    if node.is_none() {
        return Ok(false);
    }
    let uid: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT 1 FROM node_k8s
          WHERE node_id = $1 AND k8s_node_uid IS NOT DISTINCT FROM $2
          FOR SHARE"
    ))
    .bind(node_id)
    .bind(expected_uid)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(uid.is_some())
}

/// Clean up after `ids` were flipped to `failed` in this transaction because
/// `node_id` is gone. Run it after the flip, in the flip's transaction.
///
/// - **Dead uploads.** A `creating` snapshot from the gone node, quiet past
///   `dead_upload_settle`, can never finalize: its upload worker died with the
///   instance. Left `creating`, it reads as a live upload forever, and the
///   drained-node snapshot guard would refuse the reap on it. It's finalized
///   `failed`, as the worker's own failure path does.
/// - **The backup queue.** A gone node sends no census to replace its
///   `node_local_backup_pending` set, and the table's only foreign key is on
///   `sandbox_id`, so the rows would stay until the sandbox row is deleted.
///   Scoped to `node_id`: a sandbox can hold a queue row on a live node too,
///   and that row is the live node's pending work. Lock order is
///   sandboxes-then-queue, while the census publish takes the queue row first;
///   that can only deadlock against a census from the gone node itself.
/// - **Executions.** The active execution is written off as `forgotten`, the
///   pod-tracking write-off state: its pod, if one ran, died with the node.
/// - **Placement.** Released through
///   [`pplx_space_sandbox_store::lost::release_lost_placement`], so the row
///   stops pinning the gone node.
///
/// Port exposures are left alone: re-queueing them for a terminal sandbox
/// would only create work that can never apply.
pub(crate) async fn write_off_lost_sandboxes(
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
    sqlx::query(sql!(
        "DELETE FROM node_local_backup_pending
          WHERE node_id = $1 AND sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'forgotten',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = ANY($1)
            AND active"
    ))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    pplx_space_sandbox_store::lost::release_lost_placement(tx, ids).await
}

/// Emit the `sandbox_lost` and `sandbox_failed` pair for a committed flip. The
/// failed event also names the `operation` that gave the sandbox up.
pub(crate) fn emit_lost_and_failed(
    sandbox_id: SandboxId,
    details: SandboxEventDetails,
    operation: &str,
) {
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxLost,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.clone());
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SandboxFailed,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details.operation(operation));
}
