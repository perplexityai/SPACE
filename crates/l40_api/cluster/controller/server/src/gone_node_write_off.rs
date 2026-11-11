//! Bookkeeping shared by the give-ups that mark sandboxes lost because the
//! node holding their only copy is gone: `drained_node_terminator`'s
//! drained-node give-up and `lost_started_sandboxes`.
//!
//! Each caller owns its own candidate claim and terminal flip. What they share
//! is the identity re-check that keeps a same-name replacement node from
//! inheriting the dead node's verdict, and the events a committed flip emits.
//! The rows a lost sandbox leaves behind that nothing else would clean up are
//! written off by the stores:
//! [`pplx_space_snapshot_store::lost::fail_quiet_full_uploads_from_node`], then
//! [`pplx_space_sandbox_store::lost::write_off_lost_sandboxes`].

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

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
