//! Recovery of an abandoned publication after its only execution disappeared.

use pplx_space_common_core::{
    sql,
    types::{LocalArtifactId, NodeId, SandboxId, SnapshotId, SubstrateKind},
};
use sqlx::{PgExecutor, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct Candidate {
    pub sandbox_id: SandboxId,
    pub quarantine_id: Uuid,
    pub node_id: NodeId,
    pub node_name: String,
    pub execution_id: Uuid,
    pub generation: i64,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
    pub snapshot_id: SnapshotId,
    pub artifact_id: LocalArtifactId,
}

/// One execution means the absence probes cover every possible resident owner.
/// Multiple executions require a broader proof and remain quarantined. A ready
/// snapshot must predate quarantine, cover all recorded activity, and have no
/// newer full snapshot or unfinished upload. Lifecycle/placement timestamps are
/// not artifact freshness: the failed publication itself advances those clocks.
pub(super) async fn candidates<'e>(
    executor: impl PgExecutor<'e>,
    after: Option<SandboxId>,
    only: Option<SandboxId>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT s.id AS sandbox_id, q.quarantine_id, s.active_node_id AS node_id,
                n.k8s_node_name AS node_name, e.id AS execution_id, e.generation,
                e.pod_name, e.pod_namespace, e.pod_uid,
                sn.id AS snapshot_id, sn.local_artifact_id AS artifact_id
           FROM sandboxes s
           JOIN sandbox_quarantine_state q ON q.sandbox_id = s.id
           JOIN node_k8s n ON n.node_id = s.active_node_id
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
           JOIN snapshots sn ON sn.sandbox_id = s.id
          WHERE s.managed_by = $4 AND s.lifecycle = 'quarantined'
            AND s.current_local_artifact_id IS NULL
            AND s.current_local_artifact_kind IS NULL
            AND s.current_local_artifact_origin IS NULL
            AND (s.target_node_id IS NULL OR s.target_node_id = s.active_node_id)
            AND (s.restore_lease_expires_at IS NULL OR s.restore_lease_expires_at < NOW())
            AND q.reason_code = 'suspend_snapshot_publication_failed'
            AND q.entered_at < NOW() - INTERVAL '10 minutes'
            AND q.next_action_owner IS NULL AND q.claim_token IS NULL
            AND e.active AND e.state NOT IN ('creating', 'recovering')
            AND e.created_at < NOW() - INTERVAL '10 minutes'
            AND e.activation_attempt_id IS NULL
            AND (e.k8s_node_name IS NULL OR e.k8s_node_name = n.k8s_node_name)
            AND NOT EXISTS (
                SELECT 1 FROM sandbox_k8s_executions other
                 WHERE other.sandbox_id = s.id AND other.id <> e.id)
            AND NOT EXISTS (
                SELECT 1 FROM public_operations op
                 WHERE op.target_id IN (s.id::TEXT, 'sbx_' || s.id::TEXT)
                   AND op.state IN ('queued', 'running'))
            AND sn.state = 'ready' AND sn.type = 'full'
            AND sn.artifact_kind = 'full' AND sn.artifact_origin = 'suspend'
            AND sn.local_artifact_id IS NOT NULL AND sn.size_bytes > 0
            AND sn.timestamp <= q.entered_at
            AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
            AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
            AND EXISTS (
                SELECT 1 FROM snapshot_storage_locations l
                 WHERE l.snapshot_id = sn.id AND l.orphaned_at IS NULL)
            AND NOT EXISTS (
                SELECT 1 FROM snapshots newer WHERE newer.sandbox_id = s.id
                  AND (newer.state = 'creating'
                       OR (newer.type = 'full' AND newer.timestamp > sn.timestamp)))
            AND ($1::UUID IS NULL OR s.id > $1)
            AND ($2::UUID IS NULL OR s.id = $2)
          ORDER BY s.id LIMIT $3"
    ))
    .bind(after)
    .bind(only)
    .bind(limit)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor)
    .await
}

/// Recheck on fresh statement snapshots after locking the owners. No external
/// work occurs inside this transaction. A changed episode, execution, snapshot,
/// activity timestamp, or user operation invalidates the absence proof.
pub(super) async fn recover(pool: &PgPool, expected: &Candidate) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '2s'"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(expected.sandbox_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!(
        "SELECT sandbox_id FROM sandbox_quarantine_state WHERE sandbox_id = $1 FOR UPDATE"
    ))
    .bind(expected.sandbox_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "SELECT id FROM sandbox_k8s_executions WHERE sandbox_id = $1 FOR UPDATE"
    ))
    .bind(expected.sandbox_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!("SELECT id FROM snapshots WHERE id = $1 FOR SHARE"))
        .bind(expected.snapshot_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(sql!(
        "SELECT snapshot_id FROM snapshot_storage_locations WHERE snapshot_id = $1 FOR SHARE"
    ))
    .bind(expected.snapshot_id)
    .execute(&mut *tx)
    .await?;
    let current = candidates(&mut *tx, None, Some(expected.sandbox_id), 2).await?;
    if current.as_slice() != std::slice::from_ref(expected) {
        return Ok(false);
    }
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history
            (id, quarantine_id, sandbox_id, source, action, reason_code, message, attempt_count, evidence)
         SELECT $2, quarantine_id, sandbox_id, source, 'recovered_to_suspended', reason_code,
                'Worker row and execution pod absent; ready suspend snapshot metadata matches recorded activity',
                attempt_count, evidence || jsonb_build_object('snapshot_id', $3::TEXT)
           FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    )).bind(expected.sandbox_id).bind(Uuid::now_v7()).bind(expected.snapshot_id.to_string())
        .execute(&mut *tx).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended', placement = 'unplaced',
             active_node_id = NULL, target_node_id = NULL,
             lifecycle_changed_at = NOW(), placement_changed_at = NULL, updated_at = NOW(),
             current_local_artifact_id = $2, current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend', command_ready_pending = FALSE,
             restore_lease_expires_at = NULL, restore_lease_token = NULL
          WHERE id = $1"
    ))
    .bind(expected.sandbox_id)
    .bind(expected.artifact_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'suspended', pod_ip = NULL,
             terminated_at = COALESCE(terminated_at, NOW()), updated_at = NOW()
          WHERE id = $1"
    ))
    .bind(expected.execution_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(expected.sandbox_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_port_exposures SET apply_state = 'pending', last_applied_node_id = NULL,
             last_error = NULL, last_bridge_status = NULL, last_applied_bridge_generation = NULL,
             last_observed_at = NULL, updated_at = NOW()
          WHERE sandbox_id = $1 AND desired_state = 'open'"
    ))
    .bind(expected.sandbox_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(expected.sandbox_id)
    .execute(&mut *tx)
    .await?;
    pplx_space_sandbox_teardown::delete_reconcile_state_for_elements(
        &mut tx,
        &[
            pplx_space_common_svc::types::ReconcileElementRef::sandbox_desired_state(
                expected.sandbox_id,
            ),
        ],
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}
