use std::sync::LazyLock;

use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId, SubstrateKind},
};
use pplx_space_common_svc::types::{
    FACET_NETWORK_POLICY, FACET_SANDBOX_DESIRED_STATE, NetworkPolicyTarget,
    SANDBOX_FAILURE_EPISODE_KEY,
};
use pplx_space_sandbox_portability::stopped_artifact_durable;
use sqlx::PgConnection;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemoveAbsentSandboxesError {
    #[error("delete absent published sandbox observed state: {0}")]
    ObservedState(#[source] sqlx::Error),
    #[error("delete absent network policy facet status: {0}")]
    NetworkPolicy(#[source] sqlx::Error),
    #[error("clear absent sandbox facet observations: {0}")]
    FacetObservations(#[source] sqlx::Error),
    #[error("delete node-published absent stopped sandbox: {0}")]
    StoppedSandbox(#[source] sqlx::Error),
    #[error("delete absent stopped sandbox warm-pool receipts: {0}")]
    WarmPoolReceipts(#[source] sqlx::Error),
}

/// Deletes the stopped Direct sandboxes a node reported absent, and detaches
/// their snapshots' storage locations.
///
/// The delete reads the node's absence as proof that its local rootfs, a
/// stopped sandbox's only copy, is gone. That doesn't hold for a stopped
/// sandbox released from the node once its pinned artifact was durable, so
/// [`stopped_artifact_durable`] exempts it, the same term that keeps the
/// census prune's `marked_deleted` arm off it.
///
/// The candidates are locked (`FOR UPDATE`) before anything is detached or
/// deleted. Without the lock, a row a concurrent writer moved out of `stopped`
/// after this statement's snapshot was still selected, and the `DELETE`, which
/// rechecks only its join to the CTE after waiting on the row, deleted it.
/// Locking makes Postgres re-evaluate the whole condition against the row that
/// writer committed; only the durability `EXISTS`'s own table scans keep the
/// statement's snapshot.
///
/// Built once: every input is bound.
static DELETE_ABSENT_STOPPED_SANDBOXES: LazyLock<String> = LazyLock::new(|| {
    let released_with_durable_artifact = stopped_artifact_durable("s");
    sql!(
        format!(
            "WITH deleted AS MATERIALIZED (
                 SELECT s.id
                 FROM sandboxes s
                 WHERE s.active_node_id = $1
                   AND s.managed_by = $3
                   AND s.lifecycle = 'stopped'
                   AND s.id = ANY($2)
                   AND NOT {released_with_durable_artifact}
                 FOR UPDATE OF s
             ),
             detached_storage AS (
                 UPDATE snapshot_storage_locations ssl
                 SET snapshot_id = NULL, orphaned_at = NOW()
                 WHERE ssl.snapshot_id IN (
                     SELECT snapshots.id
                     FROM snapshots
                     JOIN deleted ON deleted.id = snapshots.sandbox_id
                 )
                 RETURNING 1
             )
             DELETE FROM sandboxes s
             USING deleted
             WHERE s.id = deleted.id
             RETURNING s.id"
        )
        .as_str()
    )
});

/// Remove absent observations and guarded stopped-sandbox remnants on the
/// caller's transaction. Observation/facet writes precede sandbox row locks,
/// then snapshot detachment and warm-pool receipt cleanup. Unmatched guards
/// are normal; the count is removed observed-state rows, not deleted sandboxes.
pub(crate) async fn remove(
    conn: &mut PgConnection,
    node_id: NodeId,
    absent_ids: &[SandboxId],
) -> Result<u64, RemoveAbsentSandboxesError> {
    if absent_ids.is_empty() {
        return Ok(0);
    }
    let removed = sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states sos
             WHERE sos.node_id = $1
               AND sos.sandbox_id = ANY($2)"
    ))
    .bind(node_id)
    .bind(absent_ids)
    .execute(&mut *conn)
    .await
    .map_err(RemoveAbsentSandboxesError::ObservedState)?;

    let elements = absent_ids
        .iter()
        .map(|id| NetworkPolicyTarget::sandbox(*id).as_element_string())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "DELETE FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&elements)
    .execute(&mut *conn)
    .await
    .map_err(RemoveAbsentSandboxesError::NetworkPolicy)?;

    let sandbox_elements = absent_ids
        .iter()
        .map(SandboxId::to_string)
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "UPDATE node_facet_observed_status
             SET observed_payload = COALESCE(observed_payload, '{}'::JSONB)
                                        - 'quarantine_recovery'
                                        - $4::TEXT,
                 observed_at = NOW(),
                 updated_at = NOW()
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)
               AND (
                   observed_payload ? 'quarantine_recovery'
                   OR observed_payload ? $4::TEXT
               )"
    ))
    .bind(node_id)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(&sandbox_elements)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .execute(&mut *conn)
    .await
    .map_err(RemoveAbsentSandboxesError::FacetObservations)?;

    let deleted_ids = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(
        DELETE_ABSENT_STOPPED_SANDBOXES.as_str(),
    ))
    .bind(node_id)
    .bind(absent_ids)
    .bind(SubstrateKind::Space)
    .fetch_all(&mut *conn)
    .await
    .map_err(RemoveAbsentSandboxesError::StoppedSandbox)?;
    // This delete predates finalize_sandbox_db_delete_tx and keeps its own
    // cleanup statements; the warm-pool receipt retire from that helper
    // applies here for the same reason (no FK, unreachable once the
    // sandbox row is gone).
    pplx_space_cluster_pool::WarmPoolStore::delete_claimed_slots_for_retired_executions(
        &mut *conn,
        &deleted_ids,
    )
    .await
    .map_err(RemoveAbsentSandboxesError::WarmPoolReceipts)?;
    Ok(removed.rows_affected())
}
