use pplx_space_cluster_trigger_queue::enqueue_triggers_conn;
use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::ReconcileReason;
use sqlx::PgConnection;

#[derive(Debug, thiserror::Error)]
pub(crate) enum EnqueueInventoryReconcileError {
    #[error("enqueue first-seen inventory reconcile: {0}")]
    Reconcile(#[source] sqlx::Error),
    #[error("enqueue first-seen inventory trigger: {0}")]
    Trigger(#[source] sqlx::Error),
}

/// Queue distinct first-seen references before the node's reconciliation
/// trigger, on the caller's connection. Ordinary queue-row write locks follow
/// that order. Empty references do nothing, including no trigger; zero changed
/// rows is not an error and does not suppress the trigger for nonempty input.
pub(crate) async fn enqueue(
    conn: &mut PgConnection,
    node_id: NodeId,
    refs: Vec<(&'static str, String)>,
    trigger_reason: ReconcileReason,
) -> Result<(), EnqueueInventoryReconcileError> {
    if refs.is_empty() {
        return Ok(());
    }

    let facets = refs
        .iter()
        .map(|(facet, _)| (*facet).to_string())
        .collect::<Vec<_>>();
    let elements = refs
        .iter()
        .map(|(_, element)| element.clone())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS refs(facet, element)
         )
         INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at)
         SELECT $1, facet, element, NOW(), NOW()
         FROM refs
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET generation = node_reconcile_queue.generation + 1,
             requested_at = EXCLUDED.requested_at,
             next_attempt_at = NOW(),
             attempts = 0,
             last_error = '',
             dead_at = NULL,
             dead_reason = '',
             updated_at = NOW()",
    ))
    .bind(node_id)
    .bind(&facets)
    .bind(&elements)
    .execute(&mut *conn)
    .await
    .map_err(EnqueueInventoryReconcileError::Reconcile)?;

    enqueue_triggers_conn(conn, [node_id], trigger_reason)
        .await
        .map_err(EnqueueInventoryReconcileError::Trigger)?;

    Ok(())
}
