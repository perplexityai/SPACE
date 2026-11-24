//! `node_k8s_tombstones` writes. A tombstone outlives the node's `nodes` and
//! `node_k8s` rows, and registration refuses a tombstoned node id.

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

/// Tombstone `node_id` with reason `admin-terminate`, now, or re-date and
/// rewrite its existing tombstone. Run it in the transaction that deletes the
/// node, so the node is never gone without its tombstone.
pub async fn record_admin_terminate_tombstone(
    executor: impl PgExecutor<'_>,
    node_id: NodeId,
    k8s_node_name: &str,
    role: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role, reason)
         VALUES ($1, $2, $3, 'admin-terminate')
         ON CONFLICT (node_id) DO UPDATE
         SET terminated_at = NOW(),
             k8s_node_name = EXCLUDED.k8s_node_name,
             role          = EXCLUDED.role,
             reason        = EXCLUDED.reason"
    ))
    .bind(node_id)
    .bind(k8s_node_name)
    .bind(role)
    .execute(executor)
    .await?;
    Ok(())
}
