//! `node_k8s_tombstones` writes, and the guarded delete of a terminating
//! node that runs before one ([`delete_terminating_k8s_node`]). A tombstone
//! outlives the node's `nodes` and `node_k8s` rows, and registration refuses
//! a tombstoned node id.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// Delete `node_id`'s Kubernetes row while it is in `drain_phase =
/// 'terminating'` under a v1 drain (`drain_engine IS NULL`); its `node_k8s`
/// row cascades. Returns whether the row was deleted. A v2 drain's node is
/// never deleted here. Run it in the transaction that then records the
/// node's tombstone ([`record_admin_terminate_tombstone`]), and record one
/// only when this returns `true`. The caller must pass its open transaction.
pub async fn delete_terminating_k8s_node<C>(tx: &mut C, node_id: NodeId) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let result = sqlx::query(sql!(
        "DELETE FROM nodes
         WHERE id = $1
           AND drain_phase = 'terminating'
           AND managed_by = $2
           AND drain_engine IS NULL"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(tx.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Tombstone `node_id` with reason `admin-terminate`, now, or re-date and
/// rewrite its existing tombstone. Run it in the transaction that deletes the
/// node, so the node is never gone without its tombstone. The caller must
/// pass its open transaction.
pub async fn record_admin_terminate_tombstone<C>(
    tx: &mut C,
    node_id: NodeId,
    k8s_node_name: &str,
    role: &str,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
