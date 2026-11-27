//! Which Kubernetes nodes that are neither `ready` nor draining still carry
//! live reconcile work queued longer than a retention period: the reconcile
//! queue GC's candidates for a node that is gone.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// A node with stale live reconcile work, and its Kubernetes registration.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StaleReconcileWorkNode {
    pub id: NodeId,
    pub k8s_node_name: String,
    pub k8s_node_uid: Option<String>,
}

/// Up to `limit` Kubernetes nodes after `after`, in id order, that are not
/// `ready`, not draining, and have a live ref or trigger requested at least
/// `retention_secs` ago. `NOW()` stays in the statement, so the age is
/// measured on the database clock.
pub async fn unready_nodes_with_stale_reconcile_work(
    executor: impl Executor<'_>,
    after: Option<NodeId>,
    retention_secs: i64,
    limit: i64,
) -> Result<Vec<StaleReconcileWorkNode>, DbError> {
    Ok(sqlx::query_as::<_, StaleReconcileWorkNode>(sql!(
        "SELECT n.id, k.k8s_node_name, k.k8s_node_uid
         FROM nodes n
         JOIN node_k8s k ON k.node_id = n.id
         WHERE n.managed_by = $4
           AND n.state IS DISTINCT FROM 'ready'
           AND n.drain_phase IS NULL
           AND ($1::UUID IS NULL OR n.id > $1)
           AND (
               EXISTS (
                   SELECT 1
                   FROM node_reconcile_queue q
                   WHERE q.node_id = n.id
                     AND q.dead_at IS NULL
                     AND q.requested_at <= NOW() - ($2 * INTERVAL '1 second')
               )
               OR EXISTS (
                   SELECT 1
                   FROM node_reconcile_triggers t
                   WHERE t.node_id = n.id
                     AND t.dead_at IS NULL
                     AND t.requested_at <= NOW() - ($2 * INTERVAL '1 second')
               )
           )
         ORDER BY n.id ASC
         LIMIT $3"
    ))
    .bind(after)
    .bind(retention_secs)
    .bind(limit)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
