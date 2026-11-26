//! The orphaned-nodes sweep: Kubernetes node rows whose instance is gone
//! outside the drain path ([`candidates`]), and their guarded delete
//! ([`delete_if_orphan`]).
//!
//! Both read the same eligibility:
//!
//! - `managed_by` is the Kubernetes substrate: Direct rows are owned by a
//!   different lifecycle.
//! - `drain_phase IS NULL` and no v2 drain (`drain_engine = 'v2'`, whose
//!   cancel pending has no phase): a row in a drain phase belongs to node
//!   drain and the drained-node terminator, and a v2 drain's row to the v2
//!   loop.
//! - No sandbox outside `failed` and `deleted` names the node through
//!   `active_node_id` or `target_node_id`: a suspended sandbox pinned to a
//!   gone node still has its only snapshot there. `quarantined` is not
//!   terminal here: it keeps the node association until an operator or a
//!   healer releases the sandbox.
//!
//! The delete also re-checks the `node_k8s.k8s_node_uid` the scan read:
//! `nodes.id` derives from the Kubernetes node name, so a replacement
//! instance behind the same name lands on the same row, and its
//! registration moves the uid.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// A Kubernetes node row eligible for the orphaned-nodes delete, with its
/// registration as the scan read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCandidateRow {
    pub node_id: NodeId,
    pub k8s_node_name: String,
    pub k8s_node_uid: Option<String>,
}

/// Up to `limit` eligible rows (see the module docs) with a Kubernetes node
/// name, by id, after `after` (`None` = from the start). The caller pages
/// through the ids across ticks, so a front of live nodes can't starve the
/// rest.
///
/// The placement test is two `NOT EXISTS` probes rather than one over
/// `active_node_id = n.id OR target_node_id = n.id`: the `OR` makes the
/// planner `BitmapOr` both partial indexes and read every row pinned to the
/// node, while each probe walks its own index and stops at the first
/// non-terminal row. The two forms are equivalent.
pub async fn candidates(
    executor: impl Executor<'_>,
    after: Option<NodeId>,
    limit: i64,
) -> Result<Vec<OrphanCandidateRow>, DbError> {
    let rows: Vec<(NodeId, String, Option<String>)> = sqlx::query_as(sql!(
        "SELECT n.id, k.k8s_node_name, k.k8s_node_uid
             FROM nodes n
             JOIN node_k8s k ON k.node_id = n.id
             WHERE n.managed_by = $3
               AND n.drain_phase IS NULL
               AND n.drain_engine IS DISTINCT FROM 'v2'
               AND k.k8s_node_name IS NOT NULL
               AND ($1::UUID IS NULL OR n.id > $1)
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE active_node_id = n.id
                      AND lifecycle NOT IN ('failed', 'deleted')
               )
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE target_node_id = n.id
                      AND lifecycle NOT IN ('failed', 'deleted')
               )
             ORDER BY n.id ASC
             LIMIT $2"
    ))
    .bind(after)
    .bind(limit)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(node_id, k8s_node_name, k8s_node_uid)| OrphanCandidateRow {
                node_id,
                k8s_node_name,
                k8s_node_uid,
            },
        )
        .collect())
}

/// Delete `node_id`'s row while it is still eligible (see the module docs)
/// and its `node_k8s.k8s_node_uid` is still `expected_uid`. Returns whether
/// the row was deleted.
///
/// The uid compares through `COALESCE(…, '')`, so a missing or NULL stored
/// uid matches a `None` expected one; a uid that appeared or moved since
/// the scan refuses.
pub async fn delete_if_orphan(
    executor: impl Executor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "DELETE FROM nodes n
             WHERE n.id = $1
               AND n.managed_by = $3
               AND n.drain_phase IS NULL
               AND n.drain_engine IS DISTINCT FROM 'v2'
               AND COALESCE(
                   (SELECT k8s_node_uid FROM node_k8s WHERE node_id = $1),
                   ''
               ) = COALESCE($2, '')
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE active_node_id = $1
                      AND lifecycle NOT IN ('failed', 'deleted')
               )
               AND NOT EXISTS (
                   SELECT 1 FROM sandboxes
                    WHERE target_node_id = $1
                      AND lifecycle NOT IN ('failed', 'deleted')
               )"
    ))
    .bind(node_id)
    .bind(expected_uid)
    .bind(SubstrateKind::Kubernetes)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
