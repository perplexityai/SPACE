//! Postgres state of SPACE nodes: `nodes`, the Kubernetes registration in
//! `node_k8s`, and `node_k8s_tombstones`.
//!
//! Registering a Kubernetes node ([`register_k8s_node`]), which also records
//! the EC2 instance it runs on, and the
//! registration check on `node_k8s`: whether a Kubernetes node name and UID
//! map to a SPACE node ([`registered_node`]), or which live registrations in
//! given roles carry that name ([`nodes_for_identity`]). [`lookup`] resolves
//! nodes through their registration, [`drain_reads`] reads the drain columns
//! without writing them, [`drain_write`] writes them checking only the node
//! id, [`tombstone`] records terminated nodes and deletes a terminating one
//! before its tombstone, [`inventory`] lists the Kubernetes nodes,
//! [`demotion`] demotes ready ones missing from the cluster, [`node_group`]
//! mirrors their node-group labels, and [`orphan_reap_refusal`] says why the
//! drained-node terminator's reap of a gone node's row refused. [`direct`]
//! holds the statements that join or test a node's Direct registration in
//! `node_direct` (`pplx-space-direct-store`'s): the admin node row and the
//! hard deletes of Direct nodes and their expired tombstones, and in its
//! submodules a Direct node's endpoint, the Direct substrate's placement
//! candidates, and the admin node registry's reads and the writes of a Direct
//! node's `nodes` row ([`direct::registry`]). [`stale_reconcile_work`] finds
//! the Kubernetes nodes the reconcile queue GC may treat as gone.
//! [`orphan_reap_refusal`] also counts `sandboxes` rows, which belong to
//! `pplx-space-sandbox-store`, through the residency and portability
//! fragments the reap's guards use. [`drained_node`]'s guards read
//! `sandboxes` and the snapshot tables through those fragments, and in
//! census-aware residency also `pplx-space-node-status-store`'s
//! `node_status_snapshots` and `pplx-space-sandbox-teardown`'s
//! `sandbox_observed_states`, [`orphaned_nodes`] reads `sandboxes`, and [`guarded_delete`] reads
//! `sandboxes`, `sandbox_k8s_executions` and `pplx-space-node-status-store`'s
//! `node_status_snapshots`.
//!
//! The cluster controller's sweeps issue these guarded writes of a node row,
//! each re-checking in its own statement what the caller decided on, and
//! each returning whether it wrote: [`drained_node`], the drained-node
//! terminator's `drained → terminating` flip and its reap of a drained row
//! whose instance is gone; [`orphaned_nodes`], the orphaned-nodes sweep's
//! candidates and delete; [`guarded_delete`], node drain's delete of a gone
//! node's row with no tombstone, and the heartbeat read before it; and
//! [`tombstone::delete_terminating_k8s_node`], the delete of a terminating
//! row before its tombstone.
//!
//! Every verb runs on the caller's executor, so it composes into the
//! caller's transaction. The row locks in [`locks`] and
//! [`direct::lock_admin_node`] take the caller's transaction instead, and
//! hold until it ends. [`guarded_delete`]'s verbs take the caller's
//! transaction too, and run under its row lock. Node drain's phase
//! compare-and-sets (the cluster controller's `set_drain_phase`), whose
//! `WHERE` re-checks the current phase, stay with their caller; the
//! drained-node terminator's `drained → terminating` flip is
//! [`drained_node::mark_terminating`], and a v2 drain's phase moves are in
//! [`drain_v2`].

use pplx_space_accounts::NodeId;
use pplx_space_k8s_ids::{K8sNodeName, K8sNodeUid};
use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_sql_locator::sql;

pub mod demotion;
pub mod direct;
pub mod drain_reads;
pub mod drain_v2;
pub mod drain_write;
pub mod drained_node;
pub mod guarded_delete;
pub mod inventory;
pub mod locks;
pub mod lookup;
pub mod node_group;
pub mod orphan_reap_refusal;
pub mod orphaned_nodes;
mod register;
pub mod stale_reconcile_work;
pub mod tombstone;
pub use register::{
    K8sNodeRegistration, K8sNodeRegistrationError, K8sNodeRegistrationStep, register_k8s_node,
};

#[derive(Debug, thiserror::Error)]
pub enum NodeRegistrationError {
    #[error("node registration lookup failed: {0}")]
    Query(#[from] sqlx::Error),
}

/// Require both name and UID; a registration without a UID cannot match.
pub async fn registered_node(
    executor: impl sqlx::PgExecutor<'_>,
    name: &K8sNodeName,
    uid: &K8sNodeUid,
) -> Result<Option<NodeId>, NodeRegistrationError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT node_id FROM node_k8s
         WHERE k8s_node_name = $1 AND k8s_node_uid = $2 AND k8s_node_uid IS NOT NULL"
    ))
    .bind(name.as_str())
    .bind(uid.as_str())
    .fetch_optional(executor)
    .await?)
}

/// Live registrations of the reviewed node name in `roles`, each with
/// whether its inventory UID matches (`None` when it has none).
pub async fn nodes_for_identity(
    executor: impl sqlx::PgExecutor<'_>,
    name: &K8sNodeName,
    uid: &K8sNodeUid,
    roles: &[NodeRole],
) -> Result<Vec<(NodeId, Option<bool>)>, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT n.id, k.k8s_node_uid = $2
         FROM nodes n JOIN node_k8s k ON k.node_id = n.id
         WHERE k.k8s_node_name = $1
           AND n.managed_by = $3 AND n.role = ANY($4::TEXT[])
           AND COALESCE(n.state, 'registered') != 'deleted'"
    ))
    .bind(name.as_str())
    .bind(uid.as_str())
    .bind(SubstrateKind::Kubernetes)
    .bind(roles.iter().map(|role| role.as_str()).collect::<Vec<_>>())
    .fetch_all(executor)
    .await
}

#[cfg(test)]
mod tests;
