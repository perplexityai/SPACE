//! Unguarded reads of `nodes` drain columns: one node's drain columns, the
//! per-phase work lists and counts, the terminate re-read, the draining rows
//! behind a Kubernetes node name, and the read before a terminating node's
//! delete.
//! Node drain's phase compare-and-sets (`set_drain_phase`) stay with their
//! caller;
//! [`crate::drain_write`] holds the writes that check only the node id.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeDrainPhase, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// A node's drain columns, all undecoded: a phase or an engine this build
/// does not know still reads, and each caller decides what it means. The
/// columns are read in this order, the phase first, so a caller's decode
/// error can name a column by its position.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeDrainColumns {
    pub drain_phase: Option<String>,
    pub drain_mode: Option<String>,
    pub drain_engine: Option<String>,
}

/// The drain columns of `node_id`; `None` when the `nodes` row is gone.
pub async fn drain_columns(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<NodeDrainColumns>, DbError> {
    Ok(sqlx::query_as::<_, NodeDrainColumns>(sql!(
        "SELECT drain_phase, drain_mode, drain_engine FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// How many nodes are in one drain phase.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DrainPhaseCount {
    pub drain_phase: Option<String>,
    pub count: i64,
}

/// The node count of every drain phase that has nodes. Nodes without a
/// phase are not counted.
pub async fn drain_phase_counts(
    executor: impl Executor<'_>,
) -> Result<Vec<DrainPhaseCount>, DbError> {
    Ok(sqlx::query_as::<_, DrainPhaseCount>(sql!(
        "SELECT drain_phase, COUNT(*) AS count FROM nodes
             WHERE drain_phase IS NOT NULL
             GROUP BY drain_phase"
    ))
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Every node of `managed_by` in v1 drain phase `phase`. A v2 drain's node
/// (`drain_engine` set) is left to the node drain's v2 loop.
pub async fn nodes_in_drain_phase(
    executor: impl Executor<'_>,
    phase: NodeDrainPhase,
    managed_by: SubstrateKind,
) -> Result<Vec<NodeId>, DbError> {
    Ok(sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT id FROM nodes
          WHERE drain_phase = $1 AND managed_by = $2 AND drain_engine IS NULL"
    ))
    .bind(phase.as_str())
    .bind(managed_by)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// A node in a drain phase and the raw `drain_mode` column, undecoded so a
/// value this build does not know can't drop the row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeInDrainPhase {
    pub id: NodeId,
    pub drain_mode: Option<String>,
}

/// Every node of `managed_by` in v1 drain phase `phase`, with its drain
/// mode. A v2 drain's node (`drain_engine` set) is left to the node drain's
/// v2 loop.
pub async fn nodes_in_drain_phase_with_mode(
    executor: impl Executor<'_>,
    phase: NodeDrainPhase,
    managed_by: SubstrateKind,
) -> Result<Vec<NodeInDrainPhase>, DbError> {
    Ok(sqlx::query_as::<_, NodeInDrainPhase>(sql!(
        "SELECT id, drain_mode FROM nodes
          WHERE drain_phase = $1 AND managed_by = $2 AND drain_engine IS NULL"
    ))
    .bind(phase.as_str())
    .bind(managed_by)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// What a terminator re-reads before it touches the node's infrastructure:
/// the drain phase, the registered Kubernetes node name and UID (`None`
/// without a `node_k8s` row), and the per-drain replace-after-terminate
/// intent.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TerminateTarget {
    pub phase: Option<String>,
    pub node_name: Option<String>,
    pub stored_uid: Option<String>,
    pub decrement_capacity: bool,
}

/// [`TerminateTarget`] of `node_id`, in one statement; `None` when the
/// `nodes` row is gone.
pub async fn terminate_target(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<TerminateTarget>, DbError> {
    Ok(sqlx::query_as::<_, TerminateTarget>(sql!(
        "SELECT n.drain_phase AS phase,
                    k.k8s_node_name AS node_name,
                    k.k8s_node_uid  AS stored_uid,
                    n.terminate_decrement_capacity AS decrement_capacity
             FROM nodes n
             LEFT JOIN node_k8s k ON k.node_id = n.id
             WHERE n.id = $1"
    ))
    .bind(node_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The drain columns of a node that is draining. The engine is undecoded:
/// one this build does not know still reads. A phase it does not know fails
/// the read; a v2 drain being cancelled has none.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DrainingSibling {
    pub drain_engine: Option<String>,
    pub drain_phase: Option<NodeDrainPhase>,
    pub drain_mode: Option<String>,
}

/// Every node registered under a Kubernetes node name that has a drain
/// phase or a drain engine. A v2 drain being cancelled has no phase, and its
/// node may still be sealed, so an engine without a phase counts.
/// `node_k8s.k8s_node_name` has no unique constraint, so one Kubernetes node
/// can map to several `nodes` rows (worker and builder roles), and those rows
/// can differ in both phase and mode; there is no `LIMIT`, and the caller
/// picks among them.
pub async fn draining_nodes_for_k8s_node_name(
    executor: impl Executor<'_>,
    k8s_node_name: &str,
) -> Result<Vec<DrainingSibling>, DbError> {
    Ok(sqlx::query_as::<_, DrainingSibling>(sql!(
        "SELECT n.drain_engine, n.drain_phase, n.drain_mode
           FROM node_k8s nk
           JOIN nodes n ON n.id = nk.node_id
          WHERE nk.k8s_node_name = $1
            AND (n.drain_phase IS NOT NULL OR n.drain_engine IS NOT NULL)"
    ))
    .bind(k8s_node_name)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// A Kubernetes node in `drain_phase = 'terminating'`: its role, undecoded,
/// and its Kubernetes node name when it has a registration.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TerminatingK8sNode {
    pub role: String,
    pub k8s_node_name: Option<String>,
}

/// [`TerminatingK8sNode`] of `node_id`; `None` when the `nodes` row is gone,
/// isn't Kubernetes-managed, or isn't `terminating`. A read only: the delete
/// that follows re-checks the phase itself.
pub async fn terminating_k8s_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
) -> Result<Option<TerminatingK8sNode>, DbError> {
    Ok(sqlx::query_as::<_, TerminatingK8sNode>(sql!(
        "SELECT n.role, nk.k8s_node_name
         FROM nodes n
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE n.id = $1
           AND n.drain_phase = 'terminating'
           AND n.managed_by = $2"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
