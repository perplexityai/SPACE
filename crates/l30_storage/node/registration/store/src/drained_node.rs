//! The drained-node terminator's guarded writes of a `drained` Kubernetes
//! node row: the `drained → terminating` flip ([`mark_terminating`]) and the
//! KubeNodeGone reap of a row whose instance is gone ([`delete_if_orphan`]).
//!
//! Both re-check, in the statement that writes, the same two guards over the
//! rows that still name the node: the snapshot guard, node-drain's
//! `draining → drained` gate
//! (`pplx_space_sandbox_portability::drain_gate_blockers_for_node`), and the
//! resident guard
//! (`pplx_space_sandbox_residency::active_resident_predicate`). A drained row
//! is reapable on exactly the condition that would let it advance to
//! `terminating`. Neither touches a v2 drain's row (`drain_engine IS NULL`),
//! which ends through its own loop. Each returns whether it wrote the row;
//! a refusal writes nothing, and the caller decides what to do with it.
//!
//! The snapshot guard is the predicate node-drain's `draining → drained` gate
//! counts, and the terminator's pre-check counts it too
//! (`pplx_space_sandbox_store::drain_gate_blockers::count_on_node`): a
//! sandbox mid-flight on the node, one suspended without a portable copy, or
//! one whose durable upload is still in flight from the node, blocks. It is
//! interpolated as a complete `SELECT` of the blocking ids rather than as a
//! bare predicate over `sandboxes`: the predicate's two disjuncts attribute
//! to different columns, and an `OR` over both gives the planner no indexable
//! driver. The resident guard is node-drain's resident predicate too. With
//! node drain's termination-blocking gate on, its `draining → drained` write
//! counts the same rows, so a node this guard would refuse stays in
//! `draining`. With the gate off, the default, that write counts only the
//! resident include-list, so such a node can reach `drained` and park here.
//! The refusal diagnosis ([`crate::orphan_reap_refusal`]) counts both guards
//! through the same fragments.
//! [`drain_gate_blockers_for_node`]'s docs carry the per-state rationale of
//! the snapshot guard, and [`active_resident_predicate`]'s which lifecycles
//! the resident guard counts and why.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeDrainPhase, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::drain_gate_blockers_for_node;
use pplx_space_sandbox_residency::{ResidencyEvidence, active_resident_predicate};
use pplx_space_sql_locator::sql;

/// The statement [`mark_terminating`] runs under `residency`. `$1` = node
/// id, `$2` = the new phase (`terminating`), `$3` = the expected phase
/// (`drained`).
fn mark_terminating_for(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = drain_gate_blockers_for_node("$1");
    let predicate = active_resident_predicate("sandboxes", "$1", residency);
    sql!(format!(
        "UPDATE nodes
         SET drain_phase = $2,
             drain_last_error = NULL
         WHERE id = $1
           AND drain_phase = $3
           -- A v2 drain ends through its own loop.
           AND drain_engine IS NULL
           -- Snapshot guard: block on local-disk lifecycles and in-flight
           -- durable uploads (same predicate as local_snapshots_count_query;
           -- atomic here to close the TOCTOU with the pre-check).
           AND NOT EXISTS ({snapshot_blockers})
           -- Resident guard (shared with delete_drained_node_if_orphan_query):
           -- if cordon was somehow lost and a new sandbox landed on a drained
           -- node, do not advance it to terminating — the sandbox would be
           -- killed when the node is destroyed. node_drain::handle_draining_node
           -- would need to evacuate it first, but that path only runs for
           -- 'draining' nodes, not 'drained'. Blocking here lets an operator
           -- catch the anomaly.
           AND NOT EXISTS (
             SELECT 1 FROM sandboxes
             WHERE {predicate}
         )"
    ))
}

static MARK_TERMINATING: LazyLock<String> =
    LazyLock::new(|| mark_terminating_for(ResidencyEvidence::LifecycleOnly));
static MARK_TERMINATING_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| mark_terminating_for(ResidencyEvidence::NodeCensusAware));

/// Flip `node_id` from `drained` to `terminating`, clearing
/// `drain_last_error`, only while it is a v1 drain's row and neither guard
/// (see the module docs) holds it. Returns whether the row changed.
///
/// The snapshot guard is re-checked here, atomic with the flip, so a sandbox
/// completing its suspend after the caller's pre-check can't race past it.
/// The resident guard refuses a node a sandbox landed on after a lost
/// cordon: node-drain evacuates only `draining` nodes, so refusing here
/// leaves the anomaly for an operator. `residency` must be what node-drain's
/// `draining → drained` gate used, or a node that converged past that gate
/// parks here.
pub async fn mark_terminating(
    executor: impl Executor<'_>,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> Result<bool, DbError> {
    let statement: &str = match residency {
        ResidencyEvidence::LifecycleOnly => &MARK_TERMINATING,
        ResidencyEvidence::NodeCensusAware => &MARK_TERMINATING_CENSUS_AWARE,
    };
    let result = sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(NodeDrainPhase::Terminating.as_str())
        .bind(NodeDrainPhase::Drained.as_str())
        .execute(executor.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
}

/// The statement [`delete_if_orphan`] runs under `residency`. `$1` = node
/// id, `$2` = the expected `node_k8s.k8s_node_uid`, `$3` = the Kubernetes
/// substrate.
fn delete_if_orphan_for(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = drain_gate_blockers_for_node("$1");
    let predicate = active_resident_predicate("sandboxes", "$1", residency);
    sql!(format!(
        "DELETE FROM nodes
         WHERE id = $1
           AND drain_phase = 'drained'
           AND drain_engine IS NULL
           AND managed_by = $3
           -- Identity guard: refuse if a same-name kube Node replacement
           -- bumped node_k8s.k8s_node_uid since the EC2 cross-check observed
           -- it (the row would now belong to the fresh node).
           AND EXISTS (
               SELECT 1 FROM node_k8s
               WHERE node_id = $1
                 AND k8s_node_uid IS NOT DISTINCT FROM $2
           )
           -- Snapshot guard: same marker-aware predicate as
           -- mark_terminating_query, so a durably-portable suspended sandbox
           -- does not block the reap but a local-only one does.
           AND NOT EXISTS ({snapshot_blockers})
           -- Resident guard: same shared clause as mark_terminating_query — a
           -- genuinely-active (non-suspended) sandbox still pinned to the node
           -- blocks the reap.
           AND NOT EXISTS (
             SELECT 1 FROM sandboxes
             WHERE {predicate}
         )"
    ))
}

static DELETE_IF_ORPHAN: LazyLock<String> =
    LazyLock::new(|| delete_if_orphan_for(ResidencyEvidence::LifecycleOnly));
static DELETE_IF_ORPHAN_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| delete_if_orphan_for(ResidencyEvidence::NodeCensusAware));

/// Hard-delete `node_id`'s `drained`, Kubernetes-managed v1 row for the
/// KubeNodeGone recovery, once the caller has confirmed its instance is
/// terminated. Returns whether the row was deleted.
///
/// Refuses while the row's `node_k8s.k8s_node_uid` is not `expected_uid`
/// (compared `IS NOT DISTINCT FROM`): a same-name Kubernetes Node
/// replacement lands on the same `nodes.id` and bumps the uid, and its row
/// must not be reaped. A row with no `node_k8s` row refuses whatever
/// `expected_uid` is. Refuses on either guard of the module docs too, as
/// [`mark_terminating`] does: a `suspended` row whose snapshot is durable
/// doesn't block, one whose only copy is on the node does, and so does a
/// resident. `residency` is as for [`mark_terminating`]. After a refusal,
/// [`crate::orphan_reap_refusal::diagnose`] re-evaluates the phase and
/// substrate, uid, snapshot and resident guards on their own.
pub async fn delete_if_orphan(
    executor: impl Executor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    residency: ResidencyEvidence,
) -> Result<bool, DbError> {
    let statement: &str = match residency {
        ResidencyEvidence::LifecycleOnly => &DELETE_IF_ORPHAN,
        ResidencyEvidence::NodeCensusAware => &DELETE_IF_ORPHAN_CENSUS_AWARE,
    };
    let result = sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(expected_uid)
        .bind(SubstrateKind::Kubernetes)
        .execute(executor.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
