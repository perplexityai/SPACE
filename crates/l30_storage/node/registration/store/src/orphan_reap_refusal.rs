//! Why the drained-node terminator's guarded reap of a gone node's row
//! matched nothing: each of its guards re-evaluated on its own.
//!
//! The reap itself, a guarded `DELETE` of `nodes`, stays with the terminator.
//! This read runs after it refused, so it can race concurrent writes; it feeds
//! logging, metrics and the give-up's pre-check, never a state change.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_portability::drain_gate_blockers_for_node;
use pplx_space_sandbox_residency::{ResidencyEvidence, active_resident_predicate};
use pplx_space_sql_locator::sql;

/// Each of the reap's guards, evaluated separately. All four clear at once
/// means the blocker resolved between the reap and this read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct OrphanReapRefusal {
    /// The row is no longer `drained` under the substrate the reap scoped to.
    pub phase_drifted: bool,
    /// `node_k8s.k8s_node_uid` is no longer the uid the caller observed.
    pub uid_mismatch: bool,
    /// The node's drain-gate blockers ([`drain_gate_blockers_for_node`]).
    pub snapshot_blockers: i64,
    /// The sandboxes [`active_resident_predicate`] counts against the node.
    pub active_residents: i64,
}

/// `$1` = node id, `$2` = the observed uid, `$3` = the substrate. The two
/// counts are the exact fragments behind the reap's `NOT EXISTS` guards.
fn diagnose_query(residency: ResidencyEvidence) -> String {
    let snapshot_blockers = drain_gate_blockers_for_node("$1");
    let resident_predicate = active_resident_predicate("sandboxes", "$1", residency);
    sql!(format!(
        "SELECT
             NOT EXISTS (
                 SELECT 1 FROM nodes
                 WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $3
             ) AS phase_drifted,
             NOT EXISTS (
                 SELECT 1 FROM node_k8s
                 WHERE node_id = $1
                   AND k8s_node_uid IS NOT DISTINCT FROM $2
             ) AS uid_mismatch,
             (SELECT COUNT(*)::BIGINT
                FROM ({snapshot_blockers}) node_blockers) AS snapshot_blockers,
             (SELECT COUNT(*)::BIGINT FROM sandboxes
               WHERE {resident_predicate}) AS active_residents"
    ))
}

static DIAGNOSE_LIFECYCLE_ONLY: LazyLock<String> =
    LazyLock::new(|| diagnose_query(ResidencyEvidence::LifecycleOnly));
static DIAGNOSE_NODE_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| diagnose_query(ResidencyEvidence::NodeCensusAware));

/// Re-evaluate the reap's guards for `node_id`: still `drained` under
/// `managed_by`, still registered under `expected_uid` (`IS NOT DISTINCT
/// FROM`), and how many drain-gate blockers and active residents (under
/// `residency`) hold it. A read; it takes no lock, and in the census-aware
/// mode it compares against the executor's `NOW()`.
pub async fn diagnose(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    expected_uid: Option<&str>,
    managed_by: SubstrateKind,
    residency: ResidencyEvidence,
) -> Result<OrphanReapRefusal, sqlx::Error> {
    let statement = match residency {
        ResidencyEvidence::LifecycleOnly => DIAGNOSE_LIFECYCLE_ONLY.as_str(),
        ResidencyEvidence::NodeCensusAware => DIAGNOSE_NODE_CENSUS_AWARE.as_str(),
    };
    sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(expected_uid)
        .bind(managed_by)
        .fetch_one(executor)
        .await
}
