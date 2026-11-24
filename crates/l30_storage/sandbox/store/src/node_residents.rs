//! How many sandboxes still reside on one node, as node drain's resident scan
//! and its termination-blocking gate count them.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_residency::{
    ResidencyEvidence, active_resident_predicate, occupies_node_predicate,
};
use pplx_space_sql_locator::sql;

/// The resident scan's two counts for one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct ResidentCounts {
    /// Rows that still occupy the node under the caller's residency evidence.
    pub occupying: i64,
    /// Every row in a resident lifecycle whose `active_node_id` names the node.
    pub total: i64,
}

/// `$1` = node id, `$2` = the resident lifecycles as `text[]`.
///
/// Flat, with the expensive absence proof appearing once, inside the `FILTER`:
/// the bare `COUNT(*)` doesn't reference it, so there's nothing for the
/// planner to duplicate. Returning `total` rather than a second
/// `COUNT(*) FILTER (WHERE NOT …)` is what keeps that true.
fn counts_query(residency: ResidencyEvidence) -> String {
    let occupies = occupies_node_predicate("s", "$1", residency);
    sql!(format!(
        "SELECT COUNT(*) FILTER (WHERE {occupies})::BIGINT AS occupying,
                COUNT(*)::BIGINT AS total
           FROM sandboxes s
          WHERE s.active_node_id = $1
            AND s.lifecycle = ANY($2)"
    ))
}

static COUNTS_LIFECYCLE_ONLY: LazyLock<String> =
    LazyLock::new(|| counts_query(ResidencyEvidence::LifecycleOnly));
static COUNTS_NODE_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| counts_query(ResidencyEvidence::NodeCensusAware));

/// The rows in one of `resident_lifecycles` whose `active_node_id` is
/// `node_id`, and how many of them still occupy it under `residency`
/// ([`occupies_node_predicate`]). Counts, not ids. A read; it takes no lock,
/// and in the census-aware mode it compares against the executor's `NOW()`.
pub async fn counts(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    resident_lifecycles: &[&str],
    residency: ResidencyEvidence,
) -> Result<ResidentCounts, sqlx::Error> {
    let statement = match residency {
        ResidencyEvidence::LifecycleOnly => COUNTS_LIFECYCLE_ONLY.as_str(),
        ResidencyEvidence::NodeCensusAware => COUNTS_NODE_CENSUS_AWARE.as_str(),
    };
    sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .bind(resident_lifecycles)
        .fetch_one(executor)
        .await
}

/// `$1` = node id.
///
/// Unordered on purpose: `GROUP BY` alone lets the planner hash-aggregate,
/// while `ORDER BY lifecycle` can tip it into sorting every input row to order
/// a handful of groups. The caller sorts.
fn termination_blocking_query(residency: ResidencyEvidence) -> String {
    let resident = active_resident_predicate("sandboxes", "$1", residency);
    sql!(format!(
        "SELECT lifecycle, COUNT(*)::BIGINT AS blocking
           FROM sandboxes
          WHERE {resident}
          GROUP BY lifecycle"
    ))
}

static TERMINATION_BLOCKING_LIFECYCLE_ONLY: LazyLock<String> =
    LazyLock::new(|| termination_blocking_query(ResidencyEvidence::LifecycleOnly));
static TERMINATION_BLOCKING_NODE_CENSUS_AWARE: LazyLock<String> =
    LazyLock::new(|| termination_blocking_query(ResidencyEvidence::NodeCensusAware));

/// How many rows in each lifecycle [`active_resident_predicate`] counts
/// against `node_id` under `residency`: the rows that make the drained-node
/// terminator's resident guard refuse the node. Unordered; the lifecycle is
/// the stored text, so a value the enum doesn't model is counted too. A read;
/// it takes no lock, and in the census-aware mode it compares against the
/// executor's `NOW()`.
pub async fn termination_blocking_by_lifecycle(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let statement = match residency {
        ResidencyEvidence::LifecycleOnly => TERMINATION_BLOCKING_LIFECYCLE_ONLY.as_str(),
        ResidencyEvidence::NodeCensusAware => TERMINATION_BLOCKING_NODE_CENSUS_AWARE.as_str(),
    };
    sqlx::query_as(sqlx::AssertSqlSafe(statement))
        .bind(node_id)
        .fetch_all(executor)
        .await
}
