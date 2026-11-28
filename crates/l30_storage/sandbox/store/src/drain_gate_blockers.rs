//! How many sandboxes block one node's release: node drain's `draining →
//! drained` gate and the drained-node terminator's `drained → terminating`
//! pre-check ask the same count, and both read it here.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_portability::drain_gate_blockers_for_node;
use pplx_space_sql_locator::sql;

/// `$1` = node id. Built once: the fragment is [`drain_gate_blockers_for_node`]
/// for a bind placeholder, so the composed text never changes.
///
/// The count is over the fragment's complete `SELECT` of the blocking sandbox
/// ids rather than over the bare predicate on a `sandboxes` scan: the
/// predicate's two disjuncts attribute to different columns, so an `OR` over
/// both leaves the planner no indexable driver and it reads the whole table.
/// Deliberately uncapped: the fragment is a `UNION`, so an outer `LIMIT` would
/// truncate the result without bounding the scan.
static COUNT_ON_NODE: LazyLock<String> = LazyLock::new(|| {
    let blockers = drain_gate_blockers_for_node("$1");
    sql!(format!(
        "SELECT COUNT(*)::BIGINT
           FROM (
{blockers}
               ) node_blockers"
    ))
});

/// How many sandboxes [`drain_gate_blockers_for_node`] counts against
/// `node_id`: the mid-flight and not-yet-portable suspended rows pinned to it,
/// plus any sandbox whose full snapshot upload is still in flight from it.
/// A read; it takes no lock.
pub async fn count_on_node(executor: impl Executor<'_>, node_id: NodeId) -> Result<i64, DbError> {
    Ok(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(COUNT_ON_NODE.as_str()))
            .bind(node_id)
            .fetch_one(executor.into_sqlx())
            .await?,
    )
}
