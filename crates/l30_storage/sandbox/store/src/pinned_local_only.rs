//! Which `suspended` sandboxes pinned to one node still have no durable copy:
//! node drain's promotion work queue, read from the node's placement history.
//!
//! The api's admin `:force-upload-local-snapshots` enumeration asks a
//! different question of the same rows (a stable, node-ordered page) and has
//! its own statement.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_exists, snapshot_durable_for_source_release,
};
use pplx_space_sql_locator::sql;

/// The `LIMIT` both statements carry, spelled into their text as a literal.
///
/// A literal rather than a bind parameter because the planner's
/// `tuple_fraction` comes from the `LIMIT` expression at plan time: a bound
/// `LIMIT $2` is not a constant in a generic plan, and the bias toward the
/// `target_node_id` driver would be lost under
/// `plan_cache_mode = force_generic_plan`. Node drain's enumeration cap plus
/// one, so its caller can tell "exactly the cap" from "more than the cap".
pub const PAGE_LIMIT: usize = 513;

/// `$1` = node id. The durability check is wrapped in a scalar subquery,
/// `NOT (SELECT …)`, so the planner can't pull it up into a hash or merge
/// anti-join over the whole `snapshots` ⋈ `snapshot_storage_locations`
/// product: each execution stays a per-row `SubPlan` probing
/// `idx_snapshots_sandbox_id`. The in-flight check beside it is left bare; it
/// quantifies only over the cluster's `creating` full snapshots.
/// The page is unordered so the planner keeps the `target_node_id` driver.
///
/// The full argument for the fence, the literal limit and the unordered page
/// is on `list_pinned_local_candidates` in
/// `pplx-space-cluster-controller-server`'s `node_drain/store.rs`.
static PAGE: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    sql!(format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND NOT (SELECT {snapshot_exists})
            AND NOT {inflight_upload}
          LIMIT 513"
    ))
});

/// [`PAGE`] without the rows whose `current_local_artifact_id` is NULL.
static PAGE_EXCLUDING_LEGACY_ROWS: LazyLock<String> = LazyLock::new(|| {
    let snapshot_exists = snapshot_durable_for_source_release("s");
    let inflight_upload = inflight_suspend_upload_exists("s");
    sql!(format!(
        "SELECT s.id
           FROM sandboxes s
          WHERE s.target_node_id = $1
            AND s.lifecycle = 'suspended'
            AND NOT (SELECT {snapshot_exists})
            AND NOT {inflight_upload}
            AND s.current_local_artifact_id IS NOT NULL
          LIMIT 513"
    ))
});

/// The statement [`page`] runs for `exclude_legacy_rows`; the tests EXPLAIN it.
fn page_statement(exclude_legacy_rows: bool) -> &'static str {
    if exclude_legacy_rows {
        &PAGE_EXCLUDING_LEGACY_ROWS
    } else {
        &PAGE
    }
}

/// Up to [`PAGE_LIMIT`] `suspended` sandboxes whose `target_node_id` is
/// `node_id`, with no durable copy ([`snapshot_durable_for_source_release`])
/// and no full snapshot upload in flight from any node
/// ([`inflight_suspend_upload_exists`]), in no particular order. With
/// `exclude_legacy_rows`, only rows that name their current local artifact.
/// A read; it takes no lock.
pub async fn page(
    executor: impl Executor<'_>,
    node_id: NodeId,
    exclude_legacy_rows: bool,
) -> Result<Vec<SandboxId>, DbError> {
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(page_statement(exclude_legacy_rows)))
            .bind(node_id)
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

#[cfg(test)]
mod tests;
