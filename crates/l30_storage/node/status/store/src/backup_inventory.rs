//! Whether a node's report of the local artifacts it still owes a durable
//! backup is fresh enough to believe.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_residency::node_backup_inventory_fresh;
use pplx_space_sql_locator::sql;

/// `$1` = node id. Built once: the fragment is
/// [`node_backup_inventory_fresh`] for a bind placeholder, which carries its
/// own window, `pplx_space_sandbox_residency::CENSUS_INVENTORY_MAX_AGE`, as a
/// literal, so the composed text never changes. `node_status_snapshots` is
/// keyed by `node_id`, so this is a primary-key lookup.
static FRESH: LazyLock<String> =
    LazyLock::new(|| sql!(format!("SELECT {}", node_backup_inventory_fresh("$1"))));

/// Whether `node_id` reported its pending local backups within the window by
/// the database clock: FALSE when the node has no snapshot row, has never
/// reported one, or reported it too long ago. TRUE licenses reading an empty
/// pending-backup set as "nothing pending" rather than "never reported". A
/// read; it compares against the executor's `NOW()`.
pub async fn fresh(executor: impl Executor<'_>, node_id: NodeId) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(FRESH.as_str()))
        .bind(node_id)
        .fetch_one(executor.into_sqlx())
        .await?)
}

#[cfg(test)]
mod tests;
