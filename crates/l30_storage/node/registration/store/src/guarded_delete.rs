//! The statements of node drain's guarded delete of a gone node's row, with
//! no tombstone: whether its heartbeat is fresh ([`heartbeat_fresh`]) and the
//! delete itself ([`delete_unreferenced_stale_node`]).
//!
//! The caller locks the node row `FOR UPDATE`
//! ([`crate::locks::lock_node_for_update`]), checks its own ownership and the
//! Kubernetes identity under that lock
//! ([`crate::locks::share_lock_k8s_identity`]), and runs both statements on
//! the same transaction. The delete re-checks the stale heartbeat and that
//! nothing names the node
//! ([`pplx_space_sandbox_residency::node_unreferenced`]) in the statement
//! that deletes.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_residency::{node_heartbeat_fresh, node_unreferenced};
use pplx_space_sql_locator::sql;

/// `$1` = node id.
static HEARTBEAT_FRESH: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("$1");
    sql!(format!("SELECT {fresh}").as_str())
});

/// Whether `node_id`'s heartbeat is fresh
/// ([`pplx_space_sandbox_residency::node_heartbeat_fresh`]), on the database
/// clock of the caller's transaction. The caller must pass its open
/// transaction.
pub async fn heartbeat_fresh<C>(tx: &mut C, node_id: NodeId) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(HEARTBEAT_FRESH.as_str()))
            .bind(node_id)
            .fetch_one(tx.into_sqlx())
            .await?,
    )
}

/// `$1` = node id, `$2` = substrate, `$3` = the node's Kubernetes name.
static DELETE_UNREFERENCED_STALE_NODE: LazyLock<String> = LazyLock::new(|| {
    let fresh = node_heartbeat_fresh("$1");
    let unreferenced = node_unreferenced("$1", "$3");
    sql!(
        format!(
            "DELETE FROM nodes n
              WHERE n.id = $1
                AND n.managed_by = $2
                AND NOT {fresh}
                AND {unreferenced}"
        )
        .as_str()
    )
});

/// Delete `node_id`'s `managed_by` row while its heartbeat is stale and
/// nothing names it under `k8s_node_name` (`None` matches no execution).
/// Returns whether the row was deleted; its `node_k8s` row cascades. The
/// caller holds the row lock and has checked the node's identity; a
/// refusal writes nothing. The caller must pass its open transaction.
pub async fn delete_unreferenced_stale_node<C>(
    tx: &mut C,
    node_id: NodeId,
    managed_by: SubstrateKind,
    k8s_node_name: Option<&str>,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let deleted = sqlx::query(sqlx::AssertSqlSafe(DELETE_UNREFERENCED_STALE_NODE.as_str()))
        .bind(node_id)
        .bind(managed_by)
        .bind(k8s_node_name)
        .execute(tx.into_sqlx())
        .await?
        .rows_affected();
    Ok(deleted == 1)
}

#[cfg(test)]
mod tests;
