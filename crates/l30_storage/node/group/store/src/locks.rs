//! Locks on `node_groups` that a caller takes in its own transaction and
//! relies on after the statement: each lock lasts until the caller's
//! transaction ends. Lock-timeout settings stay with the caller.

use pplx_space_node_model::NodeGroupId;
use pplx_space_sql_locator::sql;
use sqlx::{Postgres, Transaction};

/// Lock the default group's row `FOR UPDATE`, if there is one, and return
/// its id. The lock lasts until the caller's transaction ends, so a move of
/// the default that takes it first serializes behind any other.
pub async fn lock_default_node_group(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<NodeGroupId>, sqlx::Error> {
    sqlx::query_scalar::<_, NodeGroupId>(sql!(
        "SELECT id FROM node_groups WHERE is_default FOR UPDATE"
    ))
    .fetch_optional(&mut **tx)
    .await
}

/// Lock the `node_groups` table in `SHARE ROW EXCLUSIVE` mode, which
/// conflicts with the `ROW EXCLUSIVE` lock every insert, update and delete
/// takes, but not with plain reads. The lock lasts until the caller's
/// transaction ends.
pub async fn lock_node_groups_share_row_exclusive(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!("LOCK TABLE node_groups IN SHARE ROW EXCLUSIVE MODE"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
