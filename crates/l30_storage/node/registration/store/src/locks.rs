//! Row locks on `nodes` and `node_k8s` that a caller takes in its own
//! transaction and relies on after the statement: each lock lasts until the
//! caller's transaction ends. Lock-timeout settings stay with the caller.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// Share-lock `node_id`'s Kubernetes `nodes` row if it is in `drain_phase`
/// (`None` = outside any drain), and report whether it was. The lock lasts
/// until the caller's transaction ends, holding off drain-phase writers.
/// The caller must pass its open transaction.
pub async fn share_lock_k8s_node_in_drain_phase<C>(
    tx: &mut C,
    node_id: NodeId,
    drain_phase: Option<&str>,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let node: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT 1 FROM nodes
          WHERE id = $1 AND drain_phase IS NOT DISTINCT FROM $2 AND managed_by = $3
          FOR SHARE"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(node.is_some())
}

/// Share-lock `node_id`'s `node_k8s` row if it carries `k8s_node_uid`
/// (`None` = no UID), and report whether it does. The lock lasts until the
/// caller's transaction ends, holding off a UID change. The caller must pass
/// its open transaction.
pub async fn share_lock_k8s_registration_with_uid<C>(
    tx: &mut C,
    node_id: NodeId,
    k8s_node_uid: Option<&str>,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let uid: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT 1 FROM node_k8s
          WHERE node_id = $1 AND k8s_node_uid IS NOT DISTINCT FROM $2
          FOR SHARE"
    ))
    .bind(node_id)
    .bind(k8s_node_uid)
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(uid.is_some())
}

/// Share-lock `node_id`'s `node_k8s` row and read its Kubernetes identity:
/// `(k8s_node_uid, instance_id)`, `None` without a row. The lock lasts until
/// the caller's transaction ends, holding off a registration that would
/// move either. The caller must pass its open transaction.
pub async fn share_lock_k8s_identity<C>(
    tx: &mut C,
    node_id: NodeId,
) -> Result<Option<(Option<String>, Option<String>)>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as(sql!(
        "SELECT k8s_node_uid, instance_id FROM node_k8s WHERE node_id = $1 FOR SHARE"
    ))
    .bind(node_id)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// Share-lock `node_id`'s `nodes` row, and report whether it exists. The
/// lock lasts until the caller's transaction ends; a missing row locks
/// nothing, so a row inserted afterwards isn't fenced. The caller must pass
/// its open transaction.
pub async fn share_lock_node<C>(tx: &mut C, node_id: NodeId) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let row: Option<i32> = sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR SHARE"))
        .bind(node_id)
        .fetch_optional(tx.into_sqlx())
        .await?;
    Ok(row.is_some())
}

/// Lock `node_id`'s `nodes` row `FOR UPDATE`, and report whether it exists.
/// The lock lasts until the caller's transaction ends; a missing row locks
/// nothing. The caller must pass its open transaction.
pub async fn lock_node_for_update<C>(tx: &mut C, node_id: NodeId) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let row: Option<i32> = sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
        .bind(node_id)
        .fetch_optional(tx.into_sqlx())
        .await?;
    Ok(row.is_some())
}

/// Lock `node_id`'s `nodes` row `FOR NO KEY UPDATE`, and report whether it
/// exists. The lock lasts until the caller's transaction ends. Unlike
/// `FOR UPDATE`, it lets other transactions take the key-share locks that
/// foreign keys referencing the row take. The caller must pass its open
/// transaction.
pub async fn lock_node_for_no_key_update<C>(tx: &mut C, node_id: NodeId) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let row = sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT id FROM nodes WHERE id = $1 FOR NO KEY UPDATE"
    ))
    .bind(node_id)
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(row.is_some())
}

#[cfg(test)]
mod tests;
