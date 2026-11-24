//! The admin Direct node registry's statements on `nodes`: the `nodes` row a
//! registration reactivates ([`reactivate_retired_node`]), inserts
//! ([`register_node`]) or updates ([`update_registered_node`]).
//!
//! Each write is the first step of a registry transaction that then writes
//! the node's `node_direct` row through `pplx-space-direct-store`;
//! `pplx-space-direct-coordinator` runs them. A write locks the `nodes` row
//! it changes, an insert the row it adds, until the caller's transaction
//! ends. Every verb runs on the caller's executor and returns what its
//! statement matched: matching no row is an outcome, not an error.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeManagedBy, NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;

/// The `nodes` columns a registry write returns.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RegisteredNodeRow {
    pub id: NodeId,
    /// `host(address)`: the address without its prefix length.
    pub address: Option<String>,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
}

/// The `nodes` columns a registration writes, for [`reactivate_retired_node`]
/// and [`register_node`].
#[derive(Debug, Clone, Copy)]
pub struct RegistryNodeWrite<'a> {
    /// `None` or an empty string stores no address.
    pub address: Option<&'a str>,
    pub role: NodeRole,
    pub managed_by: SubstrateKind,
    /// `None` stores `registered`.
    pub state: Option<&'a str>,
    /// Set `last_heartbeat` to the transaction's start time. Otherwise a
    /// reactivated row keeps its heartbeat and a new row has none.
    pub stamp_heartbeat: bool,
}

/// The `nodes` columns an update replaces, for [`update_registered_node`].
#[derive(Debug, Clone, Copy)]
pub struct RegistryNodeUpdate<'a> {
    /// `None` or an empty string keeps the address.
    pub address: Option<&'a str>,
    /// `None` keeps the role.
    pub role: Option<NodeRole>,
    /// `None` keeps the state.
    pub state: Option<&'a str>,
    /// Set `last_heartbeat` to the transaction's start time; otherwise keep
    /// it.
    pub stamp_heartbeat: bool,
}

/// Reactivate the retired node a registration of `node_id` at
/// `node_api_endpoint` matches, and write `write` to its `nodes` row,
/// `managed_by` included: a node `write.managed_by` manages whose
/// `node_direct` row is deleted, with id `node_id` or, when `match_endpoint`,
/// that endpoint. `node_id` itself wins, then the latest
/// `node_direct.updated_at`, the latest `nodes.created_at` and the highest
/// id. One statement, so the write acts only on the row the read found.
/// `None` when no retired node matches, or when the matched row is deleted
/// before it is written; nothing changes then.
pub async fn reactivate_retired_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    node_api_endpoint: &str,
    match_endpoint: bool,
    write: &RegistryNodeWrite<'_>,
) -> Result<Option<RegisteredNodeRow>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredNodeRow>(sql!(
        "WITH candidate AS (
           SELECT nd.node_id
           FROM node_direct nd
           JOIN nodes n ON n.id = nd.node_id
           WHERE nd.deleted_at IS NOT NULL
             AND n.managed_by = $4
             AND (
               n.id = $1
               OR ($3::BOOL AND nd.node_api_endpoint = $2)
             )
           ORDER BY (n.id = $1) DESC,
                    nd.updated_at DESC,
                    n.created_at DESC,
                    n.id DESC
           LIMIT 1
         )
         UPDATE nodes n
         SET address = NULLIF($5, '')::INET,
           role = $6,
           managed_by = $4,
           state = COALESCE($7, 'registered'),
           last_heartbeat = CASE
               WHEN $8::BOOL THEN NOW()
               ELSE n.last_heartbeat
           END
         WHERE n.id = (SELECT node_id FROM candidate)
         RETURNING n.id,
                   host(n.address) AS address,
                   n.role,
                   n.managed_by,
                   n.state,
                   n.last_heartbeat,
                   n.created_at"
    ))
    .bind(node_id)
    .bind(node_api_endpoint)
    .bind(match_endpoint)
    .bind(write.managed_by)
    .bind(write.address.unwrap_or(""))
    .bind(write.role.as_str())
    .bind(write.state)
    .bind(write.stamp_heartbeat)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Register `node_id`: insert its `nodes` row with `write`. A node with that
/// id fails the insert with a unique violation of `nodes_pkey`.
pub async fn register_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    write: &RegistryNodeWrite<'_>,
) -> Result<RegisteredNodeRow, DbError> {
    Ok(sqlx::query_as::<_, RegisteredNodeRow>(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat)
         VALUES (
             $1,
             NULLIF($2, '')::INET,
             $3,
             $4,
             COALESCE($5, 'registered'),
             CASE WHEN $6::BOOL THEN NOW() ELSE NULL END
         )
         RETURNING id,
                   host(address) AS address,
                   role,
                   managed_by,
                   state,
                   last_heartbeat,
                   created_at"
    ))
    .bind(node_id)
    .bind(write.address.unwrap_or(""))
    .bind(write.role.as_str())
    .bind(write.managed_by)
    .bind(write.state)
    .bind(write.stamp_heartbeat)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Update `node_id`'s `nodes` row with `update` if `managed_by` manages it,
/// whatever its `node_direct` row's state. `None` when no such node exists.
pub async fn update_registered_node(
    executor: impl Executor<'_>,
    node_id: NodeId,
    managed_by: SubstrateKind,
    update: &RegistryNodeUpdate<'_>,
) -> Result<Option<RegisteredNodeRow>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredNodeRow>(sql!(
        "UPDATE nodes n
         SET address = COALESCE(NULLIF($2, '')::INET, address),
             role = COALESCE($3, role),
             state = COALESCE($4, state),
             last_heartbeat = CASE
                 WHEN $5::BOOL THEN NOW()
                 ELSE n.last_heartbeat
             END
         WHERE n.id = $1
           AND n.managed_by = $6
         RETURNING n.id,
                   host(n.address) AS address,
                   n.role,
                   n.managed_by,
                   n.state,
                   n.last_heartbeat,
                   n.created_at"
    ))
    .bind(node_id)
    .bind(update.address.unwrap_or(""))
    .bind(update.role.map(NodeRole::as_str))
    .bind(update.state)
    .bind(update.stamp_heartbeat)
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
