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
//!
//! The registry's reads, and the other reads of Direct nodes' registry rows,
//! each of `nodes` joined to `node_direct`: a node's row as the admin node
//! registry reads it ([`registry_node`], [`registry_nodes`]) and as
//! internal-api lists it ([`listed_nodes`]), the live worker and builder ids
//! template reconciles target ([`live_worker_and_builder_ids`]), and how many
//! Direct nodes are tombstones ([`count_tombstones`]). Each read takes any
//! executor and writes nothing.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::{NodeManagedBy, NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use sqlx::types::Json;
use sqlx::{FromRow, PgExecutor};
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

/// A node as the admin node registry reads it: its `nodes` row, its
/// `node_direct` row's columns when it has one and its Kubernetes node name
/// when it has a `node_k8s` row.
#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct RegistryNodeRow {
    pub id: NodeId,
    pub name: Option<String>,
    pub address: Option<String>,
    pub node_api_endpoint: Option<String>,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub metadata: Option<Json<serde_json::Value>>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: Option<OffsetDateTime>,
    pub deleted_at: Option<OffsetDateTime>,
    pub node_name: Option<String>,
}

/// Node `id` of any substrate, deleted Direct rows included.
pub async fn registry_node<'e>(
    executor: impl PgExecutor<'e>,
    id: NodeId,
) -> Result<Option<RegistryNodeRow>, sqlx::Error> {
    sqlx::query_as::<_, RegistryNodeRow>(sql!(
        "SELECT n.id,
                nd.name,
                host(n.address) AS address,
                nd.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                nd.metadata,
                n.last_heartbeat,
                n.created_at,
                nd.updated_at,
                nd.deleted_at,
                nk.k8s_node_name AS node_name
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE n.id = $1"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await
}

/// The nodes of any substrate, of `role` when given, by role, then name, then
/// endpoint. A node whose `node_direct` row is deleted is listed only when
/// `include_deleted`; a node with no such row always is.
pub async fn registry_nodes<'e>(
    executor: impl PgExecutor<'e>,
    role: Option<NodeRole>,
    include_deleted: bool,
) -> Result<Vec<RegistryNodeRow>, sqlx::Error> {
    sqlx::query_as::<_, RegistryNodeRow>(sql!(
        "SELECT n.id,
                nd.name,
                host(n.address) AS address,
                nd.node_api_endpoint,
                n.role,
                n.managed_by,
                n.state,
                nd.metadata,
                n.last_heartbeat,
                n.created_at,
                nd.updated_at,
                nd.deleted_at,
                nk.k8s_node_name AS node_name
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         LEFT JOIN node_k8s nk ON nk.node_id = n.id
         WHERE ($1::TEXT IS NULL OR n.role = $1)
           AND ($2::BOOL OR nd.deleted_at IS NULL)
         ORDER BY n.role ASC,
                  COALESCE(nd.name, nk.k8s_node_name) ASC NULLS LAST,
                  nd.node_api_endpoint ASC NULLS LAST"
    ))
    .bind(role.map(NodeRole::as_str))
    .bind(include_deleted)
    .fetch_all(executor)
    .await
}

/// A Direct node as internal-api lists it, with its id, role, substrate and
/// metadata as text.
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ListedNodeRow {
    pub id: String,
    pub name: Option<String>,
    pub address: Option<String>,
    pub node_api_endpoint: String,
    pub role: String,
    pub managed_by: String,
    pub state: Option<String>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub deleted_at: Option<OffsetDateTime>,
    pub metadata_json: String,
}

/// The nodes under `managed_by` with a `node_direct` row, of `role` when
/// given, by role, then name, then endpoint. Deleted rows are listed only
/// when `include_deleted`.
pub async fn listed_nodes<'e>(
    executor: impl PgExecutor<'e>,
    role: Option<NodeRole>,
    include_deleted: bool,
    managed_by: SubstrateKind,
) -> Result<Vec<ListedNodeRow>, sqlx::Error> {
    sqlx::query_as::<_, ListedNodeRow>(sql!(
        "SELECT n.id::TEXT AS id,
                    nd.name,
                    host(n.address) AS address,
                    nd.node_api_endpoint,
                    n.role,
                    n.managed_by,
                    n.state,
                    n.last_heartbeat,
                    n.created_at,
                    nd.updated_at,
                    nd.deleted_at,
                    nd.metadata::TEXT AS metadata_json
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE ($1::TEXT IS NULL OR n.role = $1)
               AND ($2::BOOL OR nd.deleted_at IS NULL)
               AND n.managed_by = $3
             ORDER BY n.role ASC, nd.name ASC NULLS LAST, nd.node_api_endpoint ASC"
    ))
    .bind(role.map(NodeRole::as_str))
    .bind(include_deleted)
    .bind(managed_by)
    .fetch_all(executor)
    .await
}

/// The worker and builder ids of every substrate other than `direct`, and
/// `direct`'s whose `node_direct` row is live, each once.
pub async fn live_worker_and_builder_ids<'e>(
    executor: impl PgExecutor<'e>,
    direct: SubstrateKind,
) -> Result<Vec<NodeId>, sqlx::Error> {
    sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT DISTINCT n.id
         FROM nodes n
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         WHERE n.role IN ('worker', 'builder')
           AND (
             n.managed_by != $1
             OR (nd.node_id IS NOT NULL AND nd.deleted_at IS NULL)
           )"
    ))
    .bind(direct)
    .fetch_all(executor)
    .await
}

/// How many nodes under `managed_by` are tombstones: `nodes.state` is
/// `deleted` and the `node_direct` row has a `deleted_at`.
pub async fn count_tombstones<'e>(
    executor: impl PgExecutor<'e>,
    managed_by: SubstrateKind,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
               FROM nodes n
               JOIN node_direct nd ON nd.node_id = n.id
              WHERE n.managed_by = $1
                AND COALESCE(n.state, 'registered') = 'deleted'
                AND nd.deleted_at IS NOT NULL"
    ))
    .bind(managed_by)
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests;
