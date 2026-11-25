//! `node_direct`: a Direct node's registry row beside its `nodes` row, as the
//! admin node registry writes it: reactivated ([`reactivate`]), inserted
//! ([`register`]) or updated while live ([`update_live`]).
//!
//! Each is the second step of a registry transaction whose first wrote the
//! node's `nodes` row through `pplx-space-node-registration-store`. Each runs
//! in a transaction that already holds the node's `nodes` row lock, as
//! `pplx-space-direct-coordinator`'s transactions do; its lock order and
//! snapshot argument rely on every writer of `node_direct` doing so. At most
//! one row is live per endpoint (`idx_node_direct_endpoint_active`): a write
//! that would make a second one fails with a unique violation, and `metadata`
//! that is not a JSON object fails `node_direct_metadata_is_object`.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;

/// A `node_direct` row as a registry write returns it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct NodeDirectRow {
    pub node_id: NodeId,
    pub name: Option<String>,
    pub node_api_endpoint: String,
    pub metadata: serde_json::Value,
    pub updated_at: OffsetDateTime,
    pub deleted_at: Option<OffsetDateTime>,
}

/// What a registration writes to a node's `node_direct` row, for
/// [`reactivate`] and [`register`].
#[derive(Debug, Clone, Copy)]
pub struct NodeDirectWrite<'a> {
    /// None stores no name; a reactivation clears the stored one.
    pub name: Option<&'a str>,
    pub node_api_endpoint: &'a str,
    pub metadata: &'a serde_json::Value,
}

/// The `node_direct` columns an update replaces, for [`update_live`]. Each
/// `None` keeps the stored value.
#[derive(Debug, Clone, Copy)]
pub struct NodeDirectUpdate<'a> {
    pub name: Option<&'a str>,
    pub node_api_endpoint: Option<&'a str>,
    pub metadata: Option<&'a serde_json::Value>,
}

/// Reactivate `node_id`'s registration: write `write`, clear `deleted_at` and
/// set `updated_at` to the transaction's start time, whether or not the row
/// was deleted. `None` when the node has no `node_direct` row.
pub async fn reactivate(
    executor: impl Executor<'_>,
    node_id: NodeId,
    write: &NodeDirectWrite<'_>,
) -> Result<Option<NodeDirectRow>, DbError> {
    Ok(sqlx::query_as::<_, NodeDirectRow>(sql!(
        "UPDATE node_direct nd
         SET name = $2,
           node_api_endpoint = $3,
           metadata = $4,
           deleted_at = NULL,
           updated_at = NOW()
         WHERE nd.node_id = $1
         RETURNING nd.node_id,
                   nd.name,
                   nd.node_api_endpoint,
                   nd.metadata,
                   nd.updated_at,
                   nd.deleted_at"
    ))
    .bind(node_id)
    .bind(write.name)
    .bind(write.node_api_endpoint)
    .bind(sqlx::types::Json(write.metadata))
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Register `node_id`: insert its live `node_direct` row with `write`. The
/// node's `nodes` row must exist (`node_direct_node_id_fkey`).
pub async fn register(
    executor: impl Executor<'_>,
    node_id: NodeId,
    write: &NodeDirectWrite<'_>,
) -> Result<NodeDirectRow, DbError> {
    Ok(sqlx::query_as::<_, NodeDirectRow>(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, metadata)
         VALUES ($1, $2, $3, $4)
         RETURNING node_id,
                   name,
                   node_api_endpoint,
                   metadata,
                   updated_at,
                   deleted_at"
    ))
    .bind(node_id)
    .bind(write.name)
    .bind(write.node_api_endpoint)
    .bind(sqlx::types::Json(write.metadata))
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Update `node_id`'s registration with `update` and set `updated_at` to the
/// transaction's start time, if the row is live. `None` when the node has no
/// `node_direct` row or its row is deleted; nothing changes then.
pub async fn update_live(
    executor: impl Executor<'_>,
    node_id: NodeId,
    update: &NodeDirectUpdate<'_>,
) -> Result<Option<NodeDirectRow>, DbError> {
    Ok(sqlx::query_as::<_, NodeDirectRow>(sql!(
        "UPDATE node_direct nd
         SET name = COALESCE($2, name),
             node_api_endpoint = COALESCE($3, node_api_endpoint),
             metadata = COALESCE($4::JSONB, metadata),
             updated_at = NOW()
         WHERE nd.node_id = $1
           AND nd.deleted_at IS NULL
         RETURNING nd.node_id,
                   nd.name,
                   nd.node_api_endpoint,
                   nd.metadata,
                   nd.updated_at,
                   nd.deleted_at"
    ))
    .bind(node_id)
    .bind(update.name)
    .bind(update.node_api_endpoint)
    .bind(update.metadata.map(sqlx::types::Json))
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
