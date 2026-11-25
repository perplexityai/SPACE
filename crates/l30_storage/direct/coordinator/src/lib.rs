//! The admin Direct node registry's transactions, which write a Direct node's
//! `nodes` row (`pplx-space-node-registration-store`) and its `node_direct`
//! row (`pplx-space-direct-store`) together.
//!
//! [`reactivate_node`] reactivates a retired node for a registration,
//! [`create_node`] registers a new node and [`update_node`] updates a
//! registered one. Each opens a transaction on the [`Db`] it is given, runs
//! the two stores' verbs, commits, and returns the node as the two rows now
//! hold it. The caller decides everything written: the node id, the values,
//! the substrate whose nodes the registry manages, and whether to stamp the
//! heartbeat. A failed step returns its error and rolls the transaction back,
//! so nothing it wrote is kept.
//!
//! # Lock order
//!
//! The `nodes` row first, then the `node_direct` row. Each transaction's
//! first write, the `UPDATE` or `INSERT`, locks the `nodes` row until the
//! transaction ends, and its `node_direct` write follows under that lock. The
//! only other production writes of `node_direct` are deletes that cascade
//! from deleting the node, which lock the `nodes` row first too. In
//! production code, every write of a node's registry rows therefore waits on
//! its `nodes` row, so none can wait on these rows in the opposite order, and
//! none changes the `node_direct` row between a transaction's two writes.
//!
//! # Snapshots and time
//!
//! Within a transaction each statement reads its own snapshot (READ
//! COMMITTED, the database's default), where one statement writing both rows
//! read one. A write that waits for a row re-checks its `WHERE` against the
//! row the other transaction left, as it does inside one statement. The
//! reactivation's candidate read and its `nodes` `UPDATE` stay one statement,
//! as they were, so the `UPDATE` acts only on the row the read found: it
//! follows that row's later updates and skips it if it was deleted, even when
//! a new row with the same id was inserted since. Once a transaction has
//! locked the `nodes` row, no production code can change the `node_direct`
//! row, so its `node_direct` write sees that row as it was when the lock was
//! taken.
//!
//! What differs from one statement is what the `node_direct` write sees of a
//! transaction that the `nodes` write waited for. One statement read
//! `node_direct` in the snapshot it took before that wait; here the later
//! statement sees what the other transaction committed. Only
//! [`update_node`]'s write depends on it. When a reactivation of the node
//! commits while the update waits for its `nodes` row, the update finds the
//! registration live and writes it, where one statement found it deleted and
//! returned no row. If that write fails, as it does at an endpoint another
//! live registration holds, the transaction rolls back. Either result is what
//! one statement did when the reactivation committed just before the update
//! started, an order the registry already allows. The orders are
//! interchangeable: the reactivation committed before this transaction wrote
//! anything, since its first write waited for it, so neither saw the other's
//! uncommitted writes, and both versions wait for a locked row and skip an
//! uncommitted one alike.
//!
//! `NOW()` is the transaction's start time in every statement, so the
//! heartbeat, `node_direct.updated_at` and a new node's `created_at` that one
//! transaction writes are one instant.
//!
//! That instant can be earlier than a row the transaction writes. When
//! another transaction commits a node after this one started but before its
//! first write, as a create of the same id can for an [`update_node`], this
//! transaction still writes that node. Its `node_direct.updated_at`, and a
//! heartbeat it stamps, are then earlier than the node's `created_at`, by
//! less than the time between this transaction's start and its first write.
//! One statement could not return that: it either started after the create
//! committed, so its `NOW()` was later, or did not see the node. The
//! registry accepts this. `node_direct.updated_at` is reported in the admin
//! node API. Below the requested id, it also decides which retired
//! registration [`reactivate_node`]'s endpoint match picks: the one with the
//! latest stamp. A stamp that much earlier changes that choice only against
//! another retired registration of the same endpoint stamped in that interval.

use pplx_space_accounts::NodeId;
use pplx_space_direct_store::nodes::{
    self as node_direct, NodeDirectRow, NodeDirectUpdate, NodeDirectWrite,
};
use pplx_space_node_model::{NodeManagedBy, NodeRole, SubstrateKind};
use pplx_space_node_registration_store::direct::registry::{
    self, RegisteredNodeRow, RegistryNodeUpdate, RegistryNodeWrite,
};
use pplx_space_postgres_pool::{Db, DbError, Tx};
use time::OffsetDateTime;

/// A Direct node as its `nodes` and `node_direct` rows hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectNode {
    pub id: NodeId,
    pub name: Option<String>,
    /// `host(address)`: the address without its prefix length.
    pub address: Option<String>,
    pub node_api_endpoint: String,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub metadata: serde_json::Value,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    /// `node_direct.updated_at`.
    pub updated_at: OffsetDateTime,
    /// `node_direct.deleted_at`.
    pub deleted_at: Option<OffsetDateTime>,
}

impl DirectNode {
    fn from_rows(node: RegisteredNodeRow, direct: NodeDirectRow) -> Self {
        Self {
            id: node.id,
            name: direct.name,
            address: node.address,
            node_api_endpoint: direct.node_api_endpoint,
            role: node.role,
            managed_by: node.managed_by,
            state: node.state,
            metadata: direct.metadata,
            last_heartbeat: node.last_heartbeat,
            created_at: node.created_at,
            updated_at: direct.updated_at,
            deleted_at: direct.deleted_at,
        }
    }
}

/// A registration of a Direct node, for [`reactivate_node`] and
/// [`create_node`].
#[derive(Debug, Clone, Copy)]
pub struct DirectNodeRegistration<'a> {
    pub node_id: NodeId,
    /// None stores no name; a reactivation clears the stored one.
    pub name: Option<&'a str>,
    /// `None` or an empty string stores no address.
    pub address: Option<&'a str>,
    pub node_api_endpoint: &'a str,
    pub role: NodeRole,
    /// The substrate whose nodes the registry manages: a reactivation
    /// considers only its nodes, and the node is recorded as its.
    pub managed_by: SubstrateKind,
    /// `None` stores `registered`.
    pub state: Option<&'a str>,
    /// A JSON object.
    pub metadata: &'a serde_json::Value,
    /// Set `last_heartbeat` to the transaction's start time. Otherwise a
    /// reactivated node keeps its heartbeat and a new one has none.
    pub stamp_heartbeat: bool,
}

impl DirectNodeRegistration<'_> {
    fn node_write(&self) -> RegistryNodeWrite<'_> {
        RegistryNodeWrite {
            address: self.address,
            role: self.role,
            managed_by: self.managed_by,
            state: self.state,
            stamp_heartbeat: self.stamp_heartbeat,
        }
    }

    fn direct_write(&self) -> NodeDirectWrite<'_> {
        NodeDirectWrite {
            name: self.name,
            node_api_endpoint: self.node_api_endpoint,
            metadata: self.metadata,
        }
    }
}

/// An update of a registered Direct node, for [`update_node`]. Each `None`
/// keeps the stored value.
#[derive(Debug, Clone, Copy)]
pub struct DirectNodeUpdate<'a> {
    pub node_id: NodeId,
    /// The substrate whose nodes the registry manages; a node another one
    /// manages is not updated.
    pub managed_by: SubstrateKind,
    pub name: Option<&'a str>,
    /// `None` or an empty string keeps the address.
    pub address: Option<&'a str>,
    pub node_api_endpoint: Option<&'a str>,
    pub role: Option<NodeRole>,
    pub state: Option<&'a str>,
    /// A JSON object.
    pub metadata: Option<&'a serde_json::Value>,
    /// Set `last_heartbeat` to the transaction's start time; otherwise keep
    /// it.
    pub stamp_heartbeat: bool,
}

/// Reactivate the retired node `registration` matches and write
/// `registration` to it.
///
/// The node is one `registration.managed_by` manages whose `node_direct` row
/// is deleted, with id `registration.node_id` or, when `match_endpoint`,
/// `registration.node_api_endpoint`; see
/// [`registry::reactivate_retired_node`] for which one wins. The reactivated
/// node keeps its id, which may not be `registration.node_id`.
///
/// `None` when no retired node matches, or when the matched node's `nodes`
/// row is deleted before it is written; nothing is written then. Also `None`
/// when its `node_direct` row is gone by the time it is written, which only a
/// direct `DELETE FROM node_direct` can cause (no production path runs one);
/// the `nodes` row's reactivation commits then.
pub async fn reactivate_node(
    db: &Db,
    registration: &DirectNodeRegistration<'_>,
    match_endpoint: bool,
) -> Result<Option<DirectNode>, DbError> {
    let mut tx = db.begin().await?;
    let node = reactivate_in(&mut tx, registration, match_endpoint).await?;
    tx.commit().await?;
    Ok(node)
}

async fn reactivate_in(
    tx: &mut Tx<'_>,
    registration: &DirectNodeRegistration<'_>,
    match_endpoint: bool,
) -> Result<Option<DirectNode>, DbError> {
    let Some(node) = registry::reactivate_retired_node(
        &mut *tx,
        registration.node_id,
        registration.node_api_endpoint,
        match_endpoint,
        &registration.node_write(),
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(direct) =
        node_direct::reactivate(&mut *tx, node.id, &registration.direct_write()).await?
    else {
        return Ok(None);
    };
    Ok(Some(DirectNode::from_rows(node, direct)))
}

/// Register a new node with `registration`. A node with its id, or a live
/// registration at its endpoint, fails with a unique violation, and nothing
/// is written.
pub async fn create_node(
    db: &Db,
    registration: &DirectNodeRegistration<'_>,
) -> Result<DirectNode, DbError> {
    let mut tx = db.begin().await?;
    let node =
        registry::register_node(&mut tx, registration.node_id, &registration.node_write()).await?;
    let direct = node_direct::register(&mut tx, node.id, &registration.direct_write()).await?;
    tx.commit().await?;
    Ok(DirectNode::from_rows(node, direct))
}

/// Update the registered node `update.node_id` with `update`.
///
/// `None` when no node with that id exists that `update.managed_by` manages;
/// nothing is written then. Also `None` when the node has no live
/// `node_direct` row: the `nodes` row's update commits, and `node_direct` is
/// left as it is.
pub async fn update_node(
    db: &Db,
    update: &DirectNodeUpdate<'_>,
) -> Result<Option<DirectNode>, DbError> {
    let mut tx = db.begin().await?;
    let node = update_in(&mut tx, update).await?;
    tx.commit().await?;
    Ok(node)
}

async fn update_in(
    tx: &mut Tx<'_>,
    update: &DirectNodeUpdate<'_>,
) -> Result<Option<DirectNode>, DbError> {
    let node_update = RegistryNodeUpdate {
        address: update.address,
        role: update.role,
        state: update.state,
        stamp_heartbeat: update.stamp_heartbeat,
    };
    let Some(node) =
        registry::update_registered_node(&mut *tx, update.node_id, update.managed_by, &node_update)
            .await?
    else {
        return Ok(None);
    };
    let direct_update = NodeDirectUpdate {
        name: update.name,
        node_api_endpoint: update.node_api_endpoint,
        metadata: update.metadata,
    };
    let Some(direct) = node_direct::update_live(&mut *tx, node.id, &direct_update).await? else {
        return Ok(None);
    };
    Ok(Some(DirectNode::from_rows(node, direct)))
}

#[cfg(test)]
mod tests;
