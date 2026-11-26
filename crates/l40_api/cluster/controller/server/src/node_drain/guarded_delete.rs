//! The guarded delete of a gone node's row, with no tombstone.
//!
//! The caller locks the node row `FOR UPDATE` ([`lock_node`]) and checks,
//! under that lock, that the node is still the one it decided on (its own
//! ownership: a drain phase and fence, or none). [`delete_locked_node`] then
//! deletes the row only while its Kubernetes identity is the one the caller
//! read, its heartbeat is stale, and nothing names it
//! ([`pplx_space_sandbox_residency::node_unreferenced`]). The
//! exclusive lock holds a registration of a replacement off until the
//! caller commits, or waits for one in flight and then reads what it
//! committed; a moved uid or instance refuses.
//!
//! The statements are `pplx_space_node_registration_store`'s: the locks in
//! `locks`, and the heartbeat read and the delete in `guarded_delete`; the
//! reference read is `pplx_space_sandbox_store::drain_terminate::node_references`. The
//! order of the checks and what each outcome means are decided here.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_node_registration_store::{guarded_delete, locks};
use pplx_space_sandbox_store::drain_terminate::{NodeReferences, node_references};
use sqlx::{Postgres, Transaction};

/// Lock the node row `FOR UPDATE`. Returns whether it exists. The lock lasts
/// until the caller's transaction ends.
pub(crate) use locks::lock_node_for_update as lock_node;

/// How many sandbox ids of each guard a refused delete names.
pub(crate) const REFERENCE_SAMPLE: u32 = 10;

/// The node a guarded delete removes, as its caller read it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NodeIdentity<'a> {
    pub node_id: NodeId,
    pub managed_by: SubstrateKind,
    /// `node_k8s.k8s_node_name`; `None` without a `node_k8s` row.
    pub k8s_node_name: Option<&'a str>,
    /// `node_k8s.k8s_node_uid` as the caller read it.
    pub k8s_node_uid: Option<&'a str>,
    /// `node_k8s.instance_id` as the caller read it.
    pub instance_id: Option<&'a str>,
}

/// What [`delete_locked_node`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GuardedDelete {
    /// The row is gone, with its `node_k8s` row and no tombstone.
    Deleted,
    /// The node's uid or recorded instance is not the one the caller read,
    /// or its row is gone: a registration moved it. Nothing was written.
    IdentityMoved,
    /// The node heartbeats again. Nothing was written.
    HeartbeatFresh,
    /// Rows or executions still name the node. Nothing was written.
    Referenced(NodeReferences),
}

/// Delete the node row the caller locked with [`lock_node`], guarded as the
/// module says. The caller commits on [`GuardedDelete::Deleted`]; nothing
/// else wrote anything.
pub(crate) async fn delete_locked_node(
    tx: &mut Transaction<'_, Postgres>,
    node: &NodeIdentity<'_>,
) -> Result<GuardedDelete, sqlx::Error> {
    let identity = locks::share_lock_k8s_identity(tx, node.node_id).await?;
    let (uid, instance_id) = identity.unwrap_or_default();
    if uid.as_deref() != node.k8s_node_uid || instance_id.as_deref() != node.instance_id {
        return Ok(GuardedDelete::IdentityMoved);
    }
    if guarded_delete::heartbeat_fresh(tx, node.node_id).await? {
        return Ok(GuardedDelete::HeartbeatFresh);
    }
    let references = node_references(
        &mut **tx,
        node.node_id,
        node.k8s_node_name,
        REFERENCE_SAMPLE,
    )
    .await?;
    if !references.is_empty() {
        return Ok(GuardedDelete::Referenced(references));
    }
    if guarded_delete::delete_unreferenced_stale_node(
        tx,
        node.node_id,
        node.managed_by,
        node.k8s_node_name,
    )
    .await?
    {
        return Ok(GuardedDelete::Deleted);
    }
    // The statement's own snapshot saw a reference, or the row went: read
    // what names it now.
    let references = node_references(
        &mut **tx,
        node.node_id,
        node.k8s_node_name,
        REFERENCE_SAMPLE,
    )
    .await?;
    Ok(if references.is_empty() {
        GuardedDelete::IdentityMoved
    } else {
        GuardedDelete::Referenced(references)
    })
}

#[cfg(all(test, feature = "kubernetes"))]
mod tests;
