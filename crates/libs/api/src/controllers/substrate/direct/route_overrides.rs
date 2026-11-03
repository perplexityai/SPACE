//! The Direct routes that put a sandbox on another node than its row's, for a
//! reader outside this module that filters on the node a sandbox response
//! reports.
//!
//! A response's `active_node_id` is a sandbox's current route node when it has
//! a route and its row's `active_node_id` otherwise (`SANDBOX_COLUMNS` in
//! `controllers/sandbox/responses.rs`). Where a route's node is the row's, the
//! row alone answers such a filter, so only the routes whose node differs from
//! the row's, including a row with no node, are read: a few, outside a route's
//! move. Finding them still reads the route table in the database; only the
//! disagreeing routes leave it.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};

/// Which disagreeing routes a filter needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverrideScope {
    /// A filter on this node: the routes to it from a row on another node or
    /// none, and the routes away from a row on it.
    Node(NodeId),
    /// A filter on whether a sandbox has a node: the routes of rows with no
    /// node. A row with a node has one either way.
    Assignment,
}

/// Sandboxes whose response reports their route's node instead of their row's,
/// within an [`OverrideScope`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RouteOverrides {
    /// Every sandbox in scope whose route's node differs from its row's
    /// `active_node_id`. Its row's node says nothing about the response.
    pub(crate) overridden: Vec<SandboxId>,
    /// Those of [`Self::overridden`] routed to the scope's node; empty for
    /// [`OverrideScope::Assignment`].
    pub(crate) on_node: Vec<SandboxId>,
}

/// The routes in `scope` whose node differs from their row's. Unordered.
pub(crate) async fn route_overrides(
    executor: impl sqlx::PgExecutor<'_>,
    scope: OverrideScope,
) -> Result<RouteOverrides, sqlx::Error> {
    let rows = match scope {
        OverrideScope::Node(node_id) => {
            sqlx::query_as::<_, (SandboxId, bool)>(sql!(
                "SELECT r.sandbox_id, TRUE
                 FROM sandbox_direct_route_identities r
                 JOIN sandboxes s ON s.id = r.sandbox_id
                 WHERE r.current_node_id = $1
                   AND s.active_node_id IS DISTINCT FROM $1
                 UNION ALL
                 SELECT r.sandbox_id, FALSE
                 FROM sandboxes s
                 JOIN sandbox_direct_route_identities r ON r.sandbox_id = s.id
                 WHERE s.active_node_id = $1
                   AND r.current_node_id <> $1"
            ))
            .bind(node_id)
            .fetch_all(executor)
            .await?
        }
        OverrideScope::Assignment => {
            sqlx::query_as::<_, (SandboxId, bool)>(sql!(
                "SELECT r.sandbox_id, FALSE
                 FROM sandbox_direct_route_identities r
                 JOIN sandboxes s ON s.id = r.sandbox_id
                 WHERE s.active_node_id IS NULL"
            ))
            .fetch_all(executor)
            .await?
        }
    };
    let mut overrides = RouteOverrides::default();
    for (sandbox_id, on_node) in rows {
        overrides.overridden.push(sandbox_id);
        if on_node {
            overrides.on_node.push(sandbox_id);
        }
    }
    Ok(overrides)
}
