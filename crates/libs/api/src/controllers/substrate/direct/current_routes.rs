//! The Direct route's current node for every routed sandbox, for a reader
//! outside this module that filters on the node a sandbox response reports.
//!
//! A response's `active_node_id` is a sandbox's current route node when it has
//! a route and its row's `active_node_id` otherwise (`SANDBOX_COLUMNS` in
//! `controllers/sandbox/responses.rs`), so a filter on that node needs the
//! routed sandboxes as well as the ones routed to the node.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};

/// The sandboxes with a current Direct route.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct CurrentRoutes {
    /// Every sandbox with a current route. Its response reports the route's
    /// node, whatever its row's `active_node_id` holds.
    pub(crate) routed: Vec<SandboxId>,
    /// Those of [`Self::routed`] whose route's node is the one asked for;
    /// empty when none was.
    pub(crate) on_node: Vec<SandboxId>,
}

/// Every sandbox with a current route and, when `node_id` is given, those
/// routed to it. Unordered.
pub(crate) async fn current_routes(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: Option<NodeId>,
) -> Result<CurrentRoutes, sqlx::Error> {
    let rows = sqlx::query_as::<_, (SandboxId, bool)>(sql!(
        "SELECT sandbox_id, (current_node_id = $1::UUID) IS TRUE
         FROM sandbox_direct_route_identities"
    ))
    .bind(node_id)
    .fetch_all(executor)
    .await?;
    let mut routes = CurrentRoutes::default();
    for (sandbox_id, on_node) in rows {
        routes.routed.push(sandbox_id);
        if on_node {
            routes.on_node.push(sandbox_id);
        }
    }
    Ok(routes)
}
