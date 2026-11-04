//! Whether a sandbox response reports a node, as whole boolean SQL predicates
//! for a statement outside this module that filters `sandboxes` on it.
//!
//! A response's `active_node_id` is a sandbox's current route node when it has
//! a route and its row's `active_node_id` otherwise (`SANDBOX_COLUMNS` in
//! `controllers/sandbox/responses.rs`). These predicates answer it for one
//! sandbox row inside the statement that tests the row, so no caller reads
//! routes first and binds them back.
//!
//! The route table does not shrink to the routes in flight: writing off a
//! sandbox from a gone node, reverting a restore and rolling back an
//! activation clear the row's `active_node_id` and leave its route, which
//! only goes with `clear_direct_route_identity` or the row. A read of every
//! route that disagrees with its row grows with those sandboxes.

/// TRUE iff the response reports the sandbox aliased `sandbox_alias` on
/// `node_id`, a UUID expression that must not be NULL; FALSE otherwise, never
/// NULL.
///
/// The first conjunct only narrows: the sandboxes whose row or route names
/// the node, which the planner finds through `idx_sandboxes_active_node` and
/// the primary key instead of testing every row in a list's order, so a node
/// with few sandboxes costs few. The route side reads the routes on the node
/// once per statement; no index covers `current_node_id`, so that read scans
/// the route table, inside the database. The second conjunct is the exact
/// test. The first is NULL for a sandbox with no route and no
/// `active_node_id`; the second is then FALSE rather than NULL, so the
/// predicate is too.
pub(crate) fn reported_on_node(sandbox_alias: &str, node_id: &str) -> String {
    format!(
        "(\n\
             ({sandbox_alias}.active_node_id = {node_id}\n\
              OR {sandbox_alias}.id = ANY(ARRAY(\n\
                  SELECT route.sandbox_id\n\
                  FROM sandbox_direct_route_identities route\n\
                  WHERE route.current_node_id = {node_id})))\n\
             AND COALESCE(\n\
                 (SELECT route.current_node_id\n\
                  FROM sandbox_direct_route_identities route\n\
                  WHERE route.sandbox_id = {sandbox_alias}.id),\n\
                 {sandbox_alias}.active_node_id\n\
             ) IS NOT DISTINCT FROM {node_id}\n\
         )"
    )
}

/// TRUE iff the response reports the sandbox aliased `sandbox_alias` on some
/// node, FALSE otherwise; never NULL.
pub(crate) fn reported_on_a_node(sandbox_alias: &str) -> String {
    format!(
        "(\n\
             {sandbox_alias}.active_node_id IS NOT NULL\n\
             OR EXISTS (\n\
                 SELECT 1\n\
                 FROM sandbox_direct_route_identities route\n\
                 WHERE route.sandbox_id = {sandbox_alias}.id\n\
             )\n\
         )"
    )
}
