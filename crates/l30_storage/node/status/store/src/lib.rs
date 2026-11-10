//! The `node_status_snapshots` row: what each node last reported about
//! itself, one row per node.
//!
//! Its writers predate this store and haven't moved in yet: the internal
//! API's node status handler and the Direct substrate's own snapshot. What
//! lives here today is [`environment_evidence`], the rule for whether a
//! node's published environments can be acted on now. The runtime upgrade
//! campaigns' lease is its first caller. A reader that only records what a
//! node's build knows, such as the catalog's edge aggregate, reads the
//! arrays without it.

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// A boolean SQL predicate, TRUE iff the node status snapshot aliased as
/// `snapshot_alias` counts as evidence of the node's published environments
/// now (`usable_environment_refs` and `known_environment_refs`):
///
/// * its latest execution inventory report is `complete`;
/// * the snapshot is fresh: `stale_after` hasn't passed;
/// * the environment arrays were received within the node's freshness
///   window, `stale_after - observed_at`. The arrays have their own receipt
///   time because older writers refresh the inventory columns without them,
///   so arrays such a writer leaves behind age out.
///
/// A NULL in any of them (no inventory report, no arrays) is FALSE, never
/// NULL, so the predicate can be negated safely. It says nothing about
/// whether the node is reachable: a caller choosing a node to act on also
/// requires `reachable IS TRUE`.
///
/// Every value is the row's own or the database clock's, so a caller builds
/// its statement once (a `LazyLock<String>`) and binds the rest.
pub fn environment_evidence(snapshot_alias: &str) -> String {
    format!(
        "COALESCE(\n\
             {snapshot_alias}.execution_inventory_status = 'complete'\n\
             AND {snapshot_alias}.stale_after > now()\n\
             AND {snapshot_alias}.environment_refs_received_at\n\
                 + ({snapshot_alias}.stale_after - {snapshot_alias}.observed_at) > now(),\n\
             FALSE\n\
         )"
    )
}
