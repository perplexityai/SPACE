//! Metric helpers for the cluster-controller's dead-node verb, which judges
//! whether a node with a stale heartbeat is lost and what reconciling each
//! sandbox row it holds would do.
//!
//! Names live in the `dead_node.*` namespace. Every series carries `mode`;
//! the verb has only a dry run so far, so it is always `dry_run`, and a
//! dashboard filtering on it keeps meaning the same thing once a write mode
//! exists.
//!
//! Each judgment adds the node's rows again, so a sum over time counts a row
//! once per judgment; `GET /dead-node-dry-runs` has each node's last counts.

use pplx_space_metrics_emit::{counter, tag};

use super::cluster_reconciler::Mode;

/// One node judged in one tick: its `verdict` (`lost` or `unknown`) and the
/// `reason` the verdict names, the first condition that failed for
/// `unknown`.
pub fn node_judged(mode: Mode, verdict: &str, reason: &str) {
    counter(
        "dead_node.nodes",
        1,
        &[
            tag("mode", mode.as_str()),
            tag("verdict", verdict),
            tag("reason", reason),
        ],
    );
}

/// `count` sandbox rows of a judged node with `outcome`, tagged with the
/// node's `verdict`: only rows of a `lost` node would be reconciled.
pub fn rows(mode: Mode, verdict: &str, outcome: &str, count: u64) {
    counter(
        "dead_node.rows",
        count,
        &[
            tag("mode", mode.as_str()),
            tag("verdict", verdict),
            tag("outcome", outcome),
        ],
    );
}

/// One database read or report write that failed, by `stage`: `scan` (the
/// stale-node page), `rows` (a node's sandbox rows) or `report` (its report
/// write). The node is judged again next sweep.
pub fn error(mode: Mode, stage: &str) {
    counter(
        "dead_node.errors",
        1,
        &[tag("mode", mode.as_str()), tag("stage", stage)],
    );
}
