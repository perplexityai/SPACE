//! Metric helpers for the cluster-controller's dead-node verb, which judges
//! which machines a node's rows ran on are dead, what reconciling each row
//! they held does, and in live mode does it.
//!
//! Names live in the `dead_node.*` namespace. Every series carries `mode`,
//! `dry_run` or `live`.
//!
//! Each judgment adds the node's rows again, so a sum over time counts a row
//! once per judgment; `GET /dead-node-status` has each node's last counts.

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
/// node's `verdict`. Rows selected by a dead execution stamp have an outcome
/// other than `unselected` whatever the node's verdict.
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

/// `count` rows of a judged node whose `outcome` live mode wrote.
pub fn rows_applied(mode: Mode, outcome: &str, count: u64) {
    counter(
        "dead_node.rows_applied",
        count,
        &[tag("mode", mode.as_str()), tag("outcome", outcome)],
    );
}

/// One lost node whose row live mode deleted.
pub fn node_deleted(mode: Mode) {
    counter("dead_node.nodes_deleted", 1, &[tag("mode", mode.as_str())]);
}

/// One database read or write that failed, by `stage`: `scan` (the
/// candidate-node page), `rows` (a node's sandbox rows), `write` (a row's
/// write), `delete` (the node row's delete) or `report` (its status write).
/// The node is judged again next sweep.
pub fn error(mode: Mode, stage: &str) {
    counter(
        "dead_node.errors",
        1,
        &[tag("mode", mode.as_str()), tag("stage", stage)],
    );
}
