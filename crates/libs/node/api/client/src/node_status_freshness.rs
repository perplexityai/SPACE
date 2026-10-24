//! How long the cluster trusts a node's status publication, and how that ties
//! to node-ctl's poll interval.
//!
//! node-ctl publishes node status on every reconcile tick, and between ticks
//! only when a facet is marked dirty. Every publication carries node-ctl's
//! reconciliation statuses, including the interval it polls at. A quiet node
//! (cordoned, or simply idle) therefore publishes about once per interval, so a
//! window shorter than the interval makes a healthy node read stale for part
//! of every tick. The cluster derives the window from the interval the node
//! reports, rather than restating it as a literal, so the two can't drift
//! apart.
//!
//! The status publish runs near the *end* of a tick: after the inbound facets
//! and the cluster reconcile claims, and before the orphan-inventory
//! observation. So the gap between two publications routinely reaches a full
//! interval and goes past it when a tick runs long. Two intervals plus 30 s
//! cover a lost publication that the next tick replaces, or a tick whose work
//! finishes up to an interval plus 30 s later than the previous one's. Nothing
//! bounds a tick's total runtime: outbound jobs time out after 90 s, but
//! inbound facets stop only after 90 s without progress, and neither the
//! cluster reconcile claims nor the orphan-inventory observation has a limit.
//! A tick that runs longer than that margin leaves the node reading stale
//! until its publication lands.

use std::time::Duration;

use crate::proto::NodeCtlStatus;

/// node-ctl's reconcile tick when neither the CLI nor its config file sets one.
pub const DEFAULT_NODE_CTL_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Name of node-ctl's node-status outbound reconciler in
/// [`NodeCtlStatus::reconciliations`]. Its `poll_interval_secs` is the cadence
/// of the node's status publications.
pub const NODE_STATUS_OUTBOUND_RECONCILER: &str = "node-status-outbound";

/// Whole poll intervals a node may go without a status publication before it
/// reads stale.
const MISSED_TICKS_TOLERATED: u32 = 2;

/// Margin on top of `MISSED_TICKS_TOLERATED` intervals for publish latency and
/// tick overrun.
const STATUS_PUBLISH_SLACK: Duration = Duration::from_secs(30);

/// Upper bound on the freshness window, whatever interval a node reports.
/// A node that has genuinely stopped publishing reads stale within this long
/// of its last publication.
const MAX_NODE_STATUS_FRESHNESS: Duration = Duration::from_secs(600);

/// The longest poll interval `MAX_NODE_STATUS_FRESHNESS` gives the full
/// tolerance, 285 s. node-ctl clamps a longer configured interval to this,
/// with a warning.
pub const MAX_NODE_CTL_POLL_INTERVAL: Duration = Duration::from_secs(
    (MAX_NODE_STATUS_FRESHNESS.as_secs() - STATUS_PUBLISH_SLACK.as_secs())
        / MISSED_TICKS_TOLERATED as u64,
);

/// Freshness window for a node publishing every `poll_interval`: two
/// intervals plus 30 s, capped at 10 minutes.
pub const fn node_status_freshness(poll_interval: Duration) -> Duration {
    let window = poll_interval
        .saturating_mul(MISSED_TICKS_TOLERATED)
        .saturating_add(STATUS_PUBLISH_SLACK);
    if window.as_nanos() > MAX_NODE_STATUS_FRESHNESS.as_nanos() {
        MAX_NODE_STATUS_FRESHNESS
    } else {
        window
    }
}

/// Freshness window for [`DEFAULT_NODE_CTL_POLL_INTERVAL`].
pub const DEFAULT_NODE_STATUS_FRESHNESS: Duration =
    node_status_freshness(DEFAULT_NODE_CTL_POLL_INTERVAL);

/// The node-status poll interval a node reported, or `None` when the report
/// doesn't carry a usable one: no node-status reconciler entry, as when
/// node-ctl couldn't read its own reconciliation status, or a zero interval.
pub fn reported_status_poll_interval(node_ctl: &NodeCtlStatus) -> Option<Duration> {
    node_ctl
        .reconciliations
        .iter()
        .find(|status| status.name == NODE_STATUS_OUTBOUND_RECONCILER)
        .map(|status| status.poll_interval_secs)
        .filter(|&secs| secs > 0)
        .map(Duration::from_secs)
}

/// Freshness window for one publication, derived from the interval the node
/// reported. `None` when it reported no usable interval: node-ctl attaches its
/// status to every publication, so that happens when node-ctl couldn't read
/// its own reconciliation status, or for a publisher that sends none.
pub fn reported_freshness(node_ctl: Option<&NodeCtlStatus>) -> Option<Duration> {
    node_ctl
        .and_then(reported_status_poll_interval)
        .map(node_status_freshness)
}

#[cfg(test)]
#[path = "node_status_freshness_tests.rs"]
mod tests;
