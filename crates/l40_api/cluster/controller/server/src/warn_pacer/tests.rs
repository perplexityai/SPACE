use pplx_space_common_core::types::NodeId;
use std::time::Duration;

use super::PerNodeWarnPacer;

const INTERVAL: Duration = Duration::from_secs(3600);
const REASON: &str = "held";

/// The window walk, on a paused clock: first observation warns, anything inside
/// the window doesn't, and the boundary is inclusive so the warn re-arms exactly
/// at `interval` rather than one tick later.
#[tokio::test(start_paused = true)]
async fn the_window_opens_once_and_re_arms_exactly_at_the_interval() {
    let pacer = PerNodeWarnPacer::new(INTERVAL);
    let node = NodeId::new();

    assert!(pacer.due(node, REASON), "the first observation must warn");
    assert!(
        !pacer.due(node, REASON),
        "an immediate second observation must be paced"
    );

    tokio::time::advance(INTERVAL - Duration::from_secs(1)).await;
    assert!(!pacer.due(node, REASON), "still inside the window");

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        pacer.due(node, REASON),
        "the warn must re-arm once the window elapses"
    );
}

/// Slots are per node. A busy node must not pace a quiet one's first warn —
/// that would hide the very first report of a second stuck node.
#[tokio::test(start_paused = true)]
async fn windows_are_independent_per_node() {
    let pacer = PerNodeWarnPacer::new(INTERVAL);
    let busy = NodeId::new();
    let quiet = NodeId::new();

    assert!(pacer.due(busy, REASON));
    assert!(!pacer.due(busy, REASON));
    assert!(
        pacer.due(quiet, REASON),
        "another node's window must not consume this one's first warn"
    );
}

/// The unbounded-growth half. A paced condition is one the node doesn't leave on
/// its own, so without pruning the map keeps a slot per node forever as the
/// fleet churns.
#[tokio::test(start_paused = true)]
async fn pruning_bounds_the_map_by_the_live_node_list() {
    let pacer = PerNodeWarnPacer::new(INTERVAL);
    let still_held = NodeId::new();
    let departed = NodeId::new();

    assert!(pacer.due(still_held, REASON));
    assert!(pacer.due(departed, REASON));
    assert_eq!(pacer.tracked(), 2);

    pacer.retain_live(&[still_held]);
    assert!(
        pacer.is_tracking(still_held),
        "a node still in the phase must keep its window, or it warns every tick"
    );
    assert!(
        !pacer.is_tracking(departed),
        "a node that left the phase must not keep a slot"
    );
    assert_eq!(pacer.tracked(), 1);

    // An empty live list is the converged fleet, not a no-op: everything goes.
    pacer.retain_live(&[]);
    assert_eq!(pacer.tracked(), 0);
}

/// Pruning a node's slot re-arms it. A node that leaves the phase and comes back
/// is a new situation and its first warn should fire, not be swallowed by a
/// window opened before it left.
#[tokio::test(start_paused = true)]
async fn a_pruned_node_warns_again_on_return() {
    let pacer = PerNodeWarnPacer::new(INTERVAL);
    let node = NodeId::new();

    assert!(pacer.due(node, REASON));
    assert!(!pacer.due(node, REASON));

    pacer.retain_live(&[]);
    assert!(
        pacer.due(node, REASON),
        "a node that left and returned must warn again rather than inherit a stale window"
    );
}

/// The reason is part of the key, so one condition's window cannot swallow
/// another's first report. Without this a node whose EC2 cross-check errors for
/// a few minutes and then resolves to "instance terminated" logs the second —
/// the one an operator acts on — below WARN.
#[tokio::test(start_paused = true)]
async fn windows_are_independent_per_reason_on_the_same_node() {
    let pacer = PerNodeWarnPacer::new(INTERVAL);
    let node = NodeId::new();

    assert!(pacer.due(node, "cross_check_failed"));
    assert!(!pacer.due(node, "cross_check_failed"));
    assert!(
        pacer.due(node, "instance_terminated"),
        "a different condition on the same node must get its own first warn"
    );

    // Pruning is still per node: both slots go when the node leaves the phase.
    assert_eq!(pacer.tracked(), 2);
    pacer.retain_live(&[]);
    assert_eq!(pacer.tracked(), 0);
}
