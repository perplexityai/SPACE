use pplx_space_common_core::types::NodeId;

use super::SandboxStateInput;

/// A live row reads its node's status only when the row's route node
/// (`Sandbox.active_node_id`) published it. A record from another node, or one
/// beside an unplaced row, leaves the stored lifecycle in charge.
#[test]
fn observation_refines_a_live_row_only_from_its_route_node() {
    let route = NodeId::new();
    let other = NodeId::new();
    for (case, route_node, observed_node, expected) in [
        ("same node", Some(route), Some(route), "paused"),
        ("different nodes", Some(route), Some(other), "started"),
        ("no route node", None, Some(other), "started"),
        ("no observed node", Some(route), None, "started"),
        ("neither node", None, None, "started"),
    ] {
        let state = SandboxStateInput::observed_on(
            "started".to_string(),
            route_node,
            Some("paused".to_string()),
            observed_node,
        );
        assert_eq!(state.rendered_lifecycle(), Some(expected), "{case}");
    }
}
