use pplx_space_common_core::types::NodeId;

use super::SandboxStateInput;

/// A live row reads its node's status only when the row's active node
/// published it. A record from another node, or one beside an unplaced row,
/// leaves the stored lifecycle in charge.
#[test]
fn observation_refines_a_live_row_only_from_its_active_node() {
    let active = NodeId::new();
    let other = NodeId::new();
    for (case, active_node, observed_node, expected) in [
        ("same node", Some(active), Some(active), "paused"),
        ("different nodes", Some(active), Some(other), "started"),
        ("no active node", None, Some(other), "started"),
        ("no observed node", Some(active), None, "started"),
        ("neither node", None, None, "started"),
    ] {
        let state = SandboxStateInput::observed_on(
            "started".to_string(),
            active_node,
            Some("paused".to_string()),
            observed_node,
        );
        assert_eq!(state.rendered_lifecycle(), Some(expected), "{case}");
    }
}
