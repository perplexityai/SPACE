use pplx_space_common_core::types::LocalArtifactId;

use super::node_copy_conflict;
use crate::controllers::substrate::NodeReportedArtifact;

/// A pin is admitted when the node holds no copy of the sandbox, or holds the
/// pinned capture suspended. It's refused while the node holds another
/// capture, a copy with no recorded capture or one it can't attribute, or the
/// running sandbox: a restore on that node would load its own copy, not the
/// pin.
#[test]
fn a_pin_is_refused_while_the_node_holds_another_copy() {
    let requested = LocalArtifactId::new();
    let held = NodeReportedArtifact {
        present: true,
        reports_suspended: true,
        pinned_artifact: Some(requested),
        ..NodeReportedArtifact::default()
    };
    let other = NodeReportedArtifact {
        pinned_artifact: Some(LocalArtifactId::new()),
        ..held.clone()
    };
    let admitted = [
        ("no node", None),
        ("no copy", Some(NodeReportedArtifact::default())),
        ("the pinned copy", Some(held.clone())),
    ];
    for (case, report) in admitted {
        assert_eq!(
            node_copy_conflict(report.as_ref(), requested),
            None,
            "{case}"
        );
    }
    let refused = [
        ("another capture", other),
        (
            "a copy without a pin",
            NodeReportedArtifact {
                pinned_artifact: None,
                ..held.clone()
            },
        ),
        (
            "unreadable pin",
            NodeReportedArtifact {
                pin_unreadable: true,
                pinned_artifact: None,
                ..held.clone()
            },
        ),
        (
            "degraded",
            NodeReportedArtifact {
                state_degraded: true,
                ..held.clone()
            },
        ),
        (
            "running",
            NodeReportedArtifact {
                vm_alive: true,
                ..held.clone()
            },
        ),
        (
            "not suspended",
            NodeReportedArtifact {
                reports_suspended: false,
                ..held.clone()
            },
        ),
    ];
    for (case, report) in refused {
        assert!(
            node_copy_conflict(Some(&report), requested).is_some(),
            "{case}"
        );
    }
}
