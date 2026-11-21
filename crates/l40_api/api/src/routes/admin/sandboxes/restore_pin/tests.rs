use pplx_space_sandbox_store::restore_pin::{PinnableCapture, RestorePinState};
use pplx_space_snapshot_model::LocalArtifactId;
use time::OffsetDateTime;

use super::{RestorePinRequest, node_copy_conflict, pinned_response, stale_pin_request};
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

/// A suspended, unpinned sandbox offering one capture, read at `read_at`.
fn pinnable_state(read_at: OffsetDateTime, artifact: LocalArtifactId) -> RestorePinState {
    RestorePinState {
        lifecycle: "suspended".to_owned(),
        lifecycle_changed_at: Some(read_at),
        pinned_artifact: None,
        restore_node_id: None,
        unordered: true,
        captures: vec![PinnableCapture {
            local_artifact_id: artifact,
            generation: Some(5),
            first_registered_at: read_at,
            source_node_id: None,
        }],
    }
}

/// The read refuses a pin the guarded write would, before the node is
/// asked: a stale token, a sandbox that isn't suspended, one already pinned,
/// or an artifact the read didn't list. A request that matches the read
/// passes.
#[test]
fn a_pin_the_read_state_refuses_is_a_conflict_before_the_node_is_asked() {
    let read_at = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
    let artifact = LocalArtifactId::new();
    let state = pinnable_state(read_at, artifact);
    let request = RestorePinRequest {
        local_artifact_id: artifact,
        lifecycle_changed_at: read_at,
    };
    assert_eq!(stale_pin_request(&state, &request), None);
    let refused = [
        (
            "a stale token",
            state.clone(),
            RestorePinRequest {
                lifecycle_changed_at: read_at + time::Duration::seconds(1),
                ..request
            },
        ),
        (
            "no token",
            RestorePinState {
                lifecycle_changed_at: None,
                ..state.clone()
            },
            request,
        ),
        (
            "not suspended",
            RestorePinState {
                lifecycle: "restoring".to_owned(),
                ..state.clone()
            },
            request,
        ),
        (
            "already pinned",
            RestorePinState {
                pinned_artifact: Some(LocalArtifactId::new()),
                ..state.clone()
            },
            request,
        ),
        (
            "an unlisted artifact",
            state.clone(),
            RestorePinRequest {
                local_artifact_id: LocalArtifactId::new(),
                ..request
            },
        ),
    ];
    for (case, state, request) in refused {
        assert!(
            stale_pin_request(&state, &request).is_some(),
            "{case} must be refused"
        );
    }
}

/// A landed pin answers with the state it was checked against, pinned: the
/// token the operator sent, unchanged, and the requested artifact.
#[test]
fn a_landed_pin_answers_with_the_state_it_was_checked_against() {
    let read_at = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
    let artifact = LocalArtifactId::new();
    let response = pinned_response(pinnable_state(read_at, artifact), artifact);
    assert_eq!(response.lifecycle, "suspended");
    assert_eq!(response.lifecycle_changed_at, Some(read_at));
    assert_eq!(response.pinned_artifact_id, Some(artifact));
    assert!(response.unordered);
    assert_eq!(response.captures.len(), 1);
    assert_eq!(response.captures[0].local_artifact_id, artifact);
}
