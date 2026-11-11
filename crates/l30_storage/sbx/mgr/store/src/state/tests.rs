use super::SandboxState::{self, *};
use super::{CleanupState, SandboxStatus, Teardown};

#[test]
fn pause_admission_retries_in_flight_lifecycle_but_rejects_invalid_stable_states() {
    for (state, expected) in [
        (Starting, tonic::Code::Unavailable),
        (Restoring, tonic::Code::Unavailable),
        (Resuming, tonic::Code::Unavailable),
        (Checkpointing, tonic::Code::Unavailable),
        (Pausing, tonic::Code::Unavailable),
        (Stopped, tonic::Code::FailedPrecondition),
        (Failed, tonic::Code::FailedPrecondition),
        (Quarantined, tonic::Code::FailedPrecondition),
    ] {
        let error = state.transition(Pausing).unwrap_err();
        assert_eq!(tonic::Status::from(error).code(), expected, "{state}");
    }
    assert_eq!(Started.transition(Pausing).unwrap(), Pausing);
}

#[test]
fn every_local_lifecycle_round_trips_through_typed_observations() {
    for state in super::SandboxState::all() {
        let wire = pplx_space_sbx_mgr_protocol::proto::SandboxLifecycleState::from(state);
        assert_eq!(wire.label(), state.as_str());
        assert_eq!(
            state.as_str().parse::<super::SandboxState>().unwrap(),
            state
        );
        assert_eq!(
            pplx_space_sbx_mgr_protocol::proto::SandboxLifecycleState::from_label(state.as_str())
                .unwrap(),
            wire
        );
    }
    "future_state".parse::<super::SandboxState>().unwrap_err();
}

// ---- Bug: Deleting must NOT transition to other states ----

#[test]
fn deleting_cannot_transition_to_started() {
    assert!(!Deleting.can_transition_to(Started));
}

#[test]
fn deleting_cannot_transition_to_creating() {
    assert!(!Deleting.can_transition_to(Creating));
}

#[test]
fn deleting_cannot_transition_to_starting() {
    assert!(!Deleting.can_transition_to(Starting));
}

#[test]
fn deleting_cannot_transition_to_stopped() {
    assert!(!Deleting.can_transition_to(Stopped));
}

#[test]
fn deleting_cannot_transition_to_paused() {
    assert!(!Deleting.can_transition_to(Paused));
}

// ---- Valid transitions TO Deleting ----

#[test]
fn created_can_transition_to_deleting() {
    assert!(Created.can_transition_to(Deleting));
}

#[test]
fn stopped_can_transition_to_deleting() {
    assert!(Stopped.can_transition_to(Deleting));
}

#[test]
fn suspended_can_transition_to_deleting() {
    assert!(Suspended.can_transition_to(Deleting));
}

#[test]
fn quarantined_can_transition_only_to_itself_or_deleting() {
    assert!(Quarantined.can_transition_to(Deleting));
    assert!(Quarantined.can_transition_to(Quarantined));
    assert!(!Quarantined.can_transition_to(Starting));
    assert!(!Quarantined.can_transition_to(Started));
    assert!(!Quarantined.can_transition_to(Failed));
}

/// A cleanup is not a lifecycle state: a row it owns admits only Delete and
/// quarantine, and required and failed are cleanup edges, not admissions.
#[test]
fn a_cleanup_owned_row_admits_only_delete_and_quarantine() {
    for owned in SandboxStatus::all()
        .into_iter()
        .filter(|status| status.is_cleanup_owned())
    {
        for target in SandboxState::all() {
            assert_eq!(
                owned.can_transition_to(target),
                matches!(target, Deleting | Quarantined),
                "{owned} -> {target}"
            );
        }
    }
}

#[test]
fn started_can_transition_to_deleting() {
    assert!(Started.can_transition_to(Deleting));
}

#[test]
fn paused_can_transition_to_deleting() {
    assert!(Paused.can_transition_to(Deleting));
}

#[test]
fn starting_can_transition_to_deleting() {
    assert!(Starting.can_transition_to(Deleting));
}

#[test]
fn stopping_can_transition_to_deleting() {
    assert!(Stopping.can_transition_to(Deleting));
}

#[test]
fn resuming_can_transition_to_deleting() {
    assert!(Resuming.can_transition_to(Deleting));
}

#[test]
fn pausing_can_transition_to_deleting() {
    assert!(Pausing.can_transition_to(Deleting));
}

#[test]
fn suspending_can_transition_to_deleting() {
    assert!(Suspending.can_transition_to(Deleting));
}

#[test]
fn restoring_can_transition_to_deleting() {
    assert!(Restoring.can_transition_to(Deleting));
}

#[test]
fn checkpointing_can_transition_to_deleting() {
    assert!(Checkpointing.can_transition_to(Deleting));
}

// ---- Checkpointing transitions ----

#[test]
fn started_can_transition_to_checkpointing() {
    assert!(Started.can_transition_to(Checkpointing));
}

#[test]
fn checkpointing_can_transition_to_started() {
    assert!(Checkpointing.can_transition_to(Started));
}

// ---- Core lifecycle transitions ----

#[test]
fn creating_to_created() {
    assert!(Creating.can_transition_to(Created));
}

#[test]
fn starting_to_started() {
    assert!(Starting.can_transition_to(Started));
}

#[test]
fn started_to_stopping() {
    assert!(Started.can_transition_to(Stopping));
}

#[test]
fn stopping_to_stopped() {
    assert!(Stopping.can_transition_to(Stopped));
}

// ---- Invalid transitions (sanity) ----

#[test]
fn stopped_cannot_transition_to_paused() {
    assert!(!Stopped.can_transition_to(Paused));
}

#[test]
fn created_cannot_transition_to_stopped() {
    assert!(!Created.can_transition_to(Stopped));
}

// Every state except `Deleting` still holds a parent-referencing instance
// subvolume (the instance subvol is removed only on full teardown, which
// is the transition into `Deleting`). This is the protected set that keeps
// GC and manual delete from dangling a live sandbox's template parent.
#[test]
fn holds_parent_referencing_instance_subvolume_covers_every_state_but_deleting() {
    for state in super::SandboxState::all() {
        assert_eq!(
            state.holds_parent_referencing_instance_subvolume(),
            state != Deleting,
            "holds_parent_referencing_instance_subvolume mismatch for {state:?}"
        );
    }
}

// ---- Guest bridge reachability ----

// The reachable set is exactly the running-or-thawing states: bridge
// RPCs must be able to land mid-wake (the exposure reconciler re-arms
// dynamic-port listeners before wake completion) but never target a
// frozen or VM-less sandbox.
#[test]
fn guest_bridge_reachable_covers_exactly_the_live_and_thawing_states() {
    let expected_reachable = [Starting, Started, Resuming, Restoring];
    for state in super::SandboxState::all() {
        assert_eq!(
            state.guest_bridge_reachable(),
            expected_reachable.contains(&state),
            "guest_bridge_reachable mismatch for {state:?}"
        );
    }
}

#[test]
fn failed_is_readable_and_only_explicit_deletion_can_exit_it() {
    assert_eq!("failed".parse::<super::SandboxState>(), Ok(Failed));
    for state in super::SandboxState::all() {
        assert!(
            !state.can_transition_to(Failed),
            "{state} must not produce Failed"
        );
        assert_eq!(
            Failed.can_transition_to(state),
            state == Deleting,
            "Failed -> {state}"
        );
    }
}

/// An event carries the lifecycle in `from_state`/`to_state` and the cleanup
/// that owns the row beside it.
#[test]
fn status_event_details_carry_the_lifecycle_beside_the_cleanup() {
    use pplx_space_events::SandboxEventDetails;

    use super::SandboxStatusEventDetails;

    for (status, cleanup_state) in [
        (SandboxStatus::from(Stopping), "none"),
        (SandboxStatus::CLEANUP_REQUIRED, "required"),
        (
            SandboxStatus::owned(Teardown::Deleting, CleanupState::Failed),
            "failed",
        ),
    ] {
        let details = SandboxEventDetails::default()
            .with_from_status(status)
            .with_to_status(status);
        let lifecycle = Some(status.lifecycle().as_str());
        assert_eq!(details.from_state.as_deref(), lifecycle);
        assert_eq!(details.to_state.as_deref(), lifecycle);
        assert_eq!(details.from_cleanup_state.as_deref(), Some(cleanup_state));
        assert_eq!(details.to_cleanup_state.as_deref(), Some(cleanup_state));
    }
}

/// A status reads as a lifecycle only while no cleanup owns the row, and the
/// readers that decide from a status answer a cleanup as they did the
/// combined status.
#[test]
fn a_cleanup_owned_status_is_no_lifecycle() {
    for status in SandboxStatus::all() {
        let Some(cleanup) = status.cleanup() else {
            assert_eq!(status, SandboxStatus::from(status.lifecycle()), "{status}");
            assert_eq!(status.unowned(), Some(status.lifecycle()), "{status}");
            continue;
        };
        assert!(!status.is_unowned(status.lifecycle()), "{status}");
        assert_eq!(status.unowned(), None, "{status}");
        assert_eq!(
            status.completes_wait(),
            cleanup == CleanupState::Failed,
            "{status}"
        );
        assert!(!status.is_transient(), "{status}");
        assert!(!status.can_authorize_net_gateway(), "{status}");
        assert!(!status.holds_runtime_outside_suspend(), "{status}");
        assert!(
            status.holds_parent_referencing_instance_subvolume(),
            "{status}"
        );
        assert!(!status.guest_bridge_reachable(), "{status}");
        assert_eq!(
            status.legacy_label(),
            format!("cleanup_{}", cleanup.as_str()),
            "{status}"
        );
    }
}
