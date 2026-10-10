use super::SandboxState::*;

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
        let wire = crate::proto::SandboxLifecycleState::from(state);
        assert_eq!(wire.label(), state.as_str());
        assert_eq!(
            state.as_str().parse::<super::SandboxState>().unwrap(),
            state
        );
        assert_eq!(
            crate::proto::SandboxLifecycleState::from_label(state.as_str()).unwrap(),
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

#[test]
fn cleanup_required_can_transition_to_cleanup_failed() {
    assert!(CleanupRequired.can_transition_to(CleanupFailed));
}

#[test]
fn cleanup_failed_can_transition_to_deleting_or_retry() {
    assert!(CleanupFailed.can_transition_to(Deleting));
    assert!(CleanupFailed.can_transition_to(CleanupRequired));
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
