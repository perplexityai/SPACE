use super::{
    NodeDrainState, SandboxAction, post_wake_lifecycle_allows_started_update,
    wake_lifecycle_allows_started_update,
};
use pplx_space_common_core::types::{NodeDrainMode, NodeDrainPhase, SandboxLifecycle};

#[test]
fn placement_affecting_actions_match_design() {
    for action in [
        SandboxAction::Start,
        SandboxAction::Wake,
        SandboxAction::Resume,
        SandboxAction::Restore,
    ] {
        assert!(
            action.affects_lifecycle(),
            "expected {action} to be placement-affecting",
        );
    }
}

#[test]
fn cleanup_and_snapshot_actions_are_not_blocked() {
    for action in [
        SandboxAction::Stop,
        SandboxAction::Kill,
        SandboxAction::Pause,
        SandboxAction::Suspend,
        SandboxAction::Checkpoint,
        SandboxAction::EnableSnapshots,
        SandboxAction::DisableSnapshots,
    ] {
        assert!(
            !action.affects_lifecycle(),
            "expected {action} to be allowed through during drain",
        );
    }
}

#[test]
fn lifecycle_action_activation_class_marks_only_resource_adding_actions() {
    use crate::cluster_admission::ClusterActivationClass;

    assert_eq!(
        SandboxAction::Start.activation_class(None),
        Some(ClusterActivationClass::Cold),
    );
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Created)),
        Some(ClusterActivationClass::Cold),
    );
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Stopped)),
        Some(ClusterActivationClass::Cold),
    );
    // `start` on a suspended sandbox is a restore activation (snapshot load).
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Suspended)),
        Some(ClusterActivationClass::Restore),
    );
    // A start that joins an in-flight restore adds no new slot.
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Restoring)),
        None,
    );
    // Retry/no-op start against a running or terminal sandbox must not charge.
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Started)),
        None,
    );
    assert_eq!(
        SandboxAction::Start.activation_class(Some(SandboxLifecycle::Failed)),
        None,
    );
    assert_eq!(
        SandboxAction::Restore.activation_class(None),
        Some(ClusterActivationClass::Restore),
    );
    assert_eq!(
        SandboxAction::Restore.activation_class(Some(SandboxLifecycle::Suspended)),
        Some(ClusterActivationClass::Restore),
    );
    // Redundant resume/restore against an already-active sandbox must not
    // charge a shared activation token.
    assert_eq!(
        SandboxAction::Restore.activation_class(Some(SandboxLifecycle::Started)),
        None,
    );
    assert_eq!(
        SandboxAction::Restore.activation_class(Some(SandboxLifecycle::Restoring)),
        None,
    );
    assert_eq!(
        SandboxAction::Wake.activation_class(Some(SandboxLifecycle::Created)),
        Some(ClusterActivationClass::Cold),
    );
    assert_eq!(
        SandboxAction::Wake.activation_class(Some(SandboxLifecycle::Stopped)),
        Some(ClusterActivationClass::Cold),
    );
    assert_eq!(
        SandboxAction::Wake.activation_class(Some(SandboxLifecycle::Suspended)),
        Some(ClusterActivationClass::Restore),
    );
    assert_eq!(
        SandboxAction::Wake.activation_class(Some(SandboxLifecycle::Started)),
        None,
    );
    assert_eq!(
        SandboxAction::Wake.activation_class(Some(SandboxLifecycle::Paused)),
        None,
    );
    assert!(SandboxAction::Wake.activation_class(None).is_none());
    assert_eq!(
        SandboxAction::Resume.activation_class(Some(SandboxLifecycle::Paused)),
        None,
    );

    // The implicit access / control-wake charge (`implicit_activation_class`)
    // is a one-line delegation to `Wake.activation_class`, so cover every
    // remaining lifecycle here: only the dormant slot-reoccupying states
    // charge; `paused` (resident, cheap resume) and every live / in-flight /
    // terminal state is a no-op reconnect.
    for lifecycle in [
        SandboxLifecycle::Creating,
        SandboxLifecycle::Starting,
        SandboxLifecycle::Restoring,
        SandboxLifecycle::Pausing,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Resuming,
        SandboxLifecycle::Suspending,
        SandboxLifecycle::Stopping,
        SandboxLifecycle::Deleting,
        SandboxLifecycle::Checkpointing,
        SandboxLifecycle::Failed,
    ] {
        assert_eq!(
            SandboxAction::Wake.activation_class(Some(lifecycle)),
            None,
            "expected implicit wake from {lifecycle} to be a no-op activation class",
        );
    }
}

#[test]
fn from_str_round_trips_for_known_actions() {
    for action in [
        SandboxAction::Start,
        SandboxAction::Wake,
        SandboxAction::Resume,
        SandboxAction::Restore,
        SandboxAction::Suspend,
        SandboxAction::Stop,
        SandboxAction::Kill,
        SandboxAction::Pause,
        SandboxAction::Checkpoint,
        SandboxAction::EnableSnapshots,
        SandboxAction::DisableSnapshots,
    ] {
        let parsed: SandboxAction = action.as_str().parse().expect("known action parses");
        assert_eq!(parsed, action, "round-trip mismatch for {action}");
    }
}

#[test]
fn from_str_rejects_unknown_action() {
    let err = "frobnicate".parse::<SandboxAction>().unwrap_err();
    assert!(err.to_string().contains("frobnicate"));
}

#[test]
fn wake_entry_accepts_recoverable_states_and_rejects_teardown_states() {
    for lifecycle in [
        SandboxLifecycle::Created,
        SandboxLifecycle::Creating,
        SandboxLifecycle::Starting,
        SandboxLifecycle::Started,
        SandboxLifecycle::Restoring,
        SandboxLifecycle::Pausing,
        SandboxLifecycle::Paused,
        SandboxLifecycle::Resuming,
        SandboxLifecycle::Suspending,
        SandboxLifecycle::Suspended,
        SandboxLifecycle::Checkpointing,
    ] {
        assert!(
            post_wake_lifecycle_allows_started_update(lifecycle),
            "{lifecycle} should allow post-wake completion"
        );
    }

    for lifecycle in [
        SandboxLifecycle::Stopping,
        SandboxLifecycle::Stopped,
        SandboxLifecycle::Deleting,
        SandboxLifecycle::Quarantined,
        SandboxLifecycle::Failed,
    ] {
        assert!(
            !post_wake_lifecycle_allows_started_update(lifecycle),
            "{lifecycle} should reject post-wake completion"
        );
    }

    for lifecycle in pplx_space_placement::StoredSandboxLifecycle::tombstones_as_str() {
        assert!(
            !wake_lifecycle_allows_started_update(lifecycle),
            "{lifecycle} should reject wake before substrate work"
        );
    }
}

fn state(phase: NodeDrainPhase, mode: NodeDrainMode) -> NodeDrainState {
    NodeDrainState { phase, mode }
}

#[test]
fn drain_state_blocks_every_phase_under_a_forced_drain() {
    for phase in [
        NodeDrainPhase::Draining,
        NodeDrainPhase::Drained,
        NodeDrainPhase::Terminating,
    ] {
        assert!(
            state(phase, NodeDrainMode::Forced).blocks_placement(),
            "phase '{phase}' should block placement under a forced drain",
        );
    }
}

#[test]
fn gentle_draining_does_not_block_but_gentle_drained_and_terminating_do() {
    // The whole point of gentle mode: while the node is still `draining`,
    // residents keep serving traffic, so the gate stays open. Once the node
    // reaches `drained` it is waiting to be destroyed and placing a sandbox
    // back on it is wrong in either mode.
    assert!(
        !state(NodeDrainPhase::Draining, NodeDrainMode::Gentle).blocks_placement(),
        "a gentle drain must not 409 user calls while the node is draining",
    );
    for phase in [NodeDrainPhase::Drained, NodeDrainPhase::Terminating] {
        assert!(
            state(phase, NodeDrainMode::Gentle).blocks_placement(),
            "phase '{phase}' must block placement even under a gentle drain",
        );
    }
}

#[test]
fn absent_or_unreadable_drain_mode_leaves_the_gate_open_while_draining() {
    // A NULL column (a row nobody has drained, or a writer that set no mode) and
    // a value this build does not recognize both read as gentle,
    // so neither closes the gate on a `draining` node. The direction matters
    // here specifically: a missing mode resolving to forced would 409 every
    // access and placement call for sandboxes on that node — user-visible
    // breakage — on the strength of a value nobody set.
    for raw in [None, Some("gentel"), Some("")] {
        let mode = NodeDrainMode::from_column_str(raw);
        assert_eq!(mode, NodeDrainMode::Gentle, "{raw:?} must read as gentle");
        assert!(
            !state(NodeDrainPhase::Draining, mode).blocks_placement(),
            "{raw:?} must not block placement while the node is draining",
        );
        // The node still stops accepting placement once it is past `draining`,
        // whatever the column says.
        assert!(state(NodeDrainPhase::Drained, mode).blocks_placement());
    }
}

#[test]
fn unknown_drain_phase_strings_are_rejected_rather_than_guessed() {
    // The CHECK constraint on `nodes.drain_phase` allows only these three
    // values, and the column is decoded into `NodeDrainPhase` at the sqlx
    // boundary. A future fourth value therefore fails the decode — the gate
    // surfaces a 500 and the call is refused — rather than being silently
    // classified. That keeps the failure closed, and adding a variant to the
    // enum forces `blocks_placement`'s match to state what it means.
    //
    // Note this is deliberately the *opposite* of how an unrecognized
    // `drain_mode` is treated (see the test above: it reads as gentle). The two
    // columns answer different questions. An unknown phase means the drain
    // state machine itself is in a state this build cannot reason about, so
    // refusing is right. An unknown mode only means "how aggressively" is
    // unclear, and there the safe answer exists — leave the residents alone —
    // so failing the read would deny users their working sandboxes for no gain.
    NodeDrainPhase::parse("future-phase").unwrap_err();
    for phase in ["draining", "drained", "terminating"] {
        NodeDrainPhase::parse(phase).unwrap();
    }
}
