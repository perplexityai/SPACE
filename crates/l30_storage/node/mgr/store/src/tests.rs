use super::*;

#[test]
fn slot_vocabularies_round_trip_through_text() {
    for state in [
        SlotState::Missing,
        SlotState::Creating,
        SlotState::Ready,
        SlotState::Leasing,
        SlotState::Leased,
        SlotState::Dirty,
        SlotState::Resetting,
        SlotState::Quarantined,
        SlotState::Destroying,
    ] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match state {
            SlotState::Missing
            | SlotState::Creating
            | SlotState::Ready
            | SlotState::Leasing
            | SlotState::Leased
            | SlotState::Dirty
            | SlotState::Resetting
            | SlotState::Quarantined
            | SlotState::Destroying => {}
        }
        assert_eq!(state.to_string().parse::<SlotState>().unwrap(), state);
    }
    for health in [
        SlotHealth::Unknown,
        SlotHealth::Healthy,
        SlotHealth::Degraded,
        SlotHealth::Invalid,
    ] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match health {
            SlotHealth::Unknown
            | SlotHealth::Healthy
            | SlotHealth::Degraded
            | SlotHealth::Invalid => {}
        }
        assert_eq!(health.to_string().parse::<SlotHealth>().unwrap(), health);
    }
    for kind in [LeaseKind::Pooled, LeaseKind::Ephemeral] {
        // A new variant fails to compile here until it is added to this match;
        // add it to the array above too.
        match kind {
            LeaseKind::Pooled | LeaseKind::Ephemeral => {}
        }
        assert_eq!(kind.to_string().parse::<LeaseKind>().unwrap(), kind);
    }
    "parked".parse::<SlotState>().unwrap_err();
    "sick".parse::<SlotHealth>().unwrap_err();
    "borrowed".parse::<LeaseKind>().unwrap_err();
}
