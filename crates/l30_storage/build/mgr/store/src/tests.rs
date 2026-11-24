use super::*;

#[test]
fn terminal_state_round_trips_wire_string() {
    for state in [
        BuildTerminalState::Succeeded,
        BuildTerminalState::Failed,
        BuildTerminalState::PublishFailed,
    ] {
        assert_eq!(state.as_str().parse::<BuildTerminalState>().unwrap(), state);
    }
}
