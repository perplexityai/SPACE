use super::*;

#[test]
fn disabled_by_default() {
    assert!(!GatewayDebugState::default().is_enabled());
}

#[test]
fn future_deadline_enables_and_is_shared_across_clones() {
    let state = GatewayDebugState::default();
    let effective = state.set_deadline(unix_now_ms() + 60_000);
    assert!(effective > unix_now_ms());
    assert!(state.is_enabled());
    // The deadline is shared across clones (same atomic).
    assert!(state.clone().is_enabled());
}

#[test]
fn zero_or_past_deadline_disables() {
    let state = GatewayDebugState::default();
    state.set_deadline(unix_now_ms() + 60_000);
    assert!(state.is_enabled());

    assert_eq!(state.set_deadline(0), 0);
    assert!(!state.is_enabled());

    state.set_deadline(unix_now_ms() + 60_000);
    assert_eq!(state.set_deadline(unix_now_ms().saturating_sub(1)), 0);
    assert!(!state.is_enabled());
}

#[test]
fn deadline_is_clamped_to_max_window() {
    let state = GatewayDebugState::default();
    let requested = unix_now_ms() + 24 * 60 * 60 * 1000; // one day out
    let effective = state.set_deadline(requested);
    assert!(effective < requested);
    assert!(effective <= unix_now_ms() + max_window_ms() + 50);
    assert!(state.is_enabled());
}
