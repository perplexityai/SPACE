use super::*;

#[test]
fn an_at_rest_session_route_round_trips_its_sandbox_and_state() {
    let sandbox_id = SandboxId::new();
    let marked = mark_session_route_at_rest(
        Status::failed_precondition("no node"),
        sandbox_id,
        SandboxLifecycle::Paused,
    );
    assert_eq!(
        session_route_at_rest(&marked),
        Some(SessionRouteAtRest {
            sandbox_id: Some(sandbox_id),
            state: Some(SandboxLifecycle::Paused),
        })
    );
    assert_eq!(
        session_route_at_rest(&Status::failed_precondition("no node")),
        None
    );
    let unavailable = mark_session_route_at_rest(
        Status::unavailable("no node"),
        sandbox_id,
        SandboxLifecycle::Paused,
    );
    assert_eq!(session_route_at_rest(&unavailable), None);
}
