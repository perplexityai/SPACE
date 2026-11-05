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

#[test]
fn the_at_rest_mark_shares_common_grpc_route_and_state_keys() {
    let route = pplx_space_common_grpc::sandbox_service_route_not_ready_status(
        Code::Unavailable,
        "not ready",
    );
    assert!(
        route
            .metadata()
            .contains_key(SANDBOX_SERVICE_ROUTE_ERROR_METADATA)
    );
    let not_running = pplx_space_common_grpc::mark_sandbox_not_running(
        Status::failed_precondition("not running"),
        SandboxLifecycle::Paused.as_str(),
    );
    assert!(not_running.metadata().contains_key(SANDBOX_STATE_METADATA));
}
