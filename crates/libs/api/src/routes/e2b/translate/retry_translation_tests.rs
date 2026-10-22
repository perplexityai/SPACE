use super::*;
use crate::controllers::substrate::SubstrateError;
use std::time::Duration;

#[test]
fn missing_restore_context_is_not_an_sdk_recoverable_timeout() {
    for translate in [controller_error, restore_controller_error] {
        for (error, expected_status, expected_message) in [
            (
                SubstrateError::RestoreContextUnavailable {
                    message: "no verified restore source; recovery requires intervention".into(),
                },
                StatusCode::PRECONDITION_FAILED,
                "no verified restore source; recovery requires intervention",
            ),
            (
                SubstrateError::RestoreSourcePending {
                    message: "local suspend is not durable; retry the same sandbox".into(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "local suspend is not durable; retry the same sandbox",
            ),
            (
                SubstrateError::Timeout {
                    message: "restore is still converging".into(),
                    phase: None,
                },
                StatusCode::BAD_GATEWAY,
                "Sandbox is not running",
            ),
        ] {
            let mapped = translate(ControllerError::from(error));
            assert_eq!(mapped.status, expected_status);
            assert_eq!(mapped.message, expected_message);
            if expected_status == StatusCode::PRECONDITION_FAILED {
                assert_eq!(mapped.retry_after(), None);
            } else if expected_status == StatusCode::SERVICE_UNAVAILABLE {
                assert!(mapped.retry_after().is_some());
            }
        }
    }
}

#[test]
fn budget_exhausted_error_becomes_sdk_recoverable_sandbox_timeout() {
    // A create/restore/wake that exhausted its ALB-aware budget maps to a
    // 503 + Retry-After. The ECW boundary must turn that into the 502
    // `sandbox_timeout` shape the E2B SDK recovers from — NOT a 409 (which
    // the SDK does not reliably auto-retry) and NOT an opaque 5xx.
    let err = ControllerError::service_unavailable("still converging")
        .with_retry_after(Duration::from_secs(5));
    let mapped = controller_error(err);
    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
    assert_eq!(mapped.message, "Sandbox is not running");
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::Timeout);
}

#[test]
fn sandbox_not_running_conflict_becomes_sdk_recoverable_sandbox_timeout() {
    let err = ControllerError::conflict("sandbox is not running");

    let mapped = controller_error(err);

    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
    assert_eq!(mapped.message, "Sandbox is not running");
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::RouteNotReady);
}

#[test]
fn failed_wake_precondition_becomes_sdk_recoverable_sandbox_timeout() {
    let err = ControllerError::precondition_failed(
        "code: 'FailedPrecondition', message: \"sandbox is failed, cannot wake for access\"",
    );

    let mapped = controller_error(err);

    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
    assert_eq!(mapped.message, "Sandbox is not running");
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::RouteNotReady);
}

#[test]
fn plain_service_unavailable_without_hint_is_not_remapped_to_sandbox_timeout() {
    // A genuine 503 (no retry hint) must keep its status through
    // `controller_error`, so we don't mislabel non-timeout failures as the
    // SDK-recoverable timeout shape.
    let err = ControllerError::service_unavailable("genuinely down");
    let mapped = controller_error(err);
    assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::BackendStatus);
}

#[test]
fn header_only_retry_after_is_preserved_without_timeout_translation() {
    let err = ControllerError::rate_limited("cluster activation admission rate limited")
        .with_retry_after_header(Duration::from_secs(2));
    let mapped = controller_error(err);
    assert_eq!(mapped.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(mapped.retry_after(), Some(Duration::from_secs(2)));
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::Admission);
}

#[test]
fn failed_terminal_restore_conflict_becomes_failed_to_place() {
    let err = ControllerError::conflict("sandbox cannot be restored while lifecycle is failed");

    let mapped = restore_controller_error(err);

    assert_eq!(mapped.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(mapped.message, "Failed to place sandbox");
    assert_eq!(mapped.origin, ResponseOrigin::Cluster);
    assert_eq!(mapped.kind, ResponseKind::Admission);
}

#[test]
fn stopped_terminal_restore_conflict_stays_conflict() {
    let err = ControllerError::conflict("sandbox cannot be restored while lifecycle is stopped");

    let mapped = restore_controller_error(err);

    assert_eq!(mapped.status, StatusCode::CONFLICT);
    assert_eq!(
        mapped.message,
        "sandbox cannot be restored while lifecycle is stopped"
    );
}

#[test]
fn channel_timeout_cancelled_408_becomes_sandbox_timeout() {
    // The dominant recurring smoke flake: a slow-but-not-stuck restore is
    // cut by the tonic channel timeout as `Code::Cancelled` → HTTP 408,
    // carrying no `retry_after` marker. Both ECW translators must surface
    // it as the SDK-recoverable `sandbox_timeout`, not a bare 408.
    let cancelled = ControllerError::from(tonic::Status::cancelled("channel timeout"));
    assert_eq!(cancelled.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(controller_error(cancelled).status, StatusCode::BAD_GATEWAY);
}

#[test]
fn spaced_process_open_timeout_becomes_sandbox_timeout() {
    let err = ControllerError::new(
        StatusCode::REQUEST_TIMEOUT,
        "ECW process stream open timed out before receiving a backend stream",
    );

    let mapped = controller_error(err);

    assert_eq!(mapped.status, StatusCode::BAD_GATEWAY);
    assert_eq!(mapped.message, "Sandbox is not running");
    assert_eq!(mapped.origin, ResponseOrigin::Node);
    assert_eq!(mapped.kind, ResponseKind::Timeout);
}

#[test]
fn grpc_deadline_504_becomes_sandbox_timeout() {
    let deadline = ControllerError::from(tonic::Status::deadline_exceeded("node rpc deadline"));
    assert_eq!(deadline.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(controller_error(deadline).status, StatusCode::BAD_GATEWAY);
}

#[test]
fn is_node_rpc_timeout_matches_only_timeout_statuses() {
    assert!(is_node_rpc_timeout(StatusCode::REQUEST_TIMEOUT));
    assert!(is_node_rpc_timeout(StatusCode::GATEWAY_TIMEOUT));
    assert!(!is_node_rpc_timeout(StatusCode::CONFLICT));
    assert!(!is_node_rpc_timeout(StatusCode::SERVICE_UNAVAILABLE));
    assert!(!is_node_rpc_timeout(StatusCode::BAD_GATEWAY));
}
