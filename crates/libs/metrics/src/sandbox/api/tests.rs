use std::time::Instant;

use super::{SandboxApiMetricIdentity, route_outcome};
use crate::emit::test_capture::capture_counters_guard;

#[test]
fn failed_route_counts_once_as_request_error() {
    let capture = capture_counters_guard();
    route_outcome(
        SandboxApiMetricIdentity::ecw("connect"),
        503,
        Instant::now(),
        None,
    );
    let captured = capture.finish();

    assert_eq!(captured.len(), 1, "{captured:?}");
    let counter = &captured[0];
    assert_eq!(counter.name, "sandbox.api.request_error");
    assert_eq!(counter.value, 1);
    for expected in [
        "op:connect",
        "surface:ecw",
        "result:error",
        "result_class:server_error",
        "status_code:503",
    ] {
        assert!(
            counter.tags.iter().any(|tag| tag == expected),
            "missing {expected}: {:?}",
            counter.tags
        );
    }
    assert!(
        !counter
            .tags
            .iter()
            .any(|tag| tag.starts_with("reason:") || tag.starts_with("step:")),
        "{:?}",
        counter.tags
    );
}

#[test]
fn successful_route_emits_no_error_counter() {
    let capture = capture_counters_guard();
    route_outcome(
        SandboxApiMetricIdentity::ecw("connect"),
        200,
        Instant::now(),
        None,
    );

    assert!(capture.finish().is_empty());
}

#[test]
fn route_outcome_never_emits_step_error_metric() {
    let capture = capture_counters_guard();
    for status_code in [400, 404, 412, 429, 500, 503] {
        route_outcome(
            SandboxApiMetricIdentity::ecw("connect"),
            status_code,
            Instant::now(),
            None,
        );
    }
    let captured = capture.finish();

    assert_eq!(captured.len(), 6);
    assert!(
        captured
            .iter()
            .all(|counter| counter.name == "sandbox.api.request_error"),
        "{captured:?}"
    );
}
