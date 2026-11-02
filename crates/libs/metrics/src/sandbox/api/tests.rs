use std::time::Instant;

use super::{
    ClientIdTag, SandboxApiMetricIdentity, lifecycle_error, route_outcome, sandbox_created,
    scope_request_client_id,
};
use crate::emit::test_capture::{CapturedCounter, capture_counters_guard};
use pplx_space_common_core::types::SandboxId;

fn client_id_tags(counter: &CapturedCounter) -> Vec<&str> {
    counter
        .tags
        .iter()
        .filter(|tag| tag.starts_with("client_id:"))
        .map(String::as_str)
        .collect()
}

/// Runs `emit` inside a request scope on this thread, where the capture lives.
fn in_request_scope(client_id: ClientIdTag, emit: impl FnOnce()) {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(scope_request_client_id(client_id, async { emit() }));
}

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

#[test]
fn route_metrics_carry_the_stamped_client_id() {
    let capture = capture_counters_guard();
    route_outcome(
        SandboxApiMetricIdentity::ecw("pause")
            .with_client_id(ClientIdTag::allowlisted("foreground_client")),
        503,
        Instant::now(),
        None,
    );
    let captured = capture.finish();

    assert_eq!(captured.len(), 1, "{captured:?}");
    assert_eq!(
        client_id_tags(&captured[0]),
        ["client_id:foreground_client"]
    );
}

#[test]
fn identity_metrics_inherit_the_request_client_id() {
    let capture = capture_counters_guard();
    in_request_scope(ClientIdTag::allowlisted("background_client"), || {
        lifecycle_error(
            SandboxId::new(),
            SandboxApiMetricIdentity::ecw("resume"),
            "boom",
        );
        sandbox_created(false);
    });
    lifecycle_error(
        SandboxId::new(),
        SandboxApiMetricIdentity::ecw("resume"),
        "boom",
    );
    let captured = capture.finish();

    let client_ids = captured.iter().map(client_id_tags).collect::<Vec<_>>();
    assert_eq!(
        client_ids,
        [
            ["client_id:background_client"],
            ["client_id:background_client"],
            ["client_id:unknown"]
        ],
        "{captured:?}"
    );
}

#[test]
fn stamped_client_id_wins_over_the_request_scope() {
    let capture = capture_counters_guard();
    in_request_scope(ClientIdTag::allowlisted("background_client"), || {
        lifecycle_error(
            SandboxId::new(),
            SandboxApiMetricIdentity::ecw("connect")
                .with_client_id(ClientIdTag::allowlisted("foreground_client")),
            "boom",
        );
    });
    let captured = capture.finish();

    assert_eq!(captured.len(), 1, "{captured:?}");
    assert_eq!(
        client_id_tags(&captured[0]),
        ["client_id:foreground_client"]
    );
}
