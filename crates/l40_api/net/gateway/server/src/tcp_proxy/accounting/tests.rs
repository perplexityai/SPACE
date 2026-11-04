use super::{Attempt, Outcome};

#[test]
fn attempt_emits_only_its_first_terminal_outcome() {
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let mut attempt = Attempt::forwarding("http2", "request");

    attempt.finish(Outcome::Success, "response_headers");
    attempt.finish(Outcome::Error, "late_error");
    drop(attempt);

    let metrics = capture.finish();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "gateway.forwarding");
    assert_eq!(metrics[0].value, 1);
    assert_eq!(
        metrics[0].tags,
        [
            "protocol:http2",
            "operation:request",
            "outcome:success",
            "reason:response_headers",
        ]
    );
}

#[test]
fn dropped_attempt_is_a_terminal_cancellation() {
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();

    drop(Attempt::origin_tcp());

    let metrics = capture.finish();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "gateway.origin.tcp_connect");
    assert_eq!(metrics[0].value, 1);
    assert_eq!(
        metrics[0].tags,
        ["outcome:cancelled", "reason:work_dropped"]
    );
}
