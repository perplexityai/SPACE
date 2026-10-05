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

#[test]
fn inspection_cancellation_keeps_the_last_reached_stage_and_captured_mode() {
    use crate::state::{PolicyNotReady, TrafficPolicy};
    use pplx_space_metrics::gateway::InspectionStage;

    for (policy, mode) in [
        (
            TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady),
            "on",
        ),
        (
            TrafficPolicy::NotReady(PolicyNotReady::NoIdentity),
            "unknown",
        ),
        (
            TrafficPolicy::Ready(std::sync::Arc::new(
                pplx_space_net_common::NetworkPolicyV1::allow_all(),
            )),
            "off",
        ),
    ] {
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let mut attempt = Attempt::inspection_admission();
        attempt.admission_mode(&policy);
        attempt.inspection_stage(InspectionStage::ClientHello);
        drop(attempt);
        let metrics = capture.finish();
        assert_eq!(metrics.len(), 1);
        assert_eq!(
            metrics[0].tags,
            [
                "outcome:cancelled".to_owned(),
                "reason:work_dropped".to_owned(),
                format!("tls_interception:{mode}"),
                "stage:client_hello".to_owned(),
                "path:unknown".to_owned(),
                "policy_reason:not_evaluated".to_owned(),
            ]
        );
    }
    for stage in [
        InspectionStage::GuestHandshake,
        InspectionStage::SessionForwarding,
    ] {
        let capture = pplx_space_metrics::test_capture::capture_counters_guard();
        let mut session = Attempt::inspection_session();
        session.inspection_stage(stage);
        drop(session);
        let metrics = capture.finish();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "gateway.inspection.session");
        assert_eq!(
            metrics[0].tags,
            [
                "outcome:cancelled".to_owned(),
                "reason:work_dropped".to_owned(),
                "tls_interception:on".to_owned(),
                format!("stage:{stage}"),
                "path:tls_inspection".to_owned(),
                "policy_reason:allowed".to_owned(),
            ]
        );
    }
}
