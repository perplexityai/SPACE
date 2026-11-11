use super::*;
use tonic::Code;

#[test]
fn ready_state_is_required_before_satisfying_pull_requests() {
    ensure_ready_state("ready", "seed@sha256:abc").unwrap();

    let err = ensure_ready_state("pulling", "seed@sha256:abc").unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("expected 'ready'"));
}
