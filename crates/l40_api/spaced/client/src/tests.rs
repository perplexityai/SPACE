use super::*;

#[test]
fn retryable_status_codes_match_bridge_policy() {
    assert!(status_is_retryable(&tonic::Status::unavailable("retry")));
    assert!(status_is_retryable(&tonic::Status::unknown("retry")));
    assert!(status_is_retryable(&tonic::Status::deadline_exceeded(
        "retry"
    )));
    assert!(!status_is_retryable(&tonic::Status::invalid_argument("no")));
}

// A refusal the guest made before starting anything goes back to the caller,
// whatever its code: the hop does not retry it on the caller's behalf.
#[test]
fn refused_before_start_is_never_retried_by_a_hop() {
    let mut refused = tonic::Status::unavailable("guest shutting down");
    refused.metadata_mut().insert(
        pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
        tonic::metadata::MetadataValue::from_static(
            pplx_space_common_core::spaced::REQUEST_REFUSED,
        ),
    );
    assert!(!status_is_retryable(&refused));
    assert!(status_is_retryable(&tonic::Status::unavailable(
        "transport closed"
    )));
}
