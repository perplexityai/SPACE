use super::*;

#[test]
fn wait_timeout_allows_zero_for_try_now() {
    let wait_timeout = validate_optional_wait_timeout(Some(&prost_types::Duration {
        seconds: 0,
        nanos: 0,
    }))
    .unwrap();

    assert_eq!(wait_timeout, Some(std::time::Duration::ZERO));
}

#[test]
fn wait_timeout_rejects_negative_values() {
    let error = validate_optional_wait_timeout(Some(&prost_types::Duration {
        seconds: -1,
        nanos: 0,
    }))
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}
