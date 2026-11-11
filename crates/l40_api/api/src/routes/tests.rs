use axum::http::StatusCode;

use super::readyz_response;

#[test]
fn readyz_fails_when_draining() {
    let response = readyz_response(true, true);

    assert_eq!(response, (StatusCode::SERVICE_UNAVAILABLE, "draining"));
}

#[test]
fn readyz_fails_until_reflector_is_ready() {
    let response = readyz_response(false, false);

    assert_eq!(
        response,
        (StatusCode::SERVICE_UNAVAILABLE, "pod reflector warming")
    );
}

#[test]
fn readyz_succeeds_when_reflector_is_ready_and_not_draining() {
    let response = readyz_response(true, false);

    assert_eq!(response, (StatusCode::OK, "ok"));
}
