use axum::http::{HeaderMap, HeaderValue};

use super::{api_key, bearer_token};

#[test]
fn extracts_bearer_token_from_authorization_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer session-token"),
    );

    assert_eq!(bearer_token(&headers), Some("session-token"));
}

#[test]
fn rejects_non_bearer_authorization_header() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Basic abc123"));

    assert_eq!(bearer_token(&headers), None);
}

#[test]
fn extracts_api_key_from_x_api_key_header() {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("e2b_test_key"));

    assert_eq!(api_key(&headers), Some("e2b_test_key"));
}

#[test]
fn rejects_empty_api_key_header() {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static(""));

    assert_eq!(api_key(&headers), None);
}
