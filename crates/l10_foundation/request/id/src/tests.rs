use super::*;
use http::HeaderValue;

#[test]
fn request_id_prefers_x_request_id() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", HeaderValue::from_static("req-123"));
    headers.insert("x-correlation-id", HeaderValue::from_static("corr-456"));
    assert_eq!(
        request_id_from_headers(&headers).as_deref(),
        Some("req-123"),
    );
}

#[test]
fn request_id_falls_back_to_correlation_id() {
    let mut headers = HeaderMap::new();
    headers.insert("x-correlation-id", HeaderValue::from_static("corr-456"));
    assert_eq!(
        request_id_from_headers(&headers).as_deref(),
        Some("corr-456"),
    );
}

#[test]
fn request_id_ignores_blank_header() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", HeaderValue::from_static("   "));
    assert_eq!(request_id_from_headers(&headers), None);
}

#[test]
fn request_id_accepts_only_bounded_token_characters() {
    let longest = "a".repeat(128);
    let too_long = "a".repeat(129);
    for (value, accepted) in [
        ("req_01a13274-0bc3-7355-9c00-50a16b9ef75e", true),
        ("trace.span:1-2_3", true),
        (longest.as_str(), true),
        (too_long.as_str(), false),
        ("two words", false),
        ("req/../path", false),
        ("\"quoted\"", false),
        ("{\"json\":1}", false),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_str(value).unwrap());
        assert_eq!(
            request_id_from_headers(&headers).is_some(),
            accepted,
            "{value:?}"
        );
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-request-id",
        HeaderValue::from_bytes(b"caf\xc3\xa9").unwrap(),
    );
    assert_eq!(request_id_from_headers(&headers), None, "non-ASCII");
}

#[test]
fn request_id_rejects_an_invalid_request_id_rather_than_falling_back() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", HeaderValue::from_static("not valid"));
    headers.insert("x-correlation-id", HeaderValue::from_static("corr-456"));
    assert_eq!(request_id_from_headers(&headers), None);
}
