use super::*;
use http::HeaderValue;
use tower_http::classify::{ClassifiedResponse, ClassifyResponse};

#[test]
fn parse_grpc_path_splits_tonic_style_path() {
    assert_eq!(
        parse_grpc_path("/pplx.space.node.NodeManager/CreateSandbox"),
        ("pplx.space.node.NodeManager", "CreateSandbox"),
    );
}

#[test]
fn parse_grpc_path_handles_missing_leading_slash() {
    assert_eq!(
        parse_grpc_path("pplx.space.node.NodeManager/CreateSandbox"),
        ("pplx.space.node.NodeManager", "CreateSandbox"),
    );
}

#[test]
fn parse_grpc_path_handles_unexpected_shape() {
    assert_eq!(parse_grpc_path("/health"), ("", "health"));
}

#[test]
fn grpc_status_from_headers_parses_numeric_value() {
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", HeaderValue::from_static("8"));
    assert_eq!(grpc_status_from_headers(&headers), Some(8));
}

#[test]
fn grpc_status_from_headers_skips_missing() {
    let headers = HeaderMap::new();
    assert_eq!(grpc_status_from_headers(&headers), None);
}

#[test]
fn grpc_server_application_statuses_are_not_failures() {
    for code in [
        tonic::Code::Ok,
        tonic::Code::Cancelled,
        tonic::Code::InvalidArgument,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unauthenticated,
    ] {
        let response = HttpResponse::builder()
            .header("grpc-status", (code as i32).to_string())
            .body(())
            .unwrap();

        let classified = grpc_server_status_classifier().classify_response(&response);
        assert!(
            matches!(classified, ClassifiedResponse::Ready(Ok(()))),
            "{code:?} should be a regular server response"
        );
        assert!(!grpc_status_is_error(code));
    }

    for code in [
        tonic::Code::Unknown,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Unimplemented,
        tonic::Code::Internal,
        tonic::Code::Unavailable,
        tonic::Code::DataLoss,
    ] {
        let response = HttpResponse::builder()
            .header("grpc-status", (code as i32).to_string())
            .body(())
            .unwrap();
        let classified = grpc_server_status_classifier().classify_response(&response);
        assert!(
            matches!(classified, ClassifiedResponse::Ready(Err(_))),
            "{code:?} should remain a server failure"
        );
        assert!(grpc_status_is_error(code));
    }
}
#[test]
fn format_grpc_request_params_escapes_special_characters() {
    let formatted = format_grpc_request_params([
        ("sandbox_id", "sbx,with,commas".to_string()),
        ("note", r#"has "quotes" and \backslash"#.to_string()),
        ("port", "8080".to_string()),
    ]);
    // Round-trips as a parseable JSON object — no separator collisions.
    let parsed: serde_json::Value = serde_json::from_str(&formatted).expect("output is valid JSON");
    assert_eq!(parsed["sandbox_id"], "sbx,with,commas");
    assert_eq!(parsed["note"], r#"has "quotes" and \backslash"#);
    assert_eq!(parsed["port"], "8080");
}

#[test]
fn failed_grpc_request_logging_is_enabled_only_for_testing_env() {
    assert!(failed_grpc_request_logging_enabled_for_env(Some("testing")));
    assert!(failed_grpc_request_logging_enabled_for_env(Some(
        " TESTING "
    )));

    assert!(!failed_grpc_request_logging_enabled_for_env(Some("prod")));
    assert!(!failed_grpc_request_logging_enabled_for_env(Some("")));
    assert!(!failed_grpc_request_logging_enabled_for_env(None));
}

#[test]
fn grpc_request_params_log_severity_keeps_application_statuses_debug() {
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::PermissionDenied),
        GrpcRequestParamsLogSeverity::Debug,
    );
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::NotFound),
        GrpcRequestParamsLogSeverity::Debug,
    );
    assert_eq!(
        grpc_request_params_log_severity(tonic::Code::Unavailable),
        GrpcRequestParamsLogSeverity::Warn,
    );
}

#[test]
fn grpc_request_params_for_log_preserves_small_params() {
    let params = r#"{"port":"8080","sandbox_id":"sbx"}"#;
    let log = grpc_request_params_for_log(params);

    assert_eq!(log.value, params);
    assert!(!log.truncated);
}

#[test]
fn grpc_request_params_for_log_truncates_large_params() {
    let params = "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES + 1);
    let log = grpc_request_params_for_log(&params);

    assert!(log.truncated);
    assert!(
        log.value
            .starts_with(&"a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES))
    );
    assert!(log.value.ends_with(&format!(
        "... [truncated, original_bytes={}]",
        MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES + 1
    )));
}

#[test]
fn truncate_utf8_for_log_truncates_on_utf8_boundary() {
    let value = format!(
        "{}é-suffix",
        "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES - 1)
    );
    let log = truncate_utf8_for_log(&value, MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES);

    assert!(log.truncated);
    assert!(!log.value.contains('\u{fffd}'));
    assert!(log.value.starts_with(&format!(
        "{}...",
        "a".repeat(MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES - 1)
    )));
}
