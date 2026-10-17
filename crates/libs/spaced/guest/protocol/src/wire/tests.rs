use tonic::Code;

use super::*;

#[test]
fn clock_conversions_preserve_values_and_reject_invalid_ranges() {
    let duration = std::time::Duration::new(42, 123);
    assert_eq!(
        duration_to_proto(duration).unwrap(),
        prost_types::Duration {
            seconds: 42,
            nanos: 123
        }
    );
    assert_eq!(
        duration_to_proto(std::time::Duration::from_secs(u64::MAX))
            .expect_err("duration overflow")
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        system_time_to_proto(std::time::UNIX_EPOCH + duration).unwrap(),
        prost_types::Timestamp {
            seconds: 42,
            nanos: 123
        }
    );
    assert_eq!(
        system_time_to_proto(std::time::UNIX_EPOCH - duration)
            .expect_err("before epoch")
            .code(),
        Code::Internal
    );
}

#[test]
fn env_round_trips_through_proto_shape() {
    let env = parse_env(HashMap::from([
        ("PATH".to_string(), "/usr/bin".to_string()),
        ("SPACE_TOKEN".to_string(), "secret".to_string()),
    ]));

    let proto = proto_env(&env);

    assert_eq!(proto.get("PATH").map(String::as_str), Some("/usr/bin"));
    assert_eq!(proto.get("SPACE_TOKEN").map(String::as_str), Some("secret"));
}

#[test]
fn parse_env_drops_invalid_entries() {
    let env = parse_env(HashMap::from([
        ("VALID".to_string(), "ok".to_string()),
        ("1BAD".to_string(), "value".to_string()),
        ("MULTILINE".to_string(), "line 1\nline 2".to_string()),
    ]));

    let proto = proto_env(&env);
    assert_eq!(proto.get("VALID").map(String::as_str), Some("ok"));
    assert_eq!(
        proto.get("MULTILINE").map(String::as_str),
        Some("line 1\nline 2")
    );
    assert!(!proto.contains_key("1BAD"));
}

#[test]
fn parse_env_caps_aggregate_overflow() {
    let env = parse_env(
        (0..=pplx_space_common_core::types::MAX_ENV_ENTRIES)
            .map(|idx| (format!("KEY_{idx}"), "value".to_string()))
            .collect(),
    );

    assert_eq!(
        proto_env(&env).len(),
        pplx_space_common_core::types::MAX_ENV_ENTRIES
    );
}

#[test]
fn globs_trim_and_round_trip_through_proto_shape() {
    let globs =
        parse_globs(vec![" **/*.rs ".to_string(), "src/**".to_string()]).expect("globs are valid");

    assert_eq!(
        proto_globs(&globs),
        vec!["**/*.rs".to_string(), "src/**".to_string()]
    );
}

#[test]
fn parse_globs_reports_invalid_arguments() {
    let error = parse_globs(vec!["/tmp/**".to_string()]).expect_err("absolute glob should fail");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("relative glob"));
}

#[test]
fn optional_idempotency_key_round_trips_through_proto_shape() {
    let key = IdempotencyKey::parse("request-1").expect("key is valid");

    assert_eq!(
        proto_idempotency_key(Some(key)).as_deref(),
        Some("request-1")
    );
    assert_eq!(proto_idempotency_key(None), None);
}
