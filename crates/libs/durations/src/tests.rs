use super::{
    SimpleDuration, SimpleDurationParseError, format_simple_duration, parse_grpc_timeout,
    parse_simple_duration, parse_simple_duration_secs,
};
use std::time::Duration;

#[test]
fn simple_duration_parser_supports_suffixes_and_decimals() {
    assert_eq!(
        parse_simple_duration("30s").unwrap(),
        Duration::from_secs(30)
    );
    assert_eq!(
        parse_simple_duration("1.5m").unwrap(),
        Duration::from_secs(90)
    );
    assert_eq!(
        parse_simple_duration("2h").unwrap(),
        Duration::from_secs(7_200)
    );
    assert_eq!(
        parse_simple_duration("0.5d").unwrap(),
        Duration::from_secs(43_200)
    );
}

#[test]
fn simple_duration_parser_rejects_missing_or_unsupported_units() {
    assert_eq!(
        parse_simple_duration("30").unwrap_err(),
        SimpleDurationParseError::UnsupportedUnit("30".to_string())
    );
    assert_eq!(
        parse_simple_duration("500ms").unwrap_err(),
        SimpleDurationParseError::Invalid("500ms".to_string())
    );
}

#[test]
fn simple_duration_secs_requires_whole_seconds() {
    assert_eq!(parse_simple_duration_secs("0.1m").unwrap(), 6);
    assert_eq!(
        parse_simple_duration_secs("1.5s").unwrap_err(),
        SimpleDurationParseError::WholeSeconds("1.5s".to_string())
    );
}

#[test]
fn simple_duration_wrapper_round_trips() {
    let duration = "1.5m".parse::<SimpleDuration>().unwrap();
    assert_eq!(duration.as_duration(), Duration::from_secs(90));
    assert_eq!(duration.to_string(), "90s");
}

#[test]
fn grpc_timeout_parser_supports_wire_units() {
    assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7_200)));
    assert_eq!(parse_grpc_timeout("3M"), Some(Duration::from_secs(180)));
    assert_eq!(parse_grpc_timeout("4S"), Some(Duration::from_secs(4)));
    assert_eq!(parse_grpc_timeout("5m"), Some(Duration::from_millis(5)));
    assert_eq!(parse_grpc_timeout("6u"), Some(Duration::from_micros(6)));
    assert_eq!(parse_grpc_timeout("7n"), Some(Duration::from_nanos(7)));
}

#[test]
fn grpc_timeout_parser_rejects_non_wire_shapes() {
    assert_eq!(parse_grpc_timeout(""), None);
    assert_eq!(parse_grpc_timeout("H"), None);
    assert_eq!(parse_grpc_timeout("1h"), None);
    assert_eq!(parse_grpc_timeout("1.5S"), None);
    assert_eq!(parse_grpc_timeout("1 S"), None);
}

#[test]
fn format_simple_duration_prefers_compact_suffixes() {
    assert_eq!(format_simple_duration(Duration::from_secs(60)), "1m");
    assert_eq!(format_simple_duration(Duration::from_secs(90)), "90s");
    assert_eq!(
        format_simple_duration(Duration::new(1, 500_000_000)),
        "1.5s"
    );
}
