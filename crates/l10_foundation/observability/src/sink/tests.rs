use super::{
    MetricsSinkChoice, ObservabilityError, Selection, build_constant_tags,
    resolve_metrics_sink_choice, validate_metrics_sink_env,
};

/// Build capabilities, as the three shapes a shipped binary can have.
const DATADOG: (bool, bool) = (true, true);
const OTEL_ONLY: (bool, bool) = (false, true);
const NO_TELEMETRY: (bool, bool) = (false, false);

fn resolve(
    raw: Option<&str>,
    (datadog, otlp): (bool, bool),
    endpoint: bool,
) -> Result<MetricsSinkChoice, ObservabilityError> {
    resolve_metrics_sink_choice(raw, datadog, otlp, endpoint)
}

/// `auto` resolves Datadog, then OTLP when an endpoint says where to send it,
/// then off.
#[test]
fn metrics_sink_resolution_covers_every_selector_and_capability() {
    for (raw, capabilities, endpoint, expected) in [
        // A Datadog build is unchanged, with or without an endpoint.
        (None, DATADOG, false, Ok(MetricsSinkChoice::Datadog)),
        (None, DATADOG, true, Ok(MetricsSinkChoice::Datadog)),
        (Some("auto"), DATADOG, false, Ok(MetricsSinkChoice::Datadog)),
        (Some("auto"), DATADOG, true, Ok(MetricsSinkChoice::Datadog)),
        // Without Datadog, an endpoint is what makes auto mean OTLP.
        (None, OTEL_ONLY, true, Ok(MetricsSinkChoice::Otlp)),
        (Some("auto"), OTEL_ONLY, true, Ok(MetricsSinkChoice::Otlp)),
        (None, OTEL_ONLY, false, Ok(MetricsSinkChoice::Off)),
        (Some("auto"), OTEL_ONLY, false, Ok(MetricsSinkChoice::Off)),
        // No telemetry compiled in: auto is off, endpoint or not.
        (None, NO_TELEMETRY, true, Ok(MetricsSinkChoice::Off)),
        (None, NO_TELEMETRY, false, Ok(MetricsSinkChoice::Off)),
        (Some("auto"), NO_TELEMETRY, true, Ok(MetricsSinkChoice::Off)),
        // Explicit selections ignore the endpoint entirely.
        (
            Some("datadog"),
            DATADOG,
            false,
            Ok(MetricsSinkChoice::Datadog),
        ),
        (Some("otlp"), DATADOG, false, Ok(MetricsSinkChoice::Otlp)),
        (Some("otlp"), OTEL_ONLY, false, Ok(MetricsSinkChoice::Otlp)),
        (Some("off"), DATADOG, true, Ok(MetricsSinkChoice::Off)),
        (Some("off"), NO_TELEMETRY, false, Ok(MetricsSinkChoice::Off)),
        // journald was removed; naming it is a configuration error.
        (Some("journald"), DATADOG, true, Err("invalid")),
        (Some("journald"), OTEL_ONLY, true, Err("invalid")),
        (Some("journald"), NO_TELEMETRY, false, Err("invalid")),
        // An uncompiled or unknown selector is a configuration error.
        (Some("datadog"), OTEL_ONLY, true, Err("datadog-unavailable")),
        (
            Some("datadog"),
            NO_TELEMETRY,
            false,
            Err("datadog-unavailable"),
        ),
        (Some("otlp"), NO_TELEMETRY, true, Err("otlp-unavailable")),
        (Some("bogus"), DATADOG, true, Err("invalid")),
        (Some("bogus"), NO_TELEMETRY, false, Err("invalid")),
    ] {
        let actual = resolve(raw, capabilities, endpoint);
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(
                    actual, expected,
                    "{raw:?} {capabilities:?} endpoint={endpoint}"
                )
            }
            (Err(kind), Err(error)) => {
                let matched = match kind {
                    "datadog-unavailable" => {
                        matches!(
                            error,
                            ObservabilityError::DatadogMetricsSinkUnavailable { .. }
                        )
                    }
                    "otlp-unavailable" => {
                        matches!(error, ObservabilityError::OtlpMetricsSinkUnavailable { .. })
                    }
                    _ => matches!(error, ObservabilityError::InvalidMetricsSink { .. }),
                };
                assert!(matched, "{raw:?}: unexpected error {error}");
            }
            (expected, actual) => {
                panic!(
                    "{raw:?} {capabilities:?} endpoint={endpoint}: want {expected:?}, got {actual:?}"
                )
            }
        }
    }
}

#[test]
fn metrics_sink_rejects_datadog_without_feature() {
    let error = resolve(Some("datadog"), OTEL_ONLY, true).unwrap_err();
    assert!(matches!(
        error,
        ObservabilityError::DatadogMetricsSinkUnavailable { raw } if raw == "datadog"
    ));
}

#[test]
fn metrics_sink_rejects_otlp_without_feature() {
    let error = resolve(Some("otlp"), (true, false), true).unwrap_err();
    assert!(matches!(
        error,
        ObservabilityError::OtlpMetricsSinkUnavailable { raw } if raw == "otlp"
    ));
}

#[test]
fn metrics_sink_rejects_invalid_value() {
    // Selecting a transport is exclusive, even in a build supporting both,
    // and the DogStatsD sink is named `datadog` rather than by its protocol.
    for value in [
        "stdout",
        "datadog+otlp",
        "otlp-noncounters",
        "datadog,otlp",
        "dogstatsd",
    ] {
        let error = resolve(Some(value), DATADOG, true).unwrap_err();
        assert!(matches!(
            error,
            ObservabilityError::InvalidMetricsSink { raw } if raw == value
        ));
    }
}

#[test]
fn build_constant_tags_appends_cluster_id_last() {
    let identity = crate::identity::Identity::from_lookup("default", |key| match key {
        "DD_ENV" => Some("prod".into()),
        "DD_SERVICE" => Some("space-api".into()),
        "DD_VERSION" => Some("abc123".into()),
        _ => None,
    });
    let tags = build_constant_tags(&identity, Some("prod-2".to_string()));
    assert_eq!(
        tags,
        vec![
            "env:prod".to_string(),
            "service:space-api".to_string(),
            "version:abc123".to_string(),
            "cluster_id:prod-2".to_string(),
        ]
    );
}

#[test]
fn build_constant_tags_omits_empty_optional_identity() {
    let identity = crate::identity::Identity::from_lookup("default", |_| None);
    assert_eq!(build_constant_tags(&identity, None), Vec::<String>::new());
    assert_eq!(
        build_constant_tags(&identity, Some(String::new())),
        Vec::<String>::new()
    );
}

#[test]
fn dd_only_constant_tags_preserve_empty_and_whitespace_bytes() {
    let identity = crate::identity::Identity::from_lookup("default", |key| match key {
        "DD_ENV" => Some(" prod ".into()),
        "DD_SERVICE" => Some("  ".into()),
        "DD_VERSION" => Some(String::new()),
        _ => None,
    });
    assert_eq!(
        build_constant_tags(&identity, Some("cluster".into())).join(","),
        "env: prod ,service:  ,version:,cluster_id:cluster"
    );
}

/// An OTLP sink `auto` chose is exempt from the startup validation, whatever
/// the environment holds: the operator never named OTLP, so an unusable
/// configuration is reported when the sink is built and metrics are disabled.
/// An explicit selection is not exempt — `otlp_process_lifecycle` covers that
/// end of it, because it needs a process to observe the failure.
#[test]
fn auto_is_exempt_from_startup_validation() {
    for choice in [
        MetricsSinkChoice::Otlp,
        MetricsSinkChoice::Datadog,
        MetricsSinkChoice::Off,
    ] {
        validate_metrics_sink_env(choice, Selection::Auto)
            .unwrap_or_else(|error| panic!("auto must never fail startup: {choice:?}: {error}"));
    }
    // A sink other than OTLP has no environment-derived half to reject, so an
    // explicit selection of one is equally safe.
    for choice in [MetricsSinkChoice::Datadog, MetricsSinkChoice::Off] {
        validate_metrics_sink_env(choice, Selection::Explicit)
            .unwrap_or_else(|error| panic!("{choice:?} has nothing to validate: {error}"));
    }
}
