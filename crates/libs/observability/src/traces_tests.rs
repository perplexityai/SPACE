use super::TraceExporter;

/// Build capabilities, as the three shapes a shipped binary can have.
const DATADOG: (bool, bool) = (true, true);
const OTEL_ONLY: (bool, bool) = (false, true);
const NO_TELEMETRY: (bool, bool) = (false, false);

fn resolve(
    raw: &str,
    (datadog, otel): (bool, bool),
    endpoint: bool,
) -> Result<TraceExporter, crate::ObservabilityError> {
    raw.parse::<TraceExporter>()?
        .resolve(datadog, otel, endpoint)
}

/// `auto` matches the metrics sink: Datadog, then OTLP when an endpoint says
/// where to send it, then off.
#[test]
fn trace_exporter_resolution_covers_every_selector_and_capability() {
    for (raw, capabilities, endpoint, expected) in [
        // A Datadog build is unchanged, with or without an endpoint.
        ("auto", DATADOG, false, Ok(TraceExporter::Datadog)),
        ("auto", DATADOG, true, Ok(TraceExporter::Datadog)),
        // Without Datadog, an endpoint is what makes auto mean OTLP.
        ("auto", OTEL_ONLY, true, Ok(TraceExporter::Otlp)),
        ("auto", OTEL_ONLY, false, Ok(TraceExporter::Off)),
        ("auto", NO_TELEMETRY, true, Ok(TraceExporter::Off)),
        ("auto", NO_TELEMETRY, false, Ok(TraceExporter::Off)),
        // Explicit selections ignore the endpoint entirely.
        ("datadog", DATADOG, false, Ok(TraceExporter::Datadog)),
        ("otlp", OTEL_ONLY, false, Ok(TraceExporter::Otlp)),
        ("otlp", DATADOG, false, Ok(TraceExporter::Otlp)),
        ("off", DATADOG, true, Ok(TraceExporter::Off)),
        ("off", NO_TELEMETRY, false, Ok(TraceExporter::Off)),
        // journald is not a trace exporter.
        ("journald", DATADOG, true, Err("invalid")),
        // An uncompiled or unknown selector is a configuration error.
        ("datadog", OTEL_ONLY, true, Err("unavailable")),
        ("otlp", NO_TELEMETRY, true, Err("unavailable")),
        ("bogus", DATADOG, true, Err("invalid")),
    ] {
        let actual = resolve(raw, capabilities, endpoint);
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(
                    actual, expected,
                    "{raw} {capabilities:?} endpoint={endpoint}"
                )
            }
            (Err(kind), Err(error)) => {
                let matched = match kind {
                    "unavailable" => matches!(
                        error,
                        crate::ObservabilityError::TraceExporterUnavailable { .. }
                    ),
                    _ => matches!(
                        error,
                        crate::ObservabilityError::InvalidTraceExporter { .. }
                    ),
                };
                assert!(matched, "{raw}: unexpected error {error}");
            }
            (expected, actual) => {
                panic!(
                    "{raw} {capabilities:?} endpoint={endpoint}: want {expected:?}, got {actual:?}"
                )
            }
        }
    }
}

#[cfg(feature = "otel")]
#[test]
fn plaintext_otlp_rejects_headers_without_exposing_values() {
    const CHILD: &str = "SPACE_TRACE_HEADER_TEST";
    if let Some(expectation) = std::env::var_os(CHILD) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let _entered = runtime.enter();
        let (exporter, _) = TraceExporter::from_env().expect("exporter selection");
        let result = super::build_provider(
            exporter,
            &crate::identity::Identity::from_env("trace-header-policy"),
        );
        if expectation == "reject" {
            let error = result.expect_err("plaintext OTLP must reject configured headers");
            assert!(matches!(error, crate::ObservabilityError::TraceInit(_)));
            assert!(!format!("{error} {error:?}").contains("space-synthetic"));
        } else if let Some(provider) = result.expect("permitted configuration") {
            provider.shutdown().expect("shutdown");
        }
        return;
    }

    // Each case gets its own process: exporter initialization reads global env.
    let cases = [
        ("otlp", None, None, "accept"),
        ("otlp", Some(""), Some(""), "accept"),
        ("otlp", Some(" \t"), Some(" \t"), "accept"),
        (
            "otlp",
            Some("authorization=space-synthetic"),
            None,
            "reject",
        ),
        ("otlp", None, Some("x-api-key=space-synthetic"), "reject"),
        (
            "otlp",
            Some(""),
            Some("x-api-key=space-synthetic"),
            "reject",
        ),
        ("otlp", Some("space-synthetic-malformed"), None, "reject"),
        (
            "off",
            Some("authorization=space-synthetic"),
            Some("x-api-key=space-synthetic"),
            "accept",
        ),
        (
            // The child always has a trace endpoint, so auto means Datadog in a
            // Datadog build and OTLP otherwise. Only the OTLP resolution
            // reaches the plaintext header policy.
            "auto",
            Some("authorization=space-synthetic"),
            Some("x-api-key=space-synthetic"),
            if cfg!(feature = "datadog") {
                "accept"
            } else {
                "reject"
            },
        ),
        (
            "datadog",
            Some("authorization=space-synthetic"),
            Some("x-api-key=space-synthetic"),
            "accept",
        ),
    ];
    for (exporter, trace_headers, common_headers, expectation) in cases {
        if exporter == "datadog" && !cfg!(feature = "datadog") {
            continue;
        }
        let mut command = std::process::Command::new(std::env::current_exe().expect("test binary"));
        for (key, _) in std::env::vars_os() {
            if ["DD_", "OTEL_", "SPACE_TRACE_", "SPACE_METRICS_"]
                .iter()
                .any(|prefix| key.to_string_lossy().starts_with(prefix))
            {
                command.env_remove(key);
            }
        }
        command
            .args([
                "--exact",
                "traces::tests::plaintext_otlp_rejects_headers_without_exposing_values",
                "--nocapture",
            ])
            .env(CHILD, expectation)
            .env("SPACE_TRACE_EXPORTER", exporter)
            .env("DD_TRACE_AGENT_URL", "http://127.0.0.1:1")
            .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1");
        if let Some(value) = trace_headers {
            command.env("OTEL_EXPORTER_OTLP_TRACES_HEADERS", value);
        }
        if let Some(value) = common_headers {
            command.env("OTEL_EXPORTER_OTLP_HEADERS", value);
        }
        let output = command
            .output()
            .expect("run isolated exporter initialization");
        assert!(
            output.status.success(),
            "exporter={exporter}, expectation={expectation}:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(feature = "otel")]
#[test]
fn disabling_traces_preserves_pool_metrics() {
    // Global subscribers and metrics sinks are process singletons. Exercise the
    // actual initializer in an isolated child rather than mutating shared env.
    if std::env::var_os("SPACE_TRACE_OFF_METRICS_TEST").is_none() {
        // Both subscriber layouts build this bridge without a trace provider,
        // and the metric has to survive in each: the per-layer one a trace
        // filter opts into, and the shared one `RUST_LOG` governs alone.
        for trace_filter in [Some("off"), None] {
            let mut command =
                std::process::Command::new(std::env::current_exe().expect("test binary"));
            command
                .args([
                    "--exact",
                    "traces::tests::disabling_traces_preserves_pool_metrics",
                    "--nocapture",
                ])
                .env("SPACE_TRACE_OFF_METRICS_TEST", "1")
                .env("SPACE_TRACE_EXPORTER", "off")
                .env("SPACE_METRICS_SINK", "off")
                .env("RUST_LOG", "error")
                .env_remove("SPACE_TRACE_FILTER");
            if let Some(filter) = trace_filter {
                command.env("SPACE_TRACE_FILTER", filter);
            }
            let output = command.output().expect("run isolated initializer");
            assert!(
                output.status.success(),
                "trace filter {trace_filter:?}:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    static ACQUIRES: AtomicUsize = AtomicUsize::new(0);
    struct CaptureSink;
    impl crate::sink::MetricSink for CaptureSink {
        fn label(&self) -> &'static str {
            "test-capture"
        }
        fn emit(&self, metric: crate::sink::Metric<'_>) {
            if metric.name == crate::DB_POOL_ACQUIRE_TOTAL_MS_METRIC {
                ACQUIRES.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    assert!(crate::METRICS.set(Box::new(CaptureSink)).is_ok());
    let guard = crate::init("trace-off-metrics").expect("off initializer");
    tracing::info!(target: "sqlx::pool::acquire", acquired_after_secs = 0.001_f64);
    assert_eq!(
        ACQUIRES.load(Ordering::Relaxed),
        1,
        "disabling trace export must preserve pool metrics in either layout"
    );
    guard.shutdown().expect("shutdown");
}

/// The endpoint and protocol rules are shared, so only the trace-specific
/// parts need covering here: the default endpoint, and that a value which any
/// other producer would refuse is refused for traces too.
#[cfg(feature = "otel")]
mod exporter_env {
    use super::super::{DEFAULT_ENDPOINT, TraceInitError, endpoint_from_value, validate_protocol};

    #[test]
    fn unset_settings_use_the_local_collector_over_grpc() {
        validate_protocol(None).unwrap();
        assert_eq!(endpoint_from_value(None).unwrap(), DEFAULT_ENDPOINT);
    }

    #[test]
    fn protocol_comparison_ignores_case_and_padding() {
        for raw in ["grpc", "GRPC", " grpc ", ""] {
            validate_protocol(Some(raw.to_string())).unwrap_or_else(|error| {
                panic!("{raw:?} must be accepted: {error}");
            });
        }
        assert!(matches!(
            validate_protocol(Some("http/protobuf".to_string())),
            Err(TraceInitError::UnsupportedProtocol(_))
        ));
    }

    /// The rule itself is tabled in `otlp_endpoint`; this only proves traces
    /// route through it and surface the rejection as an init error.
    #[test]
    fn an_unusable_endpoint_fails_initialization() {
        assert!(matches!(
            endpoint_from_value(Some("http://collector:4317/v1/traces".to_string())),
            Err(TraceInitError::UnsupportedEndpoint(_))
        ));
    }

    /// A rejected endpoint reaches operator logs, so the message must name the
    /// settings rather than repeat a value that can embed credentials.
    #[test]
    fn rejection_names_the_settings_not_the_value() {
        let Err(error) = endpoint_from_value(Some("http://user:pw@collector:4317".to_string()))
        else {
            panic!("userinfo must be rejected");
        };
        let message = error.to_string();
        assert!(
            message.contains("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            "{message}"
        );
        assert!(!message.contains("pw"), "{message}");
    }
}
