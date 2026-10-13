use std::io;
use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt as _;

#[test]
fn trace_filter_validation_is_strict_and_does_not_disclose_values() {
    const CHILD: &str = "SPACE_TRACE_FILTER_VALIDATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let error = match super::init("filter-validation") {
            Ok(_) => panic!("invalid trace filter accepted"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            super::ObservabilityError::InvalidTraceFilter
        ));
        // The message names the setting. It carries no directives, which can
        // hold customer values in a field selector, and no source that might
        // print them, so the wording itself is free to change.
        let rendered = format!("{error} {error:?}");
        assert!(rendered.contains("SPACE_TRACE_FILTER"), "{rendered}");
        assert!(!rendered.contains("synthetic-secret"), "{rendered}");
        assert!(std::error::Error::source(&error).is_none());
        return;
    }
    let mut values = vec![std::ffi::OsString::from(
        "[request{customer=synthetic-secret}]=invalid-level",
    )];
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        values.push(std::ffi::OsString::from_vec(vec![0xff]));
    }
    for value in values {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "log_filter_tests::trace_filter_validation_is_strict_and_does_not_disclose_values",
                "--nocapture",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env("SPACE_TRACE_FILTER", value)
            .env("SPACE_TRACE_EXPORTER", "off")
            .env("SPACE_METRICS_SINK", "off")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-secret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-secret"));
    }
}

#[test]
fn deployment_git_sha_prefers_runtime_env_then_identity_file() {
    assert_eq!(
        super::select_git_sha(
            Some(" env-sha \n".to_string()),
            [Some("file-sha\n".to_string())],
        ),
        "env-sha"
    );
    assert_eq!(
        super::select_git_sha(
            Some("  ".to_string()),
            [None, Some(" file-sha\n".to_string())],
        ),
        "file-sha"
    );
    assert_eq!(super::select_git_sha(None, [None]), "unknown");
}

#[test]
fn log_filters_apply_in_every_format_without_trace_export() {
    const CHILD: &str = "SPACE_LOG_FILTER_FORMAT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let guard = super::init("filter-formats").unwrap();
        tracing::info!("fixture-info");
        tracing::error!("fixture-error");
        guard.shutdown().unwrap();
        return;
    }
    for format in ["json", "compact", "pretty"] {
        for threshold in [
            "info",
            "error",
            "trace,pplx_space_observability::log_filter_tests=error",
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "log_filter_tests::log_filters_apply_in_every_format_without_trace_export",
                    "--nocapture",
                ])
                .env_clear()
                .env(CHILD, "1")
                .env("SPACE_TRACE_EXPORTER", "off")
                .env("SPACE_METRICS_SINK", "off")
                .env("SPACE_TRACE_FILTER", "debug")
                .env("SPACE_LOG_FORMAT", format)
                .env("RUST_LOG", threshold)
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert_eq!(
                stdout.contains("fixture-info"),
                threshold == "info",
                "{format}: {stdout}"
            );
            assert!(stdout.contains("fixture-error"), "{format}: {stdout}");
        }
    }
}

#[test]
fn query_events_feed_tracing_without_becoming_logs() {
    assert!(!super::should_format_target(super::SQLX_QUERY_TARGET));
    assert!(!super::should_format_target(super::RUSQLITE_QUERY_TARGET));
    assert!(!super::should_format_target(super::METRICS_TARGET));
    assert!(super::should_format_target("space::startup"));
}

#[test]
fn static_filter_optimization_preserves_envfilter_rules() {
    use tracing_subscriber::{EnvFilter, Layer as _};
    for directives in [
        "info",
        "warn",
        "off",
        "info,fixture=debug",
        "fixture=trace,fixture::quiet=off",
        "off,[request]=info",
        "off,[request{selected=true}]=debug",
    ] {
        let mut outputs = Vec::new();
        for optimized in [false, true] {
            let output = CapturedLogs::default();
            let filter = EnvFilter::try_new(directives).unwrap();
            let layer = tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(output.clone());
            let dispatch = if optimized {
                tracing::Dispatch::new(
                    tracing_subscriber::registry()
                        .with(layer.with_filter(super::layer_filter(filter))),
                )
            } else {
                tracing::Dispatch::new(
                    tracing_subscriber::registry().with(layer.with_filter(filter)),
                )
            };
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::info!(target: "fixture", "outside");
                tracing::debug!(target: "fixture::quiet", "quiet");
                let span = tracing::info_span!("request", selected = tracing::field::Empty);
                let _entered = span.enter();
                tracing::debug!(target: "fixture", "before record");
                span.record("selected", true);
                tracing::debug!(target: "fixture", "after record");
                tracing::info!(target: "fixture", "inside");
                tracing::warn!(target: "fixture", "warning");
            });
            outputs.push(output.contents());
        }
        assert_eq!(outputs[0], outputs[1], "{directives}");
    }
}

#[test]
fn slow_acquire_warnings_stay_in_logs() {
    use tracing_subscriber::Layer as _;

    let output = CapturedLogs::default();
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(output.clone())
        .with_filter(tracing_subscriber::filter::filter_fn(
            super::should_format_event,
        ));
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            target: "sqlx::pool::acquire",
            acquired_after_secs = 0.001_f64,
            "acquired connection"
        );
        tracing::warn!(
            target: "sqlx::pool::acquire",
            acquired_after_secs = 3.0_f64,
            slow_acquire_threshold_secs = 2.0_f64,
            "acquired connection, but time to acquire exceeded slow threshold"
        );
    });

    let logs = output.contents();
    assert!(
        logs.contains("exceeded slow threshold"),
        "slow-acquire WARN must stay in service logs, got: {logs:?}"
    );
    assert_eq!(
        logs.lines().count(),
        1,
        "routine acquire records must not become logs, got: {logs:?}"
    );
}

#[test]
fn json_logs_include_service_and_sandbox_context() {
    let output = CapturedLogs::default();
    let layer = tracing_subscriber::fmt::layer()
        .event_format(super::SpaceJsonFormat::with_context(
            super::JsonLogContext {
                service: "space-spaced-host".to_string(),
                environment: None,
                version: None,
                cluster_id: None,
                sandbox_id: Some("019eee45-9e12-7823-aef0-3ab147b88b6b".to_string()),
                boot_type: Some("snapshot".to_string()),
            },
        ))
        .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
        .with_writer(output.clone());
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!("spaced_host", request_id = "req-1");
        let _entered = span.enter();

        tracing::info!(pid = 1234_u64, "spaced host service started");
    });

    let line = output.contents();
    let value: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

    assert_eq!(value["service"], "space-spaced-host");
    assert_eq!(value["sandbox_id"], "019eee45-9e12-7823-aef0-3ab147b88b6b");
    assert_eq!(value["boot_type"], "snapshot");
    assert_eq!(value["message"], "spaced host service started");
    assert_eq!(value["pid"], 1234);
    assert_eq!(value["span"]["request_id"], "req-1");
    assert_eq!(value["span"]["name"], "spaced_host");
    assert!(value["timestamp"].as_str().is_some_and(|ts| !ts.is_empty()));
}

// The layer is deliberately built and used before init. This catches a
// formatter that caches Legacy too early, or an early policy lookup that pins
// a later OTLP process to the compatibility path.
#[cfg(feature = "otlp-metrics")]
#[tokio::test]
async fn logging_layers_follow_resolved_destinations_even_when_created_early() {
    const CHILD: &str = "SPACE_ATTRIBUTION_TEST_CHILD";
    if let Ok(expected) = std::env::var(CHILD) {
        let output = CapturedLogs::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(super::SpaceJsonFormat::new(
                    "producer",
                    super::LogContext::default(),
                ))
                .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                .with_writer(output.clone()),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let emit = || {
            tracing::dispatcher::with_default(&dispatch, || {
                let span = crate::with_peer_service!(tracing::info_span!(
                    "connection",
                    peer.service = "destination",
                ));
                let _entered = span.enter();
                tracing::info!(service = "logical-role", "diagnostic");
            })
        };
        emit();
        let guard = super::init("producer").unwrap();
        emit();
        guard.shutdown_async().await.unwrap();
        let records: Vec<serde_json::Value> = output
            .contents()
            .lines()
            .map(|line| {
                let mut record: serde_json::Value = serde_json::from_str(line).unwrap();
                record.as_object_mut().unwrap().remove("timestamp");
                record
            })
            .collect();
        let legacy = serde_json::json!({
            "level": "INFO",
            "message": "diagnostic",
            "service": "logical-role",
            "span": {"name": "connection", "service": "destination"},
            "target": "pplx_space_observability::log_filter_tests",
        });
        let otel = serde_json::json!({
            "level": "INFO",
            "message": "diagnostic",
            "service": "producer",
            "service.role": "logical-role",
            "span": {"name": "connection", "peer.service": "destination"},
            "target": "pplx_space_observability::log_filter_tests",
        });
        assert_eq!(
            records,
            vec![
                legacy.clone(),
                if expected == "otel" { otel } else { legacy }
            ]
        );
        return;
    }
    let mut cases = vec![("otlp", "off", "otel"), ("off", "off", "legacy")];
    if cfg!(feature = "otel") {
        cases.push(("off", "otlp", "otel"));
    }
    if cfg!(feature = "datadog") {
        cases.extend([
            ("datadog", "datadog", "legacy"),
            ("auto", "auto", "legacy"),
            ("otlp", "datadog", "legacy"),
            ("datadog", "otlp", "legacy"),
        ]);
    } else {
        cases.push(("auto", "off", "otel"));
    }
    for (metrics, traces, expected) in cases {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "log_filter_tests::logging_layers_follow_resolved_destinations_even_when_created_early", "--nocapture"])
            .env_clear()
            .env(CHILD, expected)
            .env("SPACE_METRICS_SINK", metrics)
            .env("SPACE_TRACE_EXPORTER", traces)
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
            .output().unwrap();
        assert!(
            output.status.success(),
            "metrics={metrics} traces={traces}: {output:?}"
        );
    }
}

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

struct CapturedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CapturedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
