//! Exercise process-global initialization and stdout as a collector would see it.
//! Subprocesses isolate environment resolution, disclosure gates, and subscribers.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use tracing::instrument::WithSubscriber as _;
use tracing_subscriber::layer::SubscriberExt as _;

use serde_json::Value;

#[test]
fn service_logging_is_portable_and_preserves_disclosure_boundaries() {
    struct Case {
        settings: &'static [(&'static str, &'static str)],
        service: &'static str,
        environment: Option<&'static str>,
        version: Option<&'static str>,
    }
    let cases = [
        Case {
            settings: &[],
            service: "fixture-service",
            environment: None,
            version: None,
        },
        Case {
            settings: &[
                ("SPACE_SERVICE_NAME", "neutral"),
                ("SPACE_ENV", "testing"),
                ("SPACE_VERSION", "neutral-revision"),
            ],
            service: "neutral",
            environment: Some("testing"),
            version: Some("neutral-revision"),
        },
        Case {
            settings: &[
                ("DD_SERVICE", "legacy"),
                ("DD_ENV", "testing"),
                ("DD_VERSION", "legacy-revision"),
            ],
            service: "legacy",
            environment: Some("testing"),
            version: Some("legacy-revision"),
        },
        Case {
            settings: &[
                ("SPACE_SERVICE_NAME", "neutral"),
                ("DD_SERVICE", "legacy"),
                ("OTEL_SERVICE_NAME", "otel"),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "deployment.environment.name=testing,service.version=otel-revision",
                ),
                ("SPACE_ENV", "prod"),
                ("DD_ENV", "testing"),
                ("SPACE_VERSION", "neutral-revision"),
                ("DD_VERSION", "legacy-revision"),
            ],
            service: "neutral",
            environment: Some("prod"),
            version: Some("neutral-revision"),
        },
        Case {
            settings: &[
                ("OTEL_SERVICE_NAME", "otel"),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.name=resource,deployment.environment.name=testing,service.version=otel-revision",
                ),
                ("DD_ENV", "prod"),
            ],
            service: "otel",
            environment: Some("testing"),
            version: Some("otel-revision"),
        },
        Case {
            settings: &[
                ("SPACE_SERVICE_NAME", " \n"),
                ("DD_SERVICE", "legacy"),
                ("SPACE_ENV", " "),
                ("DD_ENV", "testing"),
                ("SPACE_VERSION", ""),
                ("DD_VERSION", "legacy-revision"),
            ],
            service: "legacy",
            environment: Some("testing"),
            version: Some("legacy-revision"),
        },
        Case {
            settings: &[
                ("SPACE_SERVICE_NAME", " neutral "),
                ("DD_SERVICE", "legacy"),
                ("SPACE_ENV", " TESTING "),
                ("DD_ENV", "prod"),
                ("SPACE_VERSION", " neutral-revision "),
            ],
            service: "neutral",
            environment: Some("TESTING"),
            version: Some("neutral-revision"),
        },
        Case {
            settings: &[
                ("DD_SERVICE", "legacy"),
                ("DD_ENV", "prod"),
                ("DD_VERSION", "legacy-revision"),
            ],
            service: "legacy",
            environment: Some("prod"),
            version: Some("legacy-revision"),
        },
        Case {
            settings: &[
                ("SPACE_SERVICE_NAME", " "),
                ("DD_SERVICE", " \t"),
                ("SPACE_ENV", " "),
                ("DD_ENV", " "),
                ("SPACE_VERSION", " "),
                ("DD_VERSION", " "),
            ],
            service: "fixture-service",
            environment: None,
            version: None,
        },
    ];

    // Native Datadog must retain the pre-attribution wire contract even when
    // an OTLP endpoint is configured and both capabilities are compiled in.
    let mut modes = vec![("off", false)];
    if cfg!(feature = "otel") {
        modes.push(("otlp", true));
    }
    if cfg!(feature = "datadog") {
        modes.push(("datadog", false));
    }
    modes.push(("auto", cfg!(feature = "otel") && !cfg!(feature = "datadog")));

    // Each child is an independent re-exec of this binary that spends most of
    // its life in subscriber setup and shutdown, not in the events asserted
    // below. Start every child before reading any output, so the suite waits
    // out one child's fixed cost rather than one per case and context.
    let running: Vec<_> = cases
        .iter()
        .flat_map(|case| [false, true].map(|context| (case, context)))
        .flat_map(|(case, context)| {
            modes
                .iter()
                .map(move |&(sink, otel)| (case, context, sink, otel))
        })
        .map(|(case, context, sink, otel)| {
            let child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "logging_child", "--nocapture", "--quiet"])
                .env_clear()
                .envs(case.settings.iter().copied())
                .env(
                    "SPACE_LOGGING_TEST_CHILD",
                    if context { "context" } else { "plain" },
                )
                // Exercise the format selected by deployed containers without
                // requiring any neutral logging overrides.
                .env("KUBERNETES_SERVICE_HOST", "127.0.0.1")
                .env("SPACE_METRICS_SINK", sink)
                .env("SPACE_TRACE_EXPORTER", "off")
                .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
                .env("SPACE_CLUSTER_ID", "fixture-cluster")
                .env("SPACE_LOGGING_TEST_SERVICE", case.service)
                .env("RUST_LOG", "info")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            (case, context, sink, otel, child)
        })
        .collect();

    let mut failed = Vec::new();
    for (case, context, sink, otel, child) in running {
        let &Case {
            settings,
            service,
            environment,
            version,
        } = case;
        let output = child.wait_with_output().unwrap();
        // Each assertion below reports only the value that disagreed, and the
        // settings plus the context are what identify the child that produced
        // it. Collect that here instead of restating it in every message, and
        // report every failing child rather than only the first.
        let checked = catch_unwind(AssertUnwindSafe(|| {
            assert!(output.status.success(), "{output:?}");
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert!(!stdout.contains('\u{1b}'), "{stdout}");
            let records: Vec<Value> = stdout
                .lines()
                .filter(|line| line.starts_with('{'))
                .map(|line| serde_json::from_str(line).expect("one complete JSON object per line"))
                .collect();
            let record = records
                .iter()
                .find(|v| v["fixture"] == "ordinary")
                .expect("ordinary event");
            assert_eq!(record["service"], service);
            assert_eq!(record["env"].as_str(), environment);
            assert_eq!(record["version"].as_str(), version);
            assert_eq!(record["cluster_id"], "fixture-cluster");
            assert_eq!(record["level"], "INFO");
            assert_eq!(record["message"], "quoted \"message\"\nnext line");
            assert_eq!(record["count"], 7);
            assert_eq!(record["signed"], -7);
            assert_eq!(record["ratio"], 1.25);
            assert_eq!(record["ready"], true);
            assert_eq!(record["bytes"], "[01 02 03]");
            assert_eq!(record["span"]["operation.name"], "http.server");
            assert_eq!(record["span"]["name"], "fixture_span");
            assert_eq!(record["span"]["attempt"], 2);
            assert_eq!(record["span"]["ready"], true);
            record["timestamp"]
                .as_str()
                .unwrap()
                .parse::<jiff::Timestamp>()
                .unwrap();
            assert_eq!(
                record["sandbox_id"].as_str(),
                context.then_some("synthetic-sandbox")
            );
            assert_eq!(record["boot_type"].as_str(), context.then_some("snapshot"));
            let explicit = records.iter().find(|v| v["fixture"] == "explicit").unwrap();
            assert_eq!(
                explicit["service"],
                if otel { service } else { "event-service" }
            );
            assert_eq!(
                explicit.get("service.role"),
                otel.then_some(&Value::from("event-service"))
            );
            assert!(explicit.get("peer.service").is_none());
            assert_eq!(explicit["version"], 42);
            assert_eq!(explicit["sandbox_id"], "event-sandbox");
            let connection = records
                .iter()
                .find(|v| v["message"] == "exhausted UDS gRPC connect retry budget")
                .expect("failed peer connection diagnostic");
            assert_eq!(
                connection["service"],
                if otel { service } else { "fixture-peer" }
            );
            assert_eq!(
                connection.get("peer.service"),
                otel.then_some(&Value::from("fixture-peer"))
            );
            assert!(connection.get("service.role").is_none());
            assert!(record.get("peer.service").is_none());
            assert!(record.get("service.role").is_none());
            let equal = records
                .iter()
                .find(|v| v["fixture"] == "equal-service")
                .unwrap();
            assert_eq!(equal["service"], service);
            assert!(equal.get("service.role").is_none());
            let spans: Value = serde_json::from_str(
                records
                    .iter()
                    .find(|v| v["fixture"] == "connection-spans")
                    .unwrap()["captured"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
            // Complete creation attributes from the base connection span: no
            // extra role/peer keys are allowed in Legacy. Empty fields are not
            // recorded by tracing and remain absent from the capture.
            assert_eq!(
                spans,
                serde_json::json!([[
                    "grpc.client.connect",
                    [
                        ["otel.kind", "client"],
                        ["otel.name", "grpc.client.connect"],
                        ["operation.name", "grpc.client.connect"],
                        [
                            if otel { "peer.service" } else { "service" },
                            "fixture-peer"
                        ],
                        ["command", "Connect"],
                        ["transport", "uds"],
                        ["target", "/synthetic-missing-attribution.sock"],
                        ["retry", "0"]
                    ]
                ]])
            );
            assert!(records.iter().any(|v| v["fixture"] == "field-only"
                && v.get("message").is_none()
                && v["service"] == if otel { service } else { "legacy-role" }
                && v["service.role"] == "explicit-role"));
            assert!(
                records
                    .iter()
                    .any(|v| v["target"] == "space::startup" && v["service"] == service)
            );
            assert!(
                records
                    .iter()
                    .any(|v| v["target"] == "library::fixture"
                        && v["message"] == "library diagnostic")
            );
            assert_eq!(
                records
                    .iter()
                    .filter(|v| v["message"] == "failed gRPC request parameters")
                    .count(),
                usize::from(environment.is_some_and(|env| env.eq_ignore_ascii_case("testing")))
            );
            if let Some(details) = records
                .iter()
                .find(|v| v["message"] == "failed gRPC request parameters")
            {
                assert_eq!(details["target"], "grpc.server");
                assert_eq!(details["level"], "WARN");
                assert_eq!(details["grpc.request.params"], "{\"safe\":true}");
                assert_eq!(details["grpc.request.params_bytes"], 13);
                assert_eq!(details["grpc.request.params_truncated"], false);
            }
            assert!(
                records
                    .iter()
                    .any(|v| v["message"] == "slow acquire fixture")
            );
            assert!(!stdout.contains("suppressed fixture"));
        }));
        if checked.is_err() {
            failed.push(format!("{settings:?} (context: {context}, sink: {sink})"));
        }
    }
    assert!(failed.is_empty(), "logging children failed: {failed:#?}");
}

#[tokio::test]
async fn logging_child() {
    let Ok(mode) = std::env::var("SPACE_LOGGING_TEST_CHILD") else {
        return;
    };
    let context = if mode == "context" {
        pplx_space_common_svc::LogContext::default()
            .with_sandbox_id("synthetic-sandbox")
            .with_boot_type("snapshot")
    } else {
        pplx_space_common_svc::LogContext::default()
    };
    let _guard =
        pplx_space_common_svc::init_service_subprocess_with_log_context("fixture-service", context)
            .unwrap();
    let span = tracing::info_span!(
        "fixture_span",
        operation.name = "http.server",
        attempt = tracing::field::Empty,
        ready = true,
    );
    span.record("attempt", 2_u64);
    let _entered = span.enter();
    tracing::info!(
        fixture = "ordinary",
        count = 7_u64,
        signed = -7_i64,
        ratio = 1.25_f64,
        ready = true,
        bytes = &[1_u8, 2, 3][..],
        "quoted \"message\"\nnext line"
    );
    tracing::info!(
        fixture = "equal-service",
        service = std::env::var("SPACE_LOGGING_TEST_SERVICE").unwrap()
    );
    tracing::info!(
        fixture = "explicit",
        service = "event-service",
        version = 42_u64,
        sandbox_id = "event-sandbox",
        "explicit fields"
    );
    tracing::info!(
        fixture = "field-only",
        count = 1_u64,
        service = "legacy-role",
        service.role = "explicit-role",
    );
    let socket_dir = tempfile::tempdir().unwrap();
    pplx_space_common_grpc::connect_uds_channel_with_retry(
        "fixture-peer",
        &socket_dir.path().join("missing.sock"),
        pplx_space_common_grpc::GrpcRetryPolicy {
            max_retries: 0,
            ..Default::default()
        },
    )
    .await
    .expect_err("missing socket cannot accept a connection");
    let spans = CapturedConnectionSpans::default();
    pplx_space_common_grpc::connect_uds_channel_with_retry(
        "fixture-peer",
        std::path::Path::new("/synthetic-missing-attribution.sock"),
        pplx_space_common_grpc::GrpcRetryPolicy {
            max_retries: 0,
            ..Default::default()
        },
    )
    .with_subscriber(tracing_subscriber::registry().with(spans.clone()))
    .await
    .expect_err("synthetic socket cannot accept a connection");
    tracing::info!(
        fixture = "connection-spans",
        captured = serde_json::to_string(&*spans.0.lock().unwrap()).unwrap()
    );
    tracing::debug!(fixture = "debug", "suppressed fixture");
    // Libraries using the log facade must retain their original target.
    log::logger().log(
        &log::Record::builder()
            .args(format_args!("library diagnostic"))
            .level(log::Level::Warn)
            .target("library::fixture")
            .build(),
    );
    pplx_space_common_svc::log_failed_grpc_request_params(
        &tonic::Status::internal("synthetic failure"),
        "{\"safe\":true}",
    );
    pplx_space_common_svc::log_failed_grpc_request_params(
        &tonic::Status::internal("empty parameters"),
        "",
    );
    pplx_space_common_svc::log_failed_grpc_request_params(
        &tonic::Status::not_found("application status"),
        "{\"safe\":true}",
    );
    tracing::info!(target: "rusqlite::query", "suppressed fixture");
    tracing::info!(target: "sqlx::query", "suppressed fixture");
    tracing::info!(target: "sqlx::pool::acquire", "suppressed fixture");
    tracing::warn!(target: "sqlx::pool::acquire", "slow acquire fixture");
}

#[derive(Clone, Default)]
struct CapturedConnectionSpans(Arc<Mutex<Vec<CapturedConnectionSpan>>>);

type CapturedConnectionSpan = (
    String,
    pplx_space_observability::test_capture::CapturedEvent,
);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedConnectionSpans {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attrs.metadata().name() == "grpc.client.connect" {
            let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
            attrs.record(&mut visitor);
            self.0
                .lock()
                .expect("connection span capture lock")
                .push((attrs.metadata().name().to_string(), visitor.into_fields()));
        }
    }
}
