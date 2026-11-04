#![cfg(feature = "otlp-metrics")]

use std::net::UdpSocket;
use std::process::{Command, ExitStatus};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Metric, metric};
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

#[cfg(feature = "otel")]
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};

const CHILD_MODE_ENV: &str = "SPACE_OTLP_LIFECYCLE_TEST_CHILD";

#[derive(Debug, Default)]
struct Received {
    requests: usize,
    resource_attributes: Vec<(String, String)>,
    metrics: Vec<Metric>,
    #[cfg(feature = "otel")]
    spans: Vec<opentelemetry_proto::tonic::trace::v1::Span>,
}

#[derive(Clone, Debug, Default)]
struct CountingReceiver {
    received: Arc<Mutex<Received>>,
    request_arrived: Arc<Notify>,
}

#[tonic::async_trait]
impl MetricsService for CountingReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let request = request.into_inner();
        {
            let mut received = self
                .received
                .lock()
                .expect("request recorder lock poisoned");
            received.requests += 1;
            received
                .metrics
                .extend(request.resource_metrics.iter().flat_map(|resource| {
                    resource
                        .scope_metrics
                        .iter()
                        .flat_map(|scope| scope.metrics.clone())
                }));
            received.resource_attributes.extend(
                request
                    .resource_metrics
                    .iter()
                    .filter_map(|resource_metrics| resource_metrics.resource.as_ref())
                    .flat_map(|resource| &resource.attributes)
                    .filter_map(|attribute| {
                        let value = attribute.value.as_ref()?.value.as_ref()?;
                        match value {
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            value,
                            ) => Some((attribute.key.clone(), value.clone())),
                            _ => None,
                        }
                    }),
            );
        }
        self.request_arrived.notify_one();
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

struct Receiver {
    endpoint: String,
    count: CountingReceiver,
    shutdown: Option<oneshot::Sender<()>>,
}

#[cfg(feature = "otel")]
#[tonic::async_trait]
impl TraceService for CountingReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.received
            .lock()
            .expect("trace recorder lock poisoned")
            .spans
            .extend(
                request
                    .into_inner()
                    .resource_spans
                    .into_iter()
                    .flat_map(|resource| resource.scope_spans)
                    .flat_map(|scope| scope.spans),
            );
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

impl Receiver {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind lifecycle receiver");
        let endpoint = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("read lifecycle receiver address")
        );
        let count = CountingReceiver::default();
        let service = MetricsServiceServer::new(count.clone());
        #[cfg(feature = "otel")]
        let trace_service = TraceServiceServer::new(count.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let server = tonic::transport::Server::builder().add_service(service);
            #[cfg(feature = "otel")]
            let server = server.add_service(trace_service);
            server
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve lifecycle receiver");
        });
        Self {
            endpoint,
            count,
            shutdown: Some(shutdown_tx),
        }
    }

    fn request_count(&self) -> usize {
        self.count
            .received
            .lock()
            .expect("request recorder lock poisoned")
            .requests
    }

    fn resource_attribute_values(&self, key: &str) -> Vec<String> {
        self.count
            .received
            .lock()
            .expect("request recorder lock poisoned")
            .resource_attributes
            .iter()
            .filter(|(attribute_key, _)| attribute_key == key)
            .map(|(_, value)| value.clone())
            .collect()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn run_child_with_sink(mode: &str, endpoint: &str, hostname: &str, sink: &str) -> ExitStatus {
    child_command(mode, endpoint, hostname, Some(sink))
        .status()
        .expect("run lifecycle fixture child")
}

fn child_command(mode: &str, endpoint: &str, hostname: &str, sink: Option<&str>) -> Command {
    let mut command =
        Command::new(std::env::current_exe().expect("resolve lifecycle test executable"));
    command
        .arg("--exact")
        .arg("otlp_process_fixture_child")
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, mode)
        .env_remove("SPACE_METRICS_SINK")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_HEADERS")
        .env_remove("OTEL_EXPORTER_OTLP_HEADERS")
        // A developer shell often exports an http/protobuf collector address,
        // whose /v1/traces path the grpc profile rejects. The fixture supplies
        // its own endpoint, so clear the inherited trace settings too.
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
        .env_remove("SPACE_TRACE_EXPORTER")
        .env_remove("SPACE_SERVICE_NAME")
        .env_remove("SPACE_ENV")
        .env_remove("SPACE_VERSION")
        .env_remove("OTEL_SERVICE_NAME")
        .env_remove("OTEL_RESOURCE_ATTRIBUTES")
        .env_remove("SPACE_TRACE_FILTER")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
        .env("DD_AGENT_HOST", "127.0.0.1")
        .env("DD_SERVICE", "space-sandbox-manager-canary")
        .env("DD_ENV", "experimental")
        .env("DD_VERSION", "test-sha")
        .env("RUST_LOG", "info")
        .env("SPACE_LOG_FORMAT", "json")
        .env("SPACE_CLUSTER_ID", "test-cluster")
        .env("KUBERNETES_SERVICE_HOST", "10.0.0.1")
        .env("SPACE_K8S_POD_NAME", format!("{hostname}-pod"))
        .env_remove("SPACE_K8S_NODE_NAME")
        .env_remove("KUBE_NODE_NAME")
        .env("HOSTNAME", hostname);
    if let Some(sink) = sink {
        command.env("SPACE_METRICS_SINK", sink);
    }
    command
}

fn run_child(mode: &str, endpoint: &str, hostname: &str) -> ExitStatus {
    run_child_with_sink(mode, endpoint, hostname, "otlp")
}

#[test]
fn otlp_process_fixture_child() {
    let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
        return;
    };
    if mode == "no-runtime" {
        let _guard = pplx_space_observability::init("sandbox-manager")
            .expect("initialize synchronous fixture observability");
        pplx_space_observability::counter("lifecycle.fixture", 1, &[]);
        return;
    }
    let runtime = tokio::runtime::Runtime::new().expect("build fixture Tokio runtime");
    let _runtime_context = runtime.enter();
    let guard = pplx_space_observability::init("sandbox-manager")
        .expect("initialize fixture observability");
    pplx_space_observability::counter("lifecycle.fixture", 1, &[]);
    if mode == "selection" {
        pplx_space_observability::gauge("lifecycle.fixture.gauge", 0.0, &[]);
        pplx_space_observability::distribution("lifecycle.fixture.depth", 2.5, &[]);
        pplx_space_observability::timing_ms("lifecycle.fixture.latency", 12.5, &[]);
    }
    if mode == "filters" {
        let info = tracing::info_span!("filter_info");
        let _entered = info.enter();
        tracing::info!(fixture = "filter_info", "visible info");
        tracing::warn!(fixture = "filter_warn", "visible warn");
        let warn = tracing::warn_span!("filter_warn");
        drop(warn);
        // A disabled callsite must not even evaluate its field values.
        let evaluations = std::cell::Cell::new(0);
        let evaluate = || {
            evaluations.set(evaluations.get() + 1);
            1_u64
        };
        tracing::debug!(
            fixture = "filter_debug",
            value = evaluate(),
            "visible debug"
        );
        let debug = tracing::debug_span!("filter_debug", value = evaluate());
        drop(debug);
        // Field values must not be evaluated for a disabled callsite. Both
        // callsites above evaluate exactly once when DEBUG is enabled.
        let expected = if std::env::var_os("SPACE_TRACE_FIXTURE_DEBUG").is_some() {
            2
        } else {
            0
        };
        assert_eq!(
            evaluations.get(),
            expected,
            "disabled DEBUG callsites must short-circuit their fields"
        );
        tracing::info!(target: "sqlx::pool::acquire", acquired_after_secs = 0.001_f64);
        drop(_entered);
        drop(info);
        // SQLx's slow-acquire record. It is the only per-pod signal for a
        // saturated pool, so it has to reach logs at any verbosity. Emitted
        // outside the fixture spans, where the bridge has no parent to hang a
        // second acquire span from, so this row exercises logs alone.
        tracing::warn!(
            target: "sqlx::pool::acquire",
            acquired_after_secs = 2.5_f64,
            fixture_slow_acquire = true,
        );
    }
    if mode == "abrupt" {
        std::process::exit(17);
    }
    drop(guard);
}

#[cfg(feature = "otel")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trace_filter_defaults_to_log_verbosity_until_it_is_set() {
    const INFO: (&str, &str) = ("INFO", "visible info");
    const WARN: (&str, &str) = ("WARN", "visible warn");
    const DEBUG: (&str, &str) = ("DEBUG", "visible debug");
    // The acquire span is emitted by the SQL bridge under its own target, so
    // it follows the filter's base level rather than the acquire directive
    // that keeps the pool-acquire metric enabled.
    const ACQUIRE: &str = "db.pool.acquire";
    // An unset trace filter keeps the filter RUST_LOG produced before trace
    // selection was separated, so quieting logs still quiets spans. Setting
    // the trace filter opts into an independent selection in either direction.
    for (log_filter, trace_filter, expected_logs, expected_spans) in [
        (
            None,
            None,
            vec![INFO, WARN],
            vec![ACQUIRE, "filter_info", "filter_warn"],
        ),
        (Some("warn"), None, vec![WARN], vec!["filter_warn"]),
        (Some("error"), None, vec![], vec![]),
        (
            Some("debug"),
            None,
            vec![INFO, WARN, DEBUG],
            vec![ACQUIRE, "filter_debug", "filter_info", "filter_warn"],
        ),
        // The shared layout has to honour an acquire rule too. `RUST_LOG` is
        // the trace selection when no trace filter is set, so a refusal there
        // is a refusal of the span — while the metric below still counts both
        // waits, and the slow-acquire warning still reaches logs.
        (
            Some("info,sqlx::pool::acquire=off"),
            None,
            vec![INFO, WARN],
            vec!["filter_info", "filter_warn"],
        ),
        // A rule on a parent module reaches the target here as well.
        (
            Some("info,sqlx::pool=off"),
            None,
            vec![INFO, WARN],
            vec!["filter_info", "filter_warn"],
        ),
        (None, Some("warn"), vec![INFO, WARN], vec!["filter_warn"]),
        (
            Some("error"),
            Some("info"),
            vec![],
            vec![ACQUIRE, "filter_info", "filter_warn"],
        ),
        (Some("error"), Some("off"), vec![], vec![]),
        // The forced acquire directive keeps the metric enabled; it must not
        // override an explicit refusal of the span.
        (
            Some("error"),
            Some("info,sqlx::pool::acquire=off"),
            vec![],
            vec!["filter_info", "filter_warn"],
        ),
        // A dynamic directive alongside the refusal must not re-enable the
        // span. The span rule never names the acquire target, and one
        // unparsable-as-static rule used to discard the whole selection. The
        // rule stays at info so this row tests that and not EnvFilter's
        // separate habit of raising level interest for a dynamic directive.
        (
            Some("error"),
            Some("info,sqlx::pool::acquire=off,[unrelated_span]=info"),
            vec![],
            vec!["filter_info", "filter_warn"],
        ),
        // And the refusal must not leak into an unrelated dynamic selection:
        // with no acquire rule the span is still wanted.
        (
            Some("error"),
            Some("info,[unrelated_span]=info"),
            vec![],
            vec![ACQUIRE, "filter_info", "filter_warn"],
        ),
        // A rule on a parent module reaches this target, as it does in the
        // filter itself, so the span is refused without naming it directly.
        (
            Some("error"),
            Some("info,sqlx::pool=off"),
            vec![],
            vec!["filter_info", "filter_warn"],
        ),
        // A dynamic directive that *enables* a span must keep the pool wait
        // inside it. The filter's default excludes everything, so vetoing on
        // that default would hide the acquire span in exactly the spans the
        // operator selected by name. `filter_warn` rides along because a span
        // directive enables what is recorded within its scope too.
        (
            Some("error"),
            Some("off,[filter_info]=info"),
            vec![],
            vec![ACQUIRE, "filter_info", "filter_warn"],
        ),
    ] {
        let receiver = Receiver::start().await;
        let mut command = child_command(
            "filters",
            &receiver.endpoint,
            "filter-fixture",
            Some("otlp"),
        );
        command
            .env_remove("RUST_LOG")
            .env("SPACE_TRACE_EXPORTER", "otlp")
            .env("OTEL_TRACES_SAMPLER", "always_on");
        if let Some(filter) = log_filter {
            command.env("RUST_LOG", filter);
        }
        if let Some(filter) = trace_filter {
            command.env("SPACE_TRACE_FILTER", filter);
        }
        // Fields evaluate when DEBUG is enabled, and also when a span or field
        // directive is present: EnvFilter then cannot statically exclude a
        // callsite, so it asks about every one. That is the cost of a dynamic
        // directive, not a defect in the selection.
        if expected_spans.contains(&"filter_debug")
            || trace_filter.is_some_and(|filter| filter.contains('['))
        {
            command.env("SPACE_TRACE_FIXTURE_DEBUG", "1");
        }
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let logs: Vec<serde_json::Value> = stdout
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|record: &serde_json::Value| record.get("fixture").is_some())
            .collect();
        let actual: Vec<_> = logs
            .iter()
            .map(|record| {
                (
                    record["level"].as_str().unwrap(),
                    record["message"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            actual, expected_logs,
            "log={log_filter:?}, trace={trace_filter:?}"
        );
        assert!(
            stdout.contains("fixture_slow_acquire"),
            "the slow-acquire warning must reach logs at log={log_filter:?}, \
             trace={trace_filter:?}:\n{stdout}"
        );
        let received = receiver.count.received.lock().unwrap();
        let mut names: Vec<_> = received
            .spans
            .iter()
            .map(|span| span.name.as_str())
            // A RUST_LOG threshold of debug enables dependency spans too, so
            // assert the fixture's own spans rather than its libraries'.
            .filter(|name| *name == ACQUIRE || name.starts_with("filter_"))
            .collect();
        names.sort_unstable();
        assert_eq!(
            names, expected_spans,
            "log={log_filter:?}, trace={trace_filter:?}"
        );
        let acquire = received
            .metrics
            .iter()
            .find(|metric| metric.name == "space.db.pool.acquire_total")
            .expect("pool acquire metric regardless of filters");
        let Some(metric::Data::ExponentialHistogram(histogram)) = &acquire.data else {
            panic!("acquire histogram");
        };
        // Both the routine record and the slow-acquire warning feed it.
        assert_eq!(
            histogram
                .data_points
                .iter()
                .map(|point| point.count)
                .sum::<u64>(),
            2
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_process_exit_flushes_the_final_delta() {
    let receiver = Receiver::start().await;
    assert!(run_child("graceful", &receiver.endpoint, "fixture-one").success());
    assert_eq!(receiver.request_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abrupt_process_exit_does_not_wait_for_or_fabricate_an_export() {
    let receiver = Receiver::start().await;
    let request_arrived = receiver.count.request_arrived.notified();
    assert_eq!(
        run_child("abrupt", &receiver.endpoint, "fixture-abrupt").code(),
        Some(17)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request_arrived)
            .await
            .is_err(),
        "abrupt exit unexpectedly exported the current interval"
    );
    assert_eq!(receiver.request_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_process_producers_keep_distinct_resource_identity() {
    let receiver = Receiver::start().await;
    for hostname in ["fixture-a", "fixture-b"] {
        assert!(child_command("graceful", &receiver.endpoint, hostname, Some("otlp"))
            .env("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=shared,host.name=shared,k8s.node.name=shared,k8s.pod.name=shared,team=infra")
            .status().unwrap().success());
    }
    let mut instances = receiver.resource_attribute_values("service.instance.id");
    instances.sort_unstable();
    assert_eq!(instances.len(), 2);
    assert!(instances[0].starts_with("fixture-a:"));
    assert!(instances[1].starts_with("fixture-b:"));
    assert_ne!(instances[0], instances[1]);

    let mut pods = receiver.resource_attribute_values("k8s.pod.name");
    pods.sort_unstable();
    assert_eq!(pods, ["fixture-a-pod", "fixture-b-pod"]);
    assert!(receiver.resource_attribute_values("host.name").is_empty());
    assert!(
        receiver
            .resource_attribute_values("k8s.node.name")
            .is_empty()
    );
    assert_eq!(
        receiver.resource_attribute_values("team"),
        ["infra", "infra"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_process_mode_emits_no_otlp_request() {
    let receiver = Receiver::start().await;
    assert!(run_child_with_sink("graceful", &receiver.endpoint, "fixture-off", "off").success());
    assert_eq!(receiver.request_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_selection_exports_only_to_the_selected_metrics_sink() {
    // Every child is given an OTLP endpoint, so auto resolves Datadog when
    // compiled in and OTLP otherwise.
    let default_sink = if cfg!(feature = "datadog") {
        "datadog"
    } else if cfg!(feature = "otlp-metrics") {
        "otlp"
    } else {
        "off"
    };
    let mut cases = vec![
        (None, "selection", default_sink),
        (Some("auto"), "selection", default_sink),
        (Some("otlp"), "selection", "otlp"),
        (Some("off"), "selection", "off"),
        // A valid endpoint the exporter cannot use at runtime still starts.
        (Some("otlp"), "no-runtime", "off"),
    ];
    if cfg!(feature = "datadog") {
        cases.push((Some("datadog"), "selection", "datadog"));
    }
    for (sink, mode, expected) in cases {
        let receiver = Receiver::start().await;
        let udp = UdpSocket::bind("127.0.0.1:0").expect("bind native receiver");
        udp.set_nonblocking(true).expect("nonblocking receiver");
        let mut command = child_command(mode, &receiver.endpoint, "selection-fixture", sink);
        command.env(
            "DD_DOGSTATSD_PORT",
            udp.local_addr().unwrap().port().to_string(),
        );
        let output =
            tokio::task::spawn_blocking(move || command.output().expect("run selection fixture"))
                .await
                .expect("join selection fixture");
        assert!(output.status.success(), "{sink:?}/{mode}: {output:?}");
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 fixture output");
        assert!(
            stdout.contains(&format!("\"metrics_sink\":\"{expected}\"")),
            "{sink:?}/{mode}: {stdout}"
        );

        // UDP sends are synchronous and the child has exited. The local socket
        // queue is complete; a nonblocking drain also detects extra emissions.
        let mut payloads = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            match udp.recv(&mut buffer) {
                Ok(length) => payloads.push(String::from_utf8(buffer[..length].to_vec()).unwrap()),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("native receiver: {error}"),
            }
        }
        payloads.sort_unstable();
        if expected == "datadog" {
            let tags = "env:experimental,service:space-sandbox-manager-canary,version:test-sha,cluster_id:test-cluster";
            let mut wanted = [
                "space.lifecycle.fixture:1|c",
                "space.lifecycle.fixture.gauge:0.000|g",
                "space.lifecycle.fixture.depth:2.500|d",
                "space.lifecycle.fixture.latency:12.500|d",
            ]
            .map(|sample| format!("{sample}|#{tags}"));
            wanted.sort_unstable();
            assert_eq!(payloads, wanted, "{sink:?}/{mode}");
        } else {
            assert!(
                payloads.is_empty(),
                "{sink:?}/{mode} leaked to DogStatsD: {payloads:?}"
            );
        }
        if expected == "otlp" {
            let received = receiver.count.received.lock().unwrap();
            assert_eq!(received.requests, 1);
            assert_eq!(received.metrics.len(), 4);
            for (name, unit, value) in [
                // A count, a plain gauge and a depth are not dimensionless;
                // only the latency carries a unit.
                ("space.lifecycle.fixture", "", 1.0),
                ("space.lifecycle.fixture.gauge", "", 0.0),
                ("space.lifecycle.fixture.depth", "", 2.5),
                ("space.lifecycle.fixture.latency", "ms", 12.5),
            ] {
                let metric = received
                    .metrics
                    .iter()
                    .find(|metric| metric.name == name)
                    .unwrap();
                assert_eq!(metric.unit, unit);
                match metric.data.as_ref().unwrap() {
                    metric::Data::Sum(sum) if name == "space.lifecycle.fixture" => {
                        assert!(sum.is_monotonic);
                        assert_eq!(
                            sum.aggregation_temporality,
                            AggregationTemporality::Delta as i32
                        );
                        assert_eq!(sum.data_points.len(), 1);
                        assert_eq!(sum.data_points[0].value, Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(1)));
                    }
                    metric::Data::Gauge(gauge) if name.ends_with(".gauge") => {
                        assert_eq!(gauge.data_points.len(), 1);
                        assert_eq!(gauge.data_points[0].value, Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(value)));
                    }
                    metric::Data::ExponentialHistogram(histogram)
                        if name.ends_with(".depth") || name.ends_with(".latency") =>
                    {
                        assert_eq!(
                            histogram.aggregation_temporality,
                            AggregationTemporality::Delta as i32
                        );
                        assert_eq!(histogram.data_points.len(), 1);
                        assert_eq!(histogram.data_points[0].count, 1);
                        assert_eq!(histogram.data_points[0].sum, Some(value));
                    }
                    other => panic!("unexpected metric shape for {name}: {other:?}"),
                }
            }
        } else {
            assert_eq!(
                receiver.request_count(),
                0,
                "{sink:?}/{mode} leaked to OTLP"
            );
        }
    }
}

/// journald is no longer a metrics sink. A deployment that still names it must
/// fail startup instead of running with metrics silently off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_journald_selector_fails_startup() {
    let receiver = Receiver::start().await;
    let mut command = child_command(
        "selection",
        &receiver.endpoint,
        "journald-fixture",
        Some("journald"),
    );
    let output =
        tokio::task::spawn_blocking(move || command.output().expect("run journald fixture"))
            .await
            .expect("join journald fixture");
    assert!(
        !output.status.success(),
        "SPACE_METRICS_SINK=journald must fail startup: {output:?}"
    );
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 fixture stderr");
    assert!(
        stderr.contains(r#"InvalidMetricsSink { raw: "journald" }"#),
        "error must name the rejected selector, got {stderr}"
    );
    assert_eq!(receiver.request_count(), 0);
}

/// A selector that can never work must fail telemetry initialization, so a
/// misconfigured deployment is caught at startup rather than running blind.
#[cfg(feature = "otlp-metrics")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_otlp_metrics_configuration_fails_initialization() {
    const SECRET: &str = "fixture-header-secret";
    // The rule's own rows live in the unit tables. A process is needed only to
    // prove that a rejection reaches startup at all, and that a rejected
    // header never reaches the diagnostic.
    for (name, value, setting) in [
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "", "endpoint"),
        ("OTEL_EXPORTER_OTLP_HEADERS", SECRET, "headers"),
    ] {
        let receiver = Receiver::start().await;
        let mut command = child_command(
            "selection",
            &receiver.endpoint,
            "invalid-fixture",
            Some("otlp"),
        );
        command.env(name, value);
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(
            !output.status.success(),
            "{name}={value:?} must fail initialization: {output:?}"
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains(setting),
            "{name}: error must name the setting, got {stderr}"
        );
        // The diagnostic names the setting; it never echoes a header value.
        assert!(
            !stderr.contains(SECRET),
            "{name}: error leaked a configured value"
        );
    }
}

/// A valid endpoint that nothing is listening on is a runtime condition, not a
/// configuration error: the process starts and only warns.
#[cfg(feature = "otlp-metrics")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_otlp_endpoint_starts_and_warns() {
    let mut command = child_command(
        "selection",
        "http://127.0.0.1:1",
        "unreachable-fixture",
        Some("otlp"),
    );
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "an unreachable receiver must not fail startup: {output:?}"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("\"metrics_sink\":\"otlp\""),
        "the selected sink must still be installed: {stdout}"
    );
}

/// `auto` must not stop a process from starting over a destination the
/// operator never named, and an explicit selector must still fail.
///
/// Only reachable without the native adapter: with Datadog compiled in, `auto`
/// resolves Datadog for both signals and never reaches an OTLP configuration.
#[cfg(all(feature = "otel", not(feature = "datadog")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_tolerates_an_unusable_endpoint_and_explicit_does_not() {
    // A path is the http/protobuf convention, so this is a mistake an operator
    // really makes. The grpc profile rejects it for both signals.
    const UNUSABLE: &str = "http://127.0.0.1:4317/v1/metrics";

    let command = child_command("graceful", UNUSABLE, "auto-fixture", None);
    let output = tokio::task::spawn_blocking(move || {
        let mut command = command;
        command.output().expect("run auto fixture")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "auto must not block startup: {output:?}"
    );
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // Both signals degraded, and each said why.
    assert!(logs.contains("\"metrics_sink\":\"off\""), "{logs}");
    assert!(
        logs.contains("auto selected an unusable exporter"),
        "{logs}"
    );
    assert!(logs.contains("metrics disabled"), "{logs}");
    assert_eq!(
        logs.matches("must not include a path").count(),
        2,
        "both signals report the reason: {logs}"
    );
    // The reason names the settings, never the endpoint.
    assert!(!logs.contains("/v1/metrics"), "{logs}");

    for selector in ["SPACE_METRICS_SINK", "SPACE_TRACE_EXPORTER"] {
        let mut command = child_command("graceful", UNUSABLE, "explicit-fixture", None);
        command.env(selector, "otlp");
        let output =
            tokio::task::spawn_blocking(move || command.output().expect("run explicit fixture"))
                .await
                .unwrap();
        assert!(
            !output.status.success(),
            "{selector}=otlp must fail startup: {output:?}"
        );
    }
}
