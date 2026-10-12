use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AttributeValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, Metric as WireMetric};
use opentelemetry_sdk::metrics::Temporality;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use super::{
    EXPORT_INTERVAL, EXPORT_TIMEOUT, HISTOGRAM_MAX_SCALE, Metric, MetricKind, MetricSink,
    MetricUnit, MetricValue, OtlpMetricsConfig, OtlpMetricsInitError, OtlpSink, SHUTDOWN_TIMEOUT,
    endpoint_from_values, export_interval_from_value, temporality_from_value,
    validate_plaintext_header_values, validate_protocol_values,
};

#[derive(Clone, Debug, Default)]
struct RecordingReceiver {
    requests: Arc<Mutex<Vec<ExportMetricsServiceRequest>>>,
    failures_remaining: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl MetricsService for RecordingReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.requests
            .lock()
            .expect("OTLP request lock poisoned")
            .push(request.into_inner());
        if self
            .failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(Status::unavailable("intentional fixture failure"));
        }
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

struct TestReceiver {
    address: SocketAddr,
    recorder: RecordingReceiver,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestReceiver {
    async fn start() -> Self {
        Self::start_with_failures(0).await
    }

    async fn start_with_failures(failures: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OTLP test receiver");
        let address = listener.local_addr().expect("read receiver address");
        let recorder = RecordingReceiver::default();
        recorder
            .failures_remaining
            .store(failures, Ordering::Release);
        let service = MetricsServiceServer::new(recorder.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve OTLP test receiver");
        });
        Self {
            address,
            recorder,
            shutdown: Some(shutdown_tx),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn take_requests(&self) -> Vec<ExportMetricsServiceRequest> {
        std::mem::take(
            &mut *self
                .recorder
                .requests
                .lock()
                .expect("OTLP request lock poisoned"),
        )
    }

    fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for TestReceiver {
    fn drop(&mut self) {
        self.stop();
    }
}

fn test_sink(endpoint: String) -> OtlpSink {
    test_sink_with_limits(endpoint, 2_000, 1_024)
}

fn test_sink_with_limits(
    endpoint: String,
    attribute_cardinality_limit: usize,
    instrument_name_limit: usize,
) -> OtlpSink {
    OtlpSink::new(test_config(
        endpoint,
        attribute_cardinality_limit,
        instrument_name_limit,
    ))
    .expect("build OTLP test sink")
}

fn test_config(
    endpoint: String,
    attribute_cardinality_limit: usize,
    instrument_name_limit: usize,
) -> OtlpMetricsConfig {
    OtlpMetricsConfig {
        endpoint,
        identity: crate::identity::Identity::from_lookup("space-sandbox-manager-canary", |key| {
            match key {
                "DD_ENV" => Some("experimental".into()),
                "DD_VERSION" => Some("test-sha".into()),
                _ => None,
            }
        }),
        constant_tags: vec![
            "env:experimental".to_string(),
            "service:space-sandbox-manager-canary".to_string(),
            "version:test-sha".to_string(),
            "cluster_id:test-cluster".to_string(),
        ],
        export_interval: Duration::from_secs(3_600),
        temporality: Temporality::Delta,
        export_timeout: Duration::from_millis(500),
        attribute_cardinality_limit,
        instrument_name_limit,
    }
}

fn emit(sink: &OtlpSink, name: &str, kind: MetricKind, value: MetricValue, tags: &[&str]) {
    emit_with_unit(sink, name, kind, MetricUnit::Inferred, value, tags);
}

fn emit_with_unit(
    sink: &OtlpSink,
    name: &str,
    kind: MetricKind,
    unit: MetricUnit,
    value: MetricValue,
    tags: &[&str],
) {
    let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_string()).collect();
    sink.emit(Metric {
        name,
        kind,
        unit,
        value,
        tags: &tags,
    });
}

fn metrics(requests: &[ExportMetricsServiceRequest]) -> Vec<&WireMetric> {
    requests
        .iter()
        .flat_map(|request| &request.resource_metrics)
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .collect()
}

fn metric<'a>(requests: &'a [ExportMetricsServiceRequest], name: &str) -> &'a WireMetric {
    metrics(requests)
        .into_iter()
        .find(|metric| metric.name == name)
        .unwrap_or_else(|| panic!("missing metric {name}"))
}

fn attribute<'a>(
    attributes: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    name: &str,
) -> Option<&'a AttributeValue> {
    attributes
        .iter()
        .find(|attribute| attribute.key == name)
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| value.value.as_ref())
}

fn sum_values(metric: &WireMetric) -> Vec<i64> {
    let Data::Sum(sum) = metric.data.as_ref().expect("metric data missing") else {
        panic!("{} was not an OTLP sum", metric.name);
    };
    assert_eq!(
        sum.aggregation_temporality,
        AggregationTemporality::Delta as i32
    );
    assert!(sum.is_monotonic);
    sum.data_points
        .iter()
        .map(|point| match point.value {
            Some(Value::AsInt(value)) => value,
            _ => panic!("counter was not encoded as an OTLP int64"),
        })
        .collect()
}

fn gauge_values(metric: &WireMetric) -> Vec<f64> {
    let Data::Gauge(gauge) = metric.data.as_ref().expect("metric data missing") else {
        panic!("{} was not an OTLP gauge", metric.name);
    };
    gauge
        .data_points
        .iter()
        .map(|point| match point.value {
            Some(Value::AsDouble(value)) => value,
            _ => panic!("gauge was not encoded as an OTLP double"),
        })
        .collect()
}

#[test]
fn initialization_without_a_tokio_runtime_fails_without_panicking() {
    let result = OtlpSink::new(test_config(
        "http://127.0.0.1:4317".to_string(),
        2_000,
        1_024,
    ));
    assert!(matches!(result, Err(OtlpMetricsInitError::MissingRuntime)));
}

#[test]
fn signal_specific_otlp_configuration_has_defined_precedence() {
    assert_eq!(
        endpoint_from_values(
            Some("http://metrics:4317".to_string()),
            Some("http://general:4317".to_string()),
        )
        .unwrap(),
        "http://metrics:4317"
    );
    assert_eq!(
        endpoint_from_values(
            Some("  ".to_string()),
            Some("http://general:4317".to_string()),
        )
        .unwrap(),
        "http://general:4317"
    );
    assert!(matches!(
        endpoint_from_values(None, Some("https://secure:4317".to_string())),
        Err(OtlpMetricsInitError::UnsupportedEndpoint { .. })
    ));
    assert!(matches!(
        endpoint_from_values(None, None),
        Err(OtlpMetricsInitError::MissingEndpoint)
    ));
    // Metrics and traces apply one endpoint rule, so a typo that either
    // producer would refuse is refused here too.
    for endpoint in [
        "http://:",
        "http://user:pw@collector:4317",
        "http://collector:0",
        "http://collector:4317/path?q#f",
    ] {
        assert!(
            matches!(
                endpoint_from_values(Some(endpoint.to_string()), None),
                Err(OtlpMetricsInitError::UnsupportedEndpoint { .. })
            ),
            "{endpoint}"
        );
    }

    validate_protocol_values(Some("grpc".to_string()), Some("http/protobuf".to_string())).unwrap();
    validate_protocol_values(Some(" ".to_string()), Some("GRPC".to_string())).unwrap();
    assert!(matches!(
        validate_protocol_values(None, Some("http/protobuf".to_string())),
        Err(OtlpMetricsInitError::UnsupportedProtocol { .. })
    ));

    validate_plaintext_header_values(Some(" ".to_string()), None).unwrap();
    assert!(matches!(
        validate_plaintext_header_values(Some("authorization=Bearer%20secret".to_string()), None,),
        Err(OtlpMetricsInitError::HeadersOnPlaintextEndpoint)
    ));
    assert!(matches!(
        validate_plaintext_header_values(None, Some("x-api-key=secret".to_string()),),
        Err(OtlpMetricsInitError::HeadersOnPlaintextEndpoint)
    ));

    assert_eq!(export_interval_from_value(None).unwrap(), EXPORT_INTERVAL);
    assert_eq!(
        export_interval_from_value(Some("  ".to_string())).unwrap(),
        EXPORT_INTERVAL
    );
    assert_eq!(
        export_interval_from_value(Some(" 250 ".to_string())).unwrap(),
        Duration::from_millis(250)
    );
    // An unusable interval must disable the sink, never quietly restore the
    // default cadence an operator was trying to replace.
    for raw in ["0", "-1", "1.5", "10s", "abc"] {
        assert!(
            matches!(
                export_interval_from_value(Some(raw.to_string())),
                Err(OtlpMetricsInitError::InvalidExportInterval { .. })
            ),
            "{raw} must be rejected"
        );
    }

    // Delta unless a deployment asks otherwise: the SDK's own default,
    // cumulative, is what the native Datadog intake cannot take.
    assert_eq!(temporality_from_value(None).unwrap(), Temporality::Delta);
    assert_eq!(
        temporality_from_value(Some(" ".to_string())).unwrap(),
        Temporality::Delta
    );
    for (raw, expected) in [
        ("cumulative", Temporality::Cumulative),
        (" Delta ", Temporality::Delta),
        ("LOWMEMORY", Temporality::LowMemory),
    ] {
        assert_eq!(
            temporality_from_value(Some(raw.to_string())).unwrap(),
            expected,
            "{raw}"
        );
    }
    for raw in ["low_memory", "cumulative,delta", "0"] {
        assert!(
            matches!(
                temporality_from_value(Some(raw.to_string())),
                Err(OtlpMetricsInitError::InvalidTemporality { .. })
            ),
            "{raw} must be rejected"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_wire_types_temporality_precision_and_identity_are_explicit() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink(receiver.endpoint());
    let precise = (1_u64 << 53) + 1;

    emit(
        &sink,
        "sandbox.worker.lifecycle.count",
        MetricKind::Counter,
        MetricValue::Unsigned(precise),
        &["method:create", "status:success"],
    );
    emit(
        &sink,
        "sandbox.worker.cleanup.queue_depth",
        MetricKind::Gauge,
        MetricValue::Float(-1.25),
        &[],
    );
    for value in [0.0, 1.0, 64.0] {
        emit(
            &sink,
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            MetricKind::Distribution,
            MetricValue::Float(value),
            &["method:start"],
        );
    }
    sink.force_flush().expect("flush wire semantics");

    let requests = receiver.take_requests();
    let counter = metric(&requests, "space.sandbox.worker.lifecycle.count");
    assert_eq!(sum_values(counter), vec![precise as i64]);
    assert_eq!(counter.unit, "", "a count declares no unit");
    let Data::Sum(sum) = counter.data.as_ref().expect("counter data missing") else {
        unreachable!()
    };
    assert_eq!(
        attribute(&sum.data_points[0].attributes, "method"),
        Some(&AttributeValue::StringValue("create".to_string()))
    );
    assert_eq!(
        attribute(&sum.data_points[0].attributes, "cluster_id"),
        Some(&AttributeValue::StringValue("test-cluster".to_string()))
    );
    assert!(attribute(&sum.data_points[0].attributes, "env").is_none());
    assert!(attribute(&sum.data_points[0].attributes, "version").is_none());
    assert!(
        sum.data_points
            .iter()
            .all(|point| attribute(&point.attributes, "service_instance_id").is_none()),
        "process identity belongs on the resource, not every metric point"
    );

    let gauge = metric(&requests, "space.sandbox.worker.cleanup.queue_depth");
    assert_eq!(gauge_values(gauge), vec![-1.25]);
    assert_eq!(gauge.unit, "", "a queue depth declares no unit");

    let histogram = metric(&requests, "space.sandbox.worker.spaced_bridge.rpc_latency");
    assert_eq!(histogram.unit, "ms");
    let Data::ExponentialHistogram(histogram) =
        histogram.data.as_ref().expect("histogram data missing")
    else {
        panic!("distribution was not encoded as an exponential histogram");
    };
    assert_eq!(
        histogram.aggregation_temporality,
        AggregationTemporality::Delta as i32
    );
    let point = &histogram.data_points[0];
    assert_eq!(point.count, 3);
    assert_eq!(point.sum, Some(65.0));
    assert_eq!(point.min, Some(0.0));
    assert_eq!(point.max, Some(64.0));
    assert!(point.scale <= i32::from(HISTOGRAM_MAX_SCALE));
    let bucket_count = point.zero_count
        + point
            .positive
            .as_ref()
            .map_or(0, |buckets| buckets.bucket_counts.iter().sum());
    assert_eq!(bucket_count, point.count);

    let resource = &requests[0].resource_metrics[0]
        .resource
        .as_ref()
        .expect("resource missing")
        .attributes;
    assert_eq!(
        attribute(resource, "service.name"),
        Some(&AttributeValue::StringValue(
            "space-sandbox-manager-canary".to_string()
        ))
    );
    assert_eq!(
        attribute(resource, "space.cluster.id"),
        Some(&AttributeValue::StringValue("test-cluster".to_string()))
    );
    assert!(attribute(resource, "k8s.cluster.name").is_none());
    assert_eq!(
        attribute(resource, "deployment.environment.name"),
        Some(&AttributeValue::StringValue("experimental".to_string()))
    );
    assert_eq!(
        attribute(resource, "service.version"),
        Some(&AttributeValue::StringValue("test-sha".to_string()))
    );
    assert!(attribute(resource, "service.instance.id").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unit_bearing_product_names_export_ucum_units() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink(receiver.endpoint());
    // (product name, exported name, declared unit)
    let cases = [
        ("snapshot.gc.bytes_before", "snapshot.gc.bytes_before", "By"),
        ("snapshot.gc.bytes_freed", "snapshot.gc.bytes_freed", "By"),
        (
            "sandbox.evict.logical_bytes_freed",
            "sandbox.evict.logical_bytes_freed",
            "By",
        ),
        ("jobs.bytes", "jobs.bytes", "By"),
        ("jobs.attempts", "jobs.attempts", ""),
        (
            "sandbox.api.activation_idle_age_secs",
            "sandbox.api.activation_idle_age",
            "s",
        ),
        (
            "reconcile.queue.lag_seconds",
            "reconcile.queue.lag_seconds",
            "s",
        ),
        (
            "volume_manager.transfer.throughput_mib_s",
            "volume_manager.transfer.throughput",
            "MiBy/s",
        ),
        (
            "node_disk.sandbox_io_reservation.read_bandwidth_mib_per_sec_max",
            "node_disk.sandbox_io_reservation.read_bandwidth_max",
            "MiBy/s",
        ),
    ];

    for (name, _, _) in cases {
        emit(
            &sink,
            name,
            MetricKind::Distribution,
            MetricValue::Float(1.0),
            &[],
        );
    }
    sink.force_flush().expect("flush unit inventory cases");

    let requests = receiver.take_requests();
    for (name, exported, expected_unit) in cases {
        assert_eq!(
            metric(&requests, &format!("space.{exported}")).unit,
            expected_unit,
            "unexpected unit for {name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_windows_keep_zero_and_retire_silent_gauges() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink(receiver.endpoint());

    emit(
        &sink,
        "counter",
        MetricKind::Counter,
        MetricValue::Unsigned(5),
        &[],
    );
    emit(
        &sink,
        "zero_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(0),
        &[],
    );
    emit(
        &sink,
        "gauge",
        MetricKind::Gauge,
        MetricValue::Float(5.0),
        &[],
    );
    emit(
        &sink,
        "gauge",
        MetricKind::Gauge,
        MetricValue::Float(5.0),
        &[],
    );
    emit(
        &sink,
        "signed_fractional_gauge",
        MetricKind::Gauge,
        MetricValue::Float(-1.25),
        &[],
    );
    sink.force_flush().expect("flush first delta window");
    let first = receiver.take_requests();
    assert_eq!(sum_values(metric(&first, "space.counter")), vec![5]);
    assert_eq!(sum_values(metric(&first, "space.zero_counter")), vec![0]);
    assert_eq!(gauge_values(metric(&first, "space.gauge")), vec![5.0]);
    assert_eq!(
        gauge_values(metric(&first, "space.signed_fractional_gauge")),
        vec![-1.25]
    );

    emit(
        &sink,
        "counter",
        MetricKind::Counter,
        MetricValue::Unsigned(2),
        &[],
    );
    emit(
        &sink,
        "gauge",
        MetricKind::Gauge,
        MetricValue::Float(0.0),
        &[],
    );
    sink.force_flush().expect("flush second delta window");
    let second = receiver.take_requests();
    assert_eq!(sum_values(metric(&second, "space.counter")), vec![2]);
    assert_eq!(gauge_values(metric(&second, "space.gauge")), vec![0.0]);

    sink.force_flush().expect("flush silent delta window");
    let silent = receiver.take_requests();
    assert!(metrics(&silent).is_empty(), "silent streams must retire");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_provider_starts_a_fresh_delta_sequence() {
    let receiver = TestReceiver::start().await;
    let first_sink = test_sink(receiver.endpoint());
    emit(
        &first_sink,
        "restart_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(11),
        &[],
    );
    first_sink.shutdown();
    let first = receiver.take_requests();
    assert_eq!(
        sum_values(metric(&first, "space.restart_counter")),
        vec![11]
    );

    let second_sink = test_sink(receiver.endpoint());
    emit(
        &second_sink,
        "restart_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(2),
        &[],
    );
    second_sink.shutdown();
    let second = receiver.take_requests();
    assert_eq!(
        sum_values(metric(&second, "space.restart_counter")),
        vec![2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_values_conflicts_and_name_churn_are_bounded() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink_with_limits(receiver.endpoint(), 2_000, 2);

    emit(
        &sink,
        "valid_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(i64::MAX as u64),
        &[],
    );
    emit(
        &sink,
        "valid_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(i64::MAX as u64 + 1),
        &[],
    );
    emit(
        &sink,
        "valid_counter",
        MetricKind::Gauge,
        MetricValue::Float(4.0),
        &[],
    );
    emit(
        &sink,
        "valid_histogram",
        MetricKind::Distribution,
        MetricValue::Float(-1.0),
        &[],
    );
    emit(
        &sink,
        "valid_histogram",
        MetricKind::Distribution,
        MetricValue::Float(f64::NAN),
        &[],
    );
    emit_with_unit(
        &sink,
        "valid_histogram",
        MetricKind::Distribution,
        MetricUnit::Milliseconds,
        MetricValue::Float(2.0),
        &[],
    );
    emit(
        &sink,
        "third_name",
        MetricKind::Gauge,
        MetricValue::Float(9.0),
        &[],
    );
    sink.force_flush().expect("flush invalid value cases");

    let requests = receiver.take_requests();
    assert_eq!(
        sum_values(metric(&requests, "space.valid_counter")),
        vec![i64::MAX]
    );
    assert!(
        metrics(&requests)
            .iter()
            .all(|metric| metric.name != "space.third_name")
    );
    assert!(
        metrics(&requests)
            .iter()
            .all(|metric| metric.name != "space.valid_histogram"),
        "a histogram with only invalid values must not export"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attribute_churn_is_bounded_visible_and_recovers_after_stale_eviction() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink_with_limits(receiver.endpoint(), 2, 1_024);

    for producer in ["one", "two", "three"] {
        emit(
            &sink,
            "bounded_counter",
            MetricKind::Counter,
            MetricValue::Unsigned(1),
            &[&format!("producer:{producer}")],
        );
    }
    sink.force_flush()
        .expect("flush cardinality overflow window");
    let first = receiver.take_requests();
    let Data::Sum(sum) = metric(&first, "space.bounded_counter")
        .data
        .as_ref()
        .expect("counter data missing")
    else {
        unreachable!()
    };
    // The SDK's limit covers retained attribute sets; its separately visible
    // overflow point is additional to those two sets.
    assert_eq!(sum.data_points.len(), 3);
    assert_eq!(
        sum_values(metric(&first, "space.bounded_counter"))
            .iter()
            .sum::<i64>(),
        3
    );
    assert!(sum.data_points.iter().any(|point| {
        attribute(&point.attributes, "otel.metric.overflow")
            == Some(&AttributeValue::BoolValue(true))
    }));

    emit(
        &sink,
        "bounded_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(4),
        &["producer:four"],
    );
    sink.force_flush().expect("flush stale-eviction window");
    let second = receiver.take_requests();
    let Data::Sum(sum) = metric(&second, "space.bounded_counter")
        .data
        .as_ref()
        .expect("counter data missing")
    else {
        unreachable!()
    };
    assert_eq!(
        sum_values(metric(&second, "space.bounded_counter")),
        vec![4]
    );
    assert_eq!(
        attribute(&sum.data_points[0].attributes, "otel.metric.overflow"),
        Some(&AttributeValue::BoolValue(true))
    );
    assert!(attribute(&sum.data_points[0].attributes, "producer").is_none());

    // Delta collection marks current series stale, then evicts them on the
    // next collection. Measurements arriving during that eviction window use
    // the visible overflow series; the following interval can allocate a new
    // identity normally.
    emit(
        &sink,
        "bounded_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(5),
        &["producer:five"],
    );
    sink.force_flush()
        .expect("flush recovered cardinality window");
    let third = receiver.take_requests();
    let Data::Sum(sum) = metric(&third, "space.bounded_counter")
        .data
        .as_ref()
        .expect("counter data missing")
    else {
        unreachable!()
    };
    assert_eq!(sum_values(metric(&third, "space.bounded_counter")), vec![5]);
    assert_eq!(
        attribute(&sum.data_points[0].attributes, "producer"),
        Some(&AttributeValue::StringValue("five".to_string()))
    );
    assert!(attribute(&sum.data_points[0].attributes, "otel.metric.overflow").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_failure_is_bounded_and_a_later_window_recovers() {
    let receiver = TestReceiver::start_with_failures(1).await;
    let sink = test_sink(receiver.endpoint());

    for _ in 0..10_000 {
        emit(
            &sink,
            "counter",
            MetricKind::Counter,
            MetricValue::Unsigned(1),
            &[],
        );
    }
    assert!(sink.force_flush().is_err());
    let failed = receiver.take_requests();
    assert_eq!(sum_values(metric(&failed, "space.counter")), vec![10_000]);

    emit(
        &sink,
        "counter",
        MetricKind::Counter,
        MetricValue::Unsigned(3),
        &[],
    );
    sink.force_flush().expect("flush after receiver recovery");
    let recovered_requests = receiver.take_requests();
    assert_eq!(
        sum_values(metric(&recovered_requests, "space.counter")),
        vec![3],
        "a rejected delta is not replayed; the next healthy window remains correct"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_receiver_fails_export_without_affecting_recording() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve absent receiver address");
    let address = listener.local_addr().expect("read absent receiver address");
    drop(listener);
    let sink = test_sink(format!("http://{address}"));

    emit(
        &sink,
        "counter",
        MetricKind::Counter,
        MetricValue::Unsigned(1),
        &[],
    );
    assert!(sink.force_flush().is_err());
}

#[derive(Clone, Debug, Default)]
struct HangingReceiver {
    export_started: Arc<tokio::sync::Notify>,
}

#[tonic::async_trait]
impl MetricsService for HangingReceiver {
    async fn export(
        &self,
        _request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.export_started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_receiver_cannot_extend_shutdown_past_the_documented_bound() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind hanging receiver");
    let endpoint = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("read hanging receiver address")
    );
    let receiver = HangingReceiver::default();
    let export_started = receiver.export_started.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(receiver))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve hanging OTLP receiver");
    });

    let mut config = test_config(endpoint, 2_000, 1_024);
    config.export_timeout = EXPORT_TIMEOUT;
    let sink = OtlpSink::new(config).expect("build production-timeout OTLP test sink");
    emit(
        &sink,
        "shutdown_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(1),
        &[],
    );
    let shutdown = tokio::task::spawn_blocking(move || sink.shutdown());

    tokio::time::timeout(Duration::from_secs(1), export_started.notified())
        .await
        .expect("shutdown never attempted its final export");
    tokio::time::timeout(SHUTDOWN_TIMEOUT + Duration::from_secs(1), shutdown)
        .await
        .expect("shutdown exceeded the documented SDK bound")
        .expect("shutdown task panicked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_flushes_pending_measurements_once() {
    let receiver = TestReceiver::start().await;
    let sink = test_sink(receiver.endpoint());
    emit(
        &sink,
        "shutdown_counter",
        MetricKind::Counter,
        MetricValue::Unsigned(1),
        &[],
    );

    sink.shutdown();
    sink.shutdown();

    let requests = receiver.take_requests();
    assert_eq!(
        sum_values(metric(&requests, "space.shutdown_counter")),
        vec![1]
    );
}

#[cfg(feature = "datadog")]
mod semantic_parity;

/// UCUM `1` means dimensionless, and a Prometheus-style backend renders it as
/// a `_ratio` suffix. Only a fraction of a whole earns that; a count, depth or
/// flag declaring `1` published `..._queue_depth_ratio`, which reads as a
/// proportion the value never was.
#[test]
fn only_fractions_declare_a_dimensionless_unit() {
    use super::wire_unit;
    use crate::sink::MetricUnit;

    for name in [
        "node.ssd_free_fraction",
        "sandbox.evict.disk_usage_fraction",
        "tokio_runtime.busy_ratio",
        "volume_manager.transfer.inflation_ratio",
        // The in-use fraction of each pool's own `max_connections` cap,
        // 0.0-1.0. The only `utilization` metric the catalog emits, and the
        // one `monitor_control_plane_latency.tf` queries for saturation.
        "db.pool.utilization",
    ] {
        assert_eq!(wire_unit(name, MetricUnit::Inferred), "1", "{name}");
    }

    for name in [
        "jobs.queue.depth",
        "jobs.attempts",
        "sandbox.worker.lifecycle.count",
        "gateway.udp.active_sessions",
        "gateway.dns_attribution.cache_entries",
        "node_disk.btrfs.balance_needed",
    ] {
        assert_eq!(wire_unit(name, MetricUnit::Inferred), "", "{name}");
    }

    // A declared or name-borne unit still wins over both branches.
    assert_eq!(wire_unit("api.latency_ms", MetricUnit::Milliseconds), "ms");
    assert_eq!(wire_unit("jobs.bytes", MetricUnit::Inferred), "By");
    assert_eq!(wire_unit("queue.lag_seconds", MetricUnit::Inferred), "s");
}

/// A Prometheus-style backend appends the declared unit's full word, skipping
/// it only when the name already ends in that word. An abbreviation does not
/// match, so `api.latency_ms` with unit `ms` arrived as
/// `..._latency_ms_milliseconds`. The OTLP name drops the abbreviation; the
/// DogStatsD name keeps it, so no Datadog query moves.
#[test]
fn declared_unit_abbreviations_are_removed_from_the_otlp_name() {
    use super::without_declared_unit;

    assert_eq!(without_declared_unit("api.latency_ms", "ms"), "api.latency");
    assert_eq!(without_declared_unit("age_secs", "s"), "age");
    assert_eq!(
        without_declared_unit("transfer.throughput_mib_s", "MiBy/s"),
        "transfer.throughput"
    );
    // The abbreviation can sit mid-name, before a qualifier.
    assert_eq!(
        without_declared_unit("read_bandwidth_mib_per_sec_max", "MiBy/s"),
        "read_bandwidth_max"
    );
    // Full unit words already match what the backend appends, so they stay.
    assert_eq!(
        without_declared_unit("queue.lag_seconds", "s"),
        "queue.lag_seconds"
    );
    assert_eq!(without_declared_unit("jobs.bytes", "By"), "jobs.bytes");
    assert_eq!(
        without_declared_unit("gc.bytes_before", "By"),
        "gc.bytes_before"
    );
    // Not the declared unit, and no unit at all.
    assert_eq!(
        without_declared_unit("api.latency_ms", "By"),
        "api.latency_ms"
    );
    assert_eq!(without_declared_unit("jobs.attempts", ""), "jobs.attempts");
    // Removal must never leave an empty name or a dangling separator.
    assert_eq!(without_declared_unit("_ms", "ms"), "_ms");
    assert_eq!(without_declared_unit("queue._ms", "ms"), "queue._ms");
    assert_eq!(
        without_declared_unit("_mib_per_sec", "MiBy/s"),
        "_mib_per_sec"
    );
}

/// Every metric the catalog emits must still have its own OTLP name after the
/// abbreviation is removed.
///
/// The list is read from the source rather than written out here, so a metric
/// added later is covered without anyone remembering to add it. Two names that
/// collapse together would hand the OTel meter two instruments of different
/// kinds under one name, and the instrument cache is keyed by the product
/// name, so nothing at runtime would notice.
#[test]
fn otlp_names_stay_unique_across_the_catalog() {
    use crate::sink::MetricUnit;
    use std::collections::BTreeMap;
    use std::path::Path;

    /// Any dotted lowercase literal in these crates is a metric name: the
    /// catalog declares them inline at the emit call, as `pub const`, or in a
    /// `(name, value)` table.
    fn names_in(dir: &Path, into: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).expect("catalog directory") {
            let path = entry.expect("catalog entry").path();
            if path.is_dir() {
                names_in(&path, into);
                continue;
            }
            let Some(file) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            // Test fixtures invent names that no service emits.
            if !file.ends_with(".rs") || file.contains("test") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("catalog source");
            for literal in source.split('"').skip(1).step_by(2) {
                let dotted = literal.contains('.')
                    && literal.starts_with(|c: char| c.is_ascii_lowercase())
                    && literal.chars().all(|c| {
                        c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'
                    })
                    && !literal.ends_with('.');
                if dotted {
                    into.push(literal.to_string());
                }
            }
        }
    }

    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/libs");
    let mut names = Vec::new();
    names_in(&crates.join("metrics/src"), &mut names);
    names_in(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut names,
    );
    names.sort_unstable();
    names.dedup();
    assert!(
        names.len() > 400,
        "catalog scan found only {} names; the extraction broke",
        names.len()
    );
    // The pairs the guard exists for, and a few the rename touches, must be in
    // the scan or it is not proving anything about them.
    for required in [
        "gateway.pending_tcp.wait",
        "gateway.pending_tcp.wait_ms",
        "data_plane.ingress.preparation",
        "data_plane.ingress.preparation_ms",
        "sandbox.api.latency_ms",
        "db.pool.acquire_total_ms",
        "db.pool.utilization",
    ] {
        assert!(
            names.iter().any(|name| name == required),
            "catalog scan missed {required}"
        );
    }

    let mut emitted: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for name in &names {
        let unit = super::wire_unit(name, MetricUnit::Inferred);
        emitted
            .entry(super::without_declared_unit(name, unit).into_owned())
            .or_default()
            .push(name);
    }
    let collisions: Vec<_> = emitted
        .iter()
        .filter(|(_, sources)| sources.len() > 1)
        .collect();
    assert!(
        collisions.is_empty(),
        "these metrics would share one OTLP instrument name: {collisions:?}"
    );
}
