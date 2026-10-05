use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AttributeValue;
use opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;

use super::{TestReceiver, test_sink};
use crate::sink::datadog::DatadogSink;
use crate::sink::{Metric, MetricKind, MetricSink, MetricUnit, MetricValue};

// DogStatsD intentionally formats f64 values to three decimals. OTLP keeps the
// original f64, so semantic parity allows only that pre-existing rounding.
//
// Parity is over the *logical* metric, not the literal wire name. DogStatsD has
// no unit field, so its names carry the unit as a suffix; OTLP declares the unit
// separately and its names must not repeat it. Both sides are therefore keyed by
// the name with that abbreviation removed, so this still asserts that equal
// inputs produce equal counters, gauges, histograms and attributes.
const LEGACY_FLOAT_ABS_TOLERANCE: f64 = 0.000_500_001;

#[derive(Clone, Copy)]
struct Input<'a> {
    name: &'a str,
    kind: MetricKind,
    unit: MetricUnit,
    value: MetricValue,
    tags: &'a [&'a str],
}

impl<'a> Input<'a> {
    const fn counter(name: &'a str, value: u64, tags: &'a [&'a str]) -> Self {
        Self {
            name,
            kind: MetricKind::Counter,
            unit: MetricUnit::Inferred,
            value: MetricValue::Unsigned(value),
            tags,
        }
    }

    const fn gauge(name: &'a str, value: f64, tags: &'a [&'a str]) -> Self {
        Self {
            name,
            kind: MetricKind::Gauge,
            unit: MetricUnit::Inferred,
            value: MetricValue::Float(value),
            tags,
        }
    }

    const fn distribution(name: &'a str, value: f64, tags: &'a [&'a str]) -> Self {
        Self {
            name,
            kind: MetricKind::Distribution,
            unit: MetricUnit::Inferred,
            value: MetricValue::Float(value),
            tags,
        }
    }

    const fn timing(name: &'a str, value_ms: f64, tags: &'a [&'a str]) -> Self {
        Self {
            name,
            kind: MetricKind::Distribution,
            unit: MetricUnit::Milliseconds,
            value: MetricValue::Float(value_ms),
            tags,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Series {
    name: String,
    attributes: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
struct HistogramSummary {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

#[derive(Debug, Default)]
struct SemanticBatch {
    counters: BTreeMap<Series, u64>,
    gauges: BTreeMap<Series, f64>,
    histograms: BTreeMap<Series, HistogramSummary>,
}

#[derive(Debug)]
struct DogStatsdBatch {
    semantic: SemanticBatch,
    histogram_samples: BTreeMap<Series, Vec<f64>>,
}

#[derive(Debug)]
struct OtlpBatch {
    semantic: SemanticBatch,
    histogram_points: BTreeMap<Series, ExponentialHistogramDataPoint>,
    units: BTreeMap<String, String>,
}

struct DogStatsdReceiver {
    socket: UdpSocket,
}

impl DogStatsdReceiver {
    fn start() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind DogStatsD receiver");
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set DogStatsD receive timeout");
        Self { socket }
    }

    fn sink(&self, constant_tags: Vec<String>) -> DatadogSink {
        DatadogSink::connected_to(
            self.socket.local_addr().expect("read DogStatsD address"),
            "space".to_string(),
            constant_tags,
        )
    }

    fn receive(&self, expected: usize) -> Vec<String> {
        (0..expected)
            .map(|_| {
                let mut buffer = [0_u8; 2_048];
                let size = self
                    .socket
                    .recv(&mut buffer)
                    .expect("receive DogStatsD metric");
                std::str::from_utf8(&buffer[..size])
                    .expect("DogStatsD metric is UTF-8")
                    .to_string()
            })
            .collect()
    }
}

fn constant_tags() -> Vec<String> {
    [
        "env:experimental",
        "service:space-sandbox-manager-canary",
        "version:test-sha",
        "cluster_id:test-cluster",
    ]
    .map(str::to_string)
    .to_vec()
}

fn emit_window(sink: &dyn MetricSink, inputs: &[Input<'_>]) {
    for input in inputs {
        let tags: Vec<String> = input.tags.iter().map(|tag| (*tag).to_string()).collect();
        sink.emit(Metric {
            name: input.name,
            kind: input.kind,
            unit: input.unit,
            value: input.value,
            tags: &tags,
        });
    }
}

/// Strip the unit abbreviation the OTLP sink omits, so one key identifies the
/// same logical metric on both transports.
///
/// Keyed on the unit the sink would declare, not on the suffix alone: the sink
/// only removes an abbreviation it has actually declared, and it keeps one that
/// another metric's name already occupies. Deriving both from `wire_unit` and
/// `without_declared_unit` means this cannot drift from the sink and cannot
/// collapse two inputs that the sink keeps apart.
fn logical_name(wire_name: &str) -> String {
    let product = wire_name
        .strip_prefix("space.")
        .unwrap_or(wire_name)
        .to_string();
    let unit = crate::sink::otlp::wire_unit(&product, MetricUnit::Inferred);
    format!(
        "space.{}",
        crate::sink::otlp::without_declared_unit(&product, unit)
    )
}

fn series(name: &str, tags: impl IntoIterator<Item = (String, String)>) -> Series {
    let mut attributes: Vec<_> = tags.into_iter().collect();
    attributes.sort_unstable();
    Series {
        name: name.to_string(),
        attributes,
    }
}

fn dogstatsd_batch(datagrams: &[String]) -> DogStatsdBatch {
    let mut semantic = SemanticBatch::default();
    let mut histogram_samples = BTreeMap::<Series, Vec<f64>>::new();

    for datagram in datagrams {
        let (body, raw_tags) = datagram
            .split_once("|#")
            .map_or((datagram.as_str(), ""), |(body, tags)| (body, tags));
        let (name, value_and_kind) = body.split_once(':').expect("DogStatsD name/value");
        let (raw_value, kind) = value_and_kind
            .split_once('|')
            .expect("DogStatsD value/type");
        let key = series(
            &logical_name(name),
            raw_tags
                .split(',')
                .filter(|tag| !tag.is_empty())
                .map(|tag| {
                    tag.split_once(':').map_or_else(
                        || (tag.to_string(), "true".to_string()),
                        |(key, value)| (key.to_string(), value.to_string()),
                    )
                }),
        );

        match kind {
            "c" => {
                *semantic.counters.entry(key).or_default() +=
                    raw_value.parse::<u64>().expect("DogStatsD counter");
            }
            "g" => {
                semantic
                    .gauges
                    .insert(key, raw_value.parse().expect("DogStatsD gauge"));
            }
            "d" => {
                histogram_samples
                    .entry(key)
                    .or_default()
                    .push(raw_value.parse().expect("DogStatsD distribution"));
            }
            other => panic!("unexpected DogStatsD type {other}"),
        }
    }

    semantic.histograms = histogram_samples
        .iter()
        .map(|(key, samples)| (key.clone(), summarize(samples)))
        .collect();
    DogStatsdBatch {
        semantic,
        histogram_samples,
    }
}

fn otlp_attributes(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, String)> {
    attributes
        .iter()
        .map(|attribute| {
            let value = attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                .expect("OTLP attribute value");
            let value = match value {
                AttributeValue::StringValue(value) => value.clone(),
                AttributeValue::BoolValue(value) => value.to_string(),
                other => panic!("unexpected OTLP attribute value {other:?}"),
            };
            (attribute.key.clone(), value)
        })
        .collect()
}

fn otlp_resource_query_identity(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, String)> {
    otlp_attributes(attributes)
        .into_iter()
        .filter_map(|(key, value)| match key.as_str() {
            "deployment.environment.name" => Some(("env".to_string(), value)),
            "service.version" => Some(("version".to_string(), value)),
            _ => None,
        })
        .collect()
}

fn otlp_series(
    name: &str,
    point_attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    resource_identity: &[(String, String)],
) -> Series {
    let attributes = otlp_attributes(point_attributes);
    assert!(attributes.iter().all(|(key, _)| key != "version"));
    if attributes.iter().any(|(key, _)| key == "service.role") {
        assert!(
            attributes.iter().any(|(key, value)| {
                key == "service" && value == "space-sandbox-manager-canary"
            })
        );
    }
    series(
        name,
        attributes
            .into_iter()
            // Native DogStatsD retains duplicate identity keys. OTLP gives
            // per-measurement roles and requested versions their own keys.
            .map(|(key, value)| {
                (
                    match key.as_str() {
                        "service.role" => "service".to_string(),
                        "version.requested" => "version".to_string(),
                        _ => key,
                    },
                    value,
                )
            })
            .chain(resource_identity.iter().cloned()),
    )
}

fn otlp_batch(requests: &[ExportMetricsServiceRequest]) -> OtlpBatch {
    let mut semantic = SemanticBatch::default();
    let mut histogram_points = BTreeMap::new();
    let mut units = BTreeMap::new();

    for request in requests {
        for resource_metrics in &request.resource_metrics {
            let resource_identity = resource_metrics
                .resource
                .as_ref()
                .map(|resource| otlp_resource_query_identity(&resource.attributes))
                .unwrap_or_default();
            for metric in resource_metrics
                .scope_metrics
                .iter()
                .flat_map(|scope| &scope.metrics)
            {
                units.insert(metric.name.clone(), metric.unit.clone());
                match metric.data.as_ref().expect("OTLP metric data") {
                    Data::Sum(sum) => {
                        for point in &sum.data_points {
                            let key =
                                otlp_series(&metric.name, &point.attributes, &resource_identity);
                            let Some(NumberValue::AsInt(value)) = point.value else {
                                panic!("counter {} was not an OTLP int64", metric.name);
                            };
                            *semantic.counters.entry(key).or_default() +=
                                u64::try_from(value).expect("non-negative OTLP counter");
                        }
                    }
                    Data::Gauge(gauge) => {
                        for point in &gauge.data_points {
                            let key =
                                otlp_series(&metric.name, &point.attributes, &resource_identity);
                            let Some(NumberValue::AsDouble(value)) = point.value else {
                                panic!("gauge {} was not an OTLP double", metric.name);
                            };
                            assert!(semantic.gauges.insert(key, value).is_none());
                        }
                    }
                    Data::ExponentialHistogram(histogram) => {
                        for point in &histogram.data_points {
                            let key =
                                otlp_series(&metric.name, &point.attributes, &resource_identity);
                            let summary = HistogramSummary {
                                count: point.count,
                                sum: point.sum.expect("histogram sum"),
                                min: point.min.expect("histogram min"),
                                max: point.max.expect("histogram max"),
                            };
                            assert!(semantic.histograms.insert(key.clone(), summary).is_none());
                            assert!(histogram_points.insert(key, point.clone()).is_none());
                        }
                    }
                    other => panic!("unexpected OTLP metric data {other:?}"),
                }
            }
        }
    }

    OtlpBatch {
        semantic,
        histogram_points,
        units,
    }
}

fn summarize(samples: &[f64]) -> HistogramSummary {
    HistogramSummary {
        count: samples.len() as u64,
        sum: samples.iter().sum(),
        min: samples.iter().copied().fold(f64::INFINITY, f64::min),
        max: samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    }
}

fn assert_close(actual: f64, expected: f64, tolerance: f64, context: &str) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{context}: expected {expected}, got {actual} (tolerance {tolerance})"
    );
}

fn exact_percentile(samples: &[f64], percentile: usize) -> f64 {
    let mut samples = samples.to_vec();
    samples.sort_by(f64::total_cmp);
    let rank = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .clamp(1, samples.len());
    samples[rank - 1]
}

fn exponential_histogram_percentile(
    point: &ExponentialHistogramDataPoint,
    percentile: usize,
) -> f64 {
    let percentile = u64::try_from(percentile).expect("percentile fits u64");
    let target = point
        .count
        .saturating_mul(percentile)
        .div_ceil(100)
        .clamp(1, point.count);
    if target <= point.zero_count {
        return 0.0;
    }

    assert_eq!(
        point
            .negative
            .as_ref()
            .map_or(0, |buckets| buckets.bucket_counts.iter().sum()),
        0,
        "SPACE histograms reject negative samples"
    );
    let positive = point.positive.as_ref().expect("positive histogram buckets");
    let mut seen = point.zero_count;
    let scale = point.scale;
    assert!(scale >= 0, "fixture unexpectedly downscaled below zero");
    let base = 2_f64.powf(2_f64.powi(-scale));
    for (index, count) in positive.bucket_counts.iter().enumerate() {
        seen += count;
        if seen >= target {
            let bucket_index = positive.offset + index as i32;
            // Exponential histogram buckets contain (base^i, base^(i+1)].
            // Their geometric midpoint has a scale-defined relative error.
            let midpoint = base.powf(bucket_index as f64 + 0.5);
            return midpoint.clamp(
                point.min.expect("histogram min"),
                point.max.expect("histogram max"),
            );
        }
    }
    panic!("histogram buckets did not contain requested quantile");
}

fn assert_batches_match(dogstatsd: &DogStatsdBatch, otlp: &OtlpBatch) {
    assert_eq!(dogstatsd.semantic.counters, otlp.semantic.counters);
    assert_eq!(
        dogstatsd.semantic.gauges.keys().collect::<Vec<_>>(),
        otlp.semantic.gauges.keys().collect::<Vec<_>>()
    );
    for (key, expected) in &dogstatsd.semantic.gauges {
        assert_close(
            otlp.semantic.gauges[key],
            *expected,
            LEGACY_FLOAT_ABS_TOLERANCE,
            &format!("gauge {key:?}"),
        );
    }

    assert_eq!(
        dogstatsd.semantic.histograms.keys().collect::<Vec<_>>(),
        otlp.semantic.histograms.keys().collect::<Vec<_>>()
    );
    for (key, expected) in &dogstatsd.semantic.histograms {
        let actual = &otlp.semantic.histograms[key];
        assert_eq!(actual.count, expected.count, "histogram {key:?} count");
        assert_close(
            actual.sum,
            expected.sum,
            LEGACY_FLOAT_ABS_TOLERANCE * expected.count as f64,
            &format!("histogram {key:?} sum"),
        );
        assert_close(
            actual.min,
            expected.min,
            LEGACY_FLOAT_ABS_TOLERANCE,
            &format!("histogram {key:?} min"),
        );
        assert_close(
            actual.max,
            expected.max,
            LEGACY_FLOAT_ABS_TOLERANCE,
            &format!("histogram {key:?} max"),
        );

        let point = &otlp.histogram_points[key];
        let bucket_count = point.zero_count
            + point
                .positive
                .as_ref()
                .map_or(0, |buckets| buckets.bucket_counts.iter().sum::<u64>());
        assert_eq!(bucket_count, point.count, "histogram {key:?} buckets");
        let scale_error = 2_f64.powf(2_f64.powi(-point.scale) / 2.0) - 1.0;
        for percentile in [90, 95] {
            let expected_quantile = exact_percentile(&dogstatsd.histogram_samples[key], percentile);
            let actual_quantile = exponential_histogram_percentile(point, percentile);
            let relative_error = (actual_quantile - expected_quantile).abs() / expected_quantile;
            let rounding_error = LEGACY_FLOAT_ABS_TOLERANCE / expected_quantile;
            assert!(
                relative_error <= scale_error + rounding_error,
                "histogram {key:?} p{percentile}: expected {expected_quantile}, got {actual_quantile}; relative error {relative_error} exceeds {}",
                scale_error + rounding_error,
            );
        }
    }
}

async fn emit_and_compare_window(
    dogstatsd_receiver: &DogStatsdReceiver,
    dogstatsd_sink: &DatadogSink,
    otlp_receiver: &TestReceiver,
    otlp_sink: &super::super::OtlpSink,
    inputs: &[Input<'_>],
) -> (DogStatsdBatch, OtlpBatch) {
    emit_window(dogstatsd_sink, inputs);
    emit_window(otlp_sink, inputs);
    otlp_sink.force_flush().expect("flush OTLP parity window");

    let dogstatsd = dogstatsd_batch(&dogstatsd_receiver.receive(inputs.len()));
    let otlp = otlp_batch(&otlp_receiver.take_requests());
    assert_batches_match(&dogstatsd, &otlp);
    (dogstatsd, otlp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_inputs_have_equal_interval_semantics_across_sinks() {
    // Feed one contract table through both real sinks. The comparison ignores
    // UDP/gRPC framing and checks the interval result a metrics backend sees.
    let dogstatsd_receiver = DogStatsdReceiver::start();
    let dogstatsd_sink = dogstatsd_receiver.sink(constant_tags());
    let otlp_receiver = TestReceiver::start().await;
    let otlp_sink = test_sink(otlp_receiver.endpoint());
    let precise = (1_u64 << 53) + 1;

    let mut first = vec![
        Input::counter("template.fetch.error", 1, &["version:hash-a"]),
        Input::counter("template.fetch.error", 1, &["version:hash-b"]),
        Input::counter("template.fetch.error", 1, &["version:<unset>"]),
        Input::counter("template.fetch.error", 1, &["version:hash-a"]),
        Input::counter("grpc.client.retry", 2, &["service:peer-a"]),
        Input::counter("grpc.client.retry", 3, &["service:peer-b"]),
        Input::gauge(
            "grpc.server.rate_limiter.in_flight",
            5.0,
            &["service:interface-a"],
        ),
        Input::gauge(
            "grpc.server.rate_limiter.in_flight",
            7.0,
            &["service:interface-b"],
        ),
        Input::counter(
            "gateway.inspection.admission",
            1,
            &[
                "outcome:denied",
                "reason:first_byte_timeout",
                "tls_interception:off",
                "stage:destination_policy",
                "path:unknown",
                "policy_reason:default_deny",
            ],
        ),
        Input::counter(
            "gateway.inspection.admission",
            1,
            &[
                "outcome:error",
                "reason:no_identity",
                "tls_interception:unknown",
                "stage:policy_readiness",
                "path:unknown",
                "policy_reason:not_evaluated",
            ],
        ),
        Input::counter(
            "gateway.inspection.session",
            1,
            &[
                "outcome:error",
                "reason:tls_downstream_error",
                "tls_interception:on",
                "stage:guest_handshake",
                "path:tls_inspection",
                "policy_reason:allowed",
            ],
        ),
        Input::counter(
            "sandbox.worker.lifecycle.count",
            precise,
            &["method:create", "status:success"],
        ),
        Input::counter(
            "sandbox.worker.lifecycle.count",
            7,
            &["method:create", "status:success"],
        ),
        Input::counter(
            "sandbox.worker.lifecycle.count",
            11,
            &["method:delete", "status:success"],
        ),
        Input::gauge("sandbox.worker.cleanup.queue_depth", 5.25, &[]),
        Input::gauge("sandbox.worker.state.count", 0.0, &["state:ready"]),
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            1.0,
            &["method:start"],
        ),
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            8.0,
            &["method:start"],
        ),
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            64.0,
            &["method:start"],
        ),
        Input::timing("legacy_operation_latency", 12.3456, &[]),
        Input::distribution(
            "db.pool.utilization",
            0.9,
            &["database_identity:space_user"],
        ),
        Input::distribution(
            "db.pool.utilization",
            0.7,
            &["database_identity:space_creds"],
        ),
        Input::distribution("grpc.server.active_requests", 2.0, &[]),
        Input::distribution("grpc.server.active_requests", 9.0, &[]),
    ];
    // Twenty distinct samples put p90/p95 in different interior buckets (18
    // and 19), so returning the highest occupied bucket cannot pass parity.
    first.extend((1..=20).map(|value| {
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            f64::from(value),
            &["method:stat"],
        )
    }));
    let (_, first_otlp) = emit_and_compare_window(
        &dogstatsd_receiver,
        &dogstatsd_sink,
        &otlp_receiver,
        &otlp_sink,
        &first,
    )
    .await;
    // A count is not dimensionless: declaring UCUM `1` made Prometheus
    // publish it with a `_ratio` suffix.
    assert_eq!(first_otlp.units["space.sandbox.worker.lifecycle.count"], "");
    assert_eq!(
        first_otlp.units["space.sandbox.worker.spaced_bridge.rpc_latency"],
        "ms"
    );
    assert_eq!(first_otlp.units["space.legacy_operation_latency"], "ms");

    let second = [
        Input::counter(
            "sandbox.worker.lifecycle.count",
            3,
            &["method:create", "status:success"],
        ),
        Input::gauge("sandbox.worker.cleanup.queue_depth", 0.0, &[]),
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            2.0,
            &["method:start"],
        ),
        Input::distribution(
            "sandbox.worker.spaced_bridge.rpc_latency_ms",
            4.0,
            &["method:start"],
        ),
        Input::distribution(
            "db.pool.utilization",
            0.2,
            &["database_identity:space_user"],
        ),
        Input::distribution(
            "db.pool.utilization",
            0.3,
            &["database_identity:space_creds"],
        ),
        Input::distribution("grpc.server.active_requests", 1.0, &[]),
    ];
    let (_, second_otlp) = emit_and_compare_window(
        &dogstatsd_receiver,
        &dogstatsd_sink,
        &otlp_receiver,
        &otlp_sink,
        &second,
    )
    .await;

    assert_eq!(
        second_otlp.semantic.counters.values().copied().sum::<u64>(),
        3
    );
    assert_eq!(
        second_otlp
            .semantic
            .gauges
            .values()
            .copied()
            .collect::<Vec<_>>(),
        [0.0]
    );
    assert_close(
        second_otlp
            .semantic
            .histograms
            .values()
            .map(|histogram| histogram.max)
            .fold(f64::NEG_INFINITY, f64::max),
        4.0,
        f64::EPSILON,
        "the prior interval's high value must not leak into the next delta",
    );
    assert!(
        second_otlp
            .semantic
            .gauges
            .keys()
            .all(|series| { series.name != "space.sandbox.worker.state.count" })
    );
}
