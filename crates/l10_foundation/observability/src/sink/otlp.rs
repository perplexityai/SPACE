use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, Protocol, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{
    Aggregation, Instrument as SdkInstrument, InstrumentKind, PeriodicReader, SdkMeterProvider,
    Stream, Temporality,
};

use super::{METRICS_NAMESPACE, Metric, MetricKind, MetricSink, MetricUnit, MetricValue};
use crate::identity::Identity;
use crate::otlp_endpoint::EndpointRejection;

const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_METRICS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTLP_PROTOCOL_ENV: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
const OTLP_METRICS_PROTOCOL_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL";
const OTLP_HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_HEADERS";
const OTLP_METRICS_HEADERS_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_HEADERS";
/// OpenTelemetry's standard metric export interval, in milliseconds.
const METRIC_EXPORT_INTERVAL_ENV: &str = "OTEL_METRIC_EXPORT_INTERVAL";
/// OpenTelemetry's standard temporality selector for OTLP metric exporters.
const METRICS_TEMPORALITY_ENV: &str = "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE";

pub(super) const EXPORT_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);
// opentelemetry_sdk 0.31's PeriodicReader has its own five-second shutdown
// wait. Keep the selected bound explicit until the SDK exposes a configurable
// reader deadline.
pub(super) const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const ATTRIBUTE_CARDINALITY_LIMIT: usize = 2_000;
pub(super) const INSTRUMENT_NAME_LIMIT: usize = 1_024;
pub(super) const HISTOGRAM_MAX_SIZE: u32 = 160;
pub(super) const HISTOGRAM_MAX_SCALE: i8 = 8;

#[derive(Debug, thiserror::Error)]
pub(super) enum OtlpMetricsInitError {
    #[error(
        "missing OTLP endpoint; set OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT"
    )]
    MissingEndpoint,
    #[error("OTLP metrics currently require the grpc protocol, got {raw:?}")]
    UnsupportedProtocol { raw: String },
    #[error("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT {rejection}")]
    UnsupportedEndpoint { rejection: EndpointRejection },
    #[error("OTLP exporter headers are not allowed with the plaintext internal metrics profile")]
    HeadersOnPlaintextEndpoint,
    #[error("OTLP/gRPC metrics initialization requires an active Tokio runtime")]
    MissingRuntime,
    #[error("invalid non-Unicode environment variable {name}")]
    EnvVar {
        name: &'static str,
        #[source]
        source: std::env::VarError,
    },
    #[error(
        "OTEL_METRIC_EXPORT_INTERVAL must be a positive whole number of milliseconds, got {raw:?}"
    )]
    InvalidExportInterval { raw: String },
    #[error(
        "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE must be cumulative, delta, or lowmemory, got {raw:?}"
    )]
    InvalidTemporality { raw: String },
    #[error("invalid fixed OTLP metrics stream configuration: {0}")]
    Stream(String),
    #[error("failed to build OTLP metrics exporter: {0}")]
    Exporter(#[from] ExporterBuildError),
}

pub(super) struct OtlpSink {
    meter_provider: SdkMeterProvider,
    meter: Meter,
    constant_attributes: Vec<KeyValue>,
    instruments: Mutex<HashMap<String, CachedInstrument>>,
    instrument_name_limit: usize,
    shutdown: AtomicBool,
    warned_instrument_limit: AtomicBool,
    warned_registration_conflict: AtomicBool,
    warned_invalid_counter: AtomicBool,
    warned_invalid_float: AtomicBool,
    warned_poisoned_cache: AtomicBool,
}

#[derive(Clone)]
enum CachedInstrument {
    Counter(Counter<u64>, &'static str),
    Gauge(Gauge<f64>, &'static str),
    Histogram(Histogram<f64>, &'static str),
}

impl CachedInstrument {
    fn kind(&self) -> MetricKind {
        match self {
            Self::Counter(_, _) => MetricKind::Counter,
            Self::Gauge(_, _) => MetricKind::Gauge,
            Self::Histogram(_, _) => MetricKind::Distribution,
        }
    }

    fn unit(&self) -> &'static str {
        match self {
            Self::Counter(_, unit) | Self::Gauge(_, unit) | Self::Histogram(_, unit) => unit,
        }
    }
}

#[derive(Debug)]
struct DiagnosingExporter<E> {
    inner: E,
    failed: AtomicBool,
}

impl<E> DiagnosingExporter<E> {
    fn new(inner: E) -> Self {
        Self {
            inner,
            failed: AtomicBool::new(false),
        }
    }
}

impl<E> PushMetricExporter for DiagnosingExporter<E>
where
    E: PushMetricExporter,
{
    async fn export(
        &self,
        metrics: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> OTelSdkResult {
        let result = self.inner.export(metrics).await;
        match &result {
            Err(error) if !self.failed.swap(true, Ordering::AcqRel) => {
                tracing::warn!(%error, "otlp metrics: export failed; measurements remain non-blocking");
            }
            Ok(()) if self.failed.swap(false, Ordering::AcqRel) => {
                tracing::info!("otlp metrics: export recovered");
            }
            _ => {}
        }
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

/// The environment-derived half of the OTLP metrics configuration.
///
/// Separated from the exporter build so a wrong setting can fail telemetry
/// initialization while an unreachable receiver stays a runtime warning.
pub(super) struct OtlpEnvConfig {
    endpoint: String,
    export_interval: Duration,
    temporality: Temporality,
}

impl OtlpEnvConfig {
    pub(super) fn from_env() -> Result<Self, OtlpMetricsInitError> {
        let endpoint = configured_endpoint()?;
        validate_grpc_protocol()?;
        validate_plaintext_headers()?;
        let export_interval = configured_export_interval()?;
        let temporality = configured_temporality()?;
        Ok(Self {
            endpoint,
            export_interval,
            temporality,
        })
    }
}

impl OtlpSink {
    pub(super) fn from_validated_env(
        env: OtlpEnvConfig,
        identity: &Identity,
        constant_tags: Vec<String>,
    ) -> Result<Self, OtlpMetricsInitError> {
        Self::new(OtlpMetricsConfig {
            endpoint: env.endpoint,
            identity: identity.clone(),
            constant_tags,
            export_interval: env.export_interval,
            temporality: env.temporality,
            export_timeout: EXPORT_TIMEOUT,
            attribute_cardinality_limit: ATTRIBUTE_CARDINALITY_LIMIT,
            instrument_name_limit: INSTRUMENT_NAME_LIMIT,
        })
    }

    fn new(config: OtlpMetricsConfig) -> Result<Self, OtlpMetricsInitError> {
        // tonic's lazy channel starts its buffer worker immediately. Detect the
        // missing executor here so an explicitly selected OTLP sink fails open
        // instead of panicking in a process that initializes before Tokio.
        tokio::runtime::Handle::try_current().map_err(|_| OtlpMetricsInitError::MissingRuntime)?;
        let exporter = MetricExporter::builder()
            .with_tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(config.endpoint)
            .with_timeout(config.export_timeout)
            .with_temporality(config.temporality)
            .build()?;
        let reader = PeriodicReader::builder(DiagnosingExporter::new(exporter))
            .with_interval(config.export_interval)
            .build();

        let cardinality_limit = config.attribute_cardinality_limit;
        validate_stream_config(cardinality_limit)?;
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_view(move |instrument: &SdkInstrument| {
                let stream = if instrument.kind() == InstrumentKind::Histogram {
                    Stream::builder()
                        .with_aggregation(Aggregation::Base2ExponentialHistogram {
                            max_size: HISTOGRAM_MAX_SIZE,
                            max_scale: HISTOGRAM_MAX_SCALE,
                            record_min_max: true,
                        })
                        .with_cardinality_limit(cardinality_limit)
                } else {
                    Stream::builder().with_cardinality_limit(cardinality_limit)
                };
                Some(
                    stream
                        .build()
                        .expect("fixed OTLP metrics stream configuration was validated at startup"),
                )
            })
            .with_resource(metric_resource(&config.identity, &config.constant_tags))
            .build();
        let meter = meter_provider.meter("pplx-space-observability");
        let constant_attributes = config
            .constant_tags
            .iter()
            .filter_map(|tag| metric_point_attribute(tag))
            .collect();

        tracing::info!(
            protocol = "grpc",
            temporality = temporality_name(config.temporality),
            export_interval_ms = config.export_interval.as_millis(),
            export_timeout_ms = config.export_timeout.as_millis(),
            histogram = "base2_exponential",
            histogram_max_size = HISTOGRAM_MAX_SIZE,
            histogram_max_scale = HISTOGRAM_MAX_SCALE,
            attribute_cardinality_limit = cardinality_limit,
            instrument_name_limit = config.instrument_name_limit,
            "otlp metrics: client initialized"
        );

        Ok(Self {
            meter_provider,
            meter,
            constant_attributes,
            instruments: Mutex::new(HashMap::new()),
            instrument_name_limit: config.instrument_name_limit,
            shutdown: AtomicBool::new(false),
            warned_instrument_limit: AtomicBool::new(false),
            warned_registration_conflict: AtomicBool::new(false),
            warned_invalid_counter: AtomicBool::new(false),
            warned_invalid_float: AtomicBool::new(false),
            warned_poisoned_cache: AtomicBool::new(false),
        })
    }

    fn attributes(&self, tags: &[String]) -> Vec<KeyValue> {
        let mut attributes = self.constant_attributes.clone();
        attributes.extend(tags.iter().filter_map(|tag| {
            let mut attribute = tag_attribute(tag);
            // Legacy metric helpers use `service` for the logical RPC/socket
            // role and `version` for the requested artifact. Neither may be
            // dropped or collide with the process identity, or series merge.
            match attribute.key.as_str() {
                "service" => attribute.key = "service.role".into(),
                "version" => attribute.key = "version.requested".into(),
                "env" => return None,
                _ => {}
            }
            Some(attribute)
        }));
        attributes
    }

    fn instrument(
        &self,
        name: &str,
        kind: MetricKind,
        metric_unit: MetricUnit,
    ) -> Option<CachedInstrument> {
        let Ok(mut instruments) = self.instruments.lock() else {
            warn_once(
                &self.warned_poisoned_cache,
                "otlp metrics: instrument cache poisoned; measurement dropped",
            );
            return None;
        };
        let unit = wire_unit(name, metric_unit);

        if let Some(instrument) = instruments.get(name) {
            if instrument.kind() != kind || instrument.unit() != unit {
                warn_once(
                    &self.warned_registration_conflict,
                    "otlp metrics: one name was registered with conflicting kinds or units; conflicting measurement dropped",
                );
                return None;
            }
            return Some(instrument.clone());
        }

        if instruments.len() >= self.instrument_name_limit {
            warn_once(
                &self.warned_instrument_limit,
                "otlp metrics: instrument-name limit reached; unseen metric names are dropped",
            );
            return None;
        }

        let emitted_name = format!("{METRICS_NAMESPACE}.{}", without_declared_unit(name, unit));
        let instrument = match kind {
            MetricKind::Counter => CachedInstrument::Counter(
                self.meter.u64_counter(emitted_name).with_unit(unit).build(),
                unit,
            ),
            MetricKind::Gauge => CachedInstrument::Gauge(
                self.meter.f64_gauge(emitted_name).with_unit(unit).build(),
                unit,
            ),
            MetricKind::Distribution => CachedInstrument::Histogram(
                self.meter
                    .f64_histogram(emitted_name)
                    .with_unit(unit)
                    .build(),
                unit,
            ),
        };
        instruments.insert(name.to_string(), instrument.clone());
        Some(instrument)
    }

    #[cfg(test)]
    fn force_flush(&self) -> OTelSdkResult {
        self.meter_provider.force_flush()
    }
}

impl MetricSink for OtlpSink {
    fn label(&self) -> &'static str {
        "otlp"
    }

    fn emit(&self, metric: Metric<'_>) {
        let Some(instrument) = self.instrument(metric.name, metric.kind, metric.unit) else {
            return;
        };
        let attributes = self.attributes(metric.tags);

        match (instrument, metric.value) {
            (CachedInstrument::Counter(counter, _), MetricValue::Unsigned(value))
                if value <= i64::MAX as u64 =>
            {
                counter.add(value, &attributes);
            }
            (CachedInstrument::Counter(_, _), MetricValue::Unsigned(_)) => warn_once(
                &self.warned_invalid_counter,
                "otlp metrics: counter increment exceeds the signed OTLP wire range; measurement dropped",
            ),
            (CachedInstrument::Gauge(gauge, _), MetricValue::Float(value)) if value.is_finite() => {
                gauge.record(value, &attributes);
            }
            (CachedInstrument::Histogram(histogram, _), MetricValue::Float(value))
                if value.is_finite() && value >= 0.0 =>
            {
                histogram.record(value, &attributes);
            }
            (CachedInstrument::Gauge(_, _), MetricValue::Float(_))
            | (CachedInstrument::Histogram(_, _), MetricValue::Float(_)) => warn_once(
                &self.warned_invalid_float,
                "otlp metrics: non-finite gauge or negative/non-finite histogram measurement dropped",
            ),
            _ => warn_once(
                &self.warned_registration_conflict,
                "otlp metrics: metric kind/value mismatch; measurement dropped",
            ),
        }
    }

    fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(error) = self.meter_provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT) {
            tracing::warn!(%error, "otlp metrics: bounded shutdown did not export all pending data");
        }
    }
}

struct OtlpMetricsConfig {
    endpoint: String,
    identity: Identity,
    constant_tags: Vec<String>,
    export_interval: Duration,
    temporality: Temporality,
    export_timeout: Duration,
    attribute_cardinality_limit: usize,
    instrument_name_limit: usize,
}

fn configured_endpoint() -> Result<String, OtlpMetricsInitError> {
    endpoint_from_values(
        optional_env(OTLP_METRICS_ENDPOINT_ENV)?,
        optional_env(OTLP_ENDPOINT_ENV)?,
    )
}

fn endpoint_from_values(
    signal_specific: Option<String>,
    general: Option<String>,
) -> Result<String, OtlpMetricsInitError> {
    let endpoint = preferred_nonempty(signal_specific, general)
        .ok_or(OtlpMetricsInitError::MissingEndpoint)?;
    crate::otlp_endpoint::validate(&endpoint)
        .map_err(|rejection| OtlpMetricsInitError::UnsupportedEndpoint { rejection })?;
    Ok(endpoint)
}

fn validate_grpc_protocol() -> Result<(), OtlpMetricsInitError> {
    validate_protocol_values(
        optional_env(OTLP_METRICS_PROTOCOL_ENV)?,
        optional_env(OTLP_PROTOCOL_ENV)?,
    )
}

fn configured_export_interval() -> Result<Duration, OtlpMetricsInitError> {
    export_interval_from_value(optional_env(METRIC_EXPORT_INTERVAL_ENV)?)
}

/// Read OpenTelemetry's standard metric export interval.
///
/// Unset or blank keeps [`EXPORT_INTERVAL`], the same way a blank protocol
/// falls back to grpc. Anything else that is not a positive whole number of
/// milliseconds is rejected rather than ignored, for the same reason an
/// unsupported protocol is: the caller fails open to no metrics at all, which
/// is visible, instead of silently exporting on a cadence nobody selected.
fn export_interval_from_value(raw: Option<String>) -> Result<Duration, OtlpMetricsInitError> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(EXPORT_INTERVAL);
    };
    let millis = raw
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|millis| *millis > 0)
        .ok_or(OtlpMetricsInitError::InvalidExportInterval { raw })?;
    Ok(Duration::from_millis(millis))
}

fn configured_temporality() -> Result<Temporality, OtlpMetricsInitError> {
    temporality_from_value(optional_env(METRICS_TEMPORALITY_ENV)?)
}

/// Read OpenTelemetry's standard temporality preference, defaulting to delta.
///
/// The SDK's own default is cumulative, but delta is what Datadog's OTLP
/// intake accepts, and Datadog drops the first point of every cumulative
/// series, so production keeps delta unless a deployment opts out. A
/// Prometheus-style pipeline can select `cumulative`: its Collector then has
/// no delta-to-cumulative state to lose, so a counter that stays idle past the
/// Collector's stale window keeps its running total instead of restarting from
/// its latest increment. The values and their per-instrument meaning are the
/// OpenTelemetry specification's. Blank keeps the default, and anything else
/// is rejected for the same reason an unusable export interval is.
fn temporality_from_value(raw: Option<String>) -> Result<Temporality, OtlpMetricsInitError> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(Temporality::Delta);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "cumulative" => Ok(Temporality::Cumulative),
        "delta" => Ok(Temporality::Delta),
        "lowmemory" => Ok(Temporality::LowMemory),
        _ => Err(OtlpMetricsInitError::InvalidTemporality { raw }),
    }
}

fn temporality_name(temporality: Temporality) -> &'static str {
    match temporality {
        Temporality::Cumulative => "cumulative",
        Temporality::Delta => "delta",
        Temporality::LowMemory => "lowmemory",
        _ => "unknown",
    }
}

fn validate_plaintext_headers() -> Result<(), OtlpMetricsInitError> {
    validate_plaintext_header_values(
        optional_env(OTLP_METRICS_HEADERS_ENV)?,
        optional_env(OTLP_HEADERS_ENV)?,
    )
}

fn validate_plaintext_header_values(
    signal_specific: Option<String>,
    general: Option<String>,
) -> Result<(), OtlpMetricsInitError> {
    if preferred_nonempty(signal_specific, general).is_some() {
        return Err(OtlpMetricsInitError::HeadersOnPlaintextEndpoint);
    }
    Ok(())
}

fn validate_protocol_values(
    signal_specific: Option<String>,
    general: Option<String>,
) -> Result<(), OtlpMetricsInitError> {
    if let Some(raw) = preferred_nonempty(signal_specific, general)
        && !raw.trim().eq_ignore_ascii_case("grpc")
    {
        return Err(OtlpMetricsInitError::UnsupportedProtocol { raw });
    }
    Ok(())
}

fn preferred_nonempty(signal_specific: Option<String>, general: Option<String>) -> Option<String> {
    // A non-empty signal-specific setting wins; an empty setting behaves as
    // unset and falls back to the general OTLP setting.
    match signal_specific.filter(|value| !value.trim().is_empty()) {
        Some(value) => Some(value),
        None => general.filter(|value| !value.trim().is_empty()),
    }
}

fn optional_env(name: &'static str) -> Result<Option<String>, OtlpMetricsInitError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(source) => Err(OtlpMetricsInitError::EnvVar { name, source }),
    }
}

fn validate_stream_config(cardinality_limit: usize) -> Result<(), OtlpMetricsInitError> {
    Stream::builder()
        .with_aggregation(Aggregation::Base2ExponentialHistogram {
            max_size: HISTOGRAM_MAX_SIZE,
            max_scale: HISTOGRAM_MAX_SCALE,
            record_min_max: true,
        })
        .with_cardinality_limit(cardinality_limit)
        .build()
        .map(|_| ())
        .map_err(|error| OtlpMetricsInitError::Stream(error.to_string()))
}

fn metric_resource(identity: &Identity, constant_tags: &[String]) -> Resource {
    identity.metric_resource(tag_value(constant_tags, "cluster_id").as_deref())
}

fn tag_value(tags: &[String], key: &str) -> Option<String> {
    tags.iter().find_map(|tag| {
        tag.split_once(':')
            .filter(|(candidate, _)| *candidate == key)
            .map(|(_, value)| value.to_string())
    })
}

fn tag_attribute(tag: &str) -> KeyValue {
    match tag.split_once(':') {
        Some((key, value)) => KeyValue::new(key.to_string(), value.to_string()),
        None => KeyValue::new(tag.to_string(), true),
    }
}

fn metric_point_attribute(tag: &str) -> Option<KeyValue> {
    let key = tag.split_once(':').map_or(tag, |(key, _)| key);
    // The canonical OTel resource owns these universal identity fields, the
    // service as `service.name`. Keeping point-level copies makes receivers
    // such as the Datadog Agent surface duplicate `env`, `service` and
    // `version` tags with ambiguous precedence, and gives Prometheus a
    // `service` label beside `service_name` that says the same thing. A
    // metric's own `service` tag is a different field, `service.role`.
    if matches!(key, "env" | "service" | "version") {
        return None;
    }
    Some(tag_attribute(tag))
}

/// Stripped names another metric already emits under its own name.
///
/// Each entry is the counter half of a counter/duration pair emitted together
/// for one event. Removing `_ms` from the histogram would put two instruments
/// of different kinds and units under one OTLP name, and the instrument cache
/// is keyed by the product name, so its conflict guard would never see it.
/// Prometheus happens to keep them apart by appending `_total` and
/// `_milliseconds`, but a backend that adds no suffixes -- Datadog's OTLP
/// ingest, or Prometheus in UTF-8/NoTranslation mode -- would merge or reject
/// them. These two histograms therefore keep the suffix they publish today.
///
/// `otlp_names_stay_unique_across_the_catalog` fails if a new metric would
/// collide, so an addition is decided rather than discovered.
const NAMES_ANOTHER_METRIC_EMITS: &[&str] =
    &["data_plane.ingress.preparation", "gateway.pending_tcp.wait"];

/// Remove the unit abbreviation that [`wire_unit`] has already declared.
///
/// DogStatsD has no unit field, so the product names carry the unit in the
/// name. OTLP has one, and a Prometheus-style backend appends the declared
/// unit's full word -- skipping it only when the name already ends in that
/// same word. `queue.lag_seconds` is therefore fine, but an abbreviation does
/// not match, so `api.latency_ms` with unit `ms` arrives as
/// `..._latency_ms_milliseconds`.
///
/// Only the abbreviations observed to double are removed. `_bytes`,
/// `_seconds`, `_milliseconds` and `_mib` already match the appended word and
/// are left alone.
fn without_declared_unit<'a>(name: &'a str, unit: &str) -> Cow<'a, str> {
    // `mib_per_sec` can sit mid-name (`read_bandwidth_mib_per_sec_max`), so it
    // is removed wherever it appears; the rest are trailing tokens.
    let (infix, trailing): (&[&str], &[&str]) = match unit {
        "ms" => (&[], &["_ms"]),
        "s" => (&[], &["_secs"]),
        "MiBy/s" => (&["_mib_per_sec"], &["_mib_s"]),
        _ => return Cow::Borrowed(name),
    };
    for token in trailing {
        if let Some(stripped) = name.strip_suffix(token)
            && !stripped.is_empty()
            && !stripped.ends_with('.')
            && !NAMES_ANOTHER_METRIC_EMITS.contains(&stripped)
        {
            return Cow::Borrowed(stripped);
        }
    }
    for token in infix {
        // `replacen` avoids slicing by byte index, which clippy rejects as
        // unsound on a UTF-8 boundary even where the tokens are ASCII.
        let joined = name.replacen(token, "", 1);
        // Never leave an empty name or a dangling separator behind.
        if joined != name
            && !joined.is_empty()
            && !joined.ends_with('.')
            && !joined.starts_with('.')
            && !NAMES_ANOTHER_METRIC_EMITS.contains(&joined.as_str())
        {
            return Cow::Owned(joined);
        }
    }
    Cow::Borrowed(name)
}

fn wire_unit(name: &str, unit: MetricUnit) -> &'static str {
    if unit == MetricUnit::Milliseconds {
        return "ms";
    }

    let tokens: Vec<&str> = name.split(['.', '_']).collect();
    let has = |candidate: &str| tokens.contains(&candidate);
    let mebibytes_per_second = tokens
        .windows(3)
        .any(|parts| parts == ["mib", "per", "sec"])
        || tokens.windows(2).any(|parts| parts == ["mib", "s"]);

    if mebibytes_per_second {
        "MiBy/s"
    } else if has("ms") || has("milliseconds") {
        "ms"
    } else if has("secs") || has("seconds") {
        "s"
    } else if has("bytes") {
        "By"
    } else if has("mib") {
        "MiBy"
    } else if has("fraction") || has("ratio") || has("utilization") {
        // UCUM `1` is dimensionless, which a Prometheus-style backend renders
        // as a `_ratio` suffix. That is right for a fraction of a whole, which
        // is what all three tokens name: `db.pool.utilization` is the in-use
        // fraction of each pool's own `max_connections` cap, 0.0-1.0.
        "1"
    } else {
        // Everything else measures a count, a depth, or a flag. Declaring `1`
        // for those made Prometheus publish `..._queue_depth_ratio`, reading
        // as a proportion the value never was. No unit is the honest answer:
        // the backend appends nothing and the name stands on its own.
        ""
    }
}

fn warn_once(flag: &AtomicBool, message: &'static str) {
    if !flag.swap(true, Ordering::AcqRel) {
        tracing::warn!("{message}");
    }
}

#[cfg(test)]
mod tests;
