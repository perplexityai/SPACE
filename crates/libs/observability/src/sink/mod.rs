#[cfg(feature = "datadog")]
mod datadog;
mod journald;
#[cfg(feature = "otlp-metrics")]
mod otlp;

use pplx_space_common_core::types::process_cluster_id;

use crate::{ObservabilityError, identity::Identity, selection::Selection};
use journald::JournaldSink;

#[cfg(feature = "datadog")]
use datadog::DatadogSink;
#[cfg(feature = "otlp-metrics")]
use otlp::{OtlpEnvConfig, OtlpSink};

const METRICS_NAMESPACE: &str = "space";
const METRICS_SINK_ENV: &str = "SPACE_METRICS_SINK";
pub const METRICS_TARGET: &str = "space::metrics";

pub(super) trait MetricSink: Send + Sync {
    fn label(&self) -> &'static str;
    /// Whether emitted metrics leave the process.
    fn exports(&self) -> bool {
        true
    }
    fn emit(&self, metric: Metric<'_>);
    fn shutdown(&self) {}
}

pub(super) struct Metric<'a> {
    pub name: &'a str,
    pub kind: MetricKind,
    pub unit: MetricUnit,
    pub value: MetricValue,
    pub tags: &'a [String],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricKind {
    Counter,
    Gauge,
    Distribution,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricUnit {
    Inferred,
    Milliseconds,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum MetricValue {
    Unsigned(u64),
    Float(f64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetricsSinkChoice {
    Datadog,
    Journald,
    Otlp,
    Off,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricsSinkMode {
    Auto,
    Datadog,
    Journald,
    Otlp,
    Off,
}

struct OffSink;

impl MetricsSinkMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "datadog" => Some(Self::Datadog),
            "journald" => Some(Self::Journald),
            "otlp" => Some(Self::Otlp),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

impl MetricSink for OffSink {
    fn label(&self) -> &'static str {
        "off"
    }

    fn exports(&self) -> bool {
        false
    }

    fn emit(&self, _metric: Metric<'_>) {}
}

/// Whether a nonblank signal-specific or generic OTLP endpoint is configured.
///
/// `auto` needs this before any exporter is built, so it deliberately does not
/// apply the exporter's stricter scheme and protocol checks: an endpoint that
/// is present but unusable must fail as a configuration error under an
/// explicit selection, not silently turn `auto` into `off`.
pub(crate) fn otlp_endpoint_configured(signal_env: &str) -> bool {
    [signal_env, "OTEL_EXPORTER_OTLP_ENDPOINT"]
        .iter()
        .any(|name| std::env::var(name).is_ok_and(|value| !value.trim().is_empty()))
}

/// Resolve the selector, and report whether the operator named the result.
///
/// The provenance travels with the choice because a sink `auto` picked must
/// not fail startup; see [`crate::selection`].
pub(super) fn metrics_sink_choice_from_env()
-> Result<(MetricsSinkChoice, Selection), ObservabilityError> {
    let raw = match std::env::var(METRICS_SINK_ENV) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    };
    let choice = resolve_metrics_sink_choice(
        raw.as_deref(),
        cfg!(feature = "datadog"),
        cfg!(feature = "otlp-metrics"),
        otlp_endpoint_configured("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
    )?;
    Ok((choice, Selection::of(raw.as_deref())))
}

pub(super) fn build_metrics_sink(
    choice: MetricsSinkChoice,
    identity: &Identity,
    constant_tags: Vec<String>,
) -> Box<dyn MetricSink> {
    match choice {
        MetricsSinkChoice::Datadog => build_datadog_sink(constant_tags),
        MetricsSinkChoice::Journald => Box::new(JournaldSink::new(
            METRICS_NAMESPACE.to_string(),
            constant_tags,
        )),
        MetricsSinkChoice::Otlp => build_otlp_sink(identity, constant_tags),
        MetricsSinkChoice::Off => Box::new(OffSink),
    }
}

fn build_otlp_sink(identity: &Identity, constant_tags: Vec<String>) -> Box<dyn MetricSink> {
    #[cfg(feature = "otlp-metrics")]
    {
        // An explicit selection was already validated at init, so a failure
        // here is either a runtime condition or a configuration `auto` chose
        // and nobody asked for. Warn and disable either way, rather than block
        // startup or sandbox creation.
        let sink = OtlpEnvConfig::from_env()
            .and_then(|env| OtlpSink::from_validated_env(env, identity, constant_tags));
        match sink {
            Ok(sink) => Box::new(sink),
            Err(error) => {
                tracing::warn!(%error, "otlp metrics: initialization failed; metrics disabled");
                Box::new(OffSink)
            }
        }
    }

    #[cfg(not(feature = "otlp-metrics"))]
    {
        let _ = (identity, constant_tags);
        tracing::warn!(
            "OTLP metrics sink requested without otlp-metrics feature; disabling metrics"
        );
        Box::new(OffSink)
    }
}

fn build_datadog_sink(constant_tags: Vec<String>) -> Box<dyn MetricSink> {
    #[cfg(feature = "datadog")]
    {
        DatadogSink::from_env(METRICS_NAMESPACE.to_string(), constant_tags).map_or_else(
            || Box::new(OffSink) as Box<dyn MetricSink>,
            |sink| Box::new(sink) as Box<dyn MetricSink>,
        )
    }

    #[cfg(not(feature = "datadog"))]
    {
        let _ = constant_tags;
        tracing::warn!("datadog metrics sink requested without datadog feature; disabling metrics");
        Box::new(OffSink)
    }
}

pub(super) fn resolve_metrics_sink_choice(
    raw: Option<&str>,
    datadog_enabled: bool,
    otlp_metrics_enabled: bool,
    otlp_endpoint_configured: bool,
) -> Result<MetricsSinkChoice, ObservabilityError> {
    let mode = match raw {
        Some(raw) => {
            MetricsSinkMode::parse(raw).ok_or_else(|| ObservabilityError::InvalidMetricsSink {
                raw: raw.to_string(),
            })?
        }
        None => MetricsSinkMode::Auto,
    };

    match mode {
        MetricsSinkMode::Auto if datadog_enabled => Ok(MetricsSinkChoice::Datadog),
        // Shared Rust/Go contract: native Datadog when compiled in, otherwise
        // OTLP when an endpoint says where to send it, otherwise off. `auto`
        // never selects journald; an explicit journald still works.
        MetricsSinkMode::Auto if otlp_metrics_enabled && otlp_endpoint_configured => {
            Ok(MetricsSinkChoice::Otlp)
        }
        MetricsSinkMode::Auto => Ok(MetricsSinkChoice::Off),
        MetricsSinkMode::Datadog if datadog_enabled => Ok(MetricsSinkChoice::Datadog),
        MetricsSinkMode::Datadog => Err(ObservabilityError::DatadogMetricsSinkUnavailable {
            raw: raw.unwrap_or("datadog").to_string(),
        }),
        MetricsSinkMode::Journald => Ok(MetricsSinkChoice::Journald),
        MetricsSinkMode::Otlp if otlp_metrics_enabled => Ok(MetricsSinkChoice::Otlp),
        MetricsSinkMode::Otlp => Err(ObservabilityError::OtlpMetricsSinkUnavailable {
            raw: raw.unwrap_or("otlp").to_string(),
        }),
        MetricsSinkMode::Off => Ok(MetricsSinkChoice::Off),
    }
}

/// Reject a configuration that can never work, before telemetry starts.
///
/// Only the environment-derived half is checked here; a valid but unreachable
/// receiver is a runtime condition and must not fail initialization.
///
/// A sink `auto` selected is exempt: the operator did not name OTLP, so an
/// unusable OTLP configuration is reported by [`build_otlp_sink`] as a warning
/// with metrics disabled, rather than stopping the process.
pub(super) fn validate_metrics_sink_env(
    choice: MetricsSinkChoice,
    selection: Selection,
) -> Result<(), ObservabilityError> {
    if choice != MetricsSinkChoice::Otlp || selection.is_auto() {
        return Ok(());
    }
    #[cfg(feature = "otlp-metrics")]
    {
        // The message names the setting; it never echoes a header or endpoint.
        OtlpEnvConfig::from_env().map(|_| ()).map_err(|error| {
            ObservabilityError::InvalidOtlpMetricsConfig {
                reason: error.to_string(),
            }
        })
    }
    #[cfg(not(feature = "otlp-metrics"))]
    {
        Ok(())
    }
}

pub(super) fn shutdown_metrics() {
    if let Some(sink) = crate::METRICS.get() {
        sink.shutdown();
    }
}

pub(super) fn constant_tags_from_env(identity: &Identity) -> Vec<String> {
    // Read the shared process-global cluster id (single owner in common-core,
    // also consumed by the sandbox-event stream), so metrics and events never
    // disagree about cluster identity. A malformed value is already dropped +
    // logged once at that single owner, so it never reaches this tag.
    let cluster_id = process_cluster_id().map(|id| id.as_str().to_owned());
    build_constant_tags(identity, cluster_id)
}

/// Builds the constant tag list attached to every metric emission.
///
/// `cluster_id` is the first-class cluster identity (`SPACE_CLUSTER_ID`); it is
/// omitted when unset or empty so local/dev processes without a cluster id do
/// not emit a bare `cluster_id:` tag.
fn build_constant_tags(identity: &Identity, cluster_id: Option<String>) -> Vec<String> {
    let mut constant_tags = Vec::new();
    let [env, service, version] = identity.metric_identity();
    if let Some(env) = env {
        constant_tags.push(format!("env:{env}"));
    }
    if let Some(service) = service {
        constant_tags.push(format!("service:{service}"));
    }
    if let Some(version) = version {
        constant_tags.push(format!("version:{version}"));
    }
    if let Some(cluster_id) = cluster_id.filter(|value| !value.is_empty()) {
        constant_tags.push(format!("cluster_id:{cluster_id}"));
    }
    constant_tags
}

fn all_metric_tags(constant_tags: &[String], tags: &[String]) -> String {
    let mut all_tags = constant_tags.to_vec();
    all_tags.extend_from_slice(tags);
    all_tags.join(",")
}

#[cfg(test)]
mod tests;
