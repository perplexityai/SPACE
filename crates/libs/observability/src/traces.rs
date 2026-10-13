use crate::ObservabilityError;
use crate::selection::Selection;
use std::{fmt, str::FromStr};

#[cfg(feature = "otel")]
mod operation_names;
#[cfg(feature = "otel")]
mod origin_sampling;
#[cfg(feature = "otel")]
mod status_messages;

#[derive(Debug, thiserror::Error)]
pub enum TraceInitError {
    #[error("OTLP traces require grpc, got {0:?}")]
    UnsupportedProtocol(String),
    /// Carries the rendered requirement rather than the rejection type: this
    /// error is public and the shared rule lives behind a feature.
    #[error("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT {0}")]
    UnsupportedEndpoint(String),
    #[error(
        "OTLP traces do not allow OTEL_EXPORTER_OTLP_TRACES_HEADERS or OTEL_EXPORTER_OTLP_HEADERS on a plaintext endpoint"
    )]
    HeadersOnPlaintextEndpoint,
    #[error("OTLP/gRPC traces require an active Tokio runtime")]
    MissingRuntime,
    #[cfg(feature = "otel")]
    #[error("failed to build OTLP trace exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TraceExporter {
    Auto,
    Datadog,
    Otlp,
    Off,
}

impl TraceExporter {
    /// Resolve the selector, and report whether the operator named the result.
    ///
    /// The provenance travels with the exporter because a destination `auto`
    /// picked must not fail startup; see [`crate::selection`].
    pub(crate) fn from_env() -> Result<(Self, Selection), ObservabilityError> {
        let raw = match std::env::var("SPACE_TRACE_EXPORTER") {
            Ok(raw) => Some(raw),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        };
        let selection = Selection::of(raw.as_deref());
        let exporter = raw.as_deref().unwrap_or("auto").parse::<Self>()?.resolve(
            cfg!(feature = "datadog"),
            cfg!(feature = "otel"),
            crate::sink::otlp_endpoint_configured("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        )?;
        Ok((exporter, selection))
    }

    /// Shared Rust/Go contract: native Datadog when compiled in, otherwise OTLP
    /// when an endpoint says where to send it, otherwise off.
    ///
    /// Capabilities and endpoint presence are arguments rather than `cfg!`, so
    /// the whole matrix is table-testable from one build.
    pub(super) fn resolve(
        self,
        datadog_enabled: bool,
        otel_enabled: bool,
        otlp_endpoint_configured: bool,
    ) -> Result<Self, ObservabilityError> {
        let exporter = match self {
            Self::Auto if datadog_enabled => Self::Datadog,
            Self::Auto if otel_enabled && otlp_endpoint_configured => Self::Otlp,
            Self::Auto => Self::Off,
            explicit => explicit,
        };
        match exporter {
            Self::Datadog if !datadog_enabled => {
                Err(ObservabilityError::TraceExporterUnavailable {
                    exporter: exporter.as_str(),
                    feature: "datadog",
                })
            }
            Self::Otlp if !otel_enabled => Err(ObservabilityError::TraceExporterUnavailable {
                exporter: exporter.as_str(),
                feature: "otel",
            }),
            _ => Ok(exporter),
        }
    }
}

impl TraceExporter {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Datadog => "datadog",
            Self::Otlp => "otlp",
            Self::Off => "off",
        }
    }
}

impl fmt::Display for TraceExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TraceExporter {
    type Err = ObservabilityError;
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "datadog" => Ok(Self::Datadog),
            "otlp" => Ok(Self::Otlp),
            "off" => Ok(Self::Off),
            _ => Err(ObservabilityError::InvalidTraceExporter { raw: raw.into() }),
        }
    }
}

#[cfg(feature = "otel")]
pub(crate) fn build_provider(
    exporter: TraceExporter,
    identity: &crate::identity::Identity,
) -> Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>, ObservabilityError> {
    // Already resolved by from_env; re-resolving is idempotent for explicit
    // selections and keeps this entry point usable on its own.
    match exporter.resolve(
        cfg!(feature = "datadog"),
        cfg!(feature = "otel"),
        crate::sink::otlp_endpoint_configured("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
    )? {
        TraceExporter::Auto => unreachable!("auto exporter must be resolved"),
        TraceExporter::Off => Ok(None),
        TraceExporter::Otlp => build_otlp_provider(identity).map(Some),
        #[cfg(feature = "datadog")]
        TraceExporter::Datadog => {
            let mut builder = datadog_opentelemetry::configuration::Config::builder();
            // Read before `set_trace_sampling_rules`, which replaces them.
            let configured_rules = builder.build().trace_sampling_rules().to_vec();
            let mut config = builder
                .set_service(identity.service_name().to_string())
                .set_trace_sampling_rules(origin_sampling::datadog_rules(&configured_rules));
            if let Some(env) = identity.trace_environment() {
                config = config.set_env(env.to_string());
            }
            if let Some(version) = identity.trace_version() {
                config = config.set_version(version.to_string());
            }
            let mut tracing = datadog_opentelemetry::tracing().with_config(config.build());
            // Keep the SDK's legacy resource byte-for-byte for DD-only deployments.
            // SPACE settings are handled by the explicit config above. Only
            // replace the resource when OTel inputs need resolving too; sidecars
            // synthesize SPACE names even for otherwise DD-only deployments.
            if identity.has_otel_resource_settings() {
                tracing = tracing.with_resource(identity.trace_resource());
            }
            Ok(Some(tracing.init()))
        }
        #[cfg(not(feature = "datadog"))]
        TraceExporter::Datadog => unreachable!("unavailable exporter rejected by resolve"),
    }
}

/// Where OTLP traces go when nothing says otherwise: the local Agent or
/// Collector's gRPC port.
#[cfg(feature = "otel")]
const DEFAULT_ENDPOINT: &str = "http://localhost:4317";

#[cfg(feature = "otel")]
fn build_otlp_provider(
    identity: &crate::identity::Identity,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, ObservabilityError> {
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;

    // The SDK reads these implicitly. Reject them before it can send credentials
    // over plaintext, without including potentially secret values in errors.
    for name in [
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_HEADERS",
    ] {
        if std::env::var_os(name)
            .is_some_and(|value| value.to_str().is_none_or(|value| !value.trim().is_empty()))
        {
            return Err(TraceInitError::HeadersOnPlaintextEndpoint.into());
        }
    }
    validate_protocol(signal_setting(
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    )?)?;
    let endpoint = endpoint_from_value(signal_setting(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    )?)?;
    tokio::runtime::Handle::try_current().map_err(|_| TraceInitError::MissingRuntime)?;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(TraceInitError::Exporter)?;
    // The upstream builder honors OTEL_BSP_*; the sampler keeps the one
    // OTEL_TRACES_SAMPLER selects for every span that does not follow an
    // origin.
    Ok(SdkTracerProvider::builder()
        .with_sampler(origin_sampling::FollowOrigin::from_env())
        .with_batch_exporter(status_messages::StatusMessages::new(
            operation_names::OperationNames::new(exporter),
        ))
        .with_resource(identity.trace_resource())
        .build())
}

/// Accept the one protocol the plaintext gRPC profile supports.
///
/// Unset or blank means grpc, matching the metrics sink, and the comparison
/// ignores case because OpenTelemetry's own SDKs accept `GRPC`.
#[cfg(feature = "otel")]
fn validate_protocol(raw: Option<String>) -> Result<(), TraceInitError> {
    let Some(raw) = raw else { return Ok(()) };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("grpc") {
        return Ok(());
    }
    Err(TraceInitError::UnsupportedProtocol(raw))
}

/// Resolve the trace endpoint against the rule metrics and the Go shim share.
///
/// Traces keep a default because the Datadog Agent and a Collector both listen
/// on `localhost:4317`; metrics have never had one.
#[cfg(feature = "otel")]
fn endpoint_from_value(raw: Option<String>) -> Result<String, TraceInitError> {
    let endpoint = raw.unwrap_or_else(|| DEFAULT_ENDPOINT.into());
    crate::otlp_endpoint::validate(&endpoint)
        .map_err(|rejection| TraceInitError::UnsupportedEndpoint(rejection.to_string()))?;
    Ok(endpoint)
}

#[cfg(feature = "otel")]
fn signal_setting(signal: &str, common: &str) -> Result<Option<String>, ObservabilityError> {
    for name in [signal, common] {
        match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => return Ok(Some(value)),
            Ok(_) | Err(std::env::VarError::NotPresent) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

#[cfg(test)]
#[path = "traces_tests.rs"]
mod tests;
