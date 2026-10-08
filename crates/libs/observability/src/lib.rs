use std::fmt;
use std::io::IsTerminal;
use std::sync::OnceLock;
use std::time::Instant;

#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider as _;
use tracing_log::NormalizeEvent as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::FilterExt as _;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, JsonFields, Writer};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
// The shared layout attaches a subscriber-level filter with `.with`, so this
// is needed whether or not a trace layer is compiled in.
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::util::SubscriberInitExt as _;

pub mod attribution;
pub mod identity;
pub mod logging;
/// The endpoint rule shared by the OTLP metrics and trace exporters.
#[cfg(feature = "otlp-metrics")]
mod otlp_endpoint;
mod selection;
mod sink;
#[cfg(feature = "otel")]
mod sqlx_otel;
#[cfg(any(test, feature = "test-capture"))]
pub mod test_capture;
mod traces;

const RUSQLITE_QUERY_TARGET: &str = "rusqlite::query";
const SQLX_ACQUIRE_TARGET: &str = "sqlx::pool::acquire";
const SQLX_QUERY_TARGET: &str = "sqlx::query";

/// Canonical metric name for successful SQLx pool-acquire duration.
///
/// The tracing event bridge must emit this below `pplx-space-metrics` because
/// that facade depends on this crate. `pplx_space_metrics::db::pool` re-exports
/// the name so all pool telemetry remains discoverable from the pool module.
pub const DB_POOL_ACQUIRE_TOTAL_MS_METRIC: &str = "db.pool.acquire_total_ms";

static METRICS: OnceLock<Box<dyn sink::MetricSink>> = OnceLock::new();
static GIT_SHA: OnceLock<String> = OnceLock::new();

const GIT_SHA_PATHS: &[&str] = &["/etc/space/build-git-sha", "/space/etc/build-git-sha"];

pub use sink::METRICS_TARGET;

/// Workspace package version stamped into every SPACE binary at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Git SHA of the deployed image, resolved at runtime so commit identity does
/// not make otherwise-identical release binaries miss the compiler cache.
///
/// Images write the revision to `/etc/space/build-git-sha`; artifact bundles
/// installed below `/space` use `/space/etc/build-git-sha`. An explicit
/// `SPACE_GIT_SHA` runtime environment variable takes precedence. Local runs
/// without deployment metadata report `"unknown"`.
pub fn git_sha() -> &'static str {
    GIT_SHA.get_or_init(resolve_git_sha)
}

fn resolve_git_sha() -> String {
    select_git_sha(
        std::env::var("SPACE_GIT_SHA").ok(),
        GIT_SHA_PATHS
            .iter()
            .map(|path| std::fs::read_to_string(path).ok()),
    )
}

fn select_git_sha(
    env_sha: Option<String>,
    file_shas: impl IntoIterator<Item = Option<String>>,
) -> String {
    nonempty_git_sha(env_sha)
        .or_else(|| file_shas.into_iter().find_map(nonempty_git_sha))
        .unwrap_or_else(|| "unknown".to_string())
}

fn nonempty_git_sha(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Tracing target used by [`log_startup_banner`]. Filter on this target if you
/// want the banner to surface even when the rest of the process is at a
/// quieter default level (CLI tools default to `warn`).
pub const STARTUP_TARGET: &str = "space::startup";

/// Emit the canonical SPACE startup banner: a single `tracing::info!` with the
/// service name, workspace version, and git SHA.
///
/// `init` calls this for daemons after the subscriber is installed; CLI
/// helpers in `pplx-space-common-cli` call it the same way after their own
/// subscriber bootstrap. Code that initializes its own subscriber can call
/// this directly.
pub fn log_startup_banner(service_name: &str) {
    let git_sha = git_sha();
    tracing::info!(
        target: STARTUP_TARGET,
        service = service_name,
        version = VERSION,
        git_sha,
        "{service_name} starting up (version={VERSION}, git_sha={git_sha})",
    );
}

#[derive(Clone, Debug, Default)]
pub struct LogContext {
    sandbox_id: Option<String>,
    boot_type: Option<String>,
}

impl LogContext {
    pub fn with_sandbox_id(mut self, sandbox_id: impl Into<String>) -> Self {
        self.sandbox_id = Some(sandbox_id.into());
        self
    }

    pub fn with_boot_type(mut self, boot_type: impl Into<String>) -> Self {
        self.boot_type = Some(boot_type.into());
        self
    }
}

#[derive(Clone, Debug)]
struct JsonLogContext {
    service: String,
    environment: Option<String>,
    version: Option<String>,
    cluster_id: Option<String>,
    sandbox_id: Option<String>,
    boot_type: Option<String>,
}

impl JsonLogContext {
    fn new(service_name: &str, context: LogContext) -> Self {
        Self {
            service: service_name.to_string(),
            environment: logging::environment(),
            version: logging::version(),
            cluster_id: pplx_space_common_core::types::process_cluster_id()
                .map(|id| id.as_str().to_string()),
            sandbox_id: context.sandbox_id,
            boot_type: context.boot_type,
        }
    }

    fn insert_into(&self, values: &mut serde_json::Map<String, serde_json::Value>) {
        attribution::AttributionPolicy::current().insert_log_service(values, &self.service);
        for (key, value) in [
            ("env", &self.environment),
            ("version", &self.version),
            ("cluster_id", &self.cluster_id),
        ] {
            if let Some(value) = value {
                insert_string_if_missing(values, key, value);
            }
        }
        if let Some(sandbox_id) = &self.sandbox_id {
            insert_string_if_missing(values, "sandbox_id", sandbox_id);
        }
        if let Some(boot_type) = &self.boot_type {
            insert_string_if_missing(values, "boot_type", boot_type);
        }
    }
}

fn insert_string_if_missing(
    values: &mut serde_json::Map<String, serde_json::Value>,
    key: &'static str,
    value: &str,
) {
    values
        .entry(key)
        .or_insert_with(|| serde_json::Value::String(value.to_string()));
}

#[derive(Clone, Debug)]
struct SpaceJsonFormat {
    context: JsonLogContext,
    timer: SystemTime,
}

impl SpaceJsonFormat {
    fn new(service_name: &str, context: LogContext) -> Self {
        Self {
            context: JsonLogContext::new(service_name, context),
            timer: SystemTime,
        }
    }
}

impl<S, N> FormatEvent<S, N> for SpaceJsonFormat
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let mut timestamp = String::new();
        self.timer.format_time(&mut Writer::new(&mut timestamp))?;

        let normalized_metadata = event.normalized_metadata();
        let metadata = normalized_metadata
            .as_ref()
            .unwrap_or_else(|| event.metadata());
        let mut values = serde_json::Map::new();
        values.insert(
            "timestamp".to_string(),
            serde_json::Value::String(timestamp),
        );
        values.insert(
            "level".to_string(),
            serde_json::Value::String(metadata.level().to_string()),
        );

        event.record(&mut JsonEventVisitor {
            values: &mut values,
        });
        self.context.insert_into(&mut values);
        insert_string_if_missing(&mut values, "target", metadata.target());

        if let Some(span) = ctx.parent_span() {
            values.insert("span".to_string(), span_value::<S, N>(&span));
        }

        let line = serde_json::to_string(&values).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

struct JsonEventVisitor<'a> {
    values: &'a mut serde_json::Map<String, serde_json::Value>,
}

impl tracing::field::Visit for JsonEventVisitor<'_> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.values
            .insert(field_name(field), serde_json::Value::from(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.values
            .insert(field_name(field), serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.values
            .insert(field_name(field), serde_json::Value::from(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.values
            .insert(field_name(field), serde_json::Value::from(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.values
            .insert(field_name(field), serde_json::Value::from(value));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.values.insert(
            field_name(field),
            serde_json::Value::String(format!("{value:?}")),
        );
    }
}

fn field_name(field: &tracing::field::Field) -> String {
    field
        .name()
        .strip_prefix("r#")
        .unwrap_or(field.name())
        .to_string()
}

fn span_value<S, N>(span: &SpanRef<'_, S>) -> serde_json::Value
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    let mut values = span
        .extensions()
        .get::<FormattedFields<N>>()
        .and_then(|fields| serde_json::from_str::<serde_json::Value>(fields).ok())
        .and_then(|value| match value {
            serde_json::Value::Object(values) => Some(values),
            _ => None,
        })
        .unwrap_or_default();

    values.insert(
        "name".to_string(),
        serde_json::Value::String(span.metadata().name().to_string()),
    );
    serde_json::Value::Object(values)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogFormat {
    Json,
    Compact,
    Pretty,
}

#[derive(Debug, thiserror::Error)]
pub enum ObservabilityError {
    #[error("invalid SPACE_LOG_FORMAT={raw:?}; expected one of: json, compact, pretty, text")]
    InvalidLogFormat { raw: String },
    #[error(
        "invalid SPACE_METRICS_SINK={raw:?}; expected one of: auto, datadog, journald, otlp, off"
    )]
    InvalidMetricsSink { raw: String },
    #[error("SPACE_METRICS_SINK={raw:?} requires the datadog feature")]
    DatadogMetricsSinkUnavailable { raw: String },
    #[error("SPACE_METRICS_SINK={raw:?} requires the otlp-metrics feature")]
    OtlpMetricsSinkUnavailable { raw: String },
    #[error("invalid SPACE_TRACE_EXPORTER={raw:?}; expected auto, datadog, otlp, or off")]
    InvalidTraceExporter { raw: String },
    // The reason names the offending setting; it never carries a header or
    // endpoint value.
    #[error("invalid OTLP metrics configuration: {reason}")]
    InvalidOtlpMetricsConfig { reason: String },
    #[error("invalid SPACE_TRACE_FILTER; expected tracing EnvFilter syntax")]
    InvalidTraceFilter,
    #[error("trace exporter {exporter} requires the {feature} feature")]
    TraceExporterUnavailable {
        exporter: &'static str,
        feature: &'static str,
    },
    #[error("trace initialization failed: {0}")]
    TraceInit(#[from] traces::TraceInitError),
    #[cfg(feature = "otel")]
    #[error("trace shutdown failed: {0}")]
    TraceShutdown(#[source] opentelemetry_sdk::error::OTelSdkError),
    #[cfg(any(feature = "otel", feature = "otlp-metrics"))]
    #[error("telemetry shutdown task failed: {0}")]
    ShutdownTask(#[source] tokio::task::JoinError),
    #[error(transparent)]
    EnvVar(#[from] std::env::VarError),
    #[error("failed to initialize tracing subscriber: {0}")]
    SubscriberInit(String),
}

pub struct TelemetryGuard {
    shutdown: Option<Box<dyn FnOnce() -> Result<(), ObservabilityError> + Send + 'static>>,
    closed: bool,
}

impl TelemetryGuard {
    /// Drain telemetry before the Tokio runtime stops. In async code, call this
    /// on `tokio::task::spawn_blocking` so the exporter can still make progress.
    pub fn shutdown(mut self) -> Result<(), ObservabilityError> {
        self.shutdown_inner()
    }

    /// Drain exporters without blocking a Tokio worker. Await this before the
    /// runtime exits so pending tonic requests can complete.
    pub async fn shutdown_async(self) -> Result<(), ObservabilityError> {
        #[cfg(any(feature = "otel", feature = "otlp-metrics"))]
        {
            tokio::task::spawn_blocking(move || self.shutdown())
                .await
                .map_err(ObservabilityError::ShutdownTask)?
        }
        #[cfg(not(any(feature = "otel", feature = "otlp-metrics")))]
        self.shutdown()
    }

    fn shutdown_inner(&mut self) -> Result<(), ObservabilityError> {
        if std::mem::replace(&mut self.closed, true) {
            return Ok(());
        }
        sink::shutdown_metrics();
        if let Some(shutdown) = self.shutdown.take() {
            shutdown()?;
        }
        Ok(())
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown_inner() {
            tracing::warn!(%error, "telemetry shutdown failed");
        }
    }
}

impl LogFormat {
    fn from_env() -> Result<Self, ObservabilityError> {
        match std::env::var("SPACE_LOG_FORMAT") {
            Ok(raw) => Self::parse(&raw).ok_or(ObservabilityError::InvalidLogFormat { raw }),
            Err(std::env::VarError::NotPresent) => Ok(Self::default_for_environment()),
            Err(err) => Err(err.into()),
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Some(Self::Json),
            "compact" | "text" => Some(Self::Compact),
            "pretty" => Some(Self::Pretty),
            _ => None,
        }
    }

    fn default_for_environment() -> Self {
        if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
            || deployment_env_is_json(std::env::var("ENVIRONMENT"))
            || deployment_env_is_json(std::env::var("SPACE_ENV"))
        {
            Self::Json
        } else {
            Self::Compact
        }
    }
}

fn deployment_env_is_json(value: Result<String, std::env::VarError>) -> bool {
    matches!(
        value.ok().as_deref().map(str::trim).map(str::to_ascii_lowercase),
        Some(env) if matches!(env.as_str(), "aws" | "prod" | "production")
    )
}

pub fn init(service_name: &str) -> Result<TelemetryGuard, ObservabilityError> {
    init_with_log_context(service_name, LogContext::default())
}

pub fn init_with_log_context(
    service_name: &str,
    log_context: LogContext,
) -> Result<TelemetryGuard, ObservabilityError> {
    let identity = identity::Identity::from_env(service_name);
    let log_service_name = identity.log_service_name();
    let trace_filter = configured_trace_filter()?;
    let (trace_exporter, trace_selection) = traces::TraceExporter::from_env()?;
    let log_format = LogFormat::from_env()?;
    let (metrics_sink_choice, metrics_selection) = sink::metrics_sink_choice_from_env()?;
    // An explicitly selected sink that can never work fails here. An
    // unreachable receiver is a runtime condition handled when the sink is
    // built, and so is a configuration `auto` chose.
    sink::validate_metrics_sink_env(metrics_sink_choice, metrics_selection)?;
    let constant_metric_tags = sink::constant_tags_from_env(&identity);
    // tracing-subscriber's fmt layer writes to stdout by default, so the
    // ANSI decision must follow stdout's TTY status. Checking stderr would
    // surface escape codes when stdout is piped to a file/journal.
    let use_ansi = std::io::stdout().is_terminal();

    // `auto` must not stop a process from starting over a destination the
    // operator never named. The warning waits for the subscriber that is
    // built below, since there is nowhere to send it yet.
    #[cfg(feature = "otel")]
    let (provider, unusable_auto_traces) = match traces::build_provider(trace_exporter, &identity) {
        Ok(provider) => (provider, None),
        Err(error) if trace_selection.is_auto() => (None, Some(error)),
        Err(error) => return Err(error),
    };
    // Selection is already resolved and validated, while sink construction
    // intentionally waits for logging. Publish the policy before installing
    // any layer so startup logs and spans cannot observe a provisional mode.
    attribution::init(metrics_sink_choice, trace_exporter);
    #[cfg(feature = "otel")]
    let shutdown = if let Some(tracer_provider) = provider {
        let tracer = tracer_provider.tracer(identity.service_name().to_string());
        // SQL event spans also use the global provider; keep them on the same
        // pipeline as tracing spans and W3C propagation.
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        opentelemetry::global::set_text_map_propagator(text_map_propagator());
        match trace_filter {
            // Per-layer selection, which only a configured trace filter needs.
            Some(trace_filter) => {
                init_tracing_subscriber(
                    tracing_subscriber::registry()
                        .with(
                            // Pool-acquire events also feed metrics,
                            // independently of log or trace verbosity;
                            // `db_layer_filter` admits them without narrowing
                            // anything the operator set, and the layer asks
                            // the operator's own filter before exporting.
                            sqlx_otel::DbQueryOtelLayer::new(trace_filter.clone())
                                .with_filter(db_layer_filter(trace_filter.clone())),
                        )
                        .with(
                            tracing_opentelemetry::layer()
                                .with_tracer(tracer)
                                .with_filter(layer_filter(trace_filter)),
                        ),
                    log_service_name,
                    log_context,
                    log_format,
                    use_ansi,
                    // The slow-acquire WARN is the only per-pod signal for a
                    // saturated pool, so it reaches logs at any RUST_LOG. The
                    // shared layout gets this from its registry-level filter.
                    Some(log_filter_from_env().add_directive(acquire_directive()?)),
                )?
            }
            // The shared layout keeps its single subscriber-level filter, which
            // governs logs and spans together. That filter is also the trace
            // selection here, so the bridge is given a copy of it: the forced
            // acquire admission below must not decide emission in this layout
            // either.
            None => init_tracing_subscriber(
                tracing_subscriber::registry()
                    .with(log_filter_from_env().add_directive(acquire_directive()?))
                    .with(sqlx_otel::DbQueryOtelLayer::new(log_filter_from_env()))
                    .with(tracing_opentelemetry::layer().with_tracer(tracer)),
                log_service_name,
                log_context,
                log_format,
                use_ansi,
                None,
            )?,
        }
        Some(Box::new(move || {
            tracer_provider
                .shutdown_with_timeout(std::time::Duration::from_secs(5))
                .map_err(ObservabilityError::TraceShutdown)
        })
            as Box<
                dyn FnOnce() -> Result<(), ObservabilityError> + Send + 'static,
            >)
    } else {
        match trace_filter {
            Some(trace_filter) => {
                init_tracing_subscriber(
                    tracing_subscriber::registry()
                        // The SQL bridge also records pool-acquire metrics.
                        // Keep it installed when only trace export is disabled.
                        .with(
                            // Pool-acquire events also feed metrics,
                            // independently of log or trace verbosity;
                            // `db_layer_filter` admits them without narrowing
                            // anything the operator set, and the layer asks
                            // the operator's own filter before exporting.
                            sqlx_otel::DbQueryOtelLayer::new(trace_filter.clone())
                                .with_filter(db_layer_filter(trace_filter.clone())),
                        ),
                    log_service_name,
                    log_context,
                    log_format,
                    use_ansi,
                    Some(log_filter_from_env().add_directive(acquire_directive()?)),
                )?
            }
            None => init_tracing_subscriber(
                tracing_subscriber::registry()
                    .with(log_filter_from_env().add_directive(acquire_directive()?))
                    .with(sqlx_otel::DbQueryOtelLayer::new(log_filter_from_env())),
                log_service_name,
                log_context,
                log_format,
                use_ansi,
                None,
            )?,
        }
        None
    };

    #[cfg(not(feature = "otel"))]
    let shutdown = {
        // Without a trace pipeline there is no second layer to select for, so
        // the shared layout applies either way. The selector is still parsed
        // and resolved above, so a blank or unknown value fails, and so does
        // an explicit `datadog` or `otlp` that this build cannot serve.
        let _ = (trace_exporter, trace_filter, trace_selection);
        init_tracing_subscriber(
            tracing_subscriber::registry()
                .with(log_filter_from_env().add_directive(acquire_directive()?)),
            log_service_name,
            log_context,
            log_format,
            use_ansi,
            None,
        )?;
        None
    };

    if METRICS.get().is_none() {
        let _ = METRICS.set(sink::build_metrics_sink(
            metrics_sink_choice,
            &identity,
            constant_metric_tags,
        ));
    }
    if let Some(sink) = METRICS.get() {
        tracing::info!(metrics_sink = sink.label(), "metrics: sink initialized");
    }
    // Deferred from before the subscriber existed. The message carries the
    // reason the auto-selected destination was unusable; it names settings,
    // never their values.
    #[cfg(feature = "otel")]
    if let Some(error) = unusable_auto_traces {
        tracing::warn!(
            %error,
            "traces: auto selected an unusable exporter; traces disabled"
        );
    }

    log_startup_banner(log_service_name);

    Ok(TelemetryGuard {
        shutdown,
        closed: false,
    })
}

/// The configured trace filter, or `None` to keep the shared layout.
///
/// One `RUST_LOG`-derived filter governed logs and spans together before trace
/// selection was separated. `None` restores that layout exactly, so an unset
/// `SPACE_TRACE_FILTER` changes neither behavior nor cost; a configured value
/// opts into independent per-layer selection.
fn configured_trace_filter() -> Result<Option<tracing_subscriber::EnvFilter>, ObservabilityError> {
    let raw = match std::env::var("SPACE_TRACE_FILTER") {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => String::new(),
        // Do not retain raw directives or parser errors: field selectors can
        // contain customer values, and callers may log either Display or Debug.
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(ObservabilityError::InvalidTraceFilter);
        }
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .parse(raw)
        .map(Some)
        .map_err(|_| ObservabilityError::InvalidTraceFilter)
}

fn log_filter_from_env() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

/// Keep the pool-acquire target enabled even when a service raises its general
/// threshold: routine records feed the acquire metric/span bridge and are kept
/// out of stdout, while SQLx's slow-acquire WARN still reaches service logs.
fn acquire_directive() -> Result<tracing_subscriber::filter::Directive, ObservabilityError> {
    format!("{SQLX_ACQUIRE_TARGET}=info").parse().map_err(
        |error: tracing_subscriber::filter::ParseError| {
            ObservabilityError::SubscriberInit(error.to_string())
        },
    )
}

/// The database bridge's filter: the trace selection, plus what the
/// pool-acquire metric needs whatever that selection says.
///
/// The metric's driver classification reads `db.system` from an enclosing
/// span, so that span has to stay visible to this layer even when the trace
/// filter excludes it. Otherwise a SQLite migration pool's acquire timing is
/// recorded as PostgreSQL, and trace verbosity decides which database's
/// numbers land in a PostgreSQL metric.
///
/// The exception covers spans only. Query events carry a `db.system` field
/// too, and admitting those would export a query span the operator excluded
/// by target — `rusqlite::query=off` has to keep meaning off. Classification
/// never reads an event's scope-provided identity, so nothing is lost. The
/// predicate is a static per-callsite check, not span state.
#[cfg(feature = "otel")]
fn db_layer_filter<S>(
    filter: tracing_subscriber::EnvFilter,
) -> Box<dyn tracing_subscriber::layer::Filter<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use tracing_subscriber::filter::FilterExt as _;
    // Combined rather than `EnvFilter::add_directive`, which replaces a rule
    // for the same target: forcing `sqlx::pool::acquire=info` onto an operator
    // filter of `sqlx::pool::acquire=trace` narrowed what this layer saw,
    // which is the opposite of what the force is for. Over-admitting is free
    // here because emission is decided by the operator's own filter.
    let acquire = tracing_subscriber::filter::Targets::new().with_target(
        SQLX_ACQUIRE_TARGET,
        tracing::level_filters::LevelFilter::TRACE,
    );
    Box::new(
        layer_filter(filter)
            .or(acquire)
            .or(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.is_span() && metadata.fields().field("db.system").is_some()
            })),
    )
}

fn layer_filter<S>(
    filter: tracing_subscriber::EnvFilter,
) -> Box<dyn tracing_subscriber::layer::Filter<S> + Send + Sync> {
    // EnvFilter takes span-tracking locks even for level/target-only rules.
    // Targets implements those static rules without per-span state. Keep full
    // EnvFilter semantics whenever span/field directives require that state.
    let directives = filter.to_string();
    let statics = if directives.contains('[') {
        None
    } else {
        directives
            .parse::<tracing_subscriber::filter::Targets>()
            .ok()
    };
    match statics {
        Some(statics) => Box::new(statics),
        None => Box::new(filter),
    }
}

fn init_tracing_subscriber<S>(
    subscriber: S,
    service_name: &str,
    log_context: LogContext,
    log_format: LogFormat,
    use_ansi: bool,
    log_filter: Option<tracing_subscriber::EnvFilter>,
) -> Result<(), ObservabilityError>
where
    S: tracing_subscriber::layer::SubscriberExt
        + for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + Send
        + Sync
        + 'static,
{
    // We rely on tracing-subscriber's default writer (stdout). The caller
    // computed `use_ansi` against stdout's TTY status so the fd we write and
    // the fd we test agree.
    let log_filter = match log_filter {
        // The target gate can permit any level. An explicit TRACE hint lets the
        // combined filter retain RUST_LOG's bound and skip disabled callsites.
        Some(filter) => layer_filter(filter)
            .and(
                tracing_subscriber::filter::filter_fn(should_format_event)
                    .with_max_level_hint(tracing::level_filters::LevelFilter::TRACE),
            )
            .boxed(),
        // A subscriber-level filter already applied RUST_LOG, so the fmt layer
        // carries only the target gate, exactly as the shared layout did.
        None => tracing_subscriber::filter::FilterExt::boxed(
            tracing_subscriber::filter::filter_fn(should_format_event),
        ),
    };
    match log_format {
        LogFormat::Json => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(SpaceJsonFormat::new(service_name, log_context))
                    .fmt_fields(JsonFields::new())
                    .with_filter(log_filter),
            )
            .try_init(),
        LogFormat::Compact => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_target(true)
                    .with_ansi(use_ansi)
                    .with_filter(log_filter),
            )
            .try_init(),
        LogFormat::Pretty => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_target(true)
                    .with_ansi(use_ansi)
                    .with_filter(log_filter),
            )
            .try_init(),
    }
    .map_err(|err| ObservabilityError::SubscriberInit(err.to_string()))
}

fn should_format_event(metadata: &tracing::Metadata<'_>) -> bool {
    // SQLx emits routine per-acquire timing and its slow-acquire warning under
    // the same target. Routine sub-WARN records are telemetry input for the
    // acquire metric/span bridge; the WARN record is the only per-pod
    // slow-acquire signal, so it must stay in service logs.
    if metadata.target() == SQLX_ACQUIRE_TARGET {
        return *metadata.level() <= tracing::Level::WARN;
    }
    should_format_target(metadata.target())
}

fn should_format_target(target: &str) -> bool {
    !matches!(
        target,
        RUSQLITE_QUERY_TARGET | SQLX_QUERY_TARGET | sink::METRICS_TARGET
    )
}

#[cfg(feature = "otel")]
fn text_map_propagator() -> opentelemetry::propagation::TextMapCompositePropagator {
    opentelemetry::propagation::TextMapCompositePropagator::new(vec![
        Box::new(opentelemetry_sdk::propagation::TraceContextPropagator::new()),
        Box::new(opentelemetry_sdk::propagation::BaggagePropagator::new()),
    ])
}

pub fn tag(key: &str, value: impl ToString) -> String {
    format!("{key}:{}", value.to_string())
}

pub fn counter(name: &str, value: u64, tags: &[String]) {
    if let Some(sink) = METRICS.get() {
        sink.emit(sink::Metric {
            name,
            kind: sink::MetricKind::Counter,
            unit: sink::MetricUnit::Inferred,
            value: sink::MetricValue::Unsigned(value),
            tags,
        });
    }
}

pub fn gauge(name: &str, value: f64, tags: &[String]) {
    if let Some(sink) = METRICS.get() {
        sink.emit(sink::Metric {
            name,
            kind: sink::MetricKind::Gauge,
            unit: sink::MetricUnit::Inferred,
            value: sink::MetricValue::Float(value),
            tags,
        });
    }
}

pub fn timing_ms(name: &str, value_ms: f64, tags: &[String]) {
    if let Some(sink) = METRICS.get() {
        sink.emit(sink::Metric {
            name,
            kind: sink::MetricKind::Distribution,
            unit: sink::MetricUnit::Milliseconds,
            value: sink::MetricValue::Float(value_ms),
            tags,
        });
    }
}

/// Whether this process exports metrics: false before telemetry starts and
/// when the selected sink is off. Lets periodic samplers skip work whose
/// output nobody receives.
pub fn metrics_enabled() -> bool {
    METRICS.get().is_some_and(|sink| sink.exports())
}

pub fn distribution(name: &str, value: f64, tags: &[String]) {
    if let Some(sink) = METRICS.get() {
        sink.emit(sink::Metric {
            name,
            kind: sink::MetricKind::Distribution,
            unit: sink::MetricUnit::Inferred,
            value: sink::MetricValue::Float(value),
            tags,
        });
    }
}

pub fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1000.0
}

// gRPC trace-context propagation via W3C traceparent headers. Compiled only
// when an OpenTelemetry pipeline is compiled; otherwise the stubs below are no-ops.
#[cfg(feature = "otel")]
pub mod trace_propagation {
    use http::HeaderMap;
    use opentelemetry::propagation::{Extractor, Injector};
    use tonic::metadata::{KeyRef, MetadataKey, MetadataMap, MetadataValue};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    struct MetadataInjector<'a>(&'a mut MetadataMap);

    impl Injector for MetadataInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            if let (Ok(k), Ok(v)) = (
                MetadataKey::from_bytes(key.as_bytes()),
                value.parse::<MetadataValue<_>>(),
            ) {
                self.0.insert(k, v);
            }
        }
    }

    struct MetadataExtractor<'a>(&'a MetadataMap);

    impl Extractor for MetadataExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0
                .keys()
                .map(|k| match k {
                    KeyRef::Ascii(k) => k.as_str(),
                    KeyRef::Binary(k) => k.as_str(),
                })
                .collect()
        }
    }

    struct HeaderExtractor<'a>(&'a HeaderMap);

    impl Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    struct HeaderInjector<'a>(&'a mut HeaderMap);

    impl Injector for HeaderInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            if let (Ok(name), Ok(val)) = (
                http::HeaderName::try_from(key),
                http::HeaderValue::try_from(value),
            ) {
                self.0.insert(name, val);
            }
        }
    }

    /// Inject the current `tracing` span's OpenTelemetry context into outgoing
    /// gRPC metadata so downstream services can link their spans as children.
    pub fn inject_current_context(metadata: &mut MetadataMap) {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut MetadataInjector(metadata));
        });
    }

    /// Tonic client interceptor that injects the current span context into the
    /// outgoing request metadata.
    pub fn inject_current_context_interceptor(
        mut req: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        inject_current_context(req.metadata_mut());
        Ok(req)
    }

    /// Inject the current `tracing` span's OpenTelemetry context into outgoing
    /// HTTP headers. Used by the gRPC client trace layer, which operates at the
    /// HTTP layer (below the tonic codec) where metadata has already been
    /// promoted to `http::HeaderMap`.
    pub fn inject_current_context_into_headers(headers: &mut HeaderMap) {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(headers));
        });
    }

    /// Extract an OpenTelemetry context from incoming gRPC metadata and attach
    /// it as the parent of the current `tracing` span.
    pub fn attach_parent_context_from(metadata: &MetadataMap) {
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&MetadataExtractor(metadata))
        });
        let _ = tracing::Span::current().set_parent(cx);
    }

    /// Extract an OpenTelemetry context from incoming gRPC metadata and attach
    /// it as the parent of an explicit span before that span is entered.
    pub fn set_span_parent_context_from(span: &tracing::Span, metadata: &MetadataMap) {
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&MetadataExtractor(metadata))
        });
        let _ = span.set_parent(cx);
    }

    /// Extract an OpenTelemetry context from incoming HTTP headers and attach
    /// it as the parent of an explicit span before that span is entered.
    pub fn set_span_parent_from_headers(span: &tracing::Span, headers: &HeaderMap) {
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(headers))
        });
        let _ = span.set_parent(cx);
    }

    /// Extract an OpenTelemetry context from a W3C traceparent string and attach
    /// it as the parent of an explicit span before that span is entered.
    pub fn set_span_parent_from_traceparent(span: &tracing::Span, traceparent: &str) {
        use std::collections::HashMap;
        let mut carrier: HashMap<String, String> = HashMap::new();
        carrier.insert("traceparent".to_string(), traceparent.to_string());
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&carrier)
        });
        let _ = span.set_parent(cx);
    }

    /// Link a follower to work dispatched by a different request/process,
    /// without replacing the follower's request parent.
    pub fn link_span_from_traceparent(span: &tracing::Span, traceparent: &str) {
        use opentelemetry::trace::TraceContextExt;
        let carrier =
            std::collections::HashMap::from([("traceparent".to_string(), traceparent.to_string())]);
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&carrier)
        });
        let context_span = cx.span();
        let context = context_span.span_context();
        if context.is_valid() {
            span.add_link(context.clone());
        }
    }

    /// Serialize an explicit tracing span's OpenTelemetry context as W3C
    /// traceparent. This is useful when the child work happens on another task.
    pub fn traceparent_for_span(span: &tracing::Span) -> Option<String> {
        use std::collections::HashMap;
        let mut carrier: HashMap<String, String> = HashMap::new();
        let cx = span.context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut carrier);
        });
        carrier.remove("traceparent")
    }

    /// Serialize the current `tracing` span's OpenTelemetry context as a W3C
    /// traceparent string, suitable for stashing in storage (e.g. a K8s pod
    /// annotation) so an async consumer can resume the trace.
    pub fn current_traceparent() -> Option<String> {
        use std::collections::HashMap;
        let mut carrier: HashMap<String, String> = HashMap::new();
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut carrier);
        });
        carrier.remove("traceparent")
    }

    /// Build an OpenTelemetry context from a W3C traceparent string and attach
    /// it as the parent of the current `tracing` span. Mirror of
    /// `current_traceparent` for the consumer side.
    pub fn attach_parent_from_traceparent(traceparent: &str) {
        use std::collections::HashMap;
        let mut carrier: HashMap<String, String> = HashMap::new();
        carrier.insert("traceparent".to_string(), traceparent.to_string());
        let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&carrier)
        });
        let _ = tracing::Span::current().set_parent(cx);
    }
}

#[cfg(not(feature = "otel"))]
pub mod trace_propagation {
    /// No-op propagation when tracing support is not compiled. Generic over the metadata
    /// type so callers don't need a direct tonic dep guard.
    pub fn inject_current_context<T>(_metadata: &mut T) {}
    pub fn inject_current_context_into_headers<T>(_headers: &mut T) {}
    pub fn attach_parent_context_from<T>(_metadata: &T) {}
    pub fn set_span_parent_context_from<T>(_span: &tracing::Span, _metadata: &T) {}
    pub fn set_span_parent_from_headers<T>(_span: &tracing::Span, _headers: &T) {}
    pub fn set_span_parent_from_traceparent(_span: &tracing::Span, _traceparent: &str) {}
    pub fn link_span_from_traceparent(_span: &tracing::Span, _traceparent: &str) {}
    pub fn traceparent_for_span(_span: &tracing::Span) -> Option<String> {
        None
    }
    pub fn current_traceparent() -> Option<String> {
        None
    }
    pub fn attach_parent_from_traceparent(_traceparent: &str) {}
}

#[cfg(all(test, feature = "otel"))]
mod tests;

#[cfg(all(test, feature = "otel"))]
mod trace_parity_fixture;

#[cfg(test)]
mod log_filter_tests;
