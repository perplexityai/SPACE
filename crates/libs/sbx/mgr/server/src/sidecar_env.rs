use std::collections::HashMap;

use pplx_space_common_core::types::CLUSTER_ID_ENV;
use pplx_space_common_svc::space_api_route_ed25519::{
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV,
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV,
};
use pplx_space_observability::{identity, logging};

/// Telemetry env forwarded from sandbox-manager to the sidecars it launches
/// (`space-net-gateway`, `space-spaced-host`). These processes exist only on the
/// host's own env, so without this bridge their metrics and sandbox events would
/// carry no cluster identity. Values are forwarded raw; each sidecar validates
/// them on startup.
const TELEMETRY_ENV_KEYS: &[&str] = &[
    CLUSTER_ID_ENV,
    logging::SPACE_ENV,
    logging::SPACE_VERSION,
    // Preserve shared OTel identity across env_clear; the generated SPACE
    // service name still identifies this child rather than its manager.
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
    "DD_ENV",
    "DD_VERSION",
    "DD_AGENT_HOST",
    "DD_DOGSTATSD_PORT",
    // Preserve the APM intake port across env_clear alongside its host.
    "DD_TRACE_AGENT_PORT",
    // Child processes must keep the selected trace transport and sampling
    // policy; an omitted selector would silently restore the native default.
    "SPACE_TRACE_EXPORTER",
    // Trace verbosity is selected independently of RUST_LOG, so the child
    // would fall back to its own default and export a different span set.
    "SPACE_TRACE_FILTER",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
    "OTEL_BSP_SCHEDULE_DELAY",
    "OTEL_BSP_MAX_QUEUE_SIZE",
    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
    "OTEL_BSP_EXPORT_TIMEOUT",
    // Host sidecars share the manager's node, not its pod or hostname. Keep
    // producer IDs distinct across nodes even when their process IDs match.
    "SPACE_NODE_ID",
    "SPACE_K8S_NODE_NAME",
    "KUBE_NODE_NAME",
    // Sidecars have their own process environment. Preserve the selected
    // metrics transport and signal-specific precedence, including invalid
    // settings that the child's exporter must validate rather than ignore.
    "SPACE_METRICS_SINK",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    // A sidecar exports through its own reader, so an explicitly chosen
    // cadence has to cross the launch boundary with the endpoint it applies
    // to; the manager's interval never reaches the child.
    "OTEL_METRIC_EXPORT_INTERVAL",
    // Likewise the temporality: a child left on the delta default would feed
    // a cumulative-only pipeline the stream state its manager was moved off.
    "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
];
const ROUTE_TOKEN_ENV_KEYS: &[&str] = &[
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_ENV,
    SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH_ENV,
];
const SPACE_LOG_FORMAT: &str = "json";
/// The W3C `traceparent` a traced launch passes to the sidecar it starts, the
/// name OpenTelemetry's environment carrier uses.
pub(crate) const LAUNCH_TRACEPARENT_ENV: &str = "TRACEPARENT";

fn insert_non_empty_env_values(
    env: &mut HashMap<String, String>,
    keys: &[&str],
    mut lookup: impl FnMut(&str) -> Option<String>,
) {
    for key in keys {
        if let Some(value) = lookup(key)
            && !value.trim().is_empty()
        {
            env.insert((*key).to_string(), value);
        }
    }
}

pub fn telemetry_env_for_service(service_name: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    let parent_service = std::env::var("DD_SERVICE").ok();
    env.insert(
        "DD_SERVICE".to_string(),
        child_service_name(service_name, parent_service.as_deref()),
    );
    // The shared identity honors explicit SPACE/OTel settings, but retains the
    // raw DD suffix rule for DD-only parents. Trimming that fallback here could
    // change an existing stable Datadog child into a canary.
    let parent_identity = identity::service_name("");
    env.insert(
        logging::SPACE_SERVICE_NAME.to_string(),
        child_service_name(service_name, Some(&parent_identity)),
    );
    env.insert("SPACE_LOG_FORMAT".to_string(), SPACE_LOG_FORMAT.to_string());

    insert_non_empty_env_values(&mut env, TELEMETRY_ENV_KEYS, |key| std::env::var(key).ok());

    env
}

fn child_service_name(service_name: &str, parent_service: Option<&str>) -> String {
    if matches!(parent_service, Some(name) if name.ends_with("-canary")) {
        format!("{service_name}-canary")
    } else {
        service_name.to_string()
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "otel"))]
mod otlp_tests;

pub fn net_gateway_env() -> HashMap<String, String> {
    net_gateway_env_from_environment(&pplx_space_common_core::env::ProcessEnvironment)
}
fn net_gateway_env_from_environment(
    source: &dyn pplx_space_common_core::env::Environment,
) -> HashMap<String, String> {
    let mut env = telemetry_env_for_service("space-net-gateway");
    insert_non_empty_env_values(&mut env, ROUTE_TOKEN_ENV_KEYS, |key| source.var(key).ok());
    env
}

#[cfg(test)]
#[path = "sidecar_env_tests.rs"]
mod sidecar_env_tests;
