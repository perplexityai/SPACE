//! gRPC request parameters: recording parameters callers have reviewed as safe on the request span,
//! and the opt-in, testing-only logging of them when a request fails, truncated to a byte limit.

use super::*;

pub(super) const FAILED_GRPC_REQUEST_LOG_ENV: &str = "testing";

pub(super) const MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES: usize = 16 * 1024;

pub(super) static FAILED_GRPC_REQUEST_LOGGING_ENABLED: OnceLock<bool> = OnceLock::new();

pub fn initialize_failed_grpc_request_logging() {
    let _ = FAILED_GRPC_REQUEST_LOGGING_ENABLED.set(failed_grpc_request_logging_enabled_from_env());
}

pub(super) fn failed_grpc_request_logging_enabled_from_env() -> bool {
    failed_grpc_request_logging_enabled_for_env(
        pplx_space_observability::logging::environment().as_deref(),
    )
}

/// Serialize a list of `(name, value)` request parameters as a JSON object
/// suitable for stashing on a span as `grpc.request.params`.
///
/// JSON is used (rather than `k=v,k=v`) so values containing commas,
/// quotes, or other separators round-trip unambiguously. Keys are emitted
/// in alphabetical order (serde_json's default `Map` is a `BTreeMap`).
pub fn format_grpc_request_params(
    params: impl IntoIterator<Item = (&'static str, String)>,
) -> String {
    let map: serde_json::Map<String, serde_json::Value> = params
        .into_iter()
        .map(|(name, value)| (name.to_string(), serde_json::Value::String(value)))
        .collect();
    serde_json::Value::Object(map).to_string()
}

// `grpc.request.params` is a SPACE-specific span attribute (not OTel semconv).
// It lives under the `grpc.*` prefix alongside the other custom gRPC fields on
// the server span (`grpc.message`, `grpc.error_source`) so consumers can tell
// at a glance which attributes come from OTel's `rpc.*` semantic conventions
// and which are SPACE additions.
/// Record request parameters that have explicitly been declared safe to log.
///
/// gRPC request bodies are omitted by default. Service implementations should
/// call this only with fields reviewed as non-sensitive for the specific RPC.
pub fn record_safe_grpc_request_params(request_params: &str) {
    if request_params.is_empty() {
        return;
    }

    let span = tracing::Span::current();
    span.record("grpc.request.params", request_params);
}

/// Emit testing-only request parameter details for an RPC that returned a
/// non-OK tonic status.
///
/// The caller is responsible for passing only parameters reviewed as safe for
/// logs. This helper deliberately does not log anything outside
/// the testing logging environment (`SPACE_ENV`, with `DD_ENV` fallback).
pub fn log_failed_grpc_request_params(status: &Status, request_params: &str) {
    if !failed_grpc_request_logging_enabled() || request_params.is_empty() {
        return;
    }

    let params_log = grpc_request_params_for_log(request_params);
    match grpc_request_params_log_severity(status.code()) {
        GrpcRequestParamsLogSeverity::Warn => {
            tracing::warn!(
                target: "grpc.server",
                rpc_log_side = "server",
                rpc.grpc.status_code = status.code() as i32,
                grpc.code_name = status.code().description(),
                grpc.message = status.message(),
                grpc.request.params = %params_log.value,
                grpc.request.params_bytes = request_params.len(),
                grpc.request.params_truncated = params_log.truncated,
                "failed gRPC request parameters",
            );
        }
        GrpcRequestParamsLogSeverity::Debug => {
            tracing::debug!(
                target: "grpc.server",
                rpc_log_side = "server",
                rpc.grpc.status_code = status.code() as i32,
                grpc.code_name = status.code().description(),
                grpc.message = status.message(),
                grpc.request.params = %params_log.value,
                grpc.request.params_bytes = request_params.len(),
                grpc.request.params_truncated = params_log.truncated,
                "gRPC request returned application status",
            );
        }
    }
}

pub(super) fn failed_grpc_request_logging_enabled() -> bool {
    *FAILED_GRPC_REQUEST_LOGGING_ENABLED.get_or_init(failed_grpc_request_logging_enabled_from_env)
}

pub(super) fn failed_grpc_request_logging_enabled_for_env(env: Option<&str>) -> bool {
    env.map(str::trim)
        .is_some_and(|env| env.eq_ignore_ascii_case(FAILED_GRPC_REQUEST_LOG_ENV))
}

#[derive(Debug, PartialEq, Eq)]
pub struct TruncatedLogValue {
    pub value: String,
    pub truncated: bool,
}

pub fn truncate_utf8_for_log(value: &str, max_bytes: usize) -> TruncatedLogValue {
    let original_bytes = value.len();
    let truncated = value.len() > max_bytes;
    let logged_value = if truncated {
        let mut end = max_bytes;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        value
            .get(..end)
            .expect("truncation end is adjusted to a UTF-8 character boundary")
    } else {
        value
    };
    let mut value = logged_value.to_string();
    if truncated {
        value.push_str(&format!("... [truncated, original_bytes={original_bytes}]"));
    }

    TruncatedLogValue { value, truncated }
}

pub(super) fn grpc_request_params_for_log(params: &str) -> TruncatedLogValue {
    truncate_utf8_for_log(params, MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GrpcRequestParamsLogSeverity {
    Debug,
    Warn,
}

pub(super) fn grpc_request_params_log_severity(code: tonic::Code) -> GrpcRequestParamsLogSeverity {
    if grpc_status_is_error(code) {
        GrpcRequestParamsLogSeverity::Warn
    } else {
        GrpcRequestParamsLogSeverity::Debug
    }
}
