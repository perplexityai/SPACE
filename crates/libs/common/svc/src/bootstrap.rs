use std::borrow::Cow;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::MatchedPath;
use http::{HeaderMap, Method, Request as HttpRequest, Response as HttpResponse};
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tower_http::classify::{
    GrpcCode, GrpcErrorsAsFailures, GrpcFailureClass, ServerErrorsAsFailures,
    ServerErrorsFailureClass, SharedClassifier,
};
use tower_http::trace::{
    DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, GrpcMakeClassifier, MakeSpan, OnEos,
    OnFailure, OnResponse, TraceLayer,
};
use uuid::Uuid;

pub use pplx_space_common_grpc::{
    CLIENT_SERVICE_METADATA_KEY, GrpcClientTrace, current_service_name, set_current_service_name,
    trace_grpc_client_transport as wrap_grpc_client_channel,
};
const UNKNOWN_SERVICE: &str = "unknown";

pub const GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
pub const GRPC_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const GRPC_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(60);
const FAILED_GRPC_REQUEST_LOG_ENV: &str = "testing";
const MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES: usize = 16 * 1024;
static FAILED_GRPC_REQUEST_LOGGING_ENABLED: OnceLock<bool> = OnceLock::new();

pub fn initialize_failed_grpc_request_logging() {
    let _ = FAILED_GRPC_REQUEST_LOGGING_ENABLED.set(failed_grpc_request_logging_enabled_from_env());
}

fn failed_grpc_request_logging_enabled_from_env() -> bool {
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

fn failed_grpc_request_logging_enabled() -> bool {
    *FAILED_GRPC_REQUEST_LOGGING_ENABLED.get_or_init(failed_grpc_request_logging_enabled_from_env)
}

fn failed_grpc_request_logging_enabled_for_env(env: Option<&str>) -> bool {
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

fn grpc_request_params_for_log(params: &str) -> TruncatedLogValue {
    truncate_utf8_for_log(params, MAX_LOGGED_GRPC_REQUEST_PARAMS_BYTES)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GrpcRequestParamsLogSeverity {
    Debug,
    Warn,
}

fn grpc_request_params_log_severity(code: tonic::Code) -> GrpcRequestParamsLogSeverity {
    if grpc_status_is_error(code) {
        GrpcRequestParamsLogSeverity::Warn
    } else {
        GrpcRequestParamsLogSeverity::Debug
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SocketGroup {
    Required(&'static str),
    IfPresent(&'static str),
    BestEffort(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub struct UnixSocketOptions {
    mode: u32,
    group: Option<SocketGroup>,
}

#[derive(Debug, thiserror::Error)]
pub enum BindUnixSocketError {
    #[error("failed to create socket parent directory {path}: {source}")]
    CreateParent {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("refusing to remove non-socket path before binding unix socket: {path}")]
    ExistingPathIsNotSocket { path: std::path::PathBuf },
    #[error("failed to stat unix socket path {path}: {source}")]
    Stat {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind unix socket {path}: {source}")]
    Bind {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind unix socket staging path {path}: {source}")]
    BindStaging {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    StagingPath(#[from] UnixSocketStagingPathError),
    #[error("failed to publish unix socket {path} from staging path {staging_path}: {source}")]
    Publish {
        path: std::path::PathBuf,
        staging_path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Permissions(#[from] ApplyUnixSocketPermissionsError),
    #[error("failed to allocate a unique unix socket staging path after {attempts} attempts")]
    StagingPathExhausted {
        attempts: u8,
        #[source]
        source: Option<Box<BindUnixSocketError>>,
    },
}

impl BindUnixSocketError {
    fn is_address_in_use(&self) -> bool {
        matches!(
            self,
            Self::Bind { source, .. } | Self::BindStaging { source, .. }
                if source.kind() == std::io::ErrorKind::AddrInUse
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnixSocketStagingPathError {
    #[error("unix socket path has no file name: {path}")]
    MissingFileName { path: std::path::PathBuf },
    #[error("unix socket path has an empty file name: {path}")]
    EmptyFileName { path: std::path::PathBuf },
    #[error("unix socket staging nonce digit is out of range")]
    NonceDigitOutOfRange,
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyUnixSocketPermissionsError {
    #[error("failed to chmod unix socket {path}: {source}")]
    Chmod {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to look up unix group '{name}': {source}")]
    LookupGroup {
        name: String,
        #[source]
        source: nix::Error,
    },
    #[error("group '{name}' not found for socket {path}")]
    GroupNotFound {
        name: &'static str,
        path: std::path::PathBuf,
    },
    #[error("failed to set socket group '{name}' on {path}: {source}")]
    SetGroup {
        name: &'static str,
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("failed to look up unix group '{name}': {source}")]
pub struct UnixGroupLookupError {
    name: String,
    #[source]
    source: nix::Error,
}

#[derive(Debug, thiserror::Error)]
pub enum WaitForSignalError {
    #[error("failed to install SIGINT handler: {0}")]
    Sigint(#[source] std::io::Error),
    #[error("failed to install SIGTERM handler: {0}")]
    Sigterm(#[source] std::io::Error),
    #[cfg(not(unix))]
    #[error("failed to install Ctrl-C handler: {0}")]
    CtrlC(#[source] std::io::Error),
}

impl UnixSocketOptions {
    pub const fn new(mode: u32) -> Self {
        Self { mode, group: None }
    }

    pub const fn with_group(self, group: SocketGroup) -> Self {
        Self {
            mode: self.mode,
            group: Some(group),
        }
    }
}

pub fn bind_unix_socket(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<UnixListener, BindUnixSocketError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| BindUnixSocketError::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            return bind_unix_socket_replacement(path, options);
        }
        Ok(_) => {
            return Err(BindUnixSocketError::ExistingPathIsNotSocket {
                path: path.to_path_buf(),
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(BindUnixSocketError::Stat {
                path: path.to_path_buf(),
                source: error,
            });
        }
    }
    let listener = UnixListener::bind(path).map_err(|source| BindUnixSocketError::Bind {
        path: path.to_path_buf(),
        source,
    })?;
    apply_unix_socket_permissions(path, options)?;
    Ok(listener)
}

fn bind_unix_socket_replacement(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<UnixListener, BindUnixSocketError> {
    const MAX_STAGING_BIND_ATTEMPTS: u8 = 64;

    let seed = Uuid::now_v7();
    let mut last_collision = None;
    for attempt in 0..MAX_STAGING_BIND_ATTEMPTS {
        let staging_path = unix_socket_staging_path_for_attempt(path, seed, attempt)?;
        match bind_unix_socket_replacement_at(path, options, &staging_path) {
            Ok(listener) => return Ok(listener),
            Err(error) if error.is_address_in_use() => {
                last_collision = Some(error);
            }
            Err(error) => return Err(error),
        }
    }

    Err(BindUnixSocketError::StagingPathExhausted {
        attempts: MAX_STAGING_BIND_ATTEMPTS,
        source: last_collision.map(Box::new),
    })
}

fn bind_unix_socket_replacement_at(
    path: &Path,
    options: UnixSocketOptions,
    staging_path: &Path,
) -> Result<UnixListener, BindUnixSocketError> {
    let mut owns_staging_path = false;
    let result = (|| {
        let listener = UnixListener::bind(staging_path).map_err(|source| {
            BindUnixSocketError::BindStaging {
                path: staging_path.to_path_buf(),
                source,
            }
        })?;
        owns_staging_path = true;
        apply_unix_socket_permissions(staging_path, options)?;
        std::fs::rename(staging_path, path).map_err(|source| BindUnixSocketError::Publish {
            path: path.to_path_buf(),
            staging_path: staging_path.to_path_buf(),
            source,
        })?;
        Ok(listener)
    })();

    if result.is_err()
        && owns_staging_path
        && let Err(error) = std::fs::remove_file(staging_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            %error,
            staging_path = %staging_path.display(),
            "failed to clean up unix socket staging path"
        );
    }
    result
}

const SOCKET_NONCE_ALPHABET: &[u8; 64] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz-";
const SOCKET_STAGING_PREFIX: &[u8] = b".handoff-";

fn unix_socket_staging_shape(path: &Path) -> Option<(usize, &'static [u8])> {
    let len = path.file_name()?.as_bytes().len().min(25);
    let prefix = if len >= SOCKET_STAGING_PREFIX.len() + 8 {
        SOCKET_STAGING_PREFIX
    } else {
        &[]
    };
    (len > 0).then_some((len, prefix))
}

/// Whether a filename has the staging shape used to replace this socket.
///
/// This classifies names only. Cleanup must separately verify the inode type,
/// ownership and exclusivity of the directory before removing an entry.
pub fn is_unix_socket_staging_name(path: &Path, name: &std::ffi::OsStr) -> bool {
    let Some((len, prefix)) = unix_socket_staging_shape(path) else {
        return false;
    };
    let bytes = name.as_bytes();
    bytes.len() == len
        && bytes.strip_prefix(prefix).is_some_and(|nonce| {
            nonce
                .iter()
                .all(|byte| SOCKET_NONCE_ALPHABET.contains(byte))
        })
}

fn unix_socket_staging_path_for_attempt(
    path: &Path,
    seed: Uuid,
    attempt: u8,
) -> Result<std::path::PathBuf, UnixSocketStagingPathError> {
    let file_name =
        path.file_name()
            .ok_or_else(|| UnixSocketStagingPathError::MissingFileName {
                path: path.to_path_buf(),
            })?;
    let file_name_len = file_name.as_bytes().len();
    if file_name_len == 0 {
        return Err(UnixSocketStagingPathError::EmptyFileName {
            path: path.to_path_buf(),
        });
    }

    let (staging_file_name_len, prefix) = unix_socket_staging_shape(path).ok_or_else(|| {
        UnixSocketStagingPathError::EmptyFileName {
            path: path.to_path_buf(),
        }
    })?;
    let nonce_len = staging_file_name_len - prefix.len();
    let mut staging_file_name = Vec::with_capacity(staging_file_name_len);
    staging_file_name.extend_from_slice(prefix);

    let mut nonce = seed.as_u128().wrapping_add(u128::from(attempt));
    for _ in 0..nonce_len {
        let digit = SOCKET_NONCE_ALPHABET
            .get((nonce & 0x3f) as usize)
            .copied()
            .ok_or(UnixSocketStagingPathError::NonceDigitOutOfRange)?;
        staging_file_name.push(digit);
        nonce >>= 6;
    }

    Ok(path.with_file_name(std::ffi::OsString::from_vec(staging_file_name)))
}

pub fn apply_unix_socket_permissions(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<(), ApplyUnixSocketPermissionsError> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(options.mode)).map_err(
        |source| ApplyUnixSocketPermissionsError::Chmod {
            path: path.to_path_buf(),
            source,
        },
    )?;

    if let Some(group) = options.group {
        apply_socket_group(path, group)?;
    }

    Ok(())
}

fn apply_socket_group(
    path: &Path,
    group: SocketGroup,
) -> Result<(), ApplyUnixSocketPermissionsError> {
    let group_name = match group {
        SocketGroup::Required(name)
        | SocketGroup::IfPresent(name)
        | SocketGroup::BestEffort(name) => name,
    };

    let gid = nix::unistd::Group::from_name(group_name)
        .map(|group| group.map(|group| group.gid.as_raw()))
        .map_err(|source| ApplyUnixSocketPermissionsError::LookupGroup {
            name: group_name.to_owned(),
            source,
        })?;

    let Some(gid) = gid else {
        return match group {
            SocketGroup::Required(name) => Err(ApplyUnixSocketPermissionsError::GroupNotFound {
                name,
                path: path.to_path_buf(),
            }),
            SocketGroup::IfPresent(name) | SocketGroup::BestEffort(name) => {
                tracing::warn!(
                    socket_path = %path.display(),
                    socket_group = name,
                    "socket group missing; socket group unchanged"
                );
                Ok(())
            }
        };
    };
    if let Err(err) = std::os::unix::fs::chown(path, None, Some(gid)) {
        return match group {
            SocketGroup::Required(name) | SocketGroup::IfPresent(name) => {
                Err(ApplyUnixSocketPermissionsError::SetGroup {
                    name,
                    path: path.to_path_buf(),
                    source: err,
                })
            }
            SocketGroup::BestEffort(name) => {
                tracing::warn!(
                    socket_path = %path.display(),
                    socket_group = name,
                    error = %err,
                    "failed to set socket group; socket group unchanged"
                );
                Ok(())
            }
        };
    }

    Ok(())
}

pub fn unix_group_exists(name: &str) -> Result<bool, UnixGroupLookupError> {
    unix_group_gid(name).map(|gid| gid.is_some())
}

fn unix_group_gid(name: &str) -> Result<Option<libc::gid_t>, UnixGroupLookupError> {
    nix::unistd::Group::from_name(name)
        .map(|group| group.map(|group| group.gid.as_raw()))
        .map_err(|source| UnixGroupLookupError {
            name: name.to_owned(),
            source,
        })
}

pub fn grpc_server_builder() -> pplx_space_common_grpc::GrpcServer {
    grpc_server_builder_with_request_timeout(GRPC_REQUEST_TIMEOUT)
}

pub fn grpc_server_builder_with_request_timeout(
    request_timeout: Duration,
) -> pplx_space_common_grpc::GrpcServer {
    pplx_space_common_grpc::grpc_server_builder()
        .timeout(request_timeout)
        .http2_keepalive_interval(Some(GRPC_HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(GRPC_HTTP2_KEEPALIVE_TIMEOUT))
}

pub const REDACTED_HTTP_PATH_VALUE: &str = "[REDACTED]";

pub trait HttpSpanMetadata: Clone + Send + Sync + 'static {
    fn span_path_and_route<'a>(
        &self,
        _method: &Method,
        _headers: &HeaderMap,
        raw_path: &'a str,
        matched_route: Option<&'a str>,
    ) -> (Cow<'a, str>, Cow<'a, str>) {
        (
            Cow::Borrowed(raw_path),
            Cow::Borrowed(matched_route.unwrap_or(raw_path)),
        )
    }

    fn record_extra_fields(
        &self,
        _headers: &HeaderMap,
        _raw_path: &str,
        _matched_route: Option<&str>,
        _span: &tracing::Span,
    ) {
    }

    /// Whether this request gets no `http.server` span: by default a probe
    /// ([`is_probe_request`]). A surface that also serves user traffic on the
    /// probe paths overrides this so that traffic stays traced.
    fn is_untraced(&self, method: &Method, _headers: &HeaderMap, raw_path: &str) -> bool {
        is_probe_request(method, raw_path)
    }
}

/// A kubelet or load-balancer probe: a GET of one of [`UNTRACED_HTTP_PATHS`].
pub fn is_probe_request(method: &Method, raw_path: &str) -> bool {
    method == Method::GET && UNTRACED_HTTP_PATHS.contains(&raw_path)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultHttpSpanMetadata;

impl HttpSpanMetadata for DefaultHttpSpanMetadata {}

/// Probe endpoints whose GETs get no `http.server` span by default. Kubelets
/// and load balancers call them every few seconds, so their traces would
/// outnumber every request's, and monitors already exclude them.
const UNTRACED_HTTP_PATHS: [&str; 4] = ["/health", "/healthz", "/readyz", "/livez"];

#[derive(Clone, Debug)]
pub struct HttpServerMakeSpan<M = DefaultHttpSpanMetadata> {
    metadata: M,
}

impl<M> HttpServerMakeSpan<M>
where
    M: HttpSpanMetadata,
{
    pub fn new(metadata: M) -> Self {
        Self { metadata }
    }
}

impl Default for HttpServerMakeSpan<DefaultHttpSpanMetadata> {
    fn default() -> Self {
        Self::new(DefaultHttpSpanMetadata)
    }
}

impl<M, B> MakeSpan<B> for HttpServerMakeSpan<M>
where
    M: HttpSpanMetadata,
{
    fn make_span(&mut self, request: &HttpRequest<B>) -> tracing::Span {
        let method = request.method();
        let raw_path = request.uri().path();
        if self
            .metadata
            .is_untraced(method, request.headers(), raw_path)
        {
            return tracing::Span::none();
        }
        let matched_route = request
            .extensions()
            .get::<MatchedPath>()
            .map(|matched| matched.as_str());
        let (path, route) =
            self.metadata
                .span_path_and_route(method, request.headers(), raw_path, matched_route);
        let resource = format!("{method} {route}");

        let span = tracing::info_span!(
            "http.server",
            otel.kind = "server",
            otel.name = %resource,
            otel.status_code = tracing::field::Empty,
            // Recorded by error `IntoResponse` impls with the response error
            // message. Without a status description, the Datadog exporter
            // synthesizes `error.message` from the bare HTTP status code on
            // failed server spans.
            otel.status_description = tracing::field::Empty,
            "operation.name" = "http.server",
            http.request.method = %method,
            http.method = %method,
            http.route = %route,
            url.path = %path,
            http.target = %path,
            request_id = tracing::field::Empty,
            sandbox_id = tracing::field::Empty,
            client_class = tracing::field::Empty,
            session_id = tracing::field::Empty,
            sandbox_port = tracing::field::Empty,
            route_generation = tracing::field::Empty,
            execution_generation = tracing::field::Empty,
            current_node_id = tracing::field::Empty,
            wake_required = tracing::field::Empty,
            wake_outcome = tracing::field::Empty,
            restore_kind = tracing::field::Empty,
            restore_kind_basis = tracing::field::Empty,
            target_node = tracing::field::Empty,
            restore_waiter_wait_ms = tracing::field::Empty,
            traceparent = tracing::field::Empty,
            failure_origin = tracing::field::Empty,
            failure_reason = tracing::field::Empty,
            space.response_origin = tracing::field::Empty,
            space.response_kind = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            http.status_code = tracing::field::Empty,
        );

        // A caller-supplied id; a surface that issues its own records it later.
        if let Some(request_id) = request_id_from_headers(request.headers()) {
            span.record("request_id", request_id.as_str());
        }
        self.metadata
            .record_extra_fields(request.headers(), raw_path, matched_route, &span);
        pplx_space_observability::trace_propagation::set_span_parent_from_headers(
            &span,
            request.headers(),
        );

        span
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HttpServerOnResponse;

impl<B> OnResponse<B> for HttpServerOnResponse {
    fn on_response(self, response: &HttpResponse<B>, _latency: Duration, span: &tracing::Span) {
        let status = response.status().as_u16();
        span.record("http.response.status_code", status);
        span.record("http.status_code", status);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HttpServerOnFailure;

impl OnFailure<ServerErrorsFailureClass> for HttpServerOnFailure {
    fn on_failure(
        &mut self,
        failure: ServerErrorsFailureClass,
        _latency: Duration,
        span: &tracing::Span,
    ) {
        span.record("otel.status_code", "ERROR");
        if let ServerErrorsFailureClass::StatusCode(status) = failure {
            let status = status.as_u16();
            span.record("http.response.status_code", status);
            span.record("http.status_code", status);
        }
    }
}

/// Adds a response's `traceparent` header inside [`http_trace_layer`].
pub type TraceparentResponseLayer = tower::util::MapResponseLayer<
    fn(HttpResponse<axum::body::Body>) -> HttpResponse<axum::body::Body>,
>;

/// The tower-http layer that creates and completes the `http.server` span.
pub type HttpServerTraceLayer<M> = TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    HttpServerMakeSpan<M>,
    DefaultOnRequest,
    HttpServerOnResponse,
    DefaultOnBodyChunk,
    DefaultOnEos,
    HttpServerOnFailure,
>;

/// The `http.server` trace layer, which also returns the server span's W3C
/// `traceparent` on every response so a client can hand over the trace of a
/// request it made.
pub fn http_trace_layer<M>(
    make_span: HttpServerMakeSpan<M>,
) -> tower::layer::util::Stack<TraceparentResponseLayer, HttpServerTraceLayer<M>>
where
    M: HttpSpanMetadata,
{
    tower::layer::util::Stack::new(
        tower::util::MapResponseLayer::new(insert_traceparent_header as fn(_) -> _),
        TraceLayer::new_for_http()
            .make_span_with(make_span)
            .on_response(HttpServerOnResponse)
            .on_failure(HttpServerOnFailure),
    )
}

/// Name the request's `http.server` span in a `traceparent` response header.
///
/// The trace layer enters that span while it polls the response future this
/// runs in, so it is the current span. The header carries only the W3C
/// version, trace ID, span ID and sampled flag; trace state and baggage are
/// not returned. Nothing is added when the span has no valid trace context,
/// as with traces off, or when the response already names one, such as a
/// proxied sandbox service's own.
fn insert_traceparent_header(
    mut response: HttpResponse<axum::body::Body>,
) -> HttpResponse<axum::body::Body> {
    if response.headers().contains_key(TRACEPARENT_HEADER) {
        return response;
    }
    if let Some(traceparent) =
        pplx_space_observability::trace_propagation::traceparent_for_span(&tracing::Span::current())
        && let Ok(value) = http::HeaderValue::try_from(traceparent)
    {
        response.headers_mut().insert(TRACEPARENT_HEADER, value);
    }
    response
}

const TRACEPARENT_HEADER: http::HeaderName = http::HeaderName::from_static("traceparent");

/// gRPC trace layer used by every SPACE node-local gRPC server.
///
/// The default `tower_http::trace::TraceLayer::new_for_grpc()` emits a terse
/// `"response failed" classification=Code: <n>` log on failures, with no
/// indication of which RPC method was called, who called it, or why it failed.
/// This helper replaces those defaults so the failure log identifies:
///
/// * the gRPC method (`rpc.service` / `rpc.method` / `rpc.path`),
/// * a stable per-request id (caller-supplied `x-request-id` header if
///   present, else a freshly minted UUID v4),
/// * the gRPC status code as a number *and* its canonical name
///   (e.g. `8` → `RESOURCE_EXHAUSTED`),
/// * whether an error came back as an error-class tonic `Status`
///   (server-status) or as a transport error (transport, the response never
///   reached `Status` classification),
/// * latency in milliseconds.
///
/// All of these are also recorded as fields on the surrounding `grpc.server`
/// span so they appear on every event emitted while the request is in flight,
/// not only on the on-failure event.
///
/// Server spans follow the OpenTelemetry gRPC convention: application-level
/// response statuses such as `NOT_FOUND` and `FAILED_PRECONDITION` remain
/// visible as statuses, but do not mark the server span as an error.
pub fn grpc_trace_layer() -> TraceLayer<
    GrpcMakeClassifier,
    GrpcMakeSpan,
    DefaultOnRequest,
    GrpcOnResponse,
    DefaultOnBodyChunk,
    GrpcOnEos,
    GrpcOnFailure,
> {
    TraceLayer::new(SharedClassifier::new(grpc_server_status_classifier()))
        .make_span_with(GrpcMakeSpan)
        .on_response(GrpcOnResponse)
        .on_eos(GrpcOnEos)
        .on_failure(GrpcOnFailure)
}

/// Records one sandbox's id on every request span of a server that serves only
/// that sandbox, such as its net-gateway, so `{span.sandbox_id=...}` finds its
/// RPCs. Add it after [`grpc_trace_layer`], which makes the request span
/// current while it calls the services inside it.
#[derive(Clone, Copy, Debug)]
pub struct SandboxSpanLayer {
    sandbox_id: pplx_space_common_core::types::SandboxId,
}

impl SandboxSpanLayer {
    pub fn new(sandbox_id: pplx_space_common_core::types::SandboxId) -> Self {
        Self { sandbox_id }
    }
}

impl<S> tower::Layer<S> for SandboxSpanLayer {
    type Service = SandboxSpanService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SandboxSpanService {
            inner,
            sandbox_id: self.sandbox_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SandboxSpanService<S> {
    inner: S,
    sandbox_id: pplx_space_common_core::types::SandboxId,
}

impl<S, Request> tower::Service<Request> for SandboxSpanService<S>
where
    S: tower::Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::Span::current().record("sandbox_id", tracing::field::display(self.sandbox_id));
        self.inner.call(request)
    }
}

fn grpc_server_status_classifier() -> GrpcErrorsAsFailures {
    grpc_application_status_classifier()
}

fn grpc_application_status_classifier() -> GrpcErrorsAsFailures {
    GrpcErrorsAsFailures::new()
        .with_success(GrpcCode::Cancelled)
        .with_success(GrpcCode::InvalidArgument)
        .with_success(GrpcCode::NotFound)
        .with_success(GrpcCode::AlreadyExists)
        .with_success(GrpcCode::PermissionDenied)
        .with_success(GrpcCode::ResourceExhausted)
        .with_success(GrpcCode::FailedPrecondition)
        .with_success(GrpcCode::Aborted)
        .with_success(GrpcCode::OutOfRange)
        .with_success(GrpcCode::Unauthenticated)
}

fn grpc_status_is_error(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unimplemented
            | tonic::Code::Internal
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss
    )
}

/// `MakeSpan` impl that creates an OTel-shaped `grpc.server` span and stamps
/// it with rpc method/service, full path, and a request id.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcMakeSpan;

impl<B> MakeSpan<B> for GrpcMakeSpan {
    fn make_span(&mut self, request: &HttpRequest<B>) -> tracing::Span {
        let path = request.uri().path();
        // A health check traced by its caller keeps its server span.
        if pplx_space_common_grpc::is_untraced_grpc_path(path)
            && !pplx_space_observability::trace_propagation::headers_carry_trace_context(
                request.headers(),
            )
        {
            return tracing::Span::none();
        }
        let (rpc_service, rpc_method) = parse_grpc_path(path);
        let server_service = current_service_name();
        let client_service =
            grpc_client_service_from_headers(request.headers()).unwrap_or(UNKNOWN_SERVICE);
        let peer_identity = crate::peer_auth::peer_identity_from_http_request(request);
        let request_id = request_id_from_headers(request.headers())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let otel_name = format!("{rpc_service}/{rpc_method}");

        let span = tracing::info_span!(
            "grpc.server",
            sandbox_id = tracing::field::Empty,
            sandbox_port = tracing::field::Empty,
            route_generation = tracing::field::Empty,
            execution_generation = tracing::field::Empty,
            current_node_id = tracing::field::Empty,
            traceparent = tracing::field::Empty,
            failure_origin = tracing::field::Empty,
            failure_reason = tracing::field::Empty,
            otel.kind = "server",
            otel.name = %otel_name,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "grpc.server",
            rpc.system = "grpc",
            rpc.service = %rpc_service,
            rpc.method = %rpc_method,
            rpc.path = %path,
            rpc.server.service = %server_service,
            rpc.client.service = %client_service,
            rpc.client.peer_uid = peer_identity.as_ref().map(|identity| identity.uid()).unwrap_or_default(),
            rpc.client.peer_gid = peer_identity.as_ref().map(|identity| identity.gid()).unwrap_or_default(),
            rpc.client.peer_pid = peer_identity.as_ref().and_then(|identity| identity.pid()).unwrap_or_default(),
            rpc.client.peer_principal = tracing::field::Empty,
            grpc.request.params = tracing::field::Empty,
            grpc.request.params_bytes = tracing::field::Empty,
            grpc.request.params_truncated = tracing::field::Empty,
            grpc.message = tracing::field::Empty,
            grpc.response.outcome = tracing::field::Empty,
            grpc.response.reason = tracing::field::Empty,
            grpc.error_source = tracing::field::Empty,
            rpc.grpc.status_code = tracing::field::Empty,
            request_id = %request_id,
        );

        pplx_space_observability::trace_propagation::set_span_parent_from_headers(
            &span,
            request.headers(),
        );

        span
    }
}

/// Records the gRPC status code on the span when tonic surfaces it on the
/// response head. Streaming statuses arrive on trailers and are captured by
/// [`GrpcOnEos`] instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcOnResponse;

impl<B> OnResponse<B> for GrpcOnResponse {
    fn on_response(self, response: &HttpResponse<B>, latency: Duration, span: &tracing::Span) {
        record_grpc_status_from_headers(response.headers(), span);
        if let Some(status) = response.extensions().get::<Status>() {
            record_grpc_status_on_span(status, span);
            log_grpc_server_status(status, Some(latency), span);
            return;
        }
        if let Some(status) = Status::from_header_map(response.headers()) {
            log_grpc_server_status(&status, Some(latency), span);
        }
    }
}

/// Records status metadata from response trailers, where streaming gRPC
/// statuses normally surface.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcOnEos;

impl OnEos for GrpcOnEos {
    fn on_eos(
        self,
        trailers: Option<&HeaderMap>,
        _stream_duration: Duration,
        span: &tracing::Span,
    ) {
        if let Some(trailers) = trailers {
            record_grpc_status_from_headers(trailers, span);
            if let Some(status) = Status::from_header_map(trailers) {
                log_grpc_server_status(&status, None, span);
            }
        }
    }
}

/// Replaces tower-http's `"response failed"` event with one that names the
/// failing RPC and translates the numeric gRPC code into its canonical
/// name.
///
/// Only the **transport-failure** branch emits a tracing event; the
/// server-status branch leaves logging to `log_grpc_server_status`
/// (called from `GrpcOnResponse` / `GrpcOnEos`), which carries the full
/// status message and error source. Both branches still record the
/// span's error fields so per-span attributes stay populated.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcOnFailure;

impl OnFailure<GrpcFailureClass> for GrpcOnFailure {
    fn on_failure(&mut self, failure: GrpcFailureClass, latency: Duration, span: &tracing::Span) {
        span.record("otel.status_code", "ERROR");
        let latency_ms = latency.as_millis() as u64;

        match failure {
            // Server reached `Status::*` and the classifier saw an
            // error-class grpc-status. `log_grpc_server_status` already logs
            // this from `on_response`/`on_eos` with grpc_message +
            // grpc_error_source, so we only record on the span here and skip
            // the event to avoid double-logging every failed RPC.
            GrpcFailureClass::Status(status) => {
                let numeric = status.code_raw();
                let name = tonic::Code::from_i32(numeric).description();
                span.record("rpc.grpc.status_code", numeric);
                span.record("otel.status_description", name);
            }
            // The classifier never saw a grpc-status — the response didn't
            // make it that far (HTTP/2 reset, body error, panic-to-error,
            // etc.). No tonic `Status` reached `log_grpc_server_status`, so
            // this is the only event source for the transport-failure case.
            GrpcFailureClass::Error(error) => {
                span.record("otel.status_description", error.as_str());
                tracing::error!(
                    target: "grpc.server",
                    rpc_log_side = "server",
                    grpc_error = %error,
                    latency_ms = latency_ms,
                    failure_origin = "transport",
                    "gRPC transport error before status was returned",
                );
            }
            _ => {
                tracing::error!(
                    target: "grpc.server",
                    rpc_log_side = "server",
                    latency_ms = latency_ms,
                    failure_origin = "classifier",
                    "gRPC request failed with an unknown classification",
                );
            }
        }
    }
}

fn log_grpc_server_status(status: &Status, latency: Option<Duration>, span: &tracing::Span) {
    let grpc_code = status.code() as i32;
    let grpc_code_name = status.code().description();
    let grpc_message = status.message();
    let source = std::error::Error::source(status).map(|err| err.to_string());
    let latency_ms = latency.map(|duration| duration.as_millis() as u64);

    if status.code() == tonic::Code::Ok {
        tracing::debug!(
            target: "grpc.server",
            parent: span,
            rpc_log_side = "server",
            grpc_code,
            grpc_code_name,
            latency_ms,
            "gRPC server request completed"
        );
        return;
    }

    if !grpc_status_is_error(status.code()) {
        let guest_refusal =
            pplx_space_common_grpc::GuestServiceUnavailable::from_status(status).is_some();
        tracing::debug!(
            target: "grpc.server",
            parent: span,
            rpc_log_side = "server",
            grpc_code,
            grpc_code_name,
            grpc_message,
            grpc_error_source = source.as_deref().unwrap_or(""),
            latency_ms,
            failure_origin = if guest_refusal { "guest" } else { "server-status" },
            failure_reason = guest_refusal.then_some("connection_refused"),
            "gRPC server request returned application status"
        );
        return;
    }

    tracing::error!(
        target: "grpc.server",
        parent: span,
        rpc_log_side = "server",
        grpc_code,
        grpc_code_name,
        grpc_message,
        grpc_error_source = source.as_deref().unwrap_or(""),
        latency_ms,
        "gRPC server request failed"
    );
}

fn record_grpc_status_from_headers(headers: &HeaderMap, span: &tracing::Span) {
    if let Some(code) = grpc_status_from_headers(headers) {
        span.record("rpc.grpc.status_code", code);
    }
    if let Some(status) = Status::from_header_map(headers) {
        record_grpc_status_on_span(&status, span);
    }
}

fn record_grpc_status_on_span(status: &Status, span: &tracing::Span) {
    pplx_space_common_grpc::record_guest_service_status(status, span);
    span.record("rpc.grpc.status_code", status.code() as i32);
    if !status.message().is_empty() {
        span.record("grpc.message", status.message());
    }
    if let Some(source) = std::error::Error::source(status) {
        let source = source.to_string();
        span.record("grpc.error_source", source.as_str());
    }
}

/// Split a tonic-style gRPC path of the form `/<package>.<Service>/<Method>`
/// into `(service, method)`. Returns `("", path)` if the shape doesn't match,
/// so we still produce a usable span for non-gRPC traffic that ends up here.
fn parse_grpc_path(path: &str) -> (&str, &str) {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    match trimmed.rsplit_once('/') {
        Some((service, method)) => (service, method),
        None => ("", trimmed),
    }
}

/// The longest caller-supplied request id a span or log line accepts.
const MAX_REQUEST_ID_BYTES: usize = 128;

/// A caller-supplied request id: `x-request-id`, or `x-correlation-id` when
/// that is absent or blank. The first non-blank header decides.
///
/// The id lands on the request's span and on every log line written inside
/// it, from callers that may be unauthenticated, so it is accepted only as at
/// most 128 bytes of `[A-Za-z0-9._:-]`; anything else yields `None`.
pub fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    for name in ["x-request-id", "x-correlation-id"] {
        let Some(value) = headers.get(name) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            return None;
        };
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let valid = trimmed.len() <= MAX_REQUEST_ID_BYTES
            && trimmed.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            });
        return valid.then(|| trimmed.to_owned());
    }
    None
}

fn grpc_client_service_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(CLIENT_SERVICE_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Read tonic's `grpc-status` header off a response head, if present.
fn grpc_status_from_headers(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
}

pub fn grpc_health_service() -> (
    tonic_health::server::HealthReporter,
    tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health>,
) {
    tonic_health::server::health_reporter()
}

pub async fn wait_for_sigterm() -> Result<(), WaitForSignalError> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(WaitForSignalError::Sigterm)?;
    sigterm.recv().await;
    tracing::info!("received SIGTERM, shutting down");
    Ok(())
}

pub async fn sigterm_shutdown() {
    if let Err(error) = wait_for_sigterm().await {
        tracing::error!(error = %error, "failed to wait for SIGTERM");
    }
}

pub fn shutdown_token() -> CancellationToken {
    shutdown_token_with_drain_signal(None)
}

pub fn shutdown_token_with_drain(drain_signal: crate::lifecycle::DrainSignal) -> CancellationToken {
    shutdown_token_with_drain_signal(Some(drain_signal))
}

fn shutdown_token_with_drain_signal(
    drain_signal: Option<crate::lifecycle::DrainSignal>,
) -> CancellationToken {
    let token = CancellationToken::new();
    // Install the handlers before returning, not when the task below is first
    // polled. Callers report readiness right after this, and a stop signal
    // that arrives before its handler exists takes the default action and
    // kills the process without draining.
    let shutdown_signal = install_shutdown_signal_handlers();

    tokio::spawn({
        let token = token.clone();
        async move {
            match shutdown_signal {
                Ok(shutdown_signal) => shutdown_signal.await,
                Err(error) => {
                    tracing::error!(error = %error, "failed to wait for shutdown signal");
                }
            }
            if let Some(drain_signal) = drain_signal {
                drain_signal.begin();
            }
            token.cancel();
        }
    });

    token
}

/// Registers the stop-signal handlers and returns a future that resolves on
/// the first stop signal. A signal delivered after this returns is recorded
/// even if the future has not been polled yet.
#[cfg(unix)]
fn install_shutdown_signal_handlers() -> Result<impl Future<Output = ()>, WaitForSignalError> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt()).map_err(WaitForSignalError::Sigint)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(WaitForSignalError::Sigterm)?;

    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {
                tracing::info!("received SIGINT, shutting down");
            }
            _ = terminate.recv() => {
                tracing::info!("received SIGTERM, shutting down");
            }
        }
    })
}

#[cfg(not(unix))]
fn install_shutdown_signal_handlers() -> Result<impl Future<Output = ()>, WaitForSignalError> {
    let mut ctrl_c = tokio::signal::windows::ctrl_c().map_err(WaitForSignalError::CtrlC)?;

    Ok(async move {
        ctrl_c.recv().await;
        tracing::info!("received Ctrl-C, shutting down");
    })
}

#[cfg(test)]
mod tests;
