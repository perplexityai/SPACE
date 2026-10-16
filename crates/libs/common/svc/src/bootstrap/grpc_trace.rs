//! The gRPC server trace layer: the request span, status classification and logging, and the client
//! service and status read from headers.

use super::*;

pub(super) const UNKNOWN_SERVICE: &str = "unknown";

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
    pub(super) sandbox_id: pplx_space_common_core::types::SandboxId,
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
    pub(super) inner: S,
    pub(super) sandbox_id: pplx_space_common_core::types::SandboxId,
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

pub(super) fn grpc_server_status_classifier() -> GrpcErrorsAsFailures {
    grpc_application_status_classifier()
}

pub(super) fn grpc_application_status_classifier() -> GrpcErrorsAsFailures {
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

pub(super) fn grpc_status_is_error(code: tonic::Code) -> bool {
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

pub(super) fn log_grpc_server_status(
    status: &Status,
    latency: Option<Duration>,
    span: &tracing::Span,
) {
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

pub(super) fn record_grpc_status_from_headers(headers: &HeaderMap, span: &tracing::Span) {
    if let Some(code) = grpc_status_from_headers(headers) {
        span.record("rpc.grpc.status_code", code);
    }
    if let Some(status) = Status::from_header_map(headers) {
        record_grpc_status_on_span(&status, span);
    }
}

pub(super) fn record_grpc_status_on_span(status: &Status, span: &tracing::Span) {
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
pub(super) fn parse_grpc_path(path: &str) -> (&str, &str) {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    match trimmed.rsplit_once('/') {
        Some((service, method)) => (service, method),
        None => ("", trimmed),
    }
}

pub(super) fn grpc_client_service_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(CLIENT_SERVICE_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Read tonic's `grpc-status` header off a response head, if present.
pub(super) fn grpc_status_from_headers(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
}
