use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue, Request, Response};
use tower::Layer as _;
use tower_http::classify::{
    ClassifiedResponse, ClassifyEos, ClassifyResponse, GrpcCode, GrpcEosErrorsAsFailures,
    GrpcErrorsAsFailures, GrpcFailureClass, MakeClassifier,
};
use tower_http::trace::{
    DefaultOnBodyChunk, DefaultOnRequest, MakeSpan, OnEos, OnFailure, OnResponse, Trace, TraceLayer,
};

pub const CLIENT_SERVICE_METADATA_KEY: &str = "x-space-client-service";
const UNKNOWN_SERVICE: &str = "unknown";
static CURRENT_SERVICE_NAME: OnceLock<String> = OnceLock::new();

pub fn set_current_service_name(service_name: &str) {
    let _ = CURRENT_SERVICE_NAME.set(service_name.to_string());
}

pub fn current_service_name() -> &'static str {
    CURRENT_SERVICE_NAME
        .get()
        .map(String::as_str)
        .unwrap_or(UNKNOWN_SERVICE)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientTraceLayer;

impl<S> tower::Layer<S> for GrpcClientTraceLayer {
    type Service = GrpcClientTrace<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let trace = TraceLayer::new(GrpcClientMakeClassifier)
            .make_span_with(GrpcClientMakeSpan)
            .on_response(GrpcClientOnResponse)
            .on_eos(GrpcClientOnEos)
            .on_failure(GrpcClientOnFailure);
        trace.layer(InjectTraceparent { inner })
    }
}

pub type GrpcClientTrace<S> = Trace<
    InjectTraceparent<S>,
    GrpcClientMakeClassifier,
    GrpcClientMakeSpan,
    DefaultOnRequest,
    GrpcClientOnResponse,
    DefaultOnBodyChunk,
    GrpcClientOnEos,
    GrpcClientOnFailure,
>;

pub fn trace_grpc_client_transport<S>(inner: S) -> GrpcClientTrace<S> {
    GrpcClientTraceLayer.layer(inner)
}

const WORKER_API_SPACED_STAT_PATH: &str = "/space.node.api.v1.WorkerApi/SpacedStat";
const WORKER_API_SPACED_PREFIX: &str = "/space.node.api.v1.WorkerApi/Spaced";

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientMakeClassifier;

impl MakeClassifier for GrpcClientMakeClassifier {
    type Classifier = GrpcClientClassifier;
    type FailureClass = GrpcClientFailureClass;
    type ClassifyEos = GrpcClientEosClassifier;

    fn make_classifier<B>(&self, request: &Request<B>) -> Self::Classifier {
        let path = request.uri().path();
        let inner = if grpc_client_is_readiness_poll(path) {
            GrpcErrorsAsFailures::new().with_success(GrpcCode::FailedPrecondition)
        } else if grpc_client_application_statuses_are_expected(path) {
            grpc_application_status_classifier()
        } else if grpc_client_not_found_is_expected(path) {
            GrpcErrorsAsFailures::new().with_success(GrpcCode::NotFound)
        } else {
            GrpcErrorsAsFailures::new()
        };
        GrpcClientClassifier {
            inner,
            service_port_ingress: matches!(
                path,
                "/space.node.api.v1.WorkerApi/ServicePortIngress"
                    | "/space.sandbox.manager.v1.SandboxManager/ServicePortIngress"
                    | "/space.net.gateway.v1.NetGatewayControl/ServicePortIngress"
            ),
        }
    }
}

/// Keep the ingress exception tied to the status metadata, including trailers.
/// Other RPCs retain their existing application-status policy.
#[derive(Clone, Debug)]
pub struct GrpcClientClassifier {
    inner: GrpcErrorsAsFailures,
    service_port_ingress: bool,
}

#[derive(Clone, Debug)]
pub struct GrpcClientEosClassifier {
    inner: GrpcEosErrorsAsFailures,
    service_port_ingress: bool,
}

/// Carry the request-specific error decision to OnFailure, which cannot recover
/// the RPC path or typed metadata from Tower's numeric failure code alone.
#[derive(Debug)]
pub struct GrpcClientFailureClass {
    inner: GrpcFailureClass,
    is_error: bool,
}

impl GrpcClientFailureClass {
    fn new(inner: GrpcFailureClass, service_port_ingress: bool) -> Self {
        let is_error = match &inner {
            GrpcFailureClass::Status(status) => {
                let code = tonic::Code::from_i32(status.code_raw());
                grpc_status_is_error(code)
                    || (service_port_ingress && code == tonic::Code::FailedPrecondition)
            }
            GrpcFailureClass::Error(_) => true,
            _ => true,
        };
        Self { inner, is_error }
    }
}

impl std::fmt::Display for GrpcClientFailureClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

fn is_guest_refusal(headers: &HeaderMap) -> bool {
    tonic::Status::from_header_map(headers)
        .is_some_and(|status| crate::GuestServiceUnavailable::from_status(&status).is_some())
}

impl ClassifyResponse for GrpcClientClassifier {
    type FailureClass = GrpcClientFailureClass;
    type ClassifyEos = GrpcClientEosClassifier;

    fn classify_response<B>(
        self,
        response: &Response<B>,
    ) -> ClassifiedResponse<Self::FailureClass, Self::ClassifyEos> {
        if self.service_port_ingress && is_guest_refusal(response.headers()) {
            return ClassifiedResponse::Ready(Ok(()));
        }
        match self.inner.classify_response(response) {
            ClassifiedResponse::Ready(result) => {
                ClassifiedResponse::Ready(result.map_err(|failure| {
                    GrpcClientFailureClass::new(failure, self.service_port_ingress)
                }))
            }
            ClassifiedResponse::RequiresEos(inner) => {
                ClassifiedResponse::RequiresEos(GrpcClientEosClassifier {
                    inner,
                    service_port_ingress: self.service_port_ingress,
                })
            }
        }
    }

    fn classify_error<E: std::fmt::Display + 'static>(self, error: &E) -> Self::FailureClass {
        GrpcClientFailureClass::new(self.inner.classify_error(error), self.service_port_ingress)
    }
}

impl ClassifyEos for GrpcClientEosClassifier {
    type FailureClass = GrpcClientFailureClass;

    fn classify_eos(self, trailers: Option<&HeaderMap>) -> Result<(), Self::FailureClass> {
        if self.service_port_ingress && trailers.is_some_and(is_guest_refusal) {
            return Ok(());
        }
        self.inner
            .classify_eos(trailers)
            .map_err(|failure| GrpcClientFailureClass::new(failure, self.service_port_ingress))
    }

    fn classify_error<E: std::fmt::Display + 'static>(self, error: &E) -> Self::FailureClass {
        GrpcClientFailureClass::new(self.inner.classify_error(error), self.service_port_ingress)
    }
}

/// A readiness poll answers FAILED_PRECONDITION until the sandbox has booted.
/// That "not ready yet" is the expected answer, not a failed call, so it is
/// classified as success in the response and in the trailers alike.
fn grpc_client_is_readiness_poll(path: &str) -> bool {
    matches!(
        path,
        "/space.node.api.v1.WorkerApi/SpacedWaitReady"
            | "/space.sandbox.manager.v1.SandboxManager/SpacedWaitReady"
    )
}

fn grpc_client_not_found_is_expected(path: &str) -> bool {
    path == WORKER_API_SPACED_STAT_PATH
}

fn grpc_client_application_statuses_are_expected(path: &str) -> bool {
    let Some(method) = path.strip_prefix(WORKER_API_SPACED_PREFIX) else {
        return false;
    };

    matches!(
        method,
        "Stat"
            | "List"
            | "Read"
            | "Write"
            | "WriteBatch"
            | "Mkdir"
            | "Remove"
            | "Rename"
            | "NormalizeWatchRoot"
            | "Watch"
            | "CreateWatcher"
            | "GetWatcherEvents"
            | "RemoveWatcher"
            | "EnableFileChanges"
            | "DisableFileChanges"
            | "GetModifiedFiles"
            | "StartProcess"
            | "ConnectProcess"
            | "ListProcesses"
            | "SendSignal"
            | "SendInput"
            | "CloseStdin"
            | "ResizePty"
            | "KillProcess"
    )
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

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientMakeSpan;

impl<B> MakeSpan<B> for GrpcClientMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> tracing::Span {
        let path = request.uri().path();
        let (rpc_service, rpc_method) = parse_grpc_path(path);
        let otel_name = format!("{rpc_service}/{rpc_method}");

        let span = tracing::info_span!(
            "grpc.client",
            sandbox_id = tracing::field::Empty,
            sandbox_port = tracing::field::Empty,
            route_generation = tracing::field::Empty,
            execution_generation = tracing::field::Empty,
            current_node_id = tracing::field::Empty,
            traceparent = tracing::field::Empty,
            failure_origin = tracing::field::Empty,
            failure_reason = tracing::field::Empty,
            otel.kind = "client",
            otel.name = %otel_name,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "grpc.client",
            rpc.system = "grpc",
            rpc.service = %rpc_service,
            rpc.method = %rpc_method,
            rpc.path = %path,
            rpc.client.service = %current_service_name(),
            rpc.grpc.status_code = tracing::field::Empty,
            space.grpc.expected_not_found = grpc_client_not_found_is_expected(path),
            space.grpc.expected_application_status = grpc_client_application_statuses_are_expected(path),
        );
        if let Some(context) = request.extensions().get::<crate::SandboxTraceContext>() {
            context.record(&span);
        }
        span
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientOnResponse;

impl<B> OnResponse<B> for GrpcClientOnResponse {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &tracing::Span) {
        record_grpc_status_from_headers(response.headers(), span);
        log_grpc_client_application_status(response.headers(), latency, span);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientOnEos;

impl OnEos for GrpcClientOnEos {
    fn on_eos(self, trailers: Option<&HeaderMap>, stream_duration: Duration, span: &tracing::Span) {
        if let Some(trailers) = trailers {
            record_grpc_status_from_headers(trailers, span);
            log_grpc_client_application_status(trailers, stream_duration, span);
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcClientOnFailure;

impl OnFailure<GrpcClientFailureClass> for GrpcClientOnFailure {
    fn on_failure(
        &mut self,
        failure: GrpcClientFailureClass,
        latency: Duration,
        span: &tracing::Span,
    ) {
        let latency_ms = latency.as_millis() as u64;

        match failure.inner {
            GrpcFailureClass::Status(status) => {
                let numeric = status.code_raw();
                let tonic_code = tonic::Code::from_i32(numeric);
                let name = tonic_code.description();
                span.record("rpc.grpc.status_code", numeric);
                // Every status that reaches here marks the client span ERROR,
                // application statuses included. The code goes first: recording
                // it after the description would clear the message.
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_description", name);
                if !failure.is_error {
                    tracing::debug!(
                        target: "grpc.client",
                        rpc_log_side = "client",
                        grpc_code = numeric,
                        grpc_code_name = name,
                        latency_ms,
                        failure_origin = "server-status",
                        "gRPC client received application status",
                    );
                    return;
                }

                tracing::error!(
                    target: "grpc.client",
                    rpc_log_side = "client",
                    grpc_code = numeric,
                    grpc_code_name = name,
                    latency_ms,
                    failure_origin = "server-status",
                    "gRPC client received non-OK status",
                );
            }
            GrpcFailureClass::Error(error) => {
                span.record("otel.status_code", "ERROR");
                span.record("otel.status_description", error.as_str());
                tracing::error!(
                    target: "grpc.client",
                    rpc_log_side = "client",
                    grpc_error = %error,
                    latency_ms,
                    failure_origin = "transport",
                    "gRPC client transport error before status was returned",
                );
            }
            _ => {
                span.record("otel.status_code", "ERROR");
                tracing::error!(
                    target: "grpc.client",
                    rpc_log_side = "client",
                    latency_ms,
                    failure_origin = "classifier",
                    "gRPC client request failed with an unknown classification",
                );
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct InjectTraceparent<S> {
    inner: S,
}

impl<S, RequestBody> tower::Service<Request<RequestBody>> for InjectTraceparent<S>
where
    S: tower::Service<Request<RequestBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<RequestBody>) -> Self::Future {
        if let Ok(value) = HeaderValue::from_str(current_service_name()) {
            request
                .headers_mut()
                .insert(HeaderName::from_static(CLIENT_SERVICE_METADATA_KEY), value);
        }
        pplx_space_observability::trace_propagation::inject_current_context_into_headers(
            request.headers_mut(),
        );
        self.inner.call(request)
    }
}

fn parse_grpc_path(path: &str) -> (&str, &str) {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    match trimmed.rsplit_once('/') {
        Some((service, method)) => (service, method),
        None => ("", trimmed),
    }
}

fn grpc_status_from_headers(headers: &HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i32>().ok())
}

fn record_grpc_status_from_headers(headers: &HeaderMap, span: &tracing::Span) {
    if let Some(code) = grpc_status_from_headers(headers) {
        span.record("rpc.grpc.status_code", code);
    }
    if let Some(status) = tonic::Status::from_header_map(headers) {
        crate::record_guest_service_status(&status, span);
        span.record("rpc.grpc.status_code", status.code() as i32);
        if !status.message().is_empty() {
            span.record("grpc.message", status.message());
        }
    }
}

fn log_grpc_client_application_status(
    headers: &HeaderMap,
    latency: Duration,
    span: &tracing::Span,
) {
    let Some(code) = grpc_status_from_headers(headers).map(tonic::Code::from_i32) else {
        return;
    };
    if code == tonic::Code::Ok || grpc_status_is_error(code) {
        return;
    }

    let guest_refusal = is_guest_refusal(headers);
    tracing::debug!(
        target: "grpc.client",
        parent: span,
        rpc_log_side = "client",
        grpc_code = code as i32,
        grpc_code_name = code.description(),
        latency_ms = latency.as_millis() as u64,
        failure_origin = if guest_refusal { "guest" } else { "server-status" },
        failure_reason = guest_refusal.then_some("connection_refused"),
        "gRPC client received application status",
    );
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

#[cfg(test)]
mod tests;
