//! The HTTP server trace layer: span naming and redaction, probe filtering, and the traceparent
//! response header.

use std::borrow::Cow;
use std::time::Duration;

use axum::extract::MatchedPath;
use http::{HeaderMap, Method, Request as HttpRequest, Response as HttpResponse};
use pplx_space_request_id::request_id_from_headers;
use tower_http::classify::{ServerErrorsAsFailures, ServerErrorsFailureClass, SharedClassifier};
use tower_http::trace::{
    DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, MakeSpan, OnFailure, OnResponse, TraceLayer,
};

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
pub(super) const UNTRACED_HTTP_PATHS: [&str; 4] = ["/health", "/healthz", "/readyz", "/livez"];

#[derive(Clone, Debug)]
pub struct HttpServerMakeSpan<M = DefaultHttpSpanMetadata> {
    pub(super) metadata: M,
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
pub(super) fn insert_traceparent_header(
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

pub(super) const TRACEPARENT_HEADER: http::HeaderName =
    http::HeaderName::from_static("traceparent");
