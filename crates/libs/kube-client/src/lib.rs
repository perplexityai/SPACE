//! Kubernetes API clients for SPACE services, with `kubernetes.client`
//! tracing spans, and a shared reflector driver.
//!
//! It is its own crate so a service can build a traced Kubernetes client
//! without the rest of `pplx-space-common-svc`, which re-exports it as
//! `kube_client` behind its `k8s` feature.

mod reflector;
pub use reflector::spawn_reflector;

use std::time::Duration;

use http::{Request, Response};
use kube::{
    Client, Config, Error,
    client::{Body, ClientBuilder},
};
use tokio_util::sync::CancellationToken;
use tower_http::{
    classify::ServerErrorsFailureClass,
    trace::{MakeSpan, TraceLayer},
};
use tracing::Span;

pub async fn traced_kube_client_from_env() -> Result<Client, Error> {
    let config = Config::infer().await.map_err(Error::InferConfig)?;
    traced_kube_client(config)
}

/// A Kubernetes client whose calls get `kubernetes.client` spans.
///
/// A read made with no traced span current comes from a background loop (a
/// reflector or a periodic list) and succeeds on every idle tick, so it records
/// a span only when it fails; see [`FailureOnlyLayer`].
pub fn traced_kube_client(config: Config) -> Result<Client, Error> {
    kube_client_with_trace(
        config,
        FailureOnlyLayer {
            all_calls: false,
            shutdown: None,
        },
    )
}

/// A Kubernetes client for a periodic loop whose every successful call is the
/// idle steady state, such as lease renewal. A call with no traced span current
/// records a span only when it fails, whatever its verb; the loop marks the
/// ticks that change something itself.
///
/// Such a loop abandons its in-flight call when the process shuts down, so a
/// call dropped once `shutdown` is cancelled records nothing.
pub fn failure_traced_kube_client(
    config: Config,
    shutdown: CancellationToken,
) -> Result<Client, Error> {
    kube_client_with_trace(
        config,
        FailureOnlyLayer {
            all_calls: true,
            shutdown: Some(shutdown),
        },
    )
}

pub async fn failure_traced_kube_client_from_env(
    shutdown: CancellationToken,
) -> Result<Client, Error> {
    let config = Config::infer().await.map_err(Error::InferConfig)?;
    failure_traced_kube_client(config, shutdown)
}

fn kube_client_with_trace(config: Config, failure_only: FailureOnlyLayer) -> Result<Client, Error> {
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(KubeMakeSpan)
        .on_response(
            |response: &Response<Box<kube::client::DynBody>>, _latency: Duration, span: &Span| {
                let status = response.status();
                span.record("http.status_code", status.as_u16());
                if status.is_client_error() || status.is_server_error() {
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", status.as_str());
                } else {
                    span.record("otel.status_code", "OK");
                }
            },
        )
        .on_failure(
            |failure: ServerErrorsFailureClass, _latency: Duration, span: &Span| {
                span.record("otel.status_code", "ERROR");
                match failure {
                    ServerErrorsFailureClass::StatusCode(status) => {
                        span.record("http.status_code", status.as_u16());
                        span.record("otel.status_description", status.as_str());
                    }
                    ServerErrorsFailureClass::Error(error) => {
                        span.record("otel.status_description", error.as_str());
                    }
                }
            },
        );
    // Later layers wrap earlier ones, so the failure-only layer runs first and
    // can tell `KubeMakeSpan` to skip the span.
    Ok(ClientBuilder::try_from(config)?
        .with_layer(&trace_layer)
        .with_layer(&failure_only)
        .build())
}

/// Marks a request whose `kubernetes.client` span is recorded only on failure.
#[derive(Clone, Copy, Debug)]
struct SpanOnFailureOnly;

/// Skips the span of a successful call made with no traced span current: every
/// read, or every call when `all_calls` is set. A failed call opens a root
/// `kubernetes.client` span recording the error, so a failing background loop
/// is still traced. A call fails when it errors, answers with an unexpected
/// error status ([`is_expected_status`]), its response body fails, or it is
/// dropped before its body completes, as a caller's timeout does, unless the
/// `shutdown` token was cancelled first.
#[derive(Clone, Debug)]
struct FailureOnlyLayer {
    all_calls: bool,
    shutdown: Option<CancellationToken>,
}

impl<S> tower::Layer<S> for FailureOnlyLayer {
    type Service = FailureOnly<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FailureOnly {
            inner,
            all_calls: self.all_calls,
            shutdown: self.shutdown.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct FailureOnly<S> {
    inner: S,
    all_calls: bool,
    shutdown: Option<CancellationToken>,
}

impl<S, B> tower::Service<Request<Body>> for FailureOnly<S>
where
    S: tower::Service<Request<Body>, Response = Response<B>>,
    S::Error: std::fmt::Display,
    S::Future: Send + 'static,
    B: http_body::Body + Send + 'static,
    B::Error: std::fmt::Display,
{
    type Response = Response<FailureWatchedBody<B>>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let operation = kube_operation(&request);
        let failure_only = (self.all_calls || is_read_operation(&operation))
            && !pplx_space_observability::trace_propagation::has_traced_current_span();
        if !failure_only {
            let call = self.inner.call(request);
            return Box::pin(async move {
                Ok(call.await?.map(|body| FailureWatchedBody {
                    inner: body,
                    pending: PendingFailure::disarmed(),
                }))
            });
        }
        request.extensions_mut().insert(SpanOnFailureOnly);
        // Armed until the body completes: a drop before then is a failure.
        let mut pending = PendingFailure {
            call: Some(FailedCallSpan::new(&request, operation)),
            shutdown: self.shutdown.clone(),
        };
        let all_calls = self.all_calls;
        let call = self.inner.call(request);
        Box::pin(async move {
            let result = call.await;
            match result {
                Ok(response) => {
                    let status = response.status();
                    let expected = pending
                        .call
                        .as_ref()
                        .is_some_and(|call| is_expected_status(&call.operation, all_calls, status));
                    if (status.is_client_error() || status.is_server_error()) && !expected {
                        pending.fail(Some(status), status.as_str());
                    }
                    Ok(response.map(|body| FailureWatchedBody {
                        inner: body,
                        pending,
                    }))
                }
                Err(error) => {
                    pending.fail(None, &error.to_string());
                    Err(error)
                }
            }
        })
    }
}

fn is_read_operation(operation: &str) -> bool {
    ["get", "list", "watch"]
        .iter()
        .any(|verb| operation.starts_with(verb))
}

/// Error statuses some failure-only calls answer with as ordinary outcomes: a
/// single-object read's 404 (an existence check), and, on the lease client, a
/// write's 409 (a lost acquisition race).
fn is_expected_status(operation: &str, all_calls: bool, status: http::StatusCode) -> bool {
    match status {
        http::StatusCode::NOT_FOUND => operation == "get",
        http::StatusCode::CONFLICT => all_calls && matches!(operation, "create" | "replace"),
        _ => false,
    }
}

/// A failure-only call that has not finished: recorded as failed if it is
/// dropped while armed.
struct PendingFailure {
    call: Option<FailedCallSpan>,
    shutdown: Option<CancellationToken>,
}

impl PendingFailure {
    fn disarmed() -> Self {
        Self {
            call: None,
            shutdown: None,
        }
    }

    fn fail(&mut self, status: Option<http::StatusCode>, description: &str) {
        if let Some(call) = self.call.take() {
            call.record(status, description);
        }
    }

    fn complete(&mut self) {
        self.call = None;
    }

    /// Whether dropping the call now is a failure. A watch is a stream its
    /// consumer ends by dropping it, so only its errors count, and a loop
    /// abandons its call on purpose once the process shuts down. Any other
    /// call dropped early timed out.
    fn drop_is_failure(&self) -> bool {
        self.call
            .as_ref()
            .is_some_and(|call| call.operation != "watch")
            && !self
                .shutdown
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
    }
}

impl Drop for PendingFailure {
    fn drop(&mut self) {
        if self.drop_is_failure() {
            self.fail(
                None,
                "the call was dropped before its response completed, as by a caller's timeout",
            );
        }
    }
}

pin_project_lite::pin_project! {
    /// A response body that records its call's failure if it fails or is
    /// dropped before it ends.
    struct FailureWatchedBody<B> {
        #[pin]
        inner: B,
        pending: PendingFailure,
    }
}

impl<B> http_body::Body for FailureWatchedBody<B>
where
    B: http_body::Body,
    B::Error: std::fmt::Display,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let frame = std::task::ready!(this.inner.poll_frame(cx));
        match &frame {
            Some(Err(error)) => this.pending.fail(None, &error.to_string()),
            None => this.pending.complete(),
            Some(Ok(_)) => {}
        }
        std::task::Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// What a call's `kubernetes.client` span would have carried, kept until the
/// call ends so a failure can still be recorded with it.
struct FailedCallSpan {
    otel_name: String,
    method: String,
    target: String,
    path: String,
    operation: String,
    resource: String,
    namespace: String,
    name: String,
    started: std::time::Instant,
}

impl FailedCallSpan {
    fn new(request: &Request<Body>, operation: String) -> Self {
        let path = request.uri().path();
        let parts = k8s_path_parts(path);
        Self {
            otel_name: k8s_otel_name(&operation, parts.resource),
            method: request.method().to_string(),
            target: request.uri().to_string(),
            path: path.to_string(),
            resource: parts.resource.unwrap_or("unknown").to_string(),
            namespace: parts.namespace.unwrap_or("").to_string(),
            name: parts.name.unwrap_or("").to_string(),
            operation,
            started: std::time::Instant::now(),
        }
    }

    fn record(&self, status: Option<http::StatusCode>, description: &str) {
        let span = tracing::info_span!(
            "kubernetes.client",
            otel.kind = "client",
            otel.name = %self.otel_name,
            otel.status_code = "ERROR",
            otel.status_description = description,
            "operation.name" = "kubernetes.client",
            http.method = %self.method,
            http.target = %self.target,
            http.status_code = status.map(|status| status.as_u16()),
            url.path = %self.path,
            k8s.operation = %self.operation,
            k8s.resource = %self.resource,
            k8s.namespace = %self.namespace,
            k8s.name = %self.name,
            // The span opens when the failure is known; the call's own latency
            // is recorded here instead.
            k8s.duration_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        drop(span);
    }
}

#[derive(Clone, Copy, Debug)]
struct KubeMakeSpan;

impl MakeSpan<Body> for KubeMakeSpan {
    fn make_span(&mut self, request: &Request<Body>) -> Span {
        if request.extensions().get::<SpanOnFailureOnly>().is_some() {
            return Span::none();
        }
        let operation = kube_operation(request);
        let path = request.uri().path();
        let path_parts = k8s_path_parts(path);
        let resource = path_parts.resource.unwrap_or("unknown");
        let otel_name = k8s_otel_name(&operation, path_parts.resource);

        tracing::info_span!(
            "kubernetes.client",
            otel.kind = "client",
            otel.name = %otel_name,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "kubernetes.client",
            http.method = %request.method(),
            http.target = %request.uri(),
            http.status_code = tracing::field::Empty,
            url.path = %path,
            k8s.operation = %operation,
            k8s.resource = %resource,
            k8s.namespace = path_parts.namespace.unwrap_or(""),
            k8s.name = path_parts.name.unwrap_or(""),
        )
    }
}

fn kube_operation(request: &Request<Body>) -> String {
    request
        .extensions()
        .get::<&'static str>()
        .map(|operation| (*operation).to_string())
        .unwrap_or_else(|| request.method().as_str().to_ascii_lowercase())
}

fn k8s_otel_name(operation: &str, resource: Option<&str>) -> String {
    match resource {
        Some(resource) => format!("kubernetes.{operation} {resource}"),
        None => format!("kubernetes.{operation}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct K8sPathParts<'a> {
    namespace: Option<&'a str>,
    resource: Option<&'a str>,
    name: Option<&'a str>,
}

fn k8s_path_parts(path: &str) -> K8sPathParts<'_> {
    let segments: Vec<_> = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let mut index = match segments.as_slice() {
        ["api", _version, ..] => 2,
        ["apis", _group, _version, ..] => 3,
        _ => 0,
    };
    let namespace = if segments.get(index) == Some(&"namespaces") {
        let namespace = segments.get(index + 1).copied();
        index += 2;
        namespace
    } else {
        None
    };

    K8sPathParts {
        namespace,
        resource: segments.get(index).copied(),
        name: segments.get(index + 1).copied(),
    }
}

#[cfg(test)]
mod tests;
