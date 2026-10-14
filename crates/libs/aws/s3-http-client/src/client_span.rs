//! One `s3.client` span per S3 operation, for every client that registers
//! [`S3ClientSpans`] as an interceptor.
//!
//! The span covers the whole operation, its retries and backoff included,
//! because that is what the caller waited for; `s3.attempts` says how many
//! requests it took. Each failed attempt's cause is already logged and counted
//! where the client tracks attempts, so attempts do not get spans of their
//! own, which keeps a throttling storm from multiplying span volume.
//!
//! Attributes are bounded: the operation, an outcome, the last response's
//! status class, request and response bytes, and the attempt count. No bucket,
//! object key, upload id or range is recorded; keys can name user data. A
//! failure's status description is a fixed classification
//! ([`failure_kind`]), never SDK or server text, which can quote a key or a
//! resource ARN.

use std::sync::{Arc, Mutex, PoisonError};

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::Error;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeTransmitInterceptorContextRef, FinalizerInterceptorContextRef,
};
use aws_smithy_runtime_api::client::orchestrator::{Metadata, OrchestratorError};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::{ConfigBag, Storable, StoreReplace};

/// The interceptor that gives each S3 operation its client span.
#[derive(Clone, Copy, Debug, Default)]
pub struct S3ClientSpans;

impl Intercept for S3ClientSpans {
    fn name(&self) -> &'static str {
        "pplx-space-s3-client-spans"
    }

    // The span opens with the first attempt: operation metadata is applied
    // after the client's execution-start hooks run. The hook runs inside the
    // caller's `send()`, so the caller's current span is the parent.
    fn read_before_attempt(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request_bytes = context
            .request()
            .headers()
            .get("content-length")
            .and_then(|value| value.parse::<u64>().ok());
        if let Some(operation) = cfg.load::<OperationSpan>() {
            operation.lock().attempts += 1;
            return Ok(());
        }
        let name = cfg.load::<Metadata>().map_or("unknown", Metadata::name);
        let existence_check = is_existence_check(name);
        let span = tracing::info_span!(
            "s3.client",
            otel.kind = "client",
            otel.name = %format_args!("S3.{name}"),
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "s3.client",
            rpc.system = "aws-api",
            rpc.service = "S3",
            rpc.method = %name,
            s3.operation = %name,
            s3.outcome = tracing::field::Empty,
            s3.failure_kind = tracing::field::Empty,
            s3.attempts = tracing::field::Empty,
            s3.request.bytes = request_bytes,
            s3.response.bytes = tracing::field::Empty,
            http.status_class = tracing::field::Empty,
        );
        cfg.interceptor_state()
            .store_put(OperationSpan(Arc::new(Mutex::new(Operation {
                span,
                existence_check,
                attempts: 1,
                last_status: None,
                finished: false,
            }))));
        Ok(())
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(operation) = cfg.load::<OperationSpan>() {
            let mut operation = operation.lock();
            if let Some(response) = context.response() {
                operation.last_status = Some(response.status().as_u16());
                if let Some(bytes) = response
                    .headers()
                    .get("content-length")
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    operation.span.record("s3.response.bytes", bytes);
                }
            }
        }
        Ok(())
    }

    fn read_after_execution(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(operation) = cfg.load::<OperationSpan>() {
            let mut operation = operation.lock();
            let last_status = operation.last_status;
            match context.output_or_error() {
                Some(Ok(_)) => operation.finish(Outcome::Ok, None),
                Some(Err(_)) if last_status == Some(404) && operation.existence_check => {
                    operation.finish(Outcome::NotFound, None);
                }
                Some(Err(error)) => {
                    operation.finish(Outcome::Error, Some(failure_kind(error, last_status)));
                }
                None => operation.finish(Outcome::Error, Some("no_result")),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Ok,
    /// A 404 from an existence check ([`is_existence_check`]), which its
    /// caller treats as an answer, so the span is not marked failed.
    NotFound,
    Error,
    /// The operation timeout fired, or the caller dropped the request.
    Abandoned,
}

impl Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "not_found",
            Self::Error => "error",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Debug)]
struct OperationSpan(Arc<Mutex<Operation>>);

impl OperationSpan {
    fn lock(&self) -> std::sync::MutexGuard<'_, Operation> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Storable for OperationSpan {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug)]
struct Operation {
    span: tracing::Span,
    existence_check: bool,
    attempts: u32,
    last_status: Option<u16>,
    finished: bool,
}

impl Operation {
    /// `failure` is a [`failure_kind`] value, recorded as the failure kind and,
    /// with the operation, as the status description.
    fn finish(&mut self, outcome: Outcome, failure: Option<&'static str>) {
        self.finished = true;
        self.span.record("s3.outcome", outcome.as_str());
        self.span.record("s3.attempts", self.attempts);
        if let Some(status) = self.last_status {
            self.span.record("http.status_class", status_class(status));
        }
        match outcome {
            Outcome::Ok | Outcome::NotFound => {
                self.span.record("otel.status_code", "OK");
            }
            Outcome::Error | Outcome::Abandoned => {
                let failure = failure.unwrap_or("other");
                self.span.record("otel.status_code", "ERROR");
                self.span.record("s3.failure_kind", failure);
                self.span.record(
                    "otel.status_description",
                    format!("S3 operation failed: {failure}").as_str(),
                );
            }
        }
    }
}

impl Drop for Operation {
    /// The config bag, and this with it, is dropped without a final hook when
    /// the operation timeout fires or the caller drops the request future.
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Outcome::Abandoned, Some("abandoned"));
        }
    }
}

/// Operations whose 404 answers whether the object exists. Any other
/// operation's 404, such as `NoSuchUpload` or `NoSuchBucket`, fails it.
fn is_existence_check(operation: &str) -> bool {
    matches!(operation, "HeadObject" | "GetObject")
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

/// Why an S3 operation failed, from a closed set. The SDK's and the server's
/// text stays out of spans: an error message can quote an object key or a
/// resource ARN that names one.
fn failure_kind(error: &OrchestratorError<Error>, last_status: Option<u16>) -> &'static str {
    if let Some(connector) = error.as_connector_error() {
        // The connector reports its connect and its response read timeouts
        // alike, so this does not say which one fired.
        return if connector.is_timeout() {
            "connector_timeout"
        } else {
            "transport"
        };
    }
    if error.is_timeout_error() {
        return "timeout";
    }
    match last_status {
        Some(429 | 503) => "throttled",
        Some(500..=599) => "http_5xx",
        Some(403) => "access_denied",
        Some(404) => "not_found",
        Some(400..=499) => "http_4xx",
        _ if error.is_operation_error() => "service_error",
        _ => "other",
    }
}
