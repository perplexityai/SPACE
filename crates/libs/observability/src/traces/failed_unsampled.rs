//! Export an unsampled span only when it failed.
//!
//! The OTLP pipeline's sampler records a span under an unsampled parent, such
//! as an untraced caller's gRPC call, without sampling it
//! ([`super::origin_sampling`]), so that a failed call still reaches the
//! backend with its error, as a failed connect does. The SDK's
//! batch processor exports every span it is handed, sampled or not, so this
//! processor decides: sampled spans pass, an unsampled one passes only when its
//! status is an error.

use opentelemetry::Context;
use opentelemetry::trace::Status;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Span, SpanData, SpanProcessor};

#[derive(Debug)]
pub(super) struct FailedUnsampled<P>(pub(super) P);

impl<P: SpanProcessor> SpanProcessor for FailedUnsampled<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.0.on_start(span, cx);
    }

    fn on_end(&self, span: SpanData) {
        if span.span_context.is_sampled() || matches!(span.status, Status::Error { .. }) {
            self.0.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.0.set_resource(resource);
    }
}
