//! Test-only trace context, for code under test that saves the current span's
//! context (as [`current_traceparent`](crate::trace_propagation::current_traceparent)
//! reads it) for a later consumer to link to.
//!
//! Gated like [`test_capture`](crate::test_capture), and also behind `otel`:
//! without it there is no context to save.

use std::future::Future;

use opentelemetry::trace::TracerProvider as _;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

/// Runs `future` inside a span whose OpenTelemetry parent is `traceparent`,
/// so the context the code under test reads is in that trace.
///
/// It installs the W3C propagator process-wide, and makes an OpenTelemetry
/// subscriber this thread's default while `future` runs, so drive it on a
/// current-thread runtime.
pub async fn in_remote_parent_span<F: Future>(traceparent: &str, future: F) -> F::Output {
    opentelemetry::global::set_text_map_propagator(crate::text_map_propagator());
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test-trace")));
    let _default = tracing::subscriber::set_default(subscriber);
    let span = tracing::info_span!("remote_parent");
    crate::trace_propagation::set_span_parent_from_traceparent(&span, traceparent);
    future.instrument(span).await
}
