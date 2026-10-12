//! Give an ERROR span a status message when its cause is recorded elsewhere.
//!
//! Instrumentation marks a span failed with `otel.status_code = "ERROR"`, and
//! tracing-opentelemetry does the same for any ERROR-level event inside it,
//! in both cases with an empty message. The cause is usually recorded on the
//! span all the same, as an `error` attribute or an event, where a trace view
//! that lists failures by status message never shows it. Copying it into the
//! empty message at the export boundary covers every call site, including
//! future ones, without changing what the span records. A message set by the
//! instrumentation is never replaced.

use opentelemetry::{KeyValue, Value, trace::Status};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
};
use std::time::Duration;

/// Attributes that carry a failure's cause, most specific first.
const CAUSE_KEYS: &[&str] = &["exception.message", "error.message", "error"];

#[derive(Debug)]
pub(super) struct StatusMessages<E> {
    inner: E,
}

impl<E> StatusMessages<E> {
    pub(super) fn new(inner: E) -> Self {
        Self { inner }
    }
}

/// The cause to report for an ERROR span with no message, in order: the span's
/// own cause attribute; the latest ERROR event's cause attribute, else its
/// message; and only then a cause attribute on a lower-level event. A recovered
/// retry's warning must not explain a later failure that has its own record.
fn cause(span: &SpanData) -> Option<String> {
    if let Some(cause) = cause_attribute(&span.attributes) {
        return Some(cause);
    }
    let events = || span.events.events.iter().rev();
    events()
        .find(|event| is_error_event(&event.attributes))
        .and_then(|event| {
            cause_attribute(&event.attributes)
                .or_else(|| Some(event.name.to_string()).filter(|name| !name.is_empty()))
        })
        .or_else(|| events().find_map(|event| cause_attribute(&event.attributes)))
}

fn is_error_event(attributes: &[KeyValue]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.key.as_str() == "level"
            && matches!(&attribute.value, Value::String(level) if level.as_str() == "ERROR")
    })
}

fn cause_attribute(attributes: &[KeyValue]) -> Option<String> {
    CAUSE_KEYS.iter().find_map(|key| {
        attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == *key)
            .map(|attribute| attribute.value.to_string())
            .filter(|value| !value.is_empty())
    })
}

fn annotate(span: &mut SpanData) {
    if !matches!(&span.status, Status::Error { description } if description.is_empty()) {
        return;
    }
    if let Some(cause) = cause(span) {
        span.status = Status::error(cause);
    }
}

impl<E: SpanExporter> SpanExporter for StatusMessages<E> {
    fn export(&self, mut batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        for span in &mut batch {
            annotate(span);
        }
        self.inner.export(batch)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
}
