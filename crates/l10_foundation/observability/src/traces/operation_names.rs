//! Preserve the native Datadog generic operation names at the OTLP boundary.
//!
//! The Agent's V2 mapping falls back to title-cased SpanKind strings, whereas
//! the native Rust exporter uses lowercase. Annotate completed generic spans
//! once here so call sites, including future instrumentation, need no Datadog
//! naming fields. Specialized operation mapping remains the receiver's job.

use opentelemetry::{KeyValue, Value, trace::SpanKind};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
};
use std::time::Duration;

#[derive(Debug)]
pub(super) struct OperationNames<E> {
    inner: E,
    resource: Resource,
}

impl<E> OperationNames<E> {
    pub(super) fn new(inner: E) -> Self {
        Self {
            inner,
            resource: Resource::builder_empty().build(),
        }
    }

    fn annotate(&self, span: &mut SpanData) {
        if string_attribute(span, &self.resource, "operation.name").is_some() {
            return;
        }
        // Older Agent semantic registries recognize only messaging.operation.
        // Supply its alias without replacing a nonempty legacy value or the
        // original modern attribute. Let the receiver keep HTTP/DB/messaging
        // precedence rather than manufacturing specialized operation names.
        if !matches!(span.span_kind, SpanKind::Internal)
            && string_attribute(span, &self.resource, "messaging.system").is_some()
            && string_attribute(span, &self.resource, "messaging.operation").is_none()
            && let Some(operation) =
                string_attribute(span, &self.resource, "messaging.operation.type")
                    .map(str::to_owned)
        {
            span.attributes
                .retain(|attribute| attribute.key.as_str() != "messaging.operation");
            span.attributes
                .push(KeyValue::new("messaging.operation", operation));
        }
        let name = match span.span_kind {
            SpanKind::Internal => "internal",
            SpanKind::Producer => "producer",
            SpanKind::Consumer => "consumer",
            // Client/server already map to lowercase specialized names (or
            // client.request/server.request). Never override those mappings.
            SpanKind::Client | SpanKind::Server => return,
        };
        let has = |key| string_attribute(span, &self.resource, key).is_some();
        if has("graphql.operation.type") {
            return;
        }
        // Messaging and GraphQL are the only specialized V2 rules applicable
        // to these three kinds. Accept both messaging convention generations;
        // a partially populated messaging span still needs its generic name.
        if matches!(span.span_kind, SpanKind::Producer | SpanKind::Consumer)
            && has("messaging.system")
            && (has("messaging.operation") || has("messaging.operation.type"))
        {
            return;
        }
        // Empty/non-string operation.name does not select an Agent override.
        // Replace it instead of exporting duplicate attribute keys.
        span.attributes
            .retain(|attribute| attribute.key.as_str() != "operation.name");
        span.attributes.push(KeyValue::new("operation.name", name));
    }
}

fn string_attribute<'a>(span: &'a SpanData, resource: &'a Resource, key: &str) -> Option<&'a str> {
    let nonempty = |value: &'a Value| match value {
        Value::String(value) if !value.as_str().is_empty() => Some(value.as_str()),
        _ => None,
    };
    // Like the Agent's semantic lookup, an absent/empty/non-string span value
    // may fall back to a resource value. Do not mask resource-level overrides.
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .and_then(|attribute| nonempty(&attribute.value))
        .or_else(|| {
            resource
                .iter()
                .find(|(name, _)| name.as_str() == key)
                .and_then(|(_, value)| nonempty(value))
        })
}

impl<E: SpanExporter> SpanExporter for OperationNames<E> {
    fn export(&self, mut batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        for span in &mut batch {
            self.annotate(span);
        }
        self.inner.export(batch)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
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
