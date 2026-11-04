//! Test-only tracing event capture.
//!
//! Gated behind `test-capture`, mirroring `pplx-space-metrics`'s
//! `test-capture` feature shape: code lives under
//! `#[cfg(any(test, feature = "test-capture"))]` so this crate's own tests use
//! it without an explicit flag, while dev-dependents opt in with
//! `pplx-space-observability = { workspace = true, features = ["test-capture"] }`.
//!
//! This consolidates the hand-rolled `tracing_subscriber::Layer` +
//! `tracing::field::Visit` pairs that had accreted across SPACE test suites.
//! Two levels are exposed:
//!
//! - [`RecordingVisitor`] is the shared `Visit` implementation: every
//!   `record_*` method, with `record_debug` values rendered via `{value:?}`
//!   and trimmed of one layer of wrapping double quotes. It visits anything
//!   with a `.record(&mut dyn Visit)` method — `tracing::Event`,
//!   `tracing::span::Attributes`, `tracing::span::Record` — so it fits both
//!   event-capture and span-capture layers with their own aggregation
//!   (counting, merging into a map, per-event grouping, ...).
//! - [`CapturedEventsLayer`] is the full `Layer<S>` for the common case: an
//!   `on_event` capture of every field of every (optionally target- and/or
//!   field-filtered) event, grouped one [`CapturedEvent`] per matching event.
//!
//! Callers whose assertions depend on a field type or normalization this
//! doesn't reproduce (an untrimmed `Debug` form, a single explicitly-typed
//! field extracted during capture rather than by post-processing, span
//! capture bespoke enough that `RecordingVisitor` doesn't fit) keep their own
//! local `Visit` impl instead of converting.

use std::fmt;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// One event's fields, in emission order, as rendered `(name, value)` pairs.
///
/// `record_debug` values are rendered with `{value:?}` and have one layer of
/// wrapping double quotes trimmed, so a `String`/`&str` field logged via `?`
/// reads the same as one logged via the typed `record_str`/`%` path. Callers
/// whose assertions depend on the untrimmed `Debug` form (or on a field type
/// this layer doesn't special-case) keep their own local `Visit` impl instead
/// of converting.
pub type CapturedEvent = Vec<(String, String)>;

/// Shared, cheaply-cloned handle to a [`CapturedEventsLayer`]'s captured
/// events. Obtained via [`CapturedEventsLayer::events`] before the layer is
/// moved into a subscriber.
#[derive(Clone, Default)]
pub struct CapturedEvents(Arc<Mutex<Vec<CapturedEvent>>>);

impl CapturedEvents {
    /// A snapshot of every event captured so far, one entry per matching
    /// `tracing::Event`, in emission order.
    pub fn events(&self) -> Vec<CapturedEvent> {
        self.0
            .lock()
            .expect("captured events lock poisoned")
            .clone()
    }

    /// Every captured event's fields flattened into one list, for callers
    /// that only need "which (name, value) pairs were logged" without
    /// per-event grouping.
    pub fn fields(&self) -> Vec<(String, String)> {
        self.events().into_iter().flatten().collect()
    }
}

type EventFilter = Arc<dyn Fn(&CapturedEvent) -> bool + Send + Sync>;

/// A `tracing_subscriber::Layer` that records every event's fields as
/// `(name, value)` string pairs, optionally narrowed to one `target` and/or
/// an arbitrary predicate over the captured fields.
#[derive(Clone)]
pub struct CapturedEventsLayer {
    events: CapturedEvents,
    target: Option<&'static str>,
    filter: Option<EventFilter>,
}

impl CapturedEventsLayer {
    /// Capture every event's fields, unfiltered.
    pub fn new() -> Self {
        Self {
            events: CapturedEvents::default(),
            target: None,
            filter: None,
        }
    }

    /// Only capture events emitted with this exact `target:`.
    pub fn with_target(mut self, target: &'static str) -> Self {
        self.target = Some(target);
        self
    }

    /// Only capture events whose fields satisfy `filter`, evaluated after the
    /// target check.
    pub fn with_event_filter(
        mut self,
        filter: impl Fn(&CapturedEvent) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.filter = Some(Arc::new(filter));
        self
    }

    /// The shared handle used to read back captured events. Call this before
    /// installing the layer on a subscriber (`.with(layer)` consumes it).
    pub fn events(&self) -> CapturedEvents {
        self.events.clone()
    }
}

impl Default for CapturedEventsLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Subscriber> Layer<S> for CapturedEventsLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if let Some(target) = self.target
            && event.metadata().target() != target
        {
            return;
        }
        let mut visitor = RecordingVisitor::default();
        event.record(&mut visitor);
        let fields = visitor.into_fields();
        if self.filter.as_ref().is_none_or(|filter| filter(&fields)) {
            self.events
                .0
                .lock()
                .expect("captured events lock poisoned")
                .push(fields);
        }
    }
}

/// The shared `Visit` implementation: records every field type, rendering
/// `record_debug` values with `{value:?}` and trimming one layer of wrapping
/// double quotes so a `String`/`&str` field logged via `?` reads the same as
/// one logged via the typed `record_str`/`%` path.
///
/// Visits anything with a `.record(&mut dyn Visit)` method — pass
/// `&mut RecordingVisitor::default()` to `tracing::Event::record`,
/// `tracing::span::Attributes::record`, or `tracing::span::Record::record` —
/// then call [`Self::into_fields`] for the collected `(name, value)` pairs.
#[derive(Default)]
pub struct RecordingVisitor {
    fields: CapturedEvent,
}

impl RecordingVisitor {
    /// Consume the visitor, returning every field it recorded, in emission
    /// order.
    pub fn into_fields(self) -> CapturedEvent {
        self.fields
    }
}

impl Visit for RecordingVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields.push((
            field.name().to_string(),
            format!("{value:?}").trim_matches('"').to_string(),
        ));
    }
}

/// The last value recorded for `name` in one captured event's field list.
pub fn field_value<'a>(event: &'a CapturedEvent, name: &str) -> Option<&'a str> {
    event
        .iter()
        .rev()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests;
