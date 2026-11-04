//! Bridge SQLx query and successful pool-acquire events into OpenTelemetry.
//!
//! SQLx 0.9 emits `tracing::event!` records when queries and successful pool
//! acquires finish; it does not open spans. `tracing_opentelemetry` only
//! converts spans into OTel/APM spans, so this layer back-dates short-lived OTel
//! spans from the event durations and parents them under the active request.
//! Acquire events come from SQLx's shared `PoolInner::acquire` path, so they
//! include explicit acquires, implicit `&Pool` query acquires, and transaction
//! begins. Upstream emits no event for timeout/error attempts and supplies no
//! pool identity, driver identity, or physical-connect phase, so this layer
//! cannot report those.
//!
//! Node-local SQLite code emits compatible `rusqlite::query` events from
//! rusqlite's profile hook so those queries follow the same path.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use opentelemetry::{
    KeyValue, global,
    trace::{Span, SpanKind, Tracer},
};
use tracing::{
    Event, Id, Metadata, Subscriber,
    dispatcher::WeakDispatch,
    field::Visit,
    span::{Attributes, Record},
    subscriber::Interest,
};
use tracing_opentelemetry::get_otel_context;
use tracing_subscriber::{
    EnvFilter, Layer, layer::Context as LayerContext, layer::Filter, registry::LookupSpan,
};

use crate::{
    DB_POOL_ACQUIRE_TOTAL_MS_METRIC, RUSQLITE_QUERY_TARGET, SQLX_ACQUIRE_TARGET, SQLX_QUERY_TARGET,
    distribution,
};

const SQLITE_DB_SYSTEM: &str = "sqlite";
const DEFAULT_DB_SYSTEM: &str = "postgresql";

pub(crate) struct DbQueryOtelLayer {
    dispatch: OnceLock<WeakDispatch>,
    /// The trace filter exactly as the operator wrote it.
    ///
    /// This layer is installed with more admitted than the operator asked
    /// for, so the pool-acquire metric is recorded at any verbosity and the
    /// driver classification stays visible. That widening must not decide what
    /// gets exported, and no summary of the filter can stand in for it:
    /// reading its directives as text missed a rule on a parent module in one
    /// direction and a dynamic `[span]=level` rule in the other. So the filter
    /// itself is held here and asked.
    operator: EnvFilter,
    /// Whether the held filter carries a `[span]=level` directive.
    ///
    /// Only those need the span-tracking hooks below; a target/level-only
    /// filter answers from its static rules alone, and forwarding to it would
    /// take `EnvFilter`'s per-span locks for nothing. This is the same test
    /// `layer_filter` uses to decide the very same question.
    dynamic: bool,
}

impl DbQueryOtelLayer {
    pub(crate) fn new(operator: EnvFilter) -> Self {
        Self {
            dispatch: OnceLock::new(),
            dynamic: operator.to_string().contains('['),
            operator,
        }
    }

    /// Whether the operator's filter selects this event for tracing.
    ///
    /// Only the pool-acquire path needs this. Query events are decided by the
    /// filter this layer is installed with, which never widens for them. A
    /// traced parent is not an answer either: an enclosing span may be
    /// selected while the events inside it are not.
    fn selected_by_operator<S>(&self, event: &Event<'_>, ctx: &LayerContext<'_, S>) -> bool
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        self.operator.enabled(event.metadata(), ctx.clone())
    }

    /// The OTel context of the nearest enclosing span the trace layer recorded.
    ///
    /// `Span::current().context()` resolves against the globally current span,
    /// which the trace filter may have kept for logging only. For such a span
    /// tracing-opentelemetry answers with no context rather than with its
    /// traced ancestor, and an export built on that becomes a detached root
    /// trace while the enclosing request stays properly traced. Walking this
    /// layer's own scope outward finds the span that is actually being traced,
    /// and reports nothing when none is.
    fn traced_parent<S>(
        &self,
        event: &Event<'_>,
        ctx: &LayerContext<'_, S>,
    ) -> Option<opentelemetry::Context>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let dispatch = self.dispatch.get().and_then(WeakDispatch::upgrade)?;
        ctx.event_scope(event)?
            .find_map(|span| get_otel_context(&span.id(), &dispatch))
    }
}

impl<S> Layer<S> for DbQueryOtelLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        let _ = self.dispatch.set(subscriber.downgrade());
    }

    /// Deliberately no `max_level_hint`: the held filter's ceiling must not
    /// reach the registry, because this layer has to keep seeing the events
    /// the operator excluded in order to record the pool-acquire metric.
    ///
    /// Register every callsite with the held filter as well.
    ///
    /// `EnvFilter` builds the matcher for a dynamic `[span]=level` directive
    /// only while registering a callsite, so a filter that never sees them
    /// silently stops matching spans and reverts to its static rules. The
    /// verdict is discarded: this layer must keep seeing events the operator
    /// excluded, because the pool-acquire metric is built from them.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.dynamic {
            let _ = Filter::<S>::callsite_enabled(&self.operator, metadata);
        }
        Interest::always()
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        // The held filter tracks which dynamic directives are active by
        // observing spans, so it needs the same notifications this layer gets.
        if self.dynamic {
            self.operator.on_new_span(attrs, id, ctx.clone());
        }
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut visitor = DbSystemVisitor::default();
        attrs.record(&mut visitor);
        if let Some(db_system) = visitor.db_system {
            span.extensions_mut().insert(SqlSpanFields { db_system });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, S>) {
        // A field directive such as `[request{tenant=acme}]=info` is satisfied
        // by a field recorded after the span opens, so the held filter needs
        // these too, not just `on_new_span`.
        if self.dynamic {
            self.operator.on_record(id, values, ctx.clone());
        }
        let Some(span) = ctx.span(id) else {
            return;
        };

        let mut visitor = DbSystemVisitor::default();
        values.record(&mut visitor);
        if let Some(db_system) = visitor.db_system {
            span.extensions_mut().insert(SqlSpanFields { db_system });
        }
    }

    fn on_enter(&self, id: &Id, ctx: LayerContext<'_, S>) {
        if self.dynamic {
            self.operator.on_enter(id, ctx);
        }
    }

    fn on_exit(&self, id: &Id, ctx: LayerContext<'_, S>) {
        if self.dynamic {
            self.operator.on_exit(id, ctx);
        }
    }

    fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
        if self.dynamic {
            self.operator.on_close(id, ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        let target = event.metadata().target();
        if target == SQLX_ACQUIRE_TARGET {
            self.export_acquire(event, &ctx);
            return;
        }
        if !matches!(target, SQLX_QUERY_TARGET | RUSQLITE_QUERY_TARGET) {
            return;
        }

        let mut visitor = DbQueryEventVisitor::default();
        event.record(&mut visitor);

        let Some(parent_cx) = self.traced_parent(event, &ctx) else {
            return;
        };

        let elapsed = Duration::from_secs_f64(visitor.elapsed_secs.max(0.0));
        let end_time = SystemTime::now();
        let start_time = end_time.checked_sub(elapsed).unwrap_or(end_time);

        let statement = visitor
            .db_statement
            .or_else(|| visitor.summary.clone())
            .unwrap_or_default();
        let span_name = span_name_from_statement(&statement);
        let db_system = visitor
            .db_system
            .or_else(|| db_system_from_scope(&ctx, event))
            .unwrap_or_else(|| default_db_system_for_target(target).to_string());

        let tracer = global::tracer("db-query-events");
        let mut span = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Client)
            .with_start_time(start_time)
            .with_attributes([
                KeyValue::new("db.system", db_system),
                KeyValue::new("db.statement", statement),
                KeyValue::new("db.rows_affected", visitor.rows_affected),
                KeyValue::new("db.rows_returned", visitor.rows_returned),
            ])
            .start_with_context(&tracer, &parent_cx);
        span.end_with_timestamp(end_time);
    }
}

impl DbQueryOtelLayer {
    fn export_acquire<S>(&self, event: &Event<'_>, ctx: &LayerContext<'_, S>)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut visitor = DbAcquireEventVisitor::default();
        event.record(&mut visitor);
        let Some(elapsed) = visitor.elapsed() else {
            return;
        };

        // The upstream event carries no driver identity. Resolve it like the
        // query path — the innermost enclosing span that recorded `db.system`
        // — and default to Postgres, the only driver whose pools enable
        // routine acquire timing. SQLx's default slow-acquire warning also
        // fires for the sqlx-SQLite migration pools, whose `sqlite.migrate`
        // span records `db.system = "sqlite"`.
        let db_system =
            db_system_from_scope(ctx, event).unwrap_or_else(|| DEFAULT_DB_SYSTEM.to_string());

        // SQLx 0.9 emits this event only after a successful PoolInner acquire.
        // It covers capacity wait plus idle validation/hooks, or physical connect
        // plus after_connect. The event cannot identify the pool or split phases.
        // The metric is Postgres pool telemetry; keep other drivers out of it.
        if db_system == DEFAULT_DB_SYSTEM {
            distribution(
                DB_POOL_ACQUIRE_TOTAL_MS_METRIC,
                elapsed.as_secs_f64() * 1_000.0,
                &[],
            );
        }

        // The metric is recorded above at any verbosity; the span is the part
        // the operator's trace filter still governs.
        if !self.selected_by_operator(event, ctx) {
            return;
        }

        let Some(parent_cx) = self.traced_parent(event, ctx) else {
            return;
        };
        let end_time = SystemTime::now();
        let start_time = end_time.checked_sub(elapsed).unwrap_or(end_time);
        let tracer = global::tracer("db-pool-acquire-events");
        let mut span = tracer
            .span_builder("db.pool.acquire")
            .with_kind(SpanKind::Client)
            .with_start_time(start_time)
            .with_attributes([
                KeyValue::new("db.system", db_system),
                KeyValue::new("db.operation.name", "acquire"),
            ])
            .start_with_context(&tracer, &parent_cx);
        span.end_with_timestamp(end_time);
    }
}

#[derive(Default)]
struct DbAcquireEventVisitor {
    elapsed_secs: Option<f64>,
}

impl DbAcquireEventVisitor {
    fn elapsed(&self) -> Option<Duration> {
        self.elapsed_secs
            .filter(|elapsed| elapsed.is_finite() && *elapsed >= 0.0)
            .map(Duration::from_secs_f64)
    }
}

impl Visit for DbAcquireEventVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "acquired_after_secs" {
            self.elapsed_secs = Some(value);
        }
    }
}

fn default_db_system_for_target(target: &str) -> &'static str {
    match target {
        RUSQLITE_QUERY_TARGET => SQLITE_DB_SYSTEM,
        _ => DEFAULT_DB_SYSTEM,
    }
}

/// Word budget matching sqlx's `parse_query_summary`, so span names keep the
/// same SQL-shape naming that existing dashboards and monitors group on.
const SPAN_NAME_WORDS: usize = 4;

/// Derive the span name from the statement with any leading
/// `/*module_path:line*/` locator comment (see `pplx_space_common_core::sql!`)
/// stripped. The locator varies per call site; leaving it in the name would
/// fragment one query shape into per-call-site span names, while
/// `db.statement` already carries the locator for attribution.
fn span_name_from_statement(statement: &str) -> String {
    let sql = strip_leading_sql_comment(statement);
    let name = sql
        .split_whitespace()
        .take(SPAN_NAME_WORDS)
        .collect::<Vec<&str>>()
        .join(" ");
    if name.is_empty() {
        "db.query".to_string()
    } else {
        name
    }
}

fn strip_leading_sql_comment(sql: &str) -> &str {
    let trimmed = sql.trim_start();
    let Some(rest) = trimmed.strip_prefix("/*") else {
        return sql;
    };
    match rest.split_once("*/") {
        Some((_, after)) => after,
        // Unterminated comment: the statement was truncated mid-comment
        // (e.g. a word-budgeted summary), nothing usable to strip to.
        None => sql,
    }
}

#[derive(Clone)]
struct SqlSpanFields {
    db_system: String,
}

fn db_system_from_scope<S>(ctx: &LayerContext<'_, S>, event: &Event<'_>) -> Option<String>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let scope = ctx.event_scope(event)?;
    let mut db_system = None;
    for span in scope.from_root() {
        if let Some(fields) = span.extensions().get::<SqlSpanFields>() {
            db_system = Some(fields.db_system.clone());
        }
    }
    db_system
}

#[derive(Default)]
struct DbQueryEventVisitor {
    summary: Option<String>,
    db_system: Option<String>,
    db_statement: Option<String>,
    elapsed_secs: f64,
    rows_affected: i64,
    rows_returned: i64,
}

impl Visit for DbQueryEventVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "summary" => self.summary = Some(value.to_string()),
            "db.system" if !value.is_empty() => {
                self.db_system = Some(value.to_string());
            }
            "db.statement" if !value.is_empty() => {
                self.db_statement = Some(value.to_string());
            }
            _ => {}
        }
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "elapsed_secs" {
            self.elapsed_secs = value;
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "rows_affected" => self.rows_affected = value as i64,
            "rows_returned" => self.rows_returned = value as i64,
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        match field.name() {
            "rows_affected" => self.rows_affected = value,
            "rows_returned" => self.rows_returned = value,
            _ => {}
        }
    }
}

#[derive(Default)]
struct DbSystemVisitor {
    db_system: Option<String>,
}

impl Visit for DbSystemVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "db.system" && !value.is_empty() {
            self.db_system = Some(value.to_string());
        }
    }
}

#[cfg(test)]
mod tests;
