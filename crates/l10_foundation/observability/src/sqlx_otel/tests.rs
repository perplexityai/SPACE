use super::{DbAcquireEventVisitor, span_name_from_statement, strip_leading_sql_comment};

#[test]
fn span_name_drops_locator_comment_and_keeps_four_words() {
    let statement =
        "/*example_space_api::auth:42*/ SELECT id, org_id FROM api_tokens WHERE token = $1";
    assert_eq!(
        span_name_from_statement(statement),
        "SELECT id, org_id FROM"
    );
}

#[test]
fn span_name_without_comment_matches_sqlx_summary_shape() {
    assert_eq!(
        span_name_from_statement("SELECT drain_phase FROM nodes WHERE id = $1"),
        "SELECT drain_phase FROM nodes"
    );
}

#[test]
fn span_name_falls_back_when_statement_is_empty() {
    assert_eq!(span_name_from_statement(""), "db.query");
    assert_eq!(
        span_name_from_statement("/*example_space_api::auth:42*/ "),
        "db.query"
    );
}

#[test]
fn unterminated_comment_is_left_alone() {
    // A word-budgeted summary can truncate mid-comment; keep it rather
    // than stripping to an empty name.
    assert_eq!(strip_leading_sql_comment("/*example"), "/*example");
    assert_eq!(span_name_from_statement("/*example"), "/*example");
}

#[test]
fn acquire_bridge_resolves_db_system_and_keeps_metric_postgres_only() {
    use std::sync::Mutex;

    let _guard = GLOBAL_TRACER
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt as _;

    struct CaptureSink;
    static CAPTURED: Mutex<
        Vec<(
            String,
            crate::sink::MetricKind,
            crate::sink::MetricUnit,
            f64,
        )>,
    > = Mutex::new(Vec::new());
    impl crate::sink::MetricSink for CaptureSink {
        fn label(&self) -> &'static str {
            "test-capture"
        }

        fn emit(&self, metric: crate::sink::Metric<'_>) {
            if let crate::sink::MetricValue::Float(value) = metric.value {
                CAPTURED.lock().unwrap().push((
                    metric.name.to_string(),
                    metric.kind,
                    metric.unit,
                    value,
                ));
            }
        }
    }
    let _ = crate::METRICS.set(Box::new(CaptureSink));

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    global::set_tracer_provider(provider.clone());

    let subscriber = tracing_subscriber::registry()
        .with(super::DbQueryOtelLayer::new(
            tracing_subscriber::EnvFilter::new("info"),
        ))
        .with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("space-observability-test")),
        );

    tracing::subscriber::with_default(subscriber, || {
        {
            let span = tracing::info_span!("sqlite.migrate", db.system = "sqlite");
            let _entered = span.enter();
            tracing::warn!(
                target: "sqlx::pool::acquire",
                acquired_after_secs = 2.5_f64,
                slow_acquire_threshold_secs = 2.0_f64,
                "acquired connection, but time to acquire exceeded slow threshold"
            );
        }
        {
            let span = tracing::info_span!("handle_request");
            let _entered = span.enter();
            tracing::info!(
                target: "sqlx::pool::acquire",
                acquired_after_secs = 0.25_f64,
                "acquired connection"
            );
        }
    });

    let spans = exporter.get_finished_spans().unwrap();
    let acquire_db_systems: Vec<String> = spans
        .iter()
        .filter(|span| span.name == "db.pool.acquire")
        .map(|span| {
            span.attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == "db.system")
                .expect("db.pool.acquire span has db.system")
                .value
                .to_string()
        })
        .collect();
    assert_eq!(acquire_db_systems, ["sqlite", "postgresql"]);

    let captured = CAPTURED.lock().unwrap().clone();
    assert_eq!(
        captured,
        [(
            crate::DB_POOL_ACQUIRE_TOTAL_MS_METRIC.to_string(),
            crate::sink::MetricKind::Distribution,
            crate::sink::MetricUnit::Inferred,
            250.0
        )],
        "only Postgres acquires feed the Postgres pool metric"
    );
}
#[test]
fn acquire_elapsed_rejects_missing_or_invalid_values() {
    assert_eq!(
        DbAcquireEventVisitor {
            elapsed_secs: Some(0.125),
        }
        .elapsed(),
        Some(std::time::Duration::from_millis(125))
    );
    for elapsed_secs in [None, Some(-1.0), Some(f64::NAN), Some(f64::INFINITY)] {
        assert_eq!(DbAcquireEventVisitor { elapsed_secs }.elapsed(), None);
    }
}

/// Trace verbosity must not decide which database's numbers land in the
/// PostgreSQL pool metric.
///
/// The metric's driver classification reads `db.system` from an enclosing
/// span, so the bridge's filter has to keep those spans visible even when the
/// trace filter excludes them. Without that, a SQLite migration pool's slow
/// acquire has no identity, defaults to PostgreSQL, and is recorded.
#[test]
fn the_db_bridge_sees_identity_spans_whatever_the_trace_filter_says() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct RecordingLayer(Arc<Mutex<Vec<String>>>);
    impl<S> tracing_subscriber::Layer<S> for RecordingLayer
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_string());
        }
    }

    let seen = RecordingLayer::default();
    let db_filter = tracing_subscriber::EnvFilter::new("off");
    let subscriber = tracing_subscriber::registry()
        .with(seen.clone().with_filter(crate::db_layer_filter(db_filter)));

    tracing::subscriber::with_default(subscriber, || {
        let migrate = tracing::info_span!("sqlite.migrate", db.system = "sqlite");
        drop(migrate);
        let unrelated = tracing::info_span!("handle_request");
        drop(unrelated);
    });

    let seen = seen.0.lock().unwrap().clone();
    assert!(
        seen.contains(&"sqlite.migrate".to_string()),
        "a span carrying db.system must stay visible with traces off: {seen:?}"
    );
    assert!(
        !seen.contains(&"handle_request".to_string()),
        "the trace filter still governs every other span: {seen:?}"
    );
}

/// A filter that selects this module's spans by module path and nothing else,
/// which is the shape of a rule naming a service's own request handler.
const MODULE_SELECTED: &str = concat!("off,", module_path!(), "=info");

/// The subscriber layouts the bridge has to behave the same in.
#[derive(Clone, Copy, Debug)]
enum Layout {
    /// One subscriber-level filter for logs and traces, which is what a
    /// service gets when it sets `RUST_LOG` and nothing else.
    Shared,
    /// A per-layer filter, which a configured trace filter builds.
    PerLayer,
}

/// The global tracer provider is process-wide, so the tests that install one
/// take this lock rather than reading each other's spans.
static GLOBAL_TRACER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Exported `db.pool.acquire` spans for one filter in one layout.
///
/// Three pool-acquire events are emitted inside a `grpc.server` span, at
/// TRACE, INFO and WARN, so a row's count says which levels survived. A fourth
/// follows in an unnamed span, after that one closes.
fn exported_acquire_spans(filter: &str, layout: Layout) -> usize {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("acquire-gate-test");

    let emit = || {
        {
            // The field is declared empty and recorded afterwards, which is
            // how a field directive's subject usually arrives.
            let span = tracing::info_span!("grpc.server", tenant = tracing::field::Empty);
            span.record("tenant", "acme");
            let _entered = span.enter();
            tracing::trace!(target: "sqlx::pool::acquire", acquired_after_secs = 0.01_f64, "acquired connection");
            tracing::info!(target: "sqlx::pool::acquire", acquired_after_secs = 0.25_f64, "acquired connection");
            tracing::warn!(
                target: "sqlx::pool::acquire",
                acquired_after_secs = 2.5_f64,
                slow_acquire_threshold_secs = 2.0_f64,
                "acquired connection, but time to acquire exceeded slow threshold"
            );
        }
        // A fourth wait, in a span no `[grpc.server]` directive names. The
        // held filter tracks dynamic directives by watching spans enter and
        // leave, so a directive that outlives its span would export this one.
        let span = tracing::info_span!("cleanup");
        let _entered = span.enter();
        tracing::trace!(target: "sqlx::pool::acquire", acquired_after_secs = 0.02_f64, "acquired connection");
    };
    let operator = tracing_subscriber::EnvFilter::new(filter);
    match layout {
        Layout::Shared => {
            let subscriber = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::new(filter)
                        .add_directive(crate::acquire_directive().expect("acquire directive")),
                )
                .with(super::DbQueryOtelLayer::new(operator))
                .with(tracing_opentelemetry::layer().with_tracer(tracer));
            tracing::subscriber::with_default(subscriber, emit);
        }
        Layout::PerLayer => {
            let trace_filter = tracing_subscriber::EnvFilter::new(filter);
            let subscriber =
                tracing_subscriber::registry()
                    .with(super::DbQueryOtelLayer::new(operator).with_filter(
                        crate::db_layer_filter(tracing_subscriber::EnvFilter::new(filter)),
                    ))
                    .with(
                        tracing_opentelemetry::layer()
                            .with_tracer(tracer)
                            .with_filter(crate::layer_filter(trace_filter)),
                    );
            tracing::subscriber::with_default(subscriber, emit);
        }
    }

    exporter
        .get_finished_spans()
        .expect("finished spans")
        .iter()
        .filter(|span| span.name == "db.pool.acquire")
        .count()
}

/// The whole acquire-span decision, as a table, so a fix for one row cannot
/// quietly change another.
///
/// The operator's filter decides, in both layouts. Nothing summarises it: two
/// earlier attempts read its directives as text and both were wrong — one
/// missed a rule on a parent module, the other missed a `[span]=level` rule
/// that enables what the default excludes.
#[test]
fn the_operator_filter_decides_the_acquire_span_in_both_layouts() {
    let _guard = GLOBAL_TRACER
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    for (filter, shared, per_layer) in [
        // No rule names the target, so the general threshold governs. TRACE is
        // below `info`, and `warn` is above the enclosing span, which leaves
        // the acquire events with no traced parent to hang from.
        ("info", 2, 2),
        ("off", 0, 0),
        ("warn", 0, 0),
        // An explicit rule is honoured. Before this fix the shared layout
        // ignored all three of these and exported 2.
        ("info,sqlx::pool::acquire=off", 0, 0),
        ("info,sqlx::pool::acquire=warn", 1, 1),
        // A rule more verbose than the forced admission. The per-layer filter
        // widens to it; the shared layout's single filter cannot, because its
        // forced `sqlx::pool::acquire=info` replaces the operator's rule
        // rather than adding to it. That force predates the split layout and
        // governs logs too, so it is left as it is.
        ("info,sqlx::pool::acquire=trace", 2, 4),
        // Rules match by module path, so a rule on a parent module reaches
        // this target, and the most specific one wins.
        ("info,sqlx::pool=off", 0, 0),
        ("info,sqlx=off", 0, 0),
        ("info,sqlx::pool=off,sqlx::pool::acquire=warn", 1, 1),
        // A neighbouring target that merely shares a prefix is not this one.
        ("info,sqlx::pool::acquire_extra=off", 2, 2),
        ("info,sqlx::poolx=off", 2, 2),
        // A dynamic rule enables spans the default excludes, and the events
        // inside them. Getting these rows requires the callsite feed in
        // `register_callsite`; see the test below.
        ("off,[grpc.server]=info", 2, 2),
        ("off,[grpc.server]=warn", 1, 1),
        // The reported case: a module rule selects the request span while the
        // acquire target stays off. A traced parent supplies context, not
        // permission, so nothing is exported.
        (MODULE_SELECTED, 0, 0),
        // A field directive is satisfied by a field recorded after the span
        // opened, so the held filter has to see `on_record` as well.
        ("off,[grpc.server{tenant=acme}]=info", 2, 2),
        ("off,[grpc.server{tenant=other}]=info", 0, 0),
        // A dynamic rule for a span that is never opened changes nothing.
        ("info,[unrelated_span]=info", 2, 2),
        // And a dynamic rule stops applying when its span closes: the fourth
        // wait is outside it and below the static threshold, so three.
        ("info,[grpc.server]=trace", 3, 3),
    ] {
        assert_eq!(
            exported_acquire_spans(filter, Layout::Shared),
            shared,
            "shared layout: {filter}"
        );
        assert_eq!(
            exported_acquire_spans(filter, Layout::PerLayer),
            per_layer,
            "per-layer layout: {filter}"
        );
    }
}

/// The `[span]=level` rows above depend on `Self::dynamic` being right.
///
/// `EnvFilter::clone` resets the per-callsite matchers those directives need,
/// and the bridge rebuilds them only when it believes the filter has any. Get
/// this wrong in one direction and dynamic selection silently stops working;
/// wrong in the other and every span pays `EnvFilter`'s per-span locks for a
/// filter that keeps no per-span state.
#[test]
fn dynamic_directives_are_recognised_exactly() {
    for (filter, dynamic) in [
        ("info", false),
        ("off", false),
        ("info,sqlx::pool::acquire=off", false),
        ("info,sqlx::pool=off,sqlx::pool::acquire=warn", false),
        ("off,[grpc.server]=info", true),
        ("off,[grpc.server{tenant=acme}]=info", true),
        ("off,[{tenant=acme}]=info", true),
        ("info,[unrelated_span]=info", true),
    ] {
        assert_eq!(
            super::DbQueryOtelLayer::new(tracing_subscriber::EnvFilter::new(filter)).dynamic,
            dynamic,
            "{filter}"
        );
    }
}

/// A span kept for classification, but not traced, must not become the parent.
///
/// The bridge is admitted more than the trace filter asks for, so its view of
/// an event's scope can hold spans the trace layer never recorded. Stopping at
/// the innermost one finds a span with no OTel context and gives up, losing
/// the export even though the request around it is traced. This is the real
/// shape: a SQLite migration span the bridge sees for `db.system` alone.
#[test]
fn the_acquire_span_hangs_from_the_nearest_traced_ancestor() {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    let _guard = GLOBAL_TRACER
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    // The intermediate span is excluded by target, so the trace layer never
    // records it, while the bridge still sees it for its `db.system` field.
    const FILTER: &str = "info,classification_only=off";
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    global::set_tracer_provider(provider.clone());
    let subscriber = tracing_subscriber::registry()
        .with(
            super::DbQueryOtelLayer::new(tracing_subscriber::EnvFilter::new(FILTER)).with_filter(
                crate::db_layer_filter(tracing_subscriber::EnvFilter::new(FILTER)),
            ),
        )
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("parent-walk-test"))
                .with_filter(crate::layer_filter(tracing_subscriber::EnvFilter::new(
                    FILTER,
                ))),
        );
    tracing::subscriber::with_default(subscriber, || {
        let request = tracing::info_span!("request");
        let _request = request.enter();
        let migrate = tracing::info_span!(target: "classification_only", "sqlite.migrate", db.system = "sqlite");
        let _migrate = migrate.enter();
        tracing::info!(target: "sqlx::pool::acquire", acquired_after_secs = 0.25_f64, "acquired connection");
    });

    let spans = exporter.get_finished_spans().expect("finished spans");
    let acquire = spans
        .iter()
        .find(|span| span.name == "db.pool.acquire")
        .expect("the acquire span survives an untraced intermediate span");
    let request = spans
        .iter()
        .find(|span| span.name == "request")
        .expect("the request is traced");
    assert_eq!(
        acquire.parent_span_id,
        request.span_context.span_id(),
        "the acquire span must hang from the traced request, not a detached root"
    );
    assert!(
        !spans.iter().any(|span| span.name == "sqlite.migrate"),
        "the intermediate span is excluded from tracing"
    );
}

/// The identity exception admits spans, never events.
///
/// Query events carry a `db.system` field too. Admitting those would export a
/// query span for a target the operator excluded by name, which is what
/// `rusqlite::query=off` exists to prevent.
#[test]
fn the_identity_exception_does_not_reopen_excluded_query_events() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct SeenLayer {
        spans: Arc<Mutex<Vec<String>>>,
        events: Arc<Mutex<Vec<String>>>,
    }
    impl<S> tracing_subscriber::Layer<S> for SeenLayer
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.spans
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_string());
        }
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.events
                .lock()
                .unwrap()
                .push(event.metadata().target().to_string());
        }
    }

    let seen = SeenLayer::default();
    // What the split layout builds when the operator excludes SQLite queries.
    let db_filter = tracing_subscriber::EnvFilter::new("info,rusqlite::query=off")
        .add_directive(crate::acquire_directive().expect("acquire directive"));
    let subscriber = tracing_subscriber::registry()
        .with(seen.clone().with_filter(crate::db_layer_filter(db_filter)));

    tracing::subscriber::with_default(subscriber, || {
        let request = tracing::info_span!("handle_request");
        let _entered = request.enter();
        // The real producer's shape: an event, at an excluded target, carrying
        // the same field the identity exception looks for.
        tracing::info!(
            target: "rusqlite::query",
            summary = "SELECT 1",
            db.system = "sqlite",
            db.statement = "SELECT 1",
            rows_affected = 0_i64,
            rows_returned = 0_i64,
            elapsed_secs = 0.01_f64,
        );
        let migrate = tracing::info_span!("sqlite.migrate", db.system = "sqlite");
        drop(migrate);
    });

    let events = seen.events.lock().unwrap().clone();
    assert!(
        !events.iter().any(|target| target == "rusqlite::query"),
        "an excluded query event must stay excluded: {events:?}"
    );
    let spans = seen.spans.lock().unwrap().clone();
    assert!(
        spans.contains(&"sqlite.migrate".to_string()),
        "a span carrying db.system is still admitted: {spans:?}"
    );
}

/// Where a `[span]=level` rule stops being dependable, and why it is not this
/// bridge's doing.
///
/// `EnvFilter` answers `callsite_enabled` with "always" for any span a dynamic
/// directive names, but `enabled` refuses that span when the directive's level
/// is less verbose than the span's own. Which of the two runs depends on the
/// summed interest of every per-layer filter, so the log filter decides it:
/// with logs at `info` the interest stays "always", `enabled` is never
/// consulted and the span is traced; quieten the logs and `enabled` runs and
/// declines.
///
/// The trace layer is subject to this on its own filter, so the named span is
/// not traced either — the acquire span then has no traced parent to hang
/// from, which is the whole of its absence here. Pinned so this reads as the
/// upstream boundary it is rather than as something to fix in the gate.
#[test]
fn a_span_rule_below_the_span_level_follows_the_log_filter() {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    let _guard = GLOBAL_TRACER
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    // `warn` is less verbose than the `info` span it names, which is what
    // makes the two answers disagree. `[grpc.server]=info` and below are
    // dependable, and the table above covers those.
    const TRACE_FILTER: &str = "off,[grpc.server]=warn";
    for (log_filter, traced) in [("info", 1), ("warn", 0), ("off", 0)] {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let subscriber = tracing_subscriber::registry()
            .with(
                super::DbQueryOtelLayer::new(tracing_subscriber::EnvFilter::new(TRACE_FILTER))
                    .with_filter(crate::db_layer_filter(tracing_subscriber::EnvFilter::new(
                        TRACE_FILTER,
                    ))),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("span-rule-test"))
                    .with_filter(crate::layer_filter(tracing_subscriber::EnvFilter::new(
                        TRACE_FILTER,
                    ))),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(tracing_subscriber::EnvFilter::new(log_filter)),
            );
        tracing::subscriber::with_default(subscriber, || {
            let request = tracing::info_span!("grpc.server");
            let _request = request.enter();
            tracing::warn!(
                target: "sqlx::pool::acquire",
                acquired_after_secs = 2.5_f64,
                "acquired connection, but time to acquire exceeded slow threshold"
            );
        });
        let spans = exporter.get_finished_spans().expect("finished spans");
        let count = |name| spans.iter().filter(|span| span.name == name).count();
        assert_eq!(count("grpc.server"), traced, "parent at log={log_filter}");
        assert_eq!(
            count("db.pool.acquire"),
            traced,
            "the acquire span follows its parent, not the gate, at log={log_filter}"
        );
    }
}
