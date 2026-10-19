use std::collections::HashMap;

use opentelemetry::{
    Context, KeyValue,
    baggage::BaggageExt as _,
    propagation::TextMapPropagator as _,
    trace::{TraceContextExt as _, Tracer as _, TracerProvider as _},
};

#[test]
fn text_map_propagator_injects_trace_context_and_baggage() {
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::default();
    let span = provider.tracer("space-observability-test").start("root");
    let cx =
        Context::current_with_span(span).with_baggage(vec![KeyValue::new("space.test", "upgrade")]);

    let mut carrier = HashMap::new();
    super::text_map_propagator().inject_context(&cx, &mut carrier);

    assert!(carrier.contains_key("traceparent"));
    assert_eq!(
        carrier.get("baggage").map(String::as_str),
        Some("space.test=upgrade")
    );
}

#[test]
fn traceparent_link_preserves_follower_parent_and_rejects_invalid_context() {
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt as _;

    opentelemetry::global::set_text_map_propagator(super::text_map_propagator());
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("wake-link-test")));
    let request_traceparent = "00-11111111111111111111111111111111-2222222222222222-01";
    let leader_traceparent = "00-33333333333333333333333333333333-4444444444444444-01";
    tracing::subscriber::with_default(subscriber, || {
        let wait = tracing::info_span!("restore.wait");
        super::trace_propagation::set_span_parent_from_traceparent(&wait, request_traceparent);
        super::trace_propagation::link_span_from_traceparent(&wait, leader_traceparent);
        super::trace_propagation::link_span_from_traceparent(&wait, "invalid");
    });
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    let wait = &spans[0];
    assert_eq!(
        wait.span_context.trace_id().to_string(),
        "11111111111111111111111111111111"
    );
    assert_eq!(wait.parent_span_id.to_string(), "2222222222222222");
    assert_eq!(wait.links.links.len(), 1);
    assert_eq!(
        wait.links.links[0].span_context.trace_id().to_string(),
        "33333333333333333333333333333333"
    );
    assert_eq!(
        wait.links.links[0].span_context.span_id().to_string(),
        "4444444444444444"
    );
}

#[test]
fn only_an_unsampled_context_marked_untraced_is_withheld_as_an_origin() {
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};

    let context = |flags: TraceFlags, state: &[(&str, &str)]| {
        SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            flags,
            true,
            TraceState::from_key_value(state.iter().copied()).unwrap(),
        )
    };
    for (flags, state, untraced) in [
        (TraceFlags::default(), &[("space", "untraced")][..], true),
        (
            TraceFlags::default(),
            &[("vendor", "x"), ("space", "untraced")][..],
            true,
        ),
        (TraceFlags::SAMPLED, &[("space", "untraced")][..], false),
        (TraceFlags::default(), &[][..], false),
        (TraceFlags::default(), &[("space", "other")][..], false),
        (TraceFlags::SAMPLED, &[][..], false),
    ] {
        assert_eq!(
            super::trace_propagation::is_untraced_span_context(&context(flags, state)),
            untraced,
            "flags {flags:?}, tracestate {state:?}"
        );
    }
}

#[test]
fn a_sampled_caller_context_marked_untraced_is_still_saved_as_an_origin() {
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt as _;

    opentelemetry::global::set_text_map_propagator(super::text_map_propagator());
    let provider = SdkTracerProvider::builder().build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("origin-test")));
    let saved = |traceparent: &str| {
        let mut headers = http::HeaderMap::new();
        headers.insert("traceparent", traceparent.parse().unwrap());
        headers.insert("tracestate", "space=untraced".parse().unwrap());
        let span = tracing::info_span!("request");
        super::trace_propagation::set_span_parent_from_headers(&span, &headers);
        super::trace_propagation::traceparent_for_span(&span)
    };
    let (sampled, unsampled) = tracing::subscriber::with_default(subscriber, || {
        (
            saved("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            saved("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"),
        )
    });
    let sampled = sampled.expect("a sampled context is saved");
    assert!(sampled.starts_with("00-0af7651916cd43dd8448eb211c80319c-"));
    assert!(sampled.ends_with("-01"));
    assert_eq!(unsampled, None);
}
