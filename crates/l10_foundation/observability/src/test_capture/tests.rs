use tracing_subscriber::layer::SubscriberExt;

use super::*;

#[test]
fn captures_every_field_type_with_debug_quotes_trimmed() {
    let layer = CapturedEventsLayer::new();
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            str_field = "plain",
            bool_field = true,
            u64_field = 7u64,
            debug_field = ?"quoted",
            "an event"
        );
    });

    let captured = events.events();
    assert_eq!(captured.len(), 1);
    let fields = &captured[0];
    assert_eq!(field_value(fields, "str_field"), Some("plain"));
    assert_eq!(field_value(fields, "bool_field"), Some("true"));
    assert_eq!(field_value(fields, "u64_field"), Some("7"));
    assert_eq!(field_value(fields, "debug_field"), Some("quoted"));
    assert_eq!(field_value(fields, "message"), Some("an event"));
}

#[test]
fn with_target_drops_events_from_other_targets() {
    let layer = CapturedEventsLayer::new().with_target("kept::target");
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "kept::target", value = 1, "kept");
        tracing::info!(target: "other::target", value = 2, "dropped");
    });

    let captured = events.events();
    assert_eq!(captured.len(), 1);
    assert_eq!(field_value(&captured[0], "value"), Some("1"));
}

#[test]
fn with_event_filter_drops_events_that_fail_the_predicate() {
    let layer = CapturedEventsLayer::new()
        .with_event_filter(|fields| field_value(fields, "keep") == Some("yes"));
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(keep = "yes", "kept");
        tracing::info!(keep = "no", "dropped");
    });

    assert_eq!(events.events().len(), 1);
}

#[test]
fn fields_flattens_every_captured_event_in_order() {
    let layer = CapturedEventsLayer::new();
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(a = 1, "first");
        tracing::info!(b = 2, "second");
    });

    let flat = events.fields();
    assert_eq!(
        flat,
        vec![
            ("message".to_string(), "first".to_string()),
            ("a".to_string(), "1".to_string()),
            ("message".to_string(), "second".to_string()),
            ("b".to_string(), "2".to_string()),
        ]
    );
}
