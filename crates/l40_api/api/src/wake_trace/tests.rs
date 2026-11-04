use super::*;
use pplx_space_observability::test_capture::RecordingVisitor;
use std::{collections::BTreeMap, sync::Mutex};
use tower_http::trace::MakeSpan;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default)]
struct RecordedFields(Arc<Mutex<BTreeMap<String, String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RecordedFields {
    fn on_record(
        &self,
        _id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = RecordingVisitor::default();
        values.record(&mut visitor);
        self.0
            .lock()
            .expect("recorded fields lock poisoned")
            .extend(visitor.into_fields());
    }
}

#[tokio::test(start_paused = true)]
async fn server_fields_include_cancelled_waits_and_placement_without_guessing_missing_nodes() {
    let fields = RecordedFields::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(fields.clone()));
    let request = axum::http::Request::new(());
    let server = pplx_space_common_svc::HttpServerMakeSpan::default().make_span(&request);
    let trace = server.in_scope(WakeTrace::new);
    assert_eq!(
        fields.0.lock().expect("recorded fields lock poisoned")["restore_kind"],
        "none"
    );
    trace.wake_required();
    scope(Some(trace.clone()), async {
        let result = tokio::time::timeout(std::time::Duration::from_millis(1200), async {
            let _wait = RestoreWait::start();
            std::future::pending::<()>().await;
        })
        .await;
        assert!(result.is_err());
    })
    .await;
    assert_eq!(
        fields.0.lock().expect("recorded fields lock poisoned")["restore_waiter_wait_ms"],
        "1200"
    );
    for (source, target, expected) in [
        (Some("node-a"), Some("node-b"), "cross_node"),
        (Some("node-a"), Some("node-a"), "in_place"),
        (None, Some("node-a"), "unknown"),
    ] {
        trace.restore_placement(source, target);
        assert_eq!(
            fields.0.lock().expect("recorded fields lock poisoned")["restore_kind"],
            expected
        );
    }
    trace.wake_completed();
    trace.completed(Some("node-a"), true);
    let recorded = fields.0.lock().expect("recorded fields lock poisoned");
    assert_eq!(recorded["wake_required"], "true");
    assert_eq!(recorded["wake_outcome"], "completed");
    assert_eq!(recorded["target_node"], "node-a");
    assert_eq!(recorded["restore_kind_basis"], "placement");
}
