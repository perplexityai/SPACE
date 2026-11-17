//! What the files routes log when a call fails. The JSON log line keys each
//! field by name and the event's text is its `message`, so a field of that
//! name would replace the text.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use tracing_subscriber::layer::SubscriberExt as _;

use super::*;

/// Each event's fields as the JSON log line keeps them: by name, a later
/// field replacing an earlier one of the same name.
#[derive(Clone, Default)]
struct EventLog(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventLog {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().to_owned(), value.to_owned());
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                self.0.insert(field.name().to_owned(), format!("{value:?}"));
            }
        }
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

impl EventLog {
    /// The fields of the one event whose message is `message`.
    fn event(&self, message: &str) -> BTreeMap<String, String> {
        let events = self.0.lock().unwrap();
        let matching: Vec<_> = events
            .iter()
            .filter(|fields| fields.get("message").map(String::as_str) == Some(message))
            .collect();
        assert_eq!(matching.len(), 1, "events logged: {events:#?}");
        matching[0].clone()
    }
}

#[tokio::test]
async fn an_api_call_that_fails_is_logged_with_its_message_and_the_status() {
    if !test_process() {
        return;
    }
    let log = EventLog::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(log.clone()));
    let harness = Harness::start().await;
    harness
        .api
        .authorize(Answer::Fail(Status::unavailable("connection refused")));

    let response = harness.send(stat(&harness)).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let event = log.event("native file access call failed");
    assert_eq!(event["rpc"], "authorize");
    assert_eq!(event["grpc_message"], "connection refused");
}

#[tokio::test]
async fn a_read_that_fails_mid_stream_is_logged_with_its_message_and_the_status() {
    if !test_process() {
        return;
    }
    let log = EventLog::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(log.clone()));
    let harness = Harness::start().await;
    harness.worker.reply(Reply::Read(Ok(vec![
        Ok(FileChunk {
            data: Bytes::from_static(b"partial"),
        }),
        Err(Status::internal("guest read failed")),
    ])));

    let response = harness
        .send(harness.post("read", json!({"path": "/a", "length": 100})))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap_err();

    let event = log.event("native file read failed mid-stream");
    assert_eq!(event["grpc_message"], "guest read failed");
}
