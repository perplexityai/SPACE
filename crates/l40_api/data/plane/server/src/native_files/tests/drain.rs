//! A draining replica takes no new files request, and lets the ones it took
//! finish.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use pplx_space_sbx_mgr_client::operations::WriteResponse;
use tokio::sync::{mpsc, oneshot};

use super::*;

#[tokio::test]
async fn a_draining_replica_refuses_new_files_requests_with_the_native_503() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    harness.state.drain_started.cancel();
    for action in ["stat", "write"] {
        let read = Arc::new(AtomicBool::new(false));
        let request = Request::post(harness.path(action))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", "k")
            .body(watched_body(&read))
            .unwrap();
        let (status, headers, body) = read_body(harness.send(request).await).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{action}");
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            serde_json::to_value(&envelope.code).unwrap(),
            "service_unavailable",
            "{action}"
        );
        assert!(envelope.retryable, "{action}");
        assert_eq!(
            envelope.request_id.as_deref(),
            headers
                .get("x-request-id")
                .map(|value| value.to_str().unwrap())
        );
        assert_eq!(headers[header::RETRY_AFTER], "1", "{action}");
        // The retry should reach another replica.
        assert_eq!(headers[header::CONNECTION], "close", "{action}");
        assert!(!read.load(Ordering::SeqCst), "{action}");
    }
    assert!(harness.api.calls().is_empty());
}

#[tokio::test]
async fn a_files_request_taken_before_the_drain_finishes() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    harness
        .worker
        .reply(Reply::Write(Ok(WriteResponse { bytes_written: 1 })));
    let (chunks, received) = mpsc::channel::<Result<Bytes, std::convert::Infallible>>(1);
    let (started, reading) = oneshot::channel();
    let mut started = Some(started);
    let held = Body::from_stream(futures_util::stream::unfold(
        received,
        move |mut received| {
            if let Some(started) = started.take() {
                let _ = started.send(());
            }
            async move { received.recv().await.map(|chunk| (chunk, received)) }
        },
    ));
    let write = tokio::spawn(tower::ServiceExt::oneshot(
        crate::router(harness.state.clone()),
        Request::post(harness.path("write"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", "k-taken")
            .body(held)
            .unwrap(),
    ));
    // Authorized and reading its body when the drain starts.
    reading.await.unwrap();
    harness.state.drain_started.cancel();
    chunks
        .send(Ok(Bytes::from(
            json!({"path": "/a", "data_base64": BASE64.encode(b"x")}).to_string(),
        )))
        .await
        .unwrap();
    drop(chunks);
    assert_eq!(write.await.unwrap().unwrap().status(), StatusCode::OK);
    assert_eq!(harness.worker.calls().len(), 1);
}
