use super::*;
use crate::QueuedSlots;
use std::convert::Infallible;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

fn request(method: &str) -> http::Request<Body> {
    http::Request::builder()
        .uri(format!("/test.Filesystem/{method}"))
        .body(Body::empty())
        .expect("request")
}

#[tokio::test]
async fn overload_never_enters_decoder_and_execution_retains_admission_after_cancellation() {
    let entered = Arc::new(AtomicUsize::new(0));
    let execution = Arc::new(Mutex::new(None));
    let observed = entered.clone();
    let retained = execution.clone();
    let inner = tower::service_fn(move |mut request: http::Request<Body>| {
        observed.fetch_add(1, Ordering::SeqCst);
        let blocked = request.uri().path().ends_with("/WriteBatch");
        if blocked {
            *retained.lock().expect("execution") =
                request.extensions_mut().remove::<RequestPermit>();
        }
        async move {
            if blocked {
                std::future::pending::<()>().await;
            }
            Ok::<_, Infallible>(http::Response::new(Body::empty()))
        }
    });
    let slots = Arc::new(Semaphore::new(4));
    let mut service = RequestAdmission::new(
        inner,
        QueuedSlots::new(slots.clone()),
        &[("Write", 1), ("WriteBatch", 4), ("Rename", 1)],
    );
    let mut other_listener = service.clone();
    let pending = service.call(request("WriteBatch"));
    for method in ["Write", "WriteBatch", "Rename"] {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            other_listener.call(request(method)),
        )
        .await
        .expect("overload is rejected, not queued")
        .expect("response");
        assert_eq!(response.headers()["grpc-status"], "8");
    }
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "rejection must precede the decoder service"
    );
    other_listener
        .call(request("Watch"))
        .await
        .expect("watch unaffected");
    drop(pending);
    assert_eq!(
        slots.available_permits(),
        0,
        "execution still owns the payload"
    );
    execution.lock().expect("execution").take();
    assert_eq!(slots.available_permits(), 4);
    let response = other_listener
        .call(request("Write"))
        .await
        .expect("capacity recovered");
    assert!(!response.headers().contains_key("grpc-status"));
    assert_eq!(slots.available_permits(), 4, "success releases admission");
}

#[tokio::test]
async fn dropping_an_unpolled_request_releases_admission() {
    let inner = tower::service_fn(|_: http::Request<Body>| {
        std::future::pending::<Result<http::Response<Body>, Infallible>>()
    });
    let slots = Arc::new(Semaphore::new(1));
    let mut service =
        RequestAdmission::new(inner, QueuedSlots::new(slots.clone()), &[("Write", 1)]);
    let request = service.call(request("Write"));
    assert_eq!(slots.available_permits(), 0);
    drop(request);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn bounded_queue_does_not_decode_and_releases_on_cancel_or_timeout() {
    let entered = Arc::new(AtomicUsize::new(0));
    let observed = entered.clone();
    let inner = tower::service_fn(move |_: http::Request<Body>| {
        observed.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(http::Response::new(Body::empty())) }
    });
    let slots = Arc::new(Semaphore::new(8));
    let occupied = slots.clone().acquire_many_owned(8).await.unwrap();
    let mut service = RequestAdmission::new(
        inner,
        QueuedSlots::new(slots.clone()).with_queue(2, Duration::from_millis(50)),
        &[("Start", 4)],
    );
    let first = service.call(request("Start"));
    let second = service.clone().call(request("Start"));
    let rejected = service.call(request("Start")).await.unwrap();
    assert_eq!(rejected.headers()["grpc-status"], "8");
    assert_eq!(entered.load(Ordering::SeqCst), 0);
    // A cancelled waiter frees its place: the next request queues again.
    drop(first);
    let mut requeued = service.call(request("Start"));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut requeued)
            .await
            .is_err(),
        "the freed place queues the next request instead of rejecting it"
    );
    let rejected = second.await.unwrap();
    assert_eq!(rejected.headers()["grpc-status"], "8");
    let rejected = requeued.await.unwrap();
    assert_eq!(rejected.headers()["grpc-status"], "8");
    assert_eq!(entered.load(Ordering::SeqCst), 0);
    drop(occupied);
    service.call(request("Start")).await.unwrap();
    assert_eq!(slots.available_permits(), 8);
    assert_eq!(entered.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn twenty_starts_drain_without_expanding_decoded_payload_capacity() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let observed_active = active.clone();
    let observed_maximum = maximum.clone();
    let inner = tower::service_fn(move |_: http::Request<Body>| {
        let active = observed_active.clone();
        let maximum = observed_maximum.clone();
        async move {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, Infallible>(http::Response::new(Body::empty()))
        }
    });
    let slots = Arc::new(Semaphore::new(8));
    let mut service = RequestAdmission::new(
        inner,
        QueuedSlots::new(slots.clone()).with_queue(20, Duration::from_millis(50)),
        &[("Start", 4)],
    );
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..20 {
        requests.spawn(service.call(request("Start")));
    }
    while let Some(response) = requests.join_next().await {
        assert!(
            !response
                .unwrap()
                .unwrap()
                .headers()
                .contains_key("grpc-status")
        );
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(slots.available_permits(), 8);
}

#[tokio::test(start_paused = true)]
async fn queue_deadline_includes_time_before_first_poll() {
    let inner = tower::service_fn(
        |_: http::Request<Body>| -> std::future::Ready<Result<http::Response<Body>, Infallible>> {
            panic!("an expired queued request must not execute")
        },
    );
    let slots = Arc::new(Semaphore::new(4));
    let occupied = slots.clone().acquire_many_owned(4).await.unwrap();
    let mut service = RequestAdmission::new(
        inner,
        QueuedSlots::new(slots).with_queue(1, Duration::from_millis(50)),
        &[("Start", 4)],
    );
    let queued = service.call(request("Start"));
    tokio::time::advance(Duration::from_millis(51)).await;
    drop(occupied);
    assert_eq!(queued.await.unwrap().headers()["grpc-status"], "8");
}
