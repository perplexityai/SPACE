use super::*;
use crate::QueuedSlots;
use pplx_space_metrics_emit::test_capture::{capture_counters_guard, capture_values_guard};
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

#[tokio::test(flavor = "current_thread")]
async fn overload_never_enters_decoder_and_execution_retains_admission_after_cancellation() {
    let capture = capture_counters_guard();
    let values = capture_values_guard();
    let slots = Arc::new(Semaphore::new(4));
    let handler_slots = slots.clone();
    let entered = Arc::new(AtomicUsize::new(0));
    let execution = Arc::new(Mutex::new(None));
    let observed = entered.clone();
    let retained = execution.clone();
    let inner = tower::service_fn(move |mut request: http::Request<Body>| {
        observed.fetch_add(1, Ordering::SeqCst);
        let blocked = request.uri().path().ends_with("/WriteBatch");
        let lifetime = if blocked {
            let permit = RequestPermit::try_acquire_observed(
                handler_slots.clone(),
                4,
                request.extensions_mut().remove(),
                Budget::GuestFilesystem,
            )
            .expect("reuse transport admission");
            let lifetime = permit.request_lifetime();
            *retained.lock().expect("execution") = Some(permit);
            Some(lifetime)
        } else {
            None
        };
        async move {
            if let Some(lifetime) = lifetime {
                lifetime.observe(std::future::pending::<()>()).await;
            }
            Ok::<_, Infallible>(http::Response::new(Body::empty()))
        }
    });
    let mut service = RequestAdmission::new(
        inner,
        QueuedSlots::new(slots.clone()),
        &[("Write", 1), ("WriteBatch", 4), ("Rename", 1)],
    )
    .with_observation(Budget::GuestFilesystem);
    let mut other_listener = service.clone();
    let mut pending = service.call(request("WriteBatch"));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending()
    );
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
    let mut permit = execution.lock().expect("execution").take().unwrap();
    assert!(
        permit.inner.cancelled_at.get().is_some(),
        "dropping the request must record cancellation before the supervisor does"
    );
    // Inject elapsed retention time instead of sleeping on the wall clock.
    let started = Instant::now() - Duration::from_secs(2);
    *Arc::get_mut(&mut permit.inner)
        .expect("execution is the remaining permit owner")
        .cancelled_at
        .get_mut()
        .unwrap() = started;
    // Supervisor and caller can observe the same cancellation; count it once.
    permit.observe_cancellation();
    let minimum_ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(permit);
    let maximum_ms = started.elapsed().as_secs_f64() * 1000.0;
    let values = values.finish();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].name, "spaced.guest.payload.cancelled_hold_ms");
    assert_eq!(values[1].name, "spaced.guest.payload.cancelled_slot_ms");
    for value in &values {
        assert_eq!(value.tags, ["budget:guest_filesystem"]);
    }
    assert!((minimum_ms..=maximum_ms).contains(&values[0].value));
    assert_eq!(values[1].value, values[0].value * 4.0);
    assert_eq!(slots.available_permits(), 4);
    let response = other_listener
        .call(request("Write"))
        .await
        .expect("capacity recovered");
    assert!(!response.headers().contains_key("grpc-status"));
    assert_eq!(slots.available_permits(), 4, "success releases admission");
    let counters: Vec<_> = capture
        .finish()
        .into_iter()
        .map(|event| (event.name, event.value, event.tags))
        .collect();
    let admission = |site: &str, outcome: &str| {
        (
            "spaced.guest.payload.admission".to_owned(),
            1,
            vec![
                "budget:guest_filesystem".to_owned(),
                format!("site:{site}"),
                format!("outcome:{outcome}"),
            ],
        )
    };
    assert_eq!(
        counters,
        vec![
            admission("transport", "acquired"),
            admission("handler", "reused"),
            admission("transport", "rejected"),
            admission("transport", "rejected"),
            admission("transport", "rejected"),
            (
                "spaced.guest.payload.cancelled_slots".into(),
                4,
                vec!["budget:guest_filesystem".into()]
            ),
            (
                "spaced.guest.payload.cancelled_released_slots".into(),
                4,
                vec!["budget:guest_filesystem".into()]
            ),
            admission("transport", "acquired"),
        ]
    );
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

#[test]
fn native_admission_reuses_only_sufficient_capacity_from_the_same_budget() {
    for (same_budget, held_weight, closed, admitted, outcome) in [
        (true, 2, false, true, "reused"),
        (true, 1, false, false, "rejected"),
        (false, 2, true, false, "rejected"),
        (false, 2, false, true, "reacquired"),
    ] {
        let capture = capture_counters_guard();
        let slots = Arc::new(Semaphore::new(2));
        let origin = if same_budget {
            slots.clone()
        } else {
            Arc::new(Semaphore::new(2))
        };
        let held = RequestPermit::try_acquire_observed(
            origin.clone(),
            held_weight,
            None,
            Budget::GuestProcess,
        )
        .unwrap();
        let lifetime = held.request_lifetime();
        let observed = held.clone();
        if closed {
            slots.close();
        }
        let result =
            RequestPermit::try_acquire_observed(slots.clone(), 2, Some(held), Budget::GuestProcess);
        assert_eq!(result.is_ok(), admitted);
        if admitted {
            assert_eq!(
                slots.available_permits(),
                0,
                "the target budget owns the work"
            );
        }
        drop(result);
        assert_eq!(origin.available_permits(), 2 - held_weight as usize);
        drop(observed);
        assert_eq!(
            origin.available_permits(),
            2,
            "request observation must not retain admission"
        );
        drop(lifetime);
        let counters: Vec<_> = capture
            .finish()
            .into_iter()
            .map(|event| (event.name, event.value, event.tags))
            .collect();
        let expected: Vec<_> = ["acquired", outcome]
            .into_iter()
            .map(|outcome| {
                (
                    "spaced.guest.payload.admission".to_owned(),
                    1,
                    vec![
                        "budget:guest_process".to_owned(),
                        "site:handler".to_owned(),
                        format!("outcome:{outcome}"),
                    ],
                )
            })
            .collect();
        assert_eq!(counters, expected);
    }
}
