use pplx_space_metrics::test_capture::{
    CapturedCounter, CapturedValue, capture_counters_guard, capture_values_guard,
};

use super::*;

const LONG: Duration = Duration::from_secs(60);
const DOWN: TlsHandshakeLeg = TlsHandshakeLeg::ClientToNetgw;
const UP: TlsHandshakeLeg = TlsHandshakeLeg::NetgwToOrigin;

fn far_deadline() -> Instant {
    Instant::now() + LONG
}

fn has_tag(tags: &[String], key: &str, value: &str) -> bool {
    tags.iter().any(|tag| *tag == format!("{key}:{value}"))
}

/// The `result` tag of every admission decision on `leg`, in order.
fn admission_results(counters: &[CapturedCounter], leg: TlsHandshakeLeg) -> Vec<String> {
    counters
        .iter()
        .filter(|counter| counter.name == "gateway.tls.handshake.admission")
        .filter(|counter| has_tag(&counter.tags, "leg", leg.as_str()))
        .filter_map(|counter| {
            counter
                .tags
                .iter()
                .find_map(|tag| tag.strip_prefix("result:"))
                .map(str::to_owned)
        })
        .collect()
}

/// Every value of `name` reported for `leg`, in order.
fn samples(values: &[CapturedValue], name: &str, leg: &str) -> Vec<f64> {
    values
        .iter()
        .filter(|value| value.name == name && has_tag(&value.tags, "leg", leg))
        .map(|value| value.value)
        .collect()
}

fn queued(values: &[CapturedValue], leg: TlsHandshakeLeg) -> Vec<f64> {
    samples(values, "gateway.tls.handshake.queued", leg.as_str())
}

fn in_flight(values: &[CapturedValue], leg: &str) -> Vec<f64> {
    samples(values, "gateway.tls.handshake.in_flight", leg)
}

/// Start a task that waits for a permit, and let it join the queue.
async fn spawn_waiter(
    admission: &HandshakeAdmission,
    leg: TlsHandshakeLeg,
) -> tokio::task::JoinHandle<Result<HandshakePermit, Refused>> {
    let admission = admission.clone();
    let waiter = tokio::spawn(async move { admission.admit(leg, far_deadline()).await });
    // The paused clock advances only after the waiter has parked in the queue.
    tokio::time::sleep(Duration::from_millis(1)).await;
    waiter
}

#[tokio::test(start_paused = true)]
async fn free_capacity_admits_immediately() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let counters = capture_counters_guard();
    let values = capture_values_guard();

    let permit = admission.admit(DOWN, far_deadline()).await.unwrap();
    assert_eq!(permit.admission(), Admission::Immediate);
    drop(permit);

    assert_eq!(admission_results(&counters.finish(), DOWN), ["immediate"]);
    let values = values.finish();
    assert_eq!(in_flight(&values, DOWN.as_str()), [1.0, 0.0]);
    assert_eq!(in_flight(&values, "shared"), [1.0, 0.0]);
    assert!(queued(&values, DOWN).is_empty(), "no wait, no queue sample");
}

#[tokio::test(start_paused = true)]
async fn legs_share_one_pool_and_drop_restores_capacity() {
    let admission = HandshakeAdmission::new(2, Duration::ZERO);
    let downstream = admission.admit(DOWN, far_deadline()).await.unwrap();
    let _upstream = admission.admit(UP, far_deadline()).await.unwrap();

    assert!(
        matches!(
            admission.admit(DOWN, far_deadline()).await,
            Err(Refused::Saturated { .. })
        ),
        "both legs must consume the same pool"
    );
    drop(downstream);
    assert_eq!(
        admission
            .admit(UP, far_deadline())
            .await
            .unwrap()
            .admission(),
        Admission::Immediate,
        "a completed handshake should immediately restore capacity"
    );
}

#[tokio::test(start_paused = true)]
async fn full_pool_queues_until_a_permit_is_released() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission.admit(DOWN, far_deadline()).await.unwrap();
    let counters = capture_counters_guard();
    let values = capture_values_guard();

    let waiter = spawn_waiter(&admission, DOWN).await;
    tokio::time::sleep(Duration::from_millis(749)).await;
    drop(held);
    let permit = waiter.await.unwrap().unwrap();

    assert_eq!(
        permit.admission(),
        Admission::Queued {
            waited: Duration::from_millis(750)
        }
    );
    assert_eq!(admission_results(&counters.finish(), DOWN), ["queued"]);
    let values = values.finish();
    assert_eq!(queued(&values, DOWN), [1.0, 0.0]);
    assert_eq!(
        samples(
            &values,
            "gateway.tls.handshake.admission.wait_ms",
            DOWN.as_str()
        ),
        [750.0]
    );
}

#[tokio::test(start_paused = true)]
async fn queue_wait_stops_at_the_queue_limit() {
    let admission = HandshakeAdmission::new(1, Duration::from_secs(2));
    let _held = admission.admit(UP, far_deadline()).await.unwrap();
    let counters = capture_counters_guard();
    let values = capture_values_guard();

    let refused = admission.admit(UP, far_deadline()).await.unwrap_err();

    assert_eq!(
        refused,
        Refused::Saturated {
            waited: Duration::from_secs(2)
        }
    );
    assert_eq!(admission_results(&counters.finish(), UP), ["saturated"]);
    assert_eq!(queued(&values.finish(), UP), [1.0, 0.0]);
}

#[tokio::test(start_paused = true)]
async fn queue_wait_stops_at_an_earlier_caller_deadline() {
    let admission = HandshakeAdmission::new(1, Duration::from_secs(5));
    let _held = admission.admit(DOWN, far_deadline()).await.unwrap();

    let refused = admission
        .admit(DOWN, Instant::now() + Duration::from_millis(300))
        .await
        .unwrap_err();

    assert_eq!(
        refused,
        Refused::Saturated {
            waited: Duration::from_millis(300)
        }
    );
}

#[tokio::test(start_paused = true)]
async fn waiters_are_admitted_in_arrival_order() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission.admit(DOWN, far_deadline()).await.unwrap();
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut waiters = Vec::new();
    for (index, leg) in [DOWN, UP, DOWN].into_iter().enumerate() {
        let admission = admission.clone();
        let order_tx = order_tx.clone();
        waiters.push(tokio::spawn(async move {
            let permit = admission.admit(leg, far_deadline()).await.unwrap();
            order_tx.send(index).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(permit);
        }));
        // Let this waiter join the queue before the next one starts.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(order_tx);
    drop(held);
    for waiter in waiters {
        waiter.await.unwrap();
    }

    let mut order = Vec::new();
    while let Some(index) = order_rx.recv().await {
        order.push(index);
    }
    assert_eq!(order, [0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_waiter_leaves_the_queue() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission.admit(DOWN, far_deadline()).await.unwrap();
    let values = capture_values_guard();

    let waiter = spawn_waiter(&admission, DOWN).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(queued(&values.finish(), DOWN), [1.0, 0.0]);

    drop(held);
    assert_eq!(
        admission
            .admit(DOWN, far_deadline())
            .await
            .unwrap()
            .admission(),
        Admission::Immediate,
        "a cancelled waiter must not keep a permit"
    );
}

#[tokio::test(start_paused = true)]
async fn closing_wakes_waiters_and_refuses_new_handshakes() {
    let admission = HandshakeAdmission::new(1, MAX_QUEUE_WAIT);
    let held = admission.admit(DOWN, far_deadline()).await.unwrap();
    let waiter = spawn_waiter(&admission, UP).await;
    let counters = capture_counters_guard();
    let started = Instant::now();

    admission.close();

    assert_eq!(waiter.await.unwrap().unwrap_err(), Refused::Closed);
    assert_eq!(
        admission.admit(DOWN, far_deadline()).await.unwrap_err(),
        Refused::Closed
    );
    assert_eq!(started.elapsed(), Duration::ZERO, "closing must not wait");
    let counters = counters.finish();
    assert_eq!(admission_results(&counters, UP), ["closed"]);
    assert_eq!(admission_results(&counters, DOWN), ["closed"]);
    drop(held);
}
