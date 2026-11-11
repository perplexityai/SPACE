use super::*;

fn queued(slots: usize, waiters: usize) -> QueuedSlots {
    QueuedSlots::new(Arc::new(Semaphore::new(slots))).with_queue(waiters, Duration::from_secs(2))
}

#[tokio::test(start_paused = true)]
async fn a_queued_request_is_admitted_when_a_slot_frees() {
    let slots = queued(1, 1);
    let held = slots.acquire(1).await.expect("free slot");
    let waiting = tokio::spawn({
        let slots = slots.clone();
        async move { slots.acquire(1).await.is_some() }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(held);
    assert!(waiting.await.unwrap(), "the waiter takes the freed slot");
}

#[tokio::test(start_paused = true)]
async fn a_queued_request_gives_up_at_its_deadline() {
    let slots = queued(1, 1);
    let _held = slots.acquire(1).await.expect("free slot");
    let started = tokio::time::Instant::now();
    assert!(slots.acquire(1).await.is_none());
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

#[tokio::test]
async fn a_full_queue_rejects_at_once_and_a_dropped_waiter_frees_its_place() {
    let slots = queued(1, 1);
    let _held = slots.acquire(1).await.expect("free slot");
    let SlotRequest::Queued(first) = slots.request(1) else {
        panic!("the first waiter is queued");
    };
    assert!(matches!(slots.request(1), SlotRequest::Rejected));
    drop(first);
    assert!(matches!(slots.request(1), SlotRequest::Queued(_)));
}

#[tokio::test]
async fn without_a_queue_a_busy_request_is_rejected() {
    let slots = QueuedSlots::new(Arc::new(Semaphore::new(1)));
    let _held = slots.acquire(1).await.expect("free slot");
    assert!(matches!(slots.request(1), SlotRequest::Rejected));
}
