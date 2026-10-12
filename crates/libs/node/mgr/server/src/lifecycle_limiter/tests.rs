use super::*;

#[tokio::test]
async fn shutdown_closes_admission_but_tracks_surviving_permit_clones() {
    let tracker = tokio_util::task::TaskTracker::new();
    let limiter =
        limiter_with(LifecycleMethod::PrepareSandbox, 1, 1).with_task_tracker(tracker.clone());
    let permit = limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .unwrap();
    let blocking_owner = permit.clone();
    let mut waiting = Box::pin(limiter.acquire(LifecycleMethod::PrepareSandbox));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    limiter.close();
    let error = waiting
        .await
        .err()
        .expect("closed limiter rejects queued work");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    tracker.close();
    drop(permit);
    assert!(
        !tracker.is_empty(),
        "blocking clone still owns host mutation"
    );
    drop(blocking_owner);
    tracker.wait().await;
    assert_eq!(limiter.snapshot(LifecycleMethod::PrepareSandbox), (0, 0));
}

#[test]
fn parse_limits_accepts_pair() {
    assert_eq!(
        parse_limits("8:24"),
        Some(LifecycleLimits {
            permits: 8,
            max_queue: 24
        })
    );
}

#[test]
fn parse_limits_rejects_zero_permits_and_garbage() {
    assert!(parse_limits("0:10").is_none());
    assert!(parse_limits("abc:10").is_none());
    assert!(parse_limits("8").is_none());
}

#[test]
fn parse_limits_rejects_permits_above_tokio_cap() {
    // tokio's `Semaphore::new` panics for permits > MAX_PERMITS; a
    // fat-fingered override must fall back to defaults instead.
    let over_cap = format!(
        "{}:0",
        tokio::sync::Semaphore::MAX_PERMITS.saturating_add(1)
    );
    assert!(parse_limits(&over_cap).is_none());
}

/// The compiled-in limits, except `permits` and `max_queue` for `method`.
/// Unlike `from_env`, it ignores the runner's `SPACE_NODEMGR_LIMIT_*`.
pub(crate) fn limiter_with(
    method: LifecycleMethod,
    permits: usize,
    max_queue: usize,
) -> LifecycleLimiter {
    LifecycleLimiter::with_resolver(move |m| {
        if m == method {
            LifecycleLimits { permits, max_queue }
        } else {
            default_limits(m)
        }
    })
}

/// The compiled-in limits for every method, ignoring the environment.
pub(crate) fn default_limiter() -> LifecycleLimiter {
    LifecycleLimiter::with_resolver(default_limits)
}

fn default_limits(method: LifecycleMethod) -> LifecycleLimits {
    let (permits, max_queue) = method.default_limits();
    LifecycleLimits { permits, max_queue }
}

#[tokio::test]
async fn rejects_when_queue_full() {
    // permits=1, max_queue=1: 2 arrivals OK (1 holds permit, 1 waits), 3rd rejected.
    let limiter = Arc::new(limiter_with(LifecycleMethod::PrepareSandbox, 1, 1));

    // First arrival: acquires permit.
    let _p1 = limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .expect("first acquire");

    // Second arrival waits in queue (spawn so it doesn't block the test).
    let limiter_b = limiter.clone();
    let waiter =
        tokio::spawn(async move { limiter_b.acquire(LifecycleMethod::PrepareSandbox).await });

    // Give the waiter a moment to enqueue, then submit a third — should reject.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let third = limiter.acquire(LifecycleMethod::PrepareSandbox).await;
    match third {
        Err(status) if status.code() == tonic::Code::ResourceExhausted => {}
        Err(other) => panic!("expected ResourceExhausted, got {other:?}"),
        Ok(_) => panic!("expected rejection, got Ok"),
    }
    let (in_flight, waiting) = limiter.snapshot(LifecycleMethod::PrepareSandbox);
    assert_eq!(in_flight, 1);
    assert_eq!(waiting, 1);

    // Drop p1 — waiter should now succeed.
    drop(_p1);
    let waiter_result = waiter.await.expect("waiter join");
    waiter_result.expect("waiter acquire after drop");
}

#[test]
fn defaults_table_is_complete() {
    let limiter = LifecycleLimiter::from_env();
    for &m in LifecycleMethod::ALL {
        limiter.slot(m); // panics if missing
    }
}

#[test]
fn load_snapshot_defaults_retain_restore_burst_queue_headroom() {
    assert_eq!(LifecycleMethod::LoadSnapshot.default_limits(), (8, 56));
}

/// Tonic drops the handler future on client
/// cancellation/disconnect. If `acquire` increments `enqueued` and then
/// awaits the semaphore, dropping the future mid-await must not leak the
/// counter. Without the RAII guard, ~`permits + max_queue` cancelled calls
/// would permanently brick the method.
#[tokio::test]
async fn cancelled_acquire_does_not_leak_enqueued() {
    let limiter = Arc::new(limiter_with(LifecycleMethod::PrepareSandbox, 1, 2));

    // Hold the only permit so all later acquires park on the semaphore.
    let held = limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .expect("first acquire");

    // Spawn many waiters and immediately abort them — simulating clients
    // that disconnected or timed out while parked at acquire_owned().await.
    // We stay within max_queue (2) so each spawn passes the threshold check
    // and reaches the await point before being aborted.
    for _ in 0..2 {
        let l = limiter.clone();
        let h = tokio::spawn(async move { l.acquire(LifecycleMethod::PrepareSandbox).await });
        // Yield so the spawned task runs the fetch_add and parks on await.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        h.abort();
        let _ = h.await; // wait for cancellation to fully propagate
    }

    // After cancellation, only the held permit should be counted.
    let (in_flight, waiting) = limiter.snapshot(LifecycleMethod::PrepareSandbox);
    assert_eq!(in_flight, 1, "in_flight leaked: {in_flight}");
    assert_eq!(
        waiting, 0,
        "enqueued leaked across cancellations: {waiting}"
    );

    drop(held);

    // Limiter must still accept new arrivals up to its full capacity.
    let p1 = limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .expect("acquire after cancellations should succeed");
    drop(p1);
}

#[tokio::test]
async fn cloned_permit_keeps_capacity_reserved() {
    let limiter = limiter_with(LifecycleMethod::PrepareSandbox, 1, 0);

    let permit = limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .expect("first acquire");
    let blocking_clone = permit.clone();
    drop(permit);

    let rejected = limiter.acquire(LifecycleMethod::PrepareSandbox).await;
    match rejected {
        Err(status) if status.code() == tonic::Code::ResourceExhausted => {}
        Err(other) => panic!("expected ResourceExhausted, got {other:?}"),
        Ok(_) => panic!("expected cloned permit to keep limiter full"),
    }

    let (in_flight, waiting) = limiter.snapshot(LifecycleMethod::PrepareSandbox);
    assert_eq!(in_flight, 1);
    assert_eq!(waiting, 0);

    drop(blocking_clone);

    limiter
        .acquire(LifecycleMethod::PrepareSandbox)
        .await
        .expect("acquire after final clone drops");
}
