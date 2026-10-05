use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn counted(
    attempts: &Arc<AtomicUsize>,
    outcome: impl Fn(usize) -> Result<(), tonic::Status>,
) -> impl FnMut(Duration) -> std::future::Ready<Result<(), tonic::Status>> {
    let attempts = Arc::clone(attempts);
    move |_| std::future::ready(outcome(attempts.fetch_add(1, Ordering::SeqCst)))
}

fn busy() -> tonic::Status {
    tonic::Status::resource_exhausted("guest filesystem flush already in progress")
}

// A flush already running may predate the latest writes, so the stop waits for
// one of its own.
#[tokio::test(start_paused = true)]
async fn a_busy_guest_is_retried_until_its_own_flush_runs() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let flushed = flush_until_own_attempt_runs(
        Duration::from_secs(30),
        counted(&attempts, |n| if n < 2 { Err(busy()) } else { Ok(()) }),
    )
    .await;
    flushed.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

// Retries share the budget: a guest that stays busy cannot hold a stop longer.
#[tokio::test(start_paused = true)]
async fn retries_share_one_budget() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = tokio::time::Instant::now();
    let flushed =
        flush_until_own_attempt_runs(Duration::from_secs(5), counted(&attempts, |_| Err(busy())))
            .await;
    assert!(matches!(flushed, Err(GuestFlushError::TimedOut { .. })));
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert!(attempts.load(Ordering::SeqCst) > 1);
}

// A guest that cannot flush at all is not retried; the stop goes ahead.
#[tokio::test(start_paused = true)]
async fn a_failed_flush_is_not_retried() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let flushed = flush_until_own_attempt_runs(
        Duration::from_secs(30),
        counted(&attempts, |_| {
            Err(tonic::Status::unavailable("guest unreachable"))
        }),
    )
    .await;
    assert!(
        matches!(&flushed, Err(GuestFlushError::Guest(status)) if status.message() == "guest unreachable")
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

// A flush that never answers ends with the budget.
#[tokio::test(start_paused = true)]
async fn a_hung_flush_ends_with_the_budget() {
    let started = tokio::time::Instant::now();
    let flushed = flush_until_own_attempt_runs(Duration::from_secs(5), |_| {
        std::future::pending::<Result<(), tonic::Status>>()
    })
    .await;
    assert!(matches!(flushed, Err(GuestFlushError::TimedOut { .. })));
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}
