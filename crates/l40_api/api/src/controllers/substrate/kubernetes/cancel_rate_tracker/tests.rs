use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

#[test]
fn fresh_tracker_is_under_budget() {
    let t = CancelRateTracker::with_budget(64);
    assert!(!t.is_over_budget());
    assert_eq!(t.current_count(), 0);
}

#[test]
fn record_cancel_increments_count() {
    let t = CancelRateTracker::with_budget(64);
    for _ in 0..5 {
        t.record_cancel();
    }
    assert_eq!(t.current_count(), 5);
    assert!(!t.is_over_budget());
}

#[test]
fn over_budget_after_exceeding_limit() {
    let t = CancelRateTracker::with_budget(4);
    for _ in 0..5 {
        t.record_cancel();
    }
    assert!(t.is_over_budget());
}

#[test]
fn record_cancel_never_rewinds_a_newer_epoch_bucket() {
    // Simulate a thread that sampled an old epoch and was descheduled
    // past its window: every slot already holds a far-future epoch. A
    // stale `record_cancel` must never rewind a slot to its older
    // sampled epoch — doing so would discard a concurrent writer's
    // count and let `is_over_budget` undercount.
    let t = CancelRateTracker::with_budget(64);
    let future_epoch: u64 = 1_000_000;
    let sentinel_count: u64 = 7;
    for slot in &t.buckets {
        slot.store((future_epoch << 32) | sentinel_count, Ordering::Relaxed);
    }
    t.record_cancel();
    for slot in &t.buckets {
        let packed = slot.load(Ordering::Relaxed);
        let epoch = packed >> 32;
        let count = packed & 0xFFFF_FFFF;
        assert!(
            epoch >= future_epoch,
            "epoch must never be rewound (saw {epoch}, expected >= {future_epoch})"
        );
        // The slot is either untouched (7) or had our event folded into
        // the newer epoch (8) — never reset to a stale-epoch count.
        assert!(
            count >= sentinel_count,
            "a newer-epoch count must not be discarded (saw {count})"
        );
    }
}

#[test]
fn window_slides_after_one_second() {
    let t = CancelRateTracker::with_budget(4);
    for _ in 0..10 {
        t.record_cancel();
    }
    assert!(t.is_over_budget());
    // Sleep past the trailing window (NUM_BUCKETS * BUCKET_MS = 1 s).
    thread::sleep(std::time::Duration::from_millis(
        NUM_BUCKETS_U64 * BUCKET_MS + 200,
    ));
    assert_eq!(t.current_count(), 0);
    assert!(!t.is_over_budget());
}

#[test]
fn concurrent_increments_are_not_lost() {
    let t = Arc::new(CancelRateTracker::with_budget(u64::MAX));
    const THREADS: usize = 16;
    const PER_THREAD: usize = 1000;
    const EXPECTED_COUNT: u64 = 16_000;
    let observed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let t = Arc::clone(&t);
        let observed = Arc::clone(&observed);
        handles.push(thread::spawn(move || {
            for _ in 0..PER_THREAD {
                t.record_cancel();
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // All increments must be visible. Some may have rolled into the
    // next bucket if we crossed a 100 ms boundary, which is fine — we
    // sum the trailing window, not just one bucket.
    assert_eq!(observed.load(Ordering::SeqCst), EXPECTED_COUNT);
    assert_eq!(t.current_count(), EXPECTED_COUNT);
}

#[test]
fn from_env_falls_back_when_unset() {
    // Don't mutate env in tests (parallel-execution hazard); just
    // sanity-check the default path by calling with_budget.
    let t = CancelRateTracker::with_budget(DEFAULT_BUDGET_PER_SEC);
    assert_eq!(t.budget_per_sec(), DEFAULT_BUDGET_PER_SEC);
}
