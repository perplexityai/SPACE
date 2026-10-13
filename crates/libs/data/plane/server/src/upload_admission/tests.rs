use std::collections::BTreeSet;

use axum::http::{StatusCode, header};
use axum::response::IntoResponse;

use super::*;

const KIB: u64 = 1024;

fn admission(max_concurrent: usize, budget: u64) -> UploadAdmission {
    UploadAdmission::new(&UploadAdmissionConfig {
        max_concurrent_uploads: NonZeroUsize::new(max_concurrent).unwrap(),
        max_in_flight_bytes: NonZeroU64::new(budget).unwrap(),
        wait: DEFAULT_UPLOAD_ADMISSION_WAIT,
    })
}

fn assert_retry_after_in_range(error: &EcwProxyError) {
    let retry_after = error.retry_after.expect("429 carries Retry-After");
    assert!(
        (RETRY_AFTER_MIN..=RETRY_AFTER_MAX).contains(&retry_after),
        "Retry-After {retry_after:?} outside [{RETRY_AFTER_MIN:?}, {RETRY_AFTER_MAX:?}]"
    );
}

#[tokio::test(start_paused = true)]
async fn concurrent_upload_cap_rejects_with_429_after_the_wait() {
    let uploads = admission(2, 1 << 30);
    let _first = uploads.admit(1).await.unwrap();
    let _second = uploads.admit(1).await.unwrap();

    let started = Instant::now();
    let error = uploads.admit(1).await.unwrap_err();

    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);
}

#[tokio::test(start_paused = true)]
async fn byte_budget_rejects_a_reservation_that_does_not_fit() {
    let uploads = admission(64, 1024 * KIB);
    let _large = uploads.admit(768 * KIB).await.unwrap();

    let error = uploads.admit(512 * KIB).await.unwrap_err();
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);

    // The failed reservation must not keep its concurrent-upload slot.
    assert_eq!(uploads.available_uploads(), 63);
    let _fits = uploads.admit(256 * KIB).await.unwrap();
    assert_eq!(uploads.available_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_reservation_never_exceeds_the_whole_budget() {
    let uploads = admission(64, 8 * KIB);
    let _alone = uploads.admit(16 * KIB).await.unwrap();
    assert_eq!(uploads.available_bytes(), 0);
    uploads.admit(1).await.unwrap_err();
}

#[tokio::test(start_paused = true)]
async fn partial_kib_reservations_round_up() {
    let uploads = admission(64, 4 * KIB);
    let _one_byte = uploads.admit(1).await.unwrap();
    assert_eq!(uploads.available_bytes(), 3 * KIB);
    let _empty = uploads.admit(0).await.unwrap();
    assert_eq!(uploads.available_bytes(), 3 * KIB);
}

#[tokio::test(start_paused = true)]
async fn a_waiter_is_admitted_when_a_permit_frees_within_the_wait() {
    let uploads = Arc::new(admission(1, 1 << 30));
    let holder = uploads.admit(1).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        drop(holder);
    });

    let started = Instant::now();
    let _admitted = uploads.admit(1).await.unwrap();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT / 2);
}

#[tokio::test(start_paused = true)]
async fn dropping_a_permit_returns_its_slot_and_bytes() {
    let uploads = admission(4, 64 * KIB);
    let permit = uploads.admit(10 * KIB).await.unwrap();
    assert_eq!(uploads.available_uploads(), 3);
    assert_eq!(uploads.available_bytes(), 54 * KIB);
    drop(permit);
    assert_eq!(uploads.available_uploads(), 4);
    assert_eq!(uploads.available_bytes(), 64 * KIB);
}

#[tokio::test(start_paused = true)]
async fn rejection_is_a_429_with_a_jittered_retry_after_header() {
    let uploads = admission(1, 1 << 30);
    let _holder = uploads.admit(1).await.unwrap();

    let mut retry_afters = BTreeSet::new();
    for _ in 0..50 {
        let response = uploads.admit(1).await.unwrap_err().into_response();
        // Never 503: callers retry 503 immediately.
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let seconds: u64 = response.headers()[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!((RETRY_AFTER_MIN.as_secs()..=RETRY_AFTER_MAX.as_secs()).contains(&seconds));
        retry_afters.insert(seconds);
    }
    assert!(
        retry_afters.len() > 1,
        "Retry-After must be jittered, saw only {retry_afters:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn slot_and_byte_waits_share_one_deadline() {
    let uploads = Arc::new(admission(2, 4 * KIB));
    let _all_bytes = uploads.admit(4 * KIB).await.unwrap();
    let slot_only = uploads.admit(0).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        drop(slot_only);
    });

    // The slot frees halfway through the wait; the bytes never do. The byte
    // wait gets only what's left of the deadline, not a fresh one.
    let started = Instant::now();
    uploads.admit(KIB).await.unwrap_err();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
}
