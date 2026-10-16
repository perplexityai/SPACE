use std::collections::BTreeSet;

use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::StreamExt as _;
use http_body_util::BodyExt;

use super::*;

const KIB: u64 = 1024;

fn share(max_concurrent: usize, budget: u64) -> UploadShareConfig {
    UploadShareConfig {
        max_concurrent_uploads: NonZeroUsize::new(max_concurrent).unwrap(),
        max_in_flight_bytes: NonZeroU64::new(budget).unwrap(),
    }
}

/// Shares as large as the pod's budget, so only the pod budget binds, and no
/// body governance.
fn pod_config(max_concurrent: usize, budget: u64) -> UploadAdmissionConfig {
    UploadAdmissionConfig {
        max_concurrent_uploads: NonZeroUsize::new(max_concurrent).unwrap(),
        max_in_flight_bytes: NonZeroU64::new(budget).unwrap(),
        wait: DEFAULT_UPLOAD_ADMISSION_WAIT,
        per_org: share(max_concurrent, budget),
        per_sandbox: share(max_concurrent, budget),
        org_transfer_rate: None,
        min_body_rate: None,
    }
}

fn admission(max_concurrent: usize, budget: u64) -> UploadAdmission {
    UploadAdmission::new(&pod_config(max_concurrent, budget))
}

fn tenant() -> UploadTenant {
    sandbox_of(OrgId::new())
}

fn sandbox_of(org_id: OrgId) -> UploadTenant {
    UploadTenant {
        org_id,
        sandbox_id: SandboxId::new(),
    }
}

/// Admits as a tenant of its own, so no share binds.
async fn admit(uploads: &UploadAdmission, reserve: u64) -> Result<UploadPermit, EcwProxyError> {
    uploads.admit(tenant(), reserve).await
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
    let _first = admit(&uploads, 1).await.unwrap();
    let _second = admit(&uploads, 1).await.unwrap();

    let started = Instant::now();
    let error = admit(&uploads, 1).await.unwrap_err();

    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);
}

#[tokio::test(start_paused = true)]
async fn byte_budget_rejects_a_reservation_that_does_not_fit() {
    let uploads = admission(64, 1024 * KIB);
    let _large = admit(&uploads, 768 * KIB).await.unwrap();

    let error = admit(&uploads, 512 * KIB).await.unwrap_err();
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);

    // The failed reservation must not keep its concurrent-upload slot.
    assert_eq!(uploads.available_uploads(), 63);
    let _fits = admit(&uploads, 256 * KIB).await.unwrap();
    assert_eq!(uploads.available_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_reservation_never_exceeds_the_whole_budget() {
    let uploads = admission(64, 8 * KIB);
    let _alone = admit(&uploads, 16 * KIB).await.unwrap();
    assert_eq!(uploads.available_bytes(), 0);
    admit(&uploads, 1).await.unwrap_err();
}

#[tokio::test(start_paused = true)]
async fn partial_kib_reservations_round_up() {
    let uploads = admission(64, 4 * KIB);
    let _one_byte = admit(&uploads, 1).await.unwrap();
    assert_eq!(uploads.available_bytes(), 3 * KIB);
    let _empty = admit(&uploads, 0).await.unwrap();
    assert_eq!(uploads.available_bytes(), 3 * KIB);
}

#[tokio::test(start_paused = true)]
async fn a_waiter_is_admitted_when_a_permit_frees_within_the_wait() {
    let uploads = Arc::new(admission(1, 1 << 30));
    let holder = admit(&uploads, 1).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        drop(holder);
    });

    let started = Instant::now();
    let _admitted = admit(&uploads, 1).await.unwrap();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT / 2);
}

#[tokio::test(start_paused = true)]
async fn dropping_a_permit_returns_its_slot_and_bytes() {
    let uploads = admission(4, 64 * KIB);
    let permit = admit(&uploads, 10 * KIB).await.unwrap();
    assert_eq!(uploads.available_uploads(), 3);
    assert_eq!(uploads.available_bytes(), 54 * KIB);
    drop(permit);
    assert_eq!(uploads.available_uploads(), 4);
    assert_eq!(uploads.available_bytes(), 64 * KIB);
}

#[tokio::test(start_paused = true)]
async fn rejection_is_a_429_with_a_jittered_retry_after_header() {
    let uploads = admission(1, 1 << 30);
    let _holder = admit(&uploads, 1).await.unwrap();

    let mut retry_afters = BTreeSet::new();
    for _ in 0..50 {
        let response = admit(&uploads, 1).await.unwrap_err().into_response();
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
    let _all_bytes = admit(&uploads, 4 * KIB).await.unwrap();
    let slot_only = admit(&uploads, 0).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        drop(slot_only);
    });

    // The slot frees halfway through the wait; the bytes never do. The byte
    // wait gets only what's left of the deadline, not a fresh one.
    let started = Instant::now();
    admit(&uploads, KIB).await.unwrap_err();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
}

#[tokio::test(start_paused = true)]
async fn a_sandbox_gets_its_share_of_slots_and_no_more() {
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_sandbox: share(2, 1 << 30),
        ..pod_config(64, 1 << 30)
    });
    let busy = tenant();
    let _first = uploads.admit(busy, 1).await.unwrap();
    let _second = uploads.admit(busy, 1).await.unwrap();

    let started = Instant::now();
    let error = uploads.admit(busy, 1).await.unwrap_err();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);

    // Another sandbox in the same org still gets in at once.
    let _neighbour = uploads.admit(sandbox_of(busy.org_id), 1).await.unwrap();
    // The queued upload never held a pod slot.
    assert_eq!(uploads.available_uploads(), 61);
}

#[tokio::test(start_paused = true)]
async fn an_org_gets_its_share_of_slots_across_its_sandboxes() {
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_org: share(3, 1 << 30),
        ..pod_config(64, 1 << 30)
    });
    let org_id = OrgId::new();
    let _held: Vec<_> =
        futures_util::future::try_join_all((0..3).map(|_| uploads.admit(sandbox_of(org_id), 1)))
            .await
            .unwrap();

    let error = uploads.admit(sandbox_of(org_id), 1).await.unwrap_err();
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    let _other_org = admit(&uploads, 1).await.unwrap();
    assert_eq!(uploads.available_uploads(), 60);
}

/// A sandbox's few slots can't reserve the pod's whole byte budget.
#[tokio::test(start_paused = true)]
async fn a_sandbox_and_an_org_get_their_share_of_bytes() {
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_org: share(64, 512 * KIB),
        per_sandbox: share(4, 256 * KIB),
        ..pod_config(64, 1024 * KIB)
    });
    let org_id = OrgId::new();
    let busy = sandbox_of(org_id);
    let _sandbox_full = uploads.admit(busy, 256 * KIB).await.unwrap();
    uploads.admit(busy, KIB).await.unwrap_err();

    let _org_full = uploads.admit(sandbox_of(org_id), 256 * KIB).await.unwrap();
    uploads.admit(sandbox_of(org_id), KIB).await.unwrap_err();

    assert_eq!(uploads.available_bytes(), 512 * KIB);
    let _other_org = admit(&uploads, 512 * KIB).await.unwrap();
}

/// A share smaller than one upload admits it alone, but the pod still
/// reserves what the upload declared.
#[tokio::test(start_paused = true)]
async fn each_budget_caps_its_own_reservation() {
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_sandbox: share(4, 8 * KIB),
        ..pod_config(64, 64 * KIB)
    });
    let busy = tenant();
    let _alone = uploads.admit(busy, 16 * KIB).await.unwrap();
    assert_eq!(uploads.available_bytes(), 48 * KIB);
    uploads.admit(busy, 1).await.unwrap_err();
}

#[tokio::test(start_paused = true)]
async fn shares_are_forgotten_once_no_upload_holds_or_waits_on_them() {
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_sandbox: share(1, 1 << 30),
        ..pod_config(64, 1 << 30)
    });
    let busy = tenant();
    let held = uploads.admit(busy, 1).await.unwrap();
    let neighbour = uploads.admit(sandbox_of(busy.org_id), 1).await.unwrap();
    assert_eq!(
        (uploads.tracked_orgs(), uploads.tracked_sandboxes()),
        (1, 2)
    );

    // A waiter that times out must not leave its share behind either.
    uploads.admit(busy, 1).await.unwrap_err();
    drop(held);
    assert_eq!(
        (uploads.tracked_orgs(), uploads.tracked_sandboxes()),
        (1, 1)
    );
    drop(neighbour);
    assert_eq!(
        (uploads.tracked_orgs(), uploads.tracked_sandboxes()),
        (0, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn a_share_waiter_is_admitted_when_its_share_frees() {
    let uploads = Arc::new(UploadAdmission::new(&UploadAdmissionConfig {
        per_sandbox: share(1, 1 << 30),
        ..pod_config(64, 1 << 30)
    }));
    let busy = tenant();
    let holder = uploads.admit(busy, 1).await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        drop(holder);
    });
    let started = Instant::now();
    let _admitted = uploads.admit(busy, 1).await.unwrap();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT / 2);
    assert_eq!(uploads.tracked_sandboxes(), 1);
}

fn rate(bytes_per_second: u64, burst_bytes: u64) -> ByteRateConfig {
    ByteRateConfig {
        bytes_per_second: NonZeroU64::new(bytes_per_second).unwrap(),
        burst_bytes: NonZeroU64::new(burst_bytes).unwrap(),
    }
}

fn governed(rate: Option<ByteRateConfig>, floor: Option<MinBodyRateConfig>) -> UploadAdmission {
    UploadAdmission::new(&UploadAdmissionConfig {
        org_transfer_rate: rate,
        min_body_rate: floor,
        ..pod_config(64, 1 << 30)
    })
}

fn chunks(count: usize, len: usize) -> Body {
    Body::from_stream(futures_util::stream::iter((0..count).map(move |_| {
        Ok::<_, std::convert::Infallible>(Bytes::from(vec![0; len]))
    })))
}

/// Admits an upload for `org_id` and returns its governed body.
async fn govern_upload(
    uploads: &UploadAdmission,
    org_id: OrgId,
    body: Body,
) -> (Body, StallSignal) {
    let admitted = uploads
        .admit_upload(sandbox_of(org_id), 0, body)
        .await
        .unwrap();
    (admitted.body, admitted.stalled)
}

/// When each frame of `body` arrives, measured from the first poll.
async fn frame_times(mut body: Body) -> Vec<Duration> {
    let started = Instant::now();
    let mut times = Vec::new();
    while let Some(frame) = body.frame().await {
        frame.unwrap();
        times.push(started.elapsed());
    }
    times
}

#[tokio::test(start_paused = true)]
async fn the_byte_rate_bucket_spends_its_burst_and_then_paces_the_body() {
    let uploads = governed(Some(rate(1000, 2000)), None);
    let (body, _) = govern_upload(&uploads, OrgId::new(), chunks(4, 1000)).await;
    let times = frame_times(body).await;
    let secs = |s: u64| Duration::from_secs(s);
    // The first two chunks fit the burst; each later one waits off its debt
    // before it's handed on.
    assert_eq!(times, [secs(0), secs(0), secs(1), secs(2)]);
}

#[tokio::test(start_paused = true)]
async fn one_orgs_uploads_and_downloads_share_its_rate() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    let (upload, _) = govern_upload(&uploads, org_id, chunks(3, 1000)).await;
    let (other_org, _) = govern_upload(&uploads, OrgId::new(), chunks(3, 1000)).await;
    let upload = frame_times(upload);
    let download = frame_times(uploads.govern_download(org_id, chunks(3, 1000)));
    let other_org = frame_times(other_org);

    let started = Instant::now();
    let (upload, download, other_org) = tokio::join!(upload, download, other_org);
    // Six KB through one 1 KB/s bucket that starts with 1 KB.
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(upload.len() + download.len(), 6);
    // The other org's bucket is its own.
    assert_eq!(other_org.last(), Some(&Duration::from_secs(2)));
}

#[tokio::test(start_paused = true)]
async fn idle_full_buckets_are_swept() {
    let rates = byte_rate::OrgByteRates::new(rate(1000, 1000));
    let idle = OrgId::new();
    let in_debt = OrgId::new();
    let in_use = OrgId::new();
    rates.for_org(idle);
    rates.for_org(in_debt).debit(1_000_000, Instant::now());
    let _held = rates.for_org(in_use);
    tokio::time::advance(byte_rate::SWEEP_INTERVAL).await;
    rates.for_org(OrgId::new());
    // The idle bucket refilled and went; the indebted and the referenced
    // ones stay, along with the bucket that triggered the sweep.
    assert_eq!(rates.len(), 3);
}

/// A body fed by hand: frames go in through the sender as the test decides.
fn manual_body() -> (tokio::sync::mpsc::Sender<Bytes>, Body) {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Bytes>(16);
    let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
        .map(Ok::<_, std::convert::Infallible>);
    (sender, Body::from_stream(stream))
}

fn floor(bytes_per_second: u64, window: Duration) -> MinBodyRateConfig {
    MinBodyRateConfig {
        bytes_per_second: NonZeroU64::new(bytes_per_second).unwrap(),
        window,
    }
}

#[tokio::test(start_paused = true)]
async fn a_stalled_upload_ends_after_one_window() {
    let window = Duration::from_secs(15);
    let uploads = governed(None, Some(floor(64, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;

    let started = Instant::now();
    body.frame().await.unwrap().unwrap_err();
    assert_eq!(started.elapsed(), window);
    assert!(stalled.is_stalled());

    // Data that arrives after the stall doesn't revive the body.
    sender.send(Bytes::from(vec![0; 1 << 20])).await.unwrap();
    body.frame().await.unwrap().unwrap_err();
}

#[tokio::test(start_paused = true)]
async fn a_trickling_upload_ends_at_the_first_window_it_falls_short() {
    let window = Duration::from_secs(10);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    tokio::spawn(async move {
        // 2 KB in the first window, then 10 bytes a second.
        sender.send(Bytes::from(vec![0; 2000])).await.unwrap();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if sender.send(Bytes::from(vec![0; 10])).await.is_err() {
                return;
            }
        }
    });
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;
    let started = Instant::now();
    while body.frame().await.unwrap().is_ok() {
        assert!(!stalled.is_stalled());
    }
    assert!(stalled.is_stalled());
    // The first window passes on its 2 KB; the second gets only 100 bytes.
    assert_eq!(started.elapsed(), 2 * window);
}

#[tokio::test(start_paused = true)]
async fn an_upload_above_the_minimum_rate_runs_for_many_windows() {
    let window = Duration::from_secs(10);
    // Twice the floor, so no window close lands on a boundary race.
    let uploads = governed(None, Some(floor(50, window)));
    let (sender, body) = manual_body();
    tokio::spawn(async move {
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sender.send(Bytes::from(vec![0; 100])).await.unwrap();
        }
    });
    let (body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;
    assert_eq!(frame_times(body).await.len(), 60);
    assert!(!stalled.is_stalled());
}

/// Neither the pod's own throttle nor time the handler spends away from the
/// body counts against the client.
#[tokio::test(start_paused = true)]
async fn only_time_waiting_on_the_client_counts_toward_the_minimum_rate() {
    let window = Duration::from_secs(1);
    // The bucket allows 10 B/s; the floor asks for 50 B/s of the client.
    let uploads = governed(Some(rate(10, 10)), Some(floor(50, window)));
    let (body, _) = govern_upload(&uploads, OrgId::new(), chunks(3, 100)).await;
    assert_eq!(frame_times(body).await.len(), 3);

    let uploads = governed(None, Some(floor(50, window)));
    let (sender, body) = manual_body();
    let (mut body, _) = govern_upload(&uploads, OrgId::new(), body).await;
    sender.send(Bytes::from(vec![0; 10])).await.unwrap();
    body.frame().await.unwrap().unwrap();
    // The handler is busy for an hour, and the client sent a little more
    // meanwhile: 20 bytes in an hour, but no time spent waiting on it.
    tokio::time::advance(Duration::from_secs(3600)).await;
    sender.send(Bytes::from(vec![0; 10])).await.unwrap();
    body.frame().await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_governed_body_keeps_its_size_hint_and_end() {
    let uploads = governed(
        Some(rate(1000, 1000)),
        Some(floor(1, Duration::from_secs(1))),
    );
    let (body, _) = govern_upload(&uploads, OrgId::new(), Body::from(vec![0u8; 3000])).await;
    assert_eq!(http_body::Body::size_hint(&body).exact(), Some(3000));
    let collected = body.collect().await.unwrap().to_bytes();
    assert_eq!(collected.len(), 3000);
}

/// Uploads that finish at the same moment on different threads must still
/// leave no share behind: each drop sees the others' leases, so the last
/// one out has to be the one that removes the share.
#[test]
fn shares_are_forgotten_when_their_uploads_finish_concurrently() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let uploads = admission(64, 1 << 30);
    let busy = tenant();
    for round in 0..200 {
        let permits: Vec<_> = runtime.block_on(async {
            futures_util::future::try_join_all((0..8).map(|_| uploads.admit(busy, 1)))
                .await
                .unwrap()
        });
        let barrier = Arc::new(std::sync::Barrier::new(permits.len()));
        std::thread::scope(|scope| {
            for permit in permits {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    drop(permit);
                });
            }
        });
        assert_eq!(
            (uploads.tracked_orgs(), uploads.tracked_sandboxes()),
            (0, 0),
            "round {round}"
        );
    }
}

/// A transfer dropped while its frame waits on the throttle never moved that
/// frame on, so the org isn't left paying for it.
#[tokio::test(start_paused = true)]
async fn a_transfer_dropped_mid_throttle_refunds_its_held_frame() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    // One 6 KB frame against a 1 KB burst: the org goes 5 s into debt and the
    // frame is held.
    let mut abandoned = uploads.govern_download(org_id, chunks(1, 6000));
    tokio::time::timeout(Duration::from_millis(1), abandoned.frame())
        .await
        .unwrap_err();
    drop(abandoned);

    let times = frame_times(uploads.govern_download(org_id, chunks(1, 1000))).await;
    assert_eq!(times, [Duration::ZERO]);
}

/// A reader that finds no data, then leaves the body unpolled after the data
/// arrives (as multer does while its consumer writes the spool), isn't
/// charged for the time it spent away: the client's wait ends when the data
/// arrives, not when the reader comes back for it.
#[tokio::test(start_paused = true)]
async fn the_clients_wait_ends_when_its_data_arrives_not_when_it_is_read() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(50, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;

    // Pending: the client hasn't sent anything yet.
    assert!(futures_util::poll!(body.frame()).is_pending());
    sender.send(Bytes::from(vec![0; 10])).await.unwrap();
    // The reader is busy for an hour before it polls again.
    tokio::time::advance(Duration::from_secs(3600)).await;
    body.frame().await.unwrap().unwrap();
    assert!(!stalled.is_stalled());
}

/// Data that arrives after a window has run out can't count toward that
/// window, even when the reader hasn't yet noticed the window end.
#[tokio::test(start_paused = true)]
async fn a_late_frame_cannot_rescue_a_window_that_already_ran_out() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;

    assert!(futures_util::poll!(body.frame()).is_pending());
    // The window ends at 1 s with nothing sent; a big frame lands at 1.5 s,
    // before the reader comes back.
    tokio::time::advance(Duration::from_millis(1500)).await;
    sender.send(Bytes::from(vec![0; 1 << 20])).await.unwrap();
    body.frame().await.unwrap().unwrap_err();
    assert!(stalled.is_stalled());
}
