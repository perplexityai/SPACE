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
        http2_upload_max_duration: None,
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

/// A rate refusal's `Retry-After`: the `secs` its debt takes to clear, plus
/// the usual jitter.
fn assert_retry_after_past(error: &EcwProxyError, secs: u64) {
    let retry_after = error.retry_after.expect("429 carries Retry-After");
    let clears = Duration::from_secs(secs);
    assert!(
        (clears + RETRY_AFTER_MIN..=clears + RETRY_AFTER_MAX).contains(&retry_after),
        "Retry-After {retry_after:?} not {secs} s plus jitter"
    );
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
    let (queued, ()) = tokio::join!(uploads.admit(busy, 1), async {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        // The upload queued on its sandbox holds no pod slot while it waits.
        assert_eq!(uploads.available_uploads(), 62);
    });
    let error = queued.unwrap_err();
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);

    // Another sandbox in the same org still gets in at once.
    let _neighbour = uploads.admit(sandbox_of(busy.org_id), 1).await.unwrap();
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

    let (queued, ()) = tokio::join!(uploads.admit(sandbox_of(org_id), 1), async {
        tokio::time::sleep(DEFAULT_UPLOAD_ADMISSION_WAIT / 2).await;
        // The upload queued on its org holds no pod slot while it waits.
        assert_eq!(uploads.available_uploads(), 61);
    });
    assert_eq!(queued.unwrap_err().status, StatusCode::TOO_MANY_REQUESTS);
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
async fn govern_upload(uploads: &UploadAdmission, org_id: OrgId, body: Body) -> (Body, BodyEnd) {
    let admitted = uploads
        .admit_upload(sandbox_of(org_id), 0, body, axum::http::Version::HTTP_11)
        .await
        .unwrap();
    (admitted.body, admitted.ended)
}

/// Admits a download for `org_id` and returns its governed body.
fn govern_download(uploads: &UploadAdmission, org_id: OrgId, body: Body) -> Body {
    uploads.admit_download(org_id).unwrap().govern(body)
}

/// When each frame of `body` arrives, measured from the first poll.
async fn frame_times(mut body: Body) -> Vec<Duration> {
    let started = Instant::now();
    let mut times = Vec::new();
    // Bounded, so a body that's never woken fails the test instead of hanging
    // it. Time is paused, so the hour passes at once.
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(3600), body.frame())
        .await
        .expect("the body stopped producing frames")
    {
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

/// A download is charged to its org's bucket without being slowed; the org's
/// next download is refused while it owes, and its HTTP/1.x upload sleeps
/// the download's debt off before its own.
#[tokio::test(start_paused = true)]
async fn one_orgs_uploads_and_downloads_draw_on_one_bucket() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    let download = govern_download(&uploads, org_id, chunks(3, 1000));
    assert_eq!(frame_times(download).await, [Duration::ZERO; 3]);
    let Err(error) = uploads.admit_download(org_id) else {
        panic!("a download was admitted while its org owed the bucket");
    };
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_past(&error, 2);

    let (upload, _) = govern_upload(&uploads, org_id, chunks(3, 1000)).await;
    let (other_org, _) = govern_upload(&uploads, OrgId::new(), chunks(3, 1000)).await;
    let (upload, other_org) = tokio::join!(frame_times(upload), frame_times(other_org));
    // Six KB through one 1 KB/s bucket that starts with 1 KB.
    let secs = |s: u64| Duration::from_secs(s);
    assert_eq!(upload, [secs(3), secs(4), secs(5)]);
    // The other org's bucket is its own.
    assert_eq!(other_org, [secs(0), secs(1), secs(2)]);
}

/// `rates.for_org(org_id)`, after waiting out any sweep the lookup starts, so
/// the test sees the map as the sweep leaves it.
async fn look_up(
    rates: &Arc<byte_rate::OrgByteRates>,
    org_id: OrgId,
) -> Arc<byte_rate::ByteRateBucket> {
    if let Some(sweep) = rates.start_sweep_if_due(org_id) {
        sweep.await.unwrap();
    }
    rates.for_org(org_id)
}

#[tokio::test(start_paused = true)]
async fn idle_full_buckets_are_swept() {
    let rates = Arc::new(byte_rate::OrgByteRates::new(rate(1000, 1000)));
    let idle = OrgId::new();
    let in_debt = OrgId::new();
    let in_use = OrgId::new();
    look_up(&rates, idle).await;
    look_up(&rates, in_debt)
        .await
        .debit(1_000_000, Instant::now());
    let _held = look_up(&rates, in_use).await;

    // Not yet due: nothing goes, though the idle bucket is already full.
    tokio::time::advance(byte_rate::SWEEP_INTERVAL / 2).await;
    look_up(&rates, OrgId::new()).await;
    assert!(rates.tracks(idle));

    // Due exactly one interval after the last sweep.
    tokio::time::advance(byte_rate::SWEEP_INTERVAL / 2).await;
    look_up(&rates, OrgId::new()).await;
    // The idle bucket refilled and went; the indebted one and the referenced
    // one, though full, stay.
    assert!(!rates.tracks(idle));
    assert!(rates.tracks(in_debt));
    assert!(rates.tracks(in_use));
}

/// A body fed by hand: frames go in through the sender as the test decides.
fn manual_body() -> (tokio::sync::mpsc::Sender<Bytes>, Body) {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Bytes>(16);
    let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
        .map(Ok::<_, std::convert::Infallible>);
    (sender, Body::from_stream(stream))
}

/// The next frame of `body`, failing the test rather than hanging if the
/// body never produces one. Time is paused, so the hour passes at once.
async fn next_frame(body: &mut Body) -> Result<http_body::Frame<Bytes>, axum::Error> {
    tokio::time::timeout(Duration::from_secs(3600), body.frame())
        .await
        .expect("the body produced no frame within an hour")
        .expect("the body ended")
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
    next_frame(&mut body).await.unwrap_err();
    assert_eq!(started.elapsed(), window);
    assert!(stalled.is_stalled());

    // Data that arrives after the stall doesn't revive the body.
    sender.send(Bytes::from(vec![0; 1 << 20])).await.unwrap();
    next_frame(&mut body).await.unwrap_err();
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
    // Bounded: a client that keeps trickling would otherwise never end it.
    tokio::time::timeout(10 * window, async {
        while next_frame(&mut body).await.is_ok() {
            assert!(!stalled.is_stalled());
        }
    })
    .await
    .expect("the trickling upload was never ended");
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
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;
    let started = Instant::now();
    for _ in 0..3 {
        // The body finds no data, so the floor starts timing the client, who
        // sends at once; then the frame sleeps about 10 s in the bucket.
        let (frame, ()) = tokio::join!(next_frame(&mut body), async {
            tokio::task::yield_now().await;
            sender.send(Bytes::from(vec![0; 100])).await.unwrap();
        });
        frame.unwrap();
    }
    assert!(started.elapsed() >= Duration::from_secs(20));
    assert!(!stalled.is_stalled());

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

    // A frame held by the throttle still counts toward the hint, though the
    // inner body has handed it on.
    let (mut body, _) = govern_upload(&uploads, OrgId::new(), chunks(2, 1000)).await;
    next_frame(&mut body).await.unwrap();
    assert!(futures_util::poll!(body.frame()).is_pending());
    assert_eq!(http_body::Body::size_hint(&body).lower(), 1000);
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
    next_frame(&mut body).await.unwrap_err();
    assert!(stalled.is_stalled());
}

/// An inner body that finds nothing on its first poll and has data on the
/// next without ever waking the task, as when its data lands just before
/// the wake for it runs.
struct ReadyWithoutWaking {
    polled: bool,
}

impl http_body::Body for ReadyWithoutWaking {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if std::mem::replace(&mut self.polled, true) {
            std::task::Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from(vec![
                0;
                1 << 20
            ])))))
        } else {
            std::task::Poll::Pending
        }
    }
}

/// Data found on a poll the floor's timer caused, with no wake of its own
/// recorded yet, arrived before that timer fired, so the window it was sent
/// in still counts it.
#[tokio::test(start_paused = true)]
async fn data_found_without_its_wake_counts_toward_the_window_it_beat() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (mut body, stalled) = govern_upload(
        &uploads,
        OrgId::new(),
        Body::new(ReadyWithoutWaking { polled: false }),
    )
    .await;

    assert!(futures_util::poll!(body.frame()).is_pending());
    // The floor's timer fires and the reader polls again: the data is there.
    tokio::time::advance(Duration::from_millis(1500)).await;
    body.frame().await.unwrap().unwrap();
    assert!(!stalled.is_stalled());
}

/// Data whose wake lands exactly as a window ends belongs to the next one.
#[tokio::test(start_paused = true)]
async fn data_arriving_exactly_at_a_window_end_counts_toward_the_next() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;

    assert!(futures_util::poll!(body.frame()).is_pending());
    tokio::time::advance(window).await;
    sender.send(Bytes::from(vec![0; 1 << 20])).await.unwrap();
    next_frame(&mut body).await.unwrap_err();
    assert!(stalled.is_stalled());
}

/// One late arrival can close several windows: those the client filled pass,
/// and the first it left empty fails.
#[tokio::test(start_paused = true)]
async fn a_late_arrival_judges_each_window_it_passes() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;

    // Half a second in, 200 bytes: the first window is satisfied.
    assert!(futures_util::poll!(body.frame()).is_pending());
    tokio::time::advance(window / 2).await;
    sender.send(Bytes::from(vec![0; 200])).await.unwrap();
    body.frame().await.unwrap().unwrap();
    // Waiting again, the reader goes away; more data lands at 2.6 s.
    assert!(futures_util::poll!(body.frame()).is_pending());
    tokio::time::advance(Duration::from_millis(2100)).await;
    sender.send(Bytes::from(vec![0; 1 << 20])).await.unwrap();
    tokio::time::advance(Duration::from_millis(400)).await;
    next_frame(&mut body).await.unwrap_err();
    assert!(stalled.is_stalled());
}

/// Exactly the minimum in a window is enough.
#[tokio::test(start_paused = true)]
async fn exactly_the_minimum_in_a_window_is_enough() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;
    sender.send(Bytes::from(vec![0; 100])).await.unwrap();
    next_frame(&mut body).await.unwrap();
    // The rest of the window passes with nothing more sent.
    assert!(futures_util::poll!(body.frame()).is_pending());
    tokio::time::advance(window).await;
    sender.send(Bytes::from(vec![0; 100])).await.unwrap();
    next_frame(&mut body).await.unwrap();
    assert!(!stalled.is_stalled());
}

/// A governed body ends when its inner body does, but not while it still
/// holds a throttled frame, and never once it has stalled.
#[tokio::test(start_paused = true)]
async fn a_governed_body_ends_only_when_nothing_is_left_to_hand_on() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let finished = govern_download(&uploads, OrgId::new(), Body::empty());
    assert!(http_body::Body::is_end_stream(&finished));

    // A single 2 KB frame: read off the inner body, then held for its debt.
    let (mut throttled, _) =
        govern_upload(&uploads, OrgId::new(), Body::from(vec![0u8; 2000])).await;
    assert!(futures_util::poll!(throttled.frame()).is_pending());
    assert!(!http_body::Body::is_end_stream(&throttled));
    next_frame(&mut throttled).await.unwrap();
    assert!(http_body::Body::is_end_stream(&throttled));

    let uploads = governed(None, Some(floor(100, Duration::from_secs(1))));
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let (mut stalled_body, _) =
        govern_upload(&uploads, OrgId::new(), Body::new(EndingBody(receiver))).await;
    next_frame(&mut stalled_body).await.unwrap_err();
    drop(sender);
    // The inner body reports its end now, but the stall still stands.
    assert!(!http_body::Body::is_end_stream(&stalled_body));
}

/// A body of the frames sent on a channel that declared `remaining` bytes,
/// and reports how many are left, as hyper's HTTP/2 bodies do, but only
/// reports its end once the channel closes: an END_STREAM after the data.
struct DeclaredBody {
    frames: tokio::sync::mpsc::Receiver<Bytes>,
    remaining: u64,
}

impl http_body::Body for DeclaredBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.frames.poll_recv(cx).map(|data| {
            data.map(|data| {
                self.remaining = self.remaining.saturating_sub(data.len() as u64);
                Ok(http_body::Frame::data(data))
            })
        })
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// A body of the frames sent on a channel, which reports its end once the
/// channel is closed and drained, as hyper's own bodies do.
struct EndingBody(tokio::sync::mpsc::Receiver<Bytes>);

impl http_body::Body for EndingBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|data| data.map(|data| Ok(http_body::Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_closed() && self.0.is_empty()
    }
}

/// An HTTP/1.x upload may arrive into as much of its org's debt as the
/// admission wait covers, and is refused before it takes a permit when the
/// org owes more, so it doesn't start by holding its permits through a long
/// sleep. The refusal's `Retry-After` is no sooner than the debt it couldn't
/// wait off clears.
#[tokio::test(start_paused = true)]
async fn an_http1_upload_into_more_debt_than_the_wait_covers_is_refused() {
    use axum::http::Version;
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    // 5 KB against a 1 KB burst: 4 s of debt, past the 2 s admission wait.
    let download = govern_download(&uploads, org_id, chunks(1, 5000));
    frame_times(download).await;

    let started = Instant::now();
    let Err(error) = uploads
        .admit_upload(sandbox_of(org_id), 1, chunks(1, 1), Version::HTTP_11)
        .await
    else {
        panic!("an upload was admitted into more debt than the wait covers");
    };
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(uploads.available_uploads(), 64);
    // 2 s past the wait: retry no sooner than that.
    assert_retry_after_past(&error, 2);
    // 100 KB more puts it 102 s past: the jitter alone would be far too soon.
    let bucket = uploads.org_bucket(org_id).unwrap();
    bucket.charge(100_000, Instant::now());
    let Err(error) = uploads
        .admit_upload(sandbox_of(org_id), 1, chunks(1, 1), Version::HTTP_11)
        .await
    else {
        panic!("an upload was admitted into more debt than the wait covers");
    };
    assert_retry_after_past(&error, 102);
    drop(bucket);
    // Paying off the 100 KB leaves the first 4 s of debt.
    tokio::time::advance(Duration::from_secs(100)).await;

    // With an admission wait longer than the longest throttle sleep, an
    // upload is still admitted only into as much debt as it could sleep off.
    let long_wait = UploadAdmission::new(&UploadAdmissionConfig {
        wait: Duration::from_secs(60),
        org_transfer_rate: Some(rate(1000, 1000)),
        ..pod_config(64, 1 << 30)
    });
    let deep_org = OrgId::new();
    frame_times(govern_download(&long_wait, deep_org, chunks(1, 21_000))).await;
    let Err(error) = long_wait
        .admit_upload(sandbox_of(deep_org), 1, chunks(1, 1), Version::HTTP_11)
        .await
    else {
        panic!("an upload was admitted into more debt than it could sleep off");
    };
    // 20 s of debt, down to the 15 s it could sleep off.
    assert_retry_after_past(&error, 5);

    // Exactly the wait's worth of debt left: admitted, and it sleeps it off.
    tokio::time::advance(Duration::from_secs(2)).await;
    let (upload, _) = govern_upload(&uploads, org_id, chunks(1, 1000)).await;
    assert_eq!(frame_times(upload).await, [Duration::from_secs(3)]);
}

/// A body that ends is complete however slowly it came: the end isn't
/// judged, even when it arrives after a window the client let lapse.
#[tokio::test(start_paused = true)]
async fn a_body_that_ends_after_a_lapsed_window_is_complete() {
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    let (sender, body) = manual_body();
    let (mut body, stalled) = govern_upload(&uploads, OrgId::new(), body).await;
    sender.send(Bytes::from(vec![0; 10])).await.unwrap();
    next_frame(&mut body).await.unwrap();

    // The reader goes away; the window lapses, then the client finishes.
    assert!(futures_util::poll!(body.frame()).is_pending());
    tokio::time::advance(window * 2).await;
    drop(sender);
    let end = tokio::time::timeout(Duration::from_secs(3600), body.frame())
        .await
        .expect("the body didn't end");
    assert!(end.is_none(), "the body ends cleanly");
    assert!(!stalled.is_stalled());
}

/// A frame with no data moves nothing the bucket meters, so it's handed on at
/// once even while the org owes the bucket.
#[tokio::test(start_paused = true)]
async fn a_frame_without_data_never_waits_on_the_orgs_debt() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    // Admitted first, since an upload arriving into that much debt is refused.
    let (empty, _) = govern_upload(&uploads, org_id, chunks(1, 0)).await;
    // Another transfer puts the org ten seconds into debt.
    let (mut indebted, _) = govern_upload(&uploads, org_id, chunks(1, 11_000)).await;
    assert!(futures_util::poll!(indebted.frame()).is_pending());

    let times = frame_times(empty).await;
    assert_eq!(times, [Duration::ZERO]);
}

/// An unreferenced bucket that's still refilling goes once it has sat idle
/// for the expiry, so a slow rate can't keep idle orgs in the map for good.
#[tokio::test(start_paused = true)]
async fn idle_buckets_expire_even_before_they_refill() {
    // One byte a second: a 1 MB debt takes far longer than the expiry to pay.
    let rates = Arc::new(byte_rate::OrgByteRates::new(rate(1, 1000)));
    let slow = OrgId::new();
    look_up(&rates, slow).await.debit(1_000_000, Instant::now());

    tokio::time::advance(byte_rate::BUCKET_IDLE_EXPIRY / 2).await;
    look_up(&rates, OrgId::new()).await;
    assert!(rates.tracks(slow), "still within the expiry");

    tokio::time::advance(byte_rate::BUCKET_IDLE_EXPIRY / 2).await;
    look_up(&rates, OrgId::new()).await;
    assert!(!rates.tracks(slow), "idle past the expiry");
}

/// Past the size bound, unreferenced buckets go fullest first, at once
/// rather than at the next sweep, and buckets transfers hold always stay.
#[tokio::test(start_paused = true)]
async fn the_bucket_map_stays_within_its_bound() {
    let rates = Arc::new(byte_rate::OrgByteRates::with_max_tracked(
        rate(1000, 1000),
        3,
    ));
    let held_org = OrgId::new();
    let held = look_up(&rates, held_org).await;
    held.debit(1_000_000, Instant::now());
    let indebted = OrgId::new();
    look_up(&rates, indebted)
        .await
        .debit(1_000_000, Instant::now());
    // Less in debt than `indebted`, so fuller, though not full by the sweep.
    let half = OrgId::new();
    look_up(&rates, half).await.debit(500_000, Instant::now());
    assert_eq!(rates.tracked(), 3);

    // A fourth org arrives long before a periodic sweep is due.
    tokio::time::advance(byte_rate::BOUND_SWEEP_SPACING).await;
    let newcomer = OrgId::new();
    look_up(&rates, newcomer).await;
    assert_eq!(rates.tracked(), 3);
    assert!(rates.tracks(held_org), "a held bucket stays");
    assert!(
        rates.tracks(indebted),
        "the emptiest unreferenced bucket stays"
    );
    assert!(!rates.tracks(half), "the fullest unreferenced bucket goes");
    assert!(rates.tracks(newcomer));

    // At the bound, looking up an org already tracked evicts nothing.
    look_up(&rates, indebted).await;
    assert_eq!(rates.tracked(), 3);
    assert!(rates.tracks(newcomer));
    drop(held);
}

/// An upload's held frame already crossed the network, so dropping the upload
/// leaves it charged.
#[tokio::test(start_paused = true)]
async fn a_dropped_upload_keeps_its_held_frame_charged() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    // One 6 KB upload frame against a 1 KB burst: the org goes 5 s into debt.
    let (mut abandoned, _) = govern_upload(&uploads, org_id, chunks(1, 6000)).await;
    assert!(futures_util::poll!(abandoned.frame()).is_pending());
    drop(abandoned);

    // 2 s of debt left, which the next upload may still wait off: it goes
    // 3 s later, behind the dropped frame's charge and its own.
    tokio::time::advance(Duration::from_secs(3)).await;
    let (next, _) = govern_upload(&uploads, org_id, chunks(1, 1000)).await;
    assert_eq!(frame_times(next).await, [Duration::from_secs(3)]);
}

/// A periodic sweep at the bound, run for an org already tracked, has nothing
/// to make room for and evicts nothing.
#[tokio::test(start_paused = true)]
async fn a_periodic_sweep_at_the_bound_evicts_only_to_make_room() {
    let rates = Arc::new(byte_rate::OrgByteRates::with_max_tracked(rate(1, 1000), 3));
    let orgs: Vec<OrgId> = (0..3).map(|_| OrgId::new()).collect();
    for org_id in &orgs {
        look_up(&rates, *org_id).await.debit(500, Instant::now());
    }
    tokio::time::advance(byte_rate::SWEEP_INTERVAL).await;
    look_up(&rates, orgs[0]).await;
    assert!(orgs.iter().all(|org_id| rates.tracks(*org_id)));
}

/// Once over its bound, the map evicts down to fifteen sixteenths of it, so a
/// map held at the bound doesn't pay for an eviction on every new org.
#[tokio::test(start_paused = true)]
async fn crossing_the_bound_evicts_down_to_fifteen_sixteenths() {
    let rates = Arc::new(byte_rate::OrgByteRates::with_max_tracked(rate(1, 1000), 32));
    for _ in 0..32 {
        look_up(&rates, OrgId::new())
            .await
            .debit(500, Instant::now());
    }
    assert_eq!(rates.tracked(), 32);
    tokio::time::advance(byte_rate::BOUND_SWEEP_SPACING).await;
    look_up(&rates, OrgId::new()).await;
    // 33 would-be entries, the newcomer among them, down to 30.
    assert_eq!(rates.tracked(), 30);
}

/// A transfer's lookup never waits for the sweep it starts: the map goes past
/// its bound for a moment, and the sweep on the blocking pool trims it back.
/// Real time, since the sweep runs on a real thread.
#[tokio::test]
async fn a_lookup_past_the_bound_sweeps_in_the_background() {
    let rates = Arc::new(
        byte_rate::OrgByteRates::with_max_tracked(rate(1, 1000), 3)
            .with_bound_sweep_spacing(Duration::ZERO),
    );
    for _ in 0..3 {
        look_up(&rates, OrgId::new())
            .await
            .debit(500, Instant::now());
    }
    // Held, as the newcomer's transfer would hold it.
    let newcomer = OrgId::new();
    let _transfer = rates.for_org(newcomer);
    tokio::time::timeout(Duration::from_secs(10), async {
        while rates.tracked() > 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the background sweep never trimmed the map");
    assert!(rates.tracks(newcomer));
}

/// A map held past its bound by buckets transfers hold has nothing to evict,
/// so new orgs start bound sweeps at most once a `BOUND_SWEEP_SPACING`.
#[tokio::test(start_paused = true)]
async fn bound_sweeps_are_spaced_out() {
    let rates = Arc::new(byte_rate::OrgByteRates::with_max_tracked(rate(1, 1000), 2));
    let _held: Vec<_> = (0..2).map(|_| rates.for_org(OrgId::new())).collect();
    tokio::time::advance(byte_rate::BOUND_SWEEP_SPACING).await;
    rates
        .start_sweep_if_due(OrgId::new())
        .unwrap()
        .await
        .unwrap();
    assert!(rates.start_sweep_if_due(OrgId::new()).is_none());
    tokio::time::advance(byte_rate::BOUND_SWEEP_SPACING).await;
    assert!(rates.start_sweep_if_due(OrgId::new()).is_some());
}

/// Only an HTTP/1.x upload is held to the minimum body rate: an HTTP/2 one
/// shares its connection's flow control, so a wait for its body needn't be
/// its client's.
#[tokio::test(start_paused = true)]
async fn only_http1_uploads_are_held_to_the_minimum_body_rate() {
    use axum::http::Version;
    let window = Duration::from_secs(1);
    let uploads = governed(None, Some(floor(100, window)));
    for (version, held) in [
        (Version::HTTP_10, true),
        (Version::HTTP_11, true),
        (Version::HTTP_2, false),
        (Version::HTTP_3, false),
    ] {
        let (sender, body) = manual_body();
        let admitted = uploads
            .admit_upload(tenant(), 0, body, version)
            .await
            .unwrap();
        let mut body = admitted.body;
        // Nothing arrives for ten windows, then one byte.
        assert!(futures_util::poll!(body.frame()).is_pending());
        tokio::time::advance(window * 10).await;
        sender.send(Bytes::from_static(b"x")).await.unwrap();
        let next = next_or_end(&mut body).await.expect("the body ended");
        assert_eq!(next.is_err(), held, "{version:?}");
        assert_eq!(admitted.ended.is_stalled(), held, "{version:?}");
    }
}

/// The next frame, or `None` at the end, failing the test rather than
/// hanging if the body never produces either.
async fn next_or_end(body: &mut Body) -> Option<Result<http_body::Frame<Bytes>, axum::Error>> {
    tokio::time::timeout(Duration::from_secs(3600), body.frame())
        .await
        .expect("the body neither produced a frame nor ended within an hour")
}

/// An HTTP/2 upload takes only the permits free at once: waiting would stop
/// its stream being read, and that holds back its connection's other streams.
#[tokio::test(start_paused = true)]
async fn an_http2_upload_is_admitted_or_rejected_without_waiting() {
    use axum::http::Version;
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        per_sandbox: share(1, 1 << 30),
        ..pod_config(64, 1 << 30)
    });
    let busy = tenant();
    let _held = uploads.admit(busy, 1).await.unwrap();

    let started = Instant::now();
    let Err(error) = uploads
        .admit_upload(busy, 1, Body::empty(), Version::HTTP_2)
        .await
    else {
        panic!("an HTTP/2 upload past its share was admitted");
    };
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_in_range(&error);
    // An HTTP/1.1 upload still waits out the admission wait.
    let Err(_) = uploads
        .admit_upload(busy, 1, Body::empty(), Version::HTTP_11)
        .await
    else {
        panic!("an HTTP/1.1 upload past its share was admitted");
    };
    assert_eq!(started.elapsed(), DEFAULT_UPLOAD_ADMISSION_WAIT);
    // Free permits are taken at once.
    uploads
        .admit_upload(tenant(), 1, Body::empty(), Version::HTTP_2)
        .await
        .unwrap();
}

/// An HTTP/2 upload is charged to its org's bucket without being slowed, and
/// the org's next HTTP/2 upload is rejected until the debt is paid off, while
/// its other transfers sleep the debt off as usual.
#[tokio::test(start_paused = true)]
async fn an_http2_upload_is_charged_to_the_bucket_without_waiting_on_it() {
    use axum::http::Version;
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    let admitted = uploads
        .admit_upload(sandbox_of(org_id), 0, chunks(4, 1000), Version::HTTP_2)
        .await
        .unwrap();
    // 4 KB against a 1 KB burst, all at once.
    assert_eq!(frame_times(admitted.body).await, [Duration::ZERO; 4]);

    let Err(error) = uploads
        .admit_upload(sandbox_of(org_id), 0, chunks(1, 1), Version::HTTP_2)
        .await
    else {
        panic!("an HTTP/2 upload was admitted while its org owed the bucket");
    };
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_past(&error, 3);
    // Another org's bucket is its own.
    uploads
        .admit_upload(tenant(), 0, chunks(1, 1), Version::HTTP_2)
        .await
        .unwrap();
    // A second later, 2 s of debt is left, within what an HTTP/1.1 upload may
    // wait off: it sleeps that off before its own 1 KB.
    tokio::time::advance(Duration::from_secs(1)).await;
    let (upload, _) = govern_upload(&uploads, org_id, chunks(1, 1000)).await;
    assert_eq!(frame_times(upload).await, [Duration::from_secs(3)]);
    uploads
        .admit_upload(sandbox_of(org_id), 0, chunks(1, 1), Version::HTTP_2)
        .await
        .unwrap();
}

/// An admitted HTTP/1.x upload whose org runs up more debt than the longest
/// throttle sleep isn't held through it: the body ends, and the handler
/// answers 429 with a `Retry-After` no sooner than the debt clears.
#[tokio::test(start_paused = true)]
async fn an_upload_facing_more_debt_than_the_longest_sleep_ends_with_429() {
    let uploads = governed(Some(rate(1000, 1000)), None);
    let org_id = OrgId::new();
    let (sender, body) = manual_body();
    let (mut body, ended) = govern_upload(&uploads, org_id, body).await;
    sender.send(Bytes::from(vec![0; 1000])).await.unwrap();
    next_frame(&mut body).await.unwrap();

    // Debt within the longest sleep is slept off.
    let bucket = uploads.org_bucket(org_id).unwrap();
    bucket.charge(
        MAX_UPLOAD_THROTTLE_SLEEP.as_secs() as usize * 1000 - 1000,
        Instant::now(),
    );
    sender.send(Bytes::from(vec![0; 1000])).await.unwrap();
    let started = Instant::now();
    next_frame(&mut body).await.unwrap();
    assert_eq!(started.elapsed(), MAX_UPLOAD_THROTTLE_SLEEP);
    assert_eq!(ended.ended(), None);

    // The org's other transfers put it 100 s into debt.
    bucket.charge(100_000, Instant::now());
    sender.send(Bytes::from(vec![0; 1000])).await.unwrap();
    next_frame(&mut body).await.unwrap_err();
    assert_eq!(
        ended.ended(),
        // 101 s of debt, down to the 2 s a new upload is admitted into.
        Some(body::BodyEnded::OverRate {
            excess: Duration::from_secs(99)
        })
    );
    let error = body_ended_error(&ended).unwrap();
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_retry_after_past(&error, 99);
    // Ended for good, whatever arrives next.
    sender.send(Bytes::from(vec![0; 1])).await.unwrap();
    next_frame(&mut body).await.unwrap_err();
    assert!(!http_body::Body::is_end_stream(&body));

    // A body's last frame meeting that much debt is handed on unslowed:
    // the upload is complete, so ending it would discard it for nothing.
    tokio::time::advance(Duration::from_secs(101)).await;
    let (mut last, ended) = govern_upload(&uploads, org_id, Body::from(vec![0u8; 1000])).await;
    bucket.charge(100_000, Instant::now());
    let started = Instant::now();
    next_frame(&mut last).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(next_or_end(&mut last).await.is_none());
    assert_eq!(ended.ended(), None);
}

/// An HTTP/2 upload, which isn't held to the minimum body rate, is ended with
/// 408 once it runs past its time limit, whether it's waiting on its client
/// or still arriving; one that finishes within the limit, and any HTTP/1.x
/// upload, isn't.
#[tokio::test(start_paused = true)]
async fn an_http2_upload_past_its_time_limit_ends_with_408() {
    use axum::http::Version;
    let limit = Duration::from_secs(10);
    let uploads = UploadAdmission::new(&UploadAdmissionConfig {
        http2_upload_max_duration: Some(limit),
        ..pod_config(64, 1 << 30)
    });
    let admit = |version| {
        let (sender, body) = manual_body();
        let uploads = &uploads;
        async move {
            let admitted = uploads
                .admit_upload(tenant(), 0, body, version)
                .await
                .unwrap();
            (sender, admitted.body, admitted.ended)
        }
    };

    // Waiting on its client when the limit passes.
    let (_sender, mut waiting, ended) = admit(Version::HTTP_2).await;
    let started = Instant::now();
    next_frame(&mut waiting).await.unwrap_err();
    assert_eq!(started.elapsed(), limit);
    assert_eq!(ended.ended(), Some(body::BodyEnded::TimedOut));
    assert_eq!(
        body_ended_error(&ended).unwrap().status,
        StatusCode::REQUEST_TIMEOUT
    );

    // Still arriving: a frame after the limit ends it too.
    let (sender, mut arriving, ended) = admit(Version::HTTP_2).await;
    sender.send(Bytes::from_static(b"x")).await.unwrap();
    next_frame(&mut arriving).await.unwrap();
    tokio::time::advance(limit).await;
    sender.send(Bytes::from_static(b"x")).await.unwrap();
    next_frame(&mut arriving).await.unwrap_err();
    assert_eq!(ended.ended(), Some(body::BodyEnded::TimedOut));

    // A complete body's last frame, though it lands past the limit.
    let admitted = uploads
        .admit_upload(tenant(), 0, Body::from(vec![0u8; 10]), Version::HTTP_2)
        .await
        .unwrap();
    let mut complete = admitted.body;
    tokio::time::advance(limit).await;
    next_frame(&mut complete).await.unwrap();
    assert!(next_or_end(&mut complete).await.is_none());
    assert_eq!(admitted.ended.ended(), None);

    // All its declared bytes in before the limit and its end within the
    // grace after it: complete. Withholding the end past the grace isn't.
    for (end_after, completes) in [(body::END_GRACE / 2, true), (body::END_GRACE * 2, false)] {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let admitted = uploads
            .admit_upload(
                tenant(),
                0,
                Body::new(DeclaredBody {
                    frames: receiver,
                    remaining: 1,
                }),
                Version::HTTP_2,
            )
            .await
            .unwrap();
        let mut declared = admitted.body;
        sender.send(Bytes::from_static(b"x")).await.unwrap();
        next_frame(&mut declared).await.unwrap();
        let ender = tokio::spawn(async move {
            tokio::time::sleep(limit + end_after).await;
            drop(sender);
        });
        let next = next_or_end(&mut declared).await;
        assert_eq!(next.is_none(), completes, "{end_after:?}");
        if completes {
            assert_eq!(admitted.ended.ended(), None);
        } else {
            assert_eq!(admitted.ended.ended(), Some(body::BodyEnded::TimedOut));
        }
        ender.abort();
    }

    // Finished within the limit.
    let (sender, mut finished, ended) = admit(Version::HTTP_2).await;
    sender.send(Bytes::from_static(b"x")).await.unwrap();
    drop(sender);
    next_frame(&mut finished).await.unwrap();
    assert!(next_or_end(&mut finished).await.is_none());
    assert_eq!(ended.ended(), None);

    // HTTP/1.1 has no time limit.
    let (sender, mut http1, ended) = admit(Version::HTTP_11).await;
    tokio::time::advance(limit * 10).await;
    sender.send(Bytes::from_static(b"x")).await.unwrap();
    next_frame(&mut http1).await.unwrap();
    assert_eq!(ended.ended(), None);
}
