use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn key(ip: [u8; 4], pod_uid: &str, generation: i64) -> PodWorkerKey {
    PodWorkerKey {
        addr: SocketAddr::from((ip, 9800)),
        pod_uid: Some(PodUid::parse(pod_uid).unwrap()),
        generation,
    }
}

/// A channel that never connects: these tests exercise only which channel
/// a caller gets, not RPCs on it.
fn channel() -> Channel {
    tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
}

/// Run `client` with a dial that counts itself and hands out `watch`.
async fn client_counting(
    channels: &PodWorkerChannels,
    key: PodWorkerKey,
    dials: &AtomicUsize,
    watch: &ConnectionWatch,
) -> RuntimeClient {
    channels
        .client(key, async {
            dials.fetch_add(1, Ordering::SeqCst);
            Ok((channel(), watch.clone()))
        })
        .await
        .expect("dial succeeds")
}

#[tokio::test]
async fn calls_to_one_pod_share_a_channel_while_its_connection_is_open() {
    let channels = PodWorkerChannels::default();
    let dials = AtomicUsize::new(0);
    let watch = ConnectionWatch::default();
    for _ in 0..5 {
        client_counting(&channels, key([10, 0, 0, 1], "uid-a", 1), &dials, &watch).await;
    }
    assert_eq!(
        dials.load(Ordering::SeqCst),
        1,
        "one dial serves every call"
    );

    // The connection ended: the next caller dials, and that dial is kept.
    watch.close();
    let next = ConnectionWatch::default();
    client_counting(&channels, key([10, 0, 0, 1], "uid-a", 1), &dials, &next).await;
    client_counting(&channels, key([10, 0, 0, 1], "uid-a", 1), &dials, &next).await;
    assert_eq!(dials.load(Ordering::SeqCst), 2);
    assert_eq!(channels.len(), 1);
}

#[tokio::test]
async fn a_replaced_pod_never_gets_the_old_pods_channel() {
    let channels = PodWorkerChannels::default();
    let dials = AtomicUsize::new(0);
    let watch = ConnectionWatch::default();
    client_counting(&channels, key([10, 0, 0, 1], "uid-a", 1), &dials, &watch).await;
    // Same IP, new pod UID; same pod, new execution generation; new IP.
    client_counting(&channels, key([10, 0, 0, 1], "uid-b", 1), &dials, &watch).await;
    client_counting(&channels, key([10, 0, 0, 1], "uid-a", 2), &dials, &watch).await;
    client_counting(&channels, key([10, 0, 0, 2], "uid-a", 1), &dials, &watch).await;
    assert_eq!(dials.load(Ordering::SeqCst), 4, "each pod instance dials");
}

#[tokio::test]
async fn a_failed_dial_reaches_the_caller_and_is_not_kept() {
    let channels = PodWorkerChannels::default();
    let error = channels
        .client(key([10, 0, 0, 1], "uid-a", 1), async {
            Err(ControllerError::service_unavailable(
                "node-api not reachable",
            ))
        })
        .await
        .expect_err("the dial failed");
    assert_eq!(error.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(channels.len(), 0);

    let dials = AtomicUsize::new(0);
    client_counting(
        &channels,
        key([10, 0, 0, 1], "uid-a", 1),
        &dials,
        &ConnectionWatch::default(),
    )
    .await;
    assert_eq!(
        dials.load(Ordering::SeqCst),
        1,
        "the next caller dials afresh"
    );
}

#[tokio::test]
async fn retiring_a_client_makes_the_next_caller_dial() {
    let channels = PodWorkerChannels::default();
    let dials = AtomicUsize::new(0);
    let watch = ConnectionWatch::default();
    let first = client_counting(&channels, key([10, 0, 0, 1], "uid-a", 1), &dials, &watch).await;
    first.retire_kept_connection();
    client_counting(
        &channels,
        key([10, 0, 0, 1], "uid-a", 1),
        &dials,
        &ConnectionWatch::default(),
    )
    .await;
    assert_eq!(dials.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_entry_is_kept_until_it_is_idle_or_old() {
    let channels = PodWorkerChannels::default();
    let start = Instant::now();
    let pod = key([10, 0, 0, 1], "uid-a", 1);
    channels.keep(pod.clone(), channel(), ConnectionWatch::default(), start);
    // Use keeps an entry alive past the idle window, but not past its age.
    let mut now = start;
    while now < start + MAX_AGE - IDLE_TTL / 2 {
        now += IDLE_TTL / 2;
        assert!(channels.live(&pod, now).is_some());
    }
    assert!(channels.live(&pod, start + MAX_AGE).is_none(), "too old");
    assert_eq!(channels.len(), 0, "an expired entry is dropped on lookup");

    channels.keep(pod.clone(), channel(), ConnectionWatch::default(), start);
    assert!(channels.live(&pod, start + IDLE_TTL).is_none(), "idle");
}

#[tokio::test]
async fn a_new_entry_sweeps_retired_ones_and_the_map_is_bounded() {
    let channels = PodWorkerChannels::default();
    let start = Instant::now();
    let closed = ConnectionWatch::default();
    channels.keep(
        key([10, 0, 0, 1], "uid-a", 1),
        channel(),
        closed.clone(),
        start,
    );
    closed.close();
    channels.keep(
        key([10, 0, 0, 2], "uid-b", 1),
        channel(),
        ConnectionWatch::default(),
        start,
    );
    assert_eq!(channels.len(), 1, "the closed entry is swept");

    for index in 1..MAX_ENTRIES {
        let ip = u32::try_from(index).unwrap().to_be_bytes();
        channels.keep(
            key([11, ip[1], ip[2], ip[3]], "uid", 1),
            channel(),
            ConnectionWatch::default(),
            start,
        );
    }
    assert_eq!(channels.len(), MAX_ENTRIES);
    // Past the bound a caller still gets a client, just not a kept one.
    let overflow = key([12, 0, 0, 1], "uid", 1);
    channels.keep(
        overflow.clone(),
        channel(),
        ConnectionWatch::default(),
        start,
    );
    assert_eq!(channels.len(), MAX_ENTRIES);
    assert!(channels.live(&overflow, start).is_none());
}
