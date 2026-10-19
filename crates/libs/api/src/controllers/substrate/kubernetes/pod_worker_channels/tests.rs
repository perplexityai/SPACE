use std::sync::Arc;
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
    dials: &Arc<AtomicUsize>,
    watch: &ConnectionWatch,
) -> RuntimeClient {
    let (dials, watch) = (dials.clone(), watch.clone());
    channels
        .client(key, async move {
            dials.fetch_add(1, Ordering::SeqCst);
            Ok((channel(), watch))
        })
        .await
        .expect("dial succeeds")
}

#[tokio::test]
async fn calls_to_one_pod_share_a_channel_while_its_connection_is_open() {
    let channels = PodWorkerChannels::default();
    let dials = Arc::new(AtomicUsize::new(0));
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
    let dials = Arc::new(AtomicUsize::new(0));
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

    let dials = Arc::new(AtomicUsize::new(0));
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
    let dials = Arc::new(AtomicUsize::new(0));
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
    // Past the bound a new pod displaces the channel unused longest.
    let used_first = key([10, 0, 0, 2], "uid-b", 1);
    for index in 1..MAX_ENTRIES {
        let ip = u32::try_from(index).unwrap().to_be_bytes();
        assert!(
            channels
                .live(
                    &key([11, ip[1], ip[2], ip[3]], "uid", 1),
                    start + IDLE_TTL / 2
                )
                .is_some()
        );
    }
    let overflow = key([12, 0, 0, 1], "uid", 1);
    channels.keep(
        overflow.clone(),
        channel(),
        ConnectionWatch::default(),
        start + IDLE_TTL / 2,
    );
    assert_eq!(channels.len(), MAX_ENTRIES);
    assert!(channels.live(&overflow, start + IDLE_TTL / 2).is_some());
    assert!(
        channels.live(&used_first, start + IDLE_TTL / 2).is_none(),
        "the least recently used channel made room"
    );
}

#[tokio::test]
async fn concurrent_callers_for_one_pod_share_one_dial() {
    let channels = Arc::new(PodWorkerChannels::default());
    let dials = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let watch = ConnectionWatch::default();
    let pod = key([10, 0, 0, 1], "uid-a", 1);
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let (channels, dials, release, watch, pod) = (
                channels.clone(),
                dials.clone(),
                release.clone(),
                watch.clone(),
                pod.clone(),
            );
            tokio::spawn(async move {
                channels
                    .client(pod, async move {
                        dials.fetch_add(1, Ordering::SeqCst);
                        release.notified().await;
                        Ok((channel(), watch))
                    })
                    .await
            })
        })
        .collect();
    while dials.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    release.notify_one();
    for caller in callers {
        caller.await.unwrap().expect("the shared dial succeeds");
    }
    assert_eq!(dials.load(Ordering::SeqCst), 1, "one dial serves the burst");
    assert_eq!(channels.len(), 1);
}

#[tokio::test]
async fn a_shared_dial_failure_reaches_every_waiter_and_is_not_kept() {
    let channels = Arc::new(PodWorkerChannels::default());
    let release = Arc::new(tokio::sync::Notify::new());
    let pod = key([10, 0, 0, 1], "uid-a", 1);
    let dial = {
        let release = release.clone();
        async move {
            release.notified().await;
            Err(ControllerError::service_unavailable(
                "node-api not reachable",
            ))
        }
    };
    let first = tokio::spawn({
        let (channels, pod) = (channels.clone(), pod.clone());
        async move { channels.client(pod, dial).await }
    });
    while channels.len() == 0 {
        tokio::task::yield_now().await;
    }
    let joined = tokio::spawn({
        let (channels, pod) = (channels.clone(), pod.clone());
        async move {
            channels
                .client(pod, async { panic!("a joining caller never dials") })
                .await
        }
    });
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    release.notify_one();
    first.await.unwrap().unwrap_err();
    joined.await.unwrap().unwrap_err();
    assert_eq!(channels.len(), 0, "a failed dial is not kept");
}

#[tokio::test]
async fn a_dial_outlives_a_caller_that_gives_up() {
    let channels = Arc::new(PodWorkerChannels::default());
    let dials = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let pod = key([10, 0, 0, 1], "uid-a", 1);
    let started = tokio::spawn({
        let (channels, dials, release, pod) = (
            channels.clone(),
            dials.clone(),
            release.clone(),
            pod.clone(),
        );
        async move {
            channels
                .client(pod, async move {
                    dials.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok((channel(), ConnectionWatch::default()))
                })
                .await
        }
    });
    while dials.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    started.abort();
    let _ = started.await;
    release.notify_one();
    let joined = channels
        .client(pod.clone(), async {
            panic!("the first dial is still shared")
        })
        .await;
    assert!(
        joined.is_ok(),
        "the abandoned dial still completes for the next caller"
    );
    assert!(channels.live(&pod, Instant::now()).is_some());
}

async fn serving_health_channel() -> (Channel, tokio_util::sync::CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (_reporter, health) = tonic_health::server::health_reporter();
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, shutdown)
}

/// A peer that accepts the connection and then never answers, as a pod
/// lost without closing its sockets looks.
async fn silent_channel() -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy()
}

async fn wait_for_probe(watch: &ConnectionWatch) {
    // The probe ends within its own timeout; then it releases its claim.
    let deadline =
        Instant::now() + crate::controllers::substrate::KEPT_CONNECTION_PROBE_TIMEOUT * 3;
    while Instant::now() < deadline {
        if watch.try_begin_probe() {
            watch.end_probe();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the probe did not finish");
}

#[tokio::test]
async fn a_probe_keeps_a_connection_whose_peer_answers() {
    let (channel, shutdown) = serving_health_channel().await;
    let watch = ConnectionWatch::default();
    let client = RuntimeClient::kept(channel, watch.clone());
    client.verify_kept_connection();
    tokio::task::yield_now().await;
    wait_for_probe(&watch).await;
    assert!(!watch.is_closed(), "a healthy connection stays shared");
    shutdown.cancel();
}

#[tokio::test]
async fn a_probe_retires_a_connection_whose_peer_is_silent() {
    let watch = ConnectionWatch::default();
    let client = RuntimeClient::kept(silent_channel().await, watch.clone());
    client.verify_kept_connection();
    tokio::task::yield_now().await;
    wait_for_probe(&watch).await;
    assert!(watch.is_closed(), "the next caller dials afresh");
}
