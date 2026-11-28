use std::sync::atomic::{AtomicUsize, Ordering};

use axum::http::StatusCode;

use super::super::scripted_test_doubles::{
    KubeLog, KubeReply, KubeRequest, kube_status, scripted_kube_client, scripted_kube_client_async,
};
use super::*;

const NAMESPACE: &str = "space";
const EVENTS_PATH: &str = "/api/v1/namespaces/space/events";
const INVALID_REFUSAL: &str = "Failed to create pod sandbox: sbx-manager Wake \
    [space-err=InvalidArgument]: rootfs_size_mb=300 is smaller than cloned rootfs size 451 MiB";
/// A reason kubelet retries past, which ends no wait.
const RETRYABLE_REFUSAL: &str =
    "Failed to create pod sandbox: sbx-manager Wake [space-err=Unavailable]: restarting";

fn refusal_event_at(pod_uid: &str, message: &str, resource_version: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "name": format!("{pod_uid}.refusal"),
            "namespace": NAMESPACE,
            "resourceVersion": resource_version,
        },
        "involvedObject": { "kind": "Pod", "name": pod_uid, "uid": pod_uid },
        "reason": FAILED_CREATE_POD_SANDBOX_REASON,
        "message": message,
        "type": "Warning",
    })
}

fn refusal_event(pod_uid: &str, message: &str) -> serde_json::Value {
    refusal_event_at(pod_uid, message, "1")
}

fn event_list_at(
    resource_version: &str,
    continue_token: Option<&str>,
    items: Vec<serde_json::Value>,
) -> KubeReply {
    let mut metadata = serde_json::json!({ "resourceVersion": resource_version });
    if let Some(token) = continue_token {
        metadata["continue"] = serde_json::json!(token);
    }
    KubeReply::Json(
        StatusCode::OK,
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "EventList",
            "metadata": metadata,
            "items": items,
        }),
    )
}

fn event_list(items: Vec<serde_json::Value>) -> KubeReply {
    event_list_at("1", None, items)
}

fn is_watch(request: &KubeRequest) -> bool {
    request
        .query
        .as_deref()
        .is_some_and(|query| query.contains("watch=true"))
}

fn lists(log: &KubeLog) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|request| request.path == EVENTS_PATH && !is_watch(request))
        .count()
}

fn watches(log: &KubeLog) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|request| request.path == EVENTS_PATH && is_watch(request))
        .count()
}

/// Every read of Events is the process's one reason-filtered watch, never a
/// read scoped to one pod.
fn assert_only_the_shared_watch(log: &KubeLog) {
    let log = log.lock().unwrap();
    assert!(!log.is_empty(), "the watcher reads Events");
    for request in log.iter() {
        let query = request.query.as_deref().unwrap_or_default();
        assert_eq!(request.path, EVENTS_PATH);
        assert!(
            query.contains("fieldSelector=reason%3DFailedCreatePodSandBox")
                && !query.contains("involvedObject"),
            "only the shared, reason-filtered watch reads Events: {query}"
        );
    }
}

fn start(client: Client) -> (PodSandboxRejections, CancellationToken) {
    let shutdown = CancellationToken::new();
    let rejections =
        PodSandboxRejections::start(client, NAMESPACE, shutdown.clone(), &TaskTracker::new());
    (rejections, shutdown)
}

fn retained_refusal(
    state: &State,
    pod_uid: &str,
    fail_fast_on_memory_refusal: bool,
    now: Instant,
) -> Option<SubstrateError> {
    let retained = state.refusals.get(pod_uid)?;
    retained
        .verdicts
        .retained_error(fail_fast_on_memory_refusal, now)
}

/// A refusal reaches a wait whether it was recorded before the wait
/// registered or after, across paginated `EventList` pages, and only the wait
/// for that pod.
#[tokio::test]
async fn a_refusal_reaches_the_wait_for_its_pod_whenever_it_registers() {
    let release_list = Arc::new(tokio::sync::Notify::new());
    let release_list_reply = Arc::clone(&release_list);
    let watch_opened = Arc::new(tokio::sync::Notify::new());
    let watch_opened_reply = Arc::clone(&watch_opened);
    let (client, log) = scripted_kube_client_async(move |request| {
        let release_list = Arc::clone(&release_list_reply);
        let watch_opened = Arc::clone(&watch_opened_reply);
        async move {
            let query = request.query.as_deref().unwrap_or_default();
            if is_watch(&request) {
                watch_opened.notify_one();
                KubeReply::Hang
            } else if query.contains("continue=page-2") {
                event_list_at(
                    "7",
                    None,
                    vec![refusal_event_at("pod-registered", INVALID_REFUSAL, "5")],
                )
            } else {
                release_list.notified().await;
                event_list_at(
                    "7",
                    Some("page-2"),
                    vec![
                        refusal_event_at("pod-early", INVALID_REFUSAL, "2"),
                        refusal_event_at("pod-retried", RETRYABLE_REFUSAL, "3"),
                    ],
                )
            }
        }
    });
    let (rejections, shutdown) = start(client);
    // Registered strictly before the gated list completes: woken by page 2.
    let registered = rejections.register("pod-registered");
    release_list.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(5), registered.refused(false))
        .await
        .expect("the listed refusal reaches a registered wait");
    assert!(matches!(error, SubstrateError::InvalidRequest { .. }));
    tokio::time::timeout(Duration::from_secs(5), watch_opened.notified())
        .await
        .expect("the watch opens after paginated list completion");

    let early = rejections.register("pod-early");
    let error = tokio::time::timeout(Duration::from_millis(100), early.refused(false))
        .await
        .expect("a refusal kept before the wait registered still reaches it");
    let SubstrateError::InvalidRequest { message } = error else {
        panic!("expected the node's refusal, got {error:?}");
    };
    assert!(message.contains("smaller than cloned rootfs size 451 MiB"));

    for pod_uid in ["pod-retried", "pod-other"] {
        let wait = rejections.register(pod_uid);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), wait.refused(true))
                .await
                .is_err(),
            "{pod_uid}: no refusal that ends its wait was recorded"
        );
    }
    assert_eq!(lists(&log), 2);
    {
        let requests = log.lock().unwrap();
        let first_list = requests[0].query.as_deref().unwrap_or_default();
        let second_list = requests[1].query.as_deref().unwrap_or_default();
        let watch_query = requests[2].query.as_deref().unwrap_or_default();
        assert!(
            first_list.contains("limit=500") && !first_list.contains("continue="),
            "first list page requests bounded page size: {first_list}"
        );
        assert!(
            second_list.contains("limit=500") && second_list.contains("continue=page-2"),
            "second list page advances continue cursor: {second_list}"
        );
        assert!(
            watch_query.contains("resourceVersion=7")
                && watch_query.contains("allowWatchBookmarks=true"),
            "watch starts from the paginated snapshot's collection resourceVersion with bookmarks enabled: {watch_query}"
        );
    }
    assert_only_the_shared_watch(&log);
    shutdown.cancel();
}

/// A watch the apiserver expires (410 Gone) — whether as an in-stream `ERROR`
/// frame or a bare HTTP 410 `Status` response — is listed again, delivering
/// refusals from the fresh list and resuming the watch from that list's
/// `resourceVersion`.
#[tokio::test(start_paused = true)]
async fn an_expired_watch_is_listed_again() {
    #[derive(Clone, Copy, Debug)]
    enum ExpiredWatch {
        InStreamError,
        HttpStatus,
    }

    impl ExpiredWatch {
        fn reply(self) -> KubeReply {
            match self {
                Self::InStreamError => KubeReply::WatchEvents(vec![serde_json::json!({
                    "type": "ERROR",
                    "object": {
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "message": "too old resource version",
                        "reason": "Expired",
                        "code": 410,
                    },
                })]),
                Self::HttpStatus => kube_status(StatusCode::GONE, "too old resource version"),
            }
        }
    }

    for case in [ExpiredWatch::InStreamError, ExpiredWatch::HttpStatus] {
        let list_calls = AtomicUsize::new(0);
        let watch_calls = AtomicUsize::new(0);
        let (stream_tx, stream_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream_rx = Mutex::new(Some(stream_rx));
        let (client, log) = scripted_kube_client(move |request| {
            let query = request.query.as_deref().unwrap_or_default();
            if is_watch(request) {
                if watch_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert!(
                        query.contains("resourceVersion=1"),
                        "{case:?}: initial watch starts from initial list version: {query}"
                    );
                    return case.reply();
                }
                if query.contains("resourceVersion=9")
                    && let Some(rx) = stream_rx.lock().unwrap().take()
                {
                    let mut event = refusal_event_at("pod-b", INVALID_REFUSAL, "10");
                    event["apiVersion"] = serde_json::json!("v1");
                    event["kind"] = serde_json::json!("Event");
                    let _ = stream_tx.send(serde_json::json!({
                        "type": "ADDED",
                        "object": event,
                    }));
                    return KubeReply::WatchStream(rx);
                }
                KubeReply::Hang
            } else if list_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                event_list_at("1", None, Vec::new())
            } else {
                event_list_at(
                    "9",
                    None,
                    vec![refusal_event_at("pod-a", INVALID_REFUSAL, "7")],
                )
            }
        });
        let (rejections, shutdown) = start(client);
        let wait_a = rejections.register("pod-a");
        let wait_b = rejections.register("pod-b");
        let error_a = tokio::time::timeout(Duration::from_secs(10), wait_a.refused(false))
            .await
            .unwrap_or_else(|_| panic!("{case:?}: the re-list delivers pod-a's refusal"));
        assert!(matches!(error_a, SubstrateError::InvalidRequest { .. }));
        let error_b = tokio::time::timeout(Duration::from_secs(10), wait_b.refused(false))
            .await
            .unwrap_or_else(|_| {
                panic!("{case:?}: the watch resumed at resourceVersion=9 delivers pod-b's refusal")
            });
        assert!(matches!(error_b, SubstrateError::InvalidRequest { .. }));
        assert_eq!(lists(&log), 2, "{case:?}");
        assert_eq!(watches(&log), 2, "{case:?}");
        assert_only_the_shared_watch(&log);
        shutdown.cancel();
    }
}

/// Events the process may not read, or watches that fail to open or close
/// prematurely after listing, leave every wait to its IP, back off
/// exponentially rather than resetting to step 0 (~800ms), and keep a resumed
/// idle watch open across [`WATCH_RECOVERY_WINDOW`] before renewing cleanly.
#[tokio::test(start_paused = true)]
async fn unreadable_events_leave_waits_pending() {
    for (case, re_lists, expected_resume_rv, client_and_log) in [
        (
            "initial list 403",
            true,
            None,
            scripted_kube_client(|_| kube_status(StatusCode::FORBIDDEN, "Forbidden")),
        ),
        (
            "list 200 then immediate watch ERROR 410",
            true,
            None,
            scripted_kube_client(|request| {
                if is_watch(request) {
                    KubeReply::WatchEvents(vec![serde_json::json!({
                        "type": "ERROR",
                        "object": {
                            "kind": "Status",
                            "apiVersion": "v1",
                            "status": "Failure",
                            "message": "too old resource version",
                            "reason": "Expired",
                            "code": 410,
                        },
                    })])
                } else {
                    event_list(Vec::new())
                }
            }),
        ),
        (
            "list 200 then bare HTTP 410 watch",
            true,
            None,
            scripted_kube_client(|request| {
                if is_watch(request) {
                    kube_status(StatusCode::GONE, "too old resource version")
                } else {
                    event_list(Vec::new())
                }
            }),
        ),
        (
            "list 200 then watch 503",
            false,
            Some("1"),
            scripted_kube_client(|request| {
                if is_watch(request) {
                    kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable")
                } else {
                    event_list(Vec::new())
                }
            }),
        ),
        (
            "list 200 then immediate empty HTTP 200 watch",
            false,
            Some("1"),
            scripted_kube_client(|request| {
                if is_watch(request) {
                    KubeReply::WatchEvents(Vec::new())
                } else {
                    event_list(Vec::new())
                }
            }),
        ),
        (
            "list 200 then watch bookmark followed by premature EOF",
            false,
            Some("2"),
            scripted_kube_client(|request| {
                if is_watch(request) {
                    KubeReply::WatchEvents(vec![serde_json::json!({
                        "type": "BOOKMARK",
                        "object": {
                            "apiVersion": "v1",
                            "kind": "Event",
                            "metadata": { "resourceVersion": "2" },
                        },
                    })])
                } else {
                    event_list(Vec::new())
                }
            }),
        ),
    ] {
        let (client, log) = client_and_log;
        let (rejections, shutdown) = start(client);
        let waits: Vec<_> = (0..16)
            .map(|index| rejections.register(&format!("pod-{index}")))
            .collect();
        let pending =
            futures_util::future::select_all(waits.iter().map(|wait| Box::pin(wait.refused(true))));
        // `DefaultBackoff` starts at 800ms with factor 2.0, 30s cap, and additive
        // jitter in `[1.0x, 2.0x)`: cumulative delays before attempts 4, 5, and 6
        // lie in `[5.6s, 11.2s)`, `[12.0s, 24.0s)`, and `[24.8s, 49.6s)`, so 20s
        // of virtual time always sees 4..=5 total attempts (initial attempt plus
        // 3..=4 retries). A backoff reset on list completion or single-frame EOF
        // would issue far more attempts.
        assert!(
            tokio::time::timeout(Duration::from_secs(20), pending)
                .await
                .is_err(),
            "{case}: waits must remain pending"
        );
        let attempts = if re_lists {
            lists(&log)
        } else {
            assert_eq!(
                lists(&log),
                1,
                "{case}: non-410 watch failures resume from the last resourceVersion without re-listing"
            );
            watches(&log)
        };
        assert!(
            (4..=5).contains(&attempts),
            "{case}: exponential backoff caps total attempts across 20s: {attempts}",
        );
        if let Some(expected_rv) = expected_resume_rv {
            let watch_queries: Vec<String> = log
                .lock()
                .unwrap()
                .iter()
                .filter(|req| is_watch(req))
                .map(|req| req.query.clone().unwrap_or_default())
                .collect();
            for query in &watch_queries[1..] {
                assert!(
                    query.contains(&format!("resourceVersion={expected_rv}")),
                    "{case}: retried watch resumes from resourceVersion={expected_rv}: {query}"
                );
            }
        }
        shutdown.cancel();
    }

    // Resuming from the listed `resourceVersion` after a failed watch open keeps
    // the subsequent HTTP 200 watch stream open across `WATCH_RECOVERY_WINDOW`,
    // delivers a refusal arriving afterward, and renews without backoff when the
    // server closes the mature stream.
    let (stream_tx, stream_rx) = tokio::sync::mpsc::unbounded_channel();
    let stream_rx = Mutex::new(Some(stream_rx));
    let watch_opened = Arc::new(Notify::new());
    let watch_opened_reply = Arc::clone(&watch_opened);
    let renewed_watch = Arc::new(Notify::new());
    let renewed_watch_reply = Arc::clone(&renewed_watch);
    let watch_calls = AtomicUsize::new(0);
    let (client, log) = scripted_kube_client(move |request| {
        if is_watch(request) {
            match watch_calls.fetch_add(1, Ordering::SeqCst) {
                0 => kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable"),
                1 => {
                    watch_opened_reply.notify_one();
                    KubeReply::WatchStream(
                        stream_rx
                            .lock()
                            .unwrap()
                            .take()
                            .expect("resumed watch opens once"),
                    )
                }
                _ => {
                    renewed_watch_reply.notify_one();
                    KubeReply::Hang
                }
            }
        } else {
            event_list(Vec::new())
        }
    });
    let (rejections, shutdown) = start(client);
    let wait = rejections.register("pod-recovered");
    tokio::time::timeout(Duration::from_secs(30), watch_opened.notified())
        .await
        .expect("watcher should open the resumed watch after backoff");
    tokio::task::yield_now().await;
    tokio::time::sleep(WATCH_RECOVERY_WINDOW + Duration::from_secs(1)).await;
    let mut recovered_event = refusal_event_at("pod-recovered", INVALID_REFUSAL, "7");
    recovered_event["apiVersion"] = serde_json::json!("v1");
    recovered_event["kind"] = serde_json::json!("Event");
    stream_tx
        .send(serde_json::json!({
            "type": "ADDED",
            "object": recovered_event,
        }))
        .expect("resumed watch stream stays open past WATCH_RECOVERY_WINDOW");
    let error = tokio::time::timeout(Duration::from_secs(1), wait.refused(false))
        .await
        .expect("the open resumed watch delivers a refusal after WATCH_RECOVERY_WINDOW");
    assert!(matches!(error, SubstrateError::InvalidRequest { .. }));
    assert_eq!(lists(&log), 1);
    assert_eq!(
        watches(&log),
        2,
        "firing recovery_deadline on an open resumed watch must not drop or reopen the stream"
    );
    drop(stream_tx);
    tokio::time::timeout(Duration::from_millis(50), renewed_watch.notified())
        .await
        .expect("closing a watch after WATCH_RECOVERY_WINDOW renews immediately without backoff");
    {
        let requests = log.lock().unwrap();
        let last_watch = requests.last().expect("renewed watch logged");
        assert!(
            last_watch
                .query
                .as_deref()
                .is_some_and(|q| q.contains("resourceVersion=7")),
            "renewed watch resumes from the last observed resourceVersion: {:?}",
            last_watch.query
        );
    }
    assert_only_the_shared_watch(&log);
    shutdown.cancel();
}

/// Refusals are kept for [`REFUSAL_RETENTION`] and for at most
/// [`MAX_RETAINED_PODS`] pods, the oldest dropped first.
#[test]
fn refusals_are_kept_for_a_bounded_time_and_number_of_pods() {
    let started = Instant::now();
    let parsed = || parse_refusal(INVALID_REFUSAL).expect("valid refusal");
    let mut state = State::default();
    assert!(state.retain(
        "pod-a",
        parsed(),
        started,
        Some(started + REFUSAL_RETENTION)
    ));
    assert!(
        !state.retain(
            "pod-a",
            parsed(),
            started,
            Some(started + REFUSAL_RETENTION)
        ),
        "a repeated verdict class is kept once without waking waiters again"
    );
    let just_inside = started + REFUSAL_RETENTION - Duration::from_secs(1);
    assert!(
        !state.retain(
            "pod-a",
            parsed(),
            just_inside,
            Some(just_inside + REFUSAL_RETENTION)
        ),
        "a repeated observation refreshes retention in place without growing order"
    );
    assert_eq!(state.order.len(), state.refusals.len());
    assert!(
        retained_refusal(
            &state,
            "pod-a",
            false,
            started + REFUSAL_RETENTION + Duration::from_secs(1)
        )
        .is_some(),
        "refreshed retention window keeps an active refusal alive across retries"
    );
    assert!(retained_refusal(&state, "pod-a", false, just_inside + REFUSAL_RETENTION).is_none());

    // Fluctuating memory-admission retries while memory fail-fast is disabled
    // must never crowd out a subsequent permanent `InvalidArgument` refusal,
    // and recording the new verdict class refreshes the retention window.
    let mut state = State::default();
    for used_mib in 1000..1010 {
        let memory_msg = format!(
            "Failed to create pod sandbox: node-mgr Allocate [space-err=ResourceExhausted]: \
             {MEMORY_ADMISSION_REFUSAL_PREFIX} node uses {used_mib} MiB"
        );
        let memory = parse_refusal(&memory_msg).expect("memory refusal parses");
        let kept = state.retain(
            "pod-mem-then-invalid",
            memory,
            started,
            Some(started + REFUSAL_RETENTION),
        );
        assert_eq!(kept, used_mib == 1000);
    }
    assert!(
        retained_refusal(&state, "pod-mem-then-invalid", false, started).is_none(),
        "memory refusals remain ignored when fail-fast is disabled"
    );
    let later = started + REFUSAL_RETENTION - Duration::from_secs(10);
    assert!(
        state.retain(
            "pod-mem-then-invalid",
            parsed(),
            later,
            Some(later + REFUSAL_RETENTION)
        ),
        "later InvalidArgument refusal is retained after fluctuating memory refusals"
    );
    assert!(
        matches!(
            retained_refusal(
                &state,
                "pod-mem-then-invalid",
                false,
                started + REFUSAL_RETENTION + Duration::from_secs(1)
            ),
            Some(SubstrateError::InvalidRequest { .. })
        ),
        "refreshed retention window keeps the later InvalidArgument refusal alive"
    );

    let mut state = State::default();
    for index in 0..=MAX_RETAINED_PODS {
        state.retain(
            &format!("pod-{index}"),
            parsed(),
            started,
            Some(started + REFUSAL_RETENTION),
        );
    }
    assert_eq!(state.refusals.len(), MAX_RETAINED_PODS);
    assert!(retained_refusal(&state, "pod-0", false, started).is_none());
    assert!(
        retained_refusal(&state, &format!("pod-{MAX_RETAINED_PODS}"), false, started).is_some()
    );

    // Event timestamps age retention and order eviction regardless of `EventList`
    // key order, and the newest timestamp across legacy and `events.k8s.io/v1`
    // fields wins.
    let wall_now = jiff::Timestamp::from_second(1_760_000_000).expect("valid fixed timestamp");
    let ts_ago = |age_secs: i64| {
        wall_now
            .checked_sub(jiff::SignedDuration::from_secs(age_secs))
            .expect("valid timestamp")
    };
    let make_event_with = |uid: &str,
                           message: &str,
                           last_age_secs: Option<i64>,
                           series_age_secs: Option<i64>,
                           event_time_age_secs: Option<i64>,
                           creation_age_secs: Option<i64>| {
        let mut raw = refusal_event(uid, message);
        raw["apiVersion"] = serde_json::json!("v1");
        raw["kind"] = serde_json::json!("Event");
        let mut ev: Event = serde_json::from_value(raw).expect("valid Event JSON");
        ev.last_timestamp = last_age_secs
            .map(|age| k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(ts_ago(age)));
        ev.series = series_age_secs.map(|age| k8s_openapi::api::core::v1::EventSeries {
            count: Some(2),
            last_observed_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(
                ts_ago(age),
            )),
        });
        ev.event_time = event_time_age_secs
            .map(|age| k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(ts_ago(age)));
        ev.metadata.creation_timestamp = creation_age_secs
            .map(|age| k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(ts_ago(age)));
        ev
    };
    let make_event = |uid: &str,
                      last_age_secs: Option<i64>,
                      series_age_secs: Option<i64>,
                      event_time_age_secs: Option<i64>,
                      creation_age_secs: Option<i64>| {
        make_event_with(
            uid,
            INVALID_REFUSAL,
            last_age_secs,
            series_age_secs,
            event_time_age_secs,
            creation_age_secs,
        )
    };

    let aged = PodSandboxRejections::default();
    let stale_active_waiter = aged.register("pod-stale-with-waiter");
    let mixed_active_waiter = aged.register("pod-mixed-age");
    aged.observe(
        &make_event("pod-stale", Some(300), None, None, None),
        wall_now,
        started,
    );
    aged.observe(
        &make_event("pod-stale-with-waiter", Some(360), None, None, None),
        wall_now,
        started,
    );
    const OLD_MEMORY_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = \
        ResourceExhausted desc = sbx-manager Wake [space-err=ResourceExhausted]: \
        node memory admission refused: available 128 MiB";
    const NEWER_MEMORY_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = \
        ResourceExhausted desc = sbx-manager Wake [space-err=ResourceExhausted]: \
        node memory admission refused: available 64 MiB";
    const REPLAYED_MEMORY_REFUSAL: &str = "Failed to create pod sandbox: rpc error: code = \
        ResourceExhausted desc = sbx-manager Wake [space-err=ResourceExhausted]: \
        node memory admission refused: available 96 MiB";
    aged.observe(
        &make_event_with(
            "pod-mixed-age",
            INVALID_REFUSAL,
            Some(360),
            None,
            None,
            None,
        ),
        wall_now,
        started,
    );
    aged.observe(
        &make_event_with(
            "pod-mixed-age",
            OLD_MEMORY_REFUSAL,
            Some(360),
            None,
            None,
            None,
        ),
        wall_now,
        started,
    );
    aged.observe(
        &make_event_with(
            "pod-mixed-age",
            NEWER_MEMORY_REFUSAL,
            Some(1),
            None,
            None,
            None,
        ),
        wall_now,
        started,
    );
    aged.observe(
        &make_event_with(
            "pod-mixed-age",
            REPLAYED_MEMORY_REFUSAL,
            Some(50),
            None,
            None,
            None,
        ),
        wall_now,
        started,
    );
    aged.observe(
        &make_event("pod-nearly-expired", Some(295), None, None, None),
        wall_now,
        started,
    );
    aged.observe(
        &make_event(
            "pod-series-retry",
            Some(400),
            Some(10),
            Some(450),
            Some(460),
        ),
        wall_now,
        started,
    );
    aged.observe(
        &make_event("pod-future-skew", Some(-5), None, None, None),
        wall_now,
        started,
    );
    {
        let state = State::lock(&aged.shared);
        assert!(retained_refusal(&state, "pod-stale", false, started).is_none());
        assert!(!state.refusals.contains_key("pod-stale-with-waiter"));
        assert!(
            state.refusal("pod-stale-with-waiter", false).is_some(),
            "an active waiter still receives its refusal even when the Event timestamp is older than REFUSAL_RETENTION"
        );
        assert!(retained_refusal(&state, "pod-nearly-expired", false, started).is_some());
        assert!(
            retained_refusal(
                &state,
                "pod-nearly-expired",
                false,
                started + Duration::from_secs(5)
            )
            .is_none(),
            "an Event recorded 295s ago expires 5s later rather than getting a fresh 300s window"
        );
        assert!(
            retained_refusal(
                &state,
                "pod-series-retry",
                false,
                started + Duration::from_secs(289)
            )
            .is_some(),
            "newest series.lastObservedTime wins over older lastTimestamp/eventTime/creationTimestamp"
        );
        assert!(
            retained_refusal(
                &state,
                "pod-future-skew",
                false,
                started + REFUSAL_RETENTION - Duration::from_secs(1)
            )
            .is_some(),
            "minor future clock skew clamps to full retention window"
        );
    }
    drop(stale_active_waiter);
    assert!(
        !State::lock(&aged.shared)
            .refusals
            .contains_key("pod-stale-with-waiter"),
        "dropping a waiter whose Event timestamp already expired must not move it into refusals"
    );
    assert!(
        matches!(
            mixed_active_waiter.recorded(false),
            Some(SubstrateError::InvalidRequest { .. })
        ),
        "an active wait still sees a six-minute-old InvalidArgument alongside a fresh memory refusal"
    );
    drop(mixed_active_waiter);
    {
        let state = State::lock(&aged.shared);
        assert!(
            retained_refusal(&state, "pod-mixed-age", false, started).is_none(),
            "a fresh memory Event must not give an already-expired InvalidArgument a new retention window"
        );
        assert!(
            matches!(
                retained_refusal(&state, "pod-mixed-age", true, started),
                Some(SubstrateError::Unavailable { .. })
            ),
            "the unexpired memory verdict still transfers back to retention"
        );
    }
    let retried_mixed = aged.register("pod-mixed-age");
    assert!(
        retried_mixed.recorded(false).is_none(),
        "a subsequent wait with memory fail-fast disabled must not receive the expired InvalidRequest"
    );
    let Some(SubstrateError::Unavailable { message }) = retried_mixed.recorded(true) else {
        panic!("expected the unexpired memory refusal on re-registration");
    };
    assert!(
        message.contains("available 64 MiB"),
        "extending expiry replaces the stored message with the newer observation without letting an older replay overwrite it: {message}"
    );
    drop(retried_mixed);

    // Both etcd key-order extremes preserve the 4,096 freshest refusals.
    let relisted_fresh_first = PodSandboxRejections::default();
    relisted_fresh_first.observe(
        &make_event("pod-aaa-fresh", Some(1), None, None, None),
        wall_now,
        started,
    );
    for index in 0..MAX_RETAINED_PODS {
        relisted_fresh_first.observe(
            &make_event(&format!("pod-zzz-old-{index}"), Some(240), None, None, None),
            wall_now,
            started,
        );
    }
    assert!(
        retained_refusal(
            &State::lock(&relisted_fresh_first.shared),
            "pod-aaa-fresh",
            false,
            started
        )
        .is_some(),
        "older re-listed Events sorting after a fresh refusal in etcd key order must not evict it"
    );

    let relisted_old_last = PodSandboxRejections::default();
    for index in 0..MAX_RETAINED_PODS {
        relisted_old_last.observe(
            &make_event(&format!("pod-aaa-fresh-{index}"), Some(1), None, None, None),
            wall_now,
            started,
        );
    }
    relisted_old_last.observe(
        &make_event("pod-zzz-old", Some(240), None, None, None),
        wall_now,
        started,
    );
    {
        let state = State::lock(&relisted_old_last.shared);
        assert!(retained_refusal(&state, "pod-zzz-old", false, started).is_none());
        assert!(retained_refusal(&state, "pod-aaa-fresh-0", false, started).is_some());
    }
}

/// Verdicts move from `refusals` into `waiters` while waited on, stay there
/// until the last wait drops, and return to `refusals` with their remaining TTL
/// so a subsequent wait on the same pod UID still sees them.
#[test]
fn a_dropped_wait_unregisters() {
    let rejections = PodSandboxRejections::default();
    let wall_now = jiff::Timestamp::from_second(1_760_000_000).expect("valid fixed timestamp");
    let mut raw = refusal_event("pod-a", INVALID_REFUSAL);
    raw["apiVersion"] = serde_json::json!("v1");
    raw["kind"] = serde_json::json!("Event");
    let event: Event = serde_json::from_value(raw).expect("valid Event JSON");
    rejections.observe(&event, wall_now, Instant::now());
    {
        let state = State::lock(&rejections.shared);
        assert!(state.refusals.contains_key("pod-a"));
        assert_eq!(state.order.len(), 1);
        assert!(state.waiters.is_empty());
    }

    let first = rejections.register("pod-a");
    let second = rejections.register("pod-a");
    {
        let state = State::lock(&rejections.shared);
        assert!(
            !state.refusals.contains_key("pod-a") && state.order.is_empty(),
            "active waiters own the pod's verdicts exclusively"
        );
        assert!(state.refusal("pod-a", false).is_some());
    }

    drop(first);
    {
        let state = State::lock(&rejections.shared);
        assert!(state.waiters.contains_key("pod-a"));
        assert!(!state.refusals.contains_key("pod-a"));
    }

    drop(second);
    {
        let state = State::lock(&rejections.shared);
        assert!(state.waiters.is_empty());
        assert!(state.refusals.contains_key("pod-a"));
        assert_eq!(state.order.len(), 1);
    }

    let retried = rejections.register("pod-a");
    assert!(
        State::lock(&rejections.shared)
            .refusal("pod-a", false)
            .is_some(),
        "verdicts moved back into refusals on last drop remain visible to a retried wait"
    );
    drop(retried);
}

/// A registered wait owns its pod's verdicts in `state.waiters` outside the
/// `MAX_RETAINED_PODS` retention ring, so a burst of `> MAX_RETAINED_PODS`
/// refusals for other pods never evicts an active wait's verdict before the
/// task polls `refused()`.
#[tokio::test(flavor = "current_thread")]
async fn registered_wait_survives_refusal_map_capacity_eviction() {
    let rejections = PodSandboxRejections::default();
    let wait = rejections.register("uid-target");

    let wall_now = jiff::Timestamp::from_second(1_760_000_000).expect("valid fixed timestamp");
    let now = Instant::now();
    let parse_event = |uid: &str| -> Event {
        let mut raw = refusal_event(uid, INVALID_REFUSAL);
        raw["apiVersion"] = serde_json::json!("v1");
        raw["kind"] = serde_json::json!("Event");
        serde_json::from_value(raw).expect("valid Event JSON")
    };
    rejections.observe(&parse_event("uid-target"), wall_now, now);
    for index in 0..=MAX_RETAINED_PODS {
        rejections.observe(&parse_event(&format!("uid-flood-{index}")), wall_now, now);
    }

    {
        let state = State::lock(&rejections.shared);
        assert_eq!(state.refusals.len(), MAX_RETAINED_PODS);
        assert!(
            !state.refusals.contains_key("uid-target"),
            "unregistered map capacity stays strictly bounded"
        );
    }

    let resolved = tokio::time::timeout(Duration::from_millis(50), wait.refused(false))
        .await
        .expect("registered wait must still resolve immediately after map eviction");
    assert!(matches!(resolved, SubstrateError::InvalidRequest { .. }));
}
