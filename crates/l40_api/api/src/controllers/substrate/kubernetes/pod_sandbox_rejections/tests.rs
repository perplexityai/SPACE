use std::sync::atomic::{AtomicUsize, Ordering};

use axum::http::StatusCode;

use super::super::scripted_test_doubles::{
    KubeLog, KubeReply, KubeRequest, kube_status, scripted_kube_client,
};
use super::*;

const NAMESPACE: &str = "space";
const EVENTS_PATH: &str = "/api/v1/namespaces/space/events";
const INVALID_REFUSAL: &str = "Failed to create pod sandbox: sbx-manager Wake \
    [space-err=InvalidArgument]: rootfs_size_mb=300 is smaller than cloned rootfs size 451 MiB";
/// A reason kubelet retries past, which ends no wait.
const RETRYABLE_REFUSAL: &str =
    "Failed to create pod sandbox: sbx-manager Wake [space-err=Unavailable]: restarting";

fn refusal_event(pod_uid: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "name": format!("{pod_uid}.refusal"),
            "namespace": NAMESPACE,
            "resourceVersion": "1",
        },
        "involvedObject": { "kind": "Pod", "name": pod_uid, "uid": pod_uid },
        "reason": FAILED_CREATE_POD_SANDBOX_REASON,
        "message": message,
        "type": "Warning",
    })
}

fn event_list(items: Vec<serde_json::Value>) -> KubeReply {
    KubeReply::Json(
        StatusCode::OK,
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "EventList",
            "metadata": { "resourceVersion": "1" },
            "items": items,
        }),
    )
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

/// A refusal reaches a wait whether it was recorded before the wait
/// registered or after, and only the wait for that pod.
#[tokio::test]
async fn a_refusal_reaches_the_wait_for_its_pod_whenever_it_registers() {
    let (client, log) = scripted_kube_client(|request| {
        if is_watch(request) {
            KubeReply::Hang
        } else {
            event_list(vec![
                refusal_event("pod-early", INVALID_REFUSAL),
                refusal_event("pod-registered", INVALID_REFUSAL),
                refusal_event("pod-retried", RETRYABLE_REFUSAL),
            ])
        }
    });
    let (rejections, shutdown) = start(client);
    // Registered before the list: woken by it. The list is applied in order,
    // so once this wait resolves the earlier refusal has been kept as well.
    let registered = rejections.register("pod-registered");
    let error = tokio::time::timeout(Duration::from_secs(5), registered.refused(false))
        .await
        .expect("the listed refusal reaches a registered wait");
    assert!(matches!(error, SubstrateError::InvalidRequest { .. }));

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
    assert_eq!(lists(&log), 1);
    assert_only_the_shared_watch(&log);
    shutdown.cancel();
}

/// A watch the apiserver expires (410 Gone) is listed again, and a refusal
/// recorded meanwhile reaches the wait from the new list.
#[tokio::test]
async fn an_expired_watch_is_listed_again() {
    let list_calls = AtomicUsize::new(0);
    let watch_calls = AtomicUsize::new(0);
    let (client, log) = scripted_kube_client(move |request| {
        if is_watch(request) {
            if watch_calls.fetch_add(1, Ordering::SeqCst) > 0 {
                return KubeReply::Hang;
            }
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
        } else if list_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            event_list(Vec::new())
        } else {
            event_list(vec![refusal_event("pod-a", INVALID_REFUSAL)])
        }
    });
    let (rejections, shutdown) = start(client);
    let wait = rejections.register("pod-a");
    let error = tokio::time::timeout(Duration::from_secs(10), wait.refused(false))
        .await
        .expect("the re-list delivers the refusal");
    assert!(matches!(error, SubstrateError::InvalidRequest { .. }));
    assert_eq!(lists(&log), 2);
    assert_only_the_shared_watch(&log);
    shutdown.cancel();
}

/// Events the process may not read leave every wait to its IP, as without a
/// refusal, and are retried with backoff rather than by each wait.
#[tokio::test]
async fn unreadable_events_leave_waits_pending() {
    let (client, log) = scripted_kube_client(|_| kube_status(StatusCode::FORBIDDEN, "Forbidden"));
    let (rejections, shutdown) = start(client);
    let waits: Vec<_> = (0..16)
        .map(|index| rejections.register(&format!("pod-{index}")))
        .collect();
    let pending =
        futures_util::future::select_all(waits.iter().map(|wait| Box::pin(wait.refused(true))));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), pending)
            .await
            .is_err()
    );
    assert!(
        (1..=3).contains(&lists(&log)),
        "one backed-off watcher, not a read per wait: {} lists",
        lists(&log)
    );
    shutdown.cancel();
}

/// Refusals are kept for [`REFUSAL_RETENTION`] and for at most
/// [`MAX_RETAINED_PODS`] pods, the oldest dropped first.
#[test]
fn refusals_are_kept_for_a_bounded_time_and_number_of_pods() {
    let started = Instant::now();
    let mut state = State::default();
    assert!(state.retain("pod-a", INVALID_REFUSAL, started));
    assert!(
        !state.retain("pod-a", INVALID_REFUSAL, started),
        "a repeated message is kept once"
    );
    let just_inside = started + REFUSAL_RETENTION - Duration::from_millis(1);
    assert!(state.refusal("pod-a", false, just_inside).is_some());
    assert!(
        state
            .refusal("pod-a", false, started + REFUSAL_RETENTION)
            .is_none()
    );

    let mut state = State::default();
    for index in 0..=MAX_RETAINED_PODS {
        state.retain(&format!("pod-{index}"), INVALID_REFUSAL, started);
    }
    assert_eq!(state.refusals.len(), MAX_RETAINED_PODS);
    assert!(state.refusal("pod-0", false, started).is_none());
    assert!(
        state
            .refusal(&format!("pod-{MAX_RETAINED_PODS}"), false, started)
            .is_some()
    );
}

/// A wait's registration ends with it, so the routing table holds only
/// creates that are still waiting.
#[test]
fn a_dropped_wait_unregisters() {
    let rejections = PodSandboxRejections::default();
    let first = rejections.register("pod-a");
    let second = rejections.register("pod-a");
    drop(first);
    assert!(
        State::lock(&rejections.shared)
            .waiters
            .contains_key("pod-a")
    );
    drop(second);
    assert!(State::lock(&rejections.shared).waiters.is_empty());
}
