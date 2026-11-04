use super::*;

#[tokio::test]
async fn wait_for_returns_true_on_matching_event() {
    let (events, tx) = PodEvents::new();
    let waiter = tokio::spawn({
        let events = events.clone();
        async move { events.wait_for("pod-a", Duration::from_secs(5)).await }
    });
    // Yield so the waiter subscribes before the send.
    tokio::task::yield_now().await;
    tx.send("pod-a".to_string()).unwrap();
    assert!(waiter.await.unwrap());
}

#[tokio::test]
async fn wait_for_ignores_other_pod_names() {
    let (events, tx) = PodEvents::new();
    let waiter = tokio::spawn({
        let events = events.clone();
        async move { events.wait_for("pod-a", Duration::from_millis(250)).await }
    });
    tokio::task::yield_now().await;
    tx.send("pod-b".to_string()).unwrap();
    tx.send("pod-c".to_string()).unwrap();
    assert!(!waiter.await.unwrap());
}

#[tokio::test]
async fn wait_for_returns_false_on_timeout_with_no_events() {
    let (events, _tx) = PodEvents::new();
    assert!(!events.wait_for("pod-a", Duration::from_millis(250)).await);
}

fn pod_named(name: &str) -> Pod {
    let mut pod = Pod::default();
    pod.metadata.name = Some(name.to_string());
    pod
}

/// A bound sandbox pod requesting `slots` network slots on `node`.
fn slot_pod(name: &str, node: &str, slots: u64) -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": { "name": name },
        "spec": {
            "nodeName": node,
            "containers": [{
                "name": "sandbox",
                "resources": {
                    "requests": {
                        pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE:
                            slots.to_string()
                    }
                }
            }]
        },
        "status": { "phase": "Running" }
    }))
    .expect("valid pod fixture")
}

#[tokio::test]
async fn reflect_sandbox_pods_updates_store_and_slots_then_broadcasts_touched_names() {
    use futures_util::stream;
    use kube::runtime::watcher::Event;

    let (store, network_slots, writer) = sandbox_pod_view::sandbox_pod_cache();
    let (_events, tx) = PodEvents::new();
    let mut rx = tx.subscribe();

    // The broadcast must fire for re-list (`InitApply`), applied and deleted
    // pods alike, so a suspend-side waiter can observe pod absence without
    // polling. `Init`/`InitDone` carry no pod and broadcast nothing.
    let raw = stream::iter(
        [
            Event::Init,
            Event::InitApply(slot_pod("pod-listed", "node-a", 2)),
            Event::InitDone,
            Event::Apply(slot_pod("pod-applied", "node-a", 1)),
            Event::Delete(slot_pod("pod-listed", "node-a", 2)),
        ]
        .into_iter()
        .map(|event| Ok::<_, watcher::Error>(sandbox_pod_view::sandbox_pod_view_event(event))),
    );
    reflect_sandbox_pods(raw.boxed(), writer, tx, TEST_NS).await;

    let seen: Vec<String> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(seen, ["pod-listed", "pod-applied", "pod-listed"]);
    let cached: Vec<String> = store
        .state()
        .iter()
        .map(|pod| pod.name().to_string())
        .collect();
    assert_eq!(cached, ["pod-applied"]);
    assert_eq!(network_slots.requested_on("node-a"), 1);
}

const TEST_NS: &str = "test-ns";

fn pod_store_with(pods: &[Pod]) -> (Store<SandboxPodView>, SandboxPodWriter) {
    let (store, _, writer) = sandbox_pod_view::seeded_sandbox_pod_cache(pods.iter().cloned());
    (store, writer)
}

#[tokio::test]
async fn wait_for_pod_absence_pending_when_pod_never_cached() {
    // A pod that is never seen present (cold cache / watch restart) must
    // NOT be reported as a confirmed deletion — a bare cache miss is not
    // proof the reflector observed a delete. The wait runs out the budget
    // and returns Pending so the caller falls back to an authoritative GET.
    let (store, _writer) = pod_store_with(&[]);
    let (events, _tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_millis(150);
    assert_eq!(
        wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, false).await,
        ReflectorAbsence::Pending
    );
}

#[tokio::test]
async fn wait_for_pod_absence_confirms_fast_delete_when_present_before_delete() {
    // Fast-delete race: the pod was cached before the DELETE, but the
    // reflector removed it before the wait started, so the store is already
    // absent on entry. Seeded with present_before_delete=true, that absence
    // is a real present->absent transition and must confirm immediately
    // rather than burning the full budget.
    let (store, _writer) = pod_store_with(&[]);
    let (events, _tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    assert_eq!(
        wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, true).await,
        ReflectorAbsence::Confirmed
    );
}

#[tokio::test]
async fn wait_for_pod_absence_pending_when_only_matching_signal_and_store_absent() {
    // A name-matching event with the store still absent must NOT confirm: a
    // reflector re-list broadcasts buffered `InitApply` names for pods that
    // still exist before swapping them into the readable store, so this is
    // not proof of a delete. Without a present-then-absent transition the
    // wait stays Pending and the caller falls back to an authoritative GET.
    let (store, _writer) = pod_store_with(&[]);
    let (events, tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_millis(150);

    let waiter = wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, false);
    let driver = async {
        tokio::task::yield_now().await;
        tx.send("pod-a".to_string()).unwrap();
    };
    let (absence, ()) = tokio::join!(waiter, driver);
    assert_eq!(absence, ReflectorAbsence::Pending);
}

#[tokio::test]
async fn wait_for_pod_absence_confirms_when_pod_appears_then_is_deleted() {
    use kube::runtime::watcher::Event;
    // Cold cache on entry, but the reflector catches up: the pod first
    // appears (Terminating) and is then deleted. The present-then-absent
    // transition is a real observed delete, so this confirms.
    let pod = pod_named("pod-a");
    let (store, mut writer) = pod_store_with(&[]);
    let (events, tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    let waiter = wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, false);
    let driver = async {
        tokio::task::yield_now().await;
        writer.apply_watcher_event(&Event::Apply(pod.clone().into()));
        tx.send("pod-a".to_string()).unwrap();
        tokio::task::yield_now().await;
        writer.apply_watcher_event(&Event::Delete(pod.clone().into()));
        tx.send("pod-a".to_string()).unwrap();
    };
    let (absence, ()) = tokio::join!(waiter, driver);
    assert_eq!(absence, ReflectorAbsence::Confirmed);
}

#[tokio::test]
async fn wait_for_pod_absence_confirms_on_delete_signal() {
    use kube::runtime::watcher::Event;
    let pod = pod_named("pod-a");
    let (store, mut writer) = pod_store_with(std::slice::from_ref(&pod));
    let (events, tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    let waiter = wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, true);
    let driver = async {
        // Let the waiter take its first (present) store check and subscribe.
        tokio::task::yield_now().await;
        // Mirror the reflector ordering: store updated, then signal fired.
        writer.apply_watcher_event(&Event::Delete(pod.clone().into()));
        tx.send("pod-a".to_string()).unwrap();
    };
    let (absence, ()) = tokio::join!(waiter, driver);
    assert_eq!(absence, ReflectorAbsence::Confirmed);
}

#[tokio::test]
async fn wait_for_pod_absence_confirms_on_unrelated_event_after_store_drop() {
    use kube::runtime::watcher::Event;
    // Regression: the absence wait must re-check the store on ANY wakeup,
    // not only on a signal carrying the target pod name. If the pod's own
    // delete signal is dropped under broadcast lag, a later unrelated event
    // (here `pod-b`) must still wake the waiter to observe the drop.
    let pod = pod_named("pod-a");
    let (store, mut writer) = pod_store_with(std::slice::from_ref(&pod));
    let (events, tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_secs(5);

    let waiter = wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, true);
    let driver = async {
        tokio::task::yield_now().await;
        // pod-a's delete lands in the store but its signal never arrives;
        // only an unrelated pod-b event fires.
        writer.apply_watcher_event(&Event::Delete(pod.clone().into()));
        tx.send("pod-b".to_string()).unwrap();
    };
    let (absence, ()) = tokio::join!(waiter, driver);
    assert_eq!(absence, ReflectorAbsence::Confirmed);
}

#[tokio::test]
async fn wait_for_pod_absence_pending_when_still_present_at_deadline() {
    let pod = pod_named("pod-a");
    let (store, _writer) = pod_store_with(&[pod]);
    let (events, _tx) = PodEvents::new();
    let deadline = Instant::now() + Duration::from_millis(150);
    assert_eq!(
        wait_for_pod_absence(&store, TEST_NS, &events, "pod-a", deadline, true).await,
        ReflectorAbsence::Pending
    );
}

#[test]
fn reflector_ready_starts_not_ready_and_flips_on_mark() {
    let ready = ReflectorReady::new();
    assert!(!ready.is_ready());
    ready.mark_ready();
    assert!(ready.is_ready());
}

#[test]
fn reflector_ready_shares_state_across_clones() {
    let ready = ReflectorReady::new();
    let cloned = ready.clone();
    assert!(!cloned.is_ready());
    ready.mark_ready();
    assert!(cloned.is_ready());
}

#[tokio::test]
async fn reflector_ready_wakes_all_waiters() {
    let ready = ReflectorReady::new();
    let first = tokio::spawn({
        let ready = ready.clone();
        async move { ready.wait_until_ready().await }
    });
    let second = tokio::spawn({
        let ready = ready.clone();
        async move { ready.wait_until_ready().await }
    });
    tokio::task::yield_now().await;

    ready.mark_ready();

    tokio::time::timeout(Duration::from_secs(1), async {
        first
            .await
            .expect("first readiness waiter should finish")
            .expect("first readiness waiter should observe ready");
        second
            .await
            .expect("second readiness waiter should finish")
            .expect("second readiness waiter should observe ready");
    })
    .await
    .expect("all readiness waiters should be notified");
}

#[tokio::test]
async fn reflector_ready_wait_after_mark_returns_immediately() {
    let ready = ReflectorReady::new();
    ready.mark_ready();

    tokio::time::timeout(Duration::from_secs(1), ready.wait_until_ready())
        .await
        .expect("an already-ready reflector should not block")
        .expect("an already-ready reflector should return ready");
}

#[tokio::test]
async fn reflector_ready_retains_writer_failure_for_current_and_late_waiters() {
    let ready = ReflectorReady::new();
    let waiting = tokio::spawn({
        let ready = ready.clone();
        async move { ready.wait_until_ready().await }
    });
    tokio::task::yield_now().await;

    ready.mark_writer_dropped();
    ready.mark_ready();

    assert_eq!(
        waiting.await.expect("readiness waiter should finish"),
        Err(ReflectorReadyError::WriterDropped)
    );
    assert_eq!(
        ready.wait_until_ready().await,
        Err(ReflectorReadyError::WriterDropped)
    );
    assert!(!ready.is_ready());
}
