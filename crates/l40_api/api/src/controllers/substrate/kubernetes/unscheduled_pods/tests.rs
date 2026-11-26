use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;

use super::*;

fn pod(name: &str, node_name: Option<&str>, phase: &str, deleting: bool) -> SandboxPodView {
    pod_with_conditions(name, node_name, phase, deleting, &[])
}

fn pod_with_conditions(
    name: &str,
    node_name: Option<&str>,
    phase: &str,
    deleting: bool,
    conditions: &[serde_json::Value],
) -> SandboxPodView {
    let mut metadata = serde_json::json!({
        "name": name,
        "namespace": "space",
    });
    if deleting {
        metadata["deletionTimestamp"] = "2026-10-30T00:00:00Z".into();
    }
    let mut spec = serde_json::json!({ "containers": [] });
    if let Some(node_name) = node_name {
        spec["nodeName"] = node_name.into();
    }
    let mut status = serde_json::json!({ "phase": phase });
    if !conditions.is_empty() {
        status["conditions"] = serde_json::Value::Array(conditions.to_vec());
    }
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": metadata,
        "spec": spec,
        "status": status,
    }))
    .expect("valid pod");
    pod.into()
}

fn unschedulable_condition(message: Option<&str>) -> serde_json::Value {
    let mut cond = serde_json::json!({
        "type": "PodScheduled",
        "status": "False",
        "reason": "Unschedulable",
    });
    if let Some(message) = message {
        cond["message"] = message.into();
    }
    cond
}

fn seeded(pods: impl IntoIterator<Item = SandboxPodView>) -> UnscheduledPods {
    let index = UnscheduledPods::default();
    index.apply(&Event::Init);
    for pod in pods {
        index.apply(&Event::InitApply(pod));
    }
    index.apply(&Event::InitDone);
    index
}

#[test]
fn cold_reflector_reports_pod_cache_cold() {
    let index = UnscheduledPods::default();
    assert_eq!(
        index.count(false),
        UnscheduledPodBacklog::Unknown(UnscheduledPodBacklogUnknown::PodCacheCold),
    );
}

#[test]
fn counts_unscheduled_non_terminal_non_deleting_unassessed_pods_only() {
    let index = seeded([
        pod("pending-unscheduled", None, "Pending", false),
        pod("pending-empty-node", Some(""), "Pending", false),
        pod_with_conditions(
            "pending-scheduler-error",
            None,
            "Pending",
            false,
            &[serde_json::json!({
                "type": "PodScheduled",
                "status": "False",
                "reason": "SchedulerError",
            })],
        ),
        pod_with_conditions(
            "pending-unschedulable-with-msg",
            None,
            "Pending",
            false,
            &[unschedulable_condition(Some(
                "0/3 nodes are available: insufficient space.pplx.net/network-slots.",
            ))],
        ),
        pod_with_conditions(
            "pending-unschedulable-reason-only",
            None,
            "Pending",
            false,
            &[unschedulable_condition(None)],
        ),
        pod("pending-bound", Some("node-a"), "Pending", false),
        pod("running-bound", Some("node-a"), "Running", false),
        pod("pending-deleting", None, "Pending", true),
        pod("failed-unscheduled", None, "Failed", false),
        pod("succeeded-unscheduled", None, "Succeeded", false),
    ]);
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(3));
}

#[test]
fn bind_unschedulable_delete_and_terminal_transitions_decrement_count_idempotently() {
    let index = seeded([
        pod("pod-a", None, "Pending", false),
        pod("pod-b", None, "Pending", false),
        pod("pod-c", None, "Pending", false),
        pod("pod-d", None, "Pending", false),
    ]);
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(4));

    // Duplicate unscheduled Apply is idempotent.
    index.apply(&Event::Apply(pod("pod-a", None, "Pending", false)));
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(4));

    // Scheduler bind removes `pod-a`.
    index.apply(&Event::Apply(pod(
        "pod-a",
        Some("node-1"),
        "Pending",
        false,
    )));
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(3));

    // Scheduler marking `PodScheduled=False, reason=Unschedulable` removes `pod-d`
    // once evaluated (for example a warm-pool refill waiting on node scale-up).
    index.apply(&Event::Apply(pod_with_conditions(
        "pod-d",
        None,
        "Pending",
        false,
        &[unschedulable_condition(Some(
            "0/2 nodes are available: node affinity mismatch.",
        ))],
    )));
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(2));

    // Deletion timestamp removes `pod-b` before finalizer completion.
    index.apply(&Event::Apply(pod("pod-b", None, "Pending", true)));
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(1));

    // Explicit Delete removes `pod-c`; repeated Delete is a no-op.
    let deleted = pod("pod-c", None, "Pending", false);
    index.apply(&Event::Delete(deleted.clone()));
    index.apply(&Event::Delete(deleted));
    assert_eq!(index.count(true), UnscheduledPodBacklog::Fresh(0));
}

#[test]
fn watch_error_and_repeated_relist_preserve_original_stale_timestamp_until_swap() {
    let t0 = Instant::now();
    let index = seeded([
        pod("pod-a", None, "Pending", false),
        pod("pod-b", None, "Pending", false),
    ]);

    index.mark_watch_error_at(t0);
    assert_eq!(
        index.count_at(true, t0 + Duration::from_secs(5)),
        UnscheduledPodBacklog::Stale {
            last_count: 2,
            stale_for: Duration::from_secs(5),
        },
    );

    // A later watch error or failed re-list `Init` must NOT reset `since`, or
    // repeated apiserver `504` retries would hold a stale trip forever.
    index.mark_watch_error_at(t0 + Duration::from_secs(12));
    index.apply_at(&Event::Init, t0 + Duration::from_secs(20));
    index.apply_at(
        &Event::InitApply(pod("pod-c", None, "Pending", false)),
        t0 + Duration::from_secs(25),
    );
    assert_eq!(
        index.count_at(true, t0 + Duration::from_secs(35)),
        UnscheduledPodBacklog::Stale {
            last_count: 2,
            stale_for: Duration::from_secs(35),
        },
    );

    // Completing the re-list swaps in the rebuilt snapshot and clears staleness.
    index.apply_at(&Event::InitDone, t0 + Duration::from_secs(36));
    assert_eq!(
        index.count_at(true, t0 + Duration::from_secs(36)),
        UnscheduledPodBacklog::Fresh(1),
    );
}

#[test]
fn live_event_clears_watch_error_staleness() {
    let t0 = Instant::now();
    let index = seeded([pod("pod-a", None, "Pending", false)]);
    index.mark_watch_error_at(t0);

    index.apply_at(
        &Event::Apply(pod("pod-a", Some("node-1"), "Pending", false)),
        t0 + Duration::from_secs(3),
    );
    assert_eq!(
        index.count_at(true, t0 + Duration::from_secs(4)),
        UnscheduledPodBacklog::Fresh(0),
    );
}
