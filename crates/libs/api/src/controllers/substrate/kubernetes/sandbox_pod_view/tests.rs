use kube::runtime::reflector::ObjectRef;
use pplx_space_common_core::kubernetes::SANDBOX_PROCESS_ENV_ANNOTATION;

use super::*;

/// Planted in every field the view must not keep: container env, the
/// process-env annotation, `managedFields`, and spec/status the consumers
/// never read.
const UNREAD_MARKER: &str = "unread-marker-7f3a";

fn ts(raw: &str) -> jiff::Timestamp {
    raw.parse().expect("valid timestamp")
}

/// A sandbox pod as the apiserver returns it: managed-fields bookkeeping,
/// user env in both places it's stamped, and spec/status the view drops,
/// alongside every field a consumer reads.
fn full_sandbox_pod() -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": "space-sbx-a-g2",
            "namespace": "space",
            "uid": "pod-uid-1",
            "resourceVersion": "42",
            "creationTimestamp": "2026-10-01T00:00:00Z",
            "deletionTimestamp": "2026-10-01T00:05:00Z",
            "annotations": {
                SANDBOX_ID_ANNOTATION: "sandbox-a",
                SANDBOX_EXECUTION_GENERATION_ANNOTATION: "2",
                SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION: "node-src",
                SANDBOX_TRACEPARENT_ANNOTATION: "00-trace-span-01",
                SANDBOX_PROCESS_ENV_ANNOTATION: UNREAD_MARKER,
                "space.pplx.net/template": UNREAD_MARKER,
            },
            "labels": {
                SANDBOX_MANAGED_BY_LABEL: "space-api",
                SANDBOX_ID_ANNOTATION: "sandbox-a",
                SANDBOX_POOL_ROLE_KEY: "warm",
                "app.kubernetes.io/name": UNREAD_MARKER,
                "unread-label": UNREAD_MARKER,
            },
            "managedFields": [{
                "manager": UNREAD_MARKER,
                "operation": "Update",
                "fieldsType": "FieldsV1",
                "fieldsV1": { "f:metadata": { "f:labels": { UNREAD_MARKER: {} } } },
            }],
        },
        "spec": {
            "nodeName": "node-a",
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{
                            "matchFields": [{
                                "key": NODE_NAME_MATCH_FIELD,
                                "operator": "In",
                                "values": ["node-src"],
                            }],
                        }],
                    },
                },
            },
            "tolerations": [{ "key": UNREAD_MARKER, "operator": "Exists" }],
            "containers": [
                {
                    "name": "sandbox",
                    "env": [{ "name": "USER_TOKEN", "value": UNREAD_MARKER }],
                    "resources": { "requests": { SANDBOX_NETWORK_SLOTS_RESOURCE: "2" } },
                },
                {
                    "name": "sidecar",
                    "resources": { "requests": { SANDBOX_NETWORK_SLOTS_RESOURCE: "1" } },
                },
            ],
        },
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.7",
            "hostIP": UNREAD_MARKER,
            "conditions": [
                {
                    "type": "PodScheduled",
                    "status": "False",
                    "reason": "Unschedulable",
                    "message": "0/3 nodes are available",
                },
                { "type": "Ready", "status": "True" },
                { "type": "ContainersReady", "status": "False", "message": UNREAD_MARKER },
            ],
            "containerStatuses": [{
                "name": "sandbox",
                "image": UNREAD_MARKER,
                "imageID": "",
                "ready": false,
                "restartCount": 0,
                "state": {
                    "terminated": {
                        "exitCode": 0,
                        "finishedAt": "2026-10-01T00:04:00Z",
                    },
                },
            }],
            "initContainerStatuses": [{
                "name": "init",
                "image": "init",
                "imageID": "",
                "ready": false,
                "restartCount": 0,
                "state": { "waiting": { "reason": "ContainerCreating" } },
            }],
            "ephemeralContainerStatuses": [{
                "name": "debug",
                "image": "debug",
                "imageID": "",
                "ready": true,
                "restartCount": 0,
                "state": { "running": {} },
            }],
        },
    }))
    .expect("valid pod fixture")
}

#[test]
fn view_keeps_every_consumer_field_and_drops_the_rest() {
    let view = SandboxPodView::from(full_sandbox_pod());

    assert_eq!(view.name(), "space-sbx-a-g2");
    assert_eq!(view.namespace(), Some("space"));
    assert_eq!(view.uid(), Some("pod-uid-1"));
    assert_eq!(view.resource_version(), Some("42"));
    assert_eq!(view.creation_timestamp(), Some(ts("2026-10-01T00:00:00Z")));
    assert!(view.is_deleting());
    assert_eq!(view.annotation(SandboxPodKey::SandboxId), Some("sandbox-a"));
    assert_eq!(
        view.annotation(SandboxPodKey::ExecutionGeneration),
        Some("2")
    );
    assert_eq!(
        view.annotation(SandboxPodKey::Traceparent),
        Some("00-trace-span-01")
    );
    assert!(view.has_restore_annotation());
    // Labels and annotations stay separate maps.
    assert_eq!(view.label(SandboxPodKey::ManagedBy), Some("space-api"));
    assert_eq!(view.annotation(SandboxPodKey::ManagedBy), None);
    assert_eq!(view.label(SandboxPodKey::PoolRole), Some("warm"));
    assert_eq!(view.node_name(), Some("node-a"));
    assert!(view.has_required_source_pin());
    assert_eq!(view.network_slot_request(), 3);
    assert_eq!(view.phase(), Some("Running"));
    assert_eq!(view.pod_ip(), Some("10.0.0.7"));
    assert!(view.is_ready());
    assert_eq!(
        view.unschedulable_message(),
        Some("0/3 nodes are available")
    );
    assert_eq!(
        view.container_statuses(),
        &SandboxContainerStatuses {
            containers: Box::new([SandboxContainerState {
                terminated: Some(SandboxContainerTerminated {
                    finished_at: Some(ts("2026-10-01T00:04:00Z")),
                }),
                ..Default::default()
            }]),
            init_containers: Box::new([SandboxContainerState {
                waiting: Some(SandboxContainerWaiting {
                    reason: Some("ContainerCreating".into()),
                }),
                ..Default::default()
            }]),
            ephemeral_containers: Box::new([SandboxContainerState {
                running: true,
                ..Default::default()
            }]),
        }
    );

    // Nothing the consumers don't read survives: not the user's env (in
    // either place it's stamped), not managedFields, not unread spec, status,
    // annotations or labels.
    let retained = format!("{view:?}");
    assert!(
        !retained.contains(UNREAD_MARKER),
        "view retained an unread field: {retained}"
    );
}

#[test]
fn view_of_a_bare_pod_reads_as_absent_everywhere() {
    let view = SandboxPodView::from(Pod::default());

    assert_eq!(view.name(), "");
    assert_eq!(view.uid(), None);
    assert!(!view.is_deleting());
    assert!(!view.has_restore_annotation());
    assert_eq!(view.node_name(), None);
    assert!(!view.has_required_source_pin());
    assert_eq!(view.network_slot_request(), 0);
    assert_eq!(view.phase(), None);
    assert!(!view.is_ready());
    assert_eq!(view.unschedulable_message(), None);
    assert_eq!(view.container_statuses().all().count(), 0);
}

#[test]
fn every_key_round_trips_through_its_wire_name() {
    for &key in SandboxPodKey::ALL {
        assert_eq!(SandboxPodKey::parse(key.as_str()), Some(key));
    }
    let wire_names: std::collections::BTreeSet<_> =
        SandboxPodKey::ALL.iter().map(|key| key.as_str()).collect();
    assert_eq!(wire_names.len(), SandboxPodKey::ALL.len());
}

#[test]
fn store_keys_the_view_by_namespace_and_name() {
    let (store, _, _) = seeded_sandbox_pod_cache([full_sandbox_pod()]);

    let found = store
        .get(&ObjectRef::<SandboxPodView>::new("space-sbx-a-g2").within("space"))
        .expect("view is keyed like the pod it came from");
    assert_eq!(found.uid(), Some("pod-uid-1"));
    assert!(
        store
            .get(&ObjectRef::<SandboxPodView>::new("space-sbx-other").within("space"))
            .is_none()
    );
}

fn slot_pod(name: &str, node: Option<&str>, slots: u64, phase: &str, deleting: bool) -> Pod {
    let mut pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": { "name": name },
        "spec": {
            "nodeName": node,
            "containers": [{
                "name": "sandbox",
                "resources": { "requests": { SANDBOX_NETWORK_SLOTS_RESOURCE: slots.to_string() } },
            }],
        },
        "status": { "phase": phase },
    }))
    .expect("valid pod fixture");
    if deleting {
        pod.metadata.deletion_timestamp = Some(
            k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(ts("2026-10-01T00:00:00Z")),
        );
    }
    pod
}

fn apply(writer: &mut SandboxPodWriter, event: watcher::Event<Pod>) {
    writer.apply_watcher_event(&sandbox_pod_view_event(event));
}

#[test]
fn network_slots_track_bound_active_requests_per_node() {
    use watcher::Event;

    let (_, slots, mut writer) = seeded_sandbox_pod_cache([
        slot_pod("a", Some("node-a"), 2, "Running", false),
        slot_pod("b", Some("node-a"), 1, "Pending", false),
        slot_pod("c", Some("node-b"), 4, "Running", false),
        // Unbound, terminal and deleting pods hold no slots.
        slot_pod("unbound", None, 8, "Pending", false),
        slot_pod("finished", Some("node-a"), 8, "Succeeded", false),
        slot_pod("failed", Some("node-a"), 8, "Failed", false),
        slot_pod("deleting", Some("node-a"), 8, "Running", true),
    ]);
    assert_eq!(slots.requested_on("node-a"), 3);
    assert_eq!(slots.requested_on("node-b"), 4);
    assert_eq!(slots.requested_on("node-c"), 0);

    // An update replaces the pod's previous contribution rather than adding
    // to it, including when it moves to another node.
    apply(
        &mut writer,
        Event::Apply(slot_pod("a", Some("node-b"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-a"), 1);
    assert_eq!(slots.requested_on("node-b"), 7);
    // A status-only update (same node, same request) changes nothing.
    apply(
        &mut writer,
        Event::Apply(slot_pod("a", Some("node-b"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-b"), 7);
    // A move with the same request is still a move: the old node gives the
    // slots back and the new one takes them.
    apply(
        &mut writer,
        Event::Apply(slot_pod("a", Some("node-a"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-a"), 4);
    assert_eq!(slots.requested_on("node-b"), 4);
    apply(
        &mut writer,
        Event::Apply(slot_pod("a", Some("node-b"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-a"), 1);
    assert_eq!(slots.requested_on("node-b"), 7);

    // Going terminal or deleting releases the slots; so does the delete.
    apply(
        &mut writer,
        Event::Apply(slot_pod("c", Some("node-b"), 4, "Succeeded", false)),
    );
    assert_eq!(slots.requested_on("node-b"), 3);
    apply(
        &mut writer,
        Event::Apply(slot_pod("b", Some("node-a"), 1, "Running", true)),
    );
    assert_eq!(slots.requested_on("node-a"), 0);
    apply(
        &mut writer,
        Event::Delete(slot_pod("a", Some("node-b"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-b"), 0);
}

#[test]
fn network_slots_swap_in_a_relist_only_when_it_completes() {
    use watcher::Event;

    let (store, slots, mut writer) =
        seeded_sandbox_pod_cache([slot_pod("stale", Some("node-a"), 2, "Running", false)]);
    // The index follows the store's re-list semantics; kube's store is the
    // oracle for which pods a completed list leaves behind.
    let cached = || {
        let mut names: Vec<String> = store
            .state()
            .iter()
            .map(|pod| pod.name().to_string())
            .collect();
        names.sort();
        names
    };

    apply(&mut writer, Event::Init);
    apply(
        &mut writer,
        Event::InitApply(slot_pod("fresh", Some("node-b"), 5, "Running", false)),
    );
    // Mid-relist, readers still see the previous complete list.
    assert_eq!(cached(), ["stale"]);
    assert_eq!(slots.requested_on("node-a"), 2);
    assert_eq!(slots.requested_on("node-b"), 0);

    apply(&mut writer, Event::InitDone);
    // The relist replaces the index whole: a pod it didn't list is gone.
    assert_eq!(cached(), ["fresh"]);
    assert_eq!(slots.requested_on("node-a"), 0);
    assert_eq!(slots.requested_on("node-b"), 5);

    // A relist that restarts part-way discards what the abandoned attempt
    // buffered.
    apply(&mut writer, Event::Init);
    apply(
        &mut writer,
        Event::InitApply(slot_pod("abandoned", Some("node-a"), 7, "Running", false)),
    );
    apply(&mut writer, Event::Init);
    apply(
        &mut writer,
        Event::InitApply(slot_pod("fresh", Some("node-b"), 5, "Running", false)),
    );
    apply(&mut writer, Event::InitDone);
    assert_eq!(cached(), ["fresh"]);
    assert_eq!(slots.requested_on("node-a"), 0);
    assert_eq!(slots.requested_on("node-b"), 5);

    // `InitApply` without a preceding `Init` still lands in a buffer that
    // only `InitDone` publishes.
    apply(
        &mut writer,
        Event::InitApply(slot_pod("late", Some("node-c"), 3, "Running", false)),
    );
    assert_eq!(slots.requested_on("node-c"), 0);
    apply(&mut writer, Event::InitDone);
    assert_eq!(cached(), ["late"]);
    assert_eq!(slots.requested_on("node-b"), 0);
    assert_eq!(slots.requested_on("node-c"), 3);
}

#[test]
fn an_overflowing_slot_request_saturates_without_corrupting_the_node_total() {
    let max = u64::MAX.to_string();
    let greedy: Pod = serde_json::from_value(serde_json::json!({
        "metadata": { "name": "greedy" },
        "spec": {
            "nodeName": "node-a",
            "containers": [
                { "name": "a", "resources": { "requests": { SANDBOX_NETWORK_SLOTS_RESOURCE: max } } },
                { "name": "b", "resources": { "requests": { SANDBOX_NETWORK_SLOTS_RESOURCE: max } } },
            ],
        },
        "status": { "phase": "Running" },
    }))
    .expect("valid pod fixture");
    assert_eq!(
        SandboxPodView::from(greedy.clone()).network_slot_request(),
        u64::MAX
    );

    let (_, slots, mut writer) = seeded_sandbox_pod_cache([
        greedy.clone(),
        slot_pod("other", Some("node-a"), 1, "Running", false),
    ]);
    // The node is past `u64::MAX`; reads saturate.
    assert_eq!(slots.requested_on("node-a"), u64::MAX);

    // Removing the saturated pod leaves exactly what the other pod holds.
    apply(&mut writer, watcher::Event::Delete(greedy));
    assert_eq!(slots.requested_on("node-a"), 1);
}
