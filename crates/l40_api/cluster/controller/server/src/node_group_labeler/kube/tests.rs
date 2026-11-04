use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Node, NodeSpec, Taint};
use kube::api::ObjectMeta;

use super::*;

/// A [`SandboxNodeSelector`] over one `key=value` pair.
///
/// The type has no constructor from a raw map on purpose — the composition has
/// exactly two entry points, both `base_node_selector`, so a test cannot build
/// a selector the production path could not produce. That is the property
/// worth preserving, so this goes the long way through a config rather than
/// reaching for one.
fn selector(key: &str, value: &str) -> SandboxNodeSelector {
    SandboxPodConfigValues {
        sandbox_node_selector_key: key.to_string(),
        sandbox_node_selector_value: value.to_string(),
        extra_node_selector_labels: BTreeMap::new(),
        ..Default::default()
    }
    .base_node_selector()
}

fn node_with(labels: &[(&str, &str)], resource_version: Option<&str>, taints: &[&str]) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some("ip-a.ec2.internal".to_string()),
            uid: Some("uid-ip-a".to_string()),
            resource_version: resource_version.map(str::to_string),
            labels: Some(
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            ..Default::default()
        },
        spec: Some(NodeSpec {
            taints: Some(
                taints
                    .iter()
                    .map(|key| Taint {
                        key: (*key).to_string(),
                        effect: "NoSchedule".to_string(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The label patch must carry the
/// observed resourceVersion as a precondition. Without it, the
/// merge patch applies unconditionally and a node-group label set
/// by another actor between list and patch would be silently
/// overwritten with the default group.
#[test]
fn label_patch_carries_resource_version_precondition() {
    let node = worker_node(&node_with(&[("pool", "workers")], Some("12345"), &[]))
        .expect("named node with a resourceVersion projects");
    let patch = label_patch(&node, "workers");
    assert_eq!(patch["metadata"]["resourceVersion"], "12345");
    assert_eq!(patch["metadata"]["labels"][NODE_GROUP_LABEL], "workers");
    // The body must carry nothing else — a merge patch that also sent, say,
    // spec or status would overwrite fields this reconciler has no business
    // touching.
    assert_eq!(
        patch,
        serde_json::json!({
            "metadata": {
                "resourceVersion": "12345",
                "labels": { NODE_GROUP_LABEL: "workers" }
            }
        })
    );
}

#[test]
fn worker_node_projects_group_label_and_dedicated_taint() {
    let projected = worker_node(&node_with(
        &[("pool", "workers"), (NODE_GROUP_LABEL, "acme-dedicated")],
        Some("77"),
        &[DEDICATED_TAINT_KEY],
    ))
    .expect("named node with a resourceVersion projects");

    assert_eq!(projected.name, "ip-a.ec2.internal");
    assert_eq!(projected.uid, "uid-ip-a");
    assert_eq!(projected.resource_version, "77");
    assert_eq!(projected.node_group.as_deref(), Some("acme-dedicated"));
    assert!(projected.dedicated);
}

#[test]
fn worker_node_untainted_unlabeled_node_is_shared_and_groupless() {
    let projected = worker_node(&node_with(&[("pool", "workers")], Some("5"), &[]))
        .expect("named node with a resourceVersion projects");

    assert_eq!(projected.node_group, None);
    assert!(!projected.dedicated);
}

/// An unrelated taint must not read as "dedicated" — only
/// [`DEDICATED_TAINT_KEY`] excludes a node from the shared pool.
#[test]
fn worker_node_ignores_unrelated_taints() {
    let projected = worker_node(&node_with(
        &[("pool", "workers")],
        Some("5"),
        &["node.kubernetes.io/unreachable"],
    ))
    .expect("named node with a resourceVersion projects");

    assert!(!projected.dedicated);
}

/// Without a resourceVersion the patch precondition can't be made real, so
/// the node is skipped rather than patched unguarded.
#[test]
fn worker_node_skips_node_without_resource_version() {
    assert!(worker_node(&node_with(&[("pool", "workers")], None, &[])).is_none());
}

#[test]
fn worker_node_skips_node_without_name() {
    let mut node = node_with(&[("pool", "workers")], Some("1"), &[]);
    node.metadata.name = None;
    assert!(worker_node(&node).is_none());
}

/// A missing uid is as disqualifying as a missing resourceVersion: the
/// cross-tick baselines are keyed on uid, and the node name is not unique
/// enough to fall back on — EKS reuses it across replacement instances.
#[test]
fn worker_node_skips_node_without_uid() {
    let mut node = node_with(&[("pool", "workers")], Some("1"), &[]);
    node.metadata.uid = None;
    assert!(worker_node(&node).is_none());
}

/// The readiness gate is awkward to cover directly, because a `K8sCaches`
/// can't be constructed outside `pplx-space-api`. A cold
/// cache must read as an error, not as an empty fleet — returning zero nodes
/// would be safe but indistinguishable from a genuinely empty or
/// mis-selected worker pool.
#[test]
fn scan_rejects_a_cold_cache_instead_of_reporting_an_empty_fleet() {
    let selector = selector("pool", "workers");
    let nodes = vec![std::sync::Arc::new(node_with(
        &[("pool", "workers")],
        Some("1"),
        &[],
    ))];

    let cold = scan_worker_nodes(false, &nodes, &selector);
    assert!(
        matches!(cold, Err(WorkerNodeLabelsError::Backend(_))),
        "a cold cache must not read as an empty fleet"
    );

    let warm = scan_worker_nodes(true, &nodes, &selector).expect("a warm cache scans");
    assert_eq!(warm.len(), 1);
    assert_eq!(warm[0].name, "ip-a.ec2.internal");
}

/// A warm cache that genuinely holds no matching node is an empty `Ok`, not an
/// error — that is the case the gate exists to tell apart from a cold one.
#[test]
fn scan_of_a_warm_but_unmatched_cache_is_empty_not_an_error() {
    let selector = selector("pool", "workers");
    let nodes = vec![std::sync::Arc::new(node_with(
        &[("pool", "builders")],
        Some("1"),
        &[],
    ))];

    let scanned = scan_worker_nodes(true, &nodes, &selector).expect("a warm cache scans");
    assert!(scanned.is_empty());
}

/// The pre-write refresh re-applies every gate the tick-opening scan applied,
/// and adds the one the scan cannot: identity.
///
/// EKS derives node names from private IPs the VPC reuses, so a replacement
/// instance can register under a retired node's name. A refresh keyed on name
/// alone would hand back an object this tick never classified — and the census
/// gate that makes labeling safe was evaluated against the classified set, not
/// against whatever holds the name now.
#[test]
fn refreshed_worker_node_gates_on_identity_and_selector() {
    let selector = selector("pool", "workers");
    let cached = node_with(&[("pool", "workers")], Some("99"), &[]);

    let refreshed = refreshed_worker_node(&cached, "uid-ip-a", &selector)
        .expect("the same object still matching the selector refreshes");
    assert_eq!(
        refreshed.resource_version, "99",
        "the refresh carries the store's current version, not the caller's"
    );

    assert!(
        refreshed_worker_node(&cached, "uid-replacement", &selector).is_none(),
        "a different object under the same name is not this node"
    );

    let unselected = node_with(&[("pool", "builders")], Some("99"), &[]);
    assert!(
        refreshed_worker_node(&unselected, "uid-ip-a", &selector).is_none(),
        "a node that no longer matches the worker selector is not ours to label"
    );
}
