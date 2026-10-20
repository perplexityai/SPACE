use super::*;
use k8s_openapi::api::core::v1::{Node, Pod, PodSpec};
use kube::{
    api::ObjectMeta,
    runtime::{reflector, watcher::Event},
};
use std::sync::Arc;

fn labels(role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(NODE_ROLE_LABEL.to_string(), role.to_string())])
}

fn node(name: &str) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn node_api_pod(name: &str, node: &str, app: &str) -> Arc<Pod> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), app.to_string());
    Arc::new(Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })
}

#[test]
fn node_role_label_defaults_to_worker() {
    assert_eq!(
        node_role_from_labels("node-a", None),
        Some(NodeRole::Worker)
    );
    assert_eq!(
        node_role_from_labels("node-a", Some(&BTreeMap::new())),
        Some(NodeRole::Worker)
    );
}

#[test]
fn node_role_label_parses_known_roles() {
    assert_eq!(
        node_role_from_labels("node-a", Some(&labels("worker"))),
        Some(NodeRole::Worker)
    );
    assert_eq!(
        node_role_from_labels("node-a", Some(&labels("builder"))),
        Some(NodeRole::Builder)
    );
    assert_eq!(
        node_role_from_labels("node-a", Some(&labels("cluster"))),
        Some(NodeRole::Cluster)
    );
}

#[test]
fn node_role_label_rejects_unknown_roles() {
    assert_eq!(node_role_from_labels("node-a", Some(&labels("gpu"))), None);
}

#[test]
fn inventory_includes_builders_outside_the_sandbox_selector() {
    let sandbox_node = node("worker-a");
    let builder_node = node("builder-a");
    let infra_node = node("infra-a");
    let node_api_pods = vec![node_api_pod(
        "space-builder-node-api",
        "builder-a",
        "space-builder",
    )];
    let (pod_store, mut pod_writer) = reflector::store::<Pod>();
    pod_writer.apply_watcher_event(&Event::Init);
    for pod in &node_api_pods {
        pod_writer.apply_watcher_event(&Event::InitApply((**pod).clone()));
    }
    pod_writer.apply_watcher_event(&Event::InitDone);
    let (node_store, mut node_writer) = reflector::store::<Node>();
    node_writer.apply_watcher_event(&Event::Init);
    for node in [builder_node, infra_node] {
        node_writer.apply_watcher_event(&Event::InitApply(node));
    }
    node_writer.apply_watcher_event(&Event::InitDone);
    let caches = super::super::K8sCaches::for_test(pod_store, node_store, true);

    let inventory = inventory_nodes(vec![sandbox_node], &caches)
        .into_iter()
        .map(|(node, role)| (node.metadata.name.unwrap(), role))
        .collect::<Vec<_>>();

    assert_eq!(
        inventory,
        vec![
            ("builder-a".to_string(), NodeRole::Builder),
            ("worker-a".to_string(), NodeRole::Worker),
        ]
    );
}

#[test]
fn demotion_snapshot_includes_live_nodes_outside_worker_selector() {
    let selected_workers = vec!["worker-a".to_string()];
    let all_live_nodes = HashSet::from(["builder-a".to_string(), "worker-a".to_string()]);

    let snapshot = demotion_membership_snapshot(&selected_workers, Some(all_live_nodes));

    assert_eq!(
        snapshot,
        Some(vec!["builder-a".to_string(), "worker-a".to_string()])
    );
}

#[test]
fn demotion_snapshot_fails_closed_without_both_credible_snapshots() {
    let selected_workers = vec!["worker-a".to_string()];
    let live_nodes = HashSet::from(["worker-a".to_string()]);

    assert_eq!(
        demotion_membership_snapshot(&[], Some(live_nodes.clone())),
        None
    );
    assert_eq!(demotion_membership_snapshot(&selected_workers, None), None);
    assert_eq!(
        demotion_membership_snapshot(&selected_workers, Some(HashSet::new())),
        None
    );
}
