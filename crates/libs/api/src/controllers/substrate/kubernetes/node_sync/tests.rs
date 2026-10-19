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

fn node_json(name: &str, uid: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": name, "uid": uid},
        "status": {
            "addresses": [{"type": "InternalIP", "address": "10.0.0.5"}],
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    })
}

/// The credential-manager authority admits a caller only from a node
/// registered under its Kubernetes name and UID (`registered_node`), and
/// internal-api registers a builder without the UID. A sync binds the UID of
/// the Node running the builder's node-api pod onto that same registration,
/// so the authority's node check finds it.
#[tokio::test(flavor = "current_thread")]
async fn sync_binds_a_registered_builder_to_its_node_uid() -> pplx_space_test_env::TestResult {
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store_client_pods_and_nodes;
    use axum::http::{Method, Request, Response, StatusCode};
    use kube::client::{Body, ClientBuilder};
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid};
    use pplx_space_common_svc::sql::node_registration::registered_node;

    type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let worker = node_json("worker-a", "worker-a-uid");
    let nodes = [&worker, &node_json("builder-a", "builder-a-uid")]
        .into_iter()
        .map(|node| serde_json::from_value::<Node>(node.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let builder_node_api = node_api_pod("space-builder-node-api", "builder-a", "space-builder");
    // The sandbox-selector LIST answers with the worker; the builder comes
    // from the reflectors. Any other request fails the sync.
    let service = tower::service_fn(move |req: Request<Body>| {
        let worker = worker.clone();
        async move {
            let (status, body) = match (req.method(), req.uri().path()) {
                (&Method::GET, "/api/v1/nodes") => (
                    StatusCode::OK,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "NodeList",
                        "metadata": {},
                        "items": [worker]
                    }),
                ),
                (method, path) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "message": format!("unexpected mock Kubernetes request: {method} {path}"),
                        "code": 500
                    }),
                ),
            };
            Ok::<_, BoxError>(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body)?))?,
            )
        }
    });
    let substrate = substrate_with_store_client_pods_and_nodes(
        db.pool.clone(),
        ClientBuilder::new(service, "space").build(),
        vec![(*builder_node_api).clone()],
        nodes,
    );

    // Mirrors the rows internal-api's registration writes
    // (bootstrap_k8s_origin_node): the node row, and its node_k8s row with a
    // NULL UID.
    let builder_id = space_k8s_node_id("builder-a", NodeRole::Builder.as_str());
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, last_heartbeat)
         VALUES ($1, '10.0.0.5'::INET, 'builder', 'k8s', 'registered', NOW())"
    ))
    .bind(builder_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, 'builder-a', NULL)"
    ))
    .bind(builder_id)
    .execute(&db.pool)
    .await?;

    substrate
        .sync_labeled_nodes_to_db(Duration::from_secs(600), 10)
        .await?;

    assert_eq!(
        registered_node(
            &db.pool,
            &K8sNodeName::parse("builder-a")?,
            &K8sNodeUid::parse("builder-a-uid")?
        )
        .await?,
        Some(builder_id),
        "the authority's node check must find the builder's registration under its Node's UID"
    );
    Ok(())
}
