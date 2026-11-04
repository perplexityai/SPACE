use super::*;
use futures_util::poll;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, Node, Pod, PodCondition, PodSpec as K8sPodSpec,
    PodStatus as K8sPodStatus,
};
use kube::{api::ObjectMeta, runtime::watcher::Event};
use std::task::Poll;

fn make_pod(node: &str, ip: &str, ready: bool, app: &str) -> Pod {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), app.to_string());
    Pod {
        metadata: ObjectMeta {
            name: Some(format!("{app}-{node}")),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(K8sPodSpec {
            node_name: Some(node.to_string()),
            ..Default::default()
        }),
        status: Some(K8sPodStatus {
            pod_ip: Some(ip.to_string()),
            conditions: Some(vec![PodCondition {
                type_: "Ready".to_string(),
                status: if ready { "True" } else { "False" }.to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
    }
}

fn make_port_pod(annotation_port: Option<&str>, container_port: Option<i32>) -> Pod {
    let mut pod = make_pod("node-a", "10.0.0.1", true, "space-worker");
    if let Some(port) = annotation_port {
        pod.metadata
            .annotations
            .get_or_insert_default()
            .insert(NODE_API_PORT_ANNOTATION.to_string(), port.to_string());
    }
    if let Some(port) = container_port {
        pod.spec.as_mut().unwrap().containers = vec![Container {
            name: "node-api".to_string(),
            ports: Some(vec![ContainerPort {
                name: Some("grpc".to_string()),
                container_port: port,
                ..Default::default()
            }]),
            ..Default::default()
        }];
    }
    pod
}

fn caches_with_pods(pods: Vec<Pod>) -> K8sCaches {
    let (pod_store, mut pod_writer) = reflector::store::<Pod>();
    let (node_store, _node_writer) = reflector::store::<Node>();
    pod_writer.apply_watcher_event(&Event::Init);
    for pod in &pods {
        pod_writer.apply_watcher_event(&Event::InitApply(pod.clone()));
    }
    pod_writer.apply_watcher_event(&Event::InitDone);
    K8sCaches {
        node_api_pods: pod_store,
        nodes: node_store,
        nodes_ready: super::super::pod_reflector::ReflectorReady::always_ready(),
    }
}

#[tokio::test]
async fn startup_wait_does_not_lose_node_readiness_notification() {
    let (pod_store, mut pod_writer) = reflector::store::<Pod>();
    pod_writer.apply_watcher_event(&Event::Init);
    pod_writer.apply_watcher_event(&Event::InitDone);

    let (node_store, mut node_writer) = reflector::store::<Node>();
    let nodes_ready = super::super::pod_reflector::ReflectorReady::new();
    let nodes_ready_task = nodes_ready.clone();
    let node_store_for_ready = node_store.clone();
    tokio::spawn(async move {
        if node_store_for_ready.wait_until_ready().await.is_ok() {
            nodes_ready_task.mark_ready();
        }
    });
    tokio::task::yield_now().await;

    let caches = K8sCaches {
        node_api_pods: pod_store,
        nodes: node_store,
        nodes_ready,
    };
    let mut startup_wait = std::pin::pin!(caches.wait_until_ready());
    assert!(matches!(poll!(startup_wait.as_mut()), Poll::Pending));

    node_writer.apply_watcher_event(&Event::Init);
    node_writer.apply_watcher_event(&Event::InitDone);
    startup_wait
        .await
        .expect("startup cache wait should finish");
    tokio::task::yield_now().await;

    assert!(
        caches.nodes_ready(),
        "the durable Node readiness signal must not lose the store notification"
    );
}

#[tokio::test]
async fn startup_wait_returns_known_node_writer_failure_without_timing_out() {
    let (pod_store, mut pod_writer) = reflector::store::<Pod>();
    pod_writer.apply_watcher_event(&Event::Init);
    pod_writer.apply_watcher_event(&Event::InitDone);

    let (node_store, node_writer) = reflector::store::<Node>();
    let nodes_ready = super::super::pod_reflector::ReflectorReady::new();
    let nodes_ready_task = nodes_ready.clone();
    let node_store_for_ready = node_store.clone();
    tokio::spawn(async move {
        match node_store_for_ready.wait_until_ready().await {
            Ok(()) => nodes_ready_task.mark_ready(),
            Err(_error) => nodes_ready_task.mark_writer_dropped(),
        }
    });
    drop(node_writer);
    tokio::task::yield_now().await;

    let caches = K8sCaches {
        node_api_pods: pod_store,
        nodes: node_store,
        nodes_ready,
    };
    let result = tokio::time::timeout(Duration::from_millis(100), caches.wait_until_ready())
        .await
        .expect("a known writer failure must not wait for the startup timeout");

    assert!(matches!(result, Err(CacheReadyError::WriterDropped)));
}

#[tokio::test]
async fn startup_wait_reports_node_writer_failure_while_pods_are_pending() {
    let (pod_store, _pod_writer) = reflector::store::<Pod>();
    let (node_store, _node_writer) = reflector::store::<Node>();
    let nodes_ready = super::super::pod_reflector::ReflectorReady::new();
    nodes_ready.mark_writer_dropped();

    let caches = K8sCaches {
        node_api_pods: pod_store,
        nodes: node_store,
        nodes_ready,
    };
    let result = tokio::time::timeout(Duration::from_millis(100), caches.wait_until_ready())
        .await
        .expect("a known Node writer failure must not be hidden by pending pod readiness");

    assert!(matches!(result, Err(CacheReadyError::WriterDropped)));
}

#[test]
fn pod_is_ready_reflects_condition() {
    assert!(pod_is_ready(&make_pod(
        "n",
        "10.0.0.1",
        true,
        "space-worker"
    )));
    assert!(!pod_is_ready(&make_pod(
        "n",
        "10.0.0.1",
        false,
        "space-worker"
    )));
}

#[test]
fn ready_node_api_pod_on_picks_ready_pod_on_node() {
    let caches = caches_with_pods(vec![
        make_pod("node-a", "10.0.0.1", false, "space-worker"),
        make_pod("node-a", "10.0.0.2", true, "space-worker"),
        make_pod("node-b", "10.0.0.3", true, "space-worker"),
    ]);
    let found = caches
        .ready_node_api_pod_on("node-a", SpaceNodeRole::Worker.as_str())
        .unwrap();
    assert_eq!(pod_ip(&found), Some("10.0.0.2"));
    assert!(
        caches
            .ready_node_api_pod_on("node-z", SpaceNodeRole::Worker.as_str())
            .is_none()
    );
}

#[test]
fn ready_node_api_pod_on_disambiguates_by_role() {
    let caches = caches_with_pods(vec![
        make_pod("node-a", "10.0.0.5", true, "space-builder"),
        make_pod("node-a", "10.0.0.6", true, "space-worker"),
    ]);
    let worker = caches
        .ready_node_api_pod_on("node-a", SpaceNodeRole::Worker.as_str())
        .unwrap();
    assert_eq!(pod_ip(&worker), Some("10.0.0.6"));
    let builder = caches
        .ready_node_api_pod_on("node-a", SpaceNodeRole::Builder.as_str())
        .unwrap();
    assert_eq!(pod_ip(&builder), Some("10.0.0.5"));
}

#[test]
fn ready_node_api_pod_on_accepts_canary_worker_identity() {
    let caches = caches_with_pods(vec![make_pod(
        "node-a",
        "10.0.0.7",
        true,
        "space-worker-canary",
    )]);

    let worker = caches
        .ready_node_api_pod_on("node-a", SpaceNodeRole::Worker.as_str())
        .unwrap();

    assert_eq!(pod_ip(&worker), Some("10.0.0.7"));
}

#[test]
fn canary_worker_app_name_maps_to_worker_role() {
    let role = role_from_app_name("space-worker-canary");

    assert_eq!(role, Some(SpaceNodeRole::Worker.as_str()));
}

#[test]
fn pod_node_api_port_prefers_annotation_over_legacy_container_port() {
    let pod = make_port_pod(Some("9801"), Some(9800));
    assert_eq!(pod_node_api_port(&pod), Some(9801));
}

#[test]
fn pod_node_api_port_falls_back_to_legacy_container_port() {
    let pod = make_port_pod(None, Some(9802));
    assert_eq!(pod_node_api_port(&pod), Some(9802));
}

#[test]
fn pod_node_api_port_ignores_invalid_values() {
    assert_eq!(
        pod_node_api_port(&make_port_pod(Some("invalid"), None)),
        None
    );
    assert_eq!(pod_node_api_port(&make_port_pod(Some("70000"), None)), None);
}
