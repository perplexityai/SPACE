use super::*;
use crate::auth::ClientService;
use crate::credential_store::{CredentialStoreError, PgCredentialStore, SandboxOwner};
use async_trait::async_trait;
use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId, SandboxId};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Nodes(Option<NodeId>);
#[async_trait]
impl CredentialStore for Nodes {
    async fn registered_node(
        &self,
        _: &K8sNodeName,
        _: &K8sNodeUid,
    ) -> Result<Option<NodeId>, CredentialStoreError> {
        Ok(self.0)
    }
    async fn sandbox_owner(
        &self,
        _: SandboxId,
    ) -> Result<Option<SandboxOwner>, CredentialStoreError> {
        Ok(None)
    }
}

fn pod_api(response_pod: Option<&Pod>) -> (Api<Pod>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = calls.clone();
    let (status, response) = match response_pod {
        Some(pod) => (200, serde_json::to_vec(pod).unwrap()),
        None => (404, br#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404,"message":"pod not found"}"#.to_vec()),
    };
    let client = kube::client::ClientBuilder::new(
        tower::service_fn(move |request: http::Request<kube::client::Body>| {
            assert_eq!(request.uri().path(), "/api/v1/namespaces/space/pods/agent");
            assert_eq!(request.method(), http::Method::GET);
            observed_calls.fetch_add(1, Ordering::SeqCst);
            let response = response.clone();
            async move {
                Ok::<_, std::io::Error>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(response))
                        .unwrap(),
                )
            }
        }),
        "space",
    )
    .build();
    (Api::namespaced(client, "space"), calls)
}

fn auth(
    pod: Option<Pod>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    mode: AgentAuthzMode,
) -> AgentAuth {
    let (api, _) = pod_api(pod.as_ref());
    let (pods, mut writer) = reflector::store();
    writer.apply_watcher_event(&watcher::Event::Init);
    if let Some(pod) = pod {
        writer.apply_watcher_event(&watcher::Event::InitApply(pod));
    }
    writer.apply_watcher_event(&watcher::Event::InitDone);
    AgentAuth {
        api,
        pods,
        credential_store,
        worker_daemonset: "worker-agents".into(),
        mode,
    }
}

fn caller() -> K8sIdentity {
    K8sIdentity::from_token_review(
        ClientService::NodeCredentialAgent,
        "agent",
        "pod-uid",
        "worker",
        "node-uid",
    )
    .unwrap()
}

pub(crate) fn pod() -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": "agent", "namespace": "space", "uid": "pod-uid",
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "DaemonSet",
                "name": "worker-agents", "uid": "ds-uid", "controller": true}]},
        "spec": {"serviceAccountName": "space-credential-manager", "nodeName": "worker"},
        "status": {"phase": "Running", "podIPs": [{"ip": "10.0.0.2"}, {"ip": "2001:db8::2"}]}
    }))
    .unwrap()
}

pub(crate) fn authorizer() -> AgentAuth {
    auth(
        Some(pod()),
        Some(Arc::new(Nodes(Some(NodeId::new())))),
        AgentAuthzMode::Enforce,
    )
}

#[tokio::test]
async fn checks_current_pod_node_and_peer_in_both_modes() {
    // Each mutation represents an otherwise legitimate request. The oracle is
    // the five authorization conditions, not the check implementation.
    for case in [
        "allow",
        "uid",
        "phase",
        "owner",
        "non_controller",
        "node",
        "peer",
        "missing_peer",
        "missing_pod",
    ] {
        for mode in [AgentAuthzMode::Observe, AgentAuthzMode::Enforce] {
            let node_id = NodeId::new();
            let mut pod = pod();
            let mut peer = Some("10.0.0.2".parse().unwrap());
            let reason = match case {
                "allow" => None,
                "uid" => {
                    pod.metadata.uid = Some("replacement".into());
                    Some("pod_uid_mismatch")
                }
                "phase" => {
                    pod.status.as_mut().unwrap().phase = Some("Pending".into());
                    Some("pod_not_running")
                }
                "owner" => {
                    pod.metadata.owner_references.as_mut().unwrap()[0].name =
                        "builder-agents".into();
                    Some("pod_owner_mismatch")
                }
                "non_controller" => {
                    pod.metadata.owner_references.as_mut().unwrap()[0].controller = Some(false);
                    Some("pod_owner_mismatch")
                }
                "node" => Some("node_identity_mismatch"),
                "peer" => {
                    peer = Some("10.0.0.99".parse().unwrap());
                    Some("peer_ip_mismatch")
                }
                "missing_peer" => {
                    peer = None;
                    Some("peer_address_missing")
                }
                "missing_pod" => Some("pod_not_found"),
                _ => unreachable!(),
            };
            let auth = auth(
                (case != "missing_pod").then_some(pod),
                Some(Arc::new(Nodes((case != "node").then_some(node_id)))),
                mode,
            );
            let result = auth
                .authorize(&caller(), peer, "IssueIntermediateCertificate")
                .await;
            match (reason, mode) {
                (None, _) | (Some(_), AgentAuthzMode::Observe) => result.unwrap(),
                (Some(reason), AgentAuthzMode::Enforce) => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{case}");
                    assert_eq!(error.message(), reason, "{case}");
                }
            }
        }
    }
}

#[tokio::test]
async fn normalizes_mapped_ipv4_and_accepts_each_pod_ip() {
    let auth = authorizer();
    for ip in ["10.0.0.2", "::ffff:10.0.0.2", "2001:db8::2"] {
        auth.authorize(&caller(), Some(ip.parse().unwrap()), "IssueDek")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn dependency_failures_are_retryable_in_enforce_and_allowed_in_observe() {
    for mode in [AgentAuthzMode::Observe, AgentAuthzMode::Enforce] {
        let closed_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        closed_pool.close().await;
        for (store, reason) in [
            (
                Some(Arc::new(Nodes(Some(NodeId::new()))) as Arc<dyn CredentialStore>),
                "pod_cache_not_synced",
            ),
            (
                Some(Arc::new(PgCredentialStore::new(closed_pool.clone()))),
                "node_store_unavailable",
            ),
            (None, "node_store_unavailable"),
        ] {
            let mut auth = auth(Some(pod()), store, mode);
            let (cold_store, _writer) = reflector::store();
            if reason == "pod_cache_not_synced" {
                auth.pods = cold_store;
            }
            let result = auth
                .authorize(&caller(), Some("10.0.0.2".parse().unwrap()), "IssueDek")
                .await;
            match mode {
                AgentAuthzMode::Observe => result.unwrap(),
                AgentAuthzMode::Enforce => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::Unavailable);
                    assert_eq!(error.message(), reason);
                }
            }
        }
    }
}

#[test]
fn missing_or_unknown_mode_enforces() {
    for raw in [
        None,
        Some(""),
        Some("ENFORCE"),
        Some("observ"),
        Some("enforce"),
        Some("observe"),
    ] {
        let mode = AgentAuthzMode::configured(raw);
        assert_eq!(
            mode,
            if raw == Some("observe") {
                AgentAuthzMode::Observe
            } else {
                AgentAuthzMode::Enforce
            }
        );
    }
}

#[tokio::test]
async fn cache_miss_reads_once_but_unsynced_cache_refuses_without_live_fallback() {
    let pod = pod();
    let (api, calls) = pod_api(Some(&pod));
    let (pods, mut writer) = reflector::store();
    let auth = AgentAuth {
        api,
        pods,
        credential_store: None,
        worker_daemonset: "worker-agents".into(),
        mode: AgentAuthzMode::Enforce,
    };
    let name = "agent".parse().unwrap();
    let error = auth.pod(&name).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "pod_cache_not_synced");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    writer.apply_watcher_event(&watcher::Event::Init);
    writer.apply_watcher_event(&watcher::Event::InitDone);
    assert_eq!(*auth.pod(&name).await.unwrap().unwrap(), pod);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    writer.apply_watcher_event(&watcher::Event::Apply(pod.clone()));
    assert_eq!(*auth.pod(&name).await.unwrap().unwrap(), pod);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "a hit needs no live read");
}
