//! Per-request authorization of projected node-agent tokens. TokenReview's
//! cache proves token authenticity only; neither pod state, node registration,
//! nor the transport peer's right to use that token is cached here.

use std::{fmt, net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use futures_util::FutureExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api,
    runtime::{
        reflector::{self, ObjectRef, Store},
        watcher,
    },
};
use tokio_util::sync::CancellationToken;

use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId, PodName, PodUid};
use tonic::Status;

use crate::auth::K8sIdentity;
use crate::credential_store::CredentialStore;

const MODE_ENV: &str = "SPACE_CREDMGR_AGENT_AUTHZ_MODE";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum AgentAuthzMode {
    Observe,
    #[default]
    Enforce,
}

impl AgentAuthzMode {
    pub fn from_env() -> Self {
        Self::configured(std::env::var(MODE_ENV).ok().as_deref())
    }

    fn configured(raw: Option<&str>) -> Self {
        raw.and_then(|raw| raw.parse().ok()).unwrap_or_default()
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("agent authorization mode must be observe or enforce")]
pub(super) struct InvalidMode;

impl FromStr for AgentAuthzMode {
    type Err = InvalidMode;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "observe" => Ok(Self::Observe),
            "enforce" => Ok(Self::Enforce),
            _ => Err(InvalidMode),
        }
    }
}

impl fmt::Display for AgentAuthzMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Available in request extensions only after all five checks passed. Observe
/// failures never manufacture a verified identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedAgentIdentity {
    pub pod_uid: PodUid,
    pub node_name: K8sNodeName,
    pub node_uid: K8sNodeUid,
    pub node_id: NodeId,
    pub peer_ip: IpAddr,
}

pub(crate) struct AgentAuth {
    api: Api<Pod>,
    pods: Store<Pod>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    worker_daemonset: String,
    mode: AgentAuthzMode,
}

impl AgentAuth {
    pub(crate) fn new(
        client: kube::Client,
        namespace: &str,
        credential_store: Option<Arc<dyn CredentialStore>>,
        shutdown: CancellationToken,
    ) -> Self {
        let api = Api::namespaced(client, namespace);
        let (pods, writer) = reflector::store();
        pplx_space_common_svc::kube_client::spawn_reflector(
            "credential agent pods",
            writer,
            api.clone(),
            watcher::Config::default().fields("spec.serviceAccountName=space-credential-manager"),
            shutdown,
        );
        Self {
            api,
            pods,
            credential_store,
            worker_daemonset: std::env::var("SPACE_CREDMGR_AGENT_WORKER_DAEMONSET")
                .unwrap_or_else(|_| "space-credential-manager".to_owned()),
            mode: AgentAuthzMode::from_env(),
        }
    }

    async fn pod(&self, name: &PodName) -> Result<Option<Arc<Pod>>, Status> {
        // A cold store cannot establish the caller's current pod identity.
        // Polling readiness once is cancel-safe; no background waiter competes
        // for the store's single readiness notification.
        if !matches!(self.pods.wait_until_ready().now_or_never(), Some(Ok(()))) {
            return Err(Status::unavailable("pod_cache_not_synced"));
        }
        let key = ObjectRef::new(name.as_str())
            .within(self.api.namespace().expect("agent pod API is namespaced"));
        if let Some(pod) = self.pods.get(&key) {
            return Ok(Some(pod));
        }
        // A newly created pod can precede its watch event. Bound the live GET
        // on a miss; never issue a request-path LIST.
        match tokio::time::timeout(Duration::from_secs(5), self.api.get_opt(name.as_str())).await {
            Ok(Ok(pod)) => Ok(pod.map(Arc::new)),
            Ok(Err(error)) => {
                tracing::warn!(%error, "agent authorization pod lookup failed");
                Err(Status::unavailable("pod_api_unavailable"))
            }
            Err(_) => Err(Status::unavailable("pod_api_unavailable")),
        }
    }

    pub async fn authorize(
        &self,
        caller: &K8sIdentity,
        peer: Option<IpAddr>,
        rpc: &str,
    ) -> Result<Option<VerifiedAgentIdentity>, Status> {
        let peer = peer.map(normalize_ip);
        let pod = self.pod(caller.pod().pod_name()).await;
        // Evaluate every independent check even in observe mode and even if an
        // earlier check failed. An unavailable dependency is not a denial.
        let pod_failure = pod.as_ref().err().cloned();
        let pod = pod.as_ref().ok().and_then(|pod| pod.as_deref());
        let pod_check = |passes: bool, reason: &'static str| match &pod_failure {
            Some(error) => Err(error.clone()),
            None if pod.is_none() => Err(Status::permission_denied("pod_not_found")),
            None if passes => Ok(()),
            None => Err(Status::permission_denied(reason)),
        };
        let uid = pod_check(
            pod.is_some_and(|p| p.metadata.uid.as_deref() == Some(caller.pod().pod_uid().as_str())),
            "pod_uid_mismatch",
        );
        let running = pod_check(
            pod.and_then(|p| p.status.as_ref())
                .and_then(|s| s.phase.as_deref())
                == Some("Running"),
            "pod_not_running",
        );
        let owner = pod_check(
            pod.and_then(|p| p.metadata.owner_references.as_ref())
                .is_some_and(|owners| {
                    owners.iter().any(|owner| {
                        owner.controller == Some(true)
                            && owner.api_version == "apps/v1"
                            && owner.kind == "DaemonSet"
                            && owner.name == self.worker_daemonset
                    })
                }),
            "pod_owner_mismatch",
        );
        let node = match &self.credential_store {
            Some(store) => store
                .registered_node(caller.node().node_name(), caller.node().node_uid())
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "agent authorization node lookup failed");
                    Status::unavailable("node_store_unavailable")
                })
                .and_then(|node| {
                    node.ok_or_else(|| Status::permission_denied("node_identity_mismatch"))
                }),
            None => Err(Status::unavailable("node_store_unavailable")),
        };
        let peer_check = match peer {
            None => Err(Status::permission_denied("peer_address_missing")),
            Some(peer) => pod_check(
                pod.and_then(|p| p.status.as_ref())
                    .and_then(|s| s.pod_ips.as_ref())
                    .is_some_and(|ips| {
                        ips.iter().any(|ip| {
                            ip.ip
                                .parse::<IpAddr>()
                                .is_ok_and(|ip| normalize_ip(ip) == peer)
                        })
                    }),
                "peer_ip_mismatch",
            ),
        };
        let checks = [
            ("pod_uid", uid),
            ("pod_running", running),
            ("pod_owner", owner),
            (
                "node_identity",
                node.as_ref().map(|_| ()).map_err(Clone::clone),
            ),
            ("peer_ip", peer_check),
        ];
        let mut failure = None;
        for (check, result) in checks {
            let result_tag = match (&result, self.mode) {
                (Ok(()), _) => "allow",
                (Err(_), AgentAuthzMode::Observe) => "would_deny",
                (Err(_), AgentAuthzMode::Enforce) => "deny",
            };
            pplx_space_metrics::credmgr::agent_authz(check, result_tag, self.mode.as_str(), rpc);
            if let Err(error) = result {
                tracing::warn!(check, result = result_tag, mode = %self.mode, rpc,
                    reason = error.message(), pod_uid = %caller.pod().pod_uid(),
                    node_name = %caller.node().node_name(), peer_ip = ?peer,
                    "credential agent authorization check failed");
                // Infrastructure faults remain retryable even if another
                // independent check also denied the request.
                if failure.is_none() || error.code() == tonic::Code::Unavailable {
                    failure = Some(error);
                }
            }
        }
        match (failure, self.mode) {
            (Some(error), AgentAuthzMode::Enforce) => Err(error),
            (Some(_), AgentAuthzMode::Observe) => Ok(None),
            (None, _) => Ok(Some(VerifiedAgentIdentity {
                pod_uid: caller.pod().pod_uid().clone(),
                node_name: caller.node().node_name().clone(),
                node_uid: caller.node().node_uid().clone(),
                node_id: node.expect("successful node check"),
                peer_ip: peer.expect("successful peer check"),
            })),
        }
    }
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}

#[cfg(test)]
pub(crate) mod tests;
