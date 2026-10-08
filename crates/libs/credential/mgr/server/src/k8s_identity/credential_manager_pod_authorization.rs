//! Per-request pod authorization for credential-manager callers. TokenReview's
//! cache proves token authenticity only; neither pod state, node registration,
//! nor the transport peer's right to use that token is cached here.

use std::{fmt, net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use futures_util::FutureExt;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::ObjectRef;

use pplx_space_common_core::types::PodName;
use tonic::Status;

use super::CredmgrTokenReviewer;
use crate::auth::K8sIdentity;

const MODE_ENV: &str = "SPACE_CREDMGR_AGENT_AUTHZ_MODE";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum PodAuthorizationMode {
    Observe,
    #[default]
    Enforce,
}

impl PodAuthorizationMode {
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
#[error("pod authorization mode must be observe or enforce")]
pub(super) struct InvalidMode;

impl FromStr for PodAuthorizationMode {
    type Err = InvalidMode;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "observe" => Ok(Self::Observe),
            "enforce" => Ok(Self::Enforce),
            _ => Err(InvalidMode),
        }
    }
}

impl fmt::Display for PodAuthorizationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl CredmgrTokenReviewer {
    async fn pod(&self, name: &PodName) -> Result<Option<Arc<Pod>>, Status> {
        // A cold store cannot establish the caller's current pod identity.
        // Polling readiness once is cancel-safe; no background waiter competes
        // for the store's single readiness notification.
        if !matches!(self.pods.wait_until_ready().now_or_never(), Some(Ok(()))) {
            return Err(Status::unavailable("pod_cache_not_synced"));
        }
        let key = ObjectRef::new(name.as_str())
            .within(self.pod_api.namespace().expect("pod API is namespaced"));
        if let Some(pod) = self.pods.get(&key) {
            return Ok(Some(pod));
        }
        // A newly created pod can precede its watch event. Bound the live GET
        // on a miss; never issue a request-path LIST.
        match tokio::time::timeout(Duration::from_secs(5), self.pod_api.get_opt(name.as_str()))
            .await
        {
            Ok(Ok(pod)) => Ok(pod.map(Arc::new)),
            Ok(Err(error)) => {
                tracing::warn!(%error, "pod authorization pod lookup failed");
                Err(Status::unavailable("pod_api_unavailable"))
            }
            Err(_) => Err(Status::unavailable("pod_api_unavailable")),
        }
    }

    pub(super) async fn check_pod_authorization(
        &self,
        caller: &K8sIdentity,
        peer: Option<IpAddr>,
        rpc: &str,
    ) -> Result<(), Status> {
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
        let node = match &self.credential_store {
            Some(store) => store
                .registered_node(caller.node().node_name(), caller.node().node_uid())
                .await
                .map_err(|error| {
                    tracing::warn!(%error, "pod authorization node lookup failed");
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
            ("node_identity", node.map(|_| ())),
            ("peer_ip", peer_check),
        ];
        let mut failure = None;
        for (check, result) in checks {
            let result_tag = match (&result, self.mode) {
                (Ok(()), _) => "allow",
                (Err(_), PodAuthorizationMode::Observe) => "would_deny",
                (Err(_), PodAuthorizationMode::Enforce) => "deny",
            };
            pplx_space_metrics::credmgr::agent_authz(check, result_tag, self.mode.as_str(), rpc);
            if let Err(error) = result {
                tracing::warn!(check, result = result_tag, mode = %self.mode, rpc,
                    reason = error.message(), pod_uid = %caller.pod().pod_uid(),
                    node_name = %caller.node().node_name(), peer_ip = ?peer,
                    "credential pod authorization check failed");
                // Infrastructure faults remain retryable even if another
                // independent check also denied the request.
                if failure.is_none() || error.code() == tonic::Code::Unavailable {
                    failure = Some(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
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
