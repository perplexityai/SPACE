//! Live verification for RPCs whose authority belongs to one worker incarnation.

use super::KubernetesPodIdentity;
use tonic::{Request, Status};

/// Required verification waits for capacity; advisory verification never queues
/// and has a short independent budget. Neither infers identity on failure.
#[derive(Clone, Copy)]
pub enum NodeIdentityVerification {
    Required,
    Advisory,
}

/// Request-scoped live verification. The service injects its Kubernetes client;
/// checks never initialize clients or consult a process-global cache. Every
/// positive verdict is from this request's bounded live GET.
#[derive(Clone)]
#[cfg_attr(not(feature = "k8s"), derive(Default))]
pub struct NodeIdentityVerifier {
    #[cfg(feature = "k8s")]
    client: Option<kube::Client>,
    #[cfg(feature = "k8s")]
    checks: std::sync::Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "k8s")]
impl Default for NodeIdentityVerifier {
    fn default() -> Self {
        Self {
            #[cfg(feature = "k8s")]
            client: None,
            #[cfg(feature = "k8s")]
            checks: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
        }
    }
}

impl NodeIdentityVerifier {
    #[cfg(feature = "k8s")]
    pub fn new(client: kube::Client) -> Self {
        Self {
            client: Some(client),
            checks: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
        }
    }

    pub async fn verify<T>(
        &self,
        request: &Request<T>,
        verification: NodeIdentityVerification,
    ) -> Result<KubernetesPodIdentity, Status> {
        let identity = request
            .extensions()
            .get::<super::NodeApiAuthenticatedCaller>()
            .and_then(|caller| caller.kubernetes_pod())
            .cloned()
            .ok_or_else(|| {
                Status::permission_denied("a complete Kubernetes pod-bound identity is required")
            })?;
        let peer = request
            .remote_addr()
            .map(|address| address.ip())
            .ok_or_else(|| Status::permission_denied("node-bound caller has no peer address"))?;
        self.verify_identity(identity, peer, verification).await
    }

    #[cfg(feature = "k8s")]
    pub async fn verify_identity(
        &self,
        identity: KubernetesPodIdentity,
        peer: std::net::IpAddr,
        verification: NodeIdentityVerification,
    ) -> Result<KubernetesPodIdentity, Status> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Status::unavailable("Kubernetes identity client unavailable"))?;
        with_verification_budget(&self.checks, verification, async {
            let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
                kube::Api::namespaced(client.clone(), identity.namespace());
            let pod =
                pods.get(identity.pod_name().as_str())
                    .await
                    .map_err(|error| match error {
                        kube::Error::Api(response) if response.code == 404 => {
                            Status::permission_denied("caller pod no longer exists")
                        }
                        _ => Status::unavailable("caller pod could not be verified"),
                    })?;
            verify_observed_pod(&identity, &pod, peer)?;
            Ok(identity)
        })
        .await
    }

    #[cfg(not(feature = "k8s"))]
    async fn verify_identity(
        &self,
        _identity: KubernetesPodIdentity,
        _peer: std::net::IpAddr,
        _verification: NodeIdentityVerification,
    ) -> Result<KubernetesPodIdentity, Status> {
        Err(Status::unavailable(
            "Kubernetes identity verification is not compiled in",
        ))
    }
}

/// TokenReview extras require exactly one nonempty value. Shared with credential
/// authentication; accepting the first of several bindings would be ambiguous.
pub fn single_extra_value<'a>(
    extra: Option<&'a std::collections::BTreeMap<String, Vec<String>>>,
    key: &str,
) -> Option<&'a str> {
    match extra?.get(key)?.as_slice() {
        [value] if !value.is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(feature = "k8s")]
async fn with_verification_budget<T>(
    checks: &tokio::sync::Semaphore,
    verification: NodeIdentityVerification,
    check: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    use std::time::Duration;
    let budget = match verification {
        NodeIdentityVerification::Required => Duration::from_secs(5),
        NodeIdentityVerification::Advisory => Duration::from_millis(50),
    };
    tokio::time::timeout(budget, async {
        let _permit = match verification {
            NodeIdentityVerification::Required => checks
                .acquire()
                .await
                .map_err(|_| Status::unavailable("identity verifier closed"))?,
            NodeIdentityVerification::Advisory => checks
                .try_acquire()
                .map_err(|_| Status::unavailable("advisory identity verifier is busy"))?,
        };
        check.await
    })
    .await
    .map_err(|_| Status::unavailable("caller pod verification timed out"))?
}

#[cfg(feature = "k8s")]
fn verify_observed_pod(
    identity: &KubernetesPodIdentity,
    pod: &k8s_openapi::api::core::v1::Pod,
    peer: std::net::IpAddr,
) -> Result<(), Status> {
    let spec = pod
        .spec
        .as_ref()
        .ok_or_else(|| Status::permission_denied("caller pod has no spec"))?;
    check_pod_uid(pod, identity.pod_uid())?;
    check_pod_running(pod)?;
    check_peer_ip(pod, Some(peer))?;
    if spec.node_name.as_deref() != Some(identity.node_name().as_str())
        || spec.service_account_name.as_deref() != Some(identity.service_account())
    {
        return Err(Status::permission_denied(
            "caller pod node or service account changed",
        ));
    }
    if pod.metadata.deletion_timestamp.is_some() {
        // A rolling restart is not an ownership violation. The durable worker
        // must retry on the successor instead of terminalizing its upload.
        return Err(Status::unavailable(
            "caller daemon is draining; retry on successor",
        ));
    }
    Ok(())
}

#[cfg(all(test, feature = "k8s"))]
mod tests;

#[cfg(feature = "k8s")]
use k8s_openapi::api::core::v1::Pod;
#[cfg(feature = "k8s")]
use pplx_space_common_core::types::PodUid;
#[cfg(feature = "k8s")]
use std::net::IpAddr;

#[cfg(feature = "k8s")]
pub fn check_pod_uid(pod: &Pod, expected: &PodUid) -> Result<(), Status> {
    if pod.metadata.uid.as_deref() != Some(expected.as_str()) {
        return Err(Status::permission_denied("pod_uid_mismatch"));
    }
    Ok(())
}

#[cfg(feature = "k8s")]
pub fn check_pod_running(pod: &Pod) -> Result<(), Status> {
    if pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        != Some("Running")
    {
        return Err(Status::permission_denied("pod_not_running"));
    }
    Ok(())
}

#[cfg(feature = "k8s")]
pub fn check_peer_ip(pod: &Pod, peer: Option<IpAddr>) -> Result<(), Status> {
    let peer = peer
        .map(normalize_ip)
        .ok_or_else(|| Status::permission_denied("peer_address_missing"))?;
    let matches = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ips.as_ref())
        .is_some_and(|ips| {
            ips.iter().any(|ip| {
                ip.ip
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| normalize_ip(ip) == peer)
            })
        });
    if !matches {
        return Err(Status::permission_denied("peer_ip_mismatch"));
    }
    Ok(())
}

#[cfg(feature = "k8s")]
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}
