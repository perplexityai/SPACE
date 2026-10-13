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

/// Validate the daemon pod behind the reviewed token on every sensitive RPC,
/// including TokenReview cache hits. The caller must additionally match this
/// identity against its locked node/attempt records. No request metadata is
/// accepted as a substitute for the authentication extension.
///
/// Kubernetes I/O, including client initialization, has a five-second budget;
/// at most eight checks execute concurrently per process. Unavailable identity
/// infrastructure never grants authority.
#[cfg(feature = "k8s")]
pub async fn verify_node_bound_caller<T>(
    request: &Request<T>,
    verification: NodeIdentityVerification,
) -> Result<KubernetesPodIdentity, Status> {
    static CLIENT: tokio::sync::OnceCell<kube::Client> = tokio::sync::OnceCell::const_new();
    static CHECKS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
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
    with_verification_budget(&CHECKS, verification, async {
        let client = CLIENT
            .get_or_try_init(|| async {
                crate::kube_client::traced_kube_client_from_env()
                    .await
                    .map_err(|_| Status::unavailable("Kubernetes identity client unavailable"))
            })
            .await?;
        let pods: kube::Api<k8s_openapi::api::core::v1::Pod> =
            kube::Api::namespaced(client.clone(), identity.namespace());
        let pod = pods
            .get(identity.pod_name().as_str())
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
pub async fn verify_node_bound_caller<T>(
    _request: &Request<T>,
    _verification: NodeIdentityVerification,
) -> Result<KubernetesPodIdentity, Status> {
    Err(Status::failed_precondition(
        "Kubernetes identity verification is not compiled in",
    ))
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
    let status = pod
        .status
        .as_ref()
        .ok_or_else(|| Status::permission_denied("caller pod has no status"))?;
    let normalize = |ip: std::net::IpAddr| match ip {
        std::net::IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(ip)),
        ip => ip,
    };
    if pod.metadata.uid.as_deref() != Some(identity.pod_uid().as_str())
        || spec.node_name.as_deref() != Some(identity.node_name().as_str())
        || spec.service_account_name.as_deref() != Some(identity.service_account())
        || status.phase.as_deref() != Some("Running")
        || !status.pod_ips.as_ref().is_some_and(|ips| {
            ips.iter().any(|ip| {
                ip.ip
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| normalize(ip) == normalize(peer))
            })
        })
    {
        return Err(Status::permission_denied(
            "caller pod identity, state, or peer address changed",
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
