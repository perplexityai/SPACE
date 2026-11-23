//! Pod addresses the shim reported with recent Kubernetes activations.
//!
//! The shim reads the pod's address from the pod network namespace before it
//! calls Start or Wake; kubelet publishes the same address in the Pod's status, but
//! only once its status sync reaches the pod. Reporting it in `SandboxInfo`
//! lets the control plane bind the execution without waiting for that sync.
//! The address only matters while a create waits for it, so entries expire
//! after [`POD_ADDRESS_TTL`] and do not survive a restart.

use std::{
    net::IpAddr,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use pplx_space_k8s_ids::PodUid;
use pplx_space_net_gateway_client::proto::SpaceApiRouteIdentity;
use pplx_space_sandbox_model::SandboxId;
use tonic::Status;

/// Longer than any create's wait for its pod address.
const POD_ADDRESS_TTL: Duration = Duration::from_secs(10 * 60);

/// A pod address an activation request reported, parsed at the wire.
pub(super) struct ReportedPodAddress {
    sandbox_id: SandboxId,
    pod_uid: PodUid,
    pod_ip: IpAddr,
}

impl ReportedPodAddress {
    /// The address of `netns_path` for the pod in `route_identity`, so it
    /// requires both. `operation` names the request in refusals.
    pub(super) fn parse(
        operation: &str,
        sandbox_id: &str,
        netns_path: Option<&str>,
        pod_ip: Option<&str>,
        route_identity: Option<&SpaceApiRouteIdentity>,
    ) -> Result<Option<Self>, Status> {
        let Some(pod_ip) = pod_ip else {
            return Ok(None);
        };
        let invalid = |field: &str, error: &dyn std::fmt::Display| {
            Status::invalid_argument(format!("{operation}.{field}: {error}"))
        };
        let pod_ip = pod_ip.parse().map_err(|error| invalid("pod_ip", &error))?;
        if netns_path.is_none_or(str::is_empty) {
            return Err(invalid("pod_ip", &"requires netns_path"));
        }
        let pod_uid = route_identity
            .and_then(|identity| identity.pod_uid.as_deref())
            .ok_or_else(|| invalid("pod_ip", &"requires route_identity.pod_uid"))?
            .parse()
            .map_err(|error| invalid("route_identity.pod_uid", &error))?;
        let sandbox_id = sandbox_id
            .parse()
            .map_err(|error| invalid("sandbox_id", &error))?;
        Ok(Some(Self {
            sandbox_id,
            pod_uid,
            pod_ip,
        }))
    }
}

struct PodAddress {
    pod_uid: PodUid,
    pod_ip: IpAddr,
    recorded_at: Instant,
}

/// At most one entry per sandbox on this node; `record` prunes expired ones.
#[derive(Default)]
pub(super) struct PodAddresses(DashMap<SandboxId, PodAddress>);

impl PodAddresses {
    /// Record an address once its activation succeeded.
    pub(super) fn record(&self, reported: ReportedPodAddress) {
        let ReportedPodAddress {
            sandbox_id,
            pod_uid,
            pod_ip,
        } = reported;
        let now = Instant::now();
        self.0
            .retain(|_, address| now.duration_since(address.recorded_at) < POD_ADDRESS_TTL);
        self.0.insert(
            sandbox_id,
            PodAddress {
                pod_uid,
                pod_ip,
                recorded_at: now,
            },
        );
    }

    /// The address reported with the activation of `pod_uid`'s pod, if it is the
    /// latest one recorded for `sandbox_id` and has not expired.
    /// An expired entry is removed unless a newer activation replaced it.
    pub(super) fn get(&self, sandbox_id: &SandboxId, pod_uid: &PodUid) -> Option<IpAddr> {
        let expired_at = {
            let address = self.0.get(sandbox_id)?;
            if address.recorded_at.elapsed() < POD_ADDRESS_TTL {
                return (address.pod_uid == *pod_uid).then_some(address.pod_ip);
            }
            address.recorded_at
        };
        self.0
            .remove_if(sandbox_id, |_, address| address.recorded_at == expired_at);
        None
    }
}
