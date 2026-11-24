//! Addresses the node manager read from an activation's externally created
//! network namespace when it allocated the activation's network.
//!
//! Reporting the address in `SandboxInfo` lets the control plane bind the
//! execution without waiting for whoever created the namespace to publish it.
//! The address only matters while a create waits for it, so entries expire
//! after [`EXTERNAL_ADDRESS_TTL`] and do not survive a restart.

use std::{
    net::IpAddr,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use pplx_space_k8s_ids::PodUid;
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId, SpaceApiRouteIdentity};

/// Longer than any create's wait for its address.
const EXTERNAL_ADDRESS_TTL: Duration = Duration::from_secs(10 * 60);

struct ExternalAddress {
    execution_generation: ExecutionGeneration,
    // TODO: fence on an opaque instance ID once the route identity's
    // `pod_uid` is renamed to one.
    pod_uid: PodUid,
    address: IpAddr,
    recorded_at: Instant,
}

/// At most one entry per sandbox on this node; `record` prunes expired ones.
#[derive(Default)]
pub(super) struct ExternalAddresses(DashMap<SandboxId, ExternalAddress>);

impl ExternalAddresses {
    /// Record the address the node manager allocated for the execution
    /// `route_identity` names. An activation without an execution generation
    /// and instance records nothing; one whose address is missing or does not
    /// parse clears the sandbox's entry.
    pub(super) fn record(
        &self,
        sandbox_id: &str,
        route_identity: &SpaceApiRouteIdentity,
        address: Option<&str>,
    ) {
        let (Some(execution_generation), Some(pod_uid), Ok(sandbox_id)) = (
            route_identity.execution_generation,
            route_identity.pod_uid.clone(),
            sandbox_id.parse::<SandboxId>(),
        ) else {
            return;
        };
        let Some(Ok(address)) = address.map(str::parse::<IpAddr>) else {
            if let Some(address) = address {
                tracing::warn!(%sandbox_id, address, "ignoring unparseable external address");
            }
            self.0.remove(&sandbox_id);
            return;
        };
        let now = Instant::now();
        self.0
            .retain(|_, address| now.duration_since(address.recorded_at) < EXTERNAL_ADDRESS_TTL);
        self.0.insert(
            sandbox_id,
            ExternalAddress {
                execution_generation,
                pod_uid,
                address,
                recorded_at: now,
            },
        );
    }

    /// The address recorded for exactly the execution `route_identity` names,
    /// if it is the latest one recorded for `sandbox_id` and has not expired.
    /// An expired entry is removed unless a newer activation replaced it.
    pub(super) fn get(
        &self,
        sandbox_id: &SandboxId,
        route_identity: &SpaceApiRouteIdentity,
    ) -> Option<IpAddr> {
        let expired_at = {
            let address = self.0.get(sandbox_id)?;
            if address.recorded_at.elapsed() < EXTERNAL_ADDRESS_TTL {
                let current = route_identity.execution_generation
                    == Some(address.execution_generation)
                    && route_identity.pod_uid.as_ref() == Some(&address.pod_uid);
                return current.then_some(address.address);
            }
            address.recorded_at
        };
        self.0
            .remove_if(sandbox_id, |_, address| address.recorded_at == expired_at);
        None
    }
}
