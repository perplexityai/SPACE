use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE;
use pplx_space_node_mgr_client::proto::NetworkPoolStatusRequest;

use super::node_watch;
use crate::local_manager_request;

const NETWORK_SLOTS_FIELD_MANAGER: &str = "space-node-ctl-network-slots";

#[derive(Debug, thiserror::Error)]
pub enum NetworkSlotCapacityReconcileError {
    #[error("node-manager network pool status failed: {0}")]
    NodeManager(#[source] tonic::Status),
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch network slot capacity on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

/// Advertise the node-manager network-slot capacity on this Node's
/// `status.capacity` as the `space.pplx.net/network-slots` extended resource.
///
/// Capacity is sourced from node-manager's `network_pool_max_slots`. With
/// `memory_clamp`, it is clamped to the slots in use plus the sandboxes node
/// memory admission would still accept (see
/// [`desired_network_slot_capacity`]). The scheduler then refuses to place
/// more sandbox pods on the node than it has network slots (and, clamped,
/// memory) for, instead of letting startup fail later with
/// `ResourceExhausted`, which kubelet retries against the same node. The patch targets the
/// `/status` subresource via server-side apply with a dedicated field manager,
/// so it coexists with the kubelet-owned standard resources and persists across
/// kubelet status refreshes (which do not manage extended resources).
pub async fn reconcile_network_slot_capacity(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
    memory_clamp: bool,
) -> Result<(), NetworkSlotCapacityReconcileError> {
    let status = local_network_pool_status(memory_clamp).await?;
    let max_slots = status.max_slots;
    let capacity = desired_network_slot_capacity(
        status.max_slots,
        status.leased_slots,
        status.memory_admittable_sandboxes,
    );
    let desired = capacity.to_string();

    let current_metadata = node_cache.metadata().ok_or_else(|| {
        NetworkSlotCapacityReconcileError::NodeCacheNotReady {
            node_name: node_name.to_owned(),
        }
    })?;

    if network_slot_capacity_current(current_metadata.network_slots_capacity.as_deref(), &desired) {
        tracing::debug!(%node_name, max_slots, capacity, "network-slot capacity already current");
        return Ok(());
    }

    let nodes: Api<Node> = Api::all(kube.clone());
    nodes
        .patch_status(
            node_name,
            &network_slots_patch_params(),
            &Patch::Apply(network_slot_capacity_apply(node_name, &desired)),
        )
        .await
        .map_err(|source| NetworkSlotCapacityReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;

    tracing::info!(
        %node_name,
        max_slots,
        capacity,
        leased_slots = status.leased_slots,
        memory_admittable_sandboxes = ?status.memory_admittable_sandboxes,
        "network-slot capacity advertised on Node status"
    );
    Ok(())
}

/// The network-slot capacity to advertise: `max_slots`, or fewer when node
/// memory admission is the tighter limit. Kube-scheduler admits a pod while
/// the pods bound to the node request less than capacity, so capacity
/// `leased + admittable` leaves room for exactly the sandboxes memory
/// admission would accept, minus pods already bound but not yet admitted.
/// Capacity can drop below the requests of pods already bound here; kubelet
/// then fails a bound pod it has not yet admitted with
/// an `OutOf<resource>` reason, as memory admission would have
/// refused it. `None` (node memory unreadable) keeps the slot limit alone.
/// While memory is not the binding limit this returns `max_slots`, so the
/// Node is not re-patched on every memory fluctuation.
pub(super) fn desired_network_slot_capacity(
    max_slots: u32,
    leased_slots: u32,
    memory_admittable_sandboxes: Option<u32>,
) -> u32 {
    match memory_admittable_sandboxes {
        Some(admittable) => max_slots.min(leased_slots.saturating_add(admittable)),
        None => max_slots,
    }
}

struct LocalNetworkPoolStatus {
    max_slots: u32,
    leased_slots: u32,
    memory_admittable_sandboxes: Option<u32>,
}

async fn local_network_pool_status(
    include_memory_admission: bool,
) -> Result<LocalNetworkPoolStatus, NetworkSlotCapacityReconcileError> {
    let mut client = pplx_space_node_mgr_client::lazy();
    let response = client
        .get_network_pool_status(local_manager_request(
            NetworkPoolStatusRequest {
                include_memory_admission,
            },
            false,
        ))
        .await
        .map_err(NetworkSlotCapacityReconcileError::NodeManager)?
        .into_inner();
    Ok(LocalNetworkPoolStatus {
        max_slots: response.network_pool_max_slots,
        leased_slots: response.network_slots_leased,
        memory_admittable_sandboxes: response.memory_admittable_sandboxes,
    })
}

fn network_slot_capacity_current(current: Option<&str>, desired: &str) -> bool {
    current == Some(desired)
}

fn network_slot_capacity_apply(node_name: &str, desired: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
        },
        "status": {
            "capacity": {
                SANDBOX_NETWORK_SLOTS_RESOURCE: desired,
            }
        }
    })
}

fn network_slots_patch_params() -> PatchParams {
    PatchParams::apply(NETWORK_SLOTS_FIELD_MANAGER).force()
}

#[cfg(test)]
mod tests;
