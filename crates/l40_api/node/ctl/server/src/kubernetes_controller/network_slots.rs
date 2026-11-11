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
/// Capacity is sourced from node-manager's `network_pool_max_slots`. The
/// scheduler then refuses to place more sandbox pods on the node than it has
/// network slots, instead of letting startup fail later with
/// `ResourceExhausted` / `no idle network slots`. The patch targets the
/// `/status` subresource via server-side apply with a dedicated field manager,
/// so it coexists with the kubelet-owned standard resources and persists across
/// kubelet status refreshes (which do not manage extended resources).
pub async fn reconcile_network_slot_capacity(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
) -> Result<(), NetworkSlotCapacityReconcileError> {
    let max_slots = local_network_pool_max_slots().await?;
    let desired = max_slots.to_string();

    let current_metadata = node_cache.metadata().ok_or_else(|| {
        NetworkSlotCapacityReconcileError::NodeCacheNotReady {
            node_name: node_name.to_owned(),
        }
    })?;

    if network_slot_capacity_current(current_metadata.network_slots_capacity.as_deref(), &desired) {
        tracing::debug!(%node_name, max_slots, "network-slot capacity already current");
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

    tracing::info!(%node_name, max_slots, "network-slot capacity advertised on Node status");
    Ok(())
}

async fn local_network_pool_max_slots() -> Result<u32, NetworkSlotCapacityReconcileError> {
    let mut client = pplx_space_node_mgr_client::lazy();
    let response = client
        .get_network_pool_status(local_manager_request(NetworkPoolStatusRequest {}, false))
        .await
        .map_err(NetworkSlotCapacityReconcileError::NodeManager)?
        .into_inner();
    Ok(response.network_pool_max_slots)
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
