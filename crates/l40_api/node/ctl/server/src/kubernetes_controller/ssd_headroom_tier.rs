use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_common_core::kubernetes::{
    SSD_HEADROOM_TIER_LABEL, SSD_PLACEMENT_BLOCKED_LABEL, SSD_PLACEMENT_BLOCKED_VALUE,
    SsdHeadroomTier,
};
use pplx_space_node_mgr_client::proto::{CacheDiskStatus, NodeResourceStatusRequest};
use pplx_space_node_mgr_client::{CacheDiskSchedulingEffect, NodeResourceStatusReader};

use super::node_watch;
use crate::local_manager_request;

const SSD_HEADROOM_TIER_FIELD_MANAGER: &str = "space-node-ctl-ssd-headroom-tier";

#[derive(Debug, thiserror::Error)]
pub enum SsdHeadroomTierReconcileError {
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch SSD placement labels on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

/// Value written to [`SSD_PLACEMENT_BLOCKED_LABEL`] when the node is below the
/// hard-block threshold. Explicitly `"false"` rather than absent so the field
/// manager keeps ownership and the node's state is observable at a glance.
const SSD_PLACEMENT_UNBLOCKED_VALUE: &str = "false";

/// Advertise the node-manager `/space/cache` resource signal on the node:
/// the soft usage-keyed headroom tier (`space.pplx.net/ssd-headroom-tier`) that
/// biases sandbox placement away from filling nodes, plus the hard placement
/// block (`space.pplx.net/ssd-placement-blocked`) that removes a filling node
/// from the schedulable set before its btrfs cache can wedge at 100%.
///
/// Node-manager is the only process that probes statvfs/btrfs and owns
/// reservation-aware admission. Node-ctl only publishes that status to
/// Kubernetes. Both labels are written through one server-side apply with a
/// dedicated field manager, and only when either label actually changed.
pub async fn reconcile_ssd_headroom_tier(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
    resource_status_reader: &NodeResourceStatusReader,
) -> Result<(), SsdHeadroomTierReconcileError> {
    let cache_disk = match cache_disk_status_from_node_manager(resource_status_reader).await {
        Ok((cache_disk, fallback_error)) => {
            if let Some(error) = fallback_error {
                tracing::warn!(
                    %node_name,
                    %error,
                    "node-manager cache disk status unavailable; reusing recent status"
                );
            }
            Some(cache_disk)
        }
        Err(error) => {
            tracing::warn!(
                %node_name,
                %error,
                "node-manager cache disk status unavailable; advertising last-resort ssd-headroom and blocking placement"
            );
            None
        }
    };
    let tier = desired_tier(cache_disk.as_ref());
    let blocked = desired_block(cache_disk.as_ref());
    if let Some(cache_disk) = cache_disk.as_ref() {
        pplx_space_metrics::node_disk::ssd_free_fraction(statvfs_free_fraction(cache_disk));
    }
    pplx_space_metrics::node_disk::ssd_headroom_tier(tier.as_u8());

    let current_metadata =
        node_cache
            .metadata()
            .ok_or_else(|| SsdHeadroomTierReconcileError::NodeCacheNotReady {
                node_name: node_name.to_owned(),
            })?;
    if placement_labels_current(&current_metadata, tier, blocked) {
        tracing::debug!(%node_name, tier = %tier, blocked, "ssd placement labels already current");
        return Ok(());
    }

    let nodes: Api<Node> = Api::all(kube.clone());
    nodes
        .patch(
            node_name,
            &ssd_headroom_tier_patch_params(),
            &Patch::Apply(ssd_placement_apply(node_name, tier, blocked)),
        )
        .await
        .map_err(|source| SsdHeadroomTierReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;

    tracing::info!(
        %node_name,
        tier = %tier,
        blocked,
        free_fraction = ?cache_disk.as_ref().map(statvfs_free_fraction),
        "ssd placement labels reconciled"
    );
    Ok(())
}

async fn cache_disk_status_from_node_manager(
    resource_status_reader: &NodeResourceStatusReader,
) -> Result<
    (
        CacheDiskStatus,
        Option<pplx_space_node_mgr_client::NodeResourceStatusReadError>,
    ),
    pplx_space_node_mgr_client::NodeResourceStatusReadError,
> {
    let status = resource_status_reader
        .read(
            pplx_space_node_mgr_client::lazy(),
            local_manager_request(NodeResourceStatusRequest {}, false),
        )
        .await?;
    let cache_disk = pplx_space_node_mgr_client::cache_disk_status(&status.response)?.clone();
    Ok((cache_disk, status.fallback_error().cloned()))
}

/// The soft headroom tier for this node's `/space/cache` occupancy. After the
/// recent-status grace window expires, missing node-manager resource status
/// advertises the last-resort tier rather than retaining a stale high tier that
/// would keep steering placements toward a node whose disk we can't see.
fn desired_tier(cache_disk: Option<&CacheDiskStatus>) -> SsdHeadroomTier {
    cache_disk.map_or(SsdHeadroomTier::ZERO, |status| {
        SsdHeadroomTier::from_free_fraction(statvfs_free_fraction(status))
    })
}

fn statvfs_free_fraction(status: &CacheDiskStatus) -> f64 {
    if status.statvfs_total_bytes == 0 {
        return 0.0;
    }
    (status.statvfs_free_bytes as f64 / status.statvfs_total_bytes as f64).clamp(0.0, 1.0)
}

/// Whether new sandbox placement must be hard-blocked on this node. Warning is
/// included so the cluster stops scheduling before node-manager's hard reject.
/// `None` is used only when there is no recent node-manager status to reuse.
fn desired_block(cache_disk: Option<&CacheDiskStatus>) -> bool {
    matches!(
        cache_disk.map(CacheDiskStatus::scheduling_effect),
        Some(
            CacheDiskSchedulingEffect::WarnOnly
                | CacheDiskSchedulingEffect::Full
                | CacheDiskSchedulingEffect::Unavailable
        ) | None
    )
}

fn placement_block_label_value(blocked: bool) -> &'static str {
    if blocked {
        SSD_PLACEMENT_BLOCKED_VALUE
    } else {
        SSD_PLACEMENT_UNBLOCKED_VALUE
    }
}

fn placement_labels_current(
    current_metadata: &node_watch::NodeMetadata,
    tier: SsdHeadroomTier,
    blocked: bool,
) -> bool {
    let tier_current = current_metadata
        .labels
        .get(SSD_HEADROOM_TIER_LABEL)
        .map(String::as_str)
        == Some(tier.to_string().as_str());
    let block_current = current_metadata
        .labels
        .get(SSD_PLACEMENT_BLOCKED_LABEL)
        .map(String::as_str)
        == Some(placement_block_label_value(blocked));
    tier_current && block_current
}

fn ssd_placement_apply(node_name: &str, tier: SsdHeadroomTier, blocked: bool) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "labels": {
                SSD_HEADROOM_TIER_LABEL: tier.to_string(),
                SSD_PLACEMENT_BLOCKED_LABEL: placement_block_label_value(blocked),
            },
        }
    })
}

fn ssd_headroom_tier_patch_params() -> PatchParams {
    PatchParams::apply(SSD_HEADROOM_TIER_FIELD_MANAGER).force()
}

#[cfg(test)]
mod tests;
