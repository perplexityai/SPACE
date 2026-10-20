//! Proactive kube Node → SPACE DB inventory sync.
//!
//! In production, the `space-node-api-svc` DaemonSet registers each
//! worker with SPACE when it starts (via the gRPC `RegisterNode` call).
//! Environments that don't run that DaemonSet (kind/Tilt, direct-substrate
//! tests) never get that registration, so nodes only appear in the SPACE
//! UI after the first sandbox pod lands on them.
//!
//! `sync_labeled_nodes_to_db` fills that gap: it lists kube Nodes carrying the
//! sandbox selector and joins scheduled node-api pods to the watcher-backed
//! Node inventory for roles outside that selector. Missing-node reconciliation
//! compares stale ready rows with the full watcher-backed Node membership.
//! Idempotent —
//! re-running on a DaemonSet-managed cluster is safe and unchanged rows are
//! left alone. Called from the cluster-controller's `kube_node_sync`
//! reconciler.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use pplx_space_common_core::types::NodeRole;
use pplx_space_common_svc::types::k8s::space_k8s_node_id;

use super::{
    KubernetesSubstrate, NodeLifecycleState, NodeStateWrite, support::k8s_node_internal_ip,
};

/// Label key that identifies a SPACE node role. Presence is optional; the
/// default role is `"worker"`. Expected values: `"worker"`, `"builder"`, `"cluster"`.
const NODE_ROLE_LABEL: &str = "space.pplx.net/node-role";

#[derive(Debug, thiserror::Error)]
pub enum SyncNodeInventoryError {
    #[error("kube list nodes failed: {0}")]
    Kube(String),
    #[error("DB upsert failed for node {name}: {source}")]
    Db {
        name: String,
        #[source]
        source: crate::controllers::substrate::SubstrateError,
    },
    #[error("DB absence reconciliation failed: {0}")]
    ReconcileMissing(#[source] crate::controllers::substrate::SubstrateError),
}

/// Upsert kube Nodes selected for sandboxes or carrying a node-api pod into
/// the SPACE `nodes` + `node_k8s` tables. Then use the full watcher-backed Node
/// membership to demote stale ready rows that are absent from Kubernetes.
///
/// Returns the list of node names whose DB inventory changed. Nodes without an
/// `InternalIP` address are skipped with a warning.
pub(super) async fn sync_labeled_nodes_to_db(
    substrate: &KubernetesSubstrate,
    missing_grace: Duration,
    missing_max_per_tick: u32,
) -> Result<Vec<String>, SyncNodeInventoryError> {
    use kube::Api;
    use kube::api::ListParams;

    let selector_key = &substrate.k8s.sandbox_pod_config.sandbox_node_selector_key;
    let selector_val = &substrate.k8s.sandbox_pod_config.sandbox_node_selector_value;
    let label_selector = format!("{selector_key}={selector_val}");

    let nodes_api: Api<k8s_openapi::api::core::v1::Node> = Api::all(substrate.k8s.client.clone());
    // Keep this selector-bounded LIST as the credible sandbox membership used
    // by missing-node reconciliation; the shared reflector supplies the
    // node-api roles that cannot be expressed by that selector.
    let lp = ListParams::default().labels(&label_selector);
    let node_list = nodes_api
        .list(&lp)
        .await
        .map_err(|err| SyncNodeInventoryError::Kube(err.to_string()))?;
    let observed_node_names = node_list
        .items
        .iter()
        .filter_map(|node| node.metadata.name.clone())
        .collect::<Vec<_>>();

    let mut changed = Vec::new();
    for (node, role) in inventory_nodes(node_list.items, &substrate.k8s.caches) {
        let name = match node.metadata.name.as_deref() {
            Some(n) => n,
            None => continue,
        };
        let uid = match node.metadata.uid.as_deref() {
            Some(u) => u,
            None => continue,
        };
        let address = match k8s_node_internal_ip(&node) {
            Some(a) => a,
            None => {
                tracing::warn!(node = %name, "kube_node_sync: skipping node with no InternalIP");
                continue;
            }
        };

        let node_id = space_k8s_node_id(name, role.as_str());
        let ready = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_deref())
            .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"))
            .map(|c| c.status == "True")
            .unwrap_or(false);
        // `kube_node_sync` is the lifecycle owner: it derives state from
        // Kubernetes Node readiness and manages it on every tick.
        let state = NodeStateWrite::Manage(NodeLifecycleState::from_ready(ready));

        let inventory_changed = match substrate
            .store
            .persist_k8s_node_inventory(name, uid, node_id, role, Some(address.as_str()), state)
            .await
        {
            Ok(changed) => changed,
            Err(crate::controllers::substrate::SubstrateError::NodeTombstoned { message }) => {
                tracing::info!(
                    node = %name,
                    role = %role,
                    reason = %message,
                    "kube_node_sync: skipping tombstoned node"
                );
                continue;
            }
            Err(source) => {
                return Err(SyncNodeInventoryError::Db {
                    name: name.to_string(),
                    source,
                });
            }
        };

        if inventory_changed {
            changed.push(name.to_string());
        }
    }

    let Some(demotion_node_names) =
        demotion_membership_snapshot(&observed_node_names, substrate.live_node_names())
    else {
        tracing::warn!(
            label_selector,
            selected_nodes = observed_node_names.len(),
            "kube_node_sync: skipping missing-node reconciliation for unavailable membership"
        );
        return Ok(changed);
    };

    let demoted = substrate
        .store
        .demote_unobserved_k8s_nodes(&demotion_node_names, missing_grace, missing_max_per_tick)
        .await
        .map_err(SyncNodeInventoryError::ReconcileMissing)?;
    if !demoted.is_empty() {
        tracing::info!(
            nodes = ?demoted,
            missing_grace_secs = missing_grace.as_secs(),
            "kube_node_sync: demoted stale nodes absent from Kubernetes"
        );
        changed.extend(demoted);
    }
    changed.sort_unstable();
    changed.dedup();
    Ok(changed)
}

fn inventory_nodes(
    sandbox_nodes: Vec<k8s_openapi::api::core::v1::Node>,
    caches: &super::K8sCaches,
) -> Vec<(k8s_openapi::api::core::v1::Node, NodeRole)> {
    let mut nodes = BTreeMap::new();
    for node in sandbox_nodes {
        let Some(name) = node.metadata.name.clone() else {
            continue;
        };
        let Some(role) = node_role_from_labels(&name, node.metadata.labels.as_ref()) else {
            continue;
        };
        nodes.insert((name, role), node);
    }

    for pod in caches.node_api_pods.state() {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(name) = pod.spec.as_ref().and_then(|spec| spec.node_name.as_deref()) else {
            continue;
        };
        let Some(role) = super::caches::pod_app_name(&pod)
            .and_then(super::caches::role_from_app_name)
            .and_then(|role| NodeRole::parse(role).ok())
        else {
            continue;
        };
        let Some(node) = caches.node_by_name(name) else {
            continue;
        };
        nodes
            .entry((name.to_string(), role))
            .or_insert((*node).clone());
    }

    nodes
        .into_iter()
        .map(|((_, role), node)| (node, role))
        .collect()
}

fn demotion_membership_snapshot(
    observed_node_names: &[String],
    live_node_names: Option<HashSet<String>>,
) -> Option<Vec<String>> {
    if observed_node_names.is_empty() {
        return None;
    }
    let mut live_node_names = live_node_names?;
    if live_node_names.is_empty() {
        return None;
    }
    let mut snapshot = live_node_names.drain().collect::<Vec<_>>();
    snapshot.sort_unstable();
    Some(snapshot)
}

fn node_role_from_labels(
    node_name: &str,
    labels: Option<&BTreeMap<String, String>>,
) -> Option<NodeRole> {
    let Some(raw_role) = labels.and_then(|labels| labels.get(NODE_ROLE_LABEL)) else {
        return Some(NodeRole::Worker);
    };
    match NodeRole::parse(raw_role) {
        Ok(role) => Some(role),
        Err(err) => {
            tracing::warn!(
                node = %node_name,
                role = %raw_role,
                %err,
                "kube_node_sync: skipping node with invalid role label"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests;
