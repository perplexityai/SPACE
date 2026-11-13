//! This node's environment labels: one per environment its latest complete
//! execution inventory lists as usable (`environment_label_key` in
//! common-core). With the environment placement gate on, a sandbox pod bound
//! to an environment requires its label, so these labels are what lets
//! kube-scheduler place bound work.
//!
//! The labels follow node-manager's own report, the same observation node-ctl
//! publishes to the cluster:
//!
//! * a complete inventory sets exactly its usable environments. One without
//!   an observation (a node-manager that predates them) or with one that
//!   fails to decode sets none, as the cluster records no evidence for it;
//! * an unavailable inventory changes nothing, so a node-manager restart
//!   doesn't strip the node until its next report.
//!
//! They're written by server-side apply under their own field manager, so
//! the apply removes a label this manager set before and no longer sends,
//! and never touches the template-presence labels node-ctl applies under its
//! other manager.

use std::collections::BTreeSet;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_execution_common::environment::EnvironmentObservation;
use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_sandbox_pod::{
    ENVIRONMENT_USABLE_LABEL_VALUE, environment_label_key, is_environment_label_key,
};

use super::node_watch;
use crate::facets::execution_inventory::CollectedExecutionInventory;

const ENVIRONMENT_LABELS_FIELD_MANAGER: &str = "space-node-ctl-environments";

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentLabelsReconcileError {
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch environment labels on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

pub async fn reconcile_environment_labels(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
) -> Result<(), EnvironmentLabelsReconcileError> {
    let Some(inventory) = read_inventory().await else {
        tracing::debug!(%node_name, "execution inventory unavailable; environment labels unchanged");
        return Ok(());
    };
    let desired = desired_environment_labels(&inventory);
    let current_metadata = node_cache.metadata().ok_or_else(|| {
        EnvironmentLabelsReconcileError::NodeCacheNotReady {
            node_name: node_name.to_owned(),
        }
    })?;
    if environment_labels_current(&current_metadata, &desired) {
        tracing::debug!(%node_name, "environment labels already current");
        return Ok(());
    }
    let nodes: Api<Node> = Api::all(kube.clone());
    nodes
        .patch(
            node_name,
            &PatchParams::apply(ENVIRONMENT_LABELS_FIELD_MANAGER).force(),
            &Patch::Apply(environment_labels_apply(node_name, &desired)),
        )
        .await
        .map_err(|source| EnvironmentLabelsReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;
    tracing::info!(%node_name, environments = desired.len(), "environment labels reconciled");
    Ok(())
}

/// The node-status facet's bounded read
/// (`facets::execution_inventory::collect`): `None` when it's unavailable.
async fn read_inventory() -> Option<NodeExecutionInventory> {
    match crate::facets::execution_inventory::collect(None).await {
        Some(CollectedExecutionInventory::Complete(inventory)) => Some(inventory),
        Some(CollectedExecutionInventory::Unavailable) | None => None,
    }
}

/// The label keys `inventory` calls for: its observation's usable
/// environments, or none when it has no valid observation.
fn desired_environment_labels(inventory: &NodeExecutionInventory) -> BTreeSet<String> {
    let Some(observation) = inventory.environments.as_ref() else {
        return BTreeSet::new();
    };
    match EnvironmentObservation::try_from(observation) {
        Ok(observation) => observation
            .usable()
            .iter()
            .map(environment_label_key)
            .collect(),
        Err(error) => {
            tracing::warn!(%error, "environment labels: observation doesn't decode; removing them");
            BTreeSet::new()
        }
    }
}

fn environment_labels_current(
    current: &node_watch::NodeMetadata,
    desired: &BTreeSet<String>,
) -> bool {
    let present = desired.iter().all(|key| {
        current.labels.get(key).map(String::as_str) == Some(ENVIRONMENT_USABLE_LABEL_VALUE)
    });
    let no_stale = current
        .labels
        .keys()
        .filter(|key| is_environment_label_key(key))
        .all(|key| desired.contains(key));
    present && no_stale
}

fn environment_labels_apply(node_name: &str, desired: &BTreeSet<String>) -> serde_json::Value {
    let labels: serde_json::Map<String, serde_json::Value> = desired
        .iter()
        .map(|key| (key.clone(), ENVIRONMENT_USABLE_LABEL_VALUE.into()))
        .collect();
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "labels": labels,
        }
    })
}

#[cfg(test)]
mod tests;
