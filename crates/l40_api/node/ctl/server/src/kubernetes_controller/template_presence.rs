use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_common_core::kubernetes::{
    TEMPLATE_PRESENT_LABEL_VALUE, is_template_presence_metadata_key,
};

use super::{node_watch, template_manager};

const TEMPLATE_PRESENCE_FIELD_MANAGER: &str = "space-node-ctl";

#[derive(Debug, thiserror::Error)]
pub enum TemplatePresenceReconcileError {
    #[error("failed to list local templates: {0}")]
    ListTemplates(#[source] tonic::Status),
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch template presence metadata on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

pub async fn reconcile_template_presence(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    tmpl: &mut template_manager::Client,
    node_name: &str,
) -> Result<(), TemplatePresenceReconcileError> {
    let desired_presence = tmpl
        .local_template_presence()
        .await
        .map_err(TemplatePresenceReconcileError::ListTemplates)?;
    let nodes: Api<Node> = Api::all(kube.clone());
    let current_metadata =
        node_cache
            .metadata()
            .ok_or_else(|| TemplatePresenceReconcileError::NodeCacheNotReady {
                node_name: node_name.to_owned(),
            })?;

    if template_presence_current(&current_metadata, &desired_presence) {
        tracing::debug!(%node_name, "template node metadata already current");
        return Ok(());
    }

    let apply = template_presence_apply(node_name, &desired_presence);

    nodes
        .patch(
            node_name,
            &template_presence_patch_params(),
            &Patch::Apply(apply),
        )
        .await
        .map_err(|source| TemplatePresenceReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;

    tracing::info!(%node_name, templates = desired_presence.len(), "template node metadata reconciled");
    Ok(())
}

fn template_presence_current(
    current_metadata: &node_watch::NodeMetadata,
    desired_presence: &BTreeMap<String, String>,
) -> bool {
    let desired_metadata_present = desired_presence.iter().all(|(label, content_hash)| {
        current_metadata.labels.get(label).map(String::as_str) == Some(TEMPLATE_PRESENT_LABEL_VALUE)
            && current_metadata.annotations.get(label).map(String::as_str) == Some(content_hash)
    });

    let no_stale_labels = current_metadata
        .labels
        .keys()
        .filter(|key| is_template_presence_metadata_key(key))
        .all(|key| desired_presence.contains_key(key));

    desired_metadata_present && no_stale_labels
}

fn template_presence_apply(
    node_name: &str,
    desired_presence: &BTreeMap<String, String>,
) -> serde_json::Value {
    let labels: BTreeMap<_, _> = desired_presence
        .keys()
        .map(|label| (label.clone(), TEMPLATE_PRESENT_LABEL_VALUE))
        .collect();
    // Keep the full content hash visible for cluster admins and controllers;
    // the scheduling label only carries a compact, DNS-safe prefix.
    let annotations = desired_presence.clone();

    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "labels": labels,
            "annotations": annotations,
        }
    })
}

fn template_presence_patch_params() -> PatchParams {
    PatchParams::apply(TEMPLATE_PRESENCE_FIELD_MANAGER).force()
}

#[cfg(test)]
mod tests;
