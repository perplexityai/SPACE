use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_common_core::kubernetes::{
    is_template_write_annotation_key, parse_template_write_annotation,
};
use pplx_space_common_core::types::{DEFAULT_TEMPLATE_TAG, TemplateReference, TemplateSelector};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{TEMPLATE_MANAGER_RPC_TIMEOUT, node_watch, template_manager};

pub struct TemplatePullRequestReconciler {
    pull_rpc_timeout: Duration,
    shutdown: CancellationToken,
    in_flight: BTreeMap<String, InFlightTemplateWrite>,
    last_malformed_warnings: BTreeMap<String, String>,
}

struct InFlightTemplateWrite {
    request: TemplateWriteTaskRequest,
    handle: JoinHandle<Result<template_manager::EnsuredTemplate, TemplateWriteTaskError>>,
}

#[derive(Debug, thiserror::Error)]
enum TemplateWriteTaskError {
    #[error("template pull request cancelled")]
    Cancelled,
    #[error("template-manager request failed: {0}")]
    TemplateManager(#[from] tonic::Status),
}

#[derive(Debug, thiserror::Error)]
pub enum TemplatePullReconcileError {
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
}

#[derive(Debug, thiserror::Error)]
enum ClearTemplateAnnotationsError {
    #[error("failed to clear template write annotations on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

impl TemplatePullRequestReconciler {
    pub fn new(pull_rpc_timeout: Duration, shutdown: CancellationToken) -> Self {
        Self {
            pull_rpc_timeout,
            shutdown,
            in_flight: BTreeMap::new(),
            last_malformed_warnings: BTreeMap::new(),
        }
    }

    pub async fn reconcile(
        &mut self,
        kube: &Client,
        node_cache: &node_watch::NodeMetadataCache,
        node_name: &str,
    ) -> Result<(), TemplatePullReconcileError> {
        let current_metadata =
            node_cache
                .metadata()
                .ok_or_else(|| TemplatePullReconcileError::NodeCacheNotReady {
                    node_name: node_name.to_owned(),
                })?;
        let observed_resource_version = current_metadata.resource_version.clone();
        let requests = self.template_write_requests_by_key(&current_metadata);

        self.abort_stale_tasks(&requests, node_name);
        let satisfied_annotation_keys = self.collect_finished_tasks(&requests, node_name).await;
        if let Err(err) = self
            .clear_satisfied_annotations(
                kube,
                node_name,
                &observed_resource_version,
                &satisfied_annotation_keys,
            )
            .await
        {
            tracing::warn!(%node_name, error = %err, "failed to clear satisfied template write annotations");
        }
        self.spawn_missing_tasks(&requests, node_name, &satisfied_annotation_keys);

        Ok(())
    }

    fn abort_stale_tasks(
        &mut self,
        requests: &BTreeMap<String, TemplateWriteTaskRequest>,
        node_name: &str,
    ) {
        self.in_flight.retain(|key, in_flight| {
            let keep = requests
                .get(key)
                .is_some_and(|request| request == &in_flight.request);
            if !keep {
                tracing::info!(
                    %node_name,
                    annotation = %key,
                    reference = %in_flight.request.reference,
                    "requesting cancellation of stale template write task"
                );
                in_flight.handle.abort();
            }
            keep
        });
    }

    async fn collect_finished_tasks(
        &mut self,
        requests: &BTreeMap<String, TemplateWriteTaskRequest>,
        node_name: &str,
    ) -> Vec<String> {
        let finished_keys: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, in_flight)| in_flight.handle.is_finished())
            .map(|(key, _)| key.clone())
            .collect();
        let mut satisfied_annotation_keys = Vec::new();

        for key in finished_keys {
            let Some(in_flight) = self.in_flight.remove(&key) else {
                continue;
            };
            let request = in_flight.request;
            match in_flight.handle.await {
                Ok(Ok(outcome)) => {
                    tracing::info!(
                        %node_name,
                        annotation = %key,
                        reference = %request.reference,
                        resolved_artifact_digest = ?outcome.content_hash,
                        state = "ready",
                        "template write annotation satisfied"
                    );
                    if requests.get(&key) == Some(&request) {
                        satisfied_annotation_keys.push(key);
                    }
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        %node_name,
                        annotation = %key,
                        reference = %request.reference,
                        error = %err,
                        "template write annotation materialization failed"
                    );
                }
                Err(err) if err.is_cancelled() => {
                    tracing::info!(
                        %node_name,
                        annotation = %key,
                        reference = %request.reference,
                        "template write task cancelled"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        %node_name,
                        annotation = %key,
                        reference = %request.reference,
                        error = %err,
                        "template write task failed"
                    );
                }
            }
        }

        satisfied_annotation_keys
    }

    async fn clear_satisfied_annotations(
        &self,
        kube: &Client,
        node_name: &str,
        observed_resource_version: &str,
        satisfied_annotation_keys: &[String],
    ) -> Result<(), ClearTemplateAnnotationsError> {
        if satisfied_annotation_keys.is_empty() {
            return Ok(());
        }

        let nodes: Api<Node> = Api::all(kube.clone());
        let patch = clear_template_write_annotation_patch(
            observed_resource_version,
            satisfied_annotation_keys,
        );
        nodes
            .patch(
                node_name,
                &template_annotation_patch_params(),
                &Patch::Merge(patch),
            )
            .await
            .map_err(|source| ClearTemplateAnnotationsError::PatchNode {
                node_name: node_name.to_owned(),
                source: Box::new(source),
            })?;
        tracing::info!(
            %node_name,
            annotations = satisfied_annotation_keys.len(),
            "cleared satisfied template write annotations"
        );
        Ok(())
    }

    fn spawn_missing_tasks(
        &mut self,
        requests: &BTreeMap<String, TemplateWriteTaskRequest>,
        node_name: &str,
        satisfied_annotation_keys: &[String],
    ) {
        for (key, request) in requests {
            if satisfied_annotation_keys.contains(key) {
                tracing::debug!(
                    %node_name,
                    annotation = %key,
                    reference = %request.reference,
                    "template write task already satisfied this tick"
                );
                continue;
            }
            if self.in_flight.contains_key(key) {
                tracing::debug!(
                    %node_name,
                    annotation = %key,
                    reference = %request.reference,
                    "template write task still in flight"
                );
                continue;
            }

            tracing::info!(
                %node_name,
                annotation = %key,
                reference = %request.reference,
                "starting template write task"
            );
            let task_request = request.clone();
            let pull_rpc_timeout = self.pull_rpc_timeout;
            let shutdown = self.shutdown.child_token();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    () = shutdown.cancelled() => {
                        Err(TemplateWriteTaskError::Cancelled)
                    }
                    result = async {
                        let mut tmpl = template_manager::Client::new(
                            TEMPLATE_MANAGER_RPC_TIMEOUT, pull_rpc_timeout,
                        );
                        let (template, artifact_selector, tag) =
                            template_reference_lookup_parts(&task_request.reference);
                        tmpl.ensure_template(&template, artifact_selector.as_deref(), tag.as_deref())
                            .await
                    } => result.map_err(TemplateWriteTaskError::TemplateManager),
                }
            });
            self.in_flight.insert(
                key.clone(),
                InFlightTemplateWrite {
                    request: request.clone(),
                    handle,
                },
            );
        }
    }

    fn template_write_requests_by_key(
        &mut self,
        current_metadata: &node_watch::NodeMetadata,
    ) -> BTreeMap<String, TemplateWriteTaskRequest> {
        let mut requests = BTreeMap::new();
        let mut malformed = BTreeMap::new();

        for (key, value) in &current_metadata.annotations {
            if !is_template_write_annotation_key(key) {
                continue;
            }

            let Some(reference) = parse_template_write_annotation(key, value) else {
                malformed.insert(key.clone(), value.clone());
                if self.last_malformed_warnings.get(key) != Some(value) {
                    tracing::warn!(
                        annotation = %key,
                        value = %value,
                        "ignoring malformed template write annotation"
                    );
                }
                continue;
            };

            self.last_malformed_warnings.remove(key);
            requests.insert(key.clone(), TemplateWriteTaskRequest { reference });
        }

        self.last_malformed_warnings = malformed;
        requests
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TemplateWriteTaskRequest {
    reference: TemplateReference,
}

fn template_reference_lookup_parts(
    reference: &TemplateReference,
) -> (String, Option<String>, Option<String>) {
    let template = reference.name().as_str().to_string();
    match reference.selector() {
        TemplateSelector::Tag(tag) if tag.as_str() == DEFAULT_TEMPLATE_TAG => {
            (template, None, None)
        }
        TemplateSelector::Tag(tag) => (template, None, Some(tag.as_str().to_string())),
        TemplateSelector::ArtifactDigest(digest) => (template, Some(digest.to_string()), None),
        TemplateSelector::ArtifactDigestPrefix(prefix) => {
            (template, Some(prefix.to_string()), None)
        }
    }
}

fn clear_template_write_annotation_patch(
    observed_resource_version: &str,
    annotation_keys: &[String],
) -> serde_json::Value {
    let annotations: BTreeMap<_, _> = annotation_keys
        .iter()
        .map(|key| (key.clone(), serde_json::Value::Null))
        .collect();

    serde_json::json!({
        "metadata": {
            "resourceVersion": observed_resource_version,
            "annotations": annotations,
        }
    })
}

fn template_annotation_patch_params() -> PatchParams {
    PatchParams::default()
}

#[cfg(test)]
mod tests;
