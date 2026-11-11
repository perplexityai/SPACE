use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use pplx_space_common_core::kubernetes::HOST_CPU_LABEL;
use pplx_space_common_core::types::HostCpu;

use super::node_watch;

const HOST_CPU_FIELD_MANAGER: &str = "space-node-ctl-host-cpu";

#[derive(Debug, thiserror::Error)]
pub enum HostCpuReconcileError {
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch host CPU label on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

/// Publish this host's [`HostCpu`] identity as [`HOST_CPU_LABEL`] on the Node.
///
/// A full snapshot's recorded capture CPU is compared against this label, so
/// the value must be produced by the same derivation the sandbox manager uses
/// when it records a snapshot: [`HostCpu::current`].
///
/// A host whose processor can't be identified carries no label rather than a
/// guess, including one left by an earlier run: an apply that omits the key
/// removes it, because server-side apply drops fields its field manager owned
/// and no longer sends. That holds while this field manager is the label's
/// only owner; a key another manager also set stays. The label is written
/// with a dedicated field manager, and only when it differs from the cached
/// Node.
pub async fn reconcile_host_cpu(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
) -> Result<(), HostCpuReconcileError> {
    let host_cpu = HostCpu::current();
    let current_metadata =
        node_cache
            .metadata()
            .ok_or_else(|| HostCpuReconcileError::NodeCacheNotReady {
                node_name: node_name.to_owned(),
            })?;
    if host_cpu_label_current(&current_metadata, host_cpu) {
        tracing::debug!(%node_name, ?host_cpu, "host CPU label already current");
        return Ok(());
    }

    let nodes: Api<Node> = Api::all(kube.clone());
    nodes
        .patch(
            node_name,
            &host_cpu_patch_params(),
            &Patch::Apply(host_cpu_apply(node_name, host_cpu)),
        )
        .await
        .map_err(|source| HostCpuReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;

    tracing::info!(%node_name, ?host_cpu, "host CPU label reconciled");
    Ok(())
}

/// Whether the Node already carries exactly the desired label: this host's
/// identity, or no label at all when the identity is unknown.
fn host_cpu_label_current(
    current_metadata: &node_watch::NodeMetadata,
    host_cpu: Option<&HostCpu>,
) -> bool {
    current_metadata
        .labels
        .get(HOST_CPU_LABEL)
        .map(String::as_str)
        == host_cpu.map(HostCpu::as_str)
}

fn host_cpu_apply(node_name: &str, host_cpu: Option<&HostCpu>) -> serde_json::Value {
    let labels: serde_json::Map<String, serde_json::Value> = host_cpu
        .map(|host_cpu| (HOST_CPU_LABEL.to_owned(), host_cpu.as_str().into()))
        .into_iter()
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

fn host_cpu_patch_params() -> PatchParams {
    PatchParams::apply(HOST_CPU_FIELD_MANAGER).force()
}

#[cfg(test)]
mod tests;
