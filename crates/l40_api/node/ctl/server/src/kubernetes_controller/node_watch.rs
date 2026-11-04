use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Node;
use kube::Client;
use kube::api::{Api, WatchEvent, WatchParams};
use pplx_space_common_core::kubernetes::SANDBOX_NETWORK_SLOTS_RESOURCE;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const NODE_WATCH_TIMEOUT_SECS: u32 = 60;
const NODE_WATCH_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
enum WatchNodeMetadataError {
    #[error("Kubernetes Node watch failed: {0}")]
    Kube(#[from] kube::Error),
    #[error("Kubernetes Node watch returned error event: {status}")]
    ErrorEvent { status: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeMetadata {
    pub resource_version: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    /// Current advertised value of the `space.pplx.net/network-slots` extended
    /// resource on the Node's `status.capacity`, if present. `None` means the
    /// resource has never been advertised. Used by the network-slot capacity
    /// reconciler to skip no-op status patches.
    pub network_slots_capacity: Option<String>,
}

#[derive(Clone)]
pub struct NodeMetadataCache {
    rx: watch::Receiver<Option<NodeMetadata>>,
}

impl NodeMetadataCache {
    pub fn metadata(&self) -> Option<NodeMetadata> {
        self.rx.borrow().clone()
    }

    #[cfg(test)]
    pub async fn wait_ready(&self) -> Result<(), NodeMetadataWatchClosedError> {
        let mut rx = self.rx.clone();
        rx.wait_for(Option::is_some)
            .await
            .map(|_| ())
            .map_err(|_| NodeMetadataWatchClosedError)
    }
}

#[cfg(test)]
#[derive(Debug, thiserror::Error)]
#[error("Kubernetes Node metadata watch closed before becoming ready")]
pub struct NodeMetadataWatchClosedError;

pub fn spawn(kube: Client, node_name: String, shutdown: CancellationToken) -> NodeMetadataCache {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(watch_node_metadata(kube, node_name, tx, shutdown));
    NodeMetadataCache { rx }
}

async fn watch_node_metadata(
    kube: Client,
    node_name: String,
    tx: watch::Sender<Option<NodeMetadata>>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            result = watch_node_metadata_once(kube.clone(), &node_name, &tx, shutdown.clone()) => {
                if let Err(err) = result {
                    tracing::warn!(error = %err, %node_name, "Kubernetes Node watch failed; retrying");
                }
            }
        }

        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(NODE_WATCH_RETRY_DELAY) => {}
        }
    }
}

async fn watch_node_metadata_once(
    kube: Client,
    node_name: &str,
    tx: &watch::Sender<Option<NodeMetadata>>,
    shutdown: CancellationToken,
) -> Result<(), WatchNodeMetadataError> {
    let nodes: Api<Node> = Api::all(kube);
    let watch_params = WatchParams::default()
        .fields(&format!("metadata.name={node_name}"))
        .timeout(NODE_WATCH_TIMEOUT_SECS)
        .initial_events();
    let mut stream = nodes.watch(&watch_params, "0").await?.boxed();

    loop {
        let event = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            event = stream.try_next() => event?,
        };

        let Some(event) = event else {
            return Ok(());
        };

        match event {
            WatchEvent::Added(node) | WatchEvent::Modified(node) => {
                let network_slots_capacity = node
                    .status
                    .as_ref()
                    .and_then(|status| status.capacity.as_ref())
                    .and_then(|capacity| capacity.get(SANDBOX_NETWORK_SLOTS_RESOURCE))
                    .map(|quantity| quantity.0.clone());
                let _ = tx.send(Some(NodeMetadata {
                    resource_version: node.metadata.resource_version.unwrap_or_default(),
                    labels: node.metadata.labels.unwrap_or_default(),
                    annotations: node.metadata.annotations.unwrap_or_default(),
                    network_slots_capacity,
                }));
            }
            WatchEvent::Deleted(_) => {
                let _ = tx.send(None);
                tracing::warn!(%node_name, "watched Kubernetes Node was deleted");
            }
            WatchEvent::Bookmark(_) => {}
            WatchEvent::Error(status) => {
                return Err(WatchNodeMetadataError::ErrorEvent {
                    status: format!("{status:?}"),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests;
