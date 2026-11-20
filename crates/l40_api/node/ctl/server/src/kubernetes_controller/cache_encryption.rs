//! This node's cache-encryption report, for dedicated groups with a tenant
//! KMS key (BYOK). The AMI's cache unit writes the boot ID and key it
//! encrypted `/space/cache` under to [`STATE_DIR`] (on `/space/run`, which
//! node-ctl mounts), and removes it on a plaintext boot. node-ctl publishes
//! them as two annotations; the cluster controller claims the node for the
//! tenant only when the reported boot is the kubelet's current one.
//!
//! The report is for the current boot only: a state directory left by an
//! earlier boot (`/space/run` is on the root volume) publishes nothing.
//! Written by server-side apply under its own field manager, so sending no
//! annotations removes the ones this manager set before.

use std::collections::BTreeMap;
use std::path::Path;

use k8s_openapi::api::core::v1::Node;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};

use super::node_watch;

const FIELD_MANAGER: &str = "space-node-ctl-cache-encryption";
const STATE_DIR: &str = "/space/run/cache-encryption";
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";
pub(crate) const ENCRYPTED_BOOT_ANNOTATION: &str = "space.pplx.net/cache-encrypted-boot";
pub(crate) const ENCRYPTED_KEY_ANNOTATION: &str = "space.pplx.net/cache-encrypted-key";

#[derive(Debug, thiserror::Error)]
pub enum CacheEncryptionReconcileError {
    #[error("Kubernetes Node metadata cache is not ready for {node_name}")]
    NodeCacheNotReady { node_name: String },
    #[error("failed to patch cache-encryption annotations on node {node_name}: {source}")]
    PatchNode {
        node_name: String,
        #[source]
        source: Box<kube::Error>,
    },
}

pub async fn reconcile_cache_encryption(
    kube: &Client,
    node_cache: &node_watch::NodeMetadataCache,
    node_name: &str,
) -> Result<(), CacheEncryptionReconcileError> {
    let desired = desired_annotations(
        read_trimmed(Path::new(BOOT_ID_PATH)).await,
        read_trimmed(&Path::new(STATE_DIR).join("boot-id")).await,
        read_trimmed(&Path::new(STATE_DIR).join("key-arn")).await,
    );
    let current = node_cache.metadata().ok_or_else(|| {
        CacheEncryptionReconcileError::NodeCacheNotReady {
            node_name: node_name.to_owned(),
        }
    })?;
    if annotations_current(&current.annotations, &desired) {
        return Ok(());
    }
    Api::<Node>::all(kube.clone())
        .patch(
            node_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(apply_body(node_name, &desired)),
        )
        .await
        .map_err(|source| CacheEncryptionReconcileError::PatchNode {
            node_name: node_name.to_owned(),
            source: Box::new(source),
        })?;
    tracing::info!(%node_name, encrypted = !desired.is_empty(), "cache-encryption annotations reconciled");
    Ok(())
}

async fn read_trimmed(path: &Path) -> Option<String> {
    let text = tokio::fs::read_to_string(path).await.ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// Both annotations when the state names this boot, none otherwise.
fn desired_annotations(
    current_boot: Option<String>,
    state_boot: Option<String>,
    key_arn: Option<String>,
) -> BTreeMap<&'static str, String> {
    match (current_boot, state_boot, key_arn) {
        (Some(current), Some(state), Some(key)) if current == state => BTreeMap::from([
            (ENCRYPTED_BOOT_ANNOTATION, current),
            (ENCRYPTED_KEY_ANNOTATION, key),
        ]),
        _ => BTreeMap::new(),
    }
}

fn annotations_current(
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<&'static str, String>,
) -> bool {
    [ENCRYPTED_BOOT_ANNOTATION, ENCRYPTED_KEY_ANNOTATION]
        .iter()
        .all(|key| current.get(*key) == desired.get(key))
}

fn apply_body(node_name: &str, desired: &BTreeMap<&'static str, String>) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "annotations": desired,
        }
    })
}

#[cfg(test)]
mod tests;
