//! Production [`super::NodeMembership`] over the substrate's Node reflector.

use std::collections::HashSet;
use std::sync::Arc;

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;

use super::NodeMembership;

pub struct KubeNodeMembership {
    substrate: Arc<KubernetesSubstrate>,
}

impl KubeNodeMembership {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }
}

impl NodeMembership for KubeNodeMembership {
    type LookupError = kube::Error;

    fn live_node_uids(&self) -> Option<HashSet<String>> {
        let caches = &self.substrate.k8s.caches;
        if !caches.nodes_ready() {
            return None;
        }
        Some(
            caches
                .nodes
                .state()
                .iter()
                .filter_map(|node| node.metadata.uid.clone())
                .collect(),
        )
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, Self::LookupError> {
        self.substrate.kube_node_exists(&node_name).await
    }
}
