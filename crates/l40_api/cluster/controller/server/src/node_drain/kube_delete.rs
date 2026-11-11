//! [`InstanceTerminator`] that removes the kube Node object via
//! `kubectl delete node` semantics.
//!
//! Closer to real termination than [`super::NoopInstanceTerminator`]
//! but provider-agnostic: applicable wherever node lifecycle is
//! "delete the kube object and let something else (kind container,
//! self-managed kubelet, GitOps-driven node pool, etc.) handle the
//! underlying compute". In kind the docker container persists and
//! kubelet may re-register a fresh Node with a new UID; in
//! self-managed clusters where node objects don't auto-recreate, the
//! deletion is final.
//!
//! Identity guards match [`super::Ec2InstanceTerminator`]: re-fetch
//! `nodes.drain_phase` + `node_k8s.k8s_node_uid` before the delete
//! and refuse to act on stale rows or UID-mismatched same-name
//! replacements.
//!
//! **RBAC.** The kube `delete` verb on core/v1 nodes is **not**
//! granted by the default `api-nodes` ClusterRole (that role
//! intentionally tops out at `patch` so the api-server can't evict
//! nodes). Selecting `SPACE_NODE_DRAIN_TERMINATOR_KIND=kube-delete`
//! in the Helm values renders an additional cluster-controller-only
//! ClusterRole + binding that adds `delete`; if you wire this
//! terminator outside the provided charts, replicate that binding or
//! every termination will fail with a Kubernetes 403 and rows will
//! stay stuck in `terminating`.

use pplx_space_common_core::types::NodeId;

use kube::api::{DeleteParams, Preconditions};
use pplx_space_common_core::sql;
use sqlx::PgPool;

use super::{InstanceTerminator, TerminateError};

pub struct KubeNodeDeleteTerminator {
    pool: PgPool,
    kube_client: kube::Client,
}

impl KubeNodeDeleteTerminator {
    pub fn new(pool: PgPool, kube_client: kube::Client) -> Self {
        Self { pool, kube_client }
    }
}

impl InstanceTerminator for KubeNodeDeleteTerminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        // Same row/UID re-fetch as Ec2InstanceTerminator. If the row
        // drifted under us (admin undrain), bail out before touching
        // kube.
        let row: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(sql!(
            "SELECT n.drain_phase, k.k8s_node_name, k.k8s_node_uid
             FROM nodes n
             LEFT JOIN node_k8s k ON k.node_id = n.id
             WHERE n.id = $1"
        ))
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| TerminateError::KubeLookup(err.to_string()))?;

        let (phase, node_name, stored_uid) = row.ok_or_else(|| {
            TerminateError::KubeLookup(format!("node {node_id} row vanished before terminate"))
        })?;

        if phase.as_deref() != Some("terminating") {
            return Err(TerminateError::StaleIdentity);
        }

        let node_name = node_name.ok_or_else(|| {
            TerminateError::KubeLookup(format!(
                "node {node_id} has no node_k8s row; cannot resolve kube Node name"
            ))
        })?;

        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.kube_client.clone());

        // Fetch first to honor the UID guard. A 404 here means the
        // Node is already gone — treat as success: the desired state
        // ("kube Node deleted") is already met, and the reconciler
        // can proceed to DELETE the SPACE row.
        let node = match nodes.get(&node_name).await {
            Ok(node) => node,
            Err(kube::Error::Api(resp)) if resp.code == 404 => {
                tracing::info!(
                    node_id = %node_id,
                    node_name = %node_name,
                    "node_drain: kube Node already deleted; treating as terminated"
                );
                return Ok(());
            }
            Err(err) => return Err(TerminateError::KubeLookup(err.to_string())),
        };

        let current_uid = node.metadata.uid.as_deref();
        if let (Some(stored), Some(current)) = (stored_uid.as_deref(), current_uid)
            && stored != current
        {
            return Err(TerminateError::StaleIdentity);
        }

        // UID precondition: between the GET above and this DELETE,
        // kubelet could re-register a fresh same-named Node with a
        // new UID. Without this precondition the kube API server
        // would happily delete the replacement; with it, a UID
        // mismatch becomes a 409 Conflict which we surface as
        // `StaleIdentity` so the next tick re-evaluates against the
        // refreshed inventory state.
        let dp = DeleteParams {
            preconditions: current_uid.map(|uid| Preconditions {
                resource_version: None,
                uid: Some(uid.to_string()),
            }),
            ..Default::default()
        };
        match nodes.delete(&node_name, &dp).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(resp)) if resp.code == 404 => Ok(()),
            Err(kube::Error::Api(resp)) if resp.code == 409 => Err(TerminateError::StaleIdentity),
            Err(err) => Err(TerminateError::Provider(format!(
                "kube delete node {node_name} failed: {err}"
            ))),
        }
    }
}

#[cfg(test)]
mod kube_delete_tests;
