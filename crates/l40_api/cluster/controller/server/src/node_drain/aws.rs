use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::future::Future;

use sqlx::{FromRow, PgPool};

use super::InstanceTerminator;

#[derive(Debug, thiserror::Error)]
pub enum TerminateError {
    #[error("kube node lookup failed: {0}")]
    KubeLookup(String),
    #[error("provider id is missing or unparseable: {0}")]
    ProviderId(String),
    /// Terminator backend (AWS ASG, kube Node delete, etc.) reported
    /// an error. Free-form payload because the failure modes are
    /// provider-specific; the reconciler records this verbatim in
    /// `drain_last_error` for operator triage.
    #[error("terminator backend failed: {0}")]
    Provider(String),
    /// The kube Node object for this SPACE node id is gone. For the
    /// AWS backend, a deleted kube Node does NOT imply the underlying
    /// EC2 instance was terminated — an operator could have removed
    /// the Node manually, leaving a live ASG member. Without an EC2
    /// DescribeInstances roundtrip the reconciler can't verify the
    /// real state, so it must leave the row in `terminating` with a
    /// descriptive `drain_last_error` for manual recovery. Other
    /// terminator backends (kube-delete, noop) can treat this as a
    /// success and report it via `Provider` / `Ok(())` instead.
    #[error("kube node missing; provider state unverified")]
    KubeNodeGone,
    /// The SPACE node row was updated out from under us between the
    /// `list_terminating_nodes` query and the terminate call (UID
    /// reset by a replacement node, or admin undrain). No provider
    /// call was made; the next tick will re-evaluate against the new
    /// state.
    #[error("node identity changed; skipping termination")]
    StaleIdentity,
}

/// Thin seam over Auto Scaling `TerminateInstanceInAutoScalingGroup` so the
/// provider call remains independent from node-drain reconciliation.
pub trait AsgTerminateClient: Send + Sync + 'static {
    fn terminate_instance(
        &self,
        instance_id: String,
        should_decrement_desired_capacity: bool,
    ) -> impl Future<Output = Result<(), TerminateError>> + Send;
}

impl AsgTerminateClient for pplx_space_aws_client::autoscaling::Client {
    async fn terminate_instance(
        &self,
        instance_id: String,
        should_decrement_desired_capacity: bool,
    ) -> Result<(), TerminateError> {
        self.terminate_instance_in_auto_scaling_group(
            &instance_id,
            should_decrement_desired_capacity,
        )
        .await
        .map_err(|err| TerminateError::Provider(err.to_string()))?;
        Ok(())
    }
}

/// Row shape produced by [`Ec2InstanceTerminator::load_node_row`]. Held in
/// one struct so the AWS-call helper takes a single typed argument and the
/// row decode is named — clippy flagged the previous tuple decode as too
/// complex to read at a glance.
#[derive(Debug, Clone, FromRow)]
struct TerminateNodeRow {
    phase: Option<String>,
    node_name: Option<String>,
    stored_uid: Option<String>,
    decrement_capacity: bool,
}

/// Production [`InstanceTerminator`] that calls AWS EC2 Auto Scaling
/// `TerminateInstanceInAutoScalingGroup`. The `should_decrement_desired_capacity`
/// flag is read from the per-node `nodes.terminate_decrement_capacity` column
/// so the call honors the operator's original `replace_after_terminate`
/// intent regardless of when the actual termination call runs (a two-MNG
/// blue-green rollout needs the incumbent ASG to shrink monotonically;
/// single-node drains keep the default auto-replace behavior). Decoupled
/// from the reconciler so unit tests can swap in a fake.
pub struct Ec2InstanceTerminator<C: AsgTerminateClient = pplx_space_aws_client::autoscaling::Client>
{
    pool: PgPool,
    kube_client: kube::Client,
    autoscaling: C,
}

/// Parse `aws:///<az>/<instance-id>` -> `<instance-id>`. AWS-formatted
/// `providerID` is the canonical pointer from a Kubernetes Node back
/// to its underlying EC2 instance; everything before the final `/`
/// is bookkeeping for the cloud-provider layer. Free function so callers
/// don't have to name the [`Ec2InstanceTerminator`] generic parameter.
pub(crate) fn parse_instance_id(provider_id: &str) -> Option<&str> {
    provider_id
        .strip_prefix("aws:///")
        .and_then(|rest| rest.rsplit('/').next())
        .filter(|id| id.starts_with("i-"))
}

impl<C: AsgTerminateClient> Ec2InstanceTerminator<C> {
    pub fn new(pool: PgPool, kube_client: kube::Client, autoscaling: C) -> Self {
        Self {
            pool,
            kube_client,
            autoscaling,
        }
    }

    async fn load_node_row(&self, node_id: NodeId) -> Result<TerminateNodeRow, TerminateError> {
        let row: Option<TerminateNodeRow> = sqlx::query_as(sql!(
            "SELECT n.drain_phase AS phase,
                    k.k8s_node_name AS node_name,
                    k.k8s_node_uid  AS stored_uid,
                    n.terminate_decrement_capacity AS decrement_capacity
             FROM nodes n
             LEFT JOIN node_k8s k ON k.node_id = n.id
             WHERE n.id = $1"
        ))
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| TerminateError::KubeLookup(err.to_string()))?;

        row.ok_or_else(|| {
            TerminateError::KubeLookup(format!("node {node_id} row vanished before terminate"))
        })
    }
}

impl<C: AsgTerminateClient> InstanceTerminator for Ec2InstanceTerminator<C> {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        // Re-fetch the SPACE row's state, the previously-observed kube UID,
        // and the per-drain replace-after-terminate intent atomically. If
        // `nodes.drain_phase` is no longer `terminating` (admin undrain) we
        // are operating on stale input; bail out before touching AWS.
        let row = self.load_node_row(node_id).await?;

        if row.phase.as_deref() != Some("terminating") {
            return Err(TerminateError::StaleIdentity);
        }

        let node_name = row.node_name.ok_or_else(|| {
            TerminateError::KubeLookup(format!(
                "node {node_id} has no node_k8s row; cannot resolve providerID"
            ))
        })?;

        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> =
            kube::Api::all(self.kube_client.clone());
        let node = match nodes.get(&node_name).await {
            Ok(node) => node,
            Err(kube::Error::Api(resp)) if resp.code == 404 => {
                // The kube Node object is gone. A deleted kube Node
                // does NOT imply the underlying EC2 instance was
                // terminated — an operator may have removed the Node
                // manually, leaving a live ASG member. Without an EC2
                // DescribeInstances roundtrip we cannot verify the
                // real state, so we leave the row in `terminating`
                // with a descriptive `drain_last_error` for manual
                // recovery (see `TerminateError::KubeNodeGone`).
                return Err(TerminateError::KubeNodeGone);
            }
            Err(err) => return Err(TerminateError::KubeLookup(err.to_string())),
        };

        // Identity guard: if the kube Node's current UID differs from
        // the UID we previously persisted, a replacement node has
        // appeared with the same name. `upsert_k8s_node_role` will have
        // reset `nodes.drain_phase` on that path, but the reset may not have
        // landed yet — refuse to terminate until the next tick sees a
        // consistent view.
        let current_uid = node.metadata.uid.as_deref();
        match (row.stored_uid.as_deref(), current_uid) {
            (Some(stored), Some(current)) if stored != current => {
                return Err(TerminateError::StaleIdentity);
            }
            _ => {}
        }

        let provider_id = node
            .spec
            .as_ref()
            .and_then(|spec| spec.provider_id.as_deref())
            .ok_or_else(|| {
                TerminateError::ProviderId(format!("kube Node {node_name} has no spec.providerID"))
            })?;
        let instance_id = parse_instance_id(provider_id).ok_or_else(|| {
            TerminateError::ProviderId(format!(
                "kube Node {node_name} providerID {provider_id} is not an AWS instance id"
            ))
        })?;

        self.autoscaling
            .terminate_instance(instance_id.to_string(), row.decrement_capacity)
            .await
    }
}
