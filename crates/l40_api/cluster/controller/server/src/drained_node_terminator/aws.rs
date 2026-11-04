//! EC2-backed [`super::InstanceStateLookup`] for the KubeNodeGone recovery
//! path.
//!
//! Resolves `node_id -> node_k8s.k8s_node_name` (the EC2 `PrivateDnsName` on
//! EKS) from Postgres, then delegates the actual `DescribeInstances` call to
//! the `orphaned_nodes` EC2 seam so the fail-closed contract ("exactly
//! `terminated` -> `Gone`, everything else -> `Live`") is defined in exactly
//! one place. Feature-gated on `kubernetes` to match
//! [`super::KubeAnnotationLookup`]; the wiring only constructs it on the
//! `SPACE_NODE_DRAIN_TERMINATOR_KIND=aws` deploy.

use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sqlx::PgPool;

use super::{InstanceStateLookup, NodeInstanceState};
use crate::orphaned_nodes::aws::DescribeInstancesClient;
use crate::orphaned_nodes::{Ec2InstanceLookup, InstanceLookup, InstanceLookupError};

/// Production [`InstanceStateLookup`]. Holds the pool for the
/// `node_id -> private DNS` resolution and an [`Ec2InstanceLookup`] for the
/// EC2 `DescribeInstances` round-trip.
pub struct Ec2InstanceStateLookup {
    pool: PgPool,
    ec2: Ec2InstanceLookup,
}

impl Ec2InstanceStateLookup {
    pub fn new(pool: PgPool, client: Arc<dyn DescribeInstancesClient>) -> Self {
        Self {
            pool,
            ec2: Ec2InstanceLookup::new(client),
        }
    }
}

impl InstanceStateLookup for Ec2InstanceStateLookup {
    fn lookup_instance_state(
        &self,
        node_id: NodeId,
    ) -> Pin<Box<dyn Future<Output = Result<NodeInstanceState, InstanceLookupError>> + Send + '_>>
    {
        Box::pin(async move {
            // Read the private DNS and the kube Node uid in one query, so the
            // uid we carry into the guarded delete is the identity whose
            // instance we are about to confirm terminated.
            let row: Option<(String, Option<String>)> = sqlx::query_as(sql!(
                "SELECT k8s_node_name, k8s_node_uid FROM node_k8s WHERE node_id = $1"
            ))
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| {
                InstanceLookupError::Backend(format!("node_k8s lookup failed: {err}"))
            })?;
            // No `node_k8s` row means we can't resolve the private DNS to
            // cross-check EC2. Surface as a backend error so the recovery
            // path fails safe and parks the row rather than deleting blind.
            let Some((private_dns, expected_uid)) = row else {
                return Err(InstanceLookupError::Backend(format!(
                    "node {node_id} has no node_k8s row; cannot resolve private DNS for EC2 cross-check"
                )));
            };
            let outcome = self.ec2.lookup_by_private_dns(private_dns).await?;
            Ok(NodeInstanceState {
                outcome,
                expected_uid,
            })
        })
    }
}
