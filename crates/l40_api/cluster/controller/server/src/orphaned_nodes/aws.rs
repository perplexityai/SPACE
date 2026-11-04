//! EC2-backed [`super::InstanceLookup`].
//!
//! Same seam pattern as [`crate::node_drain::aws::AsgTerminateClient`]: a thin
//! trait wraps the SDK call so unit tests can substitute a recording fake
//! without exercising the real AWS SDK.
//!
//! ## Fail-closed contract
//!
//! Anything other than the explicit EC2 state `"terminated"` — including
//! unknown / newly-introduced state names — is treated as live. EC2 has
//! historically added state names (`hibernated`, etc.); an allowlist of
//! "live" states would silently flip a real live instance into a Gone
//! verdict whenever AWS adds a new state, so the predicate is "exactly
//! `terminated` → Gone, everything else → Live."

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{InstanceLookup, InstanceLookupError, InstanceLookupOutcome};

/// Thin seam over EC2 `DescribeInstances`. The blanket impl forwards to the
/// real AWS client; unit tests substitute a recording fake.
pub trait DescribeInstancesClient: Send + Sync + 'static {
    fn describe_instances_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>>;
}

/// Shape we extract from each matching `Instance` in the EC2 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedInstance {
    pub instance_id: String,
    /// `InstanceStateName` as a lower-case string (`pending`, `running`,
    /// `shutting-down`, `terminated`, `stopping`, `stopped`).
    pub state: String,
}

impl MatchedInstance {
    /// True unless EC2 has *definitively* terminated the instance. Fail
    /// closed for unknown / newly introduced state names.
    pub fn is_live(&self) -> bool {
        self.state.as_str() != "terminated"
    }
}

impl DescribeInstancesClient for pplx_space_aws_client::ec2::Client {
    fn describe_instances_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>> {
        Box::pin(async move {
            let instances = self
                .describe_instances_by_private_dns(&private_dns_name)
                .await
                .map_err(|err| err.to_string())?;
            Ok(instances
                .into_iter()
                .map(|instance| MatchedInstance {
                    instance_id: instance.instance_id,
                    state: instance.state,
                })
                .collect())
        })
    }
}

/// Production [`InstanceLookup`] backed by EC2 `DescribeInstances`.
pub struct Ec2InstanceLookup {
    client: Arc<dyn DescribeInstancesClient>,
}

impl Ec2InstanceLookup {
    pub fn new(client: Arc<dyn DescribeInstancesClient>) -> Self {
        Self { client }
    }
}

impl InstanceLookup for Ec2InstanceLookup {
    async fn lookup_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Result<InstanceLookupOutcome, InstanceLookupError> {
        let matches = self
            .client
            .describe_instances_by_private_dns(private_dns_name)
            .await
            .map_err(InstanceLookupError::Backend)?;
        if let Some(live) = matches.iter().find(|m| m.is_live()) {
            return Ok(InstanceLookupOutcome::Live {
                instance_id: live.instance_id.clone(),
                state: live.state.clone(),
            });
        }
        Ok(InstanceLookupOutcome::Gone)
    }
}

#[cfg(test)]
mod tests;
