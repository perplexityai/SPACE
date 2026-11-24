//! The execution environments this node can run, as node-manager reports
//! them in its execution inventory: the same observation node-ctl publishes
//! and labels the node with.
//!
//! Prefetch warms a release's twin for each environment here. A
//! node-manager that predates environment observations, refuses
//! template-manager, or can't be reached reports nothing, and prefetch acts
//! as before environments: it warms only the release a lookup without an
//! environment resolves.

use std::collections::BTreeSet;
use std::time::Duration;

use pplx_space_execution_common::environment::EnvironmentObservation;
use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_execution_spec::{EnvironmentReference, ExecutionPolicyObservation};
use pplx_space_node_mgr_client::proto::GetExecutionInventoryRequest;
use tracing::{debug, warn};

/// Whether template-manager warms builds per environment
/// ([`EnvironmentAwareness`]).
pub(crate) const ENVIRONMENT_PREFETCH_ENV: &str = "SPACE_TEMPLATE_ENVIRONMENT_PREFETCH_ENABLED";

/// Whether the node's environments drive prefetch: the environment pass,
/// which warms each release's twin per environment the node can run. Off,
/// the default, prefetch doesn't read the node's environments and behaves as
/// before environments. Lookups, Direct pools, node labels and the
/// placement-gate paths don't depend on it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum EnvironmentAwareness {
    #[default]
    Off,
    On,
}

impl EnvironmentAwareness {
    /// `1`, `true`, `yes`, `on` or `enabled` turns it on; unset or anything
    /// else leaves it off.
    pub(crate) fn from_raw(raw: Option<&str>) -> Self {
        if pplx_space_env_vars::env_flag(raw, false) {
            Self::On
        } else {
            Self::Off
        }
    }

    pub(crate) fn from_env() -> Self {
        Self::from_raw(std::env::var(ENVIRONMENT_PREFETCH_ENV).ok().as_deref())
    }

    pub(crate) fn is_on(self) -> bool {
        self == Self::On
    }
}

/// Includes the connection: a slow inventory must not hold a prefetch tick,
/// which then acts as before environments.
const READ_BUDGET: Duration = Duration::from_secs(5);

/// The environments a node's latest inventory says it can run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NodeEnvironments {
    /// Environments the node can cold boot that its enforced policy admits.
    pub(crate) runnable: BTreeSet<EnvironmentReference>,
}

impl NodeEnvironments {
    /// What `inventory` says, or `None` when it carries no environment
    /// observation or one that doesn't decode: unknown, never an empty set.
    ///
    /// The observation's usable environments are the ones the node's startup
    /// policy allows and its verified artifacts can cold boot; an applied
    /// cluster policy can narrow that, so a policy that decodes intersects
    /// them. A cluster policy the node can't read is unknown too: the node
    /// admits nothing new meanwhile, but treating that as an empty set would
    /// drop every environment's tag pins over a transient read failure.
    pub(crate) fn from_inventory(inventory: &NodeExecutionInventory) -> Option<Self> {
        let observation = match EnvironmentObservation::from_inventory(inventory) {
            Ok(Some(observation)) => observation,
            Ok(None) => return None,
            Err(error) => {
                warn!(%error, "node environment observation doesn't decode; treating it as unknown");
                return None;
            }
        };
        let policy = match inventory
            .policy
            .as_ref()
            .map(ExecutionPolicyObservation::try_from)
            .transpose()
        {
            Ok(policy) => policy,
            Err(error) => {
                warn!(%error, "node execution policy doesn't decode; treating environments as unknown");
                return None;
            }
        };
        let mut runnable = observation.usable().clone();
        if let Some(policy) = &policy {
            if policy.cluster_policy_unreadable() {
                debug!("node can't read its cluster execution policy; node environments unknown");
                return None;
            }
            let allowed: BTreeSet<_> = policy
                .allowed_environment_references()
                .iter()
                .copied()
                .collect();
            runnable.retain(|environment| allowed.contains(environment));
        }
        Some(Self { runnable })
    }
}

/// Read this node's environments from node-manager; `None` when it can't
/// tell. See the module docs for what callers do then.
pub(crate) async fn read() -> Option<NodeEnvironments> {
    observe(async {
        let mut client = pplx_space_node_mgr_client::connect()
            .await
            .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
        let mut request = tonic::Request::new(GetExecutionInventoryRequest {});
        request.set_timeout(READ_BUDGET);
        client
            .get_execution_inventory(request)
            .await
            .map(tonic::Response::into_inner)
    })
    .await
}

/// What one inventory read says, within [`READ_BUDGET`]. Every failure reads
/// as unknown, never as an empty set: a node-manager that predates
/// template-manager's access refuses it (`PermissionDenied`), one that
/// predates the RPC lacks it (`Unimplemented`), and one that hangs times
/// out. Prefetch then warms only what a lookup without an environment
/// resolves and holds environment pins for their grace period.
async fn observe(
    read: impl std::future::Future<Output = Result<NodeExecutionInventory, tonic::Status>>,
) -> Option<NodeEnvironments> {
    match tokio::time::timeout(READ_BUDGET, read).await {
        Ok(Ok(inventory)) => NodeEnvironments::from_inventory(&inventory),
        Ok(Err(status)) => {
            debug!(%status, "node execution inventory unavailable; node environments unknown");
            None
        }
        Err(_elapsed) => {
            debug!("node execution inventory read timed out; node environments unknown");
            None
        }
    }
}

#[cfg(test)]
mod tests;
