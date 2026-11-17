//! Node-bound relay for retained EBS disks, independent of object-store volumes.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::NodeRole;
use pplx_space_execution_common::proto::BlockVolumeExecution;
use pplx_space_node_api_client::cluster::AuthenticatedClusterApiClient;
use tonic::{Request, Status};

use crate::service::{SANDBOX_MANAGER_PEERS, VolumeManagerService};

#[derive(Clone)]
pub struct BlockVolumeAuthority {
    node: NodeId,
    client: AuthenticatedClusterApiClient,
}

impl BlockVolumeAuthority {
    /// An explicit node identity enables the experiment; absence adds no
    /// control-plane dependency to existing volume-manager deployments.
    pub async fn from_env() -> Result<Option<Self>, Status> {
        let Some(raw) = std::env::var_os("SPACE_EBS_NODE_ID") else {
            return Ok(None);
        };
        let raw = raw
            .to_str()
            .ok_or_else(|| Status::failed_precondition("invalid EBS node identity"))?;
        let node = pplx_space_k8s_node_id::space_node_id(raw, NodeRole::Worker)
            .map_err(|_| Status::failed_precondition("invalid EBS node identity"))?;
        let client = pplx_space_node_api_client::cluster::connect_from_env()
            .await
            .map_err(|error| {
                Status::unavailable(format!("connect block-volume authority: {error}"))
            })?;
        Ok(Some(Self { node, client }))
    }
}

impl VolumeManagerService {
    pub(crate) fn block_client<T>(
        &self,
        request: &Request<T>,
        execution: Option<&BlockVolumeExecution>,
        operation: &'static str,
    ) -> Result<AuthenticatedClusterApiClient, Status> {
        self.peer_authorizer
            .authorize_request(request, operation, SANDBOX_MANAGER_PEERS)?;
        let authority = self
            .block_volume_authority
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("block-volume relay is disabled"))?;
        let node: NodeId = execution
            .ok_or_else(|| Status::invalid_argument("execution is required"))?
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid node identity"))?;
        if node != authority.node {
            return Err(Status::permission_denied(
                "block-volume request names a different node",
            ));
        }
        Ok(authority.client.clone())
    }
}
