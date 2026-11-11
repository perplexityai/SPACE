use tonic::{Request, Response, Status};

use crate::proto::node_info_server::NodeInfo;
use crate::proto::{GetNodeInfoRequest, GetNodeInfoResponse, NodeRole, NodeSchedulingStatus};

/// Minimal `NodeInfo` implementation for the cluster control-plane service.
///
/// The cluster service is a single cluster-wide Deployment, not a node: it owns
/// no local network-slot pool and no node-ctl, so it reports a static `Ready`
/// with its role and id and nothing else. (The rich, `BackendClients`-driven
/// `NodeInfo` impl stays with the worker/builder node-api server.)
pub struct ClusterNodeInfoService {
    node_id: String,
}

impl ClusterNodeInfoService {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    fn info(&self) -> GetNodeInfoResponse {
        GetNodeInfoResponse {
            role: NodeRole::Cluster.into(),
            node_id: self.node_id.clone(),
            scheduling_status: NodeSchedulingStatus::Ready.into(),
            scheduling_reason: "cluster control-plane service has no local network slot pool"
                .to_string(),
            network_slots_ready: 0,
            network_pool_min_ready: 0,
            network_slots_degraded: 0,
            network_slots_quarantined: 0,
            node_ctl: None,
            network_slots_total: 0,
            network_slots_leased: 0,
            network_pool_max_slots: 0,
            resource_status: None,
        }
    }
}

#[tonic::async_trait]
impl NodeInfo for ClusterNodeInfoService {
    async fn get_node_info(
        &self,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        Ok(Response::new(self.info()))
    }
}
