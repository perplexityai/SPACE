use pplx_space_node_api_client::proto::node_info_server::NodeInfo;
use pplx_space_node_api_client::proto::{
    GetNodeInfoRequest, GetNodeInfoResponse, NodeRole, NodeSchedulingStatus,
};
use tonic::{Request, Response, Status};

/// Realistic-enough scheduling totals for a simulated worker. The control plane
/// uses these to make placement decisions; the simulator does not actually
/// allocate network slots, so we report a steady "ready" pool of fake slots.
const SIM_SLOTS_TOTAL: u32 = 64;
const SIM_SLOTS_READY: u32 = 64;
const SIM_SLOTS_MIN_READY: u32 = 4;
const SIM_SLOTS_MAX: u32 = 64;

pub struct SimNodeInfoService {
    role: NodeRole,
    node_id: String,
}

impl SimNodeInfoService {
    pub fn new(role: NodeRole, node_id: String) -> Self {
        Self { role, node_id }
    }

    fn node_info_response(&self) -> GetNodeInfoResponse {
        GetNodeInfoResponse {
            role: self.role.into(),
            node_id: self.node_id.clone(),
            scheduling_status: NodeSchedulingStatus::Ready.into(),
            scheduling_reason: "simulated node, always Ready".into(),
            network_slots_ready: SIM_SLOTS_READY,
            network_pool_min_ready: SIM_SLOTS_MIN_READY,
            network_slots_degraded: 0,
            network_slots_quarantined: 0,
            node_ctl: None,
            network_slots_total: SIM_SLOTS_TOTAL,
            network_slots_leased: 0,
            network_pool_max_slots: SIM_SLOTS_MAX,
            resource_status: None,
        }
    }
}

#[tonic::async_trait]
impl NodeInfo for SimNodeInfoService {
    async fn get_node_info(
        &self,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        Ok(Response::new(self.node_info_response()))
    }
}
