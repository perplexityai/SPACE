use tonic::{Request, Response, Status};

use crate::clients::BackendClients;
use crate::node_ctl;
use crate::proto::node_info_server::NodeInfo;
use crate::proto::{
    CacheDiskAdmissionState as ProtoCacheDiskAdmissionState, CacheDiskStatus, GetNodeInfoRequest,
    GetNodeInfoResponse, NodeResourceStatus, NodeRole, NodeSchedulingStatus,
};
use pplx_space_node_mgr_client::proto as node_mgr_proto;
use pplx_space_node_mgr_client::{
    CacheDiskSchedulingEffect, NetworkPoolStatusReader, NodeResourceStatusReader,
};

pub struct NodeInfoService {
    role: NodeRole,
    node_id: String,
    clients: Option<BackendClients>,
    resource_status_reader: NodeResourceStatusReader,
    network_pool_status_reader: NetworkPoolStatusReader,
}

impl NodeInfoService {
    pub fn new(role: NodeRole, node_id: String, clients: Option<BackendClients>) -> Self {
        Self::with_resource_status_reader(role, node_id, clients, NodeResourceStatusReader::new())
    }

    pub fn with_resource_status_reader(
        role: NodeRole,
        node_id: String,
        clients: Option<BackendClients>,
        resource_status_reader: NodeResourceStatusReader,
    ) -> Self {
        Self::with_status_readers(
            role,
            node_id,
            clients,
            resource_status_reader,
            NetworkPoolStatusReader::new(),
        )
    }

    pub fn with_status_readers(
        role: NodeRole,
        node_id: String,
        clients: Option<BackendClients>,
        resource_status_reader: NodeResourceStatusReader,
        network_pool_status_reader: NetworkPoolStatusReader,
    ) -> Self {
        Self {
            role,
            node_id,
            clients,
            resource_status_reader,
            network_pool_status_reader,
        }
    }

    async fn scheduling_status(&self) -> SchedulingSnapshot {
        let Some(clients) = self.clients.as_ref() else {
            return SchedulingSnapshot {
                status: NodeSchedulingStatus::Ready,
                reason: "node-api role does not use local network slot pool".to_string(),
                ready: 0,
                min_ready: 0,
                degraded: 0,
                quarantined: 0,
                total: 0,
                leased: 0,
                max_slots: 0,
                resource_status: None,
            };
        };

        let status = self
            .network_pool_status_reader
            .read(
                clients.node(),
                Request::new(node_mgr_proto::NetworkPoolStatusRequest {}),
            )
            .await;
        let response = match status {
            Ok(status) => {
                if let Some(error) = status.fallback_error() {
                    tracing::warn!(
                        %error,
                        "node-manager network pool status unavailable; reusing recent status for node info"
                    );
                }
                status.response
            }
            Err(error) => {
                return SchedulingSnapshot {
                    status: NodeSchedulingStatus::Unavailable,
                    reason: format!("node-manager status failed: {error}"),
                    ready: 0,
                    min_ready: 0,
                    degraded: 0,
                    quarantined: 0,
                    total: 0,
                    leased: 0,
                    max_slots: 0,
                    resource_status: None,
                };
            }
        };

        use pplx_space_node_mgr_client::proto::NetworkPoolStatus;
        let pool_status =
            NetworkPoolStatus::try_from(response.network_pool_status).unwrap_or_default();
        let status = match pool_status {
            NetworkPoolStatus::Healthy => {
                if response.network_pool_max_slots > 0
                    && response.network_slots_leased >= response.network_pool_max_slots
                {
                    NodeSchedulingStatus::Full
                } else if response.network_slots_ready >= response.network_pool_min_ready {
                    NodeSchedulingStatus::Ready
                } else {
                    NodeSchedulingStatus::Warming
                }
            }
            NetworkPoolStatus::Warming => NodeSchedulingStatus::Warming,
            NetworkPoolStatus::Draining => NodeSchedulingStatus::Draining,
            NetworkPoolStatus::Degraded => NodeSchedulingStatus::Degraded,
            NetworkPoolStatus::Empty => NodeSchedulingStatus::Empty,
            NetworkPoolStatus::Unspecified => NodeSchedulingStatus::Unavailable,
        };
        let reason = match status {
            NodeSchedulingStatus::Ready => format!(
                "ready slots {} >= min_ready {}",
                response.network_slots_ready, response.network_pool_min_ready
            ),
            NodeSchedulingStatus::Warming => format!(
                "warming network pool: ready slots {} < min_ready {}",
                response.network_slots_ready, response.network_pool_min_ready
            ),
            NodeSchedulingStatus::Full => format!(
                "network pool full: leased slots {} >= max_slots {}",
                response.network_slots_leased, response.network_pool_max_slots
            ),
            NodeSchedulingStatus::Draining => "network pool is draining".to_string(),
            NodeSchedulingStatus::Degraded => format!(
                "network pool degraded: degraded={} quarantined={}",
                response.network_slots_degraded, response.network_slots_quarantined
            ),
            NodeSchedulingStatus::Empty => "network pool has no ready slots".to_string(),
            NodeSchedulingStatus::Unavailable | NodeSchedulingStatus::Unspecified => {
                "network pool status unavailable".to_string()
            }
        };

        let mut snapshot = SchedulingSnapshot {
            status,
            reason,
            ready: response.network_slots_ready,
            min_ready: response.network_pool_min_ready,
            degraded: response.network_slots_degraded,
            quarantined: response.network_slots_quarantined,
            total: response.network_slots_total,
            leased: response.network_slots_leased,
            max_slots: response.network_pool_max_slots,
            resource_status: None,
        };
        self.apply_node_resource_status(clients, &mut snapshot)
            .await;
        snapshot
    }

    async fn apply_node_resource_status(
        &self,
        clients: &BackendClients,
        snapshot: &mut SchedulingSnapshot,
    ) {
        let status = self
            .resource_status_reader
            .read(
                clients.node(),
                Request::new(node_mgr_proto::NodeResourceStatusRequest {}),
            )
            .await;
        let status = match status {
            Ok(status) => status,
            Err(error) => {
                snapshot.status = NodeSchedulingStatus::Unavailable;
                snapshot.reason = format!("node-manager resource status failed: {error}");
                return;
            }
        };
        if let Some(error) = status.fallback_error() {
            tracing::warn!(
                %error,
                "node-manager resource status unavailable; reusing recent status for node info"
            );
        }
        let cache_disk = match pplx_space_node_mgr_client::cache_disk_status(&status.response) {
            Ok(cache_disk) => cache_disk,
            Err(error) => {
                snapshot.status = NodeSchedulingStatus::Unavailable;
                snapshot.reason = format!("node-manager resource status failed: {error}");
                return;
            }
        };

        snapshot.resource_status = Some(NodeResourceStatus {
            cache_disk: Some(cache_disk_status_from_node_mgr(cache_disk)),
        });
        apply_cache_disk_scheduling_effect(snapshot, cache_disk);
    }

    async fn node_info_response(&self) -> GetNodeInfoResponse {
        let scheduling = self.scheduling_status().await;
        GetNodeInfoResponse {
            role: self.role.into(),
            node_id: self.node_id.clone(),
            scheduling_status: scheduling.status.into(),
            scheduling_reason: scheduling.reason,
            network_slots_ready: scheduling.ready,
            network_pool_min_ready: scheduling.min_ready,
            network_slots_degraded: scheduling.degraded,
            network_slots_quarantined: scheduling.quarantined,
            node_ctl: if self.clients.is_some() {
                node_ctl::node_ctl_status_if_present().await
            } else {
                None
            },
            network_slots_total: scheduling.total,
            network_slots_leased: scheduling.leased,
            network_pool_max_slots: scheduling.max_slots,
            resource_status: scheduling.resource_status,
        }
    }
}

fn apply_cache_disk_scheduling_effect(
    snapshot: &mut SchedulingSnapshot,
    cache_disk: &node_mgr_proto::CacheDiskStatus,
) {
    match cache_disk.scheduling_effect() {
        CacheDiskSchedulingEffect::Open => {}
        CacheDiskSchedulingEffect::WarnOnly => {
            tracing::warn!(
                reason = %cache_disk.admission_reason,
                detail = %cache_disk.admission_detail,
                "node disk admission warning threshold crossed"
            );
            snapshot.status = NodeSchedulingStatus::Full;
            snapshot.reason = format!(
                "{}: {}",
                cache_disk.admission_reason, cache_disk.admission_detail
            );
        }
        CacheDiskSchedulingEffect::Full => {
            snapshot.status = NodeSchedulingStatus::Full;
            snapshot.reason = format!(
                "{}: {}",
                cache_disk.admission_reason, cache_disk.admission_detail
            );
        }
        CacheDiskSchedulingEffect::Unavailable => {
            snapshot.status = NodeSchedulingStatus::Unavailable;
            snapshot.reason = format!(
                "{}: {}",
                cache_disk.admission_reason, cache_disk.admission_detail
            );
        }
    }
}

struct SchedulingSnapshot {
    status: NodeSchedulingStatus,
    reason: String,
    ready: u32,
    min_ready: u32,
    degraded: u32,
    quarantined: u32,
    total: u32,
    leased: u32,
    max_slots: u32,
    resource_status: Option<NodeResourceStatus>,
}

fn cache_disk_status_from_node_mgr(status: &node_mgr_proto::CacheDiskStatus) -> CacheDiskStatus {
    CacheDiskStatus {
        statvfs_total_bytes: status.statvfs_total_bytes,
        statvfs_free_bytes: status.statvfs_free_bytes,
        statvfs_available_bytes: status.statvfs_available_bytes,
        statvfs_available_fraction: status.statvfs_available_fraction,
        effective_statvfs_available_bytes: status.effective_statvfs_available_bytes,
        pending_reservation_bytes: status.pending_reservation_bytes,
        admission_state: proto_cache_disk_admission_state(status.admission_state_enum()).into(),
        admission_reason: status.admission_reason.clone(),
        admission_detail: status.admission_detail.clone(),
        btrfs_metadata_available_bytes: status.btrfs_metadata_available_bytes,
        btrfs_device_unallocated_bytes: status.btrfs_device_unallocated_bytes,
    }
}

fn proto_cache_disk_admission_state(
    state: node_mgr_proto::CacheDiskAdmissionState,
) -> ProtoCacheDiskAdmissionState {
    match state {
        node_mgr_proto::CacheDiskAdmissionState::Ready => ProtoCacheDiskAdmissionState::Ready,
        node_mgr_proto::CacheDiskAdmissionState::Warning => ProtoCacheDiskAdmissionState::Warning,
        node_mgr_proto::CacheDiskAdmissionState::Full => ProtoCacheDiskAdmissionState::Full,
        node_mgr_proto::CacheDiskAdmissionState::Unavailable => {
            ProtoCacheDiskAdmissionState::Unavailable
        }
        node_mgr_proto::CacheDiskAdmissionState::Unspecified => {
            ProtoCacheDiskAdmissionState::Unspecified
        }
    }
}

#[tonic::async_trait]
impl NodeInfo for NodeInfoService {
    async fn get_node_info(
        &self,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        Ok(Response::new(self.node_info_response().await))
    }
}

#[cfg(test)]
mod tests;
