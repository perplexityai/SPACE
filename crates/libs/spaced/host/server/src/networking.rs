//! `Networking` host-side service. Forwards each RPC to the in-VM
//! `Networking` service over the bridge-guest channel; sandbox managers
//! call this service to install / tear down loopback bridges for ports
//! that node-mgr has just exposed at the host firewall layer.

use pplx_space_spaced_common::proto::bridge_guest::{
    CloseLoopbackBridgeRequest as BridgeGuestCloseLoopbackBridgeRequest,
    FlushGatewayNeighborRequest as BridgeGuestFlushGatewayNeighborRequest,
    LoopbackBridgeStatus as BridgeGuestLoopbackBridgeStatus,
    OpenLoopbackBridgeRequest as BridgeGuestOpenLoopbackBridgeRequest,
};
use tonic::{Request, Response, Status};

use super::*;
use crate::bridge::BridgeGuestOperationBudget;

const MAX_GUEST_LOOPBACK_BRIDGE_DETAIL_BYTES: usize = 4 * 1024;
const MAX_GUEST_SERVICE_GENERATION_BYTES: usize = 64;

#[tonic::async_trait]
impl Networking for SpacedHostService {
    async fn open_loopback_bridge(
        &self,
        request: Request<proto::OpenLoopbackBridgeRequest>,
    ) -> Result<Response<proto::OpenLoopbackBridgeResponse>, Status> {
        let req = request.into_inner();
        let port = validate_user_port(req.port)?;
        let wait_timeout = req.wait_timeout;
        let budget_wait_timeout = validate_optional_wait_timeout(wait_timeout.as_ref())?;
        let response = self
            .call_bridge_guest_networking_once(
                "bridge_guest_open_loopback_bridge",
                BridgeGuestOperationBudget::NetworkingOpen {
                    wait_timeout: budget_wait_timeout,
                },
                |mut client| async move {
                    client
                        .open_loopback_bridge(BridgeGuestOpenLoopbackBridgeRequest {
                            port,
                            wait_timeout,
                        })
                        .await
                },
            )
            .await?;
        let status = BridgeGuestLoopbackBridgeStatus::try_from(response.status)
            .map_err(|_| Status::data_loss("guest returned unknown loopback bridge status"))?;
        if response.service_generation.is_empty()
            || response.service_generation.len() > MAX_GUEST_SERVICE_GENERATION_BYTES
        {
            return Err(Status::data_loss("invalid guest service generation"));
        }
        if response.detail.len() > MAX_GUEST_LOOPBACK_BRIDGE_DETAIL_BYTES {
            return Err(Status::data_loss(
                "guest loopback bridge detail is too large",
            ));
        }
        Ok(Response::new(proto::OpenLoopbackBridgeResponse {
            // BridgeGuest and host enums are wire-compatible (same numeric
            // values), so the validated enum can be cast directly.
            status: status as i32,
            detail: response.detail,
            service_generation: response.service_generation,
        }))
    }

    async fn close_loopback_bridge(
        &self,
        request: Request<proto::CloseLoopbackBridgeRequest>,
    ) -> Result<Response<proto::CloseLoopbackBridgeResponse>, Status> {
        let req = request.into_inner();
        let port = validate_user_port(req.port)?;
        let _ = self
            .call_bridge_guest_networking_once(
                "bridge_guest_close_loopback_bridge",
                BridgeGuestOperationBudget::NetworkingControl,
                |mut client| async move {
                    client
                        .close_loopback_bridge(BridgeGuestCloseLoopbackBridgeRequest { port })
                        .await
                },
            )
            .await?;
        Ok(Response::new(proto::CloseLoopbackBridgeResponse {}))
    }

    async fn flush_gateway_neighbor(
        &self,
        _request: Request<proto::FlushGatewayNeighborRequest>,
    ) -> Result<Response<proto::FlushGatewayNeighborResponse>, Status> {
        self.call_bridge_guest_networking_once(
            "bridge_guest_flush_gateway_neighbor",
            BridgeGuestOperationBudget::NetworkingControl,
            |mut client| async move {
                client
                    .flush_gateway_neighbor(BridgeGuestFlushGatewayNeighborRequest {})
                    .await
            },
        )
        .await?;
        Ok(Response::new(proto::FlushGatewayNeighborResponse {}))
    }
}

fn validate_user_port(port: u32) -> Result<u32, Status> {
    if port == 0 || port > u16::MAX as u32 {
        return Err(Status::invalid_argument("port must be 1-65535"));
    }
    Ok(port)
}

fn validate_optional_wait_timeout(
    wait_timeout: Option<&prost_types::Duration>,
) -> Result<Option<std::time::Duration>, Status> {
    wait_timeout
        .map(|duration| {
            if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
                return Err(Status::invalid_argument(
                    "wait_timeout must be a valid non-negative duration",
                ));
            }
            Ok(std::time::Duration::new(
                duration.seconds.cast_unsigned(),
                duration.nanos.cast_unsigned(),
            ))
        })
        .transpose()
}

#[cfg(test)]
mod tests;
