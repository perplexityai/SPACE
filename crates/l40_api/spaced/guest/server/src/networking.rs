//! `Networking` bridge-guest service handler. Thin shim over
//! [`crate::port_forward::LoopbackBridgeManager`].

use std::time::Duration;

use pplx_space_spaced_common::proto::bridge_guest::{
    CloseLoopbackBridgeRequest, CloseLoopbackBridgeResponse, OpenLoopbackBridgeRequest,
    OpenLoopbackBridgeResponse, networking_server::Networking as BridgeGuestNetworking,
};
use tonic::{Request, Response, Status};

use crate::SpacedGuestService;

/// Convert an optional protobuf duration to `std::time::Duration`. Rejects
/// malformed values (negative seconds/nanos, or `nanos >= 1_000_000_000`) with
/// `invalid_argument` rather than panicking inside `Duration::new`. Unlike
/// `validate_optional_timeout`, this preserves zero — the proto contract for
/// `wait_timeout` documents `0 = don't wait`.
fn parse_optional_wait_timeout(
    duration: Option<prost_types::Duration>,
) -> Result<Option<Duration>, Status> {
    let Some(duration) = duration else {
        return Ok(None);
    };
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(
            "wait_timeout must be a valid non-negative duration",
        ));
    }
    let seconds = u64::try_from(duration.seconds).map_err(|error| {
        Status::invalid_argument(format!("wait_timeout seconds out of range: {error}"))
    })?;
    let nanos = u32::try_from(duration.nanos).map_err(|error| {
        Status::invalid_argument(format!("wait_timeout nanos out of range: {error}"))
    })?;
    Ok(Some(Duration::new(seconds, nanos)))
}

#[tonic::async_trait]
impl BridgeGuestNetworking for SpacedGuestService {
    async fn open_loopback_bridge(
        &self,
        request: Request<OpenLoopbackBridgeRequest>,
    ) -> Result<Response<OpenLoopbackBridgeResponse>, Status> {
        self.require_ready()?;
        let req = request.into_inner();
        let port =
            u16::try_from(req.port).map_err(|_| Status::invalid_argument("port out of range"))?;
        let wait = parse_optional_wait_timeout(req.wait_timeout)?;
        let (status, detail) = self.inner.loopback_bridge.open(port, wait).await;
        Ok(Response::new(OpenLoopbackBridgeResponse {
            status: status as i32,
            detail: detail.unwrap_or_default(),
            service_generation: self.inner.service_generation.clone(),
        }))
    }

    async fn close_loopback_bridge(
        &self,
        request: Request<CloseLoopbackBridgeRequest>,
    ) -> Result<Response<CloseLoopbackBridgeResponse>, Status> {
        self.require_ready()?;
        let req = request.into_inner();
        let port =
            u16::try_from(req.port).map_err(|_| Status::invalid_argument("port out of range"))?;
        self.inner.loopback_bridge.close(port).await;
        Ok(Response::new(CloseLoopbackBridgeResponse {}))
    }
}
