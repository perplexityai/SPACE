//! Generic control calls with a Firecracker-only rolling-upgrade bridge.
//! Unimplemented means the peer lacks the method; implemented handlers reserve
//! that code and return preconditions for unsupported runtime operations.

use crate::{NodeManagerGrpcClient, proto::*};
use pplx_space_execution_common::proto::RuntimeKind;
use std::future::Future;
use tonic::{Response, Status};

pub trait RuntimeControl {
    fn runtime_vm_state(
        &mut self,
        runtime: RuntimeKind,
        request: GetVmStateRequest,
    ) -> impl Future<Output = Result<Response<GetVmStateResponse>, Status>> + Send;
    fn capture_runtime_state(
        &mut self,
        runtime: RuntimeKind,
        request: CaptureVmStateRequest,
    ) -> impl Future<Output = Result<Response<CaptureVmStateResponse>, Status>> + Send;
    fn restore_runtime_state(
        &mut self,
        runtime: RuntimeKind,
        request: RestoreVmStateRequest,
    ) -> impl Future<Output = Result<Response<RestoreVmStateResponse>, Status>> + Send;
}

async fn compatible<T>(
    runtime: RuntimeKind,
    result: Result<T, Status>,
    fallback: impl AsyncFnOnce() -> Result<T, Status>,
) -> Result<T, Status> {
    match result {
        Err(status)
            if runtime == RuntimeKind::Firecracker
                && status.code() == tonic::Code::Unimplemented =>
        {
            fallback().await
        }
        result => result,
    }
}

impl RuntimeControl for NodeManagerGrpcClient {
    async fn runtime_vm_state(
        &mut self,
        runtime: RuntimeKind,
        request: GetVmStateRequest,
    ) -> Result<Response<GetVmStateResponse>, Status> {
        let result = self.get_vm_state(request.clone()).await;
        compatible(runtime, result, async || {
            let response = self
                .get_firecracker_vm_state(GetFirecrackerVmStateRequest {
                    sandbox_id: request.sandbox_id,
                })
                .await?
                .into_inner();
            Ok(Response::new(GetVmStateResponse {
                state: response.state,
            }))
        })
        .await
    }

    async fn capture_runtime_state(
        &mut self,
        runtime: RuntimeKind,
        request: CaptureVmStateRequest,
    ) -> Result<Response<CaptureVmStateResponse>, Status> {
        let result = self.capture_vm_state(request.clone()).await;
        compatible(runtime, result, async || {
            let legacy = CreateSnapshotRequest::try_from(request)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.create_snapshot(legacy).await?;
            Ok(Response::new(CaptureVmStateResponse {}))
        })
        .await
    }

    async fn restore_runtime_state(
        &mut self,
        runtime: RuntimeKind,
        request: RestoreVmStateRequest,
    ) -> Result<Response<RestoreVmStateResponse>, Status> {
        let result = self.restore_vm_state(request.clone()).await;
        compatible(runtime, result, async || {
            let legacy = LoadSnapshotRequest::try_from(request)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.load_snapshot(legacy).await?;
            Ok(Response::new(RestoreVmStateResponse {}))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
