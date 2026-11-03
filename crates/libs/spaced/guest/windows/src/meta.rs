use pplx_space_spaced_guest_protocol::{
    platform::{GuestPlatform, require_host_contract},
    proto::{
        bridge_guest::*,
        platform::{GuestOperation, GuestOs},
    },
};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(super) struct Meta {
    flush: Arc<Semaphore>,
}

pub(super) fn service() -> meta_server::MetaServer<Meta> {
    meta_server::MetaServer::new(Meta {
        flush: Arc::new(Semaphore::new(1)),
    })
    .max_decoding_message_size(64 * 1024)
    .max_encoding_message_size(64 * 1024)
}

fn unsupported() -> Status {
    let mut status =
        Status::failed_precondition("operation is not implemented by the Windows guest");
    status.metadata_mut().insert(
        "space-guest-capability-error",
        "guest_capability_unsupported"
            .parse()
            .expect("static metadata"),
    );
    status.metadata_mut().insert(
        "space-guest-capability",
        "platform_operations".parse().expect("static metadata"),
    );
    status
}

#[tonic::async_trait]
impl meta_server::Meta for Meta {
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            diagnostics: vec![],
            diagnostics_dropped: 0,
        }))
    }
    async fn wait_ready(
        &self,
        request: Request<WaitReadyRequest>,
    ) -> Result<Response<WaitReadyResponse>, Status> {
        require_host_contract(
            GuestOs::Windows,
            request.into_inner().host_contract_revision,
        )?;
        // SAFETY: GetTickCount64 takes no pointers and is available on all supported Windows versions.
        let milliseconds =
            unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() };
        let mut platform = GuestPlatform::for_target().wire();
        platform.operations = vec![GuestOperation::Flush as i32, GuestOperation::Resume as i32];
        Ok(Response::new(WaitReadyResponse {
            ready: true,
            guest_uptime: Some(prost_types::Duration {
                seconds: (milliseconds / 1000) as i64,
                nanos: ((milliseconds % 1000) * 1_000_000) as i32,
            }),
            version: env!("CARGO_PKG_VERSION").into(),
            compatibility_family: format!(
                "{}.{}",
                env!("CARGO_PKG_VERSION_MAJOR"),
                env!("CARGO_PKG_VERSION_MINOR")
            ),
            mem_total_bytes: None,
            vcpu_count: std::thread::available_parallelism()
                .ok()
                .and_then(|n| u32::try_from(n.get()).ok()),
            platform: Some(platform),
        }))
    }
    async fn configure_process_env(
        &self,
        _: Request<ConfigureProcessEnvRequest>,
    ) -> Result<Response<ConfigureProcessEnvResponse>, Status> {
        Err(unsupported())
    }
    async fn notify_resume(
        &self,
        request: Request<NotifyResumeRequest>,
    ) -> Result<Response<NotifyResumeResponse>, Status> {
        let request = request.into_inner();
        let reason = ResumeReason::try_from(request.reason)
            .map_err(|_| Status::invalid_argument("unknown resume_reason"))?;
        if reason == ResumeReason::Unspecified {
            return Err(Status::invalid_argument("resume_reason is required"));
        }
        let timestamp = request
            .host_time
            .ok_or_else(|| Status::invalid_argument("host_time is required"))?;
        let status = tokio::task::spawn_blocking(move || super::lifecycle::sync_clock(timestamp))
            .await
            .map_err(|error| Status::internal(format!("clock task failed: {error}")))?;
        let clock_sync_status = match status {
            Ok(()) => ClockSyncStatus::Synced,
            Err(super::lifecycle::LifecycleError::Timestamp) => {
                return Err(Status::invalid_argument("invalid host timestamp"));
            }
            Err(error) => {
                tracing::warn!(%error, "Windows resume clock synchronization failed");
                ClockSyncStatus::Failed
            }
        };
        Ok(Response::new(NotifyResumeResponse {
            ready: true,
            clock_sync_status: clock_sync_status as i32,
        }))
    }
    async fn flush_filesystems(
        &self,
        _: Request<FlushFilesystemsRequest>,
    ) -> Result<Response<FlushFilesystemsResponse>, Status> {
        let permit =
            self.flush.clone().try_acquire_owned().map_err(|_| {
                Status::resource_exhausted("guest volume flush already in progress")
            })?;
        // Cancellation cannot interrupt a filesystem syscall. Keep its permit
        // inside the worker until it actually returns, including across retries.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            super::lifecycle::flush_volumes()
        })
        .await
        .map_err(|error| Status::internal(format!("flush task failed: {error}")))?
        .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(FlushFilesystemsResponse {}))
    }
}
