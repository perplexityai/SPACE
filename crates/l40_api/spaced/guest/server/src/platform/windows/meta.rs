use pplx_space_spaced_guest_protocol::{
    platform::{GuestPlatform, require_host_contract},
    proto::{
        bridge_guest::*,
        platform::{GuestOperation, GuestOs},
    },
};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(super) struct Meta {
    operations: bool,
    service: super::SpacedGuestService,
    flush_slots: std::sync::Arc<tokio::sync::Semaphore>,
}

pub(super) fn service(
    operations: bool,
    service: super::SpacedGuestService,
) -> meta_server::MetaServer<Meta> {
    meta_server::MetaServer::new(Meta {
        operations,
        service,
        flush_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
    })
    .max_decoding_message_size(64 * 1024)
    .max_encoding_message_size(64 * 1024)
}

#[tonic::async_trait]
impl meta_server::Meta for Meta {
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            diagnostics: vec![],
            diagnostics_dropped: 0,
            memory_used_bytes: None,
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
        platform
            .operations
            .extend([GuestOperation::Flush as i32, GuestOperation::Resume as i32]);
        if self.operations {
            platform.operations.extend([
                GuestOperation::Process as i32,
                GuestOperation::Files as i32,
                GuestOperation::InteractiveProcessControl as i32,
            ]);
        }
        Ok(Response::new(WaitReadyResponse {
            ready: *self.service.inner.ready.borrow(),
            guest_uptime: Some(prost_types::Duration {
                seconds: (milliseconds / 1000) as i64,
                nanos: ((milliseconds % 1000) * 1_000_000) as i32,
            }),
            version: env!("CARGO_PKG_VERSION").into(),
            compatibility_family: pplx_space_spaced_guest_protocol::spaced_compatibility_family(),
            mem_total_bytes: None,
            vcpu_count: std::thread::available_parallelism()
                .ok()
                .and_then(|n| u32::try_from(n.get()).ok()),
            platform: Some(platform),
        }))
    }
    async fn configure_process_env(
        &self,
        request: Request<ConfigureProcessEnvRequest>,
    ) -> Result<Response<ConfigureProcessEnvResponse>, Status> {
        let _permit = self.service.inner.execution.admission.admit()?;
        self.service.inner.execution.admission.configure(
            super::update_backend::parse_environment(request.into_inner().env)?,
        )?;
        Ok(Response::new(ConfigureProcessEnvResponse {}))
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
        Ok(Response::new(NotifyResumeResponse {
            clock_sync_status: super::lifecycle::sync_clock(request.host_time)? as i32,
            ready: *self.service.inner.ready.borrow(),
        }))
    }
    async fn flush_filesystems(
        &self,
        _: Request<FlushFilesystemsRequest>,
    ) -> Result<Response<FlushFilesystemsResponse>, Status> {
        super::lifecycle::flush(self.flush_slots.clone()).await?;
        Ok(Response::new(FlushFilesystemsResponse {}))
    }
}
