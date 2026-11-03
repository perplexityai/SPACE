use pplx_space_spaced_guest_protocol::{
    platform::{GuestPlatform, require_host_contract},
    proto::{bridge_guest::*, platform::GuestOs},
};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(super) struct Meta;

pub(super) fn service() -> meta_server::MetaServer<Meta> {
    meta_server::MetaServer::new(Meta)
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
        Ok(Response::new(WaitReadyResponse {
            ready: true,
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
            platform: Some(GuestPlatform::for_target().wire()),
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
        _: Request<NotifyResumeRequest>,
    ) -> Result<Response<NotifyResumeResponse>, Status> {
        Err(unsupported())
    }
    async fn flush_filesystems(
        &self,
        _: Request<FlushFilesystemsRequest>,
    ) -> Result<Response<FlushFilesystemsResponse>, Status> {
        Err(unsupported())
    }
}
