//! Status, readiness and lifecycle calls to the guest agent, including the process environment it
//! configures.

use super::*;

impl SpacedHostClient {
    pub async fn ping(&self) -> Result<proto::PingResponse, tonic::Status> {
        self.call_meta("ping", |mut client| async move {
            client.ping(PingRequest {}).await
        })
        .await
    }

    pub async fn is_connected(&self) -> Result<proto::IsConnectedResponse, tonic::Status> {
        self.call_meta("is_connected", |mut client| async move {
            client.is_connected(IsConnectedRequest {}).await
        })
        .await
    }

    pub async fn ping_guest(&self) -> Result<proto::PingGuestResponse, tonic::Status> {
        self.call_meta("ping_guest", |mut client| async move {
            client.ping_guest(PingGuestRequest {}).await
        })
        .await
    }

    pub async fn get_status(&self) -> Result<proto::GetStatusResponse, tonic::Status> {
        self.call_meta("get_status", |mut client| async move {
            client.get_status(GetStatusRequest {}).await
        })
        .await
    }

    pub async fn wait_ready(
        &self,
        timeout: std::time::Duration,
    ) -> Result<proto::WaitReadyResponse, tonic::Status> {
        let timeout = ReadyTimeout::new(timeout).map_err(invalid_argument_status)?;
        self.call_meta("wait_ready", |mut client| async move {
            client
                .wait_ready(WaitReadyRequest {
                    timeout: Some(proto_duration(timeout)),
                })
                .await
        })
        .await
    }

    pub async fn configure_process_env(
        &self,
        env: std::collections::HashMap<String, String>,
    ) -> Result<(), tonic::Status> {
        let env = parse_env(env);
        self.call_meta_once("configure_process_env", |mut client| {
            let env = proto_env(&env);
            async move {
                client
                    .configure_process_env(ConfigureProcessEnvRequest { env })
                    .await
            }
        })
        .await
        .map(|_: proto::ConfigureProcessEnvResponse| ())
    }

    pub async fn notify_resume(
        &self,
        reason: ResumeReason,
    ) -> Result<proto::NotifyResumeResponse, tonic::Status> {
        let now = pplx_space_spaced_common::system_time_to_proto(std::time::SystemTime::now())?;
        self.call_meta_once("notify_resume", |mut client| async move {
            client
                .notify_resume(NotifyResumeRequest {
                    reason: reason.into(),
                    host_time: Some(now),
                })
                .await
        })
        .await
    }

    /// Prepare for a VM memory snapshot, including the final guest filesystem flush.
    ///
    /// Call this instead of `flush_guest_filesystems` for suspend or full
    /// checkpoint paths; do not call both for one snapshot.
    pub async fn prepare_suspend(&self, timeout: std::time::Duration) -> Result<(), tonic::Status> {
        self.call_meta_once("prepare_suspend", |mut client| async move {
            let mut request = tonic::Request::new(PrepareSuspendRequest {});
            request.set_timeout(timeout.min(std::time::Duration::from_secs(180)));
            client.prepare_suspend(request).await
        })
        .await
        .map(|_: proto::PrepareSuspendResponse| ())
    }

    /// Flush guest filesystems without preparing bridge connections for a VM snapshot.
    ///
    /// Use this for disk-only snapshots of a still-running VM. Memory-bearing
    /// snapshots must call `prepare_suspend` instead.
    pub async fn flush_guest_filesystems(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), tonic::Status> {
        self.call_meta_once("flush_guest_filesystems", |mut client| async move {
            let mut request = tonic::Request::new(FlushGuestFilesystemsRequest {});
            request.set_timeout(timeout.min(std::time::Duration::from_secs(180)));
            client.flush_guest_filesystems(request).await
        })
        .await
        .map(|_: proto::FlushGuestFilesystemsResponse| ())
    }
}
