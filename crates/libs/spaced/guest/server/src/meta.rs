use super::*;

#[cfg(target_os = "linux")]
fn flush_filesystem_at_path_with_syncfs(
    path: &str,
    syncfs: impl FnOnce(&std::fs::File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let root = std::fs::File::open(path)?;
    syncfs(&root)
}

async fn run_filesystem_flush(
    slots: Arc<Semaphore>,
    flush: impl FnOnce() -> Result<(), Status> + Send + 'static,
) -> Result<(), Status> {
    let permit = slots.try_acquire_owned().map_err(|_| {
        Status::resource_exhausted("guest filesystem flush already in progress; retry with backoff")
    })?;
    // A cancelled RPC cannot cancel syncfs. The worker owns admission until
    // the syscall actually returns, including during host retries.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        flush()
    })
    .await
    .map_err(|error| Status::internal(format!("guest filesystem flush task failed: {error}")))?
}

fn flush_guest_filesystems() -> Result<(), Status> {
    #[cfg(target_os = "linux")]
    {
        flush_filesystem_at_path_with_syncfs("/", |root| {
            nix::unistd::syncfs(root).map_err(Into::into)
        })
        .map_err(|error| Status::internal(format!("syncfs / failed: {error}")))?;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        Err(Status::failed_precondition(
            "guest filesystem flush is not supported on this platform",
        ))
    }
}

fn sync_guest_clock(host_time: Option<prost_types::Timestamp>) -> Result<ClockSyncStatus, Status> {
    let Some(host_time) = host_time else {
        return Err(Status::invalid_argument("host_time is required"));
    };
    sync_guest_clock_impl(host_time)
}

#[cfg(unix)]
fn sync_guest_clock_impl(host_time: prost_types::Timestamp) -> Result<ClockSyncStatus, Status> {
    if host_time.nanos < 0 || host_time.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument("host_time nanos out of range"));
    }
    let timespec = nix::sys::time::TimeSpec::new(host_time.seconds, host_time.nanos.into());
    match nix::time::clock_settime(nix::time::ClockId::CLOCK_REALTIME, timespec) {
        Ok(()) => Ok(ClockSyncStatus::Synced),
        Err(error) => {
            tracing::warn!(%error, "failed to sync guest clock to host timestamp");
            Ok(ClockSyncStatus::Failed)
        }
    }
}

#[cfg(not(unix))]
fn sync_guest_clock_impl(_host_time: prost_types::Timestamp) -> Result<ClockSyncStatus, Status> {
    Err(Status::failed_precondition(
        "guest clock sync is not supported on this platform",
    ))
}

fn parse_mem_total_bytes(meminfo: &str) -> Option<u64> {
    let mut fields = meminfo
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_ascii_whitespace();
    let _label = fields.next()?;
    let kibibytes = fields.next()?.parse::<u64>().ok()?;
    (fields.next()? == "kB")
        .then(|| kibibytes.checked_mul(1024))
        .flatten()
}

pub(crate) fn guest_mem_total_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|meminfo| parse_mem_total_bytes(&meminfo))
}

fn guest_vcpu_count() -> Option<u32> {
    std::thread::available_parallelism()
        .ok()
        .and_then(|count| u32::try_from(count.get()).ok())
}

#[cfg(test)]
mod guest_specs_tests;

impl SpacedGuestService {
    fn current_wait_ready_response(&self) -> BridgeGuestWaitReadyResponse {
        let ready = *self.inner.ready.borrow();
        BridgeGuestWaitReadyResponse {
            ready,
            guest_uptime: ready.then(|| (self.inner.guest_uptime_source)()).flatten(),
            version: pplx_space_spaced_common::spaced_version().to_string(),
            compatibility_family: pplx_space_spaced_common::spaced_compatibility_family(),
            mem_total_bytes: ready.then(guest_mem_total_bytes).flatten(),
            vcpu_count: ready.then(guest_vcpu_count).flatten(),
        }
    }

    async fn wait_until_ready(&self, timeout: Duration) -> bool {
        if *self.inner.ready.borrow() {
            return true;
        }
        let mut ready_events = self.inner.ready_events.subscribe();
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        loop {
            if *self.inner.ready.borrow() {
                return true;
            }
            tokio::select! {
                result = ready_events.changed() => {
                    if result.is_err() {
                        return *self.inner.ready.borrow();
                    }
                }
                _ = self.inner.shutdown.cancelled() => {
                    return *self.inner.ready.borrow();
                }
                _ = &mut deadline => {
                    return *self.inner.ready.borrow();
                }
            }
        }
    }
}

#[tonic::async_trait]
impl LocalMeta for SpacedGuestService {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        tracing::debug!(command = "ping", "spaced guest grpc command succeeded");
        Ok(Response::new(PingResponse {}))
    }

    async fn set_activity_state(
        &self,
        request: Request<LocalSetActivityStateRequest>,
    ) -> Result<Response<LocalSetActivityStateResponse>, Status> {
        let state = require_activity_state(request.into_inner().state())?;
        let _ = self.set_host_activity_state(state).await?;
        tracing::debug!(
            command = "set_activity_state",
            state = ?state,
            "spaced guest grpc command succeeded"
        );
        Ok(Response::new(LocalSetActivityStateResponse {
            outcome: None,
        }))
    }

    async fn request_active_for(
        &self,
        request: Request<LocalRequestActiveForRequest>,
    ) -> Result<Response<LocalRequestActiveForResponse>, Status> {
        let hold_for = validate_duration("hold_for", request.into_inner().hold_for)?;
        let response = self.request_host_active_for(hold_for).await?;
        tracing::debug!(
            command = "request_active_for",
            hold_for_secs = hold_for.as_secs_f64(),
            "spaced guest grpc command succeeded"
        );
        Ok(Response::new(LocalRequestActiveForResponse {
            outcome: response.outcome,
        }))
    }

    async fn request_shutdown(
        &self,
        request: Request<LocalShutdownRequest>,
    ) -> Result<Response<LocalShutdownResponse>, Status> {
        let reason = validate_optional_reason(request.into_inner().reason)?;
        let response = self.request_host_shutdown(reason).await?;
        tracing::debug!(
            command = "request_shutdown",
            outcome = ?response.outcome,
            "spaced guest grpc command succeeded"
        );
        Ok(Response::new(LocalShutdownResponse {
            outcome: response.outcome,
        }))
    }
}

fn require_activity_state(state: LocalActivityState) -> Result<LocalActivityState, Status> {
    match state {
        LocalActivityState::Idle | LocalActivityState::Active => Ok(state),
        LocalActivityState::Unspecified => {
            Err(Status::invalid_argument("activity_state is required"))
        }
    }
}

#[tonic::async_trait]
impl BridgeGuestMeta for SpacedGuestService {
    async fn ping(
        &self,
        _request: Request<BridgeGuestPingRequest>,
    ) -> Result<Response<BridgeGuestPingResponse>, Status> {
        tracing::debug!(
            command = "bridge_guest_ping",
            "spaced bridge guest command succeeded"
        );
        Ok(Response::new(BridgeGuestPingResponse {}))
    }

    async fn wait_ready(
        &self,
        request: Request<BridgeGuestWaitReadyRequest>,
    ) -> Result<Response<BridgeGuestWaitReadyResponse>, Status> {
        let timeout = validate_timeout(request.into_inner().timeout)?;
        let ready = self.wait_until_ready(timeout.as_duration()).await;
        let response = self.current_wait_ready_response();
        if ready {
            self.emit_ready_event();
        }
        tracing::debug!(
            command = "bridge_guest_wait_ready",
            ready = response.ready,
            "spaced bridge guest readiness wait completed"
        );
        Ok(Response::new(response))
    }

    async fn configure_process_env(
        &self,
        request: Request<ConfigureProcessEnvRequest>,
    ) -> Result<Response<ConfigureProcessEnvResponse>, Status> {
        let env = parse_env(request.into_inner().env);
        let env_len = env.as_map().len();
        *self.inner.process_env.write().await = env;
        tracing::info!(
            command = "bridge_guest_configure_process_env",
            env_len,
            "configured sandbox process env"
        );
        Ok(Response::new(ConfigureProcessEnvResponse {}))
    }

    async fn notify_resume(
        &self,
        request: Request<NotifyResumeRequest>,
    ) -> Result<Response<NotifyResumeResponse>, Status> {
        let request = request.into_inner();
        let reason =
            pplx_space_spaced_common::proto::bridge_guest::ResumeReason::try_from(request.reason)
                .map_err(|_| Status::invalid_argument("unknown resume_reason"))?;
        if reason == pplx_space_spaced_common::proto::bridge_guest::ResumeReason::Unspecified {
            return Err(Status::invalid_argument("resume_reason is required"));
        }
        let clock_sync_status = sync_guest_clock(request.host_time)?;
        let ready = *self.inner.ready.borrow();
        if ready {
            self.emit_ready_event();
        }
        Ok(Response::new(NotifyResumeResponse {
            clock_sync_status: clock_sync_status.into(),
            ready,
        }))
    }

    async fn flush_filesystems(
        &self,
        _request: Request<FlushFilesystemsRequest>,
    ) -> Result<Response<FlushFilesystemsResponse>, Status> {
        run_filesystem_flush(
            self.inner.filesystem_flush_slots.clone(),
            flush_guest_filesystems,
        )
        .await?;
        tracing::info!(
            command = "bridge_guest_flush_filesystems",
            "guest filesystems flushed"
        );
        Ok(Response::new(FlushFilesystemsResponse {}))
    }
}

use pplx_space_spaced_common::{validate_optional_reason, validate_timeout};

#[cfg(test)]
mod tests;
