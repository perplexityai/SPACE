use super::*;
use crate::bridge::BridgeGuestOperationBudget;
use crate::protocol::*;

#[tonic::async_trait]
impl Meta for SpacedHostService {
    async fn install_sandbox_trust(
        &self,
        request: Request<proto::InstallSandboxTrustRequest>,
    ) -> Result<Response<proto::InstallSandboxTrustResponse>, Status> {
        #[cfg(feature = "split-trust-prototype")]
        {
            let request = request.into_inner();
            self.call_bridge_guest_once(
                "bridge_guest_install_sandbox_trust",
                BridgeGuestOperationBudget::RuntimeMetadata,
                |mut client| {
                    let request = request.clone();
                    async move {
                        client.install_sandbox_trust(
                            pplx_space_spaced_common::proto::bridge_guest::InstallSandboxTrustRequest {
                                certificate_der: request.certificate_der,
                                sandbox_id: request.sandbox_id,
                            },
                        ).await
                    }
                },
            )
            .await?;
            Ok(Response::new(proto::InstallSandboxTrustResponse {}))
        }
        #[cfg(not(feature = "split-trust-prototype"))]
        {
            let _ = request;
            Err(Status::unimplemented(
                "sandbox trust installation is not compiled",
            ))
        }
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        tracing::debug!(command = "ping", "spaced host grpc command succeeded");
        Ok(Response::new(PingResponse {}))
    }

    async fn is_connected(
        &self,
        _request: Request<IsConnectedRequest>,
    ) -> Result<Response<IsConnectedResponse>, Status> {
        let guest_bridge_connected = self.bridge_guest_command_plane_connected().await;
        tracing::debug!(command = "is_connected", connected = guest_bridge_connected);
        Ok(Response::new(IsConnectedResponse {
            connected: guest_bridge_connected,
        }))
    }

    async fn ping_guest(
        &self,
        _request: Request<PingGuestRequest>,
    ) -> Result<Response<PingGuestResponse>, Status> {
        self.call_bridge_guest_ping().await?;
        tracing::debug!(command = "ping_guest", "spaced host grpc command succeeded");
        Ok(Response::new(PingGuestResponse {}))
    }

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let status = match self
            .refresh_guest_ready_status(Duration::from_millis(1))
            .await
        {
            Ok(status) => status,
            Err(error) => {
                tracing::trace!(%error, "bridge guest readiness status probe failed");
                None
            }
        };
        Ok(Response::new(GetStatusResponse {
            ready: status.is_some(),
            guest_uptime: status.as_ref().and_then(|status| status.guest_uptime),
            host_version: Some(proto::VersionInfo {
                version: pplx_space_spaced_common::spaced_version().to_string(),
                compatibility_family: pplx_space_spaced_common::spaced_compatibility_family(),
            }),
            guest_version: status.map(|status| proto::VersionInfo {
                version: status.version,
                compatibility_family: status.compatibility_family,
            }),
        }))
    }

    async fn wait_ready(
        &self,
        request: Request<WaitReadyRequest>,
    ) -> Result<Response<WaitReadyResponse>, Status> {
        let context = RetryContext::none().with_grpc_timeout_metadata(request.metadata());
        let timeout = validate_timeout(request.into_inner().timeout)?;
        let response = self.wait_guest_ready(timeout, context).await?;
        Ok(Response::new(WaitReadyResponse {
            guest_uptime: response.guest_uptime,
            mem_total_bytes: response.mem_total_bytes,
            vcpu_count: response.vcpu_count,
            guest_version: Some(proto::VersionInfo {
                version: response.version,
                compatibility_family: response.compatibility_family,
            }),
        }))
    }

    async fn configure_process_env(
        &self,
        request: Request<ConfigureProcessEnvRequest>,
    ) -> Result<Response<ConfigureProcessEnvResponse>, Status> {
        let request = request.into_inner();
        let env = parse_env(request.env);
        self.call_bridge_guest_once(
            "bridge_guest_configure_process_env",
            BridgeGuestOperationBudget::RuntimeMetadata,
            |mut client| {
                let env = proto_env(&env);
                async move {
                    client
                        .configure_process_env(BridgeGuestConfigureProcessEnvRequest { env })
                        .await
                }
            },
        )
        .await?;
        Ok(Response::new(ConfigureProcessEnvResponse {}))
    }

    async fn notify_resume(
        &self,
        request: Request<NotifyResumeRequest>,
    ) -> Result<Response<NotifyResumeResponse>, Status> {
        let request = request.into_inner();
        self.clear_guest_ready();
        let response = self
            .call_bridge_guest_once(
                "bridge_guest_notify_resume",
                BridgeGuestOperationBudget::RuntimeMetadata,
                |mut client| async move {
                    client
                        .notify_resume(BridgeGuestNotifyResumeRequest {
                            reason: host_resume_reason_to_bridge(request.reason)?,
                            host_time: request.host_time,
                        })
                        .await
                },
            )
            .await?;
        let clock_sync_status =
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::try_from(
                response.clock_sync_status,
            )
            .map_err(|error| {
                Status::internal(format!(
                    "bridge guest returned unknown clock sync status: {error}"
                ))
            })?;
        match clock_sync_status {
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Synced => {}
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Failed => {
                return Err(Status::failed_precondition("guest clock sync failed"));
            }
            pplx_space_spaced_common::proto::bridge_guest::ClockSyncStatus::Unspecified => {
                return Err(Status::internal(
                    "bridge guest returned unspecified clock sync status",
                ));
            }
        }
        Ok(Response::new(NotifyResumeResponse {
            clock_sync_status: response.clock_sync_status,
            ready: response.ready,
        }))
    }

    async fn flush_guest_filesystems(
        &self,
        request: Request<FlushGuestFilesystemsRequest>,
    ) -> Result<Response<FlushGuestFilesystemsResponse>, Status> {
        let timeout = RetryContext::with_timeout(Duration::from_secs(180))
            .with_grpc_timeout_metadata(request.metadata())
            .remaining()
            .unwrap_or(Duration::ZERO);
        self.call_bridge_guest_once(
            "bridge_guest_flush_filesystems",
            BridgeGuestOperationBudget::FilesystemFlush(timeout),
            |mut client| async move {
                let mut request = Request::new(BridgeGuestFlushFilesystemsRequest {});
                request.set_timeout(timeout);
                client.flush_filesystems(request).await
            },
        )
        .await?;
        tracing::info!(
            command = "flush_guest_filesystems",
            "guest filesystems flushed"
        );
        Ok(Response::new(FlushGuestFilesystemsResponse {}))
    }

    async fn prepare_suspend(
        &self,
        request: Request<PrepareSuspendRequest>,
    ) -> Result<Response<PrepareSuspendResponse>, Status> {
        let timeout = RetryContext::with_timeout(Duration::from_secs(180))
            .with_grpc_timeout_metadata(request.metadata())
            .remaining()
            .unwrap_or(Duration::ZERO);
        self.clear_guest_ready();
        self.clear_all_bridge_guest_clients().await;
        let flush_result = self
            .call_bridge_guest_once(
                "bridge_guest_flush_filesystems",
                BridgeGuestOperationBudget::FilesystemFlush(timeout),
                |mut client| async move {
                    let mut request = Request::new(BridgeGuestFlushFilesystemsRequest {});
                    request.set_timeout(timeout);
                    client.flush_filesystems(request).await
                },
            )
            .await;
        self.clear_all_bridge_guest_clients().await;
        flush_result?;
        tracing::info!(
            command = "prepare_suspend",
            "guest filesystems flushed and spaced host bridge guest clients cleared before suspend"
        );
        Ok(Response::new(PrepareSuspendResponse {}))
    }
}

#[tonic::async_trait]
impl BridgeHostMeta for SpacedHostService {
    async fn set_activity_state(
        &self,
        request: Request<BridgeHostSetActivityStateRequest>,
    ) -> Result<Response<BridgeHostSetActivityStateResponse>, Status> {
        self.authorize_bridge_host_peer(&request)?;
        self.check_guest_signal_rate("bridge_host_set_activity_state")?;
        let state = require_activity_state(request.into_inner().state())?;
        if let Err(error) = self
            .call_host_control("host_control_set_activity_state", |mut client| async move {
                client
                    .set_activity_state(HostControlSetActivityStateRequest {
                        state: state as i32,
                    })
                    .await
            })
            .await
        {
            tracing::warn!(
                %error,
                command = "bridge_host_set_activity_state",
                "host control guest signal relay failed; returning opaque response"
            );
        }
        Ok(Response::new(BridgeHostSetActivityStateResponse {
            outcome: None,
        }))
    }

    async fn request_active_for(
        &self,
        request: Request<BridgeHostRequestActiveForRequest>,
    ) -> Result<Response<BridgeHostRequestActiveForResponse>, Status> {
        self.authorize_bridge_host_peer(&request)?;
        self.check_guest_signal_rate("bridge_host_request_active_for")?;
        let hold_for = validate_duration("hold_for", request.into_inner().hold_for)?;
        if let Err(error) = self
            .call_host_control("host_control_request_active_for", |mut client| async move {
                client
                    .request_active_for(HostControlRequestActiveForRequest {
                        hold_for: Some(pplx_space_spaced_common::duration_to_proto(hold_for)?),
                    })
                    .await
            })
            .await
        {
            tracing::warn!(
                %error,
                command = "bridge_host_request_active_for",
                "host control guest signal relay failed; returning opaque response"
            );
        }
        Ok(Response::new(BridgeHostRequestActiveForResponse {
            outcome: None,
        }))
    }

    async fn request_shutdown(
        &self,
        request: Request<BridgeHostShutdownRequest>,
    ) -> Result<Response<BridgeHostShutdownResponse>, Status> {
        self.authorize_bridge_host_peer(&request)?;
        self.check_guest_signal_rate("bridge_host_request_shutdown")?;
        let reason = request.into_inner().reason;
        if matches!(reason.as_deref(), Some("")) {
            return Err(Status::invalid_argument(
                "reason must not be empty when provided",
            ));
        }
        if let Some(reason) = reason.as_deref() {
            Self::validate_guest_text_field("reason", reason, MAX_GUEST_SHUTDOWN_REASON_BYTES)?;
        }
        tracing::debug!(
            command = "bridge_host_request_shutdown",
            "guest-requested shutdown ignored"
        );
        Ok(Response::new(BridgeHostShutdownResponse { outcome: None }))
    }
}

fn require_activity_state(
    state: BridgeHostActivityState,
) -> Result<BridgeHostActivityState, Status> {
    match state {
        BridgeHostActivityState::Idle | BridgeHostActivityState::Active => Ok(state),
        BridgeHostActivityState::Unspecified => {
            Err(Status::invalid_argument("activity_state is required"))
        }
    }
}
