use super::activation_support::{
    GuestActivityKind, IdentityFreeDirectActivationAuthority, grpc_timeout_budget,
    space_api_route_identity_from_wire, space_api_route_identity_to_wire,
};
use super::cleanup::{
    CleanupCompletionDisposition, CleanupDispatchDisposition, ObservedCleanupClaim,
};
use super::cleanup_support::{
    CleanupCompletion, cleanup_sandbox_via_node, read_node_execution_fence,
};
use super::create::StartFromCheckpointLaunch;
use super::create_support::*;
use super::spaced_bridge::*;
use super::tail_support::*;
use super::*;
use futures_util::StreamExt;
use pplx_space_common_core::types::{GuestPort, SandboxId};
use pplx_space_events::{
    SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
    emit_sandbox_event,
};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use rollback_state::restage_rolled_back_full_snapshot;
use std::str::FromStr;
use tracing::Instrument;

const CURRENT_INSTANCE_DISK_ARTIFACT_PREFIX: &str = "d-stop-";
const OPERATION_DISK_ARTIFACT_PREFIX: &str = "d-operation-";

fn is_internal_snapshot_name(snapshot_name: &str) -> bool {
    snapshot_name.starts_with(CURRENT_INSTANCE_DISK_ARTIFACT_PREFIX)
        || snapshot_name.starts_with(OPERATION_DISK_ARTIFACT_PREFIX)
}

fn disk_snapshot_rollback_start_request(
    sandbox_id: String,
    netns_path: Option<String>,
) -> StartRequest {
    StartRequest {
        sandbox_id,
        blocking: false,
        netns_path,
        ..Default::default()
    }
}

fn replace_optional_snapshot_sidecar(
    source_dir: &str,
    target_dir: &str,
    filename: &str,
) -> Result<(), Status> {
    let source = std::path::PathBuf::from(source_dir).join(filename);
    let target = std::path::PathBuf::from(target_dir).join(filename);
    match std::fs::copy(&source, &target) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match std::fs::remove_file(&target) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(Status::internal(format!(
                    "remove stale snapshot sidecar {filename}: {error}"
                ))),
            }
        }
        Err(error) => Err(Status::internal(format!(
            "copy snapshot sidecar {filename}: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests;

impl SandboxManagerService {
    pub(super) async fn spaced_start_process(
        &self,
        request: Request<SpacedStartProcessRequest>,
    ) -> Result<Response<BoxedStatusStream<SpacedProcessEvent>>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::StartProcess.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::StartProcess)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.command.is_empty() {
            return Err(Status::invalid_argument("command is required"));
        }
        let command_arg0 = operation.command.first().cloned().unwrap_or_default();
        let sandbox = match self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Command,
            )
            .await
        {
            Ok(sandbox) => sandbox,
            Err(error) => {
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::GuestProcessStartFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .reason("ensure_active_for_guest_activity")
                        .program(command_arg0)
                        .bool_field(SandboxEventBoolField::Background, operation.background)
                        .bool_field(SandboxEventBoolField::CwdPresent, operation.cwd.is_some())
                        .error(error.to_string()),
                );
                return Err(error);
            }
        };
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = match self.connect_spaced_host(&sandbox).await {
            Ok(client) => client,
            Err(error) => {
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::GuestProcessStartFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .reason("connect_spaced_host")
                        .program(command_arg0)
                        .bool_field(SandboxEventBoolField::Background, operation.background)
                        .bool_field(SandboxEventBoolField::CwdPresent, operation.cwd.is_some())
                        .error(error.to_string()),
                );
                return Err(error);
            }
        };
        let options = start_process_options(operation)?;
        let start = client.start_process_stream_with_options(options);
        use super::restart_admission::GuestStartOutcome;
        let stream = match super::restart_admission::abandon_start_if_caller_cancelled(start).await
        {
            GuestStartOutcome::Opened(stream) => stream,
            // A caller that left is not a guest start failure.
            GuestStartOutcome::CallerGone => {
                return Err(Status::cancelled(
                    "process start caller disconnected before the guest opened the process stream",
                ));
            }
            GuestStartOutcome::Failed(error) => {
                emit_sandbox_event(
                    &req.sandbox_id,
                    SandboxEventType::GuestProcessStartFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .reason("start_process_stream")
                        .program(command_arg0)
                        .bool_field(SandboxEventBoolField::Background, operation.background)
                        .bool_field(SandboxEventBoolField::CwdPresent, operation.cwd.is_some())
                        .error(error.to_string()),
                );
                return Err(error);
            }
        };
        let max_events = operation.background.then_some(1);
        // Foreground process streams carry presence touches so the idle
        // clock cannot expire under a running command. Background starts are
        // excluded: their stream ends after the first event, and the process
        // intentionally outlives the caller's attention.
        let presence = (!operation.background).then(|| {
            StreamPresence::resolve(self.clone(), sandbox.id.clone(), GuestActivityKind::Command)
        });
        Ok(Response::new(fence_status_stream(
            forward_host_stream_with_presence(
                stream,
                process_event_to_spaced,
                max_events,
                presence,
            ),
            identity_token,
        )))
    }

    pub(super) async fn spaced_connect_process(
        &self,
        request: Request<SpacedConnectProcessRequest>,
    ) -> Result<Response<BoxedStatusStream<SpacedProcessEvent>>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::ConnectProcess.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::ConnectProcess)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedConnectProcess",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let identity_token = self.route_stream_identity_token(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let stream = client
            .connect_process_stream_with_cursor(
                operation.process_id.clone(),
                proto_duration_to_std(operation.timeout),
                operation.output_cursor,
            )
            .await?;
        // A reconnected foreground stream is the same liveness signal as the
        // original one (and reconnecting starts a fresh presence cap — by
        // design, reconnecting is real activity). This deliberately also
        // covers a client attached to a *background* process's output: the
        // connect request carries only a process id, so foreground-ness is
        // not knowable here without a spaced proto change — and unlike the
        // passive file/tail streams, an attached reader chose to follow
        // this process, which counts as presence; the per-stream cap bounds
        // the cost either way.
        let presence = Some(StreamPresence::resolve(
            self.clone(),
            sandbox.id.clone(),
            GuestActivityKind::Access,
        ));
        Ok(Response::new(fence_status_stream(
            forward_host_stream_with_presence(stream, process_event_to_spaced, None, presence),
            identity_token,
        )))
    }

    pub(super) async fn spaced_list_processes(
        &self,
        request: Request<SpacedListProcessesRequest>,
    ) -> Result<Response<SpacedListProcessesResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::ListProcesses.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::ListProcesses)?;
        if req.operation.is_none() {
            return Err(Status::invalid_argument(
                "spaced operation payload is required",
            ));
        }
        let sandbox = self
            .ensure_active_for_read_only_guest_activity(
                Some(route_auth.clone()),
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
                "SpacedListProcesses",
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        let processes = client
            .list_processes()
            .await?
            .into_iter()
            .map(process_info_to_spaced)
            .collect();
        Ok(Response::new(SpacedListProcessesResponse { processes }))
    }

    pub(super) async fn spaced_send_signal(
        &self,
        request: Request<SpacedSendSignalRequest>,
    ) -> Result<Response<SpacedSendSignalResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::SendSignal.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::SendSignal)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let signal = operation.signal;
        if signal <= 0 {
            return Err(Status::invalid_argument("signal must be greater than 0"));
        }
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .send_signal(operation.process_id.clone(), signal)
            .await?;
        Ok(Response::new(SpacedSendSignalResponse {}))
    }

    pub(super) async fn spaced_send_input(
        &self,
        request: Request<SpacedSendInputRequest>,
    ) -> Result<Response<SpacedSendInputResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::SendInput.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::SendInput)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .send_input(operation.process_id.clone(), operation.data.clone())
            .await?;
        Ok(Response::new(SpacedSendInputResponse {}))
    }

    pub(super) async fn spaced_close_stdin(
        &self,
        request: Request<SpacedCloseStdinRequest>,
    ) -> Result<Response<SpacedCloseStdinResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::CloseStdin.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::CloseStdin)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client.close_stdin(operation.process_id.clone()).await?;
        Ok(Response::new(SpacedCloseStdinResponse {}))
    }

    pub(super) async fn spaced_resize_pty(
        &self,
        request: Request<SpacedResizePtyRequest>,
    ) -> Result<Response<SpacedResizePtyResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::ResizePty.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::ResizePty)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let size = operation
            .size
            .ok_or_else(|| Status::invalid_argument("pty size is required"))?;
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client
            .resize_pty(operation.process_id.clone(), size)
            .await?;
        Ok(Response::new(SpacedResizePtyResponse {}))
    }

    pub(super) async fn spaced_kill_process(
        &self,
        request: Request<SpacedKillProcessRequest>,
    ) -> Result<Response<SpacedKillProcessResponse>, Status> {
        self.authorize_peer(
            &request,
            SpacedRpc::KillProcess.as_str(),
            ALLOW_SANDBOX_OPERATOR,
        )?;
        let wake_budget = grpc_timeout_budget(&request);
        let req = request.get_ref();
        let route_auth =
            self.prepare_space_api_bridge(&request, &req.sandbox_id, SpacedRpc::KillProcess)?;
        let operation = req
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        if operation.process_id.is_empty() {
            return Err(Status::invalid_argument("process_id is required"));
        }
        let sandbox = self
            .ensure_active_for_authorized_guest_activity(
                &route_auth,
                &req.sandbox_id,
                true,
                wake_budget,
                GuestActivityKind::Access,
            )
            .await?;
        route_auth.authorize_sandbox(&sandbox)?;
        let client = self.connect_spaced_host(&sandbox).await?;
        client.kill_process(operation.process_id.clone()).await?;
        Ok(Response::new(SpacedKillProcessResponse {}))
    }

    pub(super) async fn spaced_open_loopback_bridge(
        &self,
        request: Request<SpacedOpenLoopbackBridgeRequest>,
    ) -> Result<Response<SpacedOpenLoopbackBridgeResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            SpacedRpc::OpenLoopbackBridge.as_str(),
            ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
        )?;
        let req_ref = request.get_ref();
        let route_auth = self.prepare_classified_space_api_bridge(
            &request,
            &req_ref.sandbox_id,
            SpacedRpc::OpenLoopbackBridge,
            &peer,
        )?;
        let req = request.into_inner();
        let operation = req
            .operation
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        // Guest-live gate, not `Started`: exposure re-apply must be able to
        // re-arm the in-guest listener while a wake is still converging.
        let sandbox = self.lookup_guest_bridge_sandbox(&req.sandbox_id)?;
        if let Some(route_auth) = &route_auth {
            route_auth.authorize_sandbox(&sandbox)?;
        }
        let wait = proto_duration_to_std(operation.wait_timeout);
        // Bounded end-to-end: a frozen mid-wake guest parks the vsock
        // handshake indefinitely, and the elapse must come back retryable.
        let mut response = bounded_loopback_bridge_op("SpacedOpenLoopbackBridge", async {
            let client = self.connect_spaced_host(&sandbox).await?;
            client.open_loopback_bridge(operation.port, wait).await
        })
        .await?;
        if loopback_bridge_open_warrants_rearm(response.status) {
            // One close + open cycle, each under its own RPC bound. See
            // `loopback_bridge_open_warrants_rearm` for why this cures
            // guests that hold a sticky cached bind failure.
            tracing::info!(
                sandbox_id = %sandbox.id,
                port = operation.port,
                status = response.status,
                detail = %response.detail,
                "loopback bridge open returned a terminal failure; re-arming with close + open"
            );
            let _ = bounded_loopback_bridge_op("SpacedCloseLoopbackBridge", async {
                let client = self.connect_spaced_host(&sandbox).await?;
                client.close_loopback_bridge(operation.port).await
            })
            .await?;
            response = bounded_loopback_bridge_op("SpacedOpenLoopbackBridge", async {
                let client = self.connect_spaced_host(&sandbox).await?;
                client.open_loopback_bridge(operation.port, wait).await
            })
            .await?;
            if loopback_bridge_open_warrants_rearm(response.status) {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    port = operation.port,
                    status = response.status,
                    detail = %response.detail,
                    "loopback bridge still failing after close + open re-arm"
                );
            }
        }
        Ok(Response::new(SpacedOpenLoopbackBridgeResponse {
            status: response.status,
            detail: response.detail,
            service_generation: response.service_generation,
        }))
    }

    pub(super) async fn spaced_close_loopback_bridge(
        &self,
        request: Request<SpacedCloseLoopbackBridgeRequest>,
    ) -> Result<Response<SpacedCloseLoopbackBridgeResponse>, Status> {
        let peer = self.authorize_peer(
            &request,
            SpacedRpc::CloseLoopbackBridge.as_str(),
            ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
        )?;
        let req_ref = request.get_ref();
        let route_auth = self.prepare_classified_space_api_bridge(
            &request,
            &req_ref.sandbox_id,
            SpacedRpc::CloseLoopbackBridge,
            &peer,
        )?;
        let req = request.into_inner();
        let operation = req
            .operation
            .ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))?;
        // Same guest-live gate as open: a close issued mid-wake (e.g. the
        // api-side proxy-failure recovery) must reach the guest as soon as
        // it is reachable so the matching re-open can re-arm cleanly.
        let sandbox = self.lookup_guest_bridge_sandbox(&req.sandbox_id)?;
        if let Some(route_auth) = &route_auth {
            route_auth.authorize_sandbox(&sandbox)?;
        }
        // Same bound as open: a close against a frozen guest parks on the
        // vsock handshake just the same and must come back retryable.
        let _ = bounded_loopback_bridge_op("SpacedCloseLoopbackBridge", async {
            let client = self.connect_spaced_host(&sandbox).await?;
            client.close_loopback_bridge(operation.port).await
        })
        .await?;
        Ok(Response::new(SpacedCloseLoopbackBridgeResponse {}))
    }

    pub(super) async fn service_port_ingress(
        &self,
        request: Request<tonic::Streaming<ServicePortIngressFrame>>,
    ) -> Result<Response<BoxedStatusStream<ServicePortIngressFrame>>, Status> {
        self.authorize_peer(
            &request,
            "ServicePortIngress",
            ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR,
        )?;
        let mut metadata = pplx_space_common_svc::space_api_route::space_api_route_token_metadata(
            request.metadata(),
        );
        // Temporary rollout opt-in; older workers strip it and select fallback.
        if let Some(value) = request
            .metadata()
            .get(pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA)
        {
            metadata.insert(
                pplx_space_net_gateway_client::INGRESS_COMPLETION_METADATA,
                value.clone(),
            );
        }
        let mut inbound = request.into_inner();
        let first = tokio::select! {
            _ = self.shutdown_token.cancelled() => {
                return Err(Status::cancelled(
                    "service port ingress shut down before its start frame",
                ));
            }
            result = tokio::time::timeout(
                pplx_space_net_gateway_client::SERVICE_PORT_INGRESS_START_TIMEOUT,
                inbound.message(),
            ) => match result {
                Ok(result) => result?,
                Err(_) => {
                    pplx_space_metrics::sandbox::worker::service_port_ingress_rejected(
                        pplx_space_metrics::sandbox::worker::ServicePortIngressRejectionReason::StartTimeout,
                    );
                    return Err(Status::deadline_exceeded(
                        "service port ingress start frame timed out",
                    ));
                }
            },
        }
        .ok_or_else(|| Status::invalid_argument("service port ingress start frame is required"))?;
        let start = match first.message.as_ref() {
            Some(service_port_ingress_frame::Message::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "service port ingress first frame must be start",
                ));
            }
        };
        let sandbox_id = SandboxId::from_str(&start.sandbox_id)
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let sandbox_id_str = sandbox_id.to_string();
        let port = service_port_ingress_guest_port(start.port)?;
        let route_auth =
            self.prepare_service_port_ingress(&metadata, &sandbox_id_str, "ServicePortIngress")?;
        route_auth.authorize_service_port(&self.lookup_sandbox(&sandbox_id_str)?, port)?;
        self.ensure_active_for_read_only_guest_activity(
            Some(route_auth.clone()),
            &sandbox_id_str,
            true,
            Some(pplx_space_common_svc::ingress_admission::preparation_timeout()),
            GuestActivityKind::Access,
            "ServicePortIngress",
        )
        .await?;
        let ingress_protection::AdmittedIngress {
            sandbox,
            fence: identity_token,
            admission: ingress_admission,
            protection,
        } = self
            .admit_ingress(
                &sandbox_id_str,
                pplx_space_common_svc::ingress_admission::preparation_timeout(),
                |sandbox| route_auth.authorize_service_port(sandbox, port),
            )
            .await?;
        let trace_context = pplx_space_common_grpc::SandboxTraceContext {
            sandbox_id,
            port,
            route_generation: sandbox.space_api_route_identity.route_generation,
            execution_generation: sandbox.space_api_route_identity.execution_generation,
            current_node_id: sandbox.space_api_route_identity.current_node_id,
        };
        trace_context.record(&tracing::Span::current());
        bounded_loopback_bridge_op("ServicePortIngressReadiness", async {
            let client = self.connect_spaced_host(&sandbox).await?;
            let ready = client
                .open_loopback_bridge(
                    u32::from(port.get().get()),
                    Some(std::time::Duration::from_secs(2)),
                )
                .await?;
            if !matches!(
                crate::proto::SpacedLoopbackBridgeStatus::try_from(ready.status),
                Ok(crate::proto::SpacedLoopbackBridgeStatus::Direct
                    | crate::proto::SpacedLoopbackBridgeStatus::LoopbackForwarded)
            ) {
                return Err(Status::unavailable("ingress target port is not ready"));
            }
            Ok(())
        })
        .await?;

        let first = ServicePortIngressFrame {
            message: Some(service_port_ingress_frame::Message::Start(
                pplx_space_net_gateway_client::proto::ServicePortIngressStart {
                    sandbox_id: sandbox_id.to_string(),
                    port: port.get().get().into(),
                },
            )),
        };

        let shutdown_token = self.shutdown_token.child_token();
        let request_identity_token = identity_token.child_token();
        let rest = futures_util::stream::unfold(
            (inbound, shutdown_token, request_identity_token),
            move |(mut inbound, shutdown_token, request_identity_token)| async move {
                tokio::select! {
                    result = inbound.message() => {
                        match result {
                            Ok(Some(frame)) => {
                                Some((frame, (inbound, shutdown_token, request_identity_token)))
                            }
                            Ok(None) => None,
                            Err(status) => {
                                tracing::debug!(
                                    sandbox_id = %sandbox_id,
                                    sandbox_port = port.get().get(),
                                    %status,
                                    "service port ingress inbound stream ended with error"
                                );
                                None
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => None,
                    _ = request_identity_token.cancelled() => None,
                }
            },
        );
        let request_stream = futures_util::stream::once(async move { first }).chain(rest);
        let mut request = Request::new(request_stream);
        *request.metadata_mut() = metadata;
        request.extensions_mut().insert(trace_context.clone());

        let span = tracing::info_span!(
            "net_gateway.service_port_ingress",
            otel.kind = "client",
            "operation.name" = "grpc.client",
            rpc.system = "grpc",
            rpc.service = "net_gateway.control",
            rpc.method = "ServicePortIngress",
            sandbox_id = %sandbox.id,
            sandbox_port = port.get().get(),
            route_generation = tracing::field::Empty,
            execution_generation = tracing::field::Empty,
            current_node_id = tracing::field::Empty,
            traceparent = tracing::field::Empty,
            failure_origin = tracing::field::Empty,
            failure_reason = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );
        trace_context.record(&span);
        let response_result = bounded_loopback_bridge_op(
            "ServicePortIngress",
            async {
                let mut client = pplx_space_net_gateway_client::connect(&sandbox.id)
                    .await
                    .map_err(|error| {
                        Status::unavailable(format!("netgtw connect failed: {error}"))
                    })?;
                client.service_port_ingress(request).await
            }
            .instrument(span.clone()),
        )
        .await;
        record_service_port_ingress_client_span_status(&span, &response_result);
        let response = response_result?;
        // Forward statuses intact: typed metadata is part of the ingress contract.
        Ok(response.map(|stream| {
            fence_admitted_status_stream(
                Box::pin(stream),
                identity_token,
                (ingress_admission, protection),
            )
        }))
    }

    pub(super) async fn enable_snapshots(
        &self,
        request: Request<EnableSnapshotsRequest>,
    ) -> Result<Response<EnableSnapshotsResponse>, Status> {
        self.authorize_peer(&request, "EnableSnapshots", ALLOW_SANDBOX_SNAPSHOT_OPERATOR)?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = &request.get_ref().sandbox_id;
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        self.ensure_memory_state_supported(&sandbox, "enable snapshots")?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "enable snapshots")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "enable snapshots")?;
        self.ensure_snapshot_restore_supported("enable_snapshots")?;

        {
            let db = self.db_conn()?;
            crate::db::update_snapshots_enabled(&db, &sandbox.id, true)
                .map_err(|e| Status::internal(format!("db error: {e}")))?;
        }

        // If sandbox is running, start scheduling immediately
        if sandbox.state == SandboxState::Started {
            self.snapshot_scheduler
                .add_sandbox(sandbox.id.clone(), sandbox.template.clone())
                .await;
        }

        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }

        Ok(Response::new(EnableSnapshotsResponse {}))
    }

    pub(super) async fn disable_snapshots(
        &self,
        request: Request<DisableSnapshotsRequest>,
    ) -> Result<Response<DisableSnapshotsResponse>, Status> {
        self.authorize_peer(
            &request,
            "DisableSnapshots",
            ALLOW_SANDBOX_SNAPSHOT_OPERATOR,
        )?;
        let publish_node_state = !should_skip_node_publish(&request);
        let sandbox_id = &request.get_ref().sandbox_id;
        let sandbox = self.lookup_sandbox(sandbox_id)?;

        {
            let db = self.db_conn()?;
            crate::db::update_snapshots_enabled(&db, &sandbox.id, false)
                .map_err(|e| Status::internal(format!("db error: {e}")))?;
        }

        self.snapshot_scheduler.remove_sandbox(&sandbox.id).await;

        if publish_node_state {
            mark_sandbox_dirty(&self.db, &sandbox.id);
        }

        Ok(Response::new(DisableSnapshotsResponse {}))
    }

    pub(super) async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        self.authorize_peer(&request, "ListSnapshots", ALLOW_SANDBOX_OBSERVER)?;
        let req = request.get_ref();
        use pplx_space_common_core::inventory::{InventoryBudget, MAX_ENCODED_BYTES, MAX_ITEMS};
        use prost::Message as _;
        let mut request_budget = InventoryBudget::new(MAX_ITEMS, MAX_ENCODED_BYTES);
        for id in &req.sandbox_ids {
            request_budget
                .charge(id.len())
                .map_err(crate::inventory_budget::exceeded)?;
        }
        let sandbox_ids = request_sandbox_ids(&req.sandbox_ids);
        let mut snapshots = Vec::new();
        let mut response_budget = InventoryBudget::new(MAX_ITEMS, MAX_ENCODED_BYTES);

        let mut node_client = connect_node(&self.node_manager)
            .await
            .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;

        for sandbox_id in sandbox_ids {
            let sandbox = self.lookup_snapshot_inventory_sandbox(sandbox_id).await?;
            let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
            let _guard = artifact_lock.lock().await;
            let sandbox = self.lookup_snapshot_inventory_sandbox(sandbox.id).await?;
            let batch = list_snapshots_for_sandbox(
                &mut node_client,
                &sandbox,
                self.local_artifacts.clone(),
            )
            .await?;
            for snapshot in batch {
                response_budget
                    .charge(snapshot.encoded_len().saturating_add(8))
                    .map_err(crate::inventory_budget::exceeded)?;
                snapshots.push(snapshot);
            }
        }

        crate::inventory_budget::response(ListSnapshotsResponse { snapshots })
    }

    async fn lookup_snapshot_inventory_sandbox(
        &self,
        selector: String,
    ) -> Result<crate::db::SandboxRow, Status> {
        self.storage(move |db| {
            crate::db::inventory_read::lookup(db, &selector)
                .map_err(|error| error.into_status())?
                .ok_or_else(|| Status::not_found(format!("sandbox not found: {selector}")))
        })
        .await
    }
}

fn service_port_ingress_guest_port(port: u32) -> Result<GuestPort, Status> {
    let port =
        u16::try_from(port).map_err(|_| Status::invalid_argument("service port out of range"))?;
    GuestPort::try_from_u16(port)
        .map_err(|error| Status::invalid_argument(format!("service port {error}")))
}

fn record_service_port_ingress_client_span_status<T>(
    span: &tracing::Span,
    result: &Result<T, Status>,
) {
    match result {
        Ok(_) => {
            span.record("otel.status_code", "OK");
        }
        Err(status) => {
            if pplx_space_common_grpc::record_guest_service_status(status, span) {
                return;
            }
            span.record("otel.status_code", "ERROR");
            let description = status.to_string();
            span.record("otel.status_description", description.as_str());
        }
    }
}

async fn list_snapshots_for_sandbox(
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox: &crate::db::SandboxRow,
    artifacts: crate::local_artifacts::LocalArtifacts,
) -> Result<Vec<SnapshotDetail>, Status> {
    let sandbox_id = sandbox
        .id
        .parse::<SandboxId>()
        .map_err(|error| Status::internal(format!("invalid stored sandbox id: {error}")))?;
    let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox_id, true);
    let mut identities = Vec::new();
    while let Some(page) = pages.next(node_client).await? {
        if identities.len() + page.len() > 16_384 {
            return Err(Status::resource_exhausted(
                "snapshot response exceeds inventory limit",
            ));
        }
        identities.extend(page);
    }

    let sandbox_id = sandbox.id.clone();
    let template = sandbox.template.clone();
    let content_hash = sandbox.template_hash.clone();
    tokio::task::spawn_blocking(move || {
        let publications = artifacts
            .list_named_publications(&sandbox_id)
            .map_err(|error| error.into_status())?;
        let snapshots_dir =
            pplx_space_common_core::host::linux::paths::sandbox_snapshots_dir(&sandbox_id);
        project_snapshot_inventory(
            &sandbox_id,
            &template,
            content_hash.as_deref(),
            std::path::Path::new(&snapshots_dir),
            identities,
            publications,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("snapshot inventory task failed: {error}")))?
}

fn project_snapshot_inventory(
    sandbox_id: &str,
    template: &str,
    content_hash: Option<&str>,
    snapshots_dir: &std::path::Path,
    btrfs: Vec<pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo>,
    publications: Vec<crate::local_artifacts::NamedArtifactPublication>,
) -> Result<Vec<SnapshotDetail>, Status> {
    use pplx_space_common_core::inventory::{
        InventoryBudget, InventoryLimitExceeded, MAX_ENCODED_BYTES, MAX_ITEMS, MAX_RAW_BYTES,
    };
    use prost::Message as _;
    let mut wire_budget = InventoryBudget::new(MAX_ITEMS, MAX_ENCODED_BYTES);
    for snapshot in &btrfs {
        wire_budget
            .charge(snapshot.encoded_len())
            .map_err(crate::inventory_budget::exceeded)?;
    }
    let publications: std::collections::HashMap<_, _> = publications
        .into_iter()
        .map(|publication| (publication.snapshot_name.to_string(), publication))
        .collect();
    let mut snapshots = std::collections::BTreeMap::new();
    for snapshot in btrfs {
        if is_internal_snapshot_name(&snapshot.name) {
            continue;
        }
        pplx_space_common_core::types::SnapshotName::parse(&snapshot.name).map_err(|error| {
            Status::data_loss(format!("invalid snapshot inventory name: {error}"))
        })?;
        snapshots.insert(
            snapshot.name.clone(),
            SnapshotDetail {
                id: snapshot.name,
                sandbox_id: sandbox_id.to_string(),
                state: "ready".into(),
                timestamp: (!snapshot.timestamp.is_empty()).then_some(snapshot.timestamp),
                retention_tier: (!snapshot.tier.is_empty()).then_some(snapshot.tier),
                size_bytes: snapshot.exclusive_bytes,
                ..Default::default()
            },
        );
    }
    match std::fs::read_dir(snapshots_dir) {
        Ok(entries) => {
            let started = std::time::Instant::now();
            let mut directory_budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
            for (index, entry) in entries.enumerate() {
                if index >= 16_384 || started.elapsed() >= std::time::Duration::from_secs(2) {
                    return Err(Status::resource_exhausted(
                        "full snapshot inventory exceeds budget",
                    ));
                }
                let entry = entry
                    .map_err(|error| Status::internal(format!("read snapshot entry: {error}")))?;
                let name = entry.file_name().to_string_lossy().to_string();
                directory_budget
                    .charge(name.len())
                    .map_err(crate::inventory_budget::exceeded)?;
                if !name.starts_with("f-")
                    || !entry
                        .file_type()
                        .map_err(|error| Status::internal(format!("read snapshot type: {error}")))?
                        .is_dir()
                {
                    continue;
                }
                if snapshots.len() >= MAX_ITEMS && !snapshots.contains_key(&name) {
                    return Err(crate::inventory_budget::exceeded(InventoryLimitExceeded));
                }
                snapshots
                    .entry(name.clone())
                    .or_insert_with(|| SnapshotDetail {
                        id: name,
                        sandbox_id: sandbox_id.to_string(),
                        size_bytes: None,
                        ..Default::default()
                    });
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(Status::internal(format!(
                "read snapshot directory: {error}"
            )));
        }
    }
    let mut metadata_budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
    for snapshot in snapshots.values_mut() {
        let path = snapshots_dir.join(&snapshot.id);
        let full = snapshot.id.starts_with("f-");
        snapshot.r#type = if full {
            crate::proto::SnapshotType::Full
        } else {
            crate::proto::SnapshotType::Disk
        }
        .into();
        let transfer_binding = match crate::inventory_budget::read_sidecar(
            &path.join(".transfer.json"),
            &mut metadata_budget,
        ) {
            Ok(Some(bytes)) => Some(
                serde_json::from_slice::<crate::db::transfer::TransferBinding>(&bytes).is_ok_and(
                    |binding| {
                        binding.sandbox_id == sandbox_id
                            && binding.template == template
                            && Some(binding.content_hash.as_str()) == content_hash
                    },
                ),
            ),
            Ok(None) => None,
            Err(error) if error.code() == tonic::Code::ResourceExhausted => return Err(error),
            Err(error) => {
                tracing::warn!(sandbox_id, snapshot_name = %snapshot.id, %error, "cannot verify transferred snapshot binding");
                Some(false)
            }
        };
        if transfer_binding == Some(false) {
            snapshot.state = "incomplete".into();
        } else if full || transfer_binding.is_some() {
            let kind = if full {
                pplx_space_common_core::types::ArtifactKind::Full
            } else {
                pplx_space_common_core::types::ArtifactKind::Disk
            };
            let publication = publications.get(&snapshot.id).filter(|publication| {
                publication.artifact_kind == kind
                    && std::path::Path::new(&publication.local_path) == path
            });
            // A subvolume and partial VM files can survive a failed capture.
            // Transferred disk snapshots also require publication: rename can
            // survive a failed SQLite commit, which an identical retry repairs.
            // Otherwise skip_if_exists would falsely acknowledge that retry.
            let required: &[&str] = if full {
                &["vmstate", "memory"]
            } else {
                &["rootfs.ext4"]
            };
            snapshot.state = match publication.map(|publication| publication.state) {
                Some(state) if state != crate::local_artifacts::ArtifactState::Ready => {
                    state.as_str()
                }
                _ if !required.iter().all(|file| {
                    std::fs::symlink_metadata(path.join(file))
                        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
                }) =>
                {
                    "incomplete"
                }
                Some(_) => "ready",
                None => "unknown",
            }
            .into();
        }
        let metadata = match crate::inventory_budget::read_sidecar(
            &path.join("snapshot.json"),
            &mut metadata_budget,
        ) {
            Ok(Some(bytes)) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(value)
                    if value.get("sandbox_id").and_then(serde_json::Value::as_str)
                        == Some(sandbox_id)
                        && value
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .is_none_or(|name| name == snapshot.id) =>
                {
                    value
                }
                _ => {
                    tracing::warn!(sandbox_id, snapshot_name = %snapshot.id, "ignoring invalid snapshot metadata");
                    continue;
                }
            },
            Ok(None) => continue,
            Err(error) => return Err(error),
        };
        let text = |key: &str| {
            metadata
                .get(key)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        snapshot.label = text("label");
        snapshot.timestamp = snapshot
            .timestamp
            .take()
            .or_else(|| text("timestamp"))
            .or_else(|| text("created_at"));
        snapshot.retention_tier = snapshot
            .retention_tier
            .take()
            .or_else(|| text("retention_tier"));
        if let Some(source) = metadata.get("source") {
            snapshot.template_id = source
                .get("template_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            snapshot.content_hash = source
                .get("content_hash")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
        }
    }
    Ok(snapshots.into_values().collect())
}

impl SandboxManagerService {
    fn checkout_rollback_snapshot(
        &self,
        sandbox_id: &str,
        snapshot_name: &str,
    ) -> Result<Option<crate::local_artifacts::RestoreLease>, Status> {
        let name = snapshot_name
            .parse::<pplx_space_common_core::types::SnapshotName>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let Some(publication) = self
            .local_artifacts
            .get_named(sandbox_id, &name)
            .map_err(|error| Status::internal(format!("inspect rollback publication: {error}")))?
        else {
            // Older locally created snapshots may predate the artifact catalog.
            return Ok(None);
        };
        let expected_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
            sandbox_id,
            snapshot_name,
        );
        if publication.local_path != expected_path {
            return Err(Status::failed_precondition(
                "rollback publication path differs",
            ));
        }
        self.local_artifacts
            .check_out_for_restore(publication.local_artifact_id)
            .map(Some)
            .map_err(|error| {
                Status::failed_precondition(format!("rollback snapshot is in use: {error}"))
            })
    }

    fn require_snapshot_mutation_ownership(&self, sandbox_id: &str) -> Result<(), Status> {
        let db = self
            .db
            .lock()
            .map_err(|_| Status::unavailable("snapshot ownership is unavailable"))?;
        if crate::db::has_cleanup_episode(&db, sandbox_id)
            .map_err(|_| Status::unavailable("snapshot cleanup ownership cannot be read"))?
        {
            return Err(Status::failed_precondition(
                "cleanup owns this sandbox; retain its snapshots",
            ));
        }
        Ok(())
    }

    pub(super) async fn delete_snapshots(
        &self,
        request: Request<DeleteSnapshotsRequest>,
    ) -> Result<Response<DeleteSnapshotsResponse>, Status> {
        self.authorize_peer(&request, "DeleteSnapshots", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.get_ref();
        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let _guard = artifact_lock.lock().await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.require_snapshot_mutation_ownership(&sandbox.id)?;
        let protected_instance_path = sandbox.current_local_artifact_id.and_then(|artifact_id| {
            self.local_artifacts
                .get(artifact_id)
                .ok()
                .flatten()
                .filter(|artifact| {
                    artifact.artifact_origin
                        != pplx_space_common_core::types::ArtifactOrigin::Snapshot
                })
                .map(|artifact| artifact.local_path)
        });

        let mut node_client = connect_node(&self.node_manager)
            .await
            .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;

        if req.all {
            let mut deleted = Vec::new();

            // Delete btrfs snapshots (d-* subvolumes) via node-manager
            let sandbox_id = sandbox
                .id
                .parse::<SandboxId>()
                .map_err(|error| Status::internal(format!("invalid stored sandbox id: {error}")))?;
            let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox_id, false);
            while let Some(page) = pages.next(&mut node_client).await? {
                for snap in page {
                    if is_internal_snapshot_name(&snap.name) {
                        continue;
                    }
                    let local_path =
                        pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                            &sandbox.id,
                            &snap.name,
                        );
                    if protected_instance_path.as_deref() == Some(local_path.as_str()) {
                        continue;
                    }
                    if deleted.len() >= 4096 {
                        return Err(Status::resource_exhausted(
                            "deleted 4096 snapshots; retry delete-all to continue",
                        ));
                    }
                    node_client
                        .delete_cache_artifact(
                            pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                                local_path: local_path.clone(),
                            },
                        )
                        .await?;
                    if let Ok(snap_name) =
                        pplx_space_common_core::types::SnapshotName::parse(snap.name.clone())
                    {
                        match self
                            .local_artifacts
                            .check_out_named_for_delete(&sandbox.id, &snap_name)
                        {
                            Ok(lease) => {
                                if let Err(error) = lease.commit() {
                                    tracing::warn!(
                                        sandbox_id = %sandbox.id,
                                        snapshot_name = %snap.name,
                                        %error,
                                        "deleted snapshot via node-mgr but failed to remove local_artifact row",
                                    );
                                }
                            }
                            Err(
                                crate::local_artifacts::CheckoutError::NotFound(_)
                                | crate::local_artifacts::CheckoutError::NamedNotFound { .. },
                            ) => {
                                // Already gone; nothing to clean up.
                            }
                            Err(error) => {
                                tracing::warn!(
                                    sandbox_id = %sandbox.id,
                                    snapshot_name = %snap.name,
                                    %error,
                                    "deleted snapshot via node-mgr but local_artifact row remained",
                                );
                            }
                        }
                    }
                    deleted.push(snap.name);
                }
            }
            // Note: list_btrfs_snapshots returns both d-* and f-* entries, so the loop
            // above already covers full snapshots. No separate f-* scan needed.

            Ok(Response::new(DeleteSnapshotsResponse { deleted }))
        } else if let Some(snapshot_name) = req.snapshot_name.as_deref().filter(|s| !s.is_empty()) {
            validate_snapshot_name(snapshot_name)?;
            if is_internal_snapshot_name(snapshot_name) {
                return Err(Status::failed_precondition(
                    "cannot delete an internal snapshot artifact",
                ));
            }
            let local_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                &sandbox.id,
                snapshot_name,
            );
            if protected_instance_path.as_deref() == Some(local_path.as_str()) {
                return Err(Status::failed_precondition(
                    "cannot delete the current instance artifact",
                ));
            }

            // Both disk and full snapshots may be btrfs subvolumes —
            // delete via node-manager (root). DeleteCacheArtifact
            // handles both the subvolume case (privileged
            // `btrfs subvolume delete`) and the plain-directory case
            // (`remove_dir_all`), and is idempotent on a missing path.
            {
                node_client
                    .delete_cache_artifact(
                        pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                            local_path: local_path.clone(),
                        },
                    )
                    .await
                    .map_err(|e| Status::internal(format!("delete snapshot failed: {e}")))?;
            }
            if let Ok(snap_name_typed) =
                pplx_space_common_core::types::SnapshotName::parse(snapshot_name.to_string())
            {
                match self
                    .local_artifacts
                    .check_out_named_for_delete(&sandbox.id, &snap_name_typed)
                {
                    Ok(lease) => {
                        if let Err(error) = lease.commit() {
                            tracing::warn!(
                                sandbox_id = %sandbox.id,
                                snapshot_name,
                                %error,
                                "deleted snapshot but failed to remove local artifact registry entry"
                            );
                        }
                    }
                    Err(
                        crate::local_artifacts::CheckoutError::NotFound(_)
                        | crate::local_artifacts::CheckoutError::NamedNotFound { .. },
                    ) => {
                        // Row already gone (e.g. reaper raced us); fine.
                    }
                    Err(error) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            snapshot_name,
                            %error,
                            "deleted snapshot but local_artifact row could not be removed"
                        );
                    }
                }
            }

            Ok(Response::new(DeleteSnapshotsResponse {
                deleted: vec![snapshot_name.to_string()],
            }))
        } else {
            Err(Status::invalid_argument(
                "provide snapshot_name or set all=true",
            ))
        }
    }

    pub(super) async fn rollback(
        &self,
        request: Request<RollbackRequest>,
    ) -> Result<Response<RollbackResponse>, Status> {
        let peer = self.authorize_peer(&request, "Rollback", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.get_ref();
        let sandbox_id = &req.sandbox_id;
        let snapshot_name = &req.snapshot_name;
        let auto_restart = req.auto_restart;
        let netns_path = req.netns_path.clone();
        self.authorize_netns_start_context(&request, "Rollback.netns_path", &netns_path)?;
        validate_snapshot_name(snapshot_name)?;
        if is_internal_snapshot_name(snapshot_name) {
            return Err(Status::failed_precondition(
                "cannot rollback to an internal snapshot artifact",
            ));
        }

        let sandbox = self.lookup_sandbox(sandbox_id)?;
        let artifact_lock = self.auto_lifecycle.artifact_lock(&sandbox.id);
        let artifact_guard = artifact_lock.lock().await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.require_snapshot_mutation_ownership(&sandbox.id)?;
        let transferred_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
            &sandbox.id,
            snapshot_name,
        );
        // GC owns a catalog lease, not the per-sandbox artifact lock. Checkout
        // before any destructive RPC, and retain it through the last file read.
        let snapshot_lease = self.checkout_rollback_snapshot(&sandbox.id, snapshot_name)?;
        crate::db::transfer::validate_snapshot_binding(
            std::path::Path::new(&transferred_path),
            &sandbox,
        )
        .map_err(|error| Status::failed_precondition(format!("snapshot identity: {error}")))?;
        let id = sandbox.id.clone();
        let requested_route_identity =
            space_api_route_identity_from_wire(req.route_identity.as_ref())?;
        super::activation_support::validate_route_identity_sandbox(
            &requested_route_identity,
            &sandbox.id,
        )?;
        let published_route_identity = sandbox.space_api_route_identity.clone();
        let preserves_kubernetes_identity = req.route_identity.is_none()
            && published_route_identity.effective_substrate_kind()
                == Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes);
        let requested_route_identity = if preserves_kubernetes_identity {
            published_route_identity
        } else {
            requested_route_identity
        };
        let restart_authority = if req.route_identity.is_some() {
            IdentityFreeDirectActivationAuthority::Denied
        } else {
            Self::identity_free_start_authority(&peer, &sandbox)
        };
        if auto_restart
            && restart_authority == IdentityFreeDirectActivationAuthority::Denied
            && req.route_identity.is_none()
            && !preserves_kubernetes_identity
        {
            return Err(Status::failed_precondition(
                "process snapshot rollback cannot auto-restart a control-plane-owned sandbox; restart it through the control plane",
            ));
        }

        // Check every required input before stopping the VM. A published full
        // transfer may intentionally omit rootfs and is not disk-rollbackable.
        let is_full = snapshot_name.starts_with("f-");
        if is_full {
            self.ensure_snapshot_restore_supported("rollback_full_snapshot")?;
        }
        let (artifact_guard, snapshot_lease) = self
            .blocking_artifact_io(move |_| {
                let ownership = (artifact_guard, snapshot_lease);
                let path = std::path::Path::new(&transferred_path);
                pplx_space_node_runtime_core::snapshot_files::validate_snapshot_rootfs(path)
                    .map_err(|error| {
                        Status::failed_precondition(format!("cannot rollback snapshot: {error}"))
                    })?;
                if is_full {
                    pplx_space_node_runtime_core::snapshot_files::validate_snapshot_memory(path)
                        .map_err(|error| {
                            Status::failed_precondition(format!(
                                "cannot rollback snapshot: {error}"
                            ))
                        })?;
                }
                Ok(ownership)
            })
            .await?;

        let mut node_client = connect_node(&self.node_manager)
            .await
            .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;

        // Validate snapshot exists — check btrfs subvolumes for d-*, directories for f-*
        if is_full {
            let snap_path = format!("{CACHE_DIR}/{id}/snapshots/{snapshot_name}");
            if !std::path::Path::new(&snap_path).is_dir() {
                return Err(Status::not_found(format!(
                    "snapshot not found: {snapshot_name}"
                )));
            }
        } else {
            let sandbox_id = id
                .parse::<SandboxId>()
                .map_err(|error| Status::internal(format!("invalid stored sandbox id: {error}")))?;
            let mut pages = pplx_space_node_mgr_client::SnapshotPages::new(sandbox_id, false);
            let mut exists = false;
            while let Some(page) = pages.next(&mut node_client).await? {
                if page.iter().any(|snapshot| snapshot.name == *snapshot_name) {
                    exists = true;
                    break;
                }
            }
            if !exists {
                return Err(Status::not_found(format!(
                    "snapshot not found: {snapshot_name}"
                )));
            }
        }

        // If sandbox is running or paused, stop it first
        if sandbox.state == SandboxState::Started || sandbox.state == SandboxState::Paused {
            let typed_id = SandboxId::from_str(&id)
                .map_err(|error| Status::internal(format!("invalid sandbox id: {error}")))?;
            let completion = CleanupCompletion::MarkStopped;
            let admission = self
                .admit_lifecycle_cleanup_from_observed_state(
                    typed_id,
                    ObservedCleanupClaim {
                        observed_state: sandbox.state,
                        target_state: SandboxState::Stopping,
                        keep_filesystem: true,
                        completion,
                        persist_intent: false,
                        observed_execution_generation: Some(
                            sandbox.space_api_route_identity.execution_generation,
                        ),
                    },
                )
                .await
                .map_err(super::cleanup::CleanupAdmissionError::into_status)?;
            let expected_execution_generation =
                admission.expected_execution_generation(typed_id)?;

            // Stop snapshot scheduling
            self.snapshot_scheduler.remove_sandbox(&id).await;

            // Stop sidecar monitoring
            self.watchdog.remove_sandbox(&id).await;

            // Cleanup: kill procs, release network/cgroup, keep filesystem
            let cleanup = self
                .dispatch_node_action_using_admission(typed_id, &admission, || async {
                    cleanup_sandbox_via_node(
                        &self.db,
                        &mut node_client,
                        &id,
                        true,
                        "rollback cleanup failed",
                    )
                    .await
                })
                .await;
            match cleanup {
                Ok(CleanupDispatchDisposition::Dispatched) => {}
                Ok(CleanupDispatchDisposition::Superseded) => {
                    return Err(Status::aborted(
                        "snapshot rollback cleanup was superseded by a replacement execution",
                    ));
                }
                Err(error) => {
                    mark_quarantined_best_effort(
                        &self.db,
                        &id,
                        SandboxState::Stopping,
                        SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
                        &format!("rollback cleanup failed: {}", error.message()),
                    );
                    return Err(error);
                }
            }
            if let Some(execution_generation) =
                sandbox.space_api_route_identity.execution_generation
            {
                self.cancel_route_streams_for_generation(typed_id, Some(execution_generation))?;
            }
            let completion_disposition = Self::complete_cleanup_after_node_release(
                &self.db,
                &id,
                completion,
                expected_execution_generation,
                None,
                None,
            )
            .map_err(|error| {
                Status::internal(format!("complete snapshot rollback cleanup: {error}"))
            })?;
            if completion_disposition == CleanupCompletionDisposition::Superseded
                || self.lookup_sandbox(&id)?.state != SandboxState::Stopped
            {
                return Err(Status::aborted(
                    "snapshot rollback cleanup completion was superseded",
                ));
            }
        }

        // The rollback rewrites the rootfs in place, so a `stopped` row's
        // artifact pin stops naming its bytes. Drop it first, under the
        // artifact lock a current-instance backup also takes before it mints
        // or reuses a pin.
        crate::db::drop_stopped_artifact_pin(&*self.db_conn()?, &id)
            .map_err(|e| Status::internal(format!("drop stopped artifact pin: {e}")))?;

        // Perform disk rollback: replace instance/ with the snapshot's disk state.
        // For d-*: the snapshot IS the disk state (btrfs subvolume).
        // For f-*: local captures are subvolumes; imports are plain directories.
        // Node-manager restores the disk; VM state is copied to /snapshot/ below.
        // Memory-only transfers were rejected before any lifecycle mutation.
        let rollback_result = node_client
            .rollback_btrfs_snapshot(
                pplx_space_node_mgr_client::proto::RollbackBtrfsSnapshotRequest {
                    execution_fence: read_node_execution_fence(&self.db, &id)?.map(Into::into),
                    sandbox_id: id.clone(),
                    snapshot_name: snapshot_name.clone(),
                },
            )
            .await;
        if !is_full {
            // Disk-only snapshots must succeed at rollback
            rollback_result.map_err(|e| Status::internal(format!("rollback failed: {e}")))?;
        } else if let Err(e) = rollback_result {
            // Node-manager handles both btrfs subvolumes and plain dirs.
            // If we still get an error, it's a real failure.
            return Err(Status::internal(format!("rollback failed: {e}")));
        }

        // Optionally restart the sandbox
        let mut applied_route_identity = None;
        if auto_restart {
            let route_identity = if preserves_kubernetes_identity {
                // Process-snapshot rollback restarts the same pod execution.
                // Its published Kubernetes identity must survive the local
                // runtime cleanup rather than allocate a new incarnation.
                requested_route_identity
            } else {
                self.resolve_runtime_activation_identity_with_authority(
                    &id,
                    requested_route_identity,
                    false,
                    restart_authority,
                )?
                .identity
            };
            applied_route_identity = space_api_route_identity_to_wire(&route_identity);
            if is_full {
                // Full snapshot: restore VM state from snapshot files instead of fresh boot
                let snap_dir = format!("{CACHE_DIR}/{id}/snapshots/{snapshot_name}");
                let snapshot_dir = format!("{CACHE_DIR}/{id}/snapshot");
                let copy_id = id.clone();
                let writer = self.state_writer_lease("Rollback")?;
                let _artifact_guard = self
                    .blocking_artifact_io(move |service| {
                        // Reflink can fall back to a full memory-image copy. Both
                        // admission and artifact exclusion belong to that work, not
                        // to the future waiting for the blocking thread.
                        let _writer = writer;
                        let _snapshot_lease = snapshot_lease;
                        std::fs::create_dir_all(&snapshot_dir)
                            .map_err(|e| Status::internal(format!("create snapshot dir: {e}")))?;

                        // Reflink vmstate
                        let vmstate_src = format!("{snap_dir}/vmstate");
                        pplx_space_node_runtime_core::reflink_copy(
                            std::path::Path::new(&vmstate_src),
                            std::path::Path::new(&format!("{snapshot_dir}/vmstate")),
                        )
                        .map_err(|e| Status::internal(format!("reflink vmstate: {e}")))?;

                        // Reflink full memory. Memory diffs are not a supported
                        // checkpoint format.
                        let memory_src = format!("{snap_dir}/memory");
                        pplx_space_node_runtime_core::reflink_copy(
                            std::path::Path::new(&memory_src),
                            std::path::Path::new(&format!("{snapshot_dir}/memory")),
                        )
                        .map_err(|e| Status::internal(format!("reflink memory: {e}")))?;
                        replace_optional_snapshot_sidecar(
                            &snap_dir,
                            &snapshot_dir,
                            "runtime.json",
                        )?;
                        replace_optional_snapshot_sidecar(
                            &snap_dir,
                            &snapshot_dir,
                            "snapshot.json",
                        )?;

                        restage_rolled_back_full_snapshot(&*service.db_conn()?, &copy_id)?;
                        Ok(artifact_guard)
                    })
                    .await?;

                // Re-read the sandbox to get updated state
                let sandbox = self.lookup_sandbox(&id)?;
                self.start_from_checkpoint(StartFromCheckpointLaunch {
                    vcpu: None,
                    activation_auto_lifecycle: None,
                    operation_activation_attempt_id: None,
                    sandbox: &sandbox,
                    blocking: false,
                    netns_path: netns_path.clone(),
                    target_state: SandboxState::Started,
                    execution_generation: route_identity.execution_generation,
                    space_api_route_identity: route_identity,
                    route_identity_provenance:
                        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                    open_activation_claim_eligible: false,
                    restored_snapshot: RestoredSnapshotProvenance::default(),
                })
                .await?;
            } else {
                // Disk-only snapshot: fresh boot
                let mut start = disk_snapshot_rollback_start_request(id.clone(), netns_path);
                start.route_identity = space_api_route_identity_to_wire(&route_identity);
                self.start_sandbox_with_authority(start, false, restart_authority)
                    .await?;
            }
        }

        Ok(Response::new(RollbackResponse {
            sandbox_id: id,
            rolled_back_to: snapshot_name.clone(),
            route_identity: applied_route_identity,
        }))
    }
}

/// Translates a guest `StartRequest` into host-client options.
///
/// Every field the proto carries has to be named here: the host client rebuilds
/// a `StartRequest` from these options rather than forwarding the original, so
/// a field omitted at this boundary is silently dropped on its way to the
/// guest.
fn start_process_options(
    operation: &spaced_ops::StartRequest,
) -> Result<pplx_space_spaced_host_client::StartProcessOptions, Status> {
    let invalid = |error: std::fmt::Arguments<'_>| Status::invalid_argument(error.to_string());
    Ok(pplx_space_spaced_host_client::StartProcessOptions {
        command: pplx_space_common_core::types::ProcessCommand::new(operation.command.clone())
            .map_err(|error| invalid(format_args!("{error}")))?,
        cwd: operation
            .cwd
            .clone()
            .map(pplx_space_common_core::types::GuestPath::parse)
            .transpose()
            .map_err(|error| invalid(format_args!("{error}")))?,
        env: pplx_space_spaced_common::parse_env(operation.env.clone()),
        user: operation
            .user
            .clone()
            .map(GuestUserName::parse)
            .transpose()
            .map_err(|error| invalid(format_args!("{error}")))?,
        timeout: proto_duration_to_std(operation.timeout),
        background: operation.background,
        idempotency_key: operation
            .idempotency_key
            .clone()
            .map(pplx_space_common_core::types::IdempotencyKey::parse)
            .transpose()
            .map_err(|error| invalid(format_args!("{error}")))?,
        tag: operation.tag.clone(),
        stdin: operation.stdin,
        pty: operation.pty,
        clean_env: operation.clean_env,
        process_id: operation
            .process_id
            .clone()
            .map(pplx_space_common_core::types::ProcessId::parse)
            .transpose()
            .map_err(|error| invalid(format_args!("{error}")))?,
    })
}

mod rollback_state;

#[cfg(test)]
mod start_process_options_tests;

#[cfg(test)]
mod internal_snapshot_name_tests;
