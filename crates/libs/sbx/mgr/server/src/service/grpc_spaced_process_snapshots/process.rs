//! Process RPC admission and host forwarding.

use super::*;

impl SandboxManagerService {
    pub(in crate::service) async fn spaced_start_process(
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
        let recovery = pplx_space_spaced_common::recovery::validate_start(operation)?.is_some();
        if !recovery && operation.command.is_empty() {
            return Err(Status::invalid_argument("command is required"));
        }
        let command_arg0 = operation
            .recovery
            .as_ref()
            .map_or(&operation.command, |request| &request.command)
            .first()
            .cloned()
            .unwrap_or_default();
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
        let client = match if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await
        } else {
            self.connect_spaced_host(&sandbox).await
        } {
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
        let start = if recovery {
            futures_util::future::Either::Left(client.recovery_start(operation.clone()))
        } else {
            let options = start_process_options(operation)?;
            futures_util::future::Either::Right(client.start_process_stream_with_options(options))
        };
        use super::super::restart_admission::GuestStartOutcome;
        let stream = match super::super::restart_admission::abandon_start_if_caller_cancelled(start)
            .await
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

    pub(in crate::service) async fn spaced_connect_process(
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
        let recovery = pplx_space_spaced_common::recovery::validate_handle(
            &operation.process_id,
            operation.recovery.as_ref(),
        )?
        .is_some();
        if !recovery && operation.process_id.is_empty() {
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
        let client = if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await?
        } else {
            self.connect_spaced_host(&sandbox).await?
        };
        let stream = connect_process_stream(&client, operation).await?;
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

    pub(in crate::service) async fn spaced_list_processes(
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
        let Some(operation) = req.operation.as_ref() else {
            return Err(Status::invalid_argument(
                "spaced operation payload is required",
            ));
        };
        let include_exited = operation.include_exited;
        let process_id = operation.process_id.clone();
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
            .list_processes(include_exited, process_id.as_deref())
            .await?
            .into_iter()
            .map(process_info_to_spaced)
            .collect();
        Ok(Response::new(SpacedListProcessesResponse { processes }))
    }

    pub(in crate::service) async fn spaced_send_signal(
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
        let recovery = pplx_space_spaced_common::recovery::validate_handle(
            &operation.process_id,
            operation.recovery.as_ref(),
        )?
        .is_some();
        if !recovery && operation.process_id.is_empty() {
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
        let client = if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await?
        } else {
            self.connect_spaced_host(&sandbox).await?
        };
        if recovery {
            client.recovery_send_signal(operation.clone()).await?;
        } else {
            client
                .send_signal(operation.process_id.clone(), signal)
                .await?;
        }
        Ok(Response::new(SpacedSendSignalResponse {}))
    }

    pub(in crate::service) async fn spaced_send_input(
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
        let recovery = pplx_space_spaced_common::recovery::validate_handle(
            &operation.process_id,
            operation.recovery.as_ref(),
        )?
        .is_some();
        if !recovery && operation.process_id.is_empty() {
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
        let client = if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await?
        } else {
            self.connect_spaced_host(&sandbox).await?
        };
        if recovery {
            client.recovery_send_input(operation.clone()).await?;
        } else {
            client
                .send_input(operation.process_id.clone(), operation.data.clone())
                .await?;
        }
        Ok(Response::new(SpacedSendInputResponse {}))
    }

    pub(in crate::service) async fn spaced_close_stdin(
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
        let recovery = pplx_space_spaced_common::recovery::validate_handle(
            &operation.process_id,
            operation.recovery.as_ref(),
        )?
        .is_some();
        if !recovery && operation.process_id.is_empty() {
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
        let client = if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await?
        } else {
            self.connect_spaced_host(&sandbox).await?
        };
        if recovery {
            client.recovery_close_stdin(operation.clone()).await?;
        } else {
            client.close_stdin(operation.process_id.clone()).await?;
        }
        Ok(Response::new(SpacedCloseStdinResponse {}))
    }

    pub(in crate::service) async fn spaced_resize_pty(
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
        let recovery = pplx_space_spaced_common::recovery::validate_handle(
            &operation.process_id,
            operation.recovery.as_ref(),
        )?
        .is_some();
        if !recovery && operation.process_id.is_empty() {
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
        let client = if recovery {
            self.connect_recovery_host(&sandbox, &route_auth).await?
        } else {
            self.connect_spaced_host(&sandbox).await?
        };
        if recovery {
            client.recovery_resize_pty(operation.clone()).await?;
        } else {
            client
                .resize_pty(operation.process_id.clone(), size)
                .await?;
        }
        Ok(Response::new(SpacedResizePtyResponse {}))
    }

    pub(in crate::service) async fn spaced_kill_process(
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
}

/// Translates a guest `StartRequest` into host-client options.
///
/// Every field the proto carries has to be named here: the host client rebuilds
/// a `StartRequest` from these options rather than forwarding the original, so
/// a field omitted at this boundary is silently dropped on its way to the
/// guest.
pub(super) fn start_process_options(
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
