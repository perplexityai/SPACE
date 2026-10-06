use super::*;
use crate::bridge::BridgeGuestOperationBudget;
use crate::protocol::*;
use pplx_space_common_core::types::ProcessId;

const MAX_GUEST_PROCESS_INFO_ARGUMENTS: usize = 4_096;
const MAX_GUEST_PROCESS_INFO_ENVIRONMENT: usize = 4_096;
const MAX_GUEST_PROCESS_OUTPUT_BYTES: usize = 64 * 1024;

#[cfg(test)]
mod tests;

impl SpacedHostService {
    pub(super) fn process_grpc_service(
        &self,
    ) -> pplx_space_common_grpc::RequestAdmission<ProcessServer<Self>> {
        use pplx_space_spaced_common::process_limits;
        pplx_space_common_grpc::RequestAdmission::new(
            ProcessServer::new(self.clone())
                .max_decoding_message_size(process_limits::MAX_MESSAGE_BYTES),
            self.inner.process_admission.clone(),
            process_limits::PAYLOAD_METHODS,
        )
    }
}

fn map_process_event(event: ProcessEvent) -> Result<ProcessEvent, Status> {
    ProcessId::parse(&event.process_id).map_err(invalid_argument_status)?;
    if matches!(
        &event.event,
        Some(proto::process_event::Event::Stdout(output)
            | proto::process_event::Event::Stderr(output))
            if output.data.len() > MAX_GUEST_PROCESS_OUTPUT_BYTES
    ) {
        return Err(Status::resource_exhausted(
            "guest process output chunk is too large",
        ));
    }
    if let Some(proto::process_event::Event::Exit(exit)) = &event.event
        && exit.outcome.is_none()
    {
        return Err(Status::internal("bridge process exit had no outcome"));
    }
    Ok(event)
}

#[tonic::async_trait]
impl Process for SpacedHostService {
    type StartStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ConnectProcessStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessEvent, Status>> + Send>>;
    type ListProcessesStream =
        std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<ProcessInfo, Status>> + Send>>;

    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<Self::StartStream>, Status> {
        let request = bridge_process_start_request(request.into_inner())
            .map_err(pplx_space_common_grpc::mark_guest_process_error)?;
        let guest_stream = self
            .call_bridge_guest_process(
                "bridge_guest_process_start",
                BridgeGuestOperationBudget::ProcessStreamOpen,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .start(request)
                            .await
                            .map_err(pplx_space_common_grpc::mark_guest_process_error)
                    }
                },
            )
            .await?;
        Ok(Response::new(relay_stream(guest_stream, map_process_event)))
    }

    async fn connect_process(
        &self,
        request: Request<ConnectProcessRequest>,
    ) -> Result<Response<Self::ConnectProcessStream>, Status> {
        let request = bridge_process_connect_request(request.into_inner())
            .map_err(pplx_space_common_grpc::mark_guest_process_error)?;
        let guest_stream = self
            .call_bridge_guest_process(
                "bridge_guest_process_connect",
                BridgeGuestOperationBudget::ProcessStreamOpen,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .connect_process(request)
                            .await
                            .map_err(pplx_space_common_grpc::mark_guest_process_error)
                    }
                },
            )
            .await?;
        Ok(Response::new(relay_stream(guest_stream, map_process_event)))
    }

    async fn list_processes(
        &self,
        _request: Request<ListProcessesRequest>,
    ) -> Result<Response<Self::ListProcessesStream>, Status> {
        let guest_stream = self
            .call_bridge_guest_process(
                "bridge_guest_process_list",
                BridgeGuestOperationBudget::ProcessMetadata,
                |mut client| async move {
                    client
                        .list_processes(
                            pplx_space_spaced_common::proto::bridge_guest::ListProcessesRequest {},
                        )
                        .await
                },
            )
            .await?;
        Ok(Response::new(relay_stream(guest_stream, |info| {
            let process_id = ProcessId::parse(info.process_id).map_err(invalid_argument_status)?;
            if info.command.len() > MAX_GUEST_PROCESS_INFO_ARGUMENTS
                || info.env.len() > MAX_GUEST_PROCESS_INFO_ENVIRONMENT
            {
                return Err(Status::resource_exhausted(
                    "guest process metadata has too many elements",
                ));
            }
            Ok(ProcessInfo {
                process_id: process_id.as_str().to_owned(),
                ..info
            })
        })))
    }

    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let request = bridge_process_send_signal_request(request.into_inner())?;
        self.call_bridge_guest_process_once(
            "bridge_guest_process_send_signal",
            BridgeGuestOperationBudget::ProcessControl,
            |mut client| {
                let request = request.clone();
                async move { client.send_signal(request).await }
            },
        )
        .await?;
        Ok(Response::new(SendSignalResponse {}))
    }

    async fn send_input(
        &self,
        request: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: remove only this gate call.
        self.require_interactive_process_control_support("SendInput")
            .await?;
        let request = bridge_process_send_input_request(request.into_inner())?;
        self.call_bridge_guest_process_once(
            "bridge_guest_process_send_input",
            BridgeGuestOperationBudget::ProcessControl,
            |mut client| {
                let request = request.clone();
                async move { client.send_input(request).await }
            },
        )
        .await?;
        Ok(Response::new(SendInputResponse {}))
    }

    async fn close_stdin(
        &self,
        request: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: remove only this gate call.
        self.require_interactive_process_control_support("CloseStdin")
            .await?;
        let request = bridge_process_close_stdin_request(request.into_inner())?;
        self.call_bridge_guest_process_once(
            "bridge_guest_process_close_stdin",
            BridgeGuestOperationBudget::ProcessControl,
            |mut client| {
                let request = request.clone();
                async move { client.close_stdin(request).await }
            },
        )
        .await?;
        Ok(Response::new(CloseStdinResponse {}))
    }

    async fn resize_pty(
        &self,
        request: Request<ResizePtyRequest>,
    ) -> Result<Response<ResizePtyResponse>, Status> {
        // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: remove only this gate call.
        self.require_interactive_process_control_support("ResizePty")
            .await?;
        let request = bridge_process_resize_pty_request(request.into_inner())?;
        self.call_bridge_guest_process_once(
            "bridge_guest_process_resize_pty",
            BridgeGuestOperationBudget::ProcessControl,
            |mut client| {
                let request = request.clone();
                async move { client.resize_pty(request).await }
            },
        )
        .await?;
        Ok(Response::new(ResizePtyResponse {}))
    }

    async fn kill(&self, request: Request<KillRequest>) -> Result<Response<KillResponse>, Status> {
        let request = bridge_process_kill_request(request.into_inner())?;
        self.call_bridge_guest_process_once(
            "bridge_guest_process_kill",
            BridgeGuestOperationBudget::ProcessControl,
            |mut client| {
                let request = request.clone();
                async move { client.send_signal(request).await }
            },
        )
        .await?;
        Ok(Response::new(KillResponse {}))
    }
}
