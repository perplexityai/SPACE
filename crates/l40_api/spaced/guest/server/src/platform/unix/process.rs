use super::*;

#[tonic::async_trait]
impl Process for SpacedGuestService {
    type StartStream = <ExecutionService as Process>::StartStream;
    type ConnectProcessStream = <ExecutionService as Process>::ConnectProcessStream;
    type ListProcessesStream = <ExecutionService as Process>::ListProcessesStream;
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<Self::StartStream>, Status> {
        self.require_ready()?;
        self.inner.execution.start(request).await
    }
    async fn connect_process(
        &self,
        request: Request<ConnectProcessRequest>,
    ) -> Result<Response<Self::ConnectProcessStream>, Status> {
        self.require_ready()?;
        self.inner.execution.connect_process(request).await
    }
    async fn list_processes(
        &self,
        request: Request<ListProcessesRequest>,
    ) -> Result<Response<Self::ListProcessesStream>, Status> {
        self.require_ready()?;
        self.inner.execution.list_processes(request).await
    }
    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        self.require_ready()?;
        self.inner.execution.send_signal(request).await
    }
    async fn send_input(
        &self,
        request: Request<SendInputRequest>,
    ) -> Result<Response<SendInputResponse>, Status> {
        self.require_ready()?;
        self.inner.execution.send_input(request).await
    }
    async fn close_stdin(
        &self,
        request: Request<CloseStdinRequest>,
    ) -> Result<Response<CloseStdinResponse>, Status> {
        self.require_ready()?;
        self.inner.execution.close_stdin(request).await
    }
    async fn resize_pty(
        &self,
        request: Request<ResizePtyRequest>,
    ) -> Result<Response<ResizePtyResponse>, Status> {
        self.require_ready()?;
        self.inner.execution.resize_pty(request).await
    }
}
