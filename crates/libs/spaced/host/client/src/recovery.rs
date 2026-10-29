//! Preserve recovery envelopes end to end. Callers own retry deadlines and sequences.

use super::*;

impl SpacedHostClient {
    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_start(
        &self,
        request: proto::StartRequest,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_start", |mut client| {
            let request = request.clone();
            async move { client.start(request).await }
        })
        .await
    }

    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_connect_process(
        &self,
        request: proto::ConnectProcessRequest,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_connect_process", |mut client| {
            let request = request.clone();
            async move { client.connect_process(request).await }
        })
        .await
    }

    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_send_input(
        &self,
        request: proto::SendInputRequest,
    ) -> Result<proto::SendInputResponse, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_send_input", |mut client| {
            let request = request.clone();
            async move { client.send_input(request).await }
        })
        .await
    }

    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_close_stdin(
        &self,
        request: proto::CloseStdinRequest,
    ) -> Result<proto::CloseStdinResponse, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_close_stdin", |mut client| {
            let request = request.clone();
            async move { client.close_stdin(request).await }
        })
        .await
    }

    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_send_signal(
        &self,
        request: proto::SendSignalRequest,
    ) -> Result<proto::SendSignalResponse, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_send_signal", |mut client| {
            let request = request.clone();
            async move { client.send_signal(request).await }
        })
        .await
    }

    /// Forward one recovery request without lowering it to a legacy operation or
    /// retrying it implicitly. The server validates the versioned envelope.
    pub async fn recovery_resize_pty(
        &self,
        request: proto::ResizePtyRequest,
    ) -> Result<proto::ResizePtyResponse, tonic::Status> {
        if request.recovery.is_none() {
            return Err(tonic::Status::invalid_argument(
                "recovery envelope is required",
            ));
        }
        self.call_process("recovery_resize_pty", |mut client| {
            let request = request.clone();
            async move { client.resize_pty(request).await }
        })
        .await
    }
}
