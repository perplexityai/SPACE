//! Process calls: start, reconnect, list, signals, input, PTY resize and kill.

use super::*;

pub(super) const PROCESS_LIST_MAX_RECORDS: usize = 128;

pub(super) const PROCESS_LIST_MAX_ENCODED_BYTES: usize = 4 * 1024 * 1024;

pub(super) const PROCESS_LIST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) fn connect_process_request(
    process_id: &ProcessId,
    timeout: Option<ReadyTimeout>,
    output_cursor: Option<u64>,
) -> ConnectProcessRequest {
    ConnectProcessRequest {
        process_id: process_id.as_str().to_string(),
        timeout: timeout.map(proto_duration),
        output_cursor,
    }
}

impl SpacedHostClient {
    pub async fn start_process_stream_with_options(
        &self,
        options: StartProcessOptions,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        let tag = options.tag.clone();
        let command = options.command;
        let cwd = options.cwd;
        let env = options.env;
        let user = options.user;
        let timeout = options
            .timeout
            .map(ReadyTimeout::new)
            .transpose()
            .map_err(invalid_argument_status)?;
        let background = options.background;
        let stdin = options.stdin;
        let pty = options.pty;
        let clean_env = options.clean_env;
        let assigned_process_id = options.process_id;
        let idempotency_key = Some(match options.idempotency_key {
            Some(key) => key,
            None => IdempotencyKey::parse(uuid::Uuid::now_v7().to_string())
                .map_err(invalid_argument_status)?,
        });
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "process_start",
            self.config.command_retry_policy,
            || {
                let mut client = self.process.clone();
                let command = Pii::new(&command).expose_pii().argv().to_vec();
                let cwd = cwd.as_ref().map(|cwd| cwd.expose().to_string());
                let env = proto_env(&env);
                let user = user.as_ref().map(|user| user.as_str().to_string());
                let idempotency_key = idempotency_key.clone();
                let tag = tag.clone();
                let process_id = assigned_process_id
                    .as_ref()
                    .map(|process_id| process_id.to_string());
                async move {
                    client
                        .start(StartRequest {
                            command,
                            cwd,
                            env,
                            user,
                            timeout: timeout.map(proto_duration),
                            background,
                            idempotency_key: proto_idempotency_key(idempotency_key),
                            tag,
                            stdin,
                            pty,
                            clean_env,
                            process_id,
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn connect_process_stream_with_cursor(
        &self,
        process_id: String,
        timeout: Option<std::time::Duration>,
        output_cursor: Option<u64>,
    ) -> Result<tonic::Streaming<proto::ProcessEvent>, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let timeout = timeout
            .map(ReadyTimeout::new)
            .transpose()
            .map_err(invalid_argument_status)?;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "process_connect",
            self.config.command_retry_policy,
            || {
                let mut client = self.process.clone();
                let process_id = process_id.clone();
                async move {
                    client
                        .connect_process(connect_process_request(
                            &process_id,
                            timeout,
                            output_cursor,
                        ))
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn list_processes(&self) -> Result<Vec<proto::ProcessInfo>, tonic::Status> {
        tokio::time::timeout(
            PROCESS_LIST_TIMEOUT,
            pplx_space_spaced_common::retry_status(
                "spaced host",
                "process_list",
                self.config.command_retry_policy,
                || {
                    let mut client = self.process.clone();
                    async move {
                        let mut stream = client
                            .list_processes(ListProcessesRequest {})
                            .await?
                            .into_inner();
                        let mut processes = Vec::new();
                        let mut encoded_bytes = 0usize;
                        while let Some(process) = stream.message().await? {
                            if processes.len() == PROCESS_LIST_MAX_RECORDS {
                                return Err(tonic::Status::resource_exhausted(
                                    "spaced host returned too many process records",
                                ));
                            }
                            encoded_bytes = encoded_bytes
                                .checked_add(process.encoded_len())
                                .filter(|size| *size <= PROCESS_LIST_MAX_ENCODED_BYTES)
                                .ok_or_else(|| {
                                    tonic::Status::resource_exhausted(
                                        "spaced host process metadata is too large",
                                    )
                                })?;
                            processes.push(process);
                        }
                        Ok(processes)
                    }
                },
            ),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("spaced host process list timed out"))?
    }

    /// Attempts a process signal without automatic replay.
    ///
    /// Use this for security-sensitive control paths where an ambiguous
    /// transport failure must not replay a state-changing request.
    pub async fn send_signal(
        &self,
        process_id: String,
        signal: i32,
    ) -> Result<proto::SendSignalResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let signal = ProcessSignal::try_from_i32(signal).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .send_signal(SendSignalRequest {
                process_id: process_id.as_str().to_string(),
                signal: signal.as_i32(),
            })
            .await
            .map(Response::into_inner)
    }

    /// Attempts a process stdin write without replay so an ambiguous failure cannot
    /// duplicate secret input.
    pub async fn send_input(
        &self,
        process_id: String,
        data: Vec<u8>,
    ) -> Result<proto::SendInputResponse, tonic::Status> {
        let mut request = proto::SendInputRequest { process_id, data };
        let process_id = ProcessId::parse(std::mem::take(&mut request.process_id))
            .map_err(invalid_argument_status)?;
        request.process_id = process_id.as_str().to_string();
        let mut client = self.process.clone();
        client.send_input(request).await.map(Response::into_inner)
    }

    /// Attempts to close process stdin without replay so an ambiguous failure cannot
    /// replay process state transitions.
    pub async fn close_stdin(
        &self,
        process_id: String,
    ) -> Result<proto::CloseStdinResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .close_stdin(proto::CloseStdinRequest {
                process_id: process_id.as_str().to_string(),
            })
            .await
            .map(Response::into_inner)
    }

    pub async fn resize_pty(
        &self,
        process_id: String,
        size: proto::PtySize,
    ) -> Result<proto::ResizePtyResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        self.call_process("process_resize_pty", |mut client| {
            let process_id = process_id.clone();
            async move {
                client
                    .resize_pty(proto::ResizePtyRequest {
                        process_id: process_id.as_str().to_string(),
                        size: Some(size),
                    })
                    .await
            }
        })
        .await
    }

    /// Attempts to force-stop a process without replay. This is intended for bounded
    /// cleanup after an earlier control request had an ambiguous outcome.
    pub async fn kill_process(
        &self,
        process_id: String,
    ) -> Result<proto::KillResponse, tonic::Status> {
        let process_id = ProcessId::parse(process_id).map_err(invalid_argument_status)?;
        let mut client = self.process.clone();
        client
            .kill(KillRequest {
                process_id: process_id.as_str().to_string(),
            })
            .await
            .map(Response::into_inner)
    }
}
