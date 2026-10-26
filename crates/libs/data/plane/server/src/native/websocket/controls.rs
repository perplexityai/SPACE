//! Client control frames for process and watch sessions.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientFrameAction {
    Continue,
    Close,
}

pub(crate) async fn handle_process_client_control_frame<S>(
    sink: &mut S,
    state: &DataPlaneState,
    attached: &mut AttachedSession,
    process_id: Option<&ProcessId>,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { seq, data_base64 } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            let data = BASE64_STANDARD
                .decode(data_base64)
                .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
            let bytes = data.len() as u64;
            native_node_api_unary(
                state,
                attached,
                SpacedSendInputRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendInputRequest {
                        recovery: None,
                        process_id: process_id.to_string(),
                        data,
                    }),
                },
                |mut client, request| async move { client.spaced_send_input(request).await },
            )
            .await?;
            pplx_space_metrics::data_plane::stream_bytes_in(attached.kind.as_str(), bytes);
            send_server_frame(sink, &DataPlaneServerFrame::InputAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { cols, rows } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            if cols == 0 || rows == 0 {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::InvalidResize,
                        message: "terminal resize rows and cols must be non-zero".to_string(),
                        retryable: false,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            }
            let resize = native_node_api_unary(
                state,
                attached,
                SpacedResizePtyRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(ResizePtyRequest {
                        recovery: None,
                        process_id: process_id.to_string(),
                        size: Some(PtySize {
                            rows: u32::from(rows),
                            cols: u32::from(cols),
                        }),
                    }),
                },
                |mut client, request| async move { client.spaced_resize_pty(request).await },
            )
            .await;
            match resize {
                Ok(_) => {}
                Err(StreamBridgeError::BackendStatus(status))
                    if status.code() == tonic::Code::FailedPrecondition =>
                {
                    tracing::debug!(target: LOG_TARGET, grpc_message = %status.message(), "terminal resize was refused");
                    send_server_frame(
                        sink,
                        &DataPlaneServerFrame::Error {
                            code: DataPlaneErrorCode::ResizeNotSupported,
                            message: RESIZE_REFUSED.to_string(),
                            retryable: false,
                            details: Vec::new(),
                        },
                    )
                    .await
                    .map_err(|_| StreamBridgeError::ClientClosed)?;
                }
                Err(error) => return Err(error),
            }
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { seq, signal } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedSendSignalRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(SendSignalRequest {
                        recovery: None,
                        process_id: process_id.to_string(),
                        signal,
                    }),
                },
                |mut client, request| async move { client.spaced_send_signal(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::CloseStdin { seq } => {
            let Some(process_id) = process_id else {
                send_server_frame(
                    sink,
                    &DataPlaneServerFrame::Error {
                        code: DataPlaneErrorCode::ProcessNotStarted,
                        message: "process has not started yet".to_string(),
                        retryable: true,
                        details: Vec::new(),
                    },
                )
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
                return Ok(ClientFrameAction::Continue);
            };
            native_node_api_unary(
                state,
                attached,
                SpacedCloseStdinRequest {
                    sandbox_id: attached.sandbox_id.to_string(),
                    operation: Some(CloseStdinRequest {
                        recovery: None,
                        process_id: process_id.to_string(),
                    }),
                },
                |mut client, request| async move { client.spaced_close_stdin(request).await },
            )
            .await?;
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}

pub(crate) async fn handle_client_control_frame<S>(
    sink: &mut S,
    text: &str,
) -> Result<ClientFrameAction, StreamBridgeError>
where
    S: SinkExt<Message> + Unpin,
{
    let frame = serde_json::from_str::<DataPlaneClientFrame>(text)
        .map_err(|_| StreamBridgeError::InvalidClientFrame)?;
    match frame {
        DataPlaneClientFrame::Attach { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::AlreadyAttached,
                    message: "session is already attached".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Input { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "interactive stdin is not supported by this backend stream"
                        .to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Signal { .. } | DataPlaneClientFrame::CloseStdin { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::InputNotSupported,
                    message: "process control is not supported by this backend stream".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Heartbeat { seq } => {
            send_server_frame(sink, &DataPlaneServerFrame::ControlAccepted { seq })
                .await
                .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Resize { .. } => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Error {
                    code: DataPlaneErrorCode::ResizeNotSupported,
                    message: "terminal resize is not supported by this backend stream".to_string(),
                    retryable: false,
                    details: Vec::new(),
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Continue)
        }
        DataPlaneClientFrame::Close => {
            send_server_frame(
                sink,
                &DataPlaneServerFrame::Closed {
                    reason: StreamCloseReason::ClientClosed,
                },
            )
            .await
            .map_err(|_| StreamBridgeError::ClientClosed)?;
            Ok(ClientFrameAction::Close)
        }
    }
}
