use std::{convert::Infallible, fmt::Display, io::Write, time::Duration};

use axum::{
    body::{Body, Bytes, to_bytes},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::Stream;
use http_body_util::BodyExt;
use pplx_space_common_core::host::disk_admission::CacheDiskAdmissionReason;
use pplx_space_common_grpc::{GuestCapabilityUnsupported, UdsPerSandboxLimitExceeded};
use pplx_space_sbx_mgr_client::proto::{
    SpacedProcessEvent, spaced_process_event, spaced_process_exit,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{EcwProxyError, ResponseKind, ResponseOrigin, backend_transport_reset_reason};

pub(super) const E2B_CONNECT_JSON_BODY_LIMIT_BYTES: usize = 1024 * 1024;
const ECW_BACKEND_BUSY_RETRY_AFTER: Duration = Duration::from_secs(1);

pub(super) async fn decode_connect_json_request<T: DeserializeOwned>(
    body: Body,
) -> Result<T, EcwProxyError> {
    let body = to_bytes(body, E2B_CONNECT_JSON_BODY_LIMIT_BYTES)
        .await
        .map_err(|_| EcwProxyError::bad_request("Invalid Connect request body"))?;
    decode_connect_json_bytes(&body)
}

pub(super) async fn decode_connect_json_stream_request<T: DeserializeOwned>(
    body: Body,
) -> Result<Vec<T>, EcwProxyError> {
    let body = to_bytes(body, E2B_CONNECT_JSON_BODY_LIMIT_BYTES)
        .await
        .map_err(|_| EcwProxyError::bad_request("Invalid Connect request body"))?;
    decode_connect_json_stream_bytes(&body)
}

pub(super) fn decode_connect_json_bytes<T: DeserializeOwned>(
    body: &[u8],
) -> Result<T, EcwProxyError> {
    let payload = decode_connect_json_payload(body)?;
    serde_json::from_slice(payload)
        .map_err(|_| EcwProxyError::bad_request("Invalid Connect JSON request"))
}

pub(super) fn decode_connect_json_stream_bytes<T: DeserializeOwned>(
    body: &[u8],
) -> Result<Vec<T>, EcwProxyError> {
    let Some(first_non_whitespace) = body.iter().position(|byte| !byte.is_ascii_whitespace())
    else {
        return Err(EcwProxyError::bad_request("Empty Connect request body"));
    };

    let remaining = body
        .get(first_non_whitespace..)
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect request body"))?;
    match remaining.first().copied() {
        Some(b'{') => {
            let message = serde_json::from_slice(remaining)
                .map_err(|_| EcwProxyError::bad_request("Invalid Connect JSON request"))?;
            return Ok(vec![message]);
        }
        Some(b'[') => {
            return serde_json::from_slice(remaining)
                .map_err(|_| EcwProxyError::bad_request("Invalid Connect JSON request"));
        }
        Some(_) => {}
        None => return Err(EcwProxyError::bad_request("Empty Connect request body")),
    }

    let mut messages = Vec::new();
    let mut offset = first_non_whitespace;
    while offset < body.len() {
        let envelope = body
            .get(offset..offset + 5)
            .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
        let flags = envelope
            .first()
            .copied()
            .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
        if (flags & 0b0000_0001) != 0 {
            return Err(EcwProxyError::bad_request(
                "Compressed Connect requests are not supported",
            ));
        }

        let length_bytes: [u8; 4] = envelope
            .get(1..5)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let payload_start = offset + 5;
        let payload_end = payload_start
            .checked_add(length)
            .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope length"))?;
        let payload = body
            .get(payload_start..payload_end)
            .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope length"))?;

        if (flags & 0b0000_0010) == 0 {
            let message = serde_json::from_slice(payload)
                .map_err(|_| EcwProxyError::bad_request("Invalid Connect JSON request"))?;
            messages.push(message);
        }
        offset = payload_end;
    }

    if messages.is_empty() {
        return Err(EcwProxyError::bad_request("Empty Connect stream"));
    }
    Ok(messages)
}

pub(super) async fn decode_next_connect_json_stream_message<T: DeserializeOwned>(
    body: &mut Body,
    buffer: &mut Vec<u8>,
) -> Result<Option<T>, EcwProxyError> {
    loop {
        match pop_connect_json_stream_payload(buffer)? {
            Some(ConnectJsonPayload::Message(payload)) => {
                let message = serde_json::from_slice(&payload)
                    .map_err(|_| EcwProxyError::bad_request("Invalid Connect JSON request"))?;
                return Ok(Some(message));
            }
            Some(ConnectJsonPayload::EndStream) => return Ok(None),
            None => {}
        }

        let Some(frame) = body.frame().await else {
            if buffer.iter().all(u8::is_ascii_whitespace) {
                return Ok(None);
            }
            return Err(EcwProxyError::bad_request("Incomplete Connect envelope"));
        };
        let frame =
            frame.map_err(|_| EcwProxyError::bad_request("Invalid Connect request body"))?;
        if let Some(data) = frame.data_ref() {
            buffer.extend_from_slice(data);
        }
    }
}

enum ConnectJsonPayload {
    Message(Vec<u8>),
    EndStream,
}

fn pop_connect_json_stream_payload(
    buffer: &mut Vec<u8>,
) -> Result<Option<ConnectJsonPayload>, EcwProxyError> {
    let first_non_whitespace = buffer.iter().position(|byte| !byte.is_ascii_whitespace());
    let Some(first_non_whitespace) = first_non_whitespace else {
        buffer.clear();
        return Ok(None);
    };
    if first_non_whitespace > 0 {
        buffer.drain(..first_non_whitespace);
    }
    if buffer.len() < 5 {
        return Ok(None);
    }

    let flags = buffer
        .first()
        .copied()
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
    if (flags & 0b0000_0001) != 0 {
        return Err(EcwProxyError::bad_request(
            "Compressed Connect requests are not supported",
        ));
    }

    let length_bytes: [u8; 4] = buffer
        .get(1..5)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length > E2B_CONNECT_JSON_BODY_LIMIT_BYTES {
        return Err(EcwProxyError::bad_request(
            "Connect stream message is too large",
        ));
    }

    let payload_start = 5usize;
    let payload_end = payload_start
        .checked_add(length)
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope length"))?;
    if buffer.len() < payload_end {
        return Ok(None);
    }

    let payload = buffer
        .get(payload_start..payload_end)
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope length"))?
        .to_vec();
    buffer.drain(..payload_end);
    if (flags & 0b0000_0010) != 0 {
        return Ok(Some(ConnectJsonPayload::EndStream));
    }
    Ok(Some(ConnectJsonPayload::Message(payload)))
}

fn decode_connect_json_payload(body: &[u8]) -> Result<&[u8], EcwProxyError> {
    let Some(first_non_whitespace) = body.iter().position(|byte| !byte.is_ascii_whitespace())
    else {
        return Err(EcwProxyError::bad_request("Empty Connect request body"));
    };

    let remaining = body
        .get(first_non_whitespace..)
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect request body"))?;
    match remaining.first().copied() {
        Some(b'{' | b'[') => return Ok(remaining),
        Some(_) => {}
        None => return Err(EcwProxyError::bad_request("Empty Connect request body")),
    }

    if body.len() < 5 {
        return Err(EcwProxyError::bad_request("Invalid Connect envelope"));
    }

    let flags = body
        .first()
        .copied()
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
    if (flags & 0b0000_0001) != 0 {
        return Err(EcwProxyError::bad_request(
            "Compressed Connect requests are not supported",
        ));
    }
    if (flags & 0b0000_0010) != 0 {
        return Err(EcwProxyError::bad_request(
            "Unexpected Connect end-of-stream envelope",
        ));
    }

    let length_bytes: [u8; 4] = body
        .get(1..5)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope"))?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if body.len() != 5 + length {
        return Err(EcwProxyError::bad_request(
            "Invalid Connect envelope length",
        ));
    }

    body.get(5..)
        .ok_or_else(|| EcwProxyError::bad_request("Invalid Connect envelope length"))
}

pub(super) fn process_event_to_connect_message(
    event: SpacedProcessEvent,
    connected_as_start: bool,
    stdout_stream: EcwProcessStdoutStream,
) -> Option<Bytes> {
    match event.event {
        Some(spaced_process_event::Event::Started(_)) => Some(connect_process_start(event.pid)),
        Some(spaced_process_event::Event::Connected(_)) => {
            if !connected_as_start {
                return None;
            }
            Some(connect_process_start(event.pid))
        }
        Some(spaced_process_event::Event::Stdout(stdout)) => {
            Some(connect_process_output(stdout_stream.as_str(), &stdout.data))
        }
        Some(spaced_process_event::Event::Stderr(stderr)) => {
            Some(connect_process_output("stderr", &stderr.data))
        }
        Some(spaced_process_event::Event::Exit(exit)) => Some(connect_process_exit(exit.outcome)),
        None => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EcwProcessStdoutStream {
    Stdout,
    Pty,
}

impl EcwProcessStdoutStream {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Pty => "pty",
        }
    }
}

impl Display for EcwProcessStdoutStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(super) fn connect_json_message(payload: Value, end_stream: bool) -> Bytes {
    let data = serde_json::to_vec(&payload).expect("connect JSON envelope serialization");
    let data_len = u32::try_from(data.len()).expect("connect JSON envelope exceeds u32 length");
    let mut envelope = Vec::with_capacity(5 + data.len());
    let flags = if end_stream { 0b0000_0010 } else { 0 };
    envelope.push(flags);
    envelope.extend_from_slice(&data_len.to_be_bytes());
    envelope.extend_from_slice(&data);
    Bytes::from(envelope)
}

fn connect_json_envelope(
    end_stream: bool,
    payload_capacity: usize,
    write_payload: impl FnOnce(&mut Vec<u8>),
) -> Bytes {
    let flags = if end_stream { 0b0000_0010 } else { 0 };
    let mut envelope = Vec::with_capacity(5 + payload_capacity);
    envelope.push(flags);
    envelope.extend_from_slice(&[0; 4]);
    write_payload(&mut envelope);
    let data_len =
        u32::try_from(envelope.len() - 5).expect("connect JSON envelope exceeds u32 length");
    envelope
        .get_mut(1..5)
        .expect("connect envelope header was reserved")
        .copy_from_slice(&data_len.to_be_bytes());
    Bytes::from(envelope)
}

fn connect_process_start(pid: Option<impl Display>) -> Bytes {
    connect_json_envelope(false, 36, |payload| {
        payload.extend_from_slice(br#"{"event":{"start":{"pid":"#);
        match pid {
            Some(pid) => write!(payload, "{pid}").expect("writing to Vec cannot fail"),
            None => payload.extend_from_slice(b"null"),
        }
        payload.extend_from_slice(br#"}}}"#);
    })
}

fn connect_process_output(stream: &str, data: &[u8]) -> Bytes {
    let encoded_len = data.len().div_ceil(3) * 4;
    connect_json_envelope(false, 32 + stream.len() + encoded_len, |payload| {
        write!(payload, r#"{{"event":{{"data":{{"{stream}":""#)
            .expect("writing to Vec cannot fail");
        let start = payload.len();
        payload.resize(start + encoded_len, 0);
        let written = BASE64_STANDARD
            .encode_slice(
                data,
                payload
                    .get_mut(start..)
                    .expect("base64 output buffer was reserved"),
            )
            .expect("pre-sized base64 output buffer");
        payload.truncate(start + written);
        payload.extend_from_slice(br#""}}}"#);
    })
}

fn connect_process_exit(outcome: Option<spaced_process_exit::Outcome>) -> Bytes {
    connect_json_envelope(false, 128, |payload| {
        payload.extend_from_slice(br#"{"event":{"end":"#);
        match outcome {
            Some(spaced_process_exit::Outcome::Exited(exited)) => {
                write!(
                    payload,
                    r#"{{"exitCode":{},"exited":true,"status":"exited"}}"#,
                    exited.exit_code
                )
                .expect("writing to Vec cannot fail");
            }
            Some(spaced_process_exit::Outcome::Signaled(signaled)) => {
                let signal = signaled.signal.unwrap_or(0);
                write!(
                    payload,
                    r#"{{"exitCode":{},"exited":false,"status":"signaled","error":"process terminated by signal {}"}}"#,
                    128 + signal,
                    signal
                )
                .expect("writing to Vec cannot fail");
            }
            Some(spaced_process_exit::Outcome::TimedOut(_)) => {
                payload.extend_from_slice(
                    br#"{"exitCode":124,"exited":false,"status":"timed_out","error":"process timed out"}"#,
                );
            }
            Some(spaced_process_exit::Outcome::Unknown(_)) | None => {
                payload.extend_from_slice(
                    br#"{"exitCode":1,"exited":false,"status":"unknown","error":"process ended with unknown status"}"#,
                );
            }
        }
        payload.extend_from_slice(br#"}}"#);
    })
}

pub(super) fn connect_error_end_stream(code: tonic::Code, message: &str) -> Bytes {
    connect_json_message(
        json!({
            "error": {
                "code": tonic_code_to_connect_code(code),
                "message": message,
            }
        }),
        true,
    )
}

pub(super) fn connect_json_stream_response<S>(stream: S) -> Response
where
    S: Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
{
    (
        [
            (header::CONTENT_TYPE, "application/connect+json"),
            (
                header::HeaderName::from_static("connect-content-encoding"),
                "identity",
            ),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

pub(super) fn tonic_status_to_ecw_error(status: tonic::Status) -> EcwProxyError {
    let origin = if backend_transport_reset_reason(&status).is_some() {
        ResponseOrigin::Node
    } else {
        ResponseOrigin::Spaced
    };
    let kind = if origin == ResponseOrigin::Node {
        ResponseKind::BackendTransport
    } else {
        ResponseKind::BackendStatus
    };

    if tonic_status_needs_ecw_not_running_shape(&status) {
        return EcwProxyError::sandbox_timeout()
            .with_classification(origin, kind)
            .with_backend_status(status);
    }
    if tonic_status_is_guest_capability_unsupported(&status) {
        return EcwProxyError::new(
            StatusCode::NOT_IMPLEMENTED,
            status.message(),
            ResponseOrigin::Spaced,
            ResponseKind::Validation,
        )
        .with_backend_status(status);
    }
    if tonic_status_is_disk_full(&status) {
        return EcwProxyError::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "Sandbox storage is full",
            ResponseOrigin::Node,
            ResponseKind::BackendStatus,
        )
        .with_backend_status(status);
    }
    if tonic_status_is_per_sandbox_limiter(&status) {
        return EcwProxyError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Sandbox is busy; retry",
            ResponseOrigin::Node,
            ResponseKind::Admission,
        )
        .with_retry_after(ECW_BACKEND_BUSY_RETRY_AFTER)
        .with_backend_status(status);
    }
    // The guest refused before starting anything: say when to come back.
    // A capacity refusal is a 429; any other (a guest shutting down) keeps the
    // status E2B clients expect for its code and gains the hint below.
    let refusal_retry_after = pplx_space_common_grpc::is_refused_before_start(&status).then(|| {
        pplx_space_common_grpc::retry_after(&status)
            .unwrap_or(ECW_BACKEND_BUSY_RETRY_AFTER)
            .clamp(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(60),
            )
    });
    if let Some(try_again) = refusal_retry_after
        && status.code() == tonic::Code::ResourceExhausted
    {
        return EcwProxyError::new(
            StatusCode::TOO_MANY_REQUESTS,
            status.message(),
            ResponseOrigin::Spaced,
            ResponseKind::Admission,
        )
        .with_retry_after(try_again)
        .with_backend_status(status);
    }

    let http_status = match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists => StatusCode::CONFLICT,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::FailedPrecondition | tonic::Code::Aborted => StatusCode::CONFLICT,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        tonic::Code::Unavailable => StatusCode::BAD_GATEWAY,
        tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::OutOfRange
        | tonic::Code::Internal
        | tonic::Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Ok => StatusCode::OK,
    };
    let message = if http_status == StatusCode::INTERNAL_SERVER_ERROR {
        "Internal server error".to_string()
    } else {
        status.message().to_string()
    };

    let error = EcwProxyError::new(http_status, message, origin, kind);
    match refusal_retry_after {
        Some(try_again) => error.with_retry_after(try_again),
        None => error,
    }
    .with_backend_status(status)
}

fn tonic_status_needs_ecw_not_running_shape(status: &tonic::Status) -> bool {
    match status.code() {
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => true,
        tonic::Code::FailedPrecondition => {
            let message = status.message().to_ascii_lowercase();
            message.contains("sandbox is not running")
                || message.contains("cannot wake for access")
                || (message.contains("sandbox is deleting") && message.contains("cannot wake"))
        }
        _ => false,
    }
}

fn tonic_status_is_disk_full(status: &tonic::Status) -> bool {
    if status.code() != tonic::Code::ResourceExhausted {
        return false;
    }
    // The guest names its full disk or quota (EDQUOT included) in metadata;
    // the message checks remain for node-side storage admission.
    if status
        .metadata()
        .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        == Some(pplx_space_common_core::spaced::GUEST_STORAGE_FULL)
    {
        return true;
    }
    let message = status.message().to_ascii_lowercase();
    message.contains("no space left on device")
        || message.contains("enospc")
        || node_disk_admission_rejected_storage(&message)
}

fn tonic_status_is_per_sandbox_limiter(status: &tonic::Status) -> bool {
    UdsPerSandboxLimitExceeded::from_status(status).is_some()
}

fn tonic_status_is_guest_capability_unsupported(status: &tonic::Status) -> bool {
    GuestCapabilityUnsupported::from_status(status).is_some()
}

fn node_disk_admission_rejected_storage(message: &str) -> bool {
    [
        CacheDiskAdmissionReason::DiskUnknownStale,
        CacheDiskAdmissionReason::DiskAvailableBelowThreshold,
        CacheDiskAdmissionReason::BtrfsAllocationPressure,
    ]
    .iter()
    .any(|reason| message.contains(reason.as_str()))
}

fn tonic_code_to_connect_code(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::Cancelled => "canceled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::Ok => "ok",
    }
}

#[cfg(test)]
mod tests;
