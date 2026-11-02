use super::*;
use pplx_space_common_core::types::{
    GuestUserName, IdempotencyKey, Pii, ProcessCommand, ProcessId, ProcessSignal,
    validate_linux_guest_path_input,
};

pub(super) type HostFileInfoStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileInfo, Status>> + Send>>;
pub(super) type HostFileChunkStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<FileChunk, Status>> + Send>>;

fn validate_guest_path_input(path: String) -> Result<String, Status> {
    validate_linux_guest_path_input(&path).map_err(invalid_argument_status)?;
    Ok(path)
}

/// Tag a status this host relays from the guest or from host control. Only the
/// server that refuses may mark a status not ready, so a relayed one loses the
/// mark and the trace layers here and upstream still log it as a failure.
pub(super) fn host_relay_status(
    status: Status,
    command: &'static str,
    bridge: &'static str,
) -> Status {
    let mut status = pplx_space_common_grpc::RetryableNotReady::strip(status);
    status
        .metadata_mut()
        .insert("spaced-relay-command", MetadataValue::from_static(command));
    status
        .metadata_mut()
        .insert("spaced-relay-bridge", MetadataValue::from_static(bridge));
    status
}

/// Pull-based forwarding; dropping the consumer drops the upstream stream.
/// Stop after the first transport or validation error. An upstream error loses
/// its not-ready mark, as in [`host_relay_status`].
pub(super) fn relay_stream<In, Out, F>(
    stream: impl futures_util::Stream<Item = Result<In, Status>> + Send + 'static,
    map: F,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Out, Status>> + Send>>
where
    In: Send + 'static,
    Out: Send + 'static,
    F: FnMut(In) -> Result<Out, Status> + Send + 'static,
{
    use futures_util::StreamExt;
    Box::pin(futures_util::stream::unfold(
        (Some(Box::pin(stream)), map),
        |(stream, mut map)| async move {
            let mut stream = stream?;
            let item = stream
                .next()
                .await?
                .map_err(pplx_space_common_grpc::RetryableNotReady::strip)
                .and_then(&mut map);
            let stream = if item.is_err() { None } else { Some(stream) };
            Some((item, (stream, map)))
        },
    ))
}

pub(super) fn map_write_request(request: WriteRequest) -> Result<WriteRequest, Status> {
    match &request.message {
        Some(proto::write_request::Message::Open(open)) => {
            validate_linux_guest_path_input(&open.path).map_err(invalid_argument_status)?;
            open.idempotency_key
                .as_ref()
                .map(IdempotencyKey::parse)
                .transpose()
                .map_err(invalid_argument_status)?;
            open.user
                .as_ref()
                .map(GuestUserName::parse)
                .transpose()
                .map_err(invalid_argument_status)?;
            if open.append && open.idempotency_key.is_none() {
                return Err(Status::failed_precondition(
                    "append write requires idempotency_key for retry safety",
                ));
            }
        }
        Some(proto::write_request::Message::Data(_)) => {}
        None => return Err(Status::invalid_argument("write stream had empty message")),
    }
    Ok(request)
}

pub(super) fn bridge_write_batch_request(
    request: WriteBatchRequest,
) -> Result<WriteBatchRequest, Status> {
    if request.entries.len() > pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES {
        return Err(Status::invalid_argument("too many write_batch entries"));
    }
    request
        .idempotency_key
        .as_ref()
        .map(IdempotencyKey::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    for entry in &request.entries {
        validate_linux_guest_path_input(&entry.path).map_err(invalid_argument_status)?;
    }
    Ok(request)
}

pub(super) fn bridge_watch_request(mut request: WatchRequest) -> Result<WatchRequest, Status> {
    request.path = validate_guest_path_input(request.path)?;
    request.include_globs = proto_globs(&parse_globs(request.include_globs)?);
    request.exclude_globs = proto_globs(&parse_globs(request.exclude_globs)?);
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_create_watcher_request(
    mut request: CreateWatcherRequest,
) -> Result<CreateWatcherRequest, Status> {
    request.root = validate_guest_path_input(request.root)?;
    request.include_globs = proto_globs(&parse_globs(request.include_globs)?);
    request.exclude_globs = proto_globs(&parse_globs(request.exclude_globs)?);
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_normalize_watch_root_request(
    mut request: NormalizeWatchRootRequest,
) -> Result<NormalizeWatchRootRequest, Status> {
    request.path = validate_guest_path_input(request.path)?;
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_enable_file_changes_request(
    mut request: EnableFileChangesRequest,
) -> Result<EnableFileChangesRequest, Status> {
    request.roots = request
        .roots
        .into_iter()
        .map(validate_guest_path_input)
        .collect::<Result<_, _>>()?;
    request.include_globs = proto_globs(&parse_globs(request.include_globs)?);
    request.exclude_globs = proto_globs(&parse_globs(request.exclude_globs)?);
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_process_start_request(
    mut request: StartRequest,
) -> Result<StartRequest, Status> {
    let command = ProcessCommand::new(request.command).map_err(invalid_argument_status)?;
    request.command = Pii::new(&command).expose_pii().argv().to_vec();
    request.env = proto_env(&parse_env(request.env));
    request.cwd = request.cwd.map(validate_guest_path_input).transpose()?;
    request
        .user
        .as_ref()
        .map(GuestUserName::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    request.timeout = validate_optional_timeout(request.timeout)?.map(proto_duration);
    request
        .idempotency_key
        .as_ref()
        .map(IdempotencyKey::parse)
        .transpose()
        .map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_process_connect_request(
    request: ConnectProcessRequest,
) -> Result<ConnectProcessRequest, Status> {
    ProcessId::parse(&request.process_id).map_err(invalid_argument_status)?;
    validate_optional_timeout(request.timeout)?;
    Ok(request)
}

pub(super) fn bridge_process_send_signal_request(
    request: SendSignalRequest,
) -> Result<SendSignalRequest, Status> {
    ProcessId::parse(&request.process_id).map_err(invalid_argument_status)?;
    ProcessSignal::try_from_i32(request.signal).map_err(invalid_argument_status)?;
    Ok(request)
}

pub(super) fn bridge_process_send_input_request(
    request: SendInputRequest,
) -> Result<SendInputRequest, Status> {
    ProcessId::parse(&request.process_id).map_err(invalid_argument_status)?;

    Ok(request)
}

pub(super) fn bridge_process_close_stdin_request(
    request: CloseStdinRequest,
) -> Result<CloseStdinRequest, Status> {
    ProcessId::parse(&request.process_id).map_err(invalid_argument_status)?;

    Ok(request)
}

pub(super) fn bridge_process_resize_pty_request(
    request: ResizePtyRequest,
) -> Result<ResizePtyRequest, Status> {
    ProcessId::parse(&request.process_id).map_err(invalid_argument_status)?;

    Ok(request)
}

pub(super) fn bridge_process_kill_request(
    request: KillRequest,
) -> Result<pplx_space_spaced_common::proto::bridge_guest::SendSignalRequest, Status> {
    let process_id = ProcessId::parse(request.process_id).map_err(invalid_argument_status)?;
    let signal = ProcessSignal::sigkill();
    Ok(
        pplx_space_spaced_common::proto::bridge_guest::SendSignalRequest {
            process_id: process_id.as_str().to_string(),
            signal: signal.as_i32(),
        },
    )
}

pub(super) fn host_resume_reason_to_bridge(reason: i32) -> Result<i32, Status> {
    let reason = proto::ResumeReason::try_from(reason)
        .map_err(|error| Status::invalid_argument(format!("unknown resume_reason: {error}")))?;
    if reason == proto::ResumeReason::Unspecified {
        return Err(Status::invalid_argument("resume_reason is required"));
    }
    Ok(reason.into())
}

pub(super) fn bridge_file_type_to_host(value: i32) -> Result<i32, Status> {
    if value == proto::FileType::Unspecified as i32 {
        return Err(Status::internal("bridge returned unspecified file_type"));
    }
    Ok(value)
}

pub(super) fn bridge_file_event_kind_to_host(value: i32) -> Result<i32, Status> {
    if value == proto::FileEventKind::Unspecified as i32 {
        return Err(Status::internal(
            "bridge returned unspecified file_event kind",
        ));
    }
    Ok(value)
}

pub(super) use pplx_space_spaced_common::validate_timeout;

pub(super) use pplx_space_spaced_common::duration_to_proto;

#[cfg(test)]
mod tests;
