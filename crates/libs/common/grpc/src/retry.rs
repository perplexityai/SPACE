use std::future::Future;
use std::time::Duration;

use tonic::{Code, Status};

use crate::context::GrpcRetryContext;
use crate::deadline::deadline_exceeded_status;
use crate::policy::GrpcRetryPolicy;
use crate::status::RateLimited;

const TEMPORARY_TCP_FAILURE_FRAGMENTS: &[&str] = &[
    "connection closed",
    "connection refused",
    "connection reset",
    "failed to connect",
    "goaway",
    "h2 protocol error",
    "http2 error",
    "rst_stream",
    "socket closed",
    "transport error",
    "transport is closing",
];

/// Retry a replay-safe one-shot unary RPC.
///
/// The operation closure is invoked once per attempt. If it constructs a
/// channel/client internally, that connect path should not use
/// `connect_*_channel_with_retry`; map connect failure to
/// `Status::unavailable` and let this function spend the request's single
/// retry budget.
pub async fn retry_grpc_status_with_context<T, F, Fut>(
    service_name: &'static str,
    command: &'static str,
    policy: GrpcRetryPolicy,
    context: GrpcRetryContext,
    operation: F,
) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    retry_grpc_status_with_context_if(service_name, command, policy, context, |_| false, operation)
        .await
}

/// Retry a replay-safe RPC using the standard status policy plus a
/// caller-specific status predicate.
pub async fn retry_grpc_status_with_context_if<T, F, Fut, P>(
    service_name: &'static str,
    command: &'static str,
    policy: GrpcRetryPolicy,
    context: GrpcRetryContext,
    additional_retryable_status: P,
    mut operation: F,
) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
    P: Fn(&Status) -> bool,
{
    let started_at = tokio::time::Instant::now();
    let mut retry_index = 0;

    loop {
        if let Some(status) = context.terminal_status(service_name, command) {
            return Err(status);
        }

        match run_with_context(&context, service_name, command, "running RPC", operation()).await {
            Ok(response) => return Ok(response),
            Err(status) => {
                // A server refusal made before the request started goes back to
                // the caller, whatever the caller's own predicate would say.
                let delay = if crate::is_refused_before_start(&status) {
                    None
                } else {
                    retry_delay_for_status(policy, retry_index, &status).or_else(|| {
                        additional_retryable_status(&status)
                            .then(|| policy.backoff_delay(retry_index))
                            .flatten()
                    })
                };
                let Some(delay) = delay else {
                    return Err(status);
                };

                if let Err(deadline) = retry_delay_fits_budget(
                    policy,
                    started_at,
                    &context,
                    delay,
                    service_name,
                    command,
                ) {
                    return Err(deadline.unwrap_or(status));
                }

                retry_index += 1;
                pplx_space_grpc_metrics::client_retry(
                    service_name,
                    command,
                    "rpc",
                    retry_reason(&status),
                );
                pplx_space_observability::with_peer_service!(tracing::debug!(
                    peer.service = service_name,
                    command,
                    status_code = ?status.code(),
                    retry = retry_index,
                    retry_in = ?delay,
                    "gRPC command failed; will retry"
                ));
                context.sleep(delay, service_name, command).await?;
            }
        }
    }
}

pub fn should_reconnect_status(status: &Status) -> bool {
    // Reconnect is intentionally broader than retry: a caller may not be
    // allowed to replay the same RPC, but the cached channel can still be
    // suspect enough that the next independent RPC should dial again. A
    // refusal the server answered proves the channel works.
    if crate::is_refused_before_start(status) {
        return false;
    }
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::Cancelled | Code::DeadlineExceeded
    ) || is_temporary_tcp_failure(status)
}

/// True when the failure was generated client-side while connecting, before
/// any request bytes were transmitted: the local transport error chain
/// contains a [`crate::ConnectPhaseError`], which SPACE's TCP connector puts
/// on every failure to resolve or dial its peer, or an [`std::io::Error`]
/// whose kind (`ConnectionRefused`, or `NotFound` for an absent Unix socket)
/// proves the dial itself failed. A bare `TimedOut` proves nothing: the
/// kernel also reports it when an established connection gives up
/// retransmitting a request it already sent. TLS failures carry no marker and
/// do not count.
///
/// This is a typed check, not text matching: a `Status` decoded from wire
/// trailers carries no local error source chain, so a server echoing
/// connect-failure wording (for example, proxying a guest's "connection
/// refused") can never satisfy it. A request that fails this way is
/// replay-safe for every RPC, idempotent or not, because it was never sent.
/// [`client_connect_failure_cause`] says which proof the chain carried.
pub fn is_client_connect_failure(status: &Status) -> bool {
    client_connect_failure_cause(status).is_some()
}

/// Which proof made [`is_client_connect_failure`] count a failure as never
/// sent: the first of the marker or a dial-failure `io::Error` that its chain
/// walk meets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientConnectFailureCause {
    /// A [`crate::ConnectPhaseError`]: SPACE's TCP connector failed to
    /// resolve or dial the peer, whatever the underlying error, a refusal
    /// included.
    ConnectPhase,
    /// A refused dial outside SPACE's TCP connector, such as a Unix socket
    /// nothing listens on or tonic's own connector.
    Refused,
    /// A Unix socket that does not exist.
    NotFound,
}

impl ClientConnectFailureCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectPhase => "connect_phase",
            Self::Refused => "refused",
            Self::NotFound => "not_found",
        }
    }
}

impl std::fmt::Display for ClientConnectFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why [`is_client_connect_failure`] counts `status` as never sent, or `None`
/// when it does not. The same check, so a caller that needs the reason, for
/// a metric, cannot disagree with the decision.
pub fn client_connect_failure_cause(status: &Status) -> Option<ClientConnectFailureCause> {
    if status.code() != Code::Unavailable {
        return None;
    }
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if error.is::<crate::ConnectPhaseError>() {
            return Some(ClientConnectFailureCause::ConnectPhase);
        }
        if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
            return match io_error.kind() {
                std::io::ErrorKind::ConnectionRefused => Some(ClientConnectFailureCause::Refused),
                std::io::ErrorKind::NotFound => Some(ClientConnectFailureCause::NotFound),
                _ => None,
            };
        }
        source = error.source();
    }
    None
}

pub fn is_temporary_tcp_failure(status: &Status) -> bool {
    // A server refusing the request before starting it (an admission limit,
    // a guest shutting down) answered over a working transport; the caller,
    // not the hop, decides whether to retry.
    if crate::is_refused_before_start(status) {
        return false;
    }
    if matches!(status.code(), Code::Unavailable | Code::Unknown) {
        return true;
    }
    if !matches!(status.code(), Code::Internal | Code::Cancelled) {
        return false;
    }
    let mut messages = vec![status.message().to_ascii_lowercase()];
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        messages.push(error.to_string().to_ascii_lowercase());
        source = error.source();
    }
    messages.iter().any(|message| {
        TEMPORARY_TCP_FAILURE_FRAGMENTS
            .iter()
            .any(|fragment| message.contains(fragment))
    })
}

pub(crate) async fn run_with_context<T, Fut>(
    context: &GrpcRetryContext,
    service_name: &'static str,
    command: &'static str,
    phase: &'static str,
    future: Fut,
) -> Result<T, Status>
where
    Fut: Future<Output = Result<T, Status>>,
{
    match context.remaining() {
        Some(remaining) if remaining.is_zero() => {
            Err(deadline_exceeded_status(service_name, command, phase))
        }
        Some(remaining) => match tokio::time::timeout(remaining, future).await {
            Ok(result) => result,
            Err(_) => Err(deadline_exceeded_status(service_name, command, phase)),
        },
        None => future.await,
    }
}

pub(crate) fn retry_delay_for_status(
    policy: GrpcRetryPolicy,
    retry_index: usize,
    status: &Status,
) -> Option<Duration> {
    // A refusal made before the request started is the caller's to retry: its
    // retry hint is for the caller, not a cue for this hop to loop on it.
    if crate::is_refused_before_start(status) {
        return None;
    }
    match status.code() {
        // Tonic can surface HTTP/2 connection resets and closed transports as
        // Unknown when the server disappears before trailers are available.
        Code::Unavailable | Code::Unknown => policy.backoff_delay(retry_index),
        Code::Internal | Code::Cancelled if is_temporary_tcp_failure(status) => {
            policy.backoff_delay(retry_index)
        }
        Code::ResourceExhausted => {
            let retry_after = RateLimited::from_status(status)?.try_again?;
            if retry_index >= policy.max_retries || retry_after > policy.max_server_retry_after {
                return None;
            }
            Some(retry_after)
        }
        _ => None,
    }
}

pub(crate) fn retry_reason(status: &Status) -> &'static str {
    match status.code() {
        Code::Unavailable => "unavailable",
        Code::Unknown => "unknown",
        Code::Internal | Code::Cancelled if is_temporary_tcp_failure(status) => "temporary_tcp",
        Code::ResourceExhausted => "resource_exhausted",
        Code::Cancelled => "cancelled",
        Code::DeadlineExceeded => "deadline_exceeded",
        _ => "other",
    }
}

pub(crate) fn retry_delay_fits_budget(
    policy: GrpcRetryPolicy,
    started_at: tokio::time::Instant,
    context: &GrpcRetryContext,
    delay: Duration,
    service_name: &'static str,
    command: &'static str,
) -> Result<(), Option<Status>> {
    let elapsed = started_at.elapsed();
    if elapsed >= policy.max_elapsed || delay > policy.max_elapsed.saturating_sub(elapsed) {
        return Err(None);
    }
    if let Some(remaining) = context.remaining()
        && delay > remaining
    {
        return Err(Some(deadline_exceeded_status(
            service_name,
            command,
            "waiting to retry",
        )));
    }
    Ok(())
}
