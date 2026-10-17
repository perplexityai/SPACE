//! Post-SIGTERM drain of live data-plane streams.
//!
//! Two signals drive it, and they are deliberately separate. The caller's
//! shutdown token starts the drain: readiness fails, the listener stops
//! accepting, and new streams are refused. The stream shutdown token, which
//! every stream takes a child of, ends the streams the drain is waiting for,
//! and is cancelled only when the drain deadline passes.

use super::*;
use axum::extract::ws::{CloseFrame, close_code};

/// How long streams cancelled at the drain deadline get to close before the
/// process exits regardless.
pub(super) const STREAM_CANCEL_GRACE: Duration = Duration::from_secs(5);
/// `Retry-After` on a stream refused while draining. The retry reaches another
/// replica, so it does not need to wait out this one's drain.
pub(super) const DRAINING_RETRY_AFTER: Duration = Duration::from_secs(1);

/// The shutdown inputs [`serve_and_drain`] coordinates.
pub(super) struct DrainLifecycle {
    pub(super) shutdown_token: CancellationToken,
    pub(super) stream_shutdown_token: CancellationToken,
    /// Upgraded connections and other work that outlives its HTTP request.
    pub(super) tasks: TaskTracker,
    pub(super) attached_streams: Arc<AttachedStreamLimiter>,
    /// `None`: no deadline; streams run until they end, the process is
    /// killed, or `force_token` fires.
    pub(super) shutdown_grace: Option<Duration>,
    /// Cancelled by a second stop signal: ends the drain as if its deadline
    /// had passed, without waiting out the stream-open budget first.
    pub(super) force_token: CancellationToken,
}

/// How [`serve_and_drain`] stopped waiting for the streams.
enum DrainEnding {
    Drained(std::io::Result<()>),
    Cut(&'static str),
}

/// Serve until shutdown, then drain.
///
/// On `shutdown_token` the listener stops accepting: new connections are
/// refused from then on, and idle keep-alive connections close. Handlers see
/// the same token as `drain_started`, so a stream request whose headers had
/// already arrived is refused rather than attached. Streams that were already
/// attached keep running. The function returns as soon as every HTTP
/// connection and every tracked upgraded connection has finished. If that
/// takes longer than `shutdown_grace`, the stream token is cancelled and the
/// remaining streams get [`STREAM_CANCEL_GRACE`] to close before it returns
/// anyway; the process exiting closes whatever is left. Without a
/// `shutdown_grace` it waits for the streams until the process is killed or a
/// second stop signal cancels `force_token`, which ends the drain the same way
/// the deadline does.
pub(super) async fn serve_and_drain(
    listener: pplx_space_common_svc::http_listener::NodelayListener,
    app: Router,
    lifecycle: DrainLifecycle,
) -> std::io::Result<()> {
    let DrainLifecycle {
        shutdown_token,
        stream_shutdown_token,
        tasks,
        attached_streams,
        shutdown_grace,
        force_token,
    } = lifecycle;

    let stop_accepting = {
        let shutdown_token = shutdown_token.clone();
        let attached_streams = Arc::clone(&attached_streams);
        async move {
            shutdown_token.cancelled().await;
            tracing::info!(
                active_streams = attached_streams.active_total(),
                "data-plane drain started; refusing new streams"
            );
        }
    };
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(stop_accepting);
    let drained = {
        let tasks = tasks.clone();
        async move {
            server.await?;
            // The server has stopped, so nothing new can be tracked; closing
            // lets `wait` finish once the upgraded connections end.
            tasks.close();
            tasks.wait().await;
            Ok(())
        }
    };
    tokio::pin!(drained);

    // The drain ends one of three ways: the streams finish, the deadline
    // passes, or a second stop signal forces it.
    let deadline = async {
        shutdown_token.cancelled().await;
        tracing::info!(
            active_streams = attached_streams.active_total(),
            shutdown_grace_secs = shutdown_grace.map(|grace| grace.as_secs()),
            "shutdown signal received; draining data-plane streams"
        );
        match shutdown_grace {
            Some(grace) => tokio::time::sleep(grace).await,
            None => std::future::pending().await,
        }
    };
    let ending = tokio::select! {
        biased;
        result = &mut drained => DrainEnding::Drained(result),
        () = force_token.cancelled() => DrainEnding::Cut("a second stop signal forced the drain"),
        () = deadline => DrainEnding::Cut("the drain deadline passed"),
    };
    let result = match ending {
        DrainEnding::Drained(result) => result,
        DrainEnding::Cut(reason) => {
            tracing::warn!(
                active_streams = attached_streams.active_total(),
                reason,
                "cancelling data-plane streams that outlived the drain"
            );
            stream_shutdown_token.cancel();
            tokio::time::timeout(STREAM_CANCEL_GRACE, &mut drained)
                .await
                .unwrap_or_else(|_| {
                    tracing::warn!(
                        active_streams = attached_streams.active_total(),
                        "data-plane streams did not close after cancellation; exiting"
                    );
                    Ok(())
                })
        }
    };
    stream_shutdown_token.cancel();
    result
}

/// How a native WebSocket bridge ended, for the `stream_closed` metric.
pub(super) enum BridgeEnd {
    /// The client, the backend, or a revocation ended the stream.
    Completed,
    /// The drain deadline passed; the client got a 1012 close.
    Shutdown,
}

impl BridgeEnd {
    pub(super) const fn result_label(&self) -> &'static str {
        match self {
            Self::Completed => "ok",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Close a native WebSocket with 1012 (service restart), so the client knows
/// to reconnect, and likely to another replica, rather than seeing an abnormal
/// closure.
pub(super) async fn send_restart_close<S>(sink: &mut S, reason: &'static str)
where
    S: SinkExt<Message> + Unpin,
{
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: reason.into(),
        })))
        .await;
}

pub(super) const DRAINING_CLOSE_REASON: &str = "data plane is draining; retry the attach";
pub(super) const SHUTDOWN_CLOSE_REASON: &str = "data plane is shutting down; re-attach the session";

/// The response a native stream request gets while the data plane drains. A
/// 503 rather than a 429: the refusal says nothing about the caller's load,
/// and the retry lands on a replica that is not leaving.
pub(super) fn native_draining_response(kind: &'static str) -> Response {
    pplx_space_metrics::data_plane::attach(kind, "draining");
    native_draining_error()
}

/// [`native_draining_response`] without the refused-attach metric, for a
/// request that is not a stream.
pub(super) fn native_draining_error() -> Response {
    let mut response = data_plane_http_error(
        StatusCode::SERVICE_UNAVAILABLE,
        DataPlaneHttpErrorCode::ServiceUnavailable,
        "data plane is draining; retry",
        true,
        ResponseOrigin::DataPlane,
        ResponseKind::Admission,
    );
    mark_draining_response(&mut response);
    response
}

/// Run `future`, a backend open for a stream that has not started yet, unless
/// the drain deadline passes first. `None` means the deadline won: the handler
/// answers its client as draining instead of leaving it to the process exit.
pub(super) async fn before_stream_deadline<F: Future>(
    stream_shutdown_token: &CancellationToken,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        () = stream_shutdown_token.cancelled() => None,
        output = future => Some(output),
    }
}

/// Readiness fails once the drain starts. The listener closes at the same
/// moment, so a probe on a new connection is refused rather than answered;
/// the 503 reaches only a probe already in flight on an open connection.
pub(super) async fn readyz(State(state): State<DataPlaneState>) -> Response {
    if state.drain_started.is_cancelled() {
        (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
    } else {
        "ok".into_response()
    }
}

/// Marks a response as a stream refused during the drain, for
/// [`close_draining_http1_connections`].
#[derive(Clone, Copy)]
struct DrainingRefusal;

/// Mark a stream refused during the drain: retry soon, and (on HTTP/1) do not
/// reuse this connection, since it belongs to a replica that is leaving.
pub(super) fn mark_draining_response(response: &mut Response) {
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from(DRAINING_RETRY_AFTER.as_secs()),
    );
    response.extensions_mut().insert(DrainingRefusal);
}

/// Add `Connection: close` to drain refusals on HTTP/1. It lives here rather
/// than in [`mark_draining_response`] because only the request knows its
/// version: HTTP/2 forbids connection-specific headers (hyper strips them with
/// a warning), and its graceful shutdown sends GOAWAY instead.
pub(super) async fn close_draining_http1_connections(
    request: Request<Body>,
    next: Next,
) -> Response {
    let http1 = request.version() < axum::http::Version::HTTP_2;
    let mut response = next.run(request).await;
    if http1 && response.extensions().get::<DrainingRefusal>().is_some() {
        response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
    }
    response
}

#[cfg(test)]
mod tests;
