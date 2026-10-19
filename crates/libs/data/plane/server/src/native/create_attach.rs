//! `createAndAttachProcessSession`: one WebSocket that creates a process
//! session through the API and then attaches it.
//!
//! The two-step flow costs the client a round trip to the API before it can
//! open this socket. Here the socket opens first and its first frame carries
//! the create, which the data plane sends to the API's own
//! `createProcessSession` handler over the cluster network, with the caller's
//! credentials. Validation, ownership, idempotency and errors are therefore
//! the API's, unchanged; the data plane only relays the answer and, on
//! success, attaches the session the way `attachSession` would.

use super::*;
use bytes::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use pplx_space_public_api::{ProcessSessionCreateClientFrame, ProcessSessionCreateServerFrame};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// The create can wake a paused sandbox, so it gets the API's budget rather
/// than the attach deadline.
const CREATE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_CREATE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CREATE_RESPONSE_BYTES: usize = 1024 * 1024;
const METRIC_KIND: &str = "process";

/// The request headers the create is authorized and traced with. Everything
/// else the client sent stays here, so the API sees what a direct call with
/// these credentials would show it and nothing the data plane was handed.
const FORWARDED_HEADERS: [&str; 4] = ["authorization", "x-request-id", "traceparent", "tracestate"];
const FORWARDED_HEADER_PREFIX: &str = "x-space-federated-";

#[derive(Debug, thiserror::Error)]
#[error("invalid API internal URL {0:?}: expected an http:// origin")]
pub struct ApiInternalUrlError(String);

/// Sends `createProcessSession` to the API over the cluster network.
#[derive(Clone)]
pub(crate) struct ProcessSessionCreator {
    client: Client<HttpConnector, http_body_util::Full<Bytes>>,
    origin: String,
}

impl ProcessSessionCreator {
    pub(crate) fn new(origin: &str) -> Result<Self, ApiInternalUrlError> {
        let invalid = || ApiInternalUrlError(origin.to_string());
        let uri: Uri = origin.parse().map_err(|_| invalid())?;
        let authority = uri.authority().ok_or_else(invalid)?;
        // A path or query would be silently dropped below, and https would
        // need a TLS client this hop does not carry.
        if uri.scheme_str() != Some("http")
            || !matches!(uri.path(), "" | "/")
            || uri.query().is_some()
        {
            return Err(invalid());
        }
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
        connector.set_nodelay(true);
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
            origin: format!("http://{authority}"),
        })
    }

    async fn create(
        &self,
        sandbox_id: &str,
        forwarded: &HeaderMap,
        idempotency_key: Option<&str>,
        request: &serde_json::Value,
    ) -> Result<CreateOutcome, &'static str> {
        let body = serde_json::to_vec(request).map_err(|_| "invalid create request")?;
        let mut builder = Request::post(format!(
            "{}/v1/sandboxes/{}/process-sessions",
            self.origin,
            encode_path_segment(sandbox_id)
        ))
        .header(header::CONTENT_TYPE, "application/json");
        for (name, value) in forwarded {
            builder = builder.header(name, value);
        }
        if let Some(key) = idempotency_key {
            let key = HeaderValue::from_str(key).map_err(|_| "invalid idempotency key")?;
            builder = builder.header("idempotency-key", key);
        }
        let request = builder
            .body(http_body_util::Full::new(Bytes::from(body)))
            .map_err(|_| "invalid create request")?;

        let response = tokio::time::timeout(CREATE_TIMEOUT, async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|_| "the API could not be reached")?;
            let (parts, body) = response.into_parts();
            let body = http_body_util::Limited::new(body, MAX_CREATE_RESPONSE_BYTES)
                .collect()
                .await
                .map_err(|_| "the API response could not be read")?
                .to_bytes();
            Ok::<_, &'static str>((parts.status, body))
        })
        .await
        .map_err(|_| "the API did not answer in time")??;

        let (status, body) = response;
        if !status.is_success() {
            return Ok(CreateOutcome::Failed {
                status: status.as_u16(),
                error: serde_json::from_slice(&body).ok(),
            });
        }
        let session: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| "the API returned an invalid session")?;
        let handle = CreatedSession::deserialize(&session)
            .map_err(|_| "the API returned an invalid session")?;
        let token = handle
            .attach
            .map(|attach| attach.token)
            .ok_or("the API returned a session without an attachment")?;
        if handle.id.kind() != SessionKind::Process || validate_token_shape(&token).is_err() {
            return Err("the API returned an invalid session");
        }
        Ok(CreateOutcome::Created {
            session,
            session_id: handle.id,
            token,
        })
    }
}

/// Only what attaching needs, so a newer API's extra fields pass through.
#[derive(Deserialize)]
struct CreatedSession {
    id: DataPlaneSessionId,
    attach: Option<CreatedAttach>,
}

#[derive(Deserialize)]
struct CreatedAttach {
    token: String,
}

enum CreateOutcome {
    Created {
        session: serde_json::Value,
        session_id: DataPlaneSessionId,
        token: String,
    },
    Failed {
        status: u16,
        error: Option<serde_json::Value>,
    },
}

/// Percent-encode everything outside the unreserved set, so a decoded path
/// parameter cannot add segments or a query to the API URL, and the API
/// decodes it back to the same value it would have seen directly.
fn encode_path_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn forwarded_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        let name_str = name.as_str();
        if FORWARDED_HEADERS.contains(&name_str) || name_str.starts_with(FORWARDED_HEADER_PREFIX) {
            forwarded.append(name.clone(), value.clone());
        }
    }
    forwarded
}

pub(crate) async fn create_and_attach_process_session(
    State(state): State<DataPlaneState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(sandbox_id): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
    uri: Uri,
) -> Result<Response, AttachError> {
    reject_query_tokens(uri.query())?;
    let Some(creator) = state.session_create.clone() else {
        // What a data plane without this endpoint answers, so a client
        // handles both the same way: fall back to the two-step flow.
        return Ok(data_plane_http_error(
            StatusCode::NOT_FOUND,
            DataPlaneHttpErrorCode::NotFound,
            "process session create-and-attach is not enabled",
            false,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        ));
    };
    if state.drain_started.is_cancelled() {
        return Ok(native_draining_response(METRIC_KIND));
    }
    let forwarded = forwarded_headers(&headers);
    let task = state.upgrade_tasks.token();
    Ok(upgrade.on_upgrade(move |socket| async move {
        let _task = task;
        handle_create_socket(state, creator, peer.ip(), sandbox_id, forwarded, socket).await;
    }))
}

async fn handle_create_socket(
    state: DataPlaneState,
    creator: ProcessSessionCreator,
    source_ip: IpAddr,
    sandbox_id: String,
    forwarded: HeaderMap,
    mut socket: WebSocket,
) {
    // The socket is unauthenticated until the API accepts the create, so it
    // counts against the same per-address cap as one waiting to attach.
    let Ok(guard) = state.unauthenticated.try_register(source_ip) else {
        refuse_unauthenticated_socket(METRIC_KIND, socket).await;
        return;
    };

    let frame = tokio::select! {
        biased;
        () = state.drain_started.cancelled() => None,
        frame = first_create_frame(&mut socket, state.attach_deadline) => Some(frame),
    };
    let Some(frame) = frame else {
        pplx_space_metrics::data_plane::attach(METRIC_KIND, "draining");
        send_restart_close(&mut socket, DRAINING_CLOSE_REASON).await;
        return;
    };
    let (idempotency_key, request) = match frame {
        Ok(frame) => frame,
        Err(error) => {
            pplx_space_metrics::data_plane::attach(METRIC_KIND, error.as_str());
            let _ = socket.send(Message::Text(error_json(error).into())).await;
            let _ = socket.close().await;
            return;
        }
    };

    // Not raced against the drain's start: a create the API may already have
    // committed should be answered, so the client can reattach the session
    // elsewhere. Only the drain deadline cuts it short.
    let created = tokio::select! {
        () = state.stream_shutdown_token.cancelled() => Err("the data plane is shutting down"),
        created = creator.create(&sandbox_id, &forwarded, idempotency_key.as_deref(), &request) => created,
    };
    let (session, session_id, token) = match created {
        Ok(CreateOutcome::Created {
            session,
            session_id,
            token,
        }) => (session, session_id, token),
        Ok(CreateOutcome::Failed { status, error }) => {
            pplx_space_metrics::data_plane::attach(METRIC_KIND, "create_failed");
            send_create_frame(
                &mut socket,
                &ProcessSessionCreateServerFrame::CreateFailed { status, error },
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        Err(message) => {
            pplx_space_metrics::data_plane::attach(METRIC_KIND, "create_unavailable");
            tracing::warn!(error = message, "process session create-and-attach failed");
            send_create_frame(
                &mut socket,
                &ProcessSessionCreateServerFrame::CreateUnavailable {
                    message: message.to_string(),
                },
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    if !send_create_frame(
        &mut socket,
        &ProcessSessionCreateServerFrame::Session { session },
    )
    .await
    {
        return;
    }
    // The create, which can wake the sandbox, has its own budget, so the
    // route lookup gets a fresh attach deadline rather than what is left of
    // the first frame's.
    let attach_deadline = tokio::time::Instant::now() + state.attach_deadline;
    attach_registered_socket(
        state,
        guard,
        session_id,
        Some(token),
        attach_deadline,
        socket,
    )
    .await;
}

async fn first_create_frame(
    socket: &mut WebSocket,
    deadline: Duration,
) -> Result<(Option<String>, serde_json::Value), DataPlaneErrorCode> {
    let message = tokio::time::timeout(deadline, socket.recv())
        .await
        .map_err(|_| DataPlaneErrorCode::AttachDeadlineExceeded)?
        .ok_or(DataPlaneErrorCode::AttachRequired)?
        .map_err(|_| DataPlaneErrorCode::AttachFrameInvalid)?;
    let Message::Text(text) = message else {
        return Err(DataPlaneErrorCode::AttachFrameInvalid);
    };
    if text.len() > MAX_CREATE_FRAME_BYTES {
        return Err(DataPlaneErrorCode::AttachFrameInvalid);
    }
    match serde_json::from_str::<ProcessSessionCreateClientFrame>(&text)
        .map_err(|_| DataPlaneErrorCode::AttachFrameInvalid)?
    {
        ProcessSessionCreateClientFrame::Create {
            idempotency_key,
            request,
        } => Ok((idempotency_key, request)),
    }
}

/// Returns whether the client is still there.
async fn send_create_frame(
    socket: &mut WebSocket,
    frame: &ProcessSessionCreateServerFrame,
) -> bool {
    let Ok(text) = serde_json::to_string(frame) else {
        return false;
    };
    socket.send(Message::Text(text.into())).await.is_ok()
}

#[cfg(test)]
mod tests;
