//! Lets `POST /v1/sandboxes/{sandbox_id}/commands` answer with the new
//! command's event stream, so a client creates and attaches in one round trip.
//!
//! The API opens the same data-plane events stream a client would, with the
//! session token it just minted, and relays the bytes unchanged. Any failure to
//! open it degrades to the ordinary JSON handle: the client then attaches
//! itself, exactly as in the two-request flow.

use std::time::Duration;

use axum::{
    body::{Body, Bytes},
    http::{HeaderValue, Request, StatusCode, Uri, header},
    response::Response,
};

use futures_util::StreamExt;
use http_body_util::{BodyExt, Empty};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use pplx_space_public_api::{PublicCommand, data_plane_session_events_path};
use tokio_util::sync::CancellationToken;

pub const DATA_PLANE_INTERNAL_URL_ENV: &str = "SPACE_DATA_PLANE_INTERNAL_URL";

/// Event name and frame `type` of the first frame, which carries the command
/// handle. Tagged like every data-plane frame so clients that dispatch on
/// `type` skip it as unknown. It has no `id`, so `Last-Event-ID` resumption on
/// the events endpoint is unaffected.
pub const COMMAND_EVENT: &str = "command";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
// The data plane answers once the process has started. Past this, the client
// is better served by the handle and its own attach than by a stalled request.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
#[error("{DATA_PLANE_INTERNAL_URL_ENV} must be an http:// origin without path or query, got {0:?}")]
pub struct CommandStreamConfigError(String);

#[derive(Debug, thiserror::Error)]
enum OpenError {
    #[error("command has no attach token")]
    NoAttach,
    #[error("invalid data-plane request: {0}")]
    Request(#[from] axum::http::Error),
    #[error("data-plane request failed: {0}")]
    Transport(#[from] hyper_util::client::legacy::Error),
    #[error("data-plane stream did not open within {OPEN_TIMEOUT:?}")]
    Timeout,
    #[error("data-plane stream answered {status} ({content_type:?})")]
    Rejected {
        status: StatusCode,
        content_type: Option<HeaderValue>,
    },
    #[error("failed to serialize command: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Relays command event streams from the data plane. Disabled (`None` from
/// [`Self::from_env`]) unless the data plane's in-cluster origin is configured.
#[derive(Clone)]
pub struct CommandStreamProxy {
    client: Client<HttpConnector, Empty<Bytes>>,
    origin: String,
    shutdown: CancellationToken,
}

impl CommandStreamProxy {
    pub fn from_env(shutdown: CancellationToken) -> Result<Option<Self>, CommandStreamConfigError> {
        match std::env::var(DATA_PLANE_INTERNAL_URL_ENV) {
            Ok(value) if !value.trim().is_empty() => Self::new(value.trim(), shutdown).map(Some),
            _ => Ok(None),
        }
    }

    pub fn new(
        origin: &str,
        shutdown: CancellationToken,
    ) -> Result<Self, CommandStreamConfigError> {
        let invalid = || CommandStreamConfigError(origin.to_string());
        let uri: Uri = origin.parse().map_err(|_| invalid())?;
        let authority = uri.authority().ok_or_else(invalid)?;
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
            shutdown,
        })
    }

    /// Returns a `201 text/event-stream` response for `command`, or `None` when
    /// the stream could not be opened and the caller should answer with JSON.
    pub(crate) async fn stream(&self, command: &PublicCommand) -> Option<Response> {
        match self.open(command).await {
            Ok(response) => Some(response),
            Err(error) => {
                tracing::warn!(
                    session_id = %command.session_id,
                    error = %error,
                    "command event stream unavailable; returning the command handle"
                );
                None
            }
        }
    }

    async fn open(&self, command: &PublicCommand) -> Result<Response, OpenError> {
        let token = &command.attach.as_ref().ok_or(OpenError::NoAttach)?.token;
        let request = Request::get(format!(
            "{}{}",
            self.origin,
            data_plane_session_events_path(command.session_id)
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::ACCEPT, "text/event-stream")
        .body(Empty::new())?;
        let upstream = tokio::time::timeout(OPEN_TIMEOUT, self.client.request(request))
            .await
            .map_err(|_| OpenError::Timeout)??;
        let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
        let is_event_stream = content_type
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));
        if !upstream.status().is_success() || !is_event_stream {
            return Err(OpenError::Rejected {
                status: upstream.status(),
                content_type,
            });
        }

        let frame =
            serde_json::json!({ "type": COMMAND_EVENT, "command": serde_json::to_value(command)? });
        let first = Bytes::from(format!("event: {COMMAND_EVENT}\ndata: {frame}\n\n"));
        let shutdown = self.shutdown.clone();
        // A relayed stream would otherwise hold the graceful drain open for
        // as long as the command runs; ending it lets the client re-attach to
        // the events endpoint, which the data plane keeps serving.
        let body = futures_util::stream::once(async move { Ok::<_, hyper::Error>(first) })
            .chain(upstream.into_body().into_data_stream())
            .take_until(async move { shutdown.cancelled().await });

        Response::builder()
            .status(StatusCode::CREATED)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(body))
            .map_err(OpenError::Request)
    }
}

#[cfg(test)]
mod tests;
