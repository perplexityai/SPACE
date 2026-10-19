//! Explicit transitions out of HTTP framing. WebSockets stay bound to their
//! origin. CONNECT is handled separately as an opaque byte relay.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use http::{HeaderValue, Method, Response, StatusCode, Version, header};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio_util::sync::CancellationToken;

use super::super::accounting::{Attempt, Outcome};
use super::super::observation::{Close, Connection, Phase, Side};
use super::super::relay::{self, RelayFault};
use super::SanitizedRequest;

pub(super) enum Handshake {
    WebSocket { accept: HeaderValue },
}

impl Handshake {
    pub(super) fn request<B>(request: &SanitizedRequest<B>) -> Result<Option<Self>, ()> {
        if (request.method() == Method::CONNECT || request.headers().contains_key(header::UPGRADE))
            && (request.headers().contains_key(header::TRANSFER_ENCODING)
                || request
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .is_some_and(|length| length != "0"))
        {
            return Err(());
        }
        if request.method() == Method::CONNECT {
            return Err(());
        }
        if !request.headers().contains_key(header::UPGRADE) {
            return Ok(None);
        }
        if request.version() != Version::HTTP_11
            || request.method() != Method::GET
            || !websocket_headers(
                request.headers().get_all(header::UPGRADE),
                request.headers().get_all(header::CONNECTION),
            )
            || single(request.headers().get_all(header::SEC_WEBSOCKET_VERSION))? != "13"
        {
            return Err(());
        }
        let key = single(request.headers().get_all(header::SEC_WEBSOCKET_KEY))?;
        if STANDARD.decode(key.as_bytes()).map_err(|_| ())?.len() != 16 {
            return Err(());
        }
        // RFC 6455 fixes SHA-1 here; it is a handshake nonce, not certificate trust.
        let mut digest =
            aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY);
        digest.update(key.as_bytes());
        digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = STANDARD.encode(digest.finish()).parse().map_err(|_| ())?;
        Ok(Some(Self::WebSocket { accept }))
    }

    pub(super) fn accepts<B>(&self, response: &Response<B>) -> bool {
        match self {
            Self::WebSocket { accept } => {
                response.status() == StatusCode::SWITCHING_PROTOCOLS
                    && websocket_headers(
                        response.headers().get_all(header::UPGRADE),
                        response.headers().get_all(header::CONNECTION),
                    )
                    && single(response.headers().get_all(header::SEC_WEBSOCKET_ACCEPT))
                        .is_ok_and(|value| value == accept)
            }
        }
    }

    pub(super) const fn as_str(&self) -> &'static str {
        match self {
            Self::WebSocket { .. } => "websocket",
        }
    }
}

fn single(values: header::GetAll<'_, HeaderValue>) -> Result<&HeaderValue, ()> {
    let mut values = values.into_iter();
    let value = values.next().ok_or(())?;
    if values.next().is_some() {
        return Err(());
    }
    Ok(value)
}

fn websocket_headers(
    upgrade: header::GetAll<'_, HeaderValue>,
    connection: header::GetAll<'_, HeaderValue>,
) -> bool {
    single(upgrade).is_ok_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && connection.iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
            })
        })
}

#[derive(Clone, Copy)]
pub(super) enum Kind {
    WebSocket,
    Connect,
}

#[derive(Debug, thiserror::Error)]
enum HandoffError {
    #[error("downstream HTTP upgrade failed")]
    Downstream(#[source] hyper::Error),
    #[error("upstream HTTP upgrade failed")]
    Upstream(#[source] hyper::Error),
    #[error("HTTP upgrade deadline elapsed")]
    Timeout,
    #[error("HTTP session ended")]
    Cancelled,
}

/// Capture the deadline before returning the handshake response to Hyper.
pub(super) fn relay(
    guest: OnUpgrade,
    origin: OnUpgrade,
    shutdown: CancellationToken,
    protocol: &'static str,
    kind: Kind,
    observation: Connection,
) -> impl std::future::Future<Output = ()> + Send {
    let started_at = tokio::time::Instant::now();
    let (budget, idle, binding) = match kind {
        Kind::WebSocket => (
            Duration::from_secs(10),
            Some(Duration::from_secs(60)),
            "tls_http",
        ),
        Kind::Connect => (super::HEADER_TIMEOUT, None, "tls_connect"),
    };
    let deadline = started_at + budget;
    async move {
        observation.phase(Phase::Upgrade);
        let mut forwarding = Attempt::forwarding(protocol, "relay");
        let handoff = tokio::select! {
            biased;
            () = shutdown.cancelled() => Err(HandoffError::Cancelled),
            result = tokio::time::timeout_at(deadline, async {
                tokio::try_join!(
                    async { guest.await.map_err(HandoffError::Downstream) },
                    async { origin.await.map_err(HandoffError::Upstream) },
                )
            }) => result.unwrap_or(Err(HandoffError::Timeout)),
        };
        let (outcome, first_eof, fault, read) = match handoff {
            Err(error) => {
                match &error {
                    HandoffError::Downstream(error) => {
                        observation.failure(Phase::Upgrade, Some(Side::Downstream), error)
                    }
                    HandoffError::Upstream(error) => {
                        observation.failure(Phase::Upgrade, Some(Side::Upstream), error)
                    }
                    HandoffError::Timeout => observation.failure(Phase::Upgrade, None, &error),
                    HandoffError::Cancelled => observation.closed(Side::Downstream, Close::Local),
                }
                let (outcome, status) = match &error {
                    HandoffError::Cancelled => ("session_ended", Outcome::Cancelled),
                    HandoffError::Timeout => ("upgrade_timeout", Outcome::Error),
                    HandoffError::Downstream(_) | HandoffError::Upstream(_) => {
                        ("upgrade_failed", Outcome::Error)
                    }
                };
                if let HandoffError::Downstream(error) | HandoffError::Upstream(error) = &error {
                    tracing::info!(
                        http_reason = super::http_reason(error),
                        "HTTP upgrade handoff failed"
                    );
                }
                forwarding.finish(status, outcome);
                (outcome, "none", None, (0, 0))
            }
            Ok((guest, origin)) => {
                observation.phase(Phase::Relay);
                let report =
                    relay::run(TokioIo::new(guest), TokioIo::new(origin), &shutdown, idle).await;
                report.observe(&observation);
                let (status, outcome) = match &report.result {
                    Ok(()) => (
                        Outcome::Success,
                        match kind {
                            Kind::WebSocket => "upgrade_complete",
                            Kind::Connect => "connect_complete",
                        },
                    ),
                    Err(relay::Error::Cancelled) => (Outcome::Cancelled, "session_ended"),
                    Err(relay::Error::Idle) => (Outcome::Success, "upgrade_idle_timeout"),
                    Err(relay::Error::Io(error)) => {
                        tracing::info!(io_kind = ?error.kind(), raw_os_error = error.raw_os_error(), "HTTP upgrade relay failed");
                        (
                            Outcome::Error,
                            report.fault.map_or("relay_io", RelayFault::as_str),
                        )
                    }
                };
                forwarding.finish(status, outcome);
                (
                    outcome,
                    report.first_eof.map_or("none", |side| side.as_str()),
                    report.fault,
                    report.read,
                )
            }
        };
        pplx_space_metrics::gateway::domain_binding("tcp", binding, outcome, None);
        pplx_space_metrics::gateway::relay_duration_ms(
            protocol,
            outcome,
            first_eof,
            started_at.elapsed().as_secs_f64() * 1_000.0,
        );
        tracing::info!(
            boundary = "forwarding",
            protocol,
            operation = "relay",
            outcome,
            first_eof,
            fault = fault.map(RelayFault::as_str),
            guest_bytes = read.0,
            origin_bytes = read.1,
            "HTTP upgrade relay ended"
        );
    }
}
