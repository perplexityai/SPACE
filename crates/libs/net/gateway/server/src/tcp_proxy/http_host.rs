//! HTTP/1.1 relay for TCP flows admitted by a DNS-domain allow.
//!
//! A CDN address is not a hostname identity: one edge IP can route requests for
//! many unrelated tenants. Hyper owns framing here so every request on a
//! keep-alive connection is checked before it reaches the already-admitted IP.
//! The upstream socket is created only after the first request passes that
//! check, so a rejected authority cannot produce network egress.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode, header, uri::Authority};
use http_body_util::{Either, Empty};
use hyper::body::Incoming;
use hyper::client::conn::http1 as client;
use hyper::server::conn::http1 as server;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use pplx_space_net_common::DomainName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;

use super::routing_headers::{SanitizedBody, SanitizedRequest};
use crate::dns::DnsAllowCache;
use crate::egress::{HostBindingError, authorize_host};
use crate::state::{GatewayNetworkState, TrafficPolicy};

#[derive(Clone)]
pub(super) struct HostBinding {
    sandbox_id: &'static str,
    destination: SocketAddr,
    network_state: GatewayNetworkState,
    dns_allow_cache: DnsAllowCache,
}

impl HostBinding {
    pub(super) fn new(
        sandbox_id: &'static str,
        destination: SocketAddr,
        network_state: GatewayNetworkState,
        dns_allow_cache: DnsAllowCache,
    ) -> Self {
        Self {
            sandbox_id,
            destination,
            network_state,
            dns_allow_cache,
        }
    }

    pub(super) async fn authorize(&self, host: &DomainName) -> Result<(), BindingOutcome> {
        // Established requests retain the bounded PendingPolicy grace.
        let policy = match self.network_state.wait_for_tcp_policy().await {
            TrafficPolicy::Ready(policy) => policy,
            TrafficPolicy::NotReady(_) => return Err(BindingOutcome::PolicyUnavailable),
        };
        authorize_host(&policy, self.destination, host, &self.dns_allow_cache).map_err(|error| {
            match error {
                HostBindingError::HostNotAllowed => BindingOutcome::HostNotAllowed,
                HostBindingError::HostUnattributed => BindingOutcome::HostUnattributed,
                HostBindingError::PolicyDenied(_) => BindingOutcome::PolicyDenied,
            }
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BindingOutcome {
    Allowed,
    InvalidHost,
    InvalidHeaders,
    HostNotAllowed,
    HostUnattributed,
    PolicyDenied,
    PolicyUnavailable,
    UpstreamFailed,
    UpgradeUnsupported,
}

impl BindingOutcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::InvalidHost => "invalid_host",
            Self::InvalidHeaders => "invalid_headers",
            Self::HostNotAllowed => "host_not_allowed",
            Self::HostUnattributed => "host_unattributed",
            Self::PolicyDenied => "policy_denied",
            Self::PolicyUnavailable => "policy_unavailable",
            Self::UpstreamFailed => "upstream_failed",
            Self::UpgradeUnsupported => "upgrade_unsupported",
        }
    }

    pub(super) const fn refusal_status(self) -> StatusCode {
        match self {
            Self::InvalidHeaders => StatusCode::BAD_REQUEST,
            Self::PolicyUnavailable | Self::UpstreamFailed => StatusCode::BAD_GATEWAY,
            Self::Allowed
            | Self::InvalidHost
            | Self::HostNotAllowed
            | Self::HostUnattributed
            | Self::PolicyDenied
            | Self::UpgradeUnsupported => StatusCode::FORBIDDEN,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum Failure {
    #[error("TCP connect failed: {0}")]
    Connect(#[source] super::transport::Error),
    #[error("HTTP/1 handshake failed: {0}")]
    UpstreamHttp(#[source] hyper::Error),
    #[error("guest HTTP failed")]
    GuestHttp,
}

#[derive(Default)]
pub(super) struct Served {
    pub requests: u32,
    pub forwarded: u32,
    pub refused: u32,
    pub upstream_failed: u32,
    pub wire_tx: u64,
    pub wire_rx: u64,
    setup_failure: Option<Failure>,
}

type ForwardBody = Either<Incoming, Empty<hyper::body::Bytes>>;

pub(super) async fn serve<C, F, Fut, U>(
    guest: C,
    binding: HostBinding,
    connect: F,
) -> Result<Served, Failure>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(SocketAddr) -> Fut + Send + 'static,
    Fut: Future<Output = Result<U, super::transport::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let wire_tx = Arc::new(AtomicU64::new(0));
    let wire_rx = Arc::new(AtomicU64::new(0));
    let upstream = Arc::new(Mutex::new(Upstream::Pending(connect)));
    let tally = Arc::new(Mutex::new(Served::default()));

    let serving = server::Builder::new().serve_connection(
        TokioIo::new(guest),
        service_fn(|request: Request<Incoming>| {
            let request = SanitizedRequest::try_from(request.map(Either::Left));
            let upstream = upstream.clone();
            let tally = tally.clone();
            let binding = binding.clone();
            let wire_tx = wire_tx.clone();
            let wire_rx = wire_rx.clone();
            async move {
                let (response, outcome, setup_failure) = match request {
                    Ok(request) => forward(request, upstream, &binding, wire_tx, wire_rx).await,
                    Err(_) => {
                        let outcome = BindingOutcome::InvalidHeaders;
                        pplx_space_metrics::gateway::domain_binding(
                            "tcp",
                            "http_host",
                            outcome.as_str(),
                            None,
                        );
                        (refusal(outcome.refusal_status()), outcome, None)
                    }
                };
                let mut tally = tally.lock().await;
                if tally.setup_failure.is_none() {
                    tally.setup_failure = setup_failure;
                }
                tally.requests += 1;
                if outcome == BindingOutcome::Allowed {
                    tally.forwarded += 1;
                } else if outcome == BindingOutcome::UpstreamFailed {
                    tally.upstream_failed += 1;
                } else {
                    tally.refused += 1;
                }
                Ok::<_, Infallible>(response)
            }
        }),
    );
    let ended = serving.await;
    let mut served = std::mem::take(&mut *tally.lock().await);
    served.wire_tx = wire_tx.load(Ordering::Relaxed);
    served.wire_rx = wire_rx.load(Ordering::Relaxed);
    // Setup can fail only on the first in-flight HTTP/1 request, whose refusal
    // carries Connection: close so Hyper flushes the 502 before this terminal
    // conversion discards its tally. Retrying setup would make that loss material.
    if let Some(failure) = served.setup_failure.take() {
        return Err(failure);
    }
    match ended {
        Ok(()) => Ok(served),
        Err(error) if error.is_parse() => Err(Failure::GuestHttp),
        Err(_) => Ok(served),
    }
}

enum Upstream<F> {
    Pending(F),
    Connected(client::SendRequest<SanitizedBody<ForwardBody>>),
    Failed,
}

async fn forward<F, Fut, U>(
    mut request: SanitizedRequest<ForwardBody>,
    upstream: Arc<Mutex<Upstream<F>>>,
    binding: &HostBinding,
    wire_tx: Arc<AtomicU64>,
    wire_rx: Arc<AtomicU64>,
) -> (Response<ForwardBody>, BindingOutcome, Option<Failure>)
where
    F: FnOnce(SocketAddr) -> Fut + Send + 'static,
    Fut: Future<Output = Result<U, super::transport::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let authorization: Result<Authority, BindingOutcome> = async {
        let (host, authority) =
            validated_request_authority(&request).map_err(|()| BindingOutcome::InvalidHost)?;
        binding.authorize(&host).await?;
        Ok(authority)
    }
    .await;
    let authority = match authorization {
        Ok(authority) => authority,
        Err(outcome) => {
            pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
            return (refusal(outcome.refusal_status()), outcome, None);
        }
    };
    // This relay cannot leave HTTP framing. Refuse before hop-header stripping
    // can turn an upgrade into an ordinary request to the origin.
    if request.headers().contains_key(header::UPGRADE) {
        let outcome = BindingOutcome::UpgradeUnsupported;
        pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
        return (refusal(outcome.refusal_status()), outcome, None);
    }
    if normalize_upstream_routing(&mut request, &authority).is_err() {
        let outcome = BindingOutcome::InvalidHost;
        pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
        return (refusal(outcome.refusal_status()), outcome, None);
    }

    let mut upstream = upstream.lock().await;
    let sender = match connect_upstream(&mut upstream, binding.destination, wire_tx, wire_rx).await
    {
        Ok(sender) => sender,
        Err(error) => {
            let outcome = BindingOutcome::UpstreamFailed;
            let (failure, error_kind) = match &error {
                Failure::Connect(error) => ("connect", error.io_kind()),
                Failure::UpstreamHttp(_) => ("http_handshake", None),
                Failure::GuestHttp => ("guest_http", None),
            };
            tracing::debug!(
                sandbox_id = binding.sandbox_id,
                dst_port = binding.destination.port(),
                address_family = if binding.destination.is_ipv6() {
                    "ipv6"
                } else {
                    "ipv4"
                },
                failure,
                ?error_kind,
                "plaintext HTTP upstream connection failed"
            );
            pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
            return (refusal(outcome.refusal_status()), outcome, Some(error));
        }
    };
    if sender.is_closed() {
        let outcome = BindingOutcome::UpstreamFailed;
        pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
        return (refusal(outcome.refusal_status()), outcome, None);
    }
    let (response, outcome) = match request.send_http1(sender).await {
        Ok(response) if response.status() == StatusCode::SWITCHING_PROTOCOLS => {
            let outcome = BindingOutcome::UpstreamFailed;
            pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
            (refusal(outcome.refusal_status()), outcome)
        }
        Ok(response) => {
            pplx_space_metrics::gateway::domain_binding(
                "tcp",
                "http_host",
                BindingOutcome::Allowed.as_str(),
                None,
            );
            (response.map(Either::Left), BindingOutcome::Allowed)
        }
        Err(_) => {
            let outcome = BindingOutcome::UpstreamFailed;
            pplx_space_metrics::gateway::domain_binding("tcp", "http_host", outcome.as_str(), None);
            (refusal(outcome.refusal_status()), outcome)
        }
    };
    (response, outcome, None)
}

async fn connect_upstream<F, Fut, U>(
    upstream: &mut Upstream<F>,
    destination: SocketAddr,
    wire_tx: Arc<AtomicU64>,
    wire_rx: Arc<AtomicU64>,
) -> Result<&mut client::SendRequest<SanitizedBody<ForwardBody>>, Failure>
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = Result<U, super::transport::Error>>,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match std::mem::replace(upstream, Upstream::Failed) {
        Upstream::Pending(connect) => {
            let io = connect(destination).await.map_err(Failure::Connect)?;
            let metered = MeteredIo {
                inner: io,
                read: wire_rx,
                written: wire_tx,
            };
            // Hyper 1.10.1's HTTP/1 handshake performs no socket I/O and always
            // constructs Ok; peer and send failures surface as upstream_failed.
            // Keep the typed error boundary for later Hyper setup validation.
            let (sender, connection) = client::handshake(TokioIo::new(metered))
                .await
                .map_err(Failure::UpstreamHttp)?;
            // Hyper requires the connection future to be driven while request
            // senders are live. It ends when the sender or socket closes.
            tokio::spawn(connection);
            *upstream = Upstream::Connected(sender);
        }
        Upstream::Connected(sender) => {
            *upstream = Upstream::Connected(sender);
        }
        Upstream::Failed => {
            return Err(Failure::Connect(
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "upstream connection is unavailable",
                )
                .into(),
            ));
        }
    }
    match upstream {
        Upstream::Connected(sender) => Ok(sender),
        Upstream::Pending(_) | Upstream::Failed => Err(Failure::Connect(
            io::Error::other("upstream connection state transition did not install a sender")
                .into(),
        )),
    }
}

/// Make the bytes forwarded upstream express the same authority that passed
/// policy. For absolute-form targets RFC 7230 requires a proxy to replace a
/// conflicting `Host`; leaving the guest's original value in place would let a
/// CDN route on unchecked metadata. Known provider-routing headers have already
/// been removed by the sanitized request boundary.
fn normalize_upstream_routing<B>(
    request: &mut SanitizedRequest<B>,
    authority: &Authority,
) -> Result<(), ()> {
    let value = authority.as_str().parse().map_err(|_| ())?;
    request.set_host(value);
    let origin_form = request
        .uri()
        .path_and_query()
        .map(|path| path.as_str())
        .unwrap_or("/")
        .parse()
        .map_err(|_| ())?;
    *request.uri_mut() = origin_form;
    Ok(())
}

fn canonical_authority(host: &DomainName, port: Option<u16>) -> Result<Authority, ()> {
    match port {
        Some(port) => format!("{}:{port}", host.as_str()),
        None => host.as_str().to_string(),
    }
    .parse()
    .map_err(|_| ())
}

fn validated_request_authority<B>(
    request: &SanitizedRequest<B>,
) -> Result<(DomainName, Authority), ()> {
    let authority = request_authority(request)?;
    let host: DomainName = authority.host().parse().map_err(|_| ())?;
    let authority = canonical_authority(&host, authority.port_u16())?;
    Ok((host, authority))
}

/// Parse the effective HTTP request authority. Absolute-form request-target
/// authority outranks `Host`; accepting the header first would permit a
/// proxy-form request to name an allowed host in the header while targeting a
/// different service. Origin-form requests must carry exactly one `Host`, so
/// the value authorized here cannot differ from one a CDN chooses upstream.
fn request_authority<B>(request: &SanitizedRequest<B>) -> Result<Authority, ()> {
    if let Some(authority) = request.uri().authority() {
        return Ok(authority.clone());
    }
    let mut hosts = request.headers().get_all(header::HOST).iter();
    let host = hosts.next().ok_or(())?;
    if hosts.next().is_some() {
        return Err(());
    }
    host.to_str()
        .ok()
        .and_then(|value| value.parse::<Authority>().ok())
        .ok_or(())
}

fn refusal(status: StatusCode) -> Response<ForwardBody> {
    let mut response = Response::new(Either::Right(Empty::new()));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONNECTION, http::HeaderValue::from_static("close"));
    response
}

pub(super) struct MeteredIo<T> {
    pub(super) inner: T,
    pub(super) read: Arc<AtomicU64>,
    pub(super) written: Arc<AtomicU64>,
}

impl<T: AsyncRead + Unpin> AsyncRead for MeteredIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) {
            let read = buffer.filled().len().saturating_sub(before) as u64;
            self.read.fetch_add(read, Ordering::Relaxed);
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for MeteredIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let result = Pin::new(&mut self.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = result {
            self.written.fetch_add(written as u64, Ordering::Relaxed);
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests;
