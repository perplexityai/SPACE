//! Verified TLS transport and ALPN. Callers authorize before establishing the origin.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::Instrument;

use super::accounting::{Attempt, Outcome};
use super::transport;
use crate::certificates::{CertificateError, CertificateLease};
use crate::tls_interception::{
    Admission, HandshakePermit, Refused, TlsHandshakeLeg, TlsInterception,
};
use crate::tls_sni::{ClientAlpnOffer, HttpAlpn};

const HANDSHAKE_BUDGET: Duration = Duration::from_secs(10);

// Display is safe for telemetry; Debug and source chains may contain guest names.
#[derive(Debug, thiserror::Error)]
pub(super) enum TlsTransportError {
    #[error("TLS handshake capacity exhausted")]
    Capacity,
    #[error("TLS connection setup exceeded its ten-second deadline")]
    Timeout,
    #[error("certificate lease expired or its binding changed")]
    Invalidated,
    /// The gateway itself is shutting down. Distinct from
    /// [`Self::Invalidated`], which is the certificate lease ceasing to be
    /// current underneath a live session — most often a routine policy
    /// install, since lease currency is pointer-equality on the installed
    /// policy, and also identity change, CA renewal or expiry.
    #[error("gateway is shutting down")]
    Withdrawn,
    #[error("certificate target differs from the admitted TLS target")]
    Target,
    #[error("TLS protocol is unsupported")]
    Protocol,
    #[error("certificate is unusable")]
    Certificate(#[from] CertificateError),
    #[error("TLS configuration failed")]
    Configuration(#[from] rustls::Error),
    #[error("downstream TLS failed ({kind:?})", kind = .0.kind())]
    Downstream(#[source] io::Error),
    #[error("upstream connect or TLS verification failed ({kind:?})", kind = .0.kind())]
    Upstream(#[source] io::Error),
    #[error("origin TCP connect failed")]
    Connect(#[from] transport::Error),
}

impl TlsTransportError {
    pub(super) fn record(&self, stage: &'static str) {
        let failure = self.failure();
        let io_kind = self.io_error().map(io::Error::kind);
        tracing::error!(
            telemetry_type = "gateway_boundary_failure",
            stage,
            outcome = self.as_str(),
            reason = failure.reason,
            io_kind = ?io_kind,
            tls_alert = failure.alert,
            certificate_validation_reason = failure.certificate,
            "inspected connection failed"
        );
    }

    pub(super) const fn as_str(&self) -> &'static str {
        match self {
            Self::Capacity => "tls_capacity",
            Self::Timeout => "tls_timeout",
            Self::Invalidated => "tls_invalidated",
            Self::Withdrawn => "tls_withdrawn",
            Self::Target => "tls_target_error",
            Self::Protocol => "tls_protocol_error",
            Self::Certificate(_) | Self::Configuration(_) => "tls_configuration_error",
            Self::Downstream(_) => "tls_downstream_error",
            Self::Upstream(_) => "tls_upstream_error",
            Self::Connect(_) => "upstream_connect_error",
        }
    }

    fn io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Downstream(error)
            | Self::Upstream(error)
            | Self::Connect(transport::Error::Io(error)) => Some(error),
            _ => None,
        }
    }

    fn failure(&self) -> TlsFailure {
        match self {
            Self::Capacity => TlsFailure::new("capacity"),
            Self::Timeout => TlsFailure::new("timeout"),
            Self::Invalidated => TlsFailure::new("invalidated"),
            Self::Withdrawn => TlsFailure::new("gateway_shutdown"),
            Self::Target => TlsFailure::new("target_mismatch"),
            Self::Protocol => TlsFailure::new("alpn_mismatch"),
            Self::Certificate(_) => TlsFailure::new("certificate_material"),
            Self::Configuration(_) => TlsFailure::new("configuration"),
            Self::Downstream(_) | Self::Upstream(_) => TlsFailure::new("io"),
            Self::Connect(transport::Error::Timeout) => TlsFailure::new("timeout"),
            Self::Connect(transport::Error::Io(_)) => TlsFailure::new("io"),
        }
    }
}

#[derive(Clone, Copy)]
struct TlsFailure {
    reason: &'static str,
    alert: Option<&'static str>,
    /// Bounded alert class for metrics. The full `alert` name stays in logs.
    alert_class: &'static str,
    certificate: Option<&'static str>,
}

impl TlsFailure {
    const fn new(reason: &'static str) -> Self {
        Self {
            reason,
            alert: None,
            alert_class: "none",
            certificate: None,
        }
    }
}

fn negotiated(alpn: Option<&[u8]>) -> Result<HttpAlpn, TlsTransportError> {
    match selected_alpn(alpn)? {
        // TLS clients predating ALPN still use HTTP/1.1. Hyper parses their
        // application bytes; this never selects an opaque relay.
        None | Some(HttpAlpn::Http1) => Ok(HttpAlpn::Http1),
        Some(HttpAlpn::Http2) => Ok(HttpAlpn::Http2),
    }
}

fn negotiated_upstream(
    client_alpn: ClientAlpnOffer,
    selected: Option<HttpAlpn>,
) -> Result<HttpAlpn, TlsTransportError> {
    match selected {
        Some(protocol) if client_alpn.supports(protocol) => Ok(protocol),
        // An origin that ignores ALPN is legacy HTTP/1.1 only when the
        // guest advertised that fallback or sent no ALPN itself.
        None if client_alpn == ClientAlpnOffer::NoAlpn || client_alpn.supports(HttpAlpn::Http1) => {
            Ok(HttpAlpn::Http1)
        }
        _ => Err(TlsTransportError::Protocol),
    }
}

fn selected_alpn(alpn: Option<&[u8]>) -> Result<Option<HttpAlpn>, TlsTransportError> {
    alpn.map(|protocol| HttpAlpn::from_wire(protocol).ok_or(TlsTransportError::Protocol))
        .transpose()
}

/// The presented leaf and actual SNI must name the same authorized target.
pub(super) async fn accept<S>(
    stream: S,
    lease: &CertificateLease,
    interception: &TlsInterception,
) -> Result<(TlsIo<tokio_rustls::server::TlsStream<S>>, HttpAlpn), TlsTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + HANDSHAKE_BUDGET;
    let _permit = admit_handshake(interception, TlsHandshakeLeg::ClientToNetgw, deadline).await?;
    let mut attempt = HandshakeAttempt::new(TlsHandshakeLeg::ClientToNetgw);
    let result = async {
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(rustls::sign::SingleCertAndKey::from(
            lease.certified_key()?,
        )));
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.max_early_data_size = 0;
        let downstream =
            tokio::time::timeout_at(deadline, TlsAcceptor::from(Arc::new(config)).accept(stream))
                .await
                .map_err(|_| TlsTransportError::Timeout)?
                .map_err(TlsTransportError::Downstream)?;
        let actual_name = downstream
            .get_ref()
            .1
            .server_name()
            .and_then(|name| name.parse::<pplx_space_net_common::DomainName>().ok());
        if actual_name.as_ref() != Some(lease.host()) {
            return Err(TlsTransportError::Target);
        }
        let protocol = negotiated(downstream.get_ref().1.alpn_protocol())?;
        Ok((
            TlsIo::new(downstream, TlsHandshakeLeg::ClientToNetgw),
            protocol,
        ))
    }
    .await;
    attempt.finish(&result);
    result
}

/// Verify the origin leg independently from the guest-facing interception CA.
/// The ten-second budget includes the permit wait, transport setup and the
/// complete handshake.
pub(super) async fn connect<S>(
    transport: impl std::future::Future<Output = Result<S, transport::Error>>,
    lease: &CertificateLease,
    interception: &TlsInterception,
    client_alpn: ClientAlpnOffer,
) -> Result<(TlsIo<tokio_rustls::client::TlsStream<S>>, HttpAlpn), TlsTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    async {
        let deadline = tokio::time::Instant::now() + HANDSHAKE_BUDGET;
        let _permit =
            admit_handshake(interception, TlsHandshakeLeg::NetgwToOrigin, deadline).await?;
        // TCP setup is not a TLS attempt. Start TLS telemetry only after the
        // transport connects, while retaining the original shared deadline.
        let mut tcp_attempt = Attempt::origin_tcp();
        // Drop-aware so the timer shares the counter's denominator: the
        // accounting attempt records cancellation on drop, and a dial
        // abandoned mid-flight must not leave a counted attempt with no
        // duration sample.
        let mut dial = DialTimer::started();
        let stream = match tokio::time::timeout_at(deadline, async {
            if !lease.is_usable() {
                return Err(TlsTransportError::Invalidated);
            }
            transport.await.map_err(TlsTransportError::Connect)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(TlsTransportError::Timeout),
        };
        // The dial and the handshake share one deadline, so without separate
        // timers a `timeout` cannot be attributed to either phase.
        match &stream {
            Ok(_) => {
                tcp_attempt.finish(Outcome::Success, "connected");
                dial.finish("success");
            }
            Err(TlsTransportError::Invalidated) => {
                tcp_attempt.finish(Outcome::Cancelled, "lease_invalidated");
                dial.finish("cancelled");
            }
            Err(error) => {
                tcp_attempt.finish(Outcome::Error, error.failure().reason);
                dial.finish("error");
            }
        }
        let stream = stream?;

        let mut attempt = HandshakeAttempt::new(TlsHandshakeLeg::NetgwToOrigin);
        let result = match tokio::time::timeout_at(deadline, async {
            let name = ServerName::try_from(lease.host().as_str().to_owned())
                .map_err(|_| TlsTransportError::Target)?;
            let offered = client_alpn.supported().ok_or(TlsTransportError::Protocol)?;
            let upstream = TlsConnector::from(interception.upstream_config(offered))
                .connect(name, stream)
                .await
                .map_err(TlsTransportError::Upstream)?;
            let selected = selected_alpn(upstream.get_ref().1.alpn_protocol())?;
            let upstream_protocol = negotiated_upstream(client_alpn, selected)?;
            if !lease.is_usable() {
                return Err(TlsTransportError::Invalidated);
            }
            tracing::Span::current().record("otel.status_code", "OK");
            Ok((
                TlsIo::new(upstream, TlsHandshakeLeg::NetgwToOrigin),
                upstream_protocol,
            ))
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(TlsTransportError::Timeout),
        };
        attempt.finish(&result);
        result
    }
    .instrument(upstream_span(lease.host(), lease.port()))
    .await
}

/// Admit one handshake into the shared pool. A queued admission is recorded on
/// the current span. A pool that stays full fails as `tls_capacity`, the same
/// outcome that an immediate refusal reported before queueing existed. A pool
/// closed by gateway shutdown fails as `tls_withdrawn`.
async fn admit_handshake(
    interception: &TlsInterception,
    leg: TlsHandshakeLeg,
    deadline: tokio::time::Instant,
) -> Result<HandshakePermit, TlsTransportError> {
    match interception.admit_handshake(leg, deadline).await {
        Ok(permit) => {
            match permit.admission() {
                Admission::Immediate => {}
                Admission::Queued { waited } => {
                    tracing::Span::current().record(
                        "handshake_queue_ms",
                        u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                    );
                }
            }
            Ok(permit)
        }
        Err(refused) => {
            let error = match refused {
                Refused::Saturated { .. } => TlsTransportError::Capacity,
                Refused::Closed => TlsTransportError::Withdrawn,
            };
            HandshakeAttempt::record_failure(leg, &error);
            Err(error)
        }
    }
}

fn upstream_span(host: &pplx_space_net_common::DomainName, port: u16) -> tracing::Span {
    tracing::info_span!(
        "tls.client",
        otel.kind = "client",
        otel.status_code = "ERROR",
        handshake_queue_ms = tracing::field::Empty,
        server.address_length = host.as_str().len(),
        server.port = port,
        network.transport = "tcp"
    )
}

/// A dropped handshake future is a terminal cancellation, not a missing sample.
/// Times the origin dial, recording a `cancelled` sample if the dial is
/// abandoned before it returns. Without that, dropping a connection mid-dial
/// leaves the accounting attempt's cancellation counted with no matching
/// duration, so the two share a name but not a denominator.
struct DialTimer {
    started_at: tokio::time::Instant,
    finished: bool,
}

impl DialTimer {
    fn started() -> Self {
        Self {
            started_at: tokio::time::Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, outcome: &str) {
        self.finished = true;
        self.record(outcome);
    }

    fn record(&self, outcome: &str) {
        pplx_space_metrics::gateway::origin_tcp_connect_duration_ms(
            outcome,
            self.started_at.elapsed().as_secs_f64() * 1_000.0,
        );
    }
}

impl Drop for DialTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.record("cancelled");
        }
    }
}

struct HandshakeAttempt {
    leg: TlsHandshakeLeg,
    started_at: tokio::time::Instant,
    finished: bool,
}

impl HandshakeAttempt {
    fn new(leg: TlsHandshakeLeg) -> Self {
        Self {
            leg,
            started_at: tokio::time::Instant::now(),
            finished: false,
        }
    }

    fn finish<T>(&mut self, result: &Result<T, TlsTransportError>) {
        self.finished = true;
        let outcome = match result {
            Ok(_) => {
                pplx_space_metrics::gateway::tls_handshake(
                    self.leg.as_str(),
                    "success",
                    "none",
                    "none",
                );
                "success"
            }
            Err(error) => Self::record_failure(self.leg, error),
        };
        self.record_duration(outcome);
    }

    fn record_duration(&self, outcome: &str) {
        pplx_space_metrics::gateway::tls_handshake_duration_ms(
            self.leg.as_str(),
            outcome,
            self.started_at.elapsed().as_secs_f64() * 1_000.0,
        );
    }

    fn record_failure(leg: TlsHandshakeLeg, error: &TlsTransportError) -> &'static str {
        let failure = error.failure();
        let boundary = match leg {
            TlsHandshakeLeg::ClientToNetgw => "guest_tls",
            TlsHandshakeLeg::NetgwToOrigin => "origin_tls",
        };
        let outcome = if matches!(
            error,
            TlsTransportError::Invalidated | TlsTransportError::Withdrawn
        ) {
            "cancelled"
        } else {
            "error"
        };
        pplx_space_metrics::gateway::tls_handshake(
            leg.as_str(),
            outcome,
            failure.reason,
            failure.alert_class,
        );
        tracing::warn!(
            telemetry_type = "tls_handshake_error",
            boundary,
            leg = leg.as_str(),
            outcome,
            reason = failure.reason,
            io_kind = ?error.io_error().map(io::Error::kind),
            tls_alert = failure.alert,
            certificate_validation_reason = failure.certificate,
            "TLS handshake failed"
        );
        outcome
    }
}

impl Drop for HandshakeAttempt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        pplx_space_metrics::gateway::tls_handshake(
            self.leg.as_str(),
            "cancelled",
            "work_dropped",
            "none",
        );
        self.record_duration("cancelled");
    }
}

/// Reports native I/O failures from an established TLS connection regardless
/// of whether Hyper or an upgraded tunnel currently owns the stream.
pub(super) struct TlsIo<T> {
    inner: T,
    leg: TlsHandshakeLeg,
    error_reported: bool,
}

impl<T> TlsIo<T> {
    fn new(inner: T, leg: TlsHandshakeLeg) -> Self {
        Self {
            inner,
            leg,
            error_reported: false,
        }
    }

    fn record_error(&mut self, operation: &'static str, error: &io::Error) {
        if self.error_reported {
            return;
        }
        let failure = TlsFailure::new("io");
        self.error_reported = true;
        pplx_space_metrics::gateway::tls_connection_error(self.leg.as_str(), failure.reason);
        tracing::warn!(
            telemetry_type = "tls_connection_error",
            boundary = "forwarding",
            leg = self.leg.as_str(),
            operation,
            reason = failure.reason,
            io_kind = ?error.kind(),
            tls_alert = failure.alert,
            certificate_validation_reason = failure.certificate,
            "established TLS connection failed"
        );
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TlsIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Err(error)) = &result {
            self.record_error("read", error);
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TlsIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Err(error)) = &result {
            self.record_error("write", error);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if let Poll::Ready(Err(error)) = &result {
            self.record_error("flush", error);
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        if let Poll::Ready(Err(error)) = &result {
            self.record_error("shutdown", error);
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Err(error)) = &result {
            self.record_error("write_vectored", error);
        }
        result
    }
}

#[cfg(test)]
mod tests;
