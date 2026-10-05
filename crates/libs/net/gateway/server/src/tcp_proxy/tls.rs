//! Verified TLS transport. HTTP authorization owns every upstream I/O boundary.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::Instrument;

use super::accounting::{Attempt, Outcome};
use super::admission::AdmittedTcp;
use super::http_host::{HostBinding, MeteredIo};
use super::{TcpProxy, relay, tls_http};
use crate::certificates::{CertificateError, CertificateLease};
use crate::stats::ProxyStats;
use crate::tls_interception::{TlsHandshakeLeg, TlsInterception, TlsInterceptionError};
use crate::tls_sni::{ClientAlpnOffer, HttpAlpn};

const HANDSHAKE_BUDGET: Duration = Duration::from_secs(10);

// Display is safe for telemetry; Debug and source chains may contain guest names.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsTransportError {
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
    #[error("HTTP connection failed")]
    Http(#[from] hyper::Error),
}

impl TlsTransportError {
    pub(super) fn record(&self, stage: &'static str) {
        self.record_processing(stage, stage);
    }

    fn record_processing(&self, stage: &'static str, processing_stage: &'static str) {
        let failure = self.failure();
        let http = match self {
            Self::Http(error) => HttpFailure::classify(error),
            _ => HttpFailure::NOT_HTTP,
        };
        let io_kind = self.io_error().map(io::Error::kind).or(http.io_kind);
        tracing::error!(
            telemetry_type = "gateway_boundary_failure",
            stage,
            processing_stage,
            outcome = self.as_str(),
            reason = failure.reason,
            io_kind = ?io_kind,
            tls_alert = failure.alert,
            certificate_validation_reason = failure.certificate,
            http_reason = http.reason,
            h2_reason = http.h2_reason,
            h2_origin = http.h2_origin,
            "inspected connection failed"
        );
    }

    pub(crate) const fn as_str(&self) -> &'static str {
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
            Self::Http(_) => "http_connection_error",
        }
    }

    fn io_error(&self) -> Option<&io::Error> {
        match self {
            Self::Downstream(error) | Self::Upstream(error) => Some(error),
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
            Self::Downstream(error) | Self::Upstream(error) => {
                tls_failure_from_io(error).unwrap_or_else(|| TlsFailure::new("io"))
            }
            Self::Http(_) => TlsFailure::new("http"),
        }
    }
}

/// Bounded HTTP failure class. Hyper's `Display` and source chain can carry
/// peer-supplied text, so only enum-like fields leave this type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct HttpFailure {
    pub(super) reason: &'static str,
    /// Socket error kind found anywhere in the cause chain, including inside h2.
    pub(super) io_kind: Option<io::ErrorKind>,
    /// RFC 9113 error code, when HTTP/2 produced the failure.
    pub(super) h2_reason: Option<&'static str>,
    /// Who raised the HTTP/2 error: `remote` (peer frame), `local` (hyper/h2
    /// user API) or `library` (h2 detected a peer protocol violation).
    pub(super) h2_origin: Option<&'static str>,
}

impl HttpFailure {
    const NOT_HTTP: Self = Self {
        reason: "not_http",
        io_kind: None,
        h2_reason: None,
        h2_origin: None,
    };

    /// Ordered from hyper's own predicates to cause-chain inspection so the
    /// historical `parse`/`timeout`/`cancelled`/`incomplete_message`/`closed`
    /// buckets keep their meaning; only the old `protocol` catch-all splits.
    pub(super) fn classify(error: &hyper::Error) -> Self {
        let h2 = cause::<h2::Error>(error);
        let io_kind = cause::<io::Error>(error)
            .or_else(|| h2.and_then(h2::Error::get_io))
            .map(io::Error::kind);
        let reason = if error.is_parse() {
            "parse"
        } else if error.is_timeout() {
            "timeout"
        } else if error.is_canceled() {
            "cancelled"
        } else if error.is_incomplete_message() {
            "incomplete_message"
        } else if error.is_closed() {
            "closed"
        } else if error.is_body_write_aborted() {
            "body_write_aborted"
        } else if error.is_shutdown() {
            "shutdown"
        } else if error.is_user() {
            "user"
        } else if h2.is_some() {
            "h2"
        } else if io_kind.is_some() {
            "io"
        } else {
            "other"
        };
        Self {
            reason,
            io_kind,
            h2_reason: h2.and_then(h2::Error::reason).map(h2_reason),
            h2_origin: h2.map(|error| {
                if error.is_remote() {
                    "remote"
                } else if error.is_library() {
                    "library"
                } else {
                    "local"
                }
            }),
        }
    }
}

/// First error of type `T` in `error`'s source chain, including `error` itself.
fn cause<'a, T: std::error::Error + 'static>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(found) = error.downcast_ref::<T>() {
            return Some(found);
        }
        current = error.source();
    }
    None
}

const fn h2_reason(reason: h2::Reason) -> &'static str {
    match reason {
        h2::Reason::NO_ERROR => "no_error",
        h2::Reason::PROTOCOL_ERROR => "protocol_error",
        h2::Reason::INTERNAL_ERROR => "internal_error",
        h2::Reason::FLOW_CONTROL_ERROR => "flow_control_error",
        h2::Reason::SETTINGS_TIMEOUT => "settings_timeout",
        h2::Reason::STREAM_CLOSED => "stream_closed",
        h2::Reason::FRAME_SIZE_ERROR => "frame_size_error",
        h2::Reason::REFUSED_STREAM => "refused_stream",
        h2::Reason::CANCEL => "cancel",
        h2::Reason::COMPRESSION_ERROR => "compression_error",
        h2::Reason::CONNECT_ERROR => "connect_error",
        h2::Reason::ENHANCE_YOUR_CALM => "enhance_your_calm",
        h2::Reason::INADEQUATE_SECURITY => "inadequate_security",
        h2::Reason::HTTP_1_1_REQUIRED => "http_1_1_required",
        _ => "unknown",
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

/// A peer's alert is the only signal that separates a guest refusing the
/// interception CA from every other handshake failure, so the classes that
/// name a trust decision have to reach metrics rather than logs alone.
const fn alert_class(alert: rustls::AlertDescription) -> &'static str {
    match alert {
        rustls::AlertDescription::UnknownCA => "unknown_ca",
        rustls::AlertDescription::BadCertificate => "bad_certificate",
        rustls::AlertDescription::CertificateUnknown => "certificate_unknown",
        rustls::AlertDescription::CertificateExpired => "certificate_expired",
        rustls::AlertDescription::CertificateRevoked => "certificate_revoked",
        rustls::AlertDescription::HandshakeFailure => "handshake_failure",
        rustls::AlertDescription::ProtocolVersion => "protocol_version",
        _ => "other",
    }
}

fn tls_failure_from_io(error: &io::Error) -> Option<TlsFailure> {
    error
        .get_ref()?
        .downcast_ref::<rustls::Error>()
        .map(tls_failure_from_rustls)
}

fn tls_failure_from_rustls(error: &rustls::Error) -> TlsFailure {
    match error {
        rustls::Error::AlertReceived(alert) => TlsFailure {
            reason: "peer_alert",
            alert: alert.as_str().or(Some("unknown")),
            alert_class: alert_class(*alert),
            certificate: None,
        },
        rustls::Error::InvalidCertificate(certificate) => TlsFailure {
            reason: certificate_reason(certificate),
            alert: None,
            alert_class: "none",
            certificate: Some(certificate_reason(certificate)),
        },
        rustls::Error::NoApplicationProtocol => TlsFailure::new("alpn_mismatch"),
        _ => TlsFailure::new("protocol"),
    }
}

fn certificate_reason(error: &rustls::CertificateError) -> &'static str {
    match error {
        rustls::CertificateError::UnknownIssuer => "unknown_issuer",
        rustls::CertificateError::Expired | rustls::CertificateError::ExpiredContext { .. } => {
            "expired"
        }
        rustls::CertificateError::NotValidYet
        | rustls::CertificateError::NotValidYetContext { .. } => "not_valid_yet",
        rustls::CertificateError::NotValidForName
        | rustls::CertificateError::NotValidForNameContext { .. } => "name_mismatch",
        rustls::CertificateError::Revoked => "revoked",
        _ => "invalid_certificate",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HttpProtocol {
    Http1,
    Http2,
}

impl HttpProtocol {
    pub(super) fn negotiated(alpn: Option<&[u8]>) -> Result<Self, TlsTransportError> {
        match selected_alpn(alpn)? {
            // TLS clients predating ALPN still use HTTP/1.1. Hyper parses their
            // application bytes; this never selects an opaque relay.
            None | Some(HttpAlpn::Http1) => Ok(Self::Http1),
            Some(HttpAlpn::Http2) => Ok(Self::Http2),
        }
    }

    fn negotiated_upstream(
        client_alpn: ClientAlpnOffer,
        selected: Option<HttpAlpn>,
    ) -> Result<Self, TlsTransportError> {
        match selected {
            Some(protocol) if client_alpn.supports(protocol) => Ok(protocol.into()),
            // An origin that ignores ALPN is legacy HTTP/1.1 only when the
            // guest advertised that fallback or sent no ALPN itself.
            None if client_alpn == ClientAlpnOffer::NoAlpn
                || client_alpn.supports(HttpAlpn::Http1) =>
            {
                Ok(Self::Http1)
            }
            _ => Err(TlsTransportError::Protocol),
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
        }
    }
}

impl From<HttpAlpn> for HttpProtocol {
    fn from(protocol: HttpAlpn) -> Self {
        match protocol {
            HttpAlpn::Http1 => Self::Http1,
            HttpAlpn::Http2 => Self::Http2,
        }
    }
}

fn selected_alpn(alpn: Option<&[u8]>) -> Result<Option<HttpAlpn>, TlsTransportError> {
    alpn.map(|protocol| HttpAlpn::from_wire(protocol).ok_or(TlsTransportError::Protocol))
        .transpose()
}

impl TcpProxy {
    /// Each TLS handshake has eight non-queued slots and a ten-second budget.
    /// The complete HTTP/stream lifetime is fenced by the certificate lease;
    /// upstream setup happens only after a request passes authority and policy.
    pub(super) async fn terminate_tls(
        &self,
        client: TcpStream,
        admitted: AdmittedTcp,
        lease: Arc<CertificateLease>,
        client_alpn: ClientAlpnOffer,
        session: &mut Attempt,
    ) -> Result<(), TlsInterceptionError> {
        let span = tracing::info_span!(
            "tls.server",
            sandbox_id = self.sandbox_id,
            destination_port = admitted.destination().port(),
            protocol = tracing::field::Empty,
            policy_ref = tracing::field::Empty,
            otel.status_code = "ERROR",
        );
        async {
            tracing::Span::current().record(
                "policy_ref",
                &self.network_state.snapshot().await.installed_policy_ref,
            );
            let destination = admitted.destination();
            if destination.port() != lease.port() {
                session.finish(Outcome::Error, TlsTransportError::Target.as_str());
                return Err(TlsTransportError::Target.into());
            }
            let accounting = WireAccounting::new(self.stats.clone());
            let operation = async {
                let (downstream, protocol) = accept(client, &lease, &self.tls_interception).await?;
                session.inspection_stage(
                    pplx_space_metrics::gateway::InspectionStage::SessionForwarding,
                );
                tracing::Span::current().record("protocol", protocol.as_str());
                let binding = HostBinding::new(
                    self.sandbox_id,
                    destination,
                    self.network_state.clone(),
                    self.dns_allow_cache.clone(),
                );
                let interception = self.tls_interception.clone();
                let upstream_lease = lease.clone();
                let wire_read = accounting.read.clone();
                let wire_written = accounting.written.clone();
                let connect = move || async move {
                    let origin = async {
                        Ok(MeteredIo {
                            inner: relay::connect(destination).await?,
                            read: wire_read,
                            written: wire_written,
                        })
                    };
                    connect(origin, &upstream_lease, &interception, client_alpn).await
                };
                tls_http::serve(downstream, protocol, binding, lease.clone(), connect).await
            };
            let result = tokio::select! {
                biased;
                // These look identical to a client but have different
                // operators: shutdown is ours to schedule, while an
                // invalidated lease is the policy, identity or CA layer
                // changing underneath a live session.
                () = self.shutdown_token.cancelled() => Err(TlsTransportError::Withdrawn),
                () = lease.invalidated() => Err(TlsTransportError::Invalidated),
                result = operation => result,
            };
            if result.is_ok() {
                session.finish(Outcome::Success, "completed");
                tracing::Span::current().record("otel.status_code", "OK");
            } else if let Err(error) = &result {
                let outcome = match error {
                    TlsTransportError::Invalidated | TlsTransportError::Withdrawn => {
                        Outcome::Cancelled
                    }
                    _ => Outcome::Error,
                };
                session.finish(outcome, error.as_str());
                error.record_processing("tls_session", session.stage().as_str());
            }
            result.map_err(Into::into)
        }
        .instrument(span)
        .await
    }
}

/// The presented leaf and actual SNI must name the same authorized target.
pub(super) async fn accept<S>(
    stream: S,
    lease: &CertificateLease,
    interception: &TlsInterception,
) -> Result<(TlsIo<tokio_rustls::server::TlsStream<S>>, HttpProtocol), TlsTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _permit = match interception.handshake_permit(TlsHandshakeLeg::ClientToNetgw) {
        Ok(permit) => permit,
        Err(_) => {
            HandshakeAttempt::record_failure(
                TlsHandshakeLeg::ClientToNetgw,
                &TlsTransportError::Capacity,
            );
            return Err(TlsTransportError::Capacity);
        }
    };
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
        let downstream = tokio::time::timeout(
            HANDSHAKE_BUDGET,
            TlsAcceptor::from(Arc::new(config)).accept(stream),
        )
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
        let protocol = HttpProtocol::negotiated(downstream.get_ref().1.alpn_protocol())?;
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
/// The ten-second budget includes transport setup and the complete handshake.
pub(super) async fn connect<S>(
    transport: impl std::future::Future<Output = io::Result<S>>,
    lease: &CertificateLease,
    interception: &TlsInterception,
    client_alpn: ClientAlpnOffer,
) -> Result<(TlsIo<tokio_rustls::client::TlsStream<S>>, HttpProtocol), TlsTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _permit = match interception.handshake_permit(TlsHandshakeLeg::NetgwToOrigin) {
        Ok(permit) => permit,
        Err(_) => {
            HandshakeAttempt::record_failure(
                TlsHandshakeLeg::NetgwToOrigin,
                &TlsTransportError::Capacity,
            );
            return Err(TlsTransportError::Capacity);
        }
    };
    async {
        let deadline = tokio::time::Instant::now() + HANDSHAKE_BUDGET;
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
            transport.await.map_err(TlsTransportError::Upstream)
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
            let upstream_protocol = HttpProtocol::negotiated_upstream(client_alpn, selected)?;
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

fn upstream_span(host: &pplx_space_net_common::DomainName, port: u16) -> tracing::Span {
    tracing::info_span!(
        "tls.client",
        otel.kind = "client",
        otel.status_code = "ERROR",
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

/// Reports Rustls protocol failures from an established connection regardless
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
        let Some(failure) = tls_failure_from_io(error) else {
            return;
        };
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

/// Account origin-facing wire bytes on every exit, including cancellation.
struct WireAccounting {
    stats: Arc<ProxyStats>,
    read: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
}

impl WireAccounting {
    fn new(stats: Arc<ProxyStats>) -> Self {
        Self {
            stats,
            read: Arc::new(AtomicU64::new(0)),
            written: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Drop for WireAccounting {
    fn drop(&mut self) {
        let tx = self.written.load(Ordering::Relaxed);
        let rx = self.read.load(Ordering::Relaxed);
        self.stats.add_network_bytes(tx, rx);
        pplx_space_metrics::gateway::tcp_bytes("tx", tx);
        pplx_space_metrics::gateway::tcp_bytes("rx", rx);
    }
}

#[cfg(test)]
mod tests;
