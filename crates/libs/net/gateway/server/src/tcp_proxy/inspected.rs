//! Compose certificate-bound TLS transport with inspected HTTP.
use super::http_host::{HostBinding, MeteredIo};
use super::tls::TlsTransportError;
use super::tls_http::{HttpError, HttpProtocol};
use super::{TcpProxy, tls, tls_http, transport};
use crate::{
    certificates::CertificateLease,
    egress::AdmittedTcp,
    stats::ProxyStats,
    tls_sni::{ClientAlpnOffer, HttpAlpn},
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::net::TcpStream;
use tracing::Instrument;

impl TcpProxy {
    /// Each TLS handshake waits for a shared pool permit and then completes
    /// within one ten-second budget that includes the wait.
    /// The complete HTTP/stream lifetime is fenced by the certificate lease;
    /// upstream setup happens only after a request passes authority and policy.
    pub(super) async fn terminate_tls(
        &self,
        client: TcpStream,
        admitted: AdmittedTcp,
        lease: Arc<CertificateLease>,
        client_alpn: ClientAlpnOffer,
    ) -> Result<(), HttpError> {
        let span = tracing::info_span!(
            "tls.server",
            sandbox_id = self.sandbox_id,
            destination_port = admitted.destination().port(),
            negotiated = tracing::field::Empty,
            protocol = tracing::field::Empty,
            policy_ref = tracing::field::Empty,
            handshake_queue_ms = tracing::field::Empty,
            otel.status_code = "ERROR",
        );
        async {
            tracing::Span::current().record(
                "policy_ref",
                &self.network_state.snapshot().await.installed_policy_ref,
            );
            let destination = admitted.destination();
            if destination.port() != lease.port() {
                return Err(TlsTransportError::Target.into());
            }
            let accounting = WireAccounting::new(self.stats.clone());
            let operation = async {
                // ALPN is telemetry here. `tls_http::serve` detects the
                // version from the guest's first bytes and records `protocol`
                // once the first request arrives.
                let (downstream, negotiated) =
                    tls::accept(client, &lease, &self.tls_interception).await?;
                tracing::Span::current()
                    .record("negotiated", HttpProtocol::from(negotiated).as_str());
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
                let connect = move |guest_protocol| async move {
                    let origin = async {
                        Ok(MeteredIo {
                            inner: transport::connect(destination).await?,
                            read: wire_read,
                            written: wire_written,
                        })
                    };
                    let offer = upstream_alpn_offer(client_alpn, guest_protocol);
                    tls::connect(origin, &upstream_lease, &interception, offer)
                        .await
                        .map(|(io, alpn)| (io, alpn.into()))
                };
                tls_http::serve(downstream, binding, lease.clone(), connect).await
            };
            let result = tokio::select! {
                biased;
                // These look identical to a client but have different
                // operators: shutdown is ours to schedule, while an
                // invalidated lease is the policy, identity or CA layer
                // changing underneath a live session.
                () = self.shutdown_token.cancelled() => Err(TlsTransportError::Withdrawn.into()),
                () = lease.invalidated() => Err(TlsTransportError::Invalidated.into()),
                result = operation => result,
            };
            if result.is_ok() {
                tracing::Span::current().record("otel.status_code", "OK");
            } else if let Err(error) = &result {
                error.record("tls_session");
            }
            result
        }
        .instrument(span)
        .await
    }
}

/// ALPN to offer the origin given what the guest turned out to speak.
///
/// The origin leg mirrors the guest's ClientHello offer, except that a guest
/// which offered h2 and then spoke HTTP/1.1 (Go's `net/http` dialing an
/// `https://` forward proxy) gets an HTTP/1.1-only upstream. The gateway can
/// only tunnel CONNECT over an HTTP/1.1 origin leg, and offering h2 there
/// would let an h2-preferring proxy turn the CONNECT into `invalid_connect`.
/// Guests without ALPN and genuine h2 guests are unchanged.
pub(super) fn upstream_alpn_offer(
    client_alpn: ClientAlpnOffer,
    guest_protocol: HttpProtocol,
) -> ClientAlpnOffer {
    if guest_protocol == HttpProtocol::Http1 && client_alpn.supports(HttpAlpn::Http2) {
        ClientAlpnOffer::Http1
    } else {
        client_alpn
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
