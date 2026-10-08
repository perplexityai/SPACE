//! Opt-in interception admission. TLS transport never authorizes HTTP forwarding.

use std::sync::Arc;

use pplx_space_net_common::TlsInterception as PolicyTlsInterception;
use tokio::time::Instant;

use crate::certificates::{CERTIFICATE_REQUEST_BACKSTOP, CertificateLease};
use crate::sniff::MissingSniDetail;
use crate::tls_sni::{ClientHelloScan, HttpAlpn};

mod admission;

pub use crate::certificates::CertificateRequester;
use admission::HandshakeAdmission;
pub(crate) use admission::{Admission, HandshakePermit, Refused, TlsHandshakeLeg};

#[derive(Clone, Default)]
pub struct TlsInterception {
    requester: Option<CertificateRequester>,
    upstream: UpstreamTls,
}

#[derive(Clone)]
struct UpstreamTls {
    config: Arc<rustls::ClientConfig>,
    handshakes: HandshakeAdmission,
}

impl Default for UpstreamTls {
    fn default() -> Self {
        Self::new(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
    }
}

impl UpstreamTls {
    fn new(roots: rustls::RootCertStore) -> Self {
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("AWS-LC supports the default TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        Self {
            config: Arc::new(config),
            handshakes: HandshakeAdmission::default(),
        }
    }
}

pub(crate) enum InterceptionAction {
    PassThrough,
    Terminate(Arc<CertificateLease>),
}

impl TlsInterception {
    pub fn new(requester: CertificateRequester) -> Self {
        Self {
            upstream: UpstreamTls::default(),
            requester: Some(requester),
        }
    }

    /// Overrides public origin trust, independently of interception issuer trust.
    pub fn with_upstream_roots(mut self, roots: rustls::RootCertStore) -> Self {
        self.upstream = UpstreamTls::new(roots);
        self
    }

    pub(crate) fn upstream_config(&self, protocols: &[HttpAlpn]) -> Arc<rustls::ClientConfig> {
        let mut config = (*self.upstream.config).clone();
        config.alpn_protocols = protocols
            .iter()
            .map(|protocol| protocol.wire_value().to_vec())
            .collect();
        Arc::new(config)
    }

    /// Admit one handshake on `leg`, queueing behind a full pool until
    /// `deadline` at most. See [`admission`] for what the pool bounds.
    pub(crate) async fn admit_handshake(
        &self,
        leg: TlsHandshakeLeg,
        deadline: Instant,
    ) -> Result<HandshakePermit, Refused> {
        self.upstream.handshakes.admit(leg, deadline).await
    }

    /// Stop admitting TLS handshakes at gateway shutdown. Queued handshakes
    /// fail at once instead of waiting for capacity.
    pub(crate) fn close_handshakes(&self) {
        self.upstream.handshakes.close();
    }

    /// Called only after TCP policy admission. The admitted policy generation
    /// decides whether this connection passes through or is intercepted.
    pub(crate) async fn before_relay(
        &self,
        policy: PolicyTlsInterception,
        scan: &ClientHelloScan,
        missing_sni_detail: Option<MissingSniDetail>,
        port: u16,
    ) -> Result<InterceptionAction, TlsInterceptionError> {
        match policy {
            PolicyTlsInterception::Off => Ok(InterceptionAction::PassThrough),
            PolicyTlsInterception::On => {
                let requester = self
                    .requester
                    .as_ref()
                    .ok_or(TlsInterceptionError::AuthorityUnavailable)?;
                require_certificates(requester, scan, missing_sni_detail, port).await
            }
        }
    }
}

/// Certificates permit a TLS handshake, never uninspected application relay.
async fn require_certificates(
    requester: &CertificateRequester,
    scan: &ClientHelloScan,
    missing_sni_detail: Option<MissingSniDetail>,
    port: u16,
) -> Result<InterceptionAction, TlsInterceptionError> {
    let host = match scan {
        ClientHelloScan::NotTls => return Ok(InterceptionAction::PassThrough),
        ClientHelloScan::ServerName(host) => host.clone(),
        ClientHelloScan::NoServerName
        | ClientHelloScan::Incomplete
        | ClientHelloScan::Malformed => {
            return Err(TlsInterceptionError::MissingServerName(
                missing_sni_detail.expect("missing-SNI scan must retain its diagnostic detail"),
            ));
        }
    };
    // The provider bounds itself; this only catches a requester that does not.
    // It is deliberately longer so the provider's deadline fires first and
    // reports which stage ran out of budget.
    let lease = tokio::time::timeout(CERTIFICATE_REQUEST_BACKSTOP, requester(host, port))
        .await
        .map_err(|_| TlsInterceptionError::Timeout)?
        .map_err(TlsInterceptionError::CertificateRequest)?;
    Ok(InterceptionAction::Terminate(lease))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsInterceptionError {
    #[error("TLS interception authority is unavailable")]
    AuthorityUnavailable,
    #[error("TLS interception requires a complete ClientHello with a valid SNI")]
    MissingServerName(MissingSniDetail),
    #[error("certificate acquisition exceeded the two-second deadline")]
    Timeout,
    #[error("certificate acquisition failed ({code:?})", code = .0.code())]
    CertificateRequest(#[source] tonic::Status),
    #[error("TLS transport failed: {0}")]
    Transport(#[from] crate::tcp_proxy::tls::TlsTransportError),
}

impl TlsInterceptionError {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::AuthorityUnavailable => "authority_unavailable",
            Self::MissingServerName(_) => "missing_sni",
            Self::Timeout => "timeout",
            Self::CertificateRequest(_) => "certificate_error",
            Self::Transport(error) => error.as_str(),
        }
    }
}

#[cfg(test)]
mod tests;
