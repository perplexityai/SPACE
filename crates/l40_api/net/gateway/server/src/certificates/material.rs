use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use jiff::Timestamp;
use pplx_space_common_core::types::CredentialReferenceId;
use pplx_space_net_common::{DomainName, NetworkCredentialHttpMethodV1, NetworkPolicyV1};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;
use tokio_util::sync::CancellationToken;

use crate::state::{GatewayNetworkState, NetworkCredentialSnapshotError};

#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    #[error("invalid certificate or TLS key: {0}")]
    Tls(#[from] rustls::Error),
    #[error("certificate validity timestamp is out of range")]
    Validity,
    #[error("certificate identity, policy, lifetime or process binding is no longer usable")]
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetworkCredentialAccessError {
    Missing,
    TemporarilyUnavailable,
    Stale,
    InvalidMaterial,
}

#[derive(Clone)]
pub(super) struct Fence {
    pub state: GatewayNetworkState,
    pub policy: Arc<NetworkPolicyV1>,
    pub identity: CancellationToken,
    pub shutdown: CancellationToken,
    pub authority: CancellationToken,
}

impl Fence {
    pub fn is_current(&self) -> bool {
        !self.identity.is_cancelled()
            && !self.authority.is_cancelled()
            && !self.shutdown.is_cancelled()
            && self.state.is_current_tcp_policy(&self.policy)
    }
}

/// Locally signed leaf material is checked for matching keys and a current
/// execution, policy, and authority before becoming a lease. A lease is not
/// an egress permit: destination admission and per-request HTTP policy still apply.
pub struct CertificateLease {
    host: DomainName,
    port: u16,
    key: Arc<CertifiedKey>,
    not_before: Timestamp,
    not_after: Timestamp,
    fence: Fence,
}

impl fmt::Debug for CertificateLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificateLease")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("not_after", &self.not_after)
            .finish_non_exhaustive()
    }
}

pub(super) struct VerifiedMaterial {
    pub host: DomainName,
    pub port: u16,
    pub key: PrivateKeyDer<'static>,
    pub leaf: Vec<u8>,
    pub issuer: Vec<u8>,
    pub node_issuer: Vec<u8>,
    pub not_before: i64,
    pub not_after: i64,
}

impl CertificateLease {
    pub(super) fn new(material: VerifiedMaterial, fence: Fence) -> Result<Self, CertificateError> {
        let leaf = CertificateDer::from(material.leaf);
        let issuer = CertificateDer::from(material.issuer);
        let issuers = [issuer, CertificateDer::from(material.node_issuer)];
        let certified = CertifiedKey::from_der(
            std::iter::once(leaf).chain(issuers).collect(),
            material.key,
            &rustls::crypto::aws_lc_rs::default_provider(),
        )?;
        let value = Self {
            host: material.host,
            port: material.port,
            key: Arc::new(certified),
            not_before: Timestamp::from_second(material.not_before)
                .map_err(|_| CertificateError::Validity)?,
            not_after: Timestamp::from_second(material.not_after)
                .map_err(|_| CertificateError::Validity)?,
            fence,
        };
        if !value.is_usable() {
            return Err(CertificateError::Stale);
        }
        Ok(value)
    }

    pub fn host(&self) -> &DomainName {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn is_usable(&self) -> bool {
        let now = Timestamp::now();
        self.not_before <= now && now < self.not_after && self.fence.is_current()
    }

    pub(super) fn is_usable_until(&self, deadline: Timestamp) -> bool {
        self.is_usable() && deadline < self.not_after
    }

    /// Await expiry or a policy, execution or process change. Consumers race
    /// their complete connection lifetime against this, not just the handshake.
    /// Policy and wall-clock validity are rechecked once per second.
    pub async fn invalidated(&self) {
        loop {
            if !self.is_usable() {
                return;
            }
            tokio::select! {
                biased;
                () = self.fence.shutdown.cancelled() => return,
                () = self.fence.identity.cancelled() => return,
                () = self.fence.authority.cancelled() => return,
                // Recheck wall time so clock changes cannot extend validity
                // until an old monotonic expiry timer eventually fires.
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    pub fn certified_key(&self) -> Result<Arc<CertifiedKey>, CertificateError> {
        if !self.is_usable() {
            return Err(CertificateError::Stale);
        }
        Ok(self.key.clone())
    }

    pub(crate) fn network_credential_headers(
        &self,
        path: &str,
        method: NetworkCredentialHttpMethodV1,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, NetworkCredentialAccessError> {
        let bindings = self
            .fence
            .policy
            .matching_network_credential_bindings(&self.host, self.port, path, method);
        if bindings.is_empty() {
            return self
                .is_usable()
                .then(Vec::new)
                .ok_or(NetworkCredentialAccessError::Stale);
        }
        let headers = self
            .fence
            .state
            .with_network_credentials(&self.fence.policy, |credentials| {
                bindings
                    .into_iter()
                    .map(|binding| {
                        let reference_id =
                            CredentialReferenceId::from_uuid(binding.credential_reference_id);
                        let value = credentials
                            .value(reference_id)
                            .ok_or(NetworkCredentialAccessError::Missing)?;
                        let mut value = HeaderValue::from_bytes(value)
                            .map_err(|_| NetworkCredentialAccessError::InvalidMaterial)?;
                        // Hyper encodes sensitive HTTP/2 values as never-indexed.
                        value.set_sensitive(true);
                        let name = HeaderName::from_bytes(binding.header_name.as_bytes())
                            .expect("network credential policy contains validated header names");
                        Ok((name, value))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(|error| match error {
                NetworkCredentialSnapshotError::TemporarilyUnavailable => {
                    NetworkCredentialAccessError::TemporarilyUnavailable
                }
                NetworkCredentialSnapshotError::Stale => NetworkCredentialAccessError::Stale,
            })??;
        if !self.is_usable() {
            return Err(NetworkCredentialAccessError::Stale);
        }
        Ok(headers)
    }
}
