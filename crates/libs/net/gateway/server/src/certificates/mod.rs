//! Gateway-owned leaf keys, execution-bound certificate leases, and bounded acquisition.

mod authority;
mod client;
mod material;
mod timing;

pub use authority::GatewayAuthority;
pub use client::{CertificateContext, CertificateProvider};
pub(crate) use material::NetworkCredentialAccessError;
pub use material::{CertificateError, CertificateLease};

#[cfg(test)]
pub(crate) use authority::tests::{
    local_provider_with_policy, local_provider_with_state, retire_authority,
};

pub type CertificateRequester = std::sync::Arc<
    dyn Fn(
            pplx_space_net_common::DomainName,
            u16,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<std::sync::Arc<CertificateLease>, tonic::Status>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// End-to-end local lookup/key-generation/signing budget; no issuance RPC.
pub(crate) const CERTIFICATE_REQUEST_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

/// Backstop for a requester the provider does not own. It must exceed
/// [`CERTIFICATE_REQUEST_TIMEOUT`], because a backstop of equal length starts
/// first and so always fires first, censoring the provider's own
/// `deadline_exceeded` sample and its signing duration for the slow tail those
/// measurements exist to expose.
pub(crate) const CERTIFICATE_REQUEST_BACKSTOP: std::time::Duration =
    std::time::Duration::from_millis(2_500);

const _: () = assert!(
    CERTIFICATE_REQUEST_BACKSTOP.as_millis() > CERTIFICATE_REQUEST_TIMEOUT.as_millis(),
    "the backstop must outlast the provider deadline, or it fires first and censors it"
);
