//! Per-node intermediate CA issuance for MITM credential injection.
//!
//! A node generates its own CA keypair, signs a [`csr::NodeCsr`] with it, and
//! asks the authority to certify it under the SPACE root. No private key
//! crosses a process boundary: the node's stays on the node, and the root's
//! never leaves AWS Private CA.
//!
//! [`aws_private_ca`] is the only signing backend. The authority holding no CA
//! private key of its own is what puts every issuance in CloudTrail in an
//! account no SPACE service can write to.
//!
//! Keys are ECDSA P-256 and nothing else. Guests verify these chains with
//! whatever TLS stack they run, and P-256 is the one curve all of them must
//! implement (RFC 8446 §9.1, RFC 8422 §5.1.1). P-384 could not strengthen a
//! chain whose root is P-256 (`EC_prime256v1`, see the certificates Terraform),
//! and would cost a slower signature on the gateway's per-leaf hot path plus a
//! second wire contract to support forever.

pub mod aws_private_ca;
mod certificate;
pub mod csr;

use std::time::Duration;

use async_trait::async_trait;

pub const ISSUE_INTERMEDIATE_METHOD_PATH: &str =
    "/space.credential.manager.v1.CredentialManager/IssueIntermediateCertificate";

/// Rate limits for `IssueIntermediateCertificate`.
///
/// This is the only path in SPACE that reaches AWS Private CA, whose
/// `IssueCertificate` quota is 25/s across the account. Steady state is a
/// fraction of one per second, so a rate near the ceiling is a retry storm.
/// TCP peers share one bucket, making this a fleet-wide cap; a shed node backs
/// off and keeps signing under the intermediate it already holds.
pub fn issue_rate_limits() -> pplx_space_common_svc::GrpcRateLimiterLimits {
    pplx_space_common_svc::GrpcRateLimiterLimits {
        // Issuance polls for seconds at a time, so in-flight depth is not a
        // proxy for rate; small enough that a stall cannot pin tasks.
        max_global_in_flight: Some(32),
        max_in_flight_per_peer: Some(32),
        max_requests_per_peer: Some(10),
        per_peer_request_window: Duration::from_secs(1),
    }
}

/// Validity of every node intermediate CA certificate. Guests validate neither
/// CRL nor OCSP, so this window is the only bound on a leaked node signing key.
pub const INTERMEDIATE_VALIDITY: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// What the authority asks its signing backend for.
#[derive(Clone, Debug)]
pub struct IntermediateCertificateRequest {
    pub csr: csr::NodeCsr,
    pub validity: Duration,
}

/// Why issuing an intermediate failed. Split by what the caller does about it:
/// the node's refresh loop stops on the terminal variants and backs off on the
/// retryable ones.
#[derive(Debug, thiserror::Error)]
pub enum IssueIntermediateError {
    #[error("this credential authority has no intermediate CA signing backend configured")]
    NotConfigured,
    #[error(transparent)]
    Csr(#[from] csr::CsrError),
    /// Server-side fault: the request validated, so this is our own composition
    /// of the backend call.
    #[error("could not compose the signing request: {0}")]
    RequestBuild(String),
    /// The backend was reached and refused the request on its merits.
    #[error("the signing backend rejected the intermediate certificate request: {0}")]
    Rejected(String),
    #[error("the signing backend is unavailable: {0}")]
    Unavailable(String),
    /// Retryable specifically with the *same* keypair: the idempotency token
    /// re-attaches the retry to this same issuance.
    #[error(
        "the signing backend did not return a certificate within {}ms; retry with the same \
         public key",
        .timeout.as_millis()
    )]
    IssuanceTimedOut { timeout: Duration },
    #[error("the signing backend returned a certificate that could not be parsed: {0}")]
    MalformedCertificate(String),
}

/// Signs a node's intermediate CA certificate under the SPACE root.
/// [`aws_private_ca::AwsPrivateCaIssuer`] is the only implementation; the trait
/// is here so the handler can be tested without an endpoint.
#[async_trait]
pub trait IntermediateCaIssuer: Send + Sync {
    /// Returns the signed certificate, DER encoded.
    async fn issue(
        &self,
        request: &IntermediateCertificateRequest,
    ) -> Result<Vec<u8>, IssueIntermediateError>;
}
