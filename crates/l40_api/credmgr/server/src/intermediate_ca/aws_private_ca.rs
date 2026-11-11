//! [`IntermediateCaIssuer`] backed by AWS Private CA.
//!
//! Issuance is asynchronous: `IssueCertificate` returns an ARN and
//! `GetCertificate` answers `RequestInProgressException` until the certificate
//! materializes. The poll is bounded by [`AwsPrivateCaConfig::issue_timeout`]
//! and gives up retryably. A retry with the same keypair re-attaches to the
//! in-flight issuance through the idempotency token.

use std::num::ParseIntError;
use std::time::Duration;

use sha2::{Digest, Sha256};

use async_trait::async_trait;
use aws_sdk_acmpca::Client;
use aws_sdk_acmpca::error::ProvideErrorMetadata;
use aws_sdk_acmpca::operation::get_certificate::GetCertificateError;
use aws_sdk_acmpca::operation::issue_certificate::IssueCertificateError;
use aws_sdk_acmpca::primitives::Blob;
use aws_sdk_acmpca::types::{
    ApiPassthrough, Asn1Subject, ExtendedKeyUsage, ExtendedKeyUsageType, Extensions, KeyUsage,
    SigningAlgorithm, Validity, ValidityPeriodType,
};
use jiff::Timestamp;

use super::{IntermediateCaIssuer, IntermediateCertificateRequest, IssueIntermediateError};
use crate::intermediate_ca::certificate::certificate_der_from_pem;

/// The certificate template every node intermediate is issued under.
///
/// - `SubordinateCACertificate` fixes basic constraints to a critical
///   `CA:TRUE`, outranking anything the request sends.
/// - `PathLen1` permits one gateway CA below the node intermediate. Gateways
///   receive `pathLen:0` certificates and can only issue end-entity certificates.
/// - `APIPassthrough` takes extensions from the API call and copies none from
///   the CSR, unlike `_CSRPassthrough` and `_APICSRPassthrough`. The CSR is the
///   node's, so a template that honored it would be honoring caller input —
///   which is also why [`csr`](crate::intermediate_ca::csr) refuses a request
///   carrying any attribute rather than relying on this alone.
/// - `Blank` asserts no key usage, so [`intermediate_passthrough`] must set
///   them all.
const SUBORDINATE_CA_TEMPLATE_ARN: &str =
    "arn:aws:acm-pca:::template/BlankSubordinateCACertificate_PathLen1_APIPassthrough/V1";

/// Bytes of the key digest that names an issuance. 32 hex characters, inside
/// the 36-character `IdempotencyToken` limit.
const KEY_DIGEST_BYTES: usize = 16;

/// Interval between `GetCertificate` polls. Sized against its own per-account
/// quota (75/s), which a fleet-wide cold start reaches sooner than the
/// issuance quota if each issuance polls more than about three times.
const POLL_INTERVAL: Duration = Duration::from_millis(400);

/// Setting this ARN is what enables `IssueIntermediateCertificate`. Unset, the
/// RPC answers `FAILED_PRECONDITION` — the state of every cluster not running
/// MITM.
pub const CA_ARN_ENV: &str = "SPACE_CREDMGR_INTERMEDIATE_CA_ARN";
const DEFAULT_ISSUE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, thiserror::Error)]
#[error("intermediate CA timeout must be a whole number of seconds")]
pub struct ConfigError(#[from] ParseIntError);

/// Configuration for [`AwsPrivateCaIssuer`].
#[derive(Clone, Debug)]
pub struct AwsPrivateCaConfig {
    /// ARN of the root CA configured for this deployment.
    pub certificate_authority_arn: String,
    /// How long to poll `GetCertificate` before giving up retryably. Keep it
    /// under the caller's RPC deadline.
    pub issue_timeout: Duration,
}

impl AwsPrivateCaConfig {
    /// `None` when this deployment configures no root CA.
    pub fn from_env() -> Result<Option<Self>, ConfigError> {
        let Some(certificate_authority_arn) =
            std::env::var(CA_ARN_ENV).ok().filter(|arn| !arn.is_empty())
        else {
            return Ok(None);
        };
        let issue_timeout = DEFAULT_ISSUE_TIMEOUT;
        Ok(Some(Self {
            certificate_authority_arn,
            issue_timeout,
        }))
    }
}

/// Certifies node intermediate keys by calling AWS Private CA.
pub struct AwsPrivateCaIssuer {
    client: Client,
    config: AwsPrivateCaConfig,
}

impl AwsPrivateCaIssuer {
    pub fn new(client: Client, config: AwsPrivateCaConfig) -> Self {
        Self { client, config }
    }

    /// Build from validated feature configuration and the process-wide AWS
    /// SDK configuration.
    pub fn from_config(config: AwsPrivateCaConfig, aws_config: &aws_config::SdkConfig) -> Self {
        tracing::info!(
            certificate_authority_arn = config.certificate_authority_arn,
            issue_timeout_secs = config.issue_timeout.as_secs(),
            "intermediate CA issuance enabled"
        );
        Self::new(aws_sdk_acmpca::Client::new(aws_config), config)
    }

    /// Start issuance and return the new certificate's ARN.
    async fn issue_certificate(
        &self,
        request: &IntermediateCertificateRequest,
        now: Timestamp,
    ) -> Result<String, IssueIntermediateError> {
        let key_digest = key_digest(request.csr.public_key_der());

        // ABSOLUTE because DAYS is the finest relative period type.
        let not_after = now
            .as_second()
            .saturating_add(request.validity.as_secs() as i64);
        let validity = Validity::builder()
            .value(not_after)
            .r#type(ValidityPeriodType::Absolute)
            .build()
            .map_err(|error| IssueIntermediateError::RequestBuild(error.to_string()))?;

        let output = self
            .client
            .issue_certificate()
            .certificate_authority_arn(&self.config.certificate_authority_arn)
            .csr(Blob::new(request.csr.pem().into_bytes()))
            .signing_algorithm(SigningAlgorithm::Sha256Withecdsa)
            .template_arn(SUBORDINATE_CA_TEMPLATE_ARN)
            .validity(validity)
            .api_passthrough(intermediate_passthrough(&key_digest))
            // Derived from the offered key, so a retry with the same keypair
            // re-attaches instead of minting a second certificate. AWS honors
            // the token for five minutes.
            .idempotency_token(&key_digest)
            .send()
            .await
            .map_err(|error| {
                let service_error = error.into_service_error();
                classify_issue_error(&service_error)
            })?;

        output.certificate_arn.ok_or_else(|| {
            IssueIntermediateError::MalformedCertificate(
                "IssueCertificate returned no certificate ARN".to_string(),
            )
        })
    }

    /// Poll `GetCertificate` until the certificate materializes or the
    /// configured timeout elapses.
    async fn await_certificate(
        &self,
        certificate_arn: &str,
    ) -> Result<String, IssueIntermediateError> {
        let deadline = tokio::time::Instant::now() + self.config.issue_timeout;
        loop {
            let result = self
                .client
                .get_certificate()
                .certificate_authority_arn(&self.config.certificate_authority_arn)
                .certificate_arn(certificate_arn)
                .send()
                .await;

            match result {
                Ok(output) => {
                    return output.certificate.ok_or_else(|| {
                        IssueIntermediateError::MalformedCertificate(
                            "GetCertificate returned no certificate body".to_string(),
                        )
                    });
                }
                Err(error) => {
                    let service_error = error.into_service_error();
                    if !matches!(
                        service_error,
                        GetCertificateError::RequestInProgressException(_)
                    ) {
                        return Err(classify_get_error(&service_error));
                    }
                }
            }

            // Still issuing. Bound by the deadline, not a poll count.
            let next_poll = tokio::time::Instant::now() + POLL_INTERVAL;
            if next_poll >= deadline {
                return Err(IssueIntermediateError::IssuanceTimedOut {
                    timeout: self.config.issue_timeout,
                });
            }
            tokio::time::sleep_until(next_poll).await;
        }
    }
}

#[async_trait]
impl IntermediateCaIssuer for AwsPrivateCaIssuer {
    async fn issue(
        &self,
        request: &IntermediateCertificateRequest,
    ) -> Result<Vec<u8>, IssueIntermediateError> {
        let certificate_arn = self.issue_certificate(request, Timestamp::now()).await?;
        let pem = self.await_certificate(&certificate_arn).await?;
        certificate_der_from_pem(&pem)
    }
}

/// Names an issuance by the key it certifies: truncated SHA-256 of the DER
/// `SubjectPublicKeyInfo`, hex encoded. Serves as both the certificate's
/// subject and the idempotency token, so a retry with the same keypair
/// re-attaches to the in-flight issuance and the certificate it produces names
/// that key.
///
/// Derived here rather than accepted from the caller: every node's credential
/// manager authenticates as the same ServiceAccount, so a caller-chosen token
/// could collide with another node's issuance.
fn key_digest(public_key_der: &[u8]) -> String {
    let digest = Sha256::digest(public_key_der);
    let truncated = digest
        .get(..KEY_DIGEST_BYTES)
        .expect("SHA-256 is 32 bytes, longer than KEY_DIGEST_BYTES");
    hex::encode(truncated)
}

/// The certificate fields the API contributes on top of the template.
///
/// No `nameConstraints`: a meaningful deny list is per organization and a
/// per-node intermediate cannot carry one, so it waits for org-scoped CAs.
/// Until then nothing in the chain bounds what names a node can mint for.
fn intermediate_passthrough(common_name: &str) -> ApiPassthrough {
    let key_usage = KeyUsage::builder()
        .key_cert_sign(true)
        .crl_sign(true)
        .build();
    let extended_key_usage = ExtendedKeyUsage::builder()
        .extended_key_usage_type(ExtendedKeyUsageType::ServerAuth)
        .build();
    let extensions = Extensions::builder()
        .key_usage(key_usage)
        .extended_key_usage(extended_key_usage)
        .build();
    ApiPassthrough::builder()
        .subject(Asn1Subject::builder().common_name(common_name).build())
        .extensions(extensions)
        .build()
}

/// Split `IssueCertificate` failures into terminal and retryable. The node's
/// refresh loop backs off on retryable and stops on terminal.
fn classify_issue_error(error: &IssueCertificateError) -> IssueIntermediateError {
    match error {
        // Our request, our bug: the key already parsed and every other field
        // is ours.
        IssueCertificateError::InvalidArgsException(_)
        | IssueCertificateError::MalformedCsrException(_) => {
            IssueIntermediateError::Rejected(describe(error))
        }
        // The configured root CA is wrong, missing, or cannot sign.
        IssueCertificateError::InvalidArnException(_)
        | IssueCertificateError::ResourceNotFoundException(_)
        | IssueCertificateError::InvalidStateException(_) => {
            IssueIntermediateError::Rejected(describe(error))
        }
        // Quota. Steady state is a tiny fraction of the account limit, so
        // this is a burst that drains.
        IssueCertificateError::LimitExceededException(_) => {
            IssueIntermediateError::Unavailable(describe(error))
        }
        // Transport, throttling, and anything unmodelled. Treating an
        // unmodelled error as terminal would take a node out of service until
        // it restarts.
        _ => IssueIntermediateError::Unavailable(describe(error)),
    }
}

/// Split `GetCertificate` failures the same way. `RequestInProgressException`
/// never reaches here; the poll loop treats it as not-yet-ready.
fn classify_get_error(error: &GetCertificateError) -> IssueIntermediateError {
    match error {
        GetCertificateError::InvalidArnException(_)
        | GetCertificateError::InvalidStateException(_)
        | GetCertificateError::ResourceNotFoundException(_) => {
            IssueIntermediateError::Rejected(describe(error))
        }
        _ => IssueIntermediateError::Unavailable(describe(error)),
    }
}

/// AWS error code plus message. Never the request.
fn describe(error: &impl ProvideErrorMetadata) -> String {
    match (error.code(), error.message()) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        (Some(code), None) => code.to_string(),
        (None, Some(message)) => message.to_string(),
        (None, None) => "unknown AWS Private CA error".to_string(),
    }
}

#[cfg(test)]
mod tests;
