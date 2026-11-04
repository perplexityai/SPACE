//! This node's intermediate CA: holding one and signing gateway CAs under it.
//!
//! The node generates and retains its CA key. The credential authority certifies
//! it with pathLen:1 through AWS Private CA. The node can then certify a gateway's
//! own key with pathLen:0 so the gateway can sign TLS leaves without an RPC in
//! the handshake.
//!
//! The certificate is acquired lazily and shared. [`NodeCa`] holds one future;
//! a caller that finds it unresolved joins it rather than starting a second
//! issuance, so a node's first burst of connections costs one round trip and
//! not one per connection.
//! Requests in the final hour of validity refresh the node CA before signing,
//! giving gateways time to replace their chains while the previous ones work.
//!
//! Every failure to hold a usable CA is retried here and nowhere else. An
//! unreachable authority is asked again on a fast schedule; a refusal — the
//! authority has no intermediate CA configured, rejected the request, or
//! returned a certificate this node cannot delegate under — is re-probed on a
//! slow one. Both reach gateways as `UNAVAILABLE` with a typed reason, so a
//! gateway keeps retrying and a node that was refused recovers on the next
//! probe after the deployment is fixed, without a restart.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use pplx_space_credmgr_client::{
    CREDMGR_ERROR_REASON_METADATA_KEY, CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX,
    CredentialManagerGrpcClient,
};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, PublicKeyData, SerialNumber,
    SubjectPublicKeyInfo,
};
use time::OffsetDateTime;
use tokio::time::Instant;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use crate::intermediate_ca::csr::{CSR_COMMON_NAME, CsrError, NodeCsr};
use crate::proto::{IssueIntermediateCertificateRequest, IssueIntermediateCertificateResponse};

/// How far to backdate issued certificates' `notBefore` to tolerate guest clock
/// skew. Their `notAfter` is the node intermediate's.
const LEAF_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
/// RFC 5280 §4.1.2.2 permits 20 bytes.
const SERIAL_BYTES: usize = 16;
/// Bound on one issuance. Above the authority's own poll bound (20s by
/// default) so a healthy-but-slow issuance answers for itself; this fires only
/// when the authority never answers at all, which would otherwise leave every
/// caller joined to a future that can never resolve.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Renew on demand with enough overlap for authority retries and gateway bundle
/// replacement. Installed chains remain valid until their original `notAfter`.
const RENEW_BEFORE: Duration = Duration::from_secs(60 * 60);
/// Wait after the first retryable failure, doubling with each consecutive one
/// up to [`MAX_ACQUIRE_BACKOFF`]. Acquisition is driven by inbound leaf
/// requests, so without a floor the request rate becomes the upstream call
/// rate; without the doubling, a fleet keeps hammering an authority that is
/// staying down.
const INITIAL_ACQUIRE_BACKOFF: Duration = Duration::from_secs(5);
const MAX_ACQUIRE_BACKOFF: Duration = Duration::from_secs(60);
/// A refusal is re-probed too, on a slower schedule: it changes on a deploy,
/// not on a retry, and a fleet of nodes must not turn a misconfigured
/// authority into a hot loop. The cap keeps recovery after the deploy inside
/// ten minutes on every node without anyone restarting anything.
const INITIAL_REPROBE_BACKOFF: Duration = Duration::from_secs(60);
const MAX_REPROBE_BACKOFF: Duration = Duration::from_secs(10 * 60);
/// Fraction of a wait spread uniformly around it so nodes that failed
/// together do not retry together. Both schedules carry it: an authority
/// that refuses or is unreachable does so for every node at once, and an
/// unjittered fleet answers with a synchronised burst on every wait.
const RETRY_JITTER: f64 = 0.2;

/// Why the authority's answer left this node without a usable CA. Closed set:
/// it names a metric tag and travels to gateways as a status reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalReason {
    /// The authority answered `FAILED_PRECONDITION`: its deployment has no
    /// intermediate CA to issue from.
    AuthorityNotConfigured,
    /// The authority rejected the request or this node outright.
    AuthorityRefused,
    /// The authority returned a certificate this node cannot delegate under.
    UnusableCertificate,
}

impl RefusalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorityNotConfigured => "authority_not_configured",
            Self::AuthorityRefused => "authority_refused",
            Self::UnusableCertificate => "unusable_certificate",
        }
    }
}

impl std::fmt::Display for RefusalReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why no certificate was signed.
///
/// `Clone` because one acquisition is reported to every caller waiting on it,
/// failure included.
#[derive(Clone, Debug, thiserror::Error)]
pub enum NodeCaError {
    #[error(transparent)]
    Csr(#[from] CsrError),
    #[error("public_key: {0}")]
    PublicKey(String),
    /// The authority could not answer; asking again soon may work.
    #[error("the credential authority could not certify this node's key: {0}")]
    Unavailable(String),
    /// The authority refused, or returned something unusable. Asking again
    /// changes nothing until a deploy does, so it is re-probed slowly.
    #[error("the credential authority refused to certify this node's key ({reason}): {detail}")]
    Refused {
        reason: RefusalReason,
        detail: String,
    },
    /// The intermediate was in hand and signing still failed: a fault in this
    /// node, not in the request or the authority.
    #[error("could not sign the certificate: {0}")]
    Sign(String),
}

impl NodeCaError {
    fn refused(reason: RefusalReason, detail: impl Into<String>) -> Self {
        Self::Refused {
            reason,
            detail: detail.into(),
        }
    }

    fn unusable(detail: impl Into<String>) -> Self {
        Self::refused(RefusalReason::UnusableCertificate, detail)
    }

    /// The wire reason for a failure this node will retry on its own. `None`
    /// for faults in the request or in this node, which no retry here fixes.
    fn unavailable_reason(&self) -> Option<&'static str> {
        match self {
            Self::Unavailable(_) => Some("authority_unavailable"),
            Self::Refused { reason, .. } => Some(reason.as_str()),
            Self::Csr(_) | Self::PublicKey(_) | Self::Sign(_) => None,
        }
    }

    /// The status the gateway keys off. Anything about this node's own CA is
    /// `UNAVAILABLE`, whether the authority was down or refused: the node
    /// retries both itself, so the gateway's only correct move is to ask again
    /// later. The reason rides in the message prefix and in
    /// [`CREDMGR_ERROR_REASON_METADATA_KEY`] so dashboards and the gateway can
    /// tell the two apart without parsing prose. `INVALID_ARGUMENT` means fix
    /// the request; `INTERNAL` means this node is at fault.
    pub fn to_status(&self) -> Status {
        let message = self.to_string();
        match self {
            Self::Csr(_) | Self::PublicKey(_) => Status::invalid_argument(message),
            Self::Sign(_) => Status::internal(message),
            Self::Unavailable(_) | Self::Refused { .. } => {
                let reason = self
                    .unavailable_reason()
                    .expect("node CA capability failures carry a reason");
                let value = format!("{CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX}{reason}");
                let mut metadata = MetadataMap::new();
                metadata.insert(
                    CREDMGR_ERROR_REASON_METADATA_KEY,
                    value
                        .parse()
                        .expect("node CA reasons are fixed ASCII identifiers"),
                );
                Status::with_metadata(Code::Unavailable, format!("{value}: {message}"), metadata)
            }
        }
    }
}

/// A certificate from the authority and the private key it certifies.
struct Intermediate {
    certificate_der: Vec<u8>,
    issuer: Issuer<'static, KeyPair>,
    not_after: OffsetDateTime,
}

impl Intermediate {
    /// rcgen signs happily under an expired issuer, and the failure then shows
    /// up inside a guest's handshake with nothing on this node to point at.
    fn expired(&self) -> bool {
        self.not_after <= OffsetDateTime::now_utc()
    }

    /// Check the offered key and signing permissions before adopting a certificate.
    /// rcgen does not enforce an issuer's path-length constraint when signing.
    fn adopt(certificate_der: Vec<u8>, key: KeyPair) -> Result<Self, NodeCaError> {
        let (_, parsed) = X509Certificate::from_der(&certificate_der)
            .map_err(|error| NodeCaError::unusable(format!("not a DER certificate: {error}")))?;
        // Otherwise this node signs gateway CAs with a key its own certificate
        // does not attest, and every chain it produces fails.
        if parsed.tbs_certificate.subject_pki.raw != key.subject_public_key_info() {
            return Err(NodeCaError::unusable(
                "the certificate is over a different key than this node offered",
            ));
        }
        let constraints = parsed
            .basic_constraints()
            .map_err(|error| NodeCaError::unusable(format!("invalid basicConstraints: {error}")))?
            .filter(|extension| extension.value.ca)
            .ok_or_else(|| NodeCaError::unusable("the certificate is not a CA"))?;
        // Cache only issuers that can delegate. A repaired authority must be
        // re-probed on the refusal schedule, not at this certificate's expiry.
        if constraints.value.path_len_constraint == Some(0) {
            return Err(NodeCaError::unusable(
                "the node CA has pathLen:0 and cannot delegate to a gateway",
            ));
        }
        if !parsed
            .key_usage()
            .map_err(|error| NodeCaError::unusable(format!("invalid keyUsage: {error}")))?
            .is_some_and(|extension| extension.value.key_cert_sign())
        {
            return Err(NodeCaError::unusable(
                "the certificate cannot sign certificates",
            ));
        }
        let not_after =
            OffsetDateTime::from_unix_timestamp(parsed.validity().not_after.timestamp())
                .map_err(|error| NodeCaError::unusable(format!("unusable notAfter: {error}")))?;

        let issuer = Issuer::from_ca_cert_der(&certificate_der.as_slice().into(), key)
            .map_err(|error| NodeCaError::unusable(format!("not usable as an issuer: {error}")))?;

        Ok(Self {
            certificate_der,
            issuer,
            not_after,
        })
    }

    fn sign_gateway(&self, public_key: &SubjectPublicKeyInfo) -> Result<Vec<u8>, NodeCaError> {
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "space-gateway-intermediate-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.use_authority_key_identifier_extension = true;
        params.serial_number = Some(random_serial());
        params.not_before = OffsetDateTime::now_utc() - LEAF_CLOCK_SKEW;
        params.not_after = self.not_after;
        params
            .signed_by(public_key, &self.issuer)
            .map(|certificate| certificate.der().to_vec())
            .map_err(|error| NodeCaError::Sign(error.to_string()))
    }
}

fn random_serial() -> SerialNumber {
    let mut bytes = [0u8; SERIAL_BYTES];
    rand::fill(&mut bytes);
    // DER integers are signed; clearing the top bit keeps the serial positive
    // without widening the encoding.
    bytes[0] &= 0x7f;
    SerialNumber::from_slice(&bytes)
}

type IntermediateFuture = Shared<BoxFuture<'static, Result<Arc<Intermediate>, NodeCaError>>>;

/// Obtains a certificate over the offered PKCS#10 request. A boxed closure
/// rather than a trait: the production implementation is one gRPC call, and a
/// test substitutes a local root CA.
type CertifyCsr =
    Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, NodeCaError>> + Send + Sync>;

/// This node's CA key, and the certificate currently covering it.
///
/// A cache in the ordinary sense: [`current`](Self::current) hands back what it
/// holds, refreshes before expiry, and collapses concurrent acquisitions into
/// one fetch. A failed refresh retains the previous CA only while it is valid.
struct IntermediateCache {
    certify: CertifyCsr,
    /// PKCS#8 DER of this node's CA keypair, generated once and reused for the
    /// life of the process. Reuse is load-bearing: the authority derives its
    /// idempotency token from the offered key, so a retry re-attaches to an
    /// in-flight issuance instead of minting a second certificate.
    key_der: Arc<Vec<u8>>,
    /// Built once alongside the key, being a pure function of it.
    csr_der: Vec<u8>,
    current: Mutex<Attempt>,
}

/// One acquisition, and what it earned the next one.
struct Attempt {
    future: IntermediateFuture,
    /// Keeps signing possible during refresh failures. Bundles already issued
    /// carry their own issuer, so switching to a new CA does not invalidate them.
    previous: Option<Arc<Intermediate>>,
    started: Instant,
    /// Where this attempt sits in a run of failures; decides how long a
    /// failure of its own is kept before the next attempt replaces it.
    retry: RetrySchedule,
}

/// How long a failed attempt is kept before the next one replaces it, given
/// the run of failures before it.
#[derive(Clone, Copy, Debug)]
struct RetrySchedule {
    /// Consecutive failed attempts preceding this one, all of the class in
    /// `refused`. Zero after a success.
    failures: u32,
    /// Whether that run was refusals (re-probes of a configuration fact) or
    /// outages (retries). A change of class starts the doubling over.
    refused: bool,
    /// Drawn once per attempt so the wait it implies does not move while
    /// callers keep asking whether it has elapsed.
    jitter: f64,
}

impl RetrySchedule {
    fn first() -> Self {
        Self {
            failures: 0,
            refused: false,
            jitter: Self::draw_jitter(),
        }
    }

    fn draw_jitter() -> f64 {
        1.0 + RETRY_JITTER * (rand::random::<f64>() * 2.0 - 1.0)
    }

    /// The schedule for the attempt after this one, given how this one ended.
    fn next(self, outcome: &Result<Arc<Intermediate>, NodeCaError>) -> Self {
        match outcome {
            Ok(_) => Self::first(),
            Err(error) => {
                let refused = matches!(error, NodeCaError::Refused { .. });
                Self {
                    failures: if refused == self.refused {
                        self.failures.saturating_add(1)
                    } else {
                        1
                    },
                    refused,
                    jitter: Self::draw_jitter(),
                }
            }
        }
    }

    /// An outage is retried fast, a refusal re-probed slowly. Both are spread
    /// by the same jitter: the failure that produced them hit the whole fleet
    /// at the same moment.
    fn wait_after(self, error: &NodeCaError) -> Duration {
        let refused = matches!(error, NodeCaError::Refused { .. });
        let same_class_failures = if refused == self.refused {
            self.failures
        } else {
            0
        };
        let doubling = 1u32.checked_shl(same_class_failures).unwrap_or(u32::MAX);
        if refused {
            INITIAL_REPROBE_BACKOFF
                .saturating_mul(doubling)
                .min(MAX_REPROBE_BACKOFF)
        } else {
            INITIAL_ACQUIRE_BACKOFF
                .saturating_mul(doubling)
                .min(MAX_ACQUIRE_BACKOFF)
        }
        .mul_f64(self.jitter)
    }
}

impl IntermediateCache {
    fn new(certify: CertifyCsr) -> Result<Self, rcgen::Error> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        // One fixed common name and nothing else: every field of the issued
        // certificate is the authority's to set, and it refuses a request
        // carrying anything it would discard.
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, CSR_COMMON_NAME);
        let mut params = CertificateParams::default();
        params.distinguished_name = name;
        let csr_der = params.serialize_request(&key)?.der().to_vec();

        let key_der = Arc::new(key.serialize_der());
        // Built here but never polled, so nothing is fetched until the first
        // leaf is asked for.
        let current = Mutex::new(Attempt {
            future: Self::acquire(&certify, &csr_der, &key_der, None),
            previous: None,
            started: Instant::now(),
            retry: RetrySchedule::first(),
        });
        Ok(Self {
            certify,
            key_der,
            csr_der,
            current,
        })
    }

    /// Refresh within the renewal window, waiting at most ACQUIRE_TIMEOUT.
    /// On failure, keep signing with the previous CA until it expires.
    async fn current(&self) -> Result<Arc<Intermediate>, NodeCaError> {
        let (future, previous) = {
            let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
            // `peek` is `None` while an issuance is in flight, so every caller
            // arriving during one joins it instead of starting another.
            let stale = match current.future.peek() {
                None => false,
                Some(Ok(intermediate)) => {
                    intermediate.not_after <= OffsetDateTime::now_utc() + RENEW_BEFORE
                }
                // `Shared` caches the error too, so a failure has to be
                // replaced or this node never retries — but not before its
                // wait elapses, or the inbound request rate becomes the
                // upstream call rate. Inside the wait every caller gets the
                // cached failure at once. A refusal waits longest: it is a
                // configuration fact that changes on a deploy, and the fleet
                // must not hammer the authority until one happens.
                Some(Err(error)) => current.started.elapsed() >= current.retry.wait_after(error),
            };
            if stale {
                let outcome = current.future.peek().expect("a stale attempt has resolved");
                if let Err(error) = outcome
                    && let Some(reason) = error.unavailable_reason()
                {
                    pplx_space_metrics::credmgr::node_ca_reprobe(reason);
                    tracing::warn!(
                        reason,
                        %error,
                        failures = current.retry.failures,
                        waited_secs = current.started.elapsed().as_secs(),
                        "re-probing the credential authority for this node's CA after a failure"
                    );
                }
                let previous = match outcome {
                    Ok(intermediate) => Some(Arc::clone(intermediate)),
                    Err(_) => current.previous.clone(),
                };
                let retry = current.retry.next(outcome);
                *current = Attempt {
                    future: Self::acquire(
                        &self.certify,
                        &self.csr_der,
                        &self.key_der,
                        previous.as_ref().map(|intermediate| intermediate.not_after),
                    ),
                    previous,
                    started: Instant::now(),
                    retry,
                };
            }
            if matches!(current.future.peek(), Some(Ok(_)))
                || current
                    .previous
                    .as_ref()
                    .is_some_and(|previous| previous.expired())
            {
                current.previous = None;
            }
            (current.future.clone(), current.previous.clone())
        };
        match future.await {
            Ok(intermediate) if !intermediate.expired() => Ok(intermediate),
            result => {
                // Acquisition may consume the remaining overlap, so check the
                // fallback after awaiting it, never just when starting refresh.
                if let Some(previous) = previous.filter(|previous| !previous.expired()) {
                    return Ok(previous);
                }
                result.and_then(|_| {
                    Err(NodeCaError::Unavailable(
                        "the node CA has expired".to_string(),
                    ))
                })
            }
        }
    }

    /// An associated function rather than a method: the constructor needs it
    /// before `self` exists.
    fn acquire(
        certify: &CertifyCsr,
        csr_der: &[u8],
        key_der: &Arc<Vec<u8>>,
        previous_not_after: Option<OffsetDateTime>,
    ) -> IntermediateFuture {
        let certified = certify(csr_der.to_vec());
        let key_der = Arc::clone(key_der);
        async move {
            let key = KeyPair::from_pkcs8_der_and_sign_algo(
                &key_der.as_slice().into(),
                &PKCS_ECDSA_P256_SHA256,
            )
            .expect("a keypair this node generated round-trips through its own PKCS#8");
            let certificate_der = tokio::time::timeout(ACQUIRE_TIMEOUT, certified)
                .await
                .map_err(|_| {
                    NodeCaError::Unavailable(format!(
                        "no answer within {}s",
                        ACQUIRE_TIMEOUT.as_secs()
                    ))
                })??;
            let intermediate = Intermediate::adopt(certificate_der, key)?;
            if intermediate.expired()
                || previous_not_after.is_some_and(|previous| intermediate.not_after <= previous)
            {
                return Err(NodeCaError::Unavailable(
                    "the authority returned an expired node CA or did not extend its validity"
                        .to_string(),
                ));
            }
            tracing::info!(
                not_after = %intermediate.not_after,
                "this node holds a new intermediate CA certificate"
            );
            Ok(Arc::new(intermediate))
        }
        .inspect(move |result| {
            if previous_not_after.is_some() && let Err(error) = result {
                tracing::warn!(%error, "node CA refresh failed; retaining the previous CA until expiry");
            }
        })
        .boxed()
        .shared()
    }
}

/// Signs gateway CAs under the intermediate this node holds.
pub struct NodeCa {
    intermediate: IntermediateCache,
}

impl NodeCa {
    /// Certify a gateway CSR and return its issuer alongside it.
    /// Acquisition is bounded by ACQUIRE_TIMEOUT.
    pub async fn issue_intermediate(
        &self,
        request: IssueIntermediateCertificateRequest,
    ) -> Result<IssueIntermediateCertificateResponse, NodeCaError> {
        // Reject malformed requests before lazy acquisition can reach AWS.
        let csr = NodeCsr::parse(&request.csr)?;
        let public_key = SubjectPublicKeyInfo::from_der(csr.public_key_der())
            .map_err(|error| NodeCaError::PublicKey(error.to_string()))?;
        let intermediate = self.intermediate.current().await?;
        Ok(IssueIntermediateCertificateResponse {
            certificate_der: intermediate.sign_gateway(&public_key)?,
            issuer_certificate_der: intermediate.certificate_der.clone(),
        })
    }

    pub fn new(upstream: CredentialManagerGrpcClient) -> Result<Self, rcgen::Error> {
        Self::with_certifier(Arc::new(move |csr_der: Vec<u8>| {
            let mut upstream = upstream.clone();
            async move {
                upstream
                    .issue_intermediate_certificate(IssueIntermediateCertificateRequest {
                        csr: csr_der,
                    })
                    .await
                    .map(|response| response.into_inner().certificate_der)
                    .map_err(certify_error)
            }
            .boxed()
        }))
    }

    fn with_certifier(certify: CertifyCsr) -> Result<Self, rcgen::Error> {
        Ok(Self {
            intermediate: IntermediateCache::new(certify)?,
        })
    }
}

/// Split the authority's answer by how soon asking again could help. The
/// retryable set is the authority's documented one: unreachable, throttled,
/// TokenReview down, projected token aged out, or an issuance not yet finished.
/// Everything else changes on a deploy rather than on a retry, so it is
/// re-probed slowly: `FAILED_PRECONDITION` names a cluster whose authority has
/// no intermediate CA configured; the rest — `PERMISSION_DENIED` for a node not
/// on the allow list, `INVALID_ARGUMENT` for a request this node built wrong,
/// anything unexpected — is a refusal without a more specific name.
fn certify_error(status: Status) -> NodeCaError {
    let detail = format!("{:?}: {}", status.code(), status.message());
    match status.code() {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Unauthenticated
        | Code::Aborted
        | Code::Cancelled => NodeCaError::Unavailable(detail),
        Code::FailedPrecondition => {
            NodeCaError::refused(RefusalReason::AuthorityNotConfigured, detail)
        }
        _ => NodeCaError::refused(RefusalReason::AuthorityRefused, detail),
    }
}

#[cfg(test)]
mod tests;
