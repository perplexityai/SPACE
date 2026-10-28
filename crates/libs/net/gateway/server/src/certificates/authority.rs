//! One execution-owned CA, acquired independently of guest requests. Only public
//! CSRs and certificates cross HostControl; private keys stay in the gateway.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::timing::{ACQUIRE_TIMEOUT, AuthorityTiming, MIN_REMAINING};
use arc_swap::ArcSwapOption;
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use jiff::Timestamp;
use pplx_space_net_common::DomainName;
use pplx_space_spaced_common::proto::host_control::{
    IssueGatewayIntermediateCertificateRequest, IssueGatewayIntermediateCertificateResponse,
    host_control_client::HostControlClient,
};
use pplx_space_spaced_common::{LazyClient, connect_uds_channel};
use rcgen::{
    CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use rustls::pki_types::CertificateDer;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::ingress::ServicePortIngressState;
use crate::state::{GatewayNetworkState, ReadinessObserver};
use pplx_space_metrics::gateway::readiness::{self as metrics, Outcome};

const MAX_CERTIFICATE_BYTES: usize = 16 * 1024;
const LEAF_LIFETIME_SECS: i64 = 86_400;
/// Fixed subject Common Name for every leaf; the SAN carries the hostname.
const LEAF_COMMON_NAME: &str = "SPACE Gateway";
// Delegation cannot outlive the verified node CA. No independent AWS lifetime
// or idempotency contract applies to a certificate signed locally by that node.

fn launch_capability(
    env: &dyn pplx_space_common_core::env::Environment,
) -> Result<zeroize::Zeroizing<Vec<u8>>, Status> {
    parse_launch_capability(
        env.var(pplx_space_common_core::host::linux::gateway::LAUNCH_CAPABILITY_ENV),
    )
}

fn parse_launch_capability(
    value: Result<String, std::env::VarError>,
) -> Result<zeroize::Zeroizing<Vec<u8>>, Status> {
    let capability = match value {
        Ok(capability) => capability,
        // During preparation, an older supervisor cannot supply launch authority.
        // The manager still uses its existing peer/execution checks. Only absence
        // is tolerated: malformed authority and RPC refusals never downgrade.
        Err(std::env::VarError::NotPresent) => return Ok(zeroize::Zeroizing::new(Vec::new())),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(Status::failed_precondition(
                "gateway launch authority is malformed",
            ));
        }
    };
    if capability.len() != 64 || !capability.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Status::failed_precondition(
            "gateway launch authority is malformed",
        ));
    }
    Ok(zeroize::Zeroizing::new(capability.into_bytes()))
}

#[derive(Clone)]
pub struct GatewayAuthority {
    inner: Arc<AuthorityOwner>,
}

struct AuthorityOwner {
    launch_capability: Result<zeroize::Zeroizing<Vec<u8>>, Status>,
    socket: PathBuf,
    channel: LazyClient<tonic::transport::Channel>,
    identity: ServicePortIngressState,
    shutdown: CancellationToken,
    current: ArcSwapOption<AuthorityLease>,
    readiness_observer: Option<ReadinessObserver>,
    timing: AuthorityTiming,
}

impl std::fmt::Debug for GatewayAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayAuthority")
            .field("ready", &self.is_ready())
            .finish_non_exhaustive()
    }
}

struct AuthorityFence {
    identity: CancellationToken,
    shutdown: CancellationToken,
    retired: CancellationToken,
    not_before: i64,
    not_after: i64,
}

impl AuthorityFence {
    fn current(&self) -> bool {
        let now = Timestamp::now().as_second();
        !self.identity.is_cancelled()
            && !self.shutdown.is_cancelled()
            && !self.retired.is_cancelled()
            && self.not_before <= now
            && now < self.not_after
    }
}

pub(super) struct AuthorityLease {
    issuer: rcgen::Issuer<'static, KeyPair>,
    pub(super) certificate: Vec<u8>,
    pub(super) node_certificate: Vec<u8>,
    fence: AuthorityFence,
    renew_at: i64,
}

pub(super) struct SignedLeaf {
    pub der: Vec<u8>,
    pub not_before: i64,
    pub not_after: i64,
}

impl AuthorityLease {
    pub(super) fn execution_token(&self) -> CancellationToken {
        self.fence.identity.child_token()
    }
    pub(super) fn is_usable(&self) -> bool {
        self.fence.current()
    }
    pub(super) fn retired(&self) -> CancellationToken {
        self.fence.retired.child_token()
    }

    pub(super) fn sign_leaf(
        &self,
        host: &DomainName,
        public_key: &KeyPair,
    ) -> Result<SignedLeaf, Status> {
        if !self.is_usable() {
            return Err(Status::failed_precondition("gateway CA is not ready"));
        }
        let now = Timestamp::now().as_second();
        let not_before = (now - 60).max(self.fence.not_before);
        let not_after = (now + LEAF_LIFETIME_SECS).min(self.fence.not_after);
        let mut params = CertificateParams::new(vec![host.to_string()])
            .map_err(|_| Status::invalid_argument("invalid leaf hostname"))?;
        // The SAN carries the hostname. A fixed subject also accommodates DNS
        // names longer than X.509's 64-character Common Name limit.
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, LEAF_COMMON_NAME);
        params.not_before = time::OffsetDateTime::from_unix_timestamp(not_before)
            .map_err(|_| Status::internal("invalid leaf validity"))?;
        params.not_after = time::OffsetDateTime::from_unix_timestamp(not_after)
            .map_err(|_| Status::internal("invalid leaf validity"))?;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.use_authority_key_identifier_extension = true;
        let mut serial = [0u8; 16];
        SystemRandom::new()
            .fill(&mut serial)
            .map_err(|_| Status::internal("leaf serial generation failed"))?;
        serial[0] &= 0x7f;
        serial[15] |= 1;
        params.serial_number = Some(serial.to_vec().into());
        let der = params
            .signed_by(public_key, &self.issuer)
            .map_err(|_| Status::internal("local leaf signing failed"))?
            .der()
            .to_vec();
        if !self.is_usable() {
            return Err(Status::aborted("gateway CA changed during signing"));
        }
        Ok(SignedLeaf {
            der,
            not_before,
            not_after,
        })
    }
}

struct Candidate {
    key: KeyPair,
    csr: Vec<u8>,
    epoch: CancellationToken,
}

impl Candidate {
    fn new(epoch: CancellationToken) -> Result<Self, Status> {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|_| Status::internal("gateway CA key generation failed"))?;
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(
            DnType::CommonName,
            pplx_space_credmgr_client::INTERMEDIATE_CSR_COMMON_NAME,
        );
        let csr = params
            .serialize_request(&key)
            .map_err(|_| Status::internal("gateway CA CSR generation failed"))?
            .der()
            .to_vec();
        Ok(Self { key, csr, epoch })
    }

    fn validate(
        &self,
        response: IssueGatewayIntermediateCertificateResponse,
        shutdown: CancellationToken,
    ) -> Result<AuthorityLease, Status> {
        // HostControl authenticates the delegation; the guest owns root trust.
        let gateway_ca = parse_ca_certificate(&response.certificate_der)?;
        let node_ca = parse_ca_certificate(&response.issuer_certificate_der)?;
        self.validate_key_and_issuer(&gateway_ca, &node_ca)?;
        validate_ca_profiles(&gateway_ca, &node_ca)?;
        let now = Timestamp::now().as_second();
        let (not_before, not_after) = validate_ca_validity(&gateway_ca, &node_ca, now)?;
        let fence = AuthorityFence {
            identity: self.epoch.clone(),
            shutdown,
            retired: CancellationToken::new(),
            not_before,
            not_after,
        };
        let issuer = rcgen::Issuer::from_ca_cert_der(
            &CertificateDer::from(response.certificate_der.clone()),
            KeyPair::try_from(self.key.serialized_der())
                .map_err(|_| Status::internal("gateway CA key loading failed"))?,
        )
        .map_err(|_| Status::failed_precondition("invalid gateway CA issuer"))?;

        Ok(AuthorityLease {
            issuer,
            certificate: response.certificate_der,
            node_certificate: response.issuer_certificate_der,
            fence,
            renew_at: self.renewal_time(now, not_after)?,
        })
    }

    fn validate_key_and_issuer(
        &self,
        gateway_ca: &X509Certificate<'_>,
        node_ca: &X509Certificate<'_>,
    ) -> Result<(), Status> {
        if gateway_ca.public_key().raw != self.key.subject_public_key_info() {
            return Err(Status::failed_precondition("gateway CA key mismatch"));
        }
        if gateway_ca.issuer() != node_ca.subject() {
            return Err(Status::failed_precondition("gateway CA issuer mismatch"));
        }
        gateway_ca
            .verify_signature(Some(node_ca.public_key()))
            .map_err(|_| Status::failed_precondition("gateway CA does not match its node issuer"))
    }

    fn renewal_time(&self, now: i64, not_after: i64) -> Result<i64, Status> {
        // Spread renewals between 70% and 75% of the remaining lifetime, using
        // the random candidate public key, but start earlier when necessary
        // to preserve the shared admission/recovery window on short-lived CAs.
        // Never extend validity after outages.
        let jitter = i64::from(
            self.key
                .der_bytes()
                .get(1)
                .ok_or_else(|| Status::internal("missing CA public key"))?
                % 6,
        );
        Ok((now + (not_after - now) * (70 + jitter) / 100)
            .min(not_after - MIN_REMAINING.as_secs() as i64))
    }
}

fn parse_ca_certificate(der: &[u8]) -> Result<X509Certificate<'_>, Status> {
    if der.is_empty() || der.len() > MAX_CERTIFICATE_BYTES {
        return Err(Status::failed_precondition("invalid CA certificate size"));
    }
    let (trailing, certificate) = X509Certificate::from_der(der)
        .map_err(|_| Status::failed_precondition("invalid CA certificate"))?;
    if !trailing.is_empty() {
        return Err(Status::failed_precondition("trailing CA certificate bytes"));
    }
    Ok(certificate)
}

fn validate_ca_profiles(
    gateway_ca: &X509Certificate<'_>,
    node_ca: &X509Certificate<'_>,
) -> Result<(), Status> {
    let constraints = gateway_ca
        .basic_constraints()
        .map_err(|_| Status::failed_precondition("invalid gateway CA constraints"))?;
    if !constraints
        .is_some_and(|c| c.critical && c.value.ca && c.value.path_len_constraint == Some(0))
    {
        return Err(Status::failed_precondition("invalid gateway CA profile"));
    }
    let constraints = node_ca
        .basic_constraints()
        .map_err(|_| Status::failed_precondition("invalid node CA constraints"))?;
    if !constraints.is_some_and(|c| {
        c.critical && c.value.ca && c.value.path_len_constraint.is_none_or(|n| n >= 1)
    }) {
        return Err(Status::failed_precondition("invalid node CA profile"));
    }
    for certificate in [gateway_ca, node_ca] {
        let usage = certificate
            .key_usage()
            .map_err(|_| Status::failed_precondition("invalid CA key usage"))?;
        if !usage.is_some_and(|u| u.value.key_cert_sign()) {
            return Err(Status::failed_precondition(
                "CA does not permit certificate signing",
            ));
        }
        let usage = certificate
            .extended_key_usage()
            .map_err(|_| Status::failed_precondition("invalid CA extended key usage"))?;
        if usage.is_some_and(|u| !u.value.any && !u.value.server_auth) {
            return Err(Status::failed_precondition(
                "CA does not permit server authentication",
            ));
        }
    }
    Ok(())
}

fn validate_ca_validity(
    gateway_ca: &X509Certificate<'_>,
    node_ca: &X509Certificate<'_>,
    now: i64,
) -> Result<(i64, i64), Status> {
    let not_before = gateway_ca.validity().not_before.timestamp();
    let not_after = gateway_ca.validity().not_after.timestamp();
    if not_before > now || not_after <= not_before {
        return Err(Status::failed_precondition("invalid gateway CA validity"));
    }
    if node_ca.validity().not_before.timestamp() > now
        || not_after > node_ca.validity().not_after.timestamp()
    {
        return Err(Status::failed_precondition(
            "node CA does not cover gateway CA validity",
        ));
    }
    if not_after <= now + MIN_REMAINING.as_secs() as i64 {
        return Err(Status::unavailable("gateway CA awaiting node CA renewal"));
    }
    Ok((not_before, not_after))
}

impl GatewayAuthority {
    pub fn new(
        socket: PathBuf,
        identity: ServicePortIngressState,
        shutdown: CancellationToken,
    ) -> Self {
        Self::from_environment_impl(
            &pplx_space_common_core::env::ProcessEnvironment,
            socket,
            identity,
            shutdown,
        )
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
        socket: PathBuf,
        identity: ServicePortIngressState,
        shutdown: CancellationToken,
    ) -> Self {
        Self::from_environment_impl(env, socket, identity, shutdown)
    }

    fn from_environment_impl(
        env: &dyn pplx_space_common_core::env::Environment,
        socket: PathBuf,
        identity: ServicePortIngressState,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(AuthorityOwner {
                launch_capability: launch_capability(env),
                socket,
                identity,
                shutdown,
                channel: LazyClient::new(),
                current: ArcSwapOption::empty(),
                readiness_observer: None,
                timing: AuthorityTiming::default(),
            }),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.current().is_ok()
    }

    #[must_use]
    pub fn with_readiness_observer(mut self, observer: ReadinessObserver) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("attach readiness observer before cloning the authority")
            .readiness_observer = Some(observer);
        self
    }

    /// Shorten the owner loop's polling cadence.
    ///
    /// The loop drives a real tonic server here, so `tokio::time::pause` is
    /// not available to it; a test observes the same ordering by shrinking
    /// every interval together instead.
    #[cfg(test)]
    #[must_use]
    pub(super) fn with_timing(mut self, timing: AuthorityTiming) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("set authority timing before cloning the authority")
            .timing = timing;
        self
    }

    fn observe_readiness(&self) {
        if let Some(observer) = &self.inner.readiness_observer {
            observer(self.is_ready());
        }
    }

    pub(super) fn current(&self) -> Result<Arc<AuthorityLease>, Status> {
        self.inner
            .current
            .load_full()
            .filter(|ca| ca.is_usable())
            .ok_or_else(|| Status::failed_precondition("gateway CA is not ready"))
    }

    fn clear(&self) {
        if let Some(old) = self.inner.current.swap(None) {
            old.fence.retired.cancel();
        }
        self.observe_readiness();
    }

    /// A single supervised owner handles startup and renewal after an installed
    /// policy requests interception. No guest request invokes this operation.
    /// Each RPC, including connection, is bounded to 45s.
    pub fn spawn(&self, network: GatewayNetworkState) -> tokio::task::JoinHandle<()> {
        let owner = self.clone();
        tokio::spawn(async move {
            if network
                .wait_for_interception_policy(&owner.inner.shutdown)
                .await
            {
                owner.run().await;
            }
        })
    }

    async fn request(
        &self,
        candidate: &Candidate,
    ) -> Result<IssueGatewayIntermediateCertificateResponse, Status> {
        let operation = async {
            let channel = self
                .inner
                .channel
                .get_or_connect(|| async {
                    connect_uds_channel("sandbox-host-control", &self.inner.socket)
                        .await
                        .map_err(|_| {
                            Status::unavailable("gateway CA HostControl connection failed")
                        })
                })
                .await?;
            let mut client = HostControlClient::new(
                pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
            )
            .max_decoding_message_size(2 * MAX_CERTIFICATE_BYTES + 1024);
            let mut request = tonic::Request::new(IssueGatewayIntermediateCertificateRequest {
                csr: candidate.csr.clone(),
                launch_capability: self
                    .inner
                    .launch_capability
                    .as_ref()
                    .map_err(Clone::clone)?
                    .to_vec(),
                execution_generation: self
                    .inner
                    .identity
                    .identity_with_epoch()?
                    .0
                    .and_then(|identity| identity.execution_generation)
                    .ok_or_else(|| {
                        Status::failed_precondition("gateway has no execution identity")
                    })?
                    .get(),
            });
            request.set_timeout(Duration::from_secs(42));
            client
                .issue_gateway_intermediate_certificate(request)
                .await
                .map(tonic::Response::into_inner)
        };
        tokio::select! {
            biased;
            () = self.inner.shutdown.cancelled() => Err(Status::cancelled("gateway stopped")),
            () = candidate.epoch.cancelled() => Err(Status::aborted("gateway execution changed")),
            result = tokio::time::timeout(ACQUIRE_TIMEOUT, operation) => result.map_err(|_| Status::unavailable("gateway CA acquisition deadline"))?,
        }
    }

    async fn run(&self) {
        let mut candidate: Option<Arc<Candidate>> = None;
        let mut acquisition: Option<metrics::Wait> = None;
        let timing = self.inner.timing;
        let mut backoff = timing.base_backoff;
        let mut blocked_epoch: Option<CancellationToken> = None;
        loop {
            if self.inner.shutdown.is_cancelled() {
                break;
            }
            if self
                .inner
                .current
                .load()
                .as_ref()
                .is_some_and(|ca| !ca.is_usable())
            {
                self.clear();
            }
            if candidate.as_ref().is_some_and(|c| c.epoch.is_cancelled()) {
                candidate = None;
                if let Some(mut acquisition) = acquisition.take() {
                    acquisition.outcome = Outcome::Invalidated;
                }
                backoff = timing.base_backoff;
            }
            if blocked_epoch.as_ref().is_some_and(|e| !e.is_cancelled()) {
                if self.pause(timing.blocked_poll).await {
                    break;
                }
                continue;
            }
            blocked_epoch = None;
            if self
                .current()
                .is_ok_and(|ca| Timestamp::now().as_second() < ca.renew_at)
            {
                if self.pause(timing.renew_poll).await {
                    break;
                }
                continue;
            }
            if candidate.is_none() {
                let Ok((Some(identity), epoch)) = self.inner.identity.identity_with_epoch() else {
                    if self.pause(timing.blocked_poll).await {
                        break;
                    }
                    continue;
                };
                if identity.sandbox_id.is_none()
                    || identity.execution_generation.is_none()
                    || identity.route_generation.is_none()
                {
                    if self.pause(timing.blocked_poll).await {
                        break;
                    }
                    continue;
                }
                acquisition.get_or_insert_with(|| {
                    metrics::ca_acquisition(self.inner.current.load().is_some())
                });
                match tokio::task::spawn_blocking(move || Candidate::new(epoch)).await {
                    Ok(Ok(value)) => candidate = Some(Arc::new(value)),
                    _ => {
                        tracing::error!("gateway CA candidate generation failed");
                        if self.pause(timing.candidate_backoff).await {
                            break;
                        }
                        continue;
                    }
                }
            }
            let pending = candidate.as_ref().expect("candidate created above").clone();
            let result = match self.request(&pending).await {
                Ok(response) => {
                    let pending = pending.clone();
                    let shutdown = self.inner.shutdown.clone();
                    tokio::task::spawn_blocking(move || pending.validate(response, shutdown))
                        .await
                        .map_err(|_| Status::internal("gateway CA validation worker failed"))
                        .and_then(|value| value)
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(ca) if ca.is_usable() => {
                    let ca = Arc::new(ca);
                    // Zero overlap: renewal retires old leaves and flows. A
                    // later rollout can choose graceful draining explicitly.
                    if let Some(old) = self.inner.current.load_full() {
                        old.fence.retired.cancel();
                    }
                    self.inner.current.store(Some(ca));
                    self.observe_readiness();
                    if let Some(mut acquisition) = acquisition.take() {
                        acquisition.outcome = Outcome::Ready;
                    }
                    tracing::info!("gateway CA installed");
                    candidate = None;
                    backoff = timing.base_backoff;
                }
                Ok(_) => {
                    candidate = None;
                    if let Some(mut acquisition) = acquisition.take() {
                        acquisition.outcome = if self.inner.shutdown.is_cancelled() {
                            Outcome::Cancelled
                        } else {
                            Outcome::Invalidated
                        };
                    }
                }
                Err(error) => {
                    // Internal/Unknown do not prove revocation: transient
                    // service failures must not discard an unexpired CA or
                    // strand this execution. Explicit auth failures remain
                    // terminal, and expiry still fences every use. Policy
                    // hydration already confirmed registration before this
                    // owner started. PermissionDenied therefore retires this
                    // epoch even if cleanup preceded its first CA.
                    //
                    // Node-capability failures — the node credential agent
                    // holds no intermediate CA because its authority is down,
                    // not configured, or refusing — arrive as
                    // Unavailable with a typed `x-space-credmgr-error-reason`.
                    // The node re-probes its authority itself, so this loop
                    // keeps retrying with the same key under the capped
                    // backoff below rather than blocking the epoch: a CA the
                    // node cannot issue today may be issued after the next
                    // deploy without restarting this gateway. Guest egress
                    // fails closed per connection in the meantime.
                    //
                    // Whether to keep the CA already held and whether to keep
                    // asking are separate questions. Only a transient failure
                    // answers yes to the first: anything explicit retires the
                    // current authority, cancelling its fence and every leaf
                    // lease under it, because a refusal may be this execution
                    // being told it is no longer entitled to sign.
                    //
                    // FailedPrecondition answers yes to the second even though
                    // it retires the CA. The node credential agent reports a
                    // refusal from the central authority with that code, and it
                    // recovers from one without a gateway restart, so treating
                    // it as terminal parks this epoch waiting for a
                    // route-identity replacement that nothing issues. That is
                    // how an outage can outlive the repair. Sandbox-manager's own
                    // verdicts share the code and cannot be told apart from it
                    // here, so they are asked again too; the CA is retired
                    // either way, so neither keeps signing.
                    let transient = matches!(
                        error.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::ResourceExhausted
                            | tonic::Code::Cancelled
                            | tonic::Code::Internal
                            | tonic::Code::Unknown
                    );
                    let retryable = transient || error.code() == tonic::Code::FailedPrecondition;
                    tracing::warn!(
                        code = ?error.code(),
                        reason = credmgr_error_reason(&error).unwrap_or("none"),
                        retryable,
                        retry_backoff_ms = backoff.as_millis() as u64,
                        "gateway CA acquisition failed"
                    );
                    if !transient {
                        self.clear();
                    }
                    if !retryable {
                        blocked_epoch = Some(pending.epoch.clone());
                        candidate = None;
                        if let Some(mut acquisition) = acquisition.take() {
                            acquisition.outcome = if self.inner.shutdown.is_cancelled() {
                                Outcome::Cancelled
                            } else if pending.epoch.is_cancelled() {
                                Outcome::Invalidated
                            } else {
                                Outcome::Failed
                            };
                        }
                    }
                    if self.pause(backoff).await {
                        break;
                    }
                    backoff = (backoff * 2).min(timing.max_backoff);
                }
            }
        }
        self.clear();
    }

    async fn pause(&self, duration: Duration) -> bool {
        tokio::select! { biased; () = self.inner.shutdown.cancelled() => true, () = tokio::time::sleep(duration) => false }
    }
}

/// Machine-readable reason the node credential agent attached to an issuance
/// failure, when sandbox-manager forwarded its status unchanged. Only used for
/// diagnostics: retry classification keys off the gRPC code alone.
fn credmgr_error_reason(status: &Status) -> Option<&str> {
    status
        .metadata()
        .get(pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
}

#[cfg(test)]
pub(super) mod tests;
