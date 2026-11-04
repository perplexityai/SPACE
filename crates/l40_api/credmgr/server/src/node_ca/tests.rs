//! Weighted toward what the gateway CA certificate *says*: that is what a
//! guest's TLS stack validates, and it is what a future edit is most likely to
//! break.

use std::sync::atomic::{AtomicUsize, Ordering};

use rcgen::{BasicConstraints, CertificateSigningRequestParams, KeyUsagePurpose};
use tokio::task::JoinSet;
use x509_parser::extensions::GeneralName;

use super::*;

/// Stands in for AWS Private CA behind the credential authority: signs the
/// offered request as a `pathlen:1` subordinate CA, the way the authority's
/// template does.
struct TestRoot {
    certificate_der: Vec<u8>,
    issuer: Issuer<'static, KeyPair>,
    calls: AtomicUsize,
    delay: Duration,
    /// Certify a different key, as a mixed-up authority would.
    substitute_key: bool,
}

/// Deliberately not a round number a leaf might coincidentally be given.
const INTERMEDIATE_VALIDITY: Duration = Duration::from_secs(6 * 60 * 60 + 137);

impl TestRoot {
    fn new() -> Arc<Self> {
        Self::build(Duration::ZERO, false)
    }

    fn build(delay: Duration, substitute_key: bool) -> Arc<Self> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate root key");
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, "test root CA");
        let mut params = CertificateParams::default();
        params.distinguished_name = name;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let certificate_der = params
            .self_signed(&key)
            .expect("self-sign the root")
            .der()
            .to_vec();
        let issuer = Issuer::from_ca_cert_der(&certificate_der.as_slice().into(), key)
            .expect("the root is an issuer");
        Arc::new(Self {
            certificate_der,
            issuer,
            calls: AtomicUsize::new(0),
            delay,
            substitute_key,
        })
    }

    fn certify(&self, csr_der: Vec<u8>) -> Vec<u8> {
        self.certify_with_path_len(csr_der, 1)
    }

    fn certify_with_path_len(&self, csr_der: Vec<u8>, path_len: u8) -> Vec<u8> {
        self.certify_until(
            csr_der,
            path_len,
            OffsetDateTime::now_utc() + INTERMEDIATE_VALIDITY,
        )
    }

    fn certify_until(&self, csr_der: Vec<u8>, path_len: u8, not_after: OffsetDateTime) -> Vec<u8> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = CertificateSigningRequestParams::from_der(&csr_der.as_slice().into())
            .expect("the node offers a parseable PKCS#10 request");
        assert_eq!(
            request.params.distinguished_name.get(&DnType::CommonName),
            Some(&rcgen::DnValue::Utf8String(CSR_COMMON_NAME.into())),
            "the authority validates this subject exactly",
        );

        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, "node intermediate CA");
        let mut params = CertificateParams::default();
        params.distinguished_name = name;
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(path_len));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.not_after = not_after;

        let signed = if self.substitute_key {
            let other = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate other key");
            params.signed_by(&other, &self.issuer)
        } else {
            params.signed_by(&request.public_key, &self.issuer)
        };
        signed.expect("sign the intermediate").der().to_vec()
    }

    fn node_ca(self: &Arc<Self>) -> NodeCa {
        let root = Arc::clone(self);
        NodeCa::with_certifier(Arc::new(move |csr_der| {
            let root = Arc::clone(&root);
            async move {
                if !root.delay.is_zero() {
                    tokio::time::sleep(root.delay).await;
                }
                Ok(root.certify(csr_der))
            }
            .boxed()
        }))
        .expect("generate this node's CA keypair")
    }
}

/// A gateway keypair, and a request over it.
fn gateway_key() -> KeyPair {
    KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate a gateway key")
}

fn ca_request(key: &KeyPair) -> IssueIntermediateCertificateRequest {
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, CSR_COMMON_NAME);
    IssueIntermediateCertificateRequest {
        csr: params.serialize_request(key).unwrap().der().to_vec(),
    }
}

/// The contract a gateway relies on: `UNAVAILABLE`, so it keeps retrying, with
/// the reason in metadata and at the head of the message.
fn assert_node_ca_unavailable(status: &Status, reason: &str) {
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert!(
        pplx_space_credmgr_client::status_is_node_ca_unavailable(status),
        "{status:?}"
    );
    let token = format!("{CREDMGR_ERROR_REASON_NODE_CA_UNAVAILABLE_PREFIX}{reason}");
    assert_eq!(
        status
            .metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok()),
        Some(token.as_str())
    );
    assert!(
        status.message().starts_with(&format!("{token}: ")),
        "{:?}",
        status.message()
    );
}

/// Upper and lower bounds of a jittered re-probe wait after `failures`
/// consecutive refusals.
fn reprobe_wait_bounds(failures: u32) -> (Duration, Duration) {
    jitter_bounds((INITIAL_REPROBE_BACKOFF * 2u32.pow(failures)).min(MAX_REPROBE_BACKOFF))
}

/// The same, for the faster schedule an outage is retried on.
fn acquire_wait_bounds(failures: u32) -> (Duration, Duration) {
    jitter_bounds((INITIAL_ACQUIRE_BACKOFF * 2u32.pow(failures)).min(MAX_ACQUIRE_BACKOFF))
}

fn jitter_bounds(nominal: Duration) -> (Duration, Duration) {
    (
        nominal.mul_f64(1.0 - RETRY_JITTER),
        nominal.mul_f64(1.0 + RETRY_JITTER),
    )
}

fn dns_names(leaf: &X509Certificate<'_>) -> Vec<String> {
    leaf.subject_alternative_name()
        .expect("a parseable SAN")
        .expect("a SAN extension")
        .value
        .general_names
        .iter()
        .map(|name| match name {
            GeneralName::DNSName(name) => (*name).to_string(),
            other => panic!("unexpected general name {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn gateway_bundle_certifies_its_key_and_can_sign_a_leaf() {
    let root = TestRoot::new();
    let node = root.node_ca();
    let gateway_key = gateway_key();
    let request = ca_request(&gateway_key);
    let before_issuance = OffsetDateTime::now_utc().unix_timestamp();
    let bundle = node.issue_intermediate(request.clone()).await.unwrap();
    let after_issuance = OffsetDateTime::now_utc().unix_timestamp();
    let again = node.issue_intermediate(request).await.unwrap();
    assert_eq!(root.calls.load(Ordering::SeqCst), 1);
    let (_, gateway) = X509Certificate::from_der(&bundle.certificate_der).unwrap();
    let (_, node) = X509Certificate::from_der(&bundle.issuer_certificate_der).unwrap();
    let (_, root) = X509Certificate::from_der(&root.certificate_der).unwrap();
    let (_, again) = X509Certificate::from_der(&again.certificate_der).unwrap();
    assert_ne!(gateway.raw_serial(), again.raw_serial());
    assert_eq!(
        gateway.public_key().raw,
        gateway_key.subject_public_key_info()
    );
    let constraints = gateway.basic_constraints().unwrap().unwrap();
    assert!(constraints.critical && constraints.value.ca);
    assert_eq!(constraints.value.path_len_constraint, Some(0));
    let usage = gateway.key_usage().unwrap().unwrap();
    assert!(usage.critical && usage.value.key_cert_sign() && usage.value.crl_sign());
    assert!(!usage.value.digital_signature());
    assert!(
        gateway
            .extended_key_usage()
            .unwrap()
            .unwrap()
            .value
            .server_auth
    );
    assert_eq!(gateway.validity().not_after, node.validity().not_after);
    gateway.verify_signature(Some(node.public_key())).unwrap();
    node.verify_signature(Some(root.public_key())).unwrap();

    // Snapshot the issued wire certificate's profile, not random key/signature
    // bytes or wall-clock timestamps. Key binding and chain signatures above
    // remain independent assertions over the actual RPC result.
    let extensions: Vec<_> = gateway
        .extensions()
        .iter()
        .map(|extension| {
            use x509_parser::extensions::ParsedExtension;
            let value = match extension.parsed_extension() {
                ParsedExtension::SubjectKeyIdentifier(_) => {
                    "derived subject key identifier".to_owned()
                }
                ParsedExtension::AuthorityKeyIdentifier(_) => {
                    "derived issuer key identifier".to_owned()
                }
                other => format!("{other:?}"),
            };
            (extension.oid.to_id_string(), extension.critical, value)
        })
        .collect();
    assert!(
        (before_issuance - 300..=after_issuance - 300)
            .contains(&gateway.validity().not_before.timestamp())
    );
    insta::assert_debug_snapshot!(
        "issued_gateway_ca_profile",
        (
            gateway.version(),
            gateway.subject().to_string(),
            gateway.issuer().to_string(),
            gateway.signature_algorithm.algorithm.to_id_string(),
            gateway.public_key().algorithm.algorithm.to_id_string(),
            gateway
                .public_key()
                .algorithm
                .parameters
                .as_ref()
                .unwrap()
                .as_oid()
                .unwrap()
                .to_id_string(),
            gateway.validity().not_after.timestamp() - node.validity().not_after.timestamp(),
            extensions,
        )
    );

    // A consumer needs only its own private key and the returned bundle to
    // issue a leaf; neither credential-manager process participates in signing.
    let issuer =
        Issuer::from_ca_cert_der(&bundle.certificate_der.as_slice().into(), gateway_key).unwrap();
    let leaf_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let mut params = CertificateParams::new(vec!["api.example.com".to_string()]).unwrap();
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf = params.signed_by(&leaf_key, &issuer).unwrap();
    let (_, leaf) = X509Certificate::from_der(leaf.der()).unwrap();
    leaf.verify_signature(Some(gateway.public_key())).unwrap();
    assert_eq!(dns_names(&leaf), ["api.example.com"]);
}

#[tokio::test(start_paused = true)]
async fn gateway_renewal_extends_the_chain_before_expiry() {
    let root = TestRoot::new();
    let certifier = Arc::clone(&root);
    let old_deadline = OffsetDateTime::now_utc() + Duration::from_secs(10 * 60);
    let node = Arc::new(
        NodeCa::with_certifier(Arc::new(move |csr| {
            let root = Arc::clone(&certifier);
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let not_after = if root.calls.load(Ordering::SeqCst) == 0 {
                    old_deadline
                } else {
                    old_deadline + INTERMEDIATE_VALIDITY
                };
                Ok(root.certify_until(csr, 1, not_after))
            }
            .boxed()
        }))
        .unwrap(),
    );
    let request = ca_request(&gateway_key());
    let old = node.issue_intermediate(request.clone()).await.unwrap();

    let mut renewals = JoinSet::new();
    for _ in 0..16 {
        let node = Arc::clone(&node);
        let request = request.clone();
        renewals.spawn(async move { node.issue_intermediate(request).await.unwrap() });
    }
    let replacements = renewals.join_all().await;
    assert_eq!(
        root.calls.load(Ordering::SeqCst),
        2,
        "renewals share one acquisition"
    );
    let (_, old_gateway) = X509Certificate::from_der(&old.certificate_der).unwrap();
    let (_, old_node) = X509Certificate::from_der(&old.issuer_certificate_der).unwrap();
    let (_, root_certificate) = X509Certificate::from_der(&root.certificate_der).unwrap();
    let after_old_expiry = x509_parser::time::ASN1Time::from_timestamp(
        old_gateway.validity().not_after.timestamp() + 1,
    )
    .unwrap();
    assert!(!old_gateway.validity().is_valid_at(after_old_expiry));

    for replacement in &replacements {
        assert_eq!(
            replacement.issuer_certificate_der,
            replacements[0].issuer_certificate_der
        );
        let (_, gateway) = X509Certificate::from_der(&replacement.certificate_der).unwrap();
        let (_, node) = X509Certificate::from_der(&replacement.issuer_certificate_der).unwrap();
        assert!(gateway.validity().not_after > old_gateway.validity().not_after);
        assert_eq!(gateway.validity().not_after, node.validity().not_after);
        assert!(gateway.validity().is_valid_at(after_old_expiry));
        assert!(node.validity().is_valid_at(after_old_expiry));
        for (gateway, node) in [(&old_gateway, &old_node), (&gateway, &node)] {
            assert!(
                gateway.validity().is_valid() && node.validity().is_valid(),
                "both chains overlap"
            );
            gateway.verify_signature(Some(node.public_key())).unwrap();
            node.verify_signature(Some(root_certificate.public_key()))
                .unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn failed_refresh_retains_the_valid_ca_and_backs_off_before_retrying() {
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        Unavailable,
        Unchanged,
        Expired,
    }

    for failure in [Failure::Unavailable, Failure::Unchanged, Failure::Expired] {
        let root = TestRoot::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let old_deadline = OffsetDateTime::now_utc() + Duration::from_secs(10 * 60);
        let node = NodeCa::with_certifier(Arc::new(move |csr| {
            let attempt = counted.fetch_add(1, Ordering::SeqCst);
            let root = Arc::clone(&root);
            async move {
                let not_after = match attempt {
                    0 => old_deadline,
                    1 => match failure {
                        Failure::Unavailable => {
                            return Err(NodeCaError::Unavailable("authority down".to_string()));
                        }
                        Failure::Unchanged => old_deadline,
                        Failure::Expired => OffsetDateTime::now_utc() - Duration::from_secs(1),
                    },
                    _ => old_deadline + INTERMEDIATE_VALIDITY,
                };
                Ok(root.certify_until(csr, 1, not_after))
            }
            .boxed()
        }))
        .unwrap();
        let key = gateway_key();
        let old = node.issue_intermediate(ca_request(&key)).await.unwrap();
        for _ in 0..4 {
            let gateway = node.issue_intermediate(ca_request(&key)).await.unwrap();
            assert_eq!(gateway.issuer_certificate_der, old.issuer_certificate_der);
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "{failure:?}: failed refresh is backed off"
        );
        // The wait is jittered, so only its upper bound is guaranteed to clear it.
        tokio::time::advance(acquire_wait_bounds(0).1).await;
        let replacement = node.issue_intermediate(ca_request(&key)).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let (_, old) = X509Certificate::from_der(&old.certificate_der).unwrap();
        let (_, replacement) = X509Certificate::from_der(&replacement.certificate_der).unwrap();
        assert!(replacement.validity().not_after > old.validity().not_after);
    }
}

#[tokio::test]
async fn expired_node_certificates_cannot_issue_gateway_cas_when_acquisition_is_unavailable() {
    let root = TestRoot::new();
    let node = NodeCa::with_certifier(Arc::new(|_| {
        async { Err(NodeCaError::Unavailable("authority down".to_string())) }.boxed()
    }))
    .unwrap();
    // Seed the state left by an idle node whose last acquisition has expired.
    // Certificate time is wall time, independent of Tokio's paused retry clock.
    let certificate = root.certify_until(
        node.intermediate.csr_der.clone(),
        1,
        OffsetDateTime::now_utc() - Duration::from_secs(1),
    );
    let key = KeyPair::from_pkcs8_der_and_sign_algo(
        &node.intermediate.key_der.as_slice().into(),
        &PKCS_ECDSA_P256_SHA256,
    )
    .unwrap();
    node.intermediate.current.lock().unwrap().future =
        futures_util::future::ready(Ok(Arc::new(Intermediate::adopt(certificate, key).unwrap())))
            .boxed()
            .shared();

    let key = gateway_key();
    let error = node.issue_intermediate(ca_request(&key)).await.unwrap_err();
    assert_eq!(error.to_status().code(), Code::Unavailable);
}

#[tokio::test]
async fn gateway_issuance_validates_the_csr_before_acquiring_the_node_ca() {
    let root = TestRoot::new();
    let node = root.node_ca();
    let mut bad_signature = ca_request(&gateway_key()).csr;
    *bad_signature.last_mut().unwrap() ^= 1;
    // The CSR validator tests own the full input matrix; this pins validation
    // and proof of key possession before the node contacts its authority.
    for csr in [vec![], bad_signature] {
        let error = node
            .issue_intermediate(IssueIntermediateCertificateRequest { csr })
            .await
            .unwrap_err();
        assert_eq!(error.to_status().code(), Code::InvalidArgument);
    }
    assert_eq!(root.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn refuses_authority_certificates_without_ca_signing_permissions() {
    for is_ca in [
        IsCa::ExplicitNoCa,
        IsCa::Ca(BasicConstraints::Constrained(1)),
    ] {
        let key = gateway_key();
        let mut params = CertificateParams::default();
        params.is_ca = is_ca;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let certificate = params.self_signed(&key).unwrap();
        assert!(matches!(
            Intermediate::adopt(certificate.der().to_vec(), key),
            Err(NodeCaError::Refused {
                reason: RefusalReason::UnusableCertificate,
                ..
            })
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn legacy_issuer_recovers_after_authority_repair() {
    let root = TestRoot::new();
    let path_len = Arc::new(AtomicUsize::new(0));
    let node = NodeCa::with_certifier(Arc::new({
        let root = root.clone();
        let path_len = path_len.clone();
        move |csr| {
            let root = root.clone();
            let path_len = path_len.clone();
            async move {
                Ok(root.certify_with_path_len(csr, path_len.load(Ordering::SeqCst) as u8))
            }
            .boxed()
        }
    }))
    .unwrap();
    let gateway_key = gateway_key();
    let error = node
        .issue_intermediate(ca_request(&gateway_key))
        .await
        .unwrap_err();
    assert_node_ca_unavailable(
        &error.to_status(),
        RefusalReason::UnusableCertificate.as_str(),
    );
    assert_eq!(root.calls.load(Ordering::SeqCst), 1);

    path_len.store(1, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(15 * 60)).await;
    let issued = node
        .issue_intermediate(ca_request(&gateway_key))
        .await
        .expect("repairing the authority must recover the same node without a restart");
    assert_eq!(root.calls.load(Ordering::SeqCst), 2);
    let (_, gateway) = X509Certificate::from_der(&issued.certificate_der).unwrap();
    let (_, issuer) = X509Certificate::from_der(&issued.issuer_certificate_der).unwrap();
    assert_eq!(
        gateway.public_key().raw,
        gateway_key.subject_public_key_info()
    );
    gateway.verify_signature(Some(issuer.public_key())).unwrap();
}

/// Without coalescing a node's first burst of connections would be one issuance
/// each. Paused time makes it deterministic: every task parks on the shared
/// future before the clock advances past the issuance delay.
#[tokio::test(start_paused = true)]
async fn concurrent_requests_share_one_issuance() {
    let root = TestRoot::build(Duration::from_secs(1), false);
    let node_ca = Arc::new(root.node_ca());

    let mut requests = JoinSet::new();
    for _ in 0..16 {
        let node_ca = Arc::clone(&node_ca);
        requests.spawn(async move { node_ca.issue_intermediate(ca_request(&gateway_key())).await });
    }
    while let Some(joined) = requests.join_next().await {
        joined
            .expect("the request task ran")
            .expect("every waiter gets a gateway CA");
    }

    assert_eq!(root.calls.load(Ordering::SeqCst), 1);
}

/// Signing under a key the certificate does not attest produces a chain that
/// fails inside a guest's handshake, with nothing on the node to point at.
#[tokio::test]
async fn refuses_a_certificate_issued_over_a_different_key() {
    let error = TestRoot::build(Duration::ZERO, true)
        .node_ca()
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect_err("the certificate is refused");

    assert!(
        matches!(
            error,
            NodeCaError::Refused {
                reason: RefusalReason::UnusableCertificate,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_node_ca_unavailable(
        &error.to_status(),
        RefusalReason::UnusableCertificate.as_str(),
    );
}

/// An authority that accepts the call and never answers. Without a bound every
/// caller joins a future that can never resolve, and only a restart recovers.
#[tokio::test(start_paused = true)]
async fn an_authority_that_never_answers_times_out() {
    let node_ca = NodeCa::with_certifier(Arc::new(|_csr| std::future::pending().boxed()))
        .expect("generate this node's CA keypair");

    let error = node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect_err("the call must not hang");
    assert_eq!(error.to_status().code(), Code::Unavailable);
}

/// A retryable failure must not turn the inbound gateway-CA request rate into the
/// upstream call rate; it must still be retried; and a run of them must back
/// off, or a fleet keeps hammering an authority that is staying down.
#[tokio::test(start_paused = true)]
async fn consecutive_retryable_failures_back_off_exponentially() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let node_ca = NodeCa::with_certifier(Arc::new(move |_csr| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move { Err(NodeCaError::Unavailable("authority down".to_string())) }.boxed()
    }))
    .expect("generate this node's CA keypair");

    for _ in 0..4 {
        node_ca
            .issue_intermediate(ca_request(&gateway_key()))
            .await
            .expect_err("the authority is down");
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "four requests, one attempt"
    );

    let attempt = async |after: Duration| {
        tokio::time::advance(after).await;
        node_ca
            .issue_intermediate(ca_request(&gateway_key()))
            .await
            .expect_err("the authority is still down");
        calls.load(Ordering::SeqCst)
    };
    // Waits are jittered, so advance by the bound that settles each question
    // whatever was drawn: the upper bound clears a wait, and a later wait's
    // lower bound is what must still exceed the time already advanced.
    let (_, first_upper) = acquire_wait_bounds(0);
    let (second_lower, second_upper) = acquire_wait_bounds(1);
    assert!(
        first_upper < second_lower,
        "the doubling outruns the jitter"
    );
    assert_eq!(attempt(first_upper).await, 2, "the wait expires");
    // The next wait is twice the first, so the same advance is not enough.
    assert_eq!(attempt(first_upper).await, 2, "still backing off");
    assert_eq!(attempt(second_upper).await, 3, "twice the wait expires");
}

/// The authority has no intermediate CA configured, so it
/// refuses. The refusal must not poison the node for the life of the process;
/// once the authority is fixed the node recovers on its next re-probe, and the
/// gateways it refused meanwhile were told to keep asking.
#[tokio::test(start_paused = true)]
async fn a_refusal_is_reprobed_after_backoff_and_recovers_without_a_restart() {
    let root = TestRoot::new();
    let configured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let (counted, enabled, certifier) = (Arc::clone(&calls), Arc::clone(&configured), root.clone());
    let node_ca = NodeCa::with_certifier(Arc::new(move |csr| {
        counted.fetch_add(1, Ordering::SeqCst);
        let (enabled, root) = (Arc::clone(&enabled), Arc::clone(&certifier));
        async move {
            if enabled.load(Ordering::SeqCst) {
                Ok(root.certify(csr))
            } else {
                Err(certify_error(Status::failed_precondition(
                    "intermediate CA issuer is not configured",
                )))
            }
        }
        .boxed()
    }))
    .unwrap();
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();

    // Every caller inside the wait is answered from the cached refusal.
    for _ in 0..8 {
        let error = node_ca
            .issue_intermediate(ca_request(&gateway_key()))
            .await
            .expect_err("the authority is not configured");
        assert!(
            matches!(
                error,
                NodeCaError::Refused {
                    reason: RefusalReason::AuthorityNotConfigured,
                    ..
                }
            ),
            "got {error:?}"
        );
        assert_node_ca_unavailable(
            &error.to_status(),
            RefusalReason::AuthorityNotConfigured.as_str(),
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "eight requests, one probe");

    // The re-probe fires inside the jittered window around the first wait and
    // not before it.
    let (lower, upper) = reprobe_wait_bounds(0);
    tokio::time::advance(lower - Duration::from_millis(1)).await;
    node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect_err("still inside the wait");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no probe before the wait");
    tokio::time::advance(upper - lower + Duration::from_millis(2)).await;
    node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect_err("still refused");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one re-probe after the wait"
    );

    // A second consecutive refusal waits about twice as long.
    let (lower, upper) = reprobe_wait_bounds(1);
    tokio::time::advance(lower - Duration::from_millis(1)).await;
    node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect_err("still inside the doubled wait");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // The deploy lands. Nothing restarts; the next probe succeeds and the
    // cached refusal is gone.
    configured.store(true, Ordering::SeqCst);
    tokio::time::advance(upper - lower + Duration::from_millis(2)).await;
    let bundle = node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect("the node recovers on its next probe");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let (_, gateway) = X509Certificate::from_der(&bundle.certificate_der).unwrap();
    let (_, node) = X509Certificate::from_der(&bundle.issuer_certificate_der).unwrap();
    gateway.verify_signature(Some(node.public_key())).unwrap();
    node_ca
        .issue_intermediate(ca_request(&gateway_key()))
        .await
        .expect("a held CA serves without another probe");
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let reprobes: Vec<_> = capture
        .finish()
        .into_iter()
        .filter(|counter| counter.name == "credmgr.node_ca.reprobes")
        .collect();
    assert_eq!(reprobes.len(), 2, "one counter per re-probe: {reprobes:?}");
    for reprobe in reprobes {
        assert_eq!(reprobe.value, 1);
        assert_eq!(reprobe.tags, ["reason:authority_not_configured"]);
    }
}

/// Consecutive refusals double the wait up to the cap, jitter included; a
/// change of failure class or a success starts the doubling over.
#[test]
fn reprobe_waits_double_to_a_cap_and_reset_on_success() {
    let refusal = || NodeCaError::refused(RefusalReason::AuthorityRefused, "no");
    let refused = || Err::<Arc<Intermediate>, _>(refusal());
    let mut schedule = RetrySchedule::first();
    let mut previous = Duration::ZERO;
    for failures in 0..12 {
        let wait = schedule.wait_after(&refusal());
        let (lower, upper) = reprobe_wait_bounds(failures);
        assert!(
            (lower..=upper).contains(&wait),
            "failures={failures}: {wait:?} outside {lower:?}..={upper:?}"
        );
        assert!(wait <= MAX_REPROBE_BACKOFF.mul_f64(1.0 + RETRY_JITTER));
        if failures > 0 && failures < 4 {
            assert!(wait > previous, "failures={failures}: the wait grows");
        }
        previous = wait;
        schedule = schedule.next(&refused());
    }
    // An outage after a run of refusals is retried on the fast schedule from
    // the start, and a refusal after a run of outages likewise.
    let outage = NodeCaError::Unavailable("down".to_string());
    let (outage_lower, outage_upper) = acquire_wait_bounds(0);
    assert!((outage_lower..=outage_upper).contains(&schedule.wait_after(&outage)));
    let after_outage = schedule.next(&Err(outage.clone()));
    let (doubled_lower, doubled_upper) = acquire_wait_bounds(1);
    assert!((doubled_lower..=doubled_upper).contains(&after_outage.wait_after(&outage)));
    let (lower, upper) = reprobe_wait_bounds(0);
    assert!((lower..=upper).contains(&after_outage.wait_after(&refusal())));

    // A success forgets the run entirely.
    let root = TestRoot::new();
    let node = NodeCa::with_certifier(Arc::new(|_| std::future::pending().boxed())).unwrap();
    let key = KeyPair::from_pkcs8_der_and_sign_algo(
        &node.intermediate.key_der.as_slice().into(),
        &PKCS_ECDSA_P256_SHA256,
    )
    .unwrap();
    let held = Intermediate::adopt(root.certify(node.intermediate.csr_der.clone()), key).unwrap();
    let after_success = after_outage.next(&Ok(Arc::new(held)));
    assert_eq!(after_success.failures, 0);
    assert!((lower..=upper).contains(&after_success.wait_after(&refusal())));
    assert!((outage_lower..=outage_upper).contains(&after_success.wait_after(&outage)));
}

/// The recovery budget bounds how fast a fleet recovers after the deploy that
/// fixes an authority. Every other test here derives its expectations from these same
/// constants, so it would follow a change to hours without complaint and
/// leave the documents contradicting the code. Spell them out once.
#[test]
fn the_documented_retry_schedule_is_what_the_code_uses() {
    assert_eq!(INITIAL_ACQUIRE_BACKOFF, Duration::from_secs(5));
    assert_eq!(MAX_ACQUIRE_BACKOFF, Duration::from_secs(60));
    assert_eq!(INITIAL_REPROBE_BACKOFF, Duration::from_secs(60));
    assert_eq!(MAX_REPROBE_BACKOFF, Duration::from_secs(10 * 60));
    // Both caps are reached before the jitter is applied, so the documented
    // worst case is the cap plus its spread, not the cap. These two also pin
    // the spread itself: only a 20% jitter turns 10m into 12m and 60s into 72s.
    assert_eq!(
        MAX_REPROBE_BACKOFF.mul_f64(1.0 + RETRY_JITTER),
        Duration::from_secs(12 * 60)
    );
    assert_eq!(
        MAX_ACQUIRE_BACKOFF.mul_f64(1.0 + RETRY_JITTER),
        Duration::from_secs(72)
    );
}

/// The jitter has to be real and it has to hold still. A constant spread
/// desynchronises nothing, and one re-drawn per call moves the deadline under
/// callers that are asking whether it has passed yet.
#[test]
fn jitter_spreads_waits_across_attempts_and_holds_within_one() {
    let refusal = NodeCaError::refused(RefusalReason::AuthorityRefused, "no");
    let outage = NodeCaError::Unavailable("down".to_string());

    for error in [&refusal, &outage] {
        let waits: std::collections::BTreeSet<_> = (0..64)
            .map(|_| RetrySchedule::first().wait_after(error))
            .collect();
        // A fleet refused at the same instant draws these independently, so a
        // spread narrow enough to collide here is one that re-probes in step.
        assert!(
            waits.len() >= 8,
            "{} distinct waits across 64 attempts: the jitter is not spreading them",
            waits.len()
        );

        let schedule = RetrySchedule::first();
        let first = schedule.wait_after(error);
        for _ in 0..8 {
            assert_eq!(
                schedule.wait_after(error),
                first,
                "the wait moved while the same attempt was being asked about it"
            );
        }
    }
}

/// Gateways arriving while a re-probe is in flight join it; gateways arriving
/// inside the wait get the cached refusal at once. Either way one node makes
/// one upstream call per probe, whatever its request rate.
#[tokio::test(start_paused = true)]
async fn concurrent_callers_coalesce_on_one_reprobe() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let node_ca = Arc::new(
        NodeCa::with_certifier(Arc::new(move |_csr| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Err(certify_error(Status::permission_denied(
                    "node not on the allow list",
                )))
            }
            .boxed()
        }))
        .unwrap(),
    );

    let burst = |node_ca: &Arc<NodeCa>| {
        let mut requests = JoinSet::new();
        for _ in 0..16 {
            let node_ca = Arc::clone(node_ca);
            requests.spawn(async move {
                node_ca
                    .issue_intermediate(ca_request(&gateway_key()))
                    .await
                    .expect_err("the authority refuses")
            });
        }
        requests.join_all()
    };
    for error in burst(&node_ca).await {
        assert_node_ca_unavailable(&error.to_status(), RefusalReason::AuthorityRefused.as_str());
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the first burst shares one probe"
    );

    // Inside the wait: served from the cache, no upstream call.
    burst(&node_ca).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Past the wait: the next burst shares one re-probe.
    let (_, upper) = reprobe_wait_bounds(0);
    tokio::time::advance(upper + Duration::from_millis(1)).await;
    burst(&node_ca).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the second burst shares one re-probe"
    );
}

/// The wire split gateways key off. Only faults in the request or in this
/// node are anything other than `UNAVAILABLE`.
#[test]
fn statuses_split_capability_failures_from_request_and_node_faults() {
    let cases = [
        (
            certify_error(Status::failed_precondition("no issuer")),
            "authority_not_configured",
        ),
        (
            certify_error(Status::permission_denied("not allow listed")),
            "authority_refused",
        ),
        (
            certify_error(Status::invalid_argument("bad csr")),
            "authority_refused",
        ),
        (certify_error(Status::internal("boom")), "authority_refused"),
        (
            certify_error(Status::unavailable("down")),
            "authority_unavailable",
        ),
        (
            certify_error(Status::unauthenticated("token aged out")),
            "authority_unavailable",
        ),
        (NodeCaError::unusable("pathLen:0"), "unusable_certificate"),
    ];
    for (error, reason) in cases {
        assert_node_ca_unavailable(&error.to_status(), reason);
    }
    assert!(matches!(
        certify_error(Status::unavailable("down")),
        NodeCaError::Unavailable(_)
    ));

    let sign = NodeCaError::Sign("rcgen".to_string()).to_status();
    assert_eq!(sign.code(), Code::Internal);
    assert!(
        sign.metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .is_none()
    );
    let public_key = NodeCaError::PublicKey("bad".to_string()).to_status();
    assert_eq!(public_key.code(), Code::InvalidArgument);
    assert!(
        public_key
            .metadata()
            .get(CREDMGR_ERROR_REASON_METADATA_KEY)
            .is_none()
    );
}
