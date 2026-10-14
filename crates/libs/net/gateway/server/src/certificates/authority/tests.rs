use super::super::{CertificateContext, CertificateProvider};
use super::*;
use crate::state::{GatewayNetworkState, NetworkStateReason, PulledNetworkPolicy};
use pplx_space_common_svc::core::types::{
    ExecutionGeneration, RouteGeneration, SandboxId, SpaceApiRouteIdentity,
};

#[test]
fn launch_authority_allows_only_absence_during_preparation() {
    use std::env::VarError;

    for (value, expected) in [
        (Err(VarError::NotPresent), Some(Vec::new())),
        (Ok("a".repeat(64)), Some(vec![b'a'; 64])),
        (Ok(String::new()), None),
        (Ok("a".repeat(63)), None),
        (Ok("z".repeat(64)), None),
        (Err(VarError::NotUnicode("invalid".into())), None),
    ] {
        match expected {
            Some(bytes) => assert_eq!(*parse_launch_capability(value).unwrap(), bytes),
            None => assert_eq!(
                parse_launch_capability(value).unwrap_err().code(),
                tonic::Code::FailedPrecondition,
            ),
        }
    }
}

fn root() -> (rcgen::Certificate, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let mut params = CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = params.self_signed(&key).unwrap();
    (certificate, rcgen::Issuer::new(params, key))
}

fn issue(
    candidate: &Candidate,
    root: &rcgen::Issuer<'_, rcgen::KeyPair>,
) -> IssueGatewayIntermediateCertificateResponse {
    let (node_certificate, node) = node(root);
    let mut request =
        rcgen::CertificateSigningRequestParams::from_der(&candidate.csr.clone().into()).unwrap();
    request.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    request.params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    request.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    request.params.not_before =
        time::OffsetDateTime::from_unix_timestamp(Timestamp::now().as_second() - 60).unwrap();
    request.params.not_after =
        time::OffsetDateTime::from_unix_timestamp(Timestamp::now().as_second() + 3600).unwrap();
    IssueGatewayIntermediateCertificateResponse {
        certificate_der: request.signed_by(&node).unwrap().der().to_vec(),
        issuer_certificate_der: node_certificate.der().to_vec(),
    }
}

fn node(
    root: &rcgen::Issuer<'_, rcgen::KeyPair>,
) -> (rcgen::Certificate, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "test node CA");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(1));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.not_before = time::OffsetDateTime::now_utc() - time::Duration::days(1);
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(7);
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = params.signed_by(&key, root).unwrap();
    (certificate, rcgen::Issuer::new(params, key))
}

fn identity() -> SpaceApiRouteIdentity {
    SpaceApiRouteIdentity {
        sandbox_id: Some(SandboxId::new()),
        execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
        route_generation: Some(RouteGeneration::new(1).unwrap()),
        ..Default::default()
    }
}

pub(crate) async fn local_provider() -> (rcgen::Certificate, GatewayAuthority, CertificateProvider)
{
    local_provider_with_state(GatewayNetworkState::pending_policy()).await
}

pub(crate) fn advance_route(owner: &GatewayAuthority) {
    let (identity, _) = owner.inner.identity.identity_with_epoch().unwrap();
    let mut identity = identity.unwrap();
    identity.route_generation =
        Some(RouteGeneration::new(identity.route_generation.unwrap().get() + 1).unwrap());
    owner.inner.identity.update_identity(identity).unwrap();
}

pub(crate) fn leaf_with_lifetime(
    owner: &GatewayAuthority,
    host: DomainName,
    lifetime_secs: i64,
    fence: super::super::material::Fence,
) -> super::super::CertificateLease {
    let key = rcgen::KeyPair::generate().unwrap();
    let now = Timestamp::now().as_second();
    let ca = owner.current().unwrap();
    let mut params = CertificateParams::new(vec![host.to_string()]).unwrap();
    params.not_before = time::OffsetDateTime::from_unix_timestamp(now - 1).unwrap();
    params.not_after = time::OffsetDateTime::from_unix_timestamp(now + lifetime_secs).unwrap();
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf = params.signed_by(&key, &ca.issuer).unwrap();
    super::super::CertificateLease::new(
        super::super::material::VerifiedMaterial {
            host,
            port: 443,
            key: key.into(),
            leaf: leaf.der().to_vec(),
            issuer: ca.certificate.clone(),
            node_issuer: ca.node_certificate.clone(),
            not_before: now - 1,
            not_after: now + lifetime_secs,
        },
        fence,
    )
    .unwrap()
}

pub(crate) async fn local_provider_with_state(
    network: GatewayNetworkState,
) -> (rcgen::Certificate, GatewayAuthority, CertificateProvider) {
    local_provider_with_policy(
        network,
        pplx_space_net_common::NetworkPolicyV1::allow_all(),
        Default::default(),
    )
    .await
}

pub(crate) async fn local_provider_with_policy(
    network: GatewayNetworkState,
    policy: pplx_space_net_common::NetworkPolicyV1,
    network_credentials: crate::state::NetworkCredentialSnapshot,
) -> (rcgen::Certificate, GatewayAuthority, CertificateProvider) {
    let (root_certificate, root) = root();
    let identity = ServicePortIngressState::new(Some(identity()));
    let (_, epoch) = identity.identity_with_epoch().unwrap();
    let candidate = Candidate::new(epoch).unwrap();
    let shutdown = CancellationToken::new();
    let ca = candidate
        .validate(issue(&candidate, &root), shutdown.clone())
        .unwrap();
    let owner = GatewayAuthority::new(
        "/does-not-exist/host-control.sock".into(),
        identity.clone(),
        shutdown.clone(),
    );
    owner.inner.current.store(Some(Arc::new(ca)));
    let pulled = Arc::new(std::sync::Mutex::new(Some((policy, network_credentials))));
    let loader: crate::state::PolicyLoader = Arc::new(move || {
        let (policy, network_credentials) = pulled
            .lock()
            .unwrap()
            .take()
            .expect("local policy loader called once");
        Box::pin(async move {
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref: pplx_space_net_common::EffectiveNetworkPolicyRef::cluster(
                    pplx_space_net_common::EffectiveNetworkPolicyRefScope::Sandbox,
                    SandboxId::new().to_string(),
                    1,
                    "a".repeat(64),
                )
                .unwrap(),
                network_credentials,
            })
        })
    });
    network
        .refresh(loader, None, NetworkStateReason::Startup)
        .await;
    let provider =
        CertificateProvider::new(owner.clone(), CertificateContext::new(network, shutdown));
    (root_certificate, owner, provider)
}

#[tokio::test]
async fn local_authority_signs_leaves_without_host_control_and_retires_leases() {
    let (root_certificate, owner, provider) = local_provider().await;
    let first = provider
        .get("good.com".parse().unwrap(), 443)
        .await
        .unwrap();
    let second = provider
        .get("good.com".parse().unwrap(), 443)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    provider
        .get("other.com".parse().unwrap(), 8443)
        .await
        .unwrap();
    let certified = first.certified_key().unwrap();
    let (_, leaf) = X509Certificate::from_der(&certified.cert[0]).unwrap();
    assert_eq!(leaf.subject().to_string(), "CN=SPACE TLS interception");
    assert_ne!(leaf.subject(), leaf.issuer());
    let san = leaf.subject_alternative_name().unwrap().unwrap();
    assert_eq!(
        san.value.general_names,
        vec![x509_parser::extensions::GeneralName::DNSName("good.com")]
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root_certificate.der().clone()).unwrap();
    let crypto = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut guest_config = rustls::ClientConfig::builder_with_provider(crypto.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    guest_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let mut gateway_config = rustls::ServerConfig::builder_with_provider(crypto)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(rustls::sign::SingleCertAndKey::from(certified)));
    gateway_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let (guest_io, gateway_io) = tokio::io::duplex(16 * 1024);
    let connector = tokio_rustls::TlsConnector::from(Arc::new(guest_config));
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(gateway_config));
    let (guest, gateway) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::try_join!(
            connector.connect("good.com".try_into().unwrap(), guest_io),
            acceptor.accept(gateway_io),
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        guest.get_ref().1.alpn_protocol(),
        Some(b"http/1.1".as_slice())
    );
    assert_eq!(gateway.get_ref().1.server_name(), Some("good.com"));
    owner.clear();
    assert!(!first.is_usable());
    first.certified_key().unwrap_err();
    tokio::time::timeout(Duration::from_secs(1), first.invalidated())
        .await
        .unwrap();
    provider
        .get("new.com".parse().unwrap(), 443)
        .await
        .unwrap_err();
}

#[test]
fn authority_install_requires_matching_key_and_node_issuer() {
    let (_, issuer) = root();
    let epoch = CancellationToken::new();
    let candidate = Candidate::new(epoch.clone()).unwrap();
    let good = issue(&candidate, &issuer);
    candidate
        .validate(good.clone(), CancellationToken::new())
        .unwrap();
    for node_bytes in [
        Vec::new(),
        issue(&candidate, &issuer).issuer_certificate_der,
        {
            let mut bytes = good.issuer_certificate_der.clone();
            bytes.push(0);
            bytes
        },
    ] {
        let mut invalid = good.clone();
        invalid.issuer_certificate_der = node_bytes;
        assert!(
            candidate
                .validate(invalid, CancellationToken::new())
                .is_err(),
            "missing, substituted or trailing-data node certificates must fail installation"
        );
    }
    let other = Candidate::new(CancellationToken::new()).unwrap();
    assert!(
        other
            .validate(good.clone(), CancellationToken::new())
            .is_err()
    );
    let (_, other_root) = root();
    candidate
        .validate(issue(&candidate, &other_root), CancellationToken::new())
        .expect("a consistent HostControl delegation needs no independently pinned root");
    epoch.cancel();
    let late = candidate.validate(good, CancellationToken::new()).unwrap();
    assert!(
        !late.is_usable(),
        "an old execution cannot install a late response"
    );
}

#[test]
fn authority_install_rejects_invalid_ca_profiles() {
    let (_, root) = root();
    let candidate = Candidate::new(CancellationToken::new()).unwrap();
    for invalid_node in [false, true] {
        for profile in ["not_ca", "path_length", "key_usage", "client_only"] {
            let mut node_params = CertificateParams::default();
            node_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(1));
            node_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            let mut request =
                rcgen::CertificateSigningRequestParams::from_der(&candidate.csr.clone().into())
                    .unwrap();
            request.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
            request.params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            request.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let params = if invalid_node {
                &mut node_params
            } else {
                &mut request.params
            };
            match profile {
                "not_ca" => params.is_ca = rcgen::IsCa::ExplicitNoCa,
                "path_length" => {
                    params.is_ca =
                        rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(if invalid_node {
                            0
                        } else {
                            1
                        }))
                }
                "key_usage" => params.key_usages = vec![KeyUsagePurpose::DigitalSignature],
                "client_only" => {
                    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth]
                }
                _ => unreachable!(),
            }
            let node_key = rcgen::KeyPair::generate().unwrap();
            let node_certificate = node_params.signed_by(&node_key, &root).unwrap();
            let node_issuer = rcgen::Issuer::new(node_params, node_key);
            let response = IssueGatewayIntermediateCertificateResponse {
                certificate_der: request.signed_by(&node_issuer).unwrap().der().to_vec(),
                issuer_certificate_der: node_certificate.der().to_vec(),
            };
            let error = candidate
                .validate(response, CancellationToken::new())
                .err()
                .unwrap();
            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                "invalid_node={invalid_node}, profile={profile}"
            );
        }
    }
}

#[test]
fn authority_validity_is_bounded_by_node_ca_and_renewal_headroom() {
    let (_, issuer) = root();
    let candidate = Candidate::new(CancellationToken::new()).unwrap();
    let (node_certificate, node) = node(&issuer);
    for (starts_in, ends_in, accepted) in [
        (-300, 86_400, true),
        (-300, 604_790, true),
        (-300, 604_830, false),
        (30, 3600, false),
        (-3600, -30, false),
        (-3600, 30, false),
        (-3600, 180, false),
        (-3600, 200, true),
        (-3600, 300, true),
    ] {
        let now = Timestamp::now().as_second();
        let mut csr =
            rcgen::CertificateSigningRequestParams::from_der(&candidate.csr.clone().into())
                .unwrap();
        csr.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
        csr.params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        csr.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        csr.params.not_before = time::OffsetDateTime::from_unix_timestamp(now + starts_in).unwrap();
        csr.params.not_after = time::OffsetDateTime::from_unix_timestamp(now + ends_in).unwrap();
        let result = candidate.validate(
            IssueGatewayIntermediateCertificateResponse {
                certificate_der: csr.signed_by(&node).unwrap().der().to_vec(),
                issuer_certificate_der: node_certificate.der().to_vec(),
            },
            CancellationToken::new(),
        );
        assert_eq!(
            result.is_ok(),
            accepted,
            "validity offsets {starts_in}/{ends_in}"
        );
        if let Ok(ca) = result {
            let (_, node_x509) = X509Certificate::from_der(node_certificate.der()).unwrap();
            assert!(ca.fence.not_after <= node_x509.validity().not_after.timestamp());
            assert!(ca.fence.not_after <= now + ends_in);
            assert!(
                ca.renew_at <= ca.fence.not_after - MIN_REMAINING.as_secs() as i64,
                "renewal must start before the leaf admission reserve plus recovery headroom"
            );
            assert!(
                ca.renew_at > now,
                "accepted CA must not immediately reissue"
            );
        } else if ends_in <= 180 && starts_in < ends_in {
            assert_eq!(result.err().unwrap().code(), tonic::Code::Unavailable);
        }
    }

    // An expired certificate signed by a substituted issuer is an integrity failure,
    // not permission to churn candidate keys forever.
    let (_, wrong_root) = root();
    let mut stale =
        rcgen::CertificateSigningRequestParams::from_der(&candidate.csr.clone().into()).unwrap();
    stale.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    stale.params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    stale.params.not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    stale.params.not_after = time::OffsetDateTime::now_utc() - time::Duration::seconds(1);
    let error = candidate
        .validate(
            IssueGatewayIntermediateCertificateResponse {
                certificate_der: stale.signed_by(&wrong_root).unwrap().der().to_vec(),
                issuer_certificate_der: node_certificate.der().to_vec(),
            },
            CancellationToken::new(),
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn owner_issues_over_host_control_after_assignment_and_stops_on_shutdown() {
    use host_control_server::{HostControl, HostControlServer};
    use pplx_space_spaced_common::proto::host_control::*;
    use tonic::{Request, Response};
    enum Reply {
        Error(Status),
        Certificate(i64),
    }
    struct Manager {
        root: rcgen::Issuer<'static, rcgen::KeyPair>,
        node_certificate: rcgen::Certificate,
        calls: tokio::sync::mpsc::Sender<IssueGatewayIntermediateCertificateRequest>,
        replies: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Reply>>,
    }
    #[tonic::async_trait]
    impl HostControl for Manager {
        async fn issue_gateway_intermediate_certificate(
            &self,
            request: Request<IssueGatewayIntermediateCertificateRequest>,
        ) -> Result<Response<IssueGatewayIntermediateCertificateResponse>, Status> {
            let request = request.into_inner();
            self.calls.send(request.clone()).await.unwrap();
            let remaining = match self.replies.lock().await.recv().await.unwrap() {
                Reply::Error(error) => return Err(error),
                Reply::Certificate(remaining) => remaining,
            };
            let mut csr =
                rcgen::CertificateSigningRequestParams::from_der(&request.csr.into()).unwrap();
            csr.params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
            csr.params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            csr.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            csr.params.not_before =
                time::OffsetDateTime::from_unix_timestamp(Timestamp::now().as_second() - 60)
                    .unwrap();
            csr.params.not_after =
                time::OffsetDateTime::from_unix_timestamp(Timestamp::now().as_second() + remaining)
                    .unwrap();
            Ok(Response::new(IssueGatewayIntermediateCertificateResponse {
                certificate_der: csr.signed_by(&self.root).unwrap().der().to_vec(),
                issuer_certificate_der: self.node_certificate.der().to_vec(),
            }))
        }
        async fn get_network_policy(
            &self,
            _: Request<GetNetworkPolicyRequest>,
        ) -> Result<Response<GetNetworkPolicyResponse>, Status> {
            Err(Status::unimplemented("not used by authority owner"))
        }
        async fn set_activity_state(
            &self,
            _: Request<SetActivityStateRequest>,
        ) -> Result<Response<SetActivityStateResponse>, Status> {
            Err(Status::unimplemented("not used by authority owner"))
        }
        async fn request_active_for(
            &self,
            _: Request<RequestActiveForRequest>,
        ) -> Result<Response<RequestActiveForResponse>, Status> {
            Err(Status::unimplemented("not used by authority owner"))
        }
        async fn request_shutdown(
            &self,
            _: Request<RequestShutdownRequest>,
        ) -> Result<Response<RequestShutdownResponse>, Status> {
            Err(Status::unimplemented("not used by authority owner"))
        }
    }
    let (_, root) = root();
    let (node_certificate, root) = node(&root);
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("host-control.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let (calls, mut received) = tokio::sync::mpsc::channel(2);
    let (replies, scripted_replies) = tokio::sync::mpsc::channel(1);
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(HostControlServer::new(Manager {
                root,
                node_certificate,
                calls,
                replies: tokio::sync::Mutex::new(scripted_replies),
            }))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                server_shutdown.cancelled(),
            )
            .await
            .unwrap();
    });
    let assigned = identity();
    let identities = ServicePortIngressState::new(Some(assigned));
    let network = GatewayNetworkState::pending_policy().with_readiness_metrics();
    let authority_cleared = Arc::new(tokio::sync::Notify::new());
    // The owner drives a real tonic UDS server here, so the loop's scheduling
    // pauses cannot run on a paused clock. Shrink the intervals instead: the
    // ordering each assertion below depends on is the loop's, and it is
    // unchanged because the intervals stay far longer than the
    // microsecond-scale work between them.
    let timing = AuthorityTiming {
        renew_poll: Duration::from_millis(25),
        blocked_poll: Duration::from_millis(25),
        base_backoff: Duration::from_millis(25),
        max_backoff: Duration::from_secs(3),
        candidate_backoff: Duration::from_millis(500),
    };
    let mut owner = GatewayAuthority::new(socket, identities.clone(), shutdown.clone())
        .with_timing(timing)
        .with_readiness_observer({
            let observe = network.interception_readiness_observer();
            let authority_cleared = authority_cleared.clone();
            Arc::new(move |ready| {
                observe(ready);
                if !ready {
                    authority_cleared.notify_one();
                }
            })
        });
    Arc::get_mut(&mut owner.inner).unwrap().launch_capability =
        Ok(zeroize::Zeroizing::new(vec![b'a'; 64]));
    let owner_lifetime = Arc::downgrade(&owner.inner);
    let network = network.with_traffic_gate({
        let owner = owner.clone();
        Arc::new(move || owner.is_ready())
    });
    let loader: crate::state::PolicyLoader = Arc::new(|| {
        Box::pin(async {
            let mut policy = pplx_space_net_common::NetworkPolicyV1::allow_all();
            policy.tls_interception = pplx_space_net_common::TlsInterception::On;
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref: pplx_space_net_common::EffectiveNetworkPolicyRef::local(
                    pplx_space_net_common::EffectiveNetworkPolicyRefScope::Sandbox,
                    "sandbox-1",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
                network_credentials: Default::default(),
            })
        })
    });
    let worker = owner.spawn(network.clone());
    // A complete launch identity is not proof that its PID has been registered.
    tokio::time::timeout(Duration::from_millis(300), received.recv())
        .await
        .expect_err("CA acquisition must wait for authenticated policy hydration");
    assert!(!owner.is_ready());
    // The policy is Ready before the CA exists; only the interception
    // gate reports the missing dependency.
    let hydrated = network
        .refresh(loader, None, NetworkStateReason::Startup)
        .await;
    assert_eq!(hydrated.mode, crate::proto::NetworkState::Ready);
    assert_eq!(
        hydrated.interception_readiness,
        crate::state::InterceptionReadiness::DependenciesNotReady
    );
    let request = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    // Cleanup can retire the registration before the first CA is installed.
    // An authorized policy fetch has already ruled out a launch-registration gap.
    replies
        .send(Reply::Error(Status::permission_denied(
            "host control caller is not the active net-gateway for any sandbox",
        )))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), authority_cleared.notified())
        .await
        .expect("cleanup denial must retire even an execution without a CA");
    assert!(!owner.is_ready());
    tokio::time::pause();
    tokio::time::timeout(timing.max_backoff * 2, received.recv())
        .await
        .expect_err("a retired execution must not retry CA acquisition");
    tokio::time::resume();
    advance_route(&owner);
    let replacement_request = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(request.csr, replacement_request.csr);
    let request = replacement_request;
    // Manager-side activation gaps, lost responses, and short-lived node CAs
    // retry the same candidate. There is no gateway AWS-idempotency record to
    // escape by rekeying, and the route identity epoch is unchanged here.
    replies
        .send(Reply::Error(Status::unavailable(
            "gateway CA execution identity is incomplete",
        )))
        .await
        .unwrap();
    let same_key = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.csr, same_key.csr);
    assert!(!owner.is_ready());
    replies.send(Reply::Certificate(-1)).await.unwrap();
    let fresh_key = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(same_key.csr, fresh_key.csr);
    assert!(!owner.is_ready());
    replies.send(Reply::Certificate(3600)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !owner.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    received.try_recv().unwrap_err();
    let installed = network.snapshot().await;
    assert_eq!(installed.mode, crate::proto::NetworkState::Ready);
    assert_eq!(
        installed.interception_readiness,
        crate::state::InterceptionReadiness::Ready
    );

    // Move only the renewal schedule, not certificate validity or the clock.
    // The UDS fixture controls each response so recovery is observed before
    // allowing the next issuance to complete.
    // The node credential agent reports every failure to hold its own CA —
    // authority down, not configured, or refusing — as UNAVAILABLE with a
    // typed reason, and retries the authority itself. The gateway must keep
    // asking rather than retire this epoch. The reason is spelled out rather
    // than built from the shared prefix constant: credmgr and net-gateway
    // deploy separately, so a rename of that constant is a rolling-deploy
    // skew and must fail here instead of moving both sides together.
    let mut node_ca_refused = tonic::metadata::MetadataMap::new();
    node_ca_refused.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        "node_ca_unavailable:authority_not_configured"
            .parse()
            .unwrap(),
    );
    let node_ca_refused = Status::with_metadata(
        tonic::Code::Unavailable,
        "node_ca_unavailable:authority_not_configured: the credential authority refused",
        node_ca_refused,
    );
    assert!(pplx_space_credmgr_client::status_is_node_ca_unavailable(
        &node_ca_refused
    ));
    for error in [
        Status::internal("temporary"),
        Status::unknown("temporary"),
        Status::unavailable("node CA awaiting renewal"),
        node_ca_refused,
        Status::aborted("ownership changed"),
        Status::permission_denied("denied"),
        Status::unauthenticated("denied"),
        // A credential manager predating the typed reason reports every
        // node-CA refusal this way, with no metadata to tell it apart from a
        // sandbox-manager verdict. Retiring the epoch here would leave a
        // gateway rolled ahead of that credential manager permanently without
        // a CA, so it retries and recovers once the refusal stops.
        Status::failed_precondition(
            "node_ca_unavailable:authority_not_configured: the credential authority refused",
        ),
        Status::failed_precondition("invalid profile"),
    ] {
        let installed = owner.inner.current.swap(None).unwrap();
        let mut ca = Arc::try_unwrap(installed).ok().unwrap();
        ca.renew_at = 0;
        let old = Arc::new(ca);
        owner.inner.current.store(Some(old.clone()));
        let attempted = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap()
            .unwrap();
        // Spelled out rather than read from the production classifier, so a
        // change there has to be restated here to pass. Retaining the held CA
        // and continuing to ask are separate outcomes: only a transient
        // failure keeps the CA, while FailedPrecondition retires it and keeps
        // asking anyway.
        let transient = matches!(
            error.code(),
            tonic::Code::Internal | tonic::Code::Unknown | tonic::Code::Unavailable
        );
        let asks_again_in_this_epoch = transient || error.code() == tonic::Code::FailedPrecondition;
        replies.send(Reply::Error(error)).await.unwrap();
        if transient {
            let retried = tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .expect("transient issuance error must retry in the same epoch")
                .unwrap();
            assert!(owner.is_ready(), "renewal must retain its still-valid CA");
            assert_eq!(
                network.snapshot().await.mode,
                crate::proto::NetworkState::Ready
            );
            assert!(old.is_usable());
            assert_eq!(
                attempted.csr, retried.csr,
                "retryable errors preserve the candidate key"
            );
        } else {
            tokio::time::timeout(Duration::from_secs(5), async {
                while owner.is_ready() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let retired = network.snapshot().await;
            assert_eq!(retired.mode, crate::proto::NetworkState::Ready);
            assert_eq!(
                retired.interception_readiness,
                crate::state::InterceptionReadiness::DependenciesNotReady
            );
            assert!(
                !old.is_usable(),
                "explicit denial must retire old authority"
            );
            if !asks_again_in_this_epoch {
                let (current, _) = identities.identity_with_epoch().unwrap();
                let mut next = current.unwrap();
                next.route_generation =
                    Some(RouteGeneration::new(next.route_generation.unwrap().get() + 1).unwrap());
                identities.update_identity(next).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), received.recv())
                .await
                .expect(
                    "a refusal an older credential manager would send must keep this epoch asking",
                )
                .unwrap();
        }
        replies.send(Reply::Certificate(3600)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !owner.current().is_ok_and(|ca| !Arc::ptr_eq(&ca, &old)) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !old.is_usable(),
            "successful replacement keeps zero overlap"
        );
    }
    shutdown.cancel();
    worker.await.unwrap();
    server.await.unwrap();
    assert!(!owner.is_ready());
    let stopped = network.snapshot().await;
    assert_eq!(stopped.mode, crate::proto::NetworkState::Ready);
    assert_eq!(
        stopped.interception_readiness,
        crate::state::InterceptionReadiness::DependenciesNotReady
    );
    drop(network);
    drop(owner);
    assert!(
        owner_lifetime.upgrade().is_none(),
        "readiness observation must not retain the CA owner or its key material"
    );
}

#[tokio::test(start_paused = true)]
async fn owner_shutdown_does_not_wait_for_policy_registration() {
    let shutdown = CancellationToken::new();
    let owner = GatewayAuthority::new(
        "/does-not-exist/host-control.sock".into(),
        ServicePortIngressState::new(Some(identity())),
        shutdown.clone(),
    );
    let worker = owner.spawn(GatewayNetworkState::pending_policy());
    tokio::task::yield_now().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("shutdown must cancel the registration wait")
        .unwrap();
    assert!(!owner.is_ready());
}
