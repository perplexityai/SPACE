use super::*;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pplx_space_common_core::host::linux::gateway::LAUNCH_CAPABILITY_ENV;
use pplx_space_common_core::types::{ExecutionGeneration, RouteGeneration};
use pplx_space_credmgr::node_ca::NodeCa;
use pplx_space_credmgr_client::proto::{
    IssueIntermediateCertificateRequest, IssueIntermediateCertificateResponse,
};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, NetworkPolicyV1, TlsInterception,
};
use pplx_space_net_gateway::certificates::{
    CertificateContext, CertificateProvider, GatewayAuthority,
};
use pplx_space_net_gateway::control::NetGatewayControlService;
use pplx_space_net_gateway::debug::GatewayDebugState;
use pplx_space_net_gateway::ingress::ServicePortIngressState;
use pplx_space_net_gateway::proto::net_gateway_control_server::NetGatewayControl;
use pplx_space_net_gateway::state::{
    GatewayNetworkState, NetworkCredentialSnapshot, PolicyLoader, PolicyNotReady,
    PulledNetworkPolicy, TrafficPolicy,
};
use pplx_space_spaced_common::proto::host_control::host_control_server::HostControlServer;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::task::AbortOnDropHandle;

// The node socket fixture delegates to the production CA cache and status mapper.
// External signing and guest readiness are local fixtures; manager ownership
// checks, gateway acquisition, finalization and cleanup use production code.
#[tokio::test(start_paused = true)]
async fn unavailable_ca_recovers_in_the_same_started_execution() {
    for restore in [false, true] {
        exercise_recovery(restore).await;
    }
}

async fn settle() {
    // Keep virtual time under the test's control while local RPCs make progress.
    for _ in 0..128 {
        tokio::task::yield_now().await;
    }
    // Gateway key generation and the manager's authorization reads run on the
    // blocking pool, which no count of yields waits for. A paused clock does
    // not auto-advance while a blocking task runs, so this sleep also waits
    // for all of them. It moves virtual time by only 1 ms.
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn serve_issuance<F, Fut>(
    socket: &std::path::Path,
    shutdown: CancellationToken,
    issue: F,
) -> AbortOnDropHandle<()>
where
    F: Fn(IssueIntermediateCertificateRequest) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<IssueIntermediateCertificateResponse, Status>>
        + Send
        + 'static,
{
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let service = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        assert_eq!(
            request.uri().path(),
            pplx_space_credmgr::intermediate_ca::ISSUE_INTERMEDIATE_METHOD_PATH
        );
        let issue = issue.clone();
        async move {
            let unary = tower::service_fn(
                move |request: Request<IssueIntermediateCertificateRequest>| {
                    let result = issue(request.into_inner());
                    async move { result.await.map(Response::new) }
                },
            );
            Ok::<_, Infallible>(
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(unary, request)
                    .await,
            )
        }
    });
    AbortOnDropHandle::new(tokio::spawn(async move {
        tokio::select! {
            result = tonic::transport::Server::builder().serve_with_incoming(service, UnixListenerStream::new(listener)) => result.unwrap(),
            () = shutdown.cancelled() => {}
        }
    }))
}

async fn exercise_recovery(restore: bool) {
    let directory = tempfile::tempdir().unwrap();
    let shutdown = CancellationToken::new();
    let configured = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut root_params = CertificateParams::default();
    root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    root_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let root_key = KeyPair::generate().unwrap();
    let root_cert = root_params.self_signed(&root_key).unwrap();
    let root = Arc::new(rcgen::Issuer::new(root_params, root_key));
    let authority_socket = directory.path().join("authority.sock");
    let authority_server = serve_issuance(&authority_socket, shutdown.clone(), {
        let configured = configured.clone();
        let attempts = attempts.clone();
        move |request| {
            let configured = configured.clone();
            let attempts = attempts.clone();
            let root = root.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if !configured.load(Ordering::SeqCst) {
                    return Err(Status::failed_precondition("no intermediate CA configured"));
                }
                let mut csr =
                    CertificateSigningRequestParams::from_der(&request.csr.into()).unwrap();
                csr.params.is_ca = IsCa::Ca(BasicConstraints::Constrained(1));
                csr.params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
                Ok(IssueIntermediateCertificateResponse {
                    certificate_der: csr.signed_by(&root).unwrap().der().to_vec(),
                    issuer_certificate_der: Vec::new(),
                })
            }
        }
    });
    let upstream = pplx_space_credmgr_client::CredmgrEndpoint::Uds(authority_socket)
        .connect()
        .await
        .unwrap();
    let node = Arc::new(NodeCa::new(upstream).unwrap());
    let node_socket = directory.path().join("node.sock");
    let node_server = serve_issuance(&node_socket, shutdown.clone(), move |request| {
        let node = node.clone();
        async move {
            node.issue_intermediate(request)
                .await
                .map_err(|error| error.to_status())
        }
    });

    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let initial_state = if restore {
        SandboxState::Suspended
    } else {
        SandboxState::Stopped
    };
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, &id, initial_state);
    let identity = SpaceApiRouteIdentity {
        sandbox_id: Some(sandbox_id),
        execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
        route_generation: Some(RouteGeneration::new(3).unwrap()),
        ..Default::default()
    };
    let episode = crate::db::begin_activation(
        &conn,
        &id,
        initial_state,
        if restore {
            crate::db::ActivationKind::Restore
        } else {
            crate::db::ActivationKind::ColdStart
        },
        identity.execution_generation,
        None,
    )
    .unwrap()
    .unwrap();
    let (peer, _other) = tokio::net::UnixStream::pair().unwrap();
    let peer = peer.peer_cred().unwrap();
    let gateway_pid = u32::try_from(peer.pid().unwrap()).unwrap();
    crate::db::update_runtime_fields_for_activation(
        &conn,
        sandbox_id,
        episode.id,
        crate::db::RuntimeFields {
            uid: peer.uid(),
            slot: 1,
            guest_cid: 52,
            pid: 4242,
            spaced_host_pid: Some(4243),
            netgtw_pid: gateway_pid,
            vsock_uds_path: "fixture-vsock",
            api_socket_path: "fixture-firecracker",
            space_api_route_identity: identity.clone(),
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            spaced_endpoint_proof: None,
        },
    )
    .unwrap();
    let mut service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            credential_manager_socket_path: node_socket,
            shutdown_token: shutdown.clone(),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    let _gateway = super::super::super::gateway_identity::tests::GatewayNode::attach(&mut service);
    let manager_socket = directory.path().join("manager.sock");
    let listener = tokio::net::UnixListener::bind(&manager_socket).unwrap();
    let manager_server = AbortOnDropHandle::new(tokio::spawn({
        let service = service.clone();
        let shutdown = shutdown.clone();
        async move {
            tonic::transport::Server::builder()
                .add_service(HostControlServer::new(service))
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(listener),
                    shutdown.cancelled(),
                )
                .await
                .unwrap();
        }
    }));
    let ingress = ServicePortIngressState::new(Some(identity.clone()));
    let authority = {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set(LAUNCH_CAPABILITY_ENV, "a".repeat(64));
        GatewayAuthority::from_environment(&env, manager_socket, ingress.clone(), shutdown.clone())
    };
    let network = GatewayNetworkState::pending_policy();
    let network = network.with_traffic_gate({
        let authority = authority.clone();
        Arc::new(move || authority.is_ready())
    });
    let gateway_task = AbortOnDropHandle::new(authority.spawn(network.clone()));
    let policy_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        &id,
        "a".repeat(64),
    )
    .unwrap();
    let loader: PolicyLoader = Arc::new(move || {
        let policy_ref = policy_ref.clone();
        Box::pin(async move {
            let mut policy = NetworkPolicyV1::allow_all();
            policy.tls_interception = TlsInterception::On;
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref,
                network_credentials: NetworkCredentialSnapshot::default(),
            })
        })
    });
    let control = Arc::new(NetGatewayControlService::new(
        Arc::default(),
        network.clone(),
        loader,
        GatewayDebugState::default(),
        ingress,
        shutdown.clone(),
    ));
    let network_ready = {
        let db = service.db.clone();
        let id = id.clone();
        async move {
            super::super::super::network_runtime::refresh_network_policy_with_request(&db, &id, None, "start", move |message| {
                let control = control.clone();
                async move {
                    // Stand in for the authenticated manager on this local control call.
                    let mut request = Request::new(message);
                    request.extensions_mut().insert(pplx_space_common_svc::PeerIdentity::new(0, 0, Some(std::process::id().try_into().unwrap())));
                    control.refresh_network_policy(request).await.map(Response::into_inner)
                        .map_err(super::super::super::network_runtime::NetworkPolicyRefreshAttemptError::Rpc)
                }
            }).await.map_err(network_policy_refresh_error_to_status)
        }
    };
    let (cleanup_tx, mut cleanup_rx) = tokio::sync::oneshot::channel();
    let cleanup = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        SandboxQuarantineReasonCode::Unspecified,
        "activation interrupted",
        move |_, _, _, _| {
            let _ = cleanup_tx.send(());
        },
    );
    spawn_guarded_start_state_transition(&service, &id, cleanup, {
        let db = service.db.clone();
        let id = id.clone();
        async move {
            if restore {
                mark_restore_after_network_ready(
                    &db,
                    &id,
                    SandboxState::Started,
                    Some(episode.id),
                    network_ready,
                )
                .await
            } else {
                complete_start_after_readiness(
                    db,
                    &id,
                    &BTreeMap::new(),
                    60,
                    "start",
                    Some(episode.id),
                    StartReadiness {
                        guest: std::future::ready(Ok(
                            pplx_space_spaced_host_client::proto::WaitReadyResponse::default()
                                .into(),
                        )),
                        policy: Some(network_ready),
                    },
                )
                .await
            }
        }
    })
    .unwrap();

    for _ in 0..90 {
        settle().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    settle().await;
    assert!(
        attempts.load(Ordering::SeqCst) > 0,
        "the authority must actually refuse issuance"
    );
    assert!(!authority.is_ready());
    assert_same_execution(&service, &id, &identity, gateway_pid);
    assert!(
        matches!(
            cleanup_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ),
        "successful startup must disarm cleanup"
    );
    for policy in [
        network.wait_for_tcp_policy().await,
        network.udp_policy().await,
    ] {
        assert!(matches!(
            policy,
            TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady)
        ));
    }
    let provider = CertificateProvider::new(
        authority.clone(),
        CertificateContext::new(network.clone(), shutdown.clone()),
    );
    provider
        .get("recovery.example".parse().unwrap(), 443)
        .await
        .unwrap_err();
    let refused_attempts = attempts.load(Ordering::SeqCst);
    // Checked during the refusal, not after the repair: a worker that gives up
    // on a refusing authority is the regression, and by the time the authority
    // is ready again this can no longer discriminate.
    assert!(
        !gateway_task.is_finished(),
        "the acquisition worker must outlive a refusing authority"
    );
    configured.store(true, Ordering::SeqCst);
    for _ in 0..900 {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        if authority.is_ready() {
            break;
        }
    }
    assert!(
        authority.is_ready(),
        "the same gateway must recover after authority repair"
    );
    assert!(
        attempts.load(Ordering::SeqCst) > refused_attempts,
        "repair must trigger a fresh authority request"
    );
    assert_same_execution(&service, &id, &identity, gateway_pid);
    assert!(matches!(
        network.wait_for_tcp_policy().await,
        TrafficPolicy::Ready(_)
    ));
    let certificate = provider
        .get("recovery.example".parse().unwrap(), 443)
        .await
        .unwrap()
        .certified_key()
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root_cert.der().clone()).unwrap();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .unwrap();
    use rustls::client::danger::ServerCertVerifier;
    verifier
        .verify_server_cert(
            &certificate.cert[0],
            &certificate.cert[1..],
            &"recovery.example".try_into().unwrap(),
            &[],
            rustls::pki_types::UnixTime::now(),
        )
        .unwrap();

    shutdown.cancel();
    gateway_task.await.unwrap();
    manager_server.await.unwrap();
    node_server.await.unwrap();
    authority_server.await.unwrap();
}

fn assert_same_execution(
    service: &SandboxManagerService,
    id: &str,
    identity: &SpaceApiRouteIdentity,
    gateway_pid: u32,
) {
    let db = service.db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(&row.space_api_route_identity, identity);
    assert_eq!(row.netgtw_pid, Some(gateway_pid));
    assert_eq!(row.pid, Some(4242));
    assert!(crate::db::cleanup_episode(&db, id).unwrap().is_none());
}
