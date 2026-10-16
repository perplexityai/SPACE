use super::super::tests::service_tests::insert_test_sandbox;
use super::*;
use pplx_space_common_core::types::RouteGeneration;

struct Fixture {
    _gateway: super::super::gateway_identity::tests::GatewayNode,
    service: SandboxManagerService,
    sandbox_id: SandboxId,
    peer: tokio::net::unix::UCred,
    request: IssueGatewayIntermediateCertificateRequest,
}

impl Fixture {
    fn new() -> Self {
        Self::with_options(SandboxManagerServiceOptions::for_test())
    }

    fn with_options(options: SandboxManagerServiceOptions) -> Self {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
        let (peer, _other) = tokio::net::UnixStream::pair().unwrap();
        let cred = peer.peer_cred().unwrap();
        crate::db::update_netgtw_pid(
            &conn,
            &sandbox_id.to_string(),
            cred.uid(),
            cred.pid().unwrap().try_into().unwrap(),
        )
        .unwrap();
        crate::db::update_space_api_route_identity(
            &conn,
            sandbox_id,
            &SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
                route_generation: Some(RouteGeneration::new(1).unwrap()),
                ..Default::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        let mut service = SandboxManagerService::new_with_options(conn, options);
        let gateway = super::super::gateway_identity::tests::GatewayNode::attach(&mut service);
        Self {
            _gateway: gateway,
            service,
            sandbox_id,
            peer: cred,
            request: IssueGatewayIntermediateCertificateRequest {
                csr: vec![1, 2, 3],
                launch_capability: vec![b'a'; 64],
                execution_generation: 1,
            },
        }
    }

    fn request(&self) -> Request<IssueGatewayIntermediateCertificateRequest> {
        let mut request = Request::new(self.request.clone());
        request
            .extensions_mut()
            .insert(tonic::transport::server::UdsConnectInfo {
                peer_addr: None,
                peer_cred: Some(self.peer),
            });
        request
    }

    fn issuer(&mut self, issuer: super::Issuer) -> Arc<Semaphore> {
        let capacity = Arc::new(Semaphore::new(MAX_ISSUANCES));
        self.service.gateway_authority = GatewayAuthority {
            issuer,
            capacity: capacity.clone(),
        };
        capacity
    }

    fn sandbox(&self) -> crate::db::SandboxRow {
        crate::db::get_sandbox(
            &self.service.db.lock().unwrap(),
            &self.sandbox_id.to_string(),
        )
        .unwrap()
        .unwrap()
    }

    fn set_state(&self, state: SandboxState) {
        self.service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
                crate::db::fixture::params![state.as_str(), self.sandbox_id.to_string()],
            )
            .unwrap();
    }

    fn write_route_identity(&self, identity: &SpaceApiRouteIdentity) {
        self.service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes
                 SET space_api_route_generation = ?1,
                     space_api_route_node_id = ?2,
                     space_api_route_execution_generation = ?3,
                     space_api_route_pod_uid = ?4,
                     space_api_route_substrate_kind = ?5,
                     space_api_route_activation_attempt_id = ?6,
                     space_api_route_identity_provenance = 'unfenced'
                 WHERE id = ?7",
                crate::db::fixture::params![
                    identity
                        .route_generation
                        .and_then(|generation| i64::try_from(generation.get()).ok()),
                    identity.current_node_id.map(|node_id| node_id.to_string()),
                    identity
                        .execution_generation
                        .map(|generation| generation.get()),
                    identity.pod_uid.as_ref().map(|pod_uid| pod_uid.as_str()),
                    identity.substrate_kind.map(|kind| kind.to_string()),
                    identity
                        .activation_attempt_id
                        .map(|attempt| attempt.to_string()),
                    self.sandbox_id.to_string(),
                ],
            )
            .unwrap();
    }

    async fn issue(&self) -> Result<IssueGatewayIntermediateCertificateResponse, Status> {
        HostControl::issue_gateway_intermediate_certificate(&self.service, self.request())
            .await
            .map(Response::into_inner)
    }
}

#[tokio::test]
async fn host_control_authentication_includes_unassigned_pool_gateways() {
    let fixture = Fixture::new();
    for pool in [false, true] {
        if pool {
            let db = fixture.service.db.lock().unwrap();
            crate::db::set_pool_template(&db, &fixture.sandbox_id.to_string(), Some("warm-pool"))
                .unwrap();
            crate::db::update_space_api_route_identity(
                &db,
                fixture.sandbox_id,
                &SpaceApiRouteIdentity::default(),
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
            )
            .unwrap();
        }
        let sandbox = fixture
            .service
            .authenticated_net_gateway_host_control_sandbox(&fixture.request())
            .unwrap();
        assert_eq!(sandbox.id, fixture.sandbox_id.to_string());
    }
    assert_eq!(
        fixture
            .service
            .authenticated_net_gateway_host_control_sandbox(&Request::new(()))
            .map(|_| ())
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    crate::db::update_netgtw_pid(
        &fixture.service.db.lock().unwrap(),
        &fixture.sandbox_id.to_string(),
        65534,
        999999,
    )
    .unwrap();
    assert_eq!(
        fixture
            .service
            .authenticated_net_gateway_host_control_sandbox(&fixture.request())
            .map(|_| ())
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

// Credential-manager and gateway tests own certificate signing and validation.
// HostControl must forward these bytes unchanged.
fn issued_response() -> NodeResponse {
    NodeResponse {
        certificate_der: vec![4, 5],
        issuer_certificate_der: vec![6, 7],
    }
}

#[tokio::test]
async fn gateway_authority_uses_the_configured_node_socket() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("custom-credmgr.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let fixture = Fixture::with_options(SandboxManagerServiceOptions {
        credential_manager_socket_path: socket,
        ..SandboxManagerServiceOptions::for_test()
    });
    let expected = fixture.request.csr.clone();
    let issue = tower::service_fn(move |request: Request<NodeRequest>| {
        assert_eq!(request.get_ref().csr, expected);
        async move { Ok::<_, Status>(Response::new(issued_response())) }
    });
    let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        assert_eq!(
            request.uri().path(),
            "/space.credential.manager.v1.CredentialManager/IssueIntermediateCertificate"
        );
        let issue = issue.clone();
        async move {
            Ok::<_, std::convert::Infallible>(
                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(issue, request)
                    .await,
            )
        }
    });
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server_connections = connections.clone();
    use futures_util::StreamExt as _;
    let server = tonic::transport::Server::builder().serve_with_incoming(
        rpc,
        tokio_stream::wrappers::UnixListenerStream::new(listener).inspect(move |_| {
            server_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }),
    );
    let exercise = async {
        fixture
            .service
            .credential_manager_client
            .get_or_try_init(|| fixture.service.credential_manager_endpoint.connect())
            .await
            .unwrap();
        let response = fixture
            .issue()
            .await
            .expect("CA issuance must use the configured socket");
        assert_eq!(response.certificate_der, issued_response().certificate_der);
        assert_eq!(
            response.issuer_certificate_der,
            issued_response().issuer_certificate_der
        );
        assert_eq!(
            connections.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "CA requests reuse the manager's existing credmgr channel"
        );
    };
    tokio::select! {
        result = server => panic!("fixture server ended: {result:?}"),
        result = tokio::time::timeout(std::time::Duration::from_secs(5), exercise) => result.unwrap(),
    }
}

// Peer registration is covered by the HostControl lookup tests.
#[tokio::test]
async fn gateway_authority_rejects_invalid_requests_before_issuer() {
    let mut fixture = Fixture::new();
    fixture.issuer(Arc::new(|_| panic!("invalid request reached issuer")));
    for csr in [
        Vec::new(),
        vec![0; pplx_space_credmgr_client::MAX_INTERMEDIATE_CSR_BYTES + 1],
    ] {
        let request = fixture
            .request()
            .map(|_| IssueGatewayIntermediateCertificateRequest {
                csr,
                ..Default::default()
            });
        assert_eq!(
            HostControl::issue_gateway_intermediate_certificate(&fixture.service, request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[tokio::test]
async fn gateway_authority_retries_incomplete_identity_during_activation_transitions() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for state in [
        SandboxState::Starting,
        SandboxState::Restoring,
        SandboxState::Resuming,
    ] {
        let mut fixture = Fixture::new();
        let calls = calls.clone();
        fixture.issuer(Arc::new(move |_| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(issued_response()) })
        }));
        let mut identity = fixture.sandbox().space_api_route_identity;
        identity.route_generation = None;
        fixture.write_route_identity(&identity);
        fixture.set_state(state);

        assert_eq!(
            fixture.issue().await.unwrap_err().code(),
            tonic::Code::Unavailable,
            "{state:?}"
        );
    }
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "incomplete activation identity must not reach credential-manager"
    );
}

#[tokio::test]
async fn gateway_authority_retries_until_identity_and_launch_evidence_publish() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut fixture = Fixture::new();
    let calls_for_issuer = calls.clone();
    let expected_csr = fixture.request.csr.clone();
    fixture.issuer(Arc::new(move |request| {
        assert_eq!(request.csr, expected_csr);
        calls_for_issuer.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(issued_response()) })
    }));
    let mut identity = fixture.sandbox().space_api_route_identity;
    identity.route_generation = None;
    fixture.write_route_identity(&identity);
    fixture.set_state(SandboxState::Restoring);

    assert_eq!(
        fixture.issue().await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    identity.route_generation = Some(RouteGeneration::new(2).unwrap());
    fixture.write_route_identity(&identity);
    // A recycled PID can satisfy the DB registration before the supervisor
    // finishes publishing evidence. Preserve retry ownership without issuing.
    fixture
        ._gateway
        .authorized
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        fixture.issue().await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    fixture
        ._gateway
        .authorized
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let response = fixture
        .issue()
        .await
        .expect("completed restore identity should allow issuance");

    assert_eq!(response.certificate_der, issued_response().certificate_der);
    assert_eq!(
        response.issuer_certificate_der,
        issued_response().issuer_certificate_der
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gateway_authority_keeps_invalid_incomplete_identity_terminal() {
    for case in [
        "pool",
        "orphaned",
        "stopping",
        "started",
        "sandbox_id_mismatch",
    ] {
        let mut fixture = Fixture::new();
        fixture.issuer(Arc::new(|_| panic!("invalid context reached issuer")));
        let mut identity = fixture.sandbox().space_api_route_identity;
        identity.route_generation = None;
        fixture.write_route_identity(&identity);
        match case {
            "pool" => crate::db::set_pool_template(
                &fixture.service.db.lock().unwrap(),
                &fixture.sandbox_id.to_string(),
                Some("warm-pool"),
            )
            .unwrap(),
            "orphaned" => crate::db::update_user(
                &fixture.service.db.lock().unwrap(),
                &fixture.sandbox_id.to_string(),
                &ORPHANED_SANDBOX_OWNER,
            )
            .unwrap(),
            "stopping" => fixture.set_state(SandboxState::Stopping),
            "started" => {}
            "sandbox_id_mismatch" => {
                let mut sandbox = fixture.sandbox();
                sandbox.space_api_route_identity.sandbox_id = Some(SandboxId::new());
                assert_eq!(
                    validate_gateway_authority_sandbox(&sandbox, false)
                        .unwrap_err()
                        .code(),
                    tonic::Code::FailedPrecondition,
                    "{case}"
                );
                continue;
            }
            _ => unreachable!(),
        }

        assert_eq!(
            fixture.issue().await.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{case}"
        );
    }
}

#[tokio::test]
async fn gateway_authority_rechecks_ownership_after_await_but_not_ordinary_policy() {
    for change in [
        "execution",
        "route",
        "owner",
        "gateway",
        "launch_incarnation",
        "pool",
        "stopping",
        "cleanup",
        "policy",
    ] {
        let mut fixture = Fixture::new();
        let db = fixture.service.db.clone();
        let id = fixture.sandbox_id;
        let launch_authorized = fixture._gateway.authorized.clone();
        fixture.issuer(Arc::new(move |_| {
            let db = db.clone();
            let launch_authorized = launch_authorized.clone();
            Box::pin(async move {
                let issued = issued_response();
                let db = db.lock().unwrap();
                match change {
                    "launch_incarnation" => launch_authorized.store(false, std::sync::atomic::Ordering::SeqCst),
                    "execution" | "route" => {
                        let mut identity = crate::db::get_sandbox(&db, &id.to_string()).unwrap().unwrap().space_api_route_identity;
                        if change == "execution" { identity.execution_generation = Some(ExecutionGeneration::new(2).unwrap()); }
                        else { identity.route_generation = Some(RouteGeneration::new(2).unwrap()); }
                        crate::db::update_space_api_route_identity(&db, id, &identity, crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced).unwrap();
                    }
                    "owner" => { crate::db::update_user(&db, &id.to_string(), &"another@example.com".parse().unwrap()).unwrap(); }
                    "gateway" => {
                        let sandbox = crate::db::get_sandbox(&db, &id.to_string()).unwrap().unwrap();
                        crate::db::update_netgtw_pid(&db, &id.to_string(), sandbox.uid.unwrap(), sandbox.netgtw_pid.unwrap() + 1).unwrap();
                    }
                    "pool" => { crate::db::set_pool_template(&db, &id.to_string(), Some("warm-pool")).unwrap(); }
                    "stopping" => crate::db::test_fixtures::state_write_fixtures::apply_edge(&db, &id.to_string(), &crate::db::edges::STOP_ADMISSION),
                    "cleanup" => { crate::db::mark_cleanup_required(&db, &id.to_string(), true, crate::db::CleanupCompletion::MarkSuspended, "restore failed").unwrap(); }
                    "policy" => { crate::db::update_network_policy_override(&db, &id.to_string(), Some(&NetworkPolicyV1::deny_all()), 2, crate::db::PolicyAuthority::Cluster, false, false).unwrap(); }
                    _ => unreachable!(),
                }
                Ok(issued)
            })
        }));
        let result = fixture.issue().await;
        let expected = match change {
            "policy" => None,
            "pool" | "stopping" => Some(tonic::Code::FailedPrecondition),
            "gateway" | "cleanup" => Some(tonic::Code::PermissionDenied),
            "launch_incarnation" => Some(tonic::Code::Unavailable),
            _ => Some(tonic::Code::Aborted),
        };
        assert_eq!(result.err().map(|e| e.code()), expected, "{change}");
    }
}

#[tokio::test]
async fn gateway_authority_bounds_wait_and_releases_capacity_on_timeout_or_cancellation() {
    let mut fixture = Fixture::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let capacity = fixture.issuer(Arc::new({
        let entered = entered.clone();
        move |_| {
            entered.notify_one();
            Box::pin(std::future::pending())
        }
    }));
    let all = capacity.acquire_many(MAX_ISSUANCES as u32).await.unwrap();
    assert_eq!(
        fixture.issue().await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    drop(all);
    let mut pending = Box::pin(fixture.issue());
    tokio::select! {
        () = entered.notified() => {},
        result = &mut pending => panic!("issuer must remain pending: {result:?}"),
    }
    tokio::time::pause();
    tokio::time::advance(ISSUANCE_TIMEOUT).await;
    let error = pending.await.unwrap_err();
    tokio::time::resume();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("same key"));
    assert_eq!(capacity.available_permits(), MAX_ISSUANCES);
    {
        let mut pending = Box::pin(fixture.issue());
        tokio::select! {
            () = entered.notified() => {},
            result = &mut pending => panic!("issuer must remain pending: {result:?}"),
        }
        assert_eq!(capacity.available_permits(), MAX_ISSUANCES - 1);
    }
    assert_eq!(capacity.available_permits(), MAX_ISSUANCES);
    let mut pending = Box::pin(fixture.issue());
    tokio::select! {
        () = entered.notified() => {},
        result = &mut pending => panic!("issuer must remain pending: {result:?}"),
    }
    fixture.service.shutdown_token.cancel();
    assert_eq!(pending.await.unwrap_err().code(), tonic::Code::Unavailable);
    assert_eq!(capacity.available_permits(), MAX_ISSUANCES);
}

#[tokio::test]
async fn gateway_authority_bounds_issuer_response() {
    for (size, node_size) in [
        (0, 1),
        (MAX_CERTIFICATE_BYTES + 1, 1),
        (1, 0),
        (1, MAX_CERTIFICATE_BYTES + 1),
    ] {
        let mut fixture = Fixture::new();
        fixture.issuer(Arc::new(move |_| {
            Box::pin(async move {
                Ok(NodeResponse {
                    certificate_der: vec![0; size],
                    issuer_certificate_der: vec![0; node_size],
                })
            })
        }));
        assert_eq!(
            fixture.issue().await.unwrap_err().code(),
            tonic::Code::Internal
        );
    }
    let mut fixture = Fixture::new();
    fixture.issuer(Arc::new(|_| {
        Box::pin(async { Err(Status::unavailable("node CA renewal unavailable")) })
    }));
    let error = fixture.issue().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn gateway_authority_deadline_covers_database_contention_before_and_after_issuance() {
    for after_issuance in [false, true] {
        let mut fixture = Fixture::new();
        // Isolate this RPC from unrelated health samplers that also use the DB.
        fixture.service.shutdown_token.cancel();
        fixture.service.task_tracker.close();
        fixture.service.task_tracker.wait().await;
        fixture.service.shutdown_token = CancellationToken::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let capacity = fixture.issuer(Arc::new({
            let entered = entered.clone();
            let resume = resume.clone();
            let calls = calls.clone();
            move |_| {
                let entered = entered.clone();
                let resume = resume.clone();
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    entered.notify_one();
                    if after_issuance {
                        resume.notified().await;
                    }
                    Ok(issued_response())
                })
            }
        }));
        let mut request = fixture.request();
        request.set_timeout(Duration::from_millis(200));
        let call = fixture.service.issue_gateway_authority(request);
        tokio::pin!(call);
        if after_issuance {
            tokio::select! {
                () = entered.notified() => {},
                result = &mut call => panic!("issuer must remain pending: {result:?}"),
            }
        }
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let db = fixture.service.db.clone();
        let holder = tokio::task::spawn_blocking(move || {
            let _guard = db.lock().unwrap();
            locked_tx.send(()).unwrap();
            // Bound a broken synchronous lookup without allowing it to pass.
            release_rx.recv_timeout(Duration::from_secs(2))
        });
        locked_rx.await.unwrap();
        resume.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(1), &mut call).await;
        assert_eq!(
            capacity.available_permits(),
            MAX_ISSUANCES - 1,
            "timing out a waiter must not release a still-blocked worker's slot"
        );
        drop(release_tx);
        assert!(
            matches!(
                holder.await.unwrap(),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
            ),
            "authorization blocked the runtime until the OS watchdog released the DB"
        );
        assert_eq!(result.unwrap().unwrap_err().code(), tonic::Code::Cancelled);
        tokio::time::timeout(Duration::from_secs(1), async {
            while capacity.available_permits() != MAX_ISSUANCES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(after_issuance)
        );
    }
}
