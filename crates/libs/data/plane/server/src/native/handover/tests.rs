use super::*;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;
use pplx_space_common_core::types::{
    ClusterId, ExecutionGeneration, NodeManagedBy, RouteGeneration, SandboxActivationAttemptId,
    SandboxRouteIdentity,
};
use pplx_space_common_svc::space_api_route::{
    RouteIdentityRemediation, SpaceApiRouteIdentityDimension, set_route_identity_rejection_metadata,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, SpaceApiRouteEd25519Authority,
    SpaceApiRouteEd25519Issuer, SpaceApiRouteEd25519KeyId, SpaceApiRouteEd25519Signer,
};
use pplx_space_node_api_client::proto::DataPlaneBackendEndpoint;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

fn authority() -> SpaceApiRouteEd25519Authority {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Authority::new(
        SpaceApiRouteEd25519Issuer::parse("space-api").unwrap(),
        ClusterId::parse("testing").unwrap(),
        SpaceApiRouteEd25519Signer::from_pkcs8(
            SpaceApiRouteEd25519KeyId::parse("test-key").unwrap(),
            pkcs8.as_ref(),
        )
        .unwrap(),
    )
}

fn verifier(authority: &SpaceApiRouteEd25519Authority) -> Arc<SpaceApiRouteEd25519Verifier> {
    Arc::new(
        SpaceApiRouteEd25519Verifier::new(
            authority.issuer().clone(),
            authority.cluster_id().clone(),
            [authority.verification_key()],
        )
        .unwrap(),
    )
}

fn route_token(
    authority: &SpaceApiRouteEd25519Authority,
    sandbox_id: SandboxId,
    node_id: NodeId,
    execution_generation: i64,
) -> String {
    authority
        .mint_space_api_bridge(
            SandboxRouteIdentity::try_new(
                sandbox_id,
                RouteGeneration::new(execution_generation.cast_unsigned()).unwrap(),
                node_id,
                NodeManagedBy::Space,
                ExecutionGeneration::new(execution_generation).unwrap(),
                SandboxActivationAttemptId::new(),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .secret()
        .to_string()
}

fn attach_token() -> String {
    format!("{DATA_PLANE_TOKEN_PREFIX}{}", "d".repeat(64))
}

/// A fake cluster API and worker for one process session. The worker records
/// the route token on every start, and refuses the handed-over one when
/// `refuse` holds it, as a worker refuses a route that a restore or migration
/// replaced.
struct Fake {
    endpoint: String,
    sandbox_id: SandboxId,
    node_id: NodeId,
    resolved_route_token: String,
    resolves: Arc<AtomicUsize>,
    started_with: Arc<Mutex<Vec<String>>>,
    refuse: Arc<Mutex<Option<String>>>,
}

async fn spawn_fake(
    authority: &SpaceApiRouteEd25519Authority,
    session_id: DataPlaneSessionId,
) -> Fake {
    use prost::Message as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let resolved_route_token = route_token(authority, sandbox_id, node_id, 6);
    let mut route = crate::tests::route_response_with_token(
        session_id,
        Some(ProtoSpaceApiRouteToken {
            token: resolved_route_token.clone(),
        }),
    );
    route.sandbox_id = sandbox_id.to_string();
    route.current_node_id = node_id.to_string();
    route.backend = Some(DataPlaneBackendEndpoint {
        node_id: node_id.to_string(),
        endpoint: endpoint.clone(),
        transport: None,
    });
    route.adapter_state_json = r#"{"command":["true"]}"#.to_string();
    let route = route.encode_to_vec();
    let process_id = pplx_space_public_api::process_id_for_session(session_id).to_string();
    let mut started = vec![0];
    let encoded = ProcessEvent {
        process_id,
        pid: Some(42),
        event: Some(spaced_process_event::Event::Started(
            pplx_space_sbx_mgr_client::operations::ProcessStarted { start_time: None },
        )),
    }
    .encode_to_vec();
    started.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    started.extend(encoded);
    let started = bytes::Bytes::from(started);

    let fake = Fake {
        endpoint,
        sandbox_id,
        node_id,
        resolved_route_token,
        resolves: Arc::new(AtomicUsize::new(0)),
        started_with: Arc::new(Mutex::new(Vec::new())),
        refuse: Arc::new(Mutex::new(None)),
    };
    let resolves = Arc::clone(&fake.resolves);
    let started_with = Arc::clone(&fake.started_with);
    let refuse = Arc::clone(&fake.refuse);
    let service = axum::Router::new().fallback(move |request: Request<Body>| {
        let route = route.clone();
        let started = started.clone();
        let resolves = Arc::clone(&resolves);
        let started_with = Arc::clone(&started_with);
        let refuse = Arc::clone(&refuse);
        async move {
            let path = request.uri().path().to_string();
            if path.ends_with("/Check") {
                return crate::tests::grpc_test_response(vec![8, 1]);
            }
            if path.ends_with("/ResolveDataPlaneSessionRoute") {
                resolves.fetch_add(1, Ordering::SeqCst);
                return crate::tests::grpc_test_response(route);
            }
            if path.ends_with("/BindDataPlaneSessionProcess") {
                return crate::tests::grpc_test_response(Vec::new());
            }
            assert!(
                path.ends_with("/SpacedStartProcess"),
                "unexpected call {path}"
            );
            let token = request
                .headers()
                .get(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            started_with.lock().unwrap().push(token.clone());
            if refuse.lock().unwrap().as_deref() == Some(token.as_str()) {
                let mut status = tonic::Status::permission_denied("stale execution generation");
                set_route_identity_rejection_metadata(
                    &mut status,
                    RouteIdentityRemediation::RefreshRoute,
                    Some(SpaceApiRouteIdentityDimension::ExecutionGeneration),
                );
                return status.into_http::<Body>();
            }
            let frames = futures_util::stream::iter([Ok::<_, Infallible>(
                hyper::body::Frame::data(started),
            )])
            .chain(futures_util::stream::pending());
            Response::builder()
                .header("content-type", "application/grpc")
                .body(Body::new(http_body_util::StreamBody::new(frames)))
                .unwrap()
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    fake
}

/// The handover the API mints for this session's fresh route.
fn handover_route(
    authority: &SpaceApiRouteEd25519Authority,
    fake: &Fake,
    session_id: DataPlaneSessionId,
) -> DataPlaneRouteHandoverRoute {
    DataPlaneRouteHandoverRoute {
        session_id: session_id.to_string(),
        token_sha256: token_sha256(&attach_token()),
        org_id: OrgId::new(),
        sandbox_id: fake.sandbox_id,
        kind: session_id.kind().as_str().to_string(),
        route_generation: 2,
        current_node_id: fake.node_id,
        backend_endpoint: fake.endpoint.clone(),
        execution_generation: ExecutionGeneration::new(5).unwrap(),
        revocation_epoch: 0,
        org_revocation_epoch: 0,
        adapter_state_json: r#"{"command":["true"]}"#.to_string(),
        space_api_route_token: route_token(authority, fake.sandbox_id, fake.node_id, 5),
    }
}

fn test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("test-token")),
        ),
    ])
}

enum Listener {
    Internal,
    Public,
}

/// Opens the session's event stream with `handover` and waits for the start.
async fn open_with_handover(
    fake: &Fake,
    verifier: Arc<SpaceApiRouteEd25519Verifier>,
    session_id: DataPlaneSessionId,
    handover: Option<String>,
    listener: Listener,
) {
    let mut state = crate::tests::test_data_plane_state().await;
    state.cluster_api = ClusterApiClients::service_only(
        pplx_space_node_api_client::cluster::connect(fake.endpoint.clone(), "test")
            .await
            .unwrap(),
    );
    state.revocation_check_interval = Duration::from_secs(3600);
    state.route_handover = Some(verifier);
    let app = match listener {
        Listener::Internal => router(state).layer(Extension(InternalListener)),
        Listener::Public => router(state),
    };
    let mut request = Request::get(format!("/v1/sessions/{session_id}/events"))
        .header(header::AUTHORIZATION, format!("Bearer {}", attach_token()));
    if let Some(handover) = handover {
        request = request.header(DATA_PLANE_ROUTE_HANDOVER_HEADER, handover);
    }
    let response = tower::ServiceExt::oneshot(app, request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let mut seen = String::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !seen.contains("process_started") {
            let frame = body.frame().await.expect("stream open").unwrap();
            if let Ok(data) = frame.into_data() {
                seen.push_str(&String::from_utf8_lossy(&data));
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("process_started within 5s; saw {seen}"));
}

#[tokio::test]
async fn a_valid_handover_opens_the_stream_without_resolving() {
    if !test_process() {
        return;
    }
    let authority = authority();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_fake(&authority, session_id).await;
    let route = handover_route(&authority, &fake, session_id);
    let handed_token = route.space_api_route_token.clone();
    let handover = authority.mint_data_plane_route_handover(route).unwrap();

    open_with_handover(
        &fake,
        verifier(&authority),
        session_id,
        Some(handover),
        Listener::Internal,
    )
    .await;

    assert_eq!(
        fake.resolves.load(Ordering::SeqCst),
        0,
        "no route resolution"
    );
    assert_eq!(
        *fake.started_with.lock().unwrap(),
        vec![handed_token],
        "the process started on the handed-over route"
    );
}

#[tokio::test]
async fn refused_handovers_fall_back_to_resolving_the_route() {
    if !test_process() {
        return;
    }
    let authority = authority();
    let impostor = authority_with_other_key();
    for (case, listener, handover) in [
        ("public listener", Listener::Public, Handover::Valid),
        ("forged", Listener::Internal, Handover::SignedBy(&impostor)),
        ("expired", Listener::Internal, Handover::Expired),
        ("other session", Listener::Internal, Handover::OtherSession),
        (
            "other attach token",
            Listener::Internal,
            Handover::OtherToken,
        ),
        (
            "mismatched route token",
            Listener::Internal,
            Handover::OtherExecution,
        ),
        ("garbage", Listener::Internal, Handover::Garbage),
    ] {
        let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
        let fake = spawn_fake(&authority, session_id).await;
        let handover = handover.mint(&authority, &fake, session_id);

        open_with_handover(
            &fake,
            verifier(&authority),
            session_id,
            Some(handover),
            listener,
        )
        .await;

        assert_eq!(
            fake.resolves.load(Ordering::SeqCst),
            1,
            "{case}: the route was resolved"
        );
        assert_eq!(
            *fake.started_with.lock().unwrap(),
            vec![fake.resolved_route_token.clone()],
            "{case}: the process started on the resolved route"
        );
    }
}

#[tokio::test]
async fn a_route_that_went_stale_after_its_handover_is_refreshed() {
    if !test_process() {
        return;
    }
    let authority = authority();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_fake(&authority, session_id).await;
    let route = handover_route(&authority, &fake, session_id);
    let handed_token = route.space_api_route_token.clone();
    // A restore replaced the execution the handover names.
    *fake.refuse.lock().unwrap() = Some(handed_token.clone());
    let handover = authority.mint_data_plane_route_handover(route).unwrap();

    open_with_handover(
        &fake,
        verifier(&authority),
        session_id,
        Some(handover),
        Listener::Internal,
    )
    .await;

    assert_eq!(fake.resolves.load(Ordering::SeqCst), 1, "resolved once");
    assert_eq!(
        *fake.started_with.lock().unwrap(),
        vec![handed_token, fake.resolved_route_token.clone()],
        "the refused start was retried on the resolved route"
    );
}

#[tokio::test]
async fn handovers_are_ignored_where_none_are_verified() {
    if !test_process() {
        return;
    }
    let authority = authority();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let fake = spawn_fake(&authority, session_id).await;
    let handover = authority
        .mint_data_plane_route_handover(handover_route(&authority, &fake, session_id))
        .unwrap();
    let state = crate::tests::test_data_plane_state().await;
    let offered = OfferedRouteHandover {
        handover,
        internal_listener: true,
    };
    assert_eq!(
        accept_route_handover(&state, session_id, &attach_token(), &offered).unwrap_err(),
        "not_configured"
    );
}

fn authority_with_other_key() -> SpaceApiRouteEd25519Authority {
    authority()
}

enum Handover<'a> {
    Valid,
    SignedBy(&'a SpaceApiRouteEd25519Authority),
    Expired,
    OtherSession,
    OtherToken,
    OtherExecution,
    Garbage,
}

impl Handover<'_> {
    fn mint(
        &self,
        authority: &SpaceApiRouteEd25519Authority,
        fake: &Fake,
        session_id: DataPlaneSessionId,
    ) -> String {
        let mut route = handover_route(authority, fake, session_id);
        match self {
            Self::Valid => {}
            Self::SignedBy(other) => return other.mint_data_plane_route_handover(route).unwrap(),
            Self::Expired => return expired_handover(authority, route),
            Self::OtherSession => {
                route.session_id =
                    DataPlaneSessionId::new_for_kind(SessionKind::Process).to_string();
            }
            Self::OtherToken => {
                route.token_sha256 =
                    token_sha256(&format!("{DATA_PLANE_TOKEN_PREFIX}{}", "e".repeat(64)));
            }
            Self::OtherExecution => {
                route.space_api_route_token =
                    route_token(authority, fake.sandbox_id, fake.node_id, 4);
            }
            Self::Garbage => return "dp-handover-ed25519.test-key.e30.AAAA".to_string(),
        }
        authority.mint_data_plane_route_handover(route).unwrap()
    }
}

fn expired_handover(
    authority: &SpaceApiRouteEd25519Authority,
    route: DataPlaneRouteHandoverRoute,
) -> String {
    use pplx_space_common_svc::space_api_route_ed25519::{
        DATA_PLANE_ROUTE_HANDOVER_AUDIENCE, DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION,
        DataPlaneRouteHandoverClaims,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    authority
        .sign_data_plane_route_handover_claims(&DataPlaneRouteHandoverClaims {
            schema_version: DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION,
            issuer: authority.issuer().clone(),
            audience: DATA_PLANE_ROUTE_HANDOVER_AUDIENCE.to_string(),
            cluster_id: authority.cluster_id().clone(),
            route,
            issued_at_unix_secs: now - 60,
            expires_at_unix_secs: now - 50,
        })
        .unwrap()
}
