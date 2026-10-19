use super::*;
use crate::command_stream::CommandStreamProxy;
use crate::routes::AppState;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Method, Request, StatusCode, header},
    response::Response,
    routing::get,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    DATA_PLANE_ROUTE_HANDOVER_HEADER, SPACE_API_ROUTE_ED25519_ISSUER_ENV,
    SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
    SpaceApiRouteEd25519Token, SpaceApiRouteEd25519Verifier,
    decode_unverified_space_api_route_ed25519_claims,
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use pplx_space_internal_api_server::ClusterApiService;
use pplx_space_internal_api_server::proto::{
    ResolveDataPlaneSessionRouteRequest, cluster_api_server::ClusterApi,
};
use pplx_space_public_api::{DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE, PublicSandboxId};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

const KEY: &str = "route-handover-test-key";

fn route_signing_test_process() -> bool {
    let key_path = pplx_space_common_svc::space_api_route_ed25519::test_signing_key_path()
        .to_str()
        .unwrap()
        .to_string();
    pplx_space_test_env::enter_test_process(&[
        ("SPACE_CLUSTER_ID", Some(std::ffi::OsStr::new("testing"))),
        (
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
            Some(std::ffi::OsStr::new(&key_path)),
        ),
        (
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
            Some(std::ffi::OsStr::new("handover-test-key")),
        ),
        (
            SPACE_API_ROUTE_ED25519_ISSUER_ENV,
            Some(std::ffi::OsStr::new("space-api")),
        ),
    ])
}

/// A data plane that records the handover each events request carries.
async fn spawn_data_plane() -> (String, Arc<Mutex<Vec<Option<String>>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            DATA_PLANE_SESSION_EVENTS_PATH_TEMPLATE,
            get(
                |State(seen): State<Arc<Mutex<Vec<Option<String>>>>>, headers: HeaderMap| async move {
                    seen.lock().unwrap().push(
                        headers
                            .get(DATA_PLANE_ROUTE_HANDOVER_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                    );
                    Response::builder()
                        .header(header::CONTENT_TYPE, "text/event-stream")
                        .body(Body::from("event: attached\ndata: {}\n\n"))
                        .unwrap()
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, seen)
}

struct Placed {
    sandbox: PublicSandboxId,
    node_id: NodeId,
}

/// A started sandbox on a ready node with a running execution.
async fn place_sandbox(pool: &PgPool, address: &str, generation: i64) -> TestDbResult<Placed> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .bind(address)
    .execute(pool)
    .await?;
    let sandbox = PublicSandboxId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_override_json,
             network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', 'started', 'placed', $4, $4, $5, 1)"
    ))
    .bind(sandbox.as_uuid())
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             id, sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             pod_ip, route_activation_id, state, active
         )
         VALUES ($1, $2, $3, $4, 'space', $5, '10.80.0.9'::INET, $6, 'running', true)"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox.as_uuid())
    .bind(generation)
    .bind(format!(
        "sandbox-pod-{}-{generation}",
        sandbox.as_uuid().simple()
    ))
    .bind(format!("pod-uid-{generation}"))
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(Placed { sandbox, node_id })
}

async fn post_streamed_command(app: &Router, sandbox: PublicSandboxId) -> TestDbResult<Value> {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/v1/sandboxes/{sandbox}/commands"))
                .header(header::AUTHORIZATION, format!("Bearer {KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "text/event-stream")
                .header("idempotency-key", Uuid::now_v7().to_string())
                .body(Body::from(serde_json::to_vec(
                    &serde_json::json!({"command": ["echo", "hello"]}),
                )?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    let body = String::from_utf8(body.to_vec())?;
    let (first, _) = body.split_once("\n\n").expect("command event");
    let data = first
        .strip_prefix("event: command\ndata: ")
        .expect("command event first");
    Ok(serde_json::from_str::<Value>(data)?["command"].clone())
}

use serde_json::Value;

#[tokio::test]
async fn the_handover_carries_the_route_resolution_would_return() -> TestDbResult<()> {
    if !route_signing_test_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'route-handover-test', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&pool)
    .await?;
    let authority = Arc::new(SpaceApiRouteEd25519Authority::load_required_from_env()?);
    let verifier = SpaceApiRouteEd25519Verifier::new(
        authority.issuer().clone(),
        authority.cluster_id().clone(),
        [authority.verification_key()],
    )?;
    let (origin, seen) = spawn_data_plane().await;
    let mut state = AppState::from_pool(pool.clone()).await?;
    state.command_stream = Some(
        CommandStreamProxy::new(&origin, CancellationToken::new())?.with_route_handover(
            RouteHandoverIssuer::new(pool.clone(), Arc::clone(&authority)),
        ),
    );
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    let cluster_api = ClusterApiService::new(pool.clone());

    for (address, generation) in [("10.0.0.21", 4), ("fd00::21", 9)] {
        let placed = place_sandbox(&pool, address, generation).await?;
        let command = post_streamed_command(&app, placed.sandbox).await?;
        let session_id = command["session_id"].as_str().unwrap().to_string();
        let token = command["attach"]["token"].as_str().unwrap().to_string();
        let handover = seen
            .lock()
            .unwrap()
            .last()
            .cloned()
            .flatten()
            .expect("the stream request carried a handover");
        let handed = verifier.verify_data_plane_route_handover(&handover)?;

        let resolved = cluster_api
            .resolve_data_plane_session_route(tonic::Request::new(
                ResolveDataPlaneSessionRouteRequest {
                    session_id: session_id.clone(),
                    protocol_session_id: None,
                    token_sha256: Some(hex::encode(Sha256::digest(token.as_bytes()))),
                    required_scope: Some(SessionKind::Process.connect_scope().as_str().to_string()),
                    requested_transport: None,
                },
            ))
            .await?
            .into_inner();
        let revocation = resolved.revocation.unwrap();
        let backend = resolved.backend.unwrap();
        assert_eq!(handed.session_id, resolved.session_id, "{address}");
        assert_eq!(
            handed.sandbox_id.to_string(),
            resolved.sandbox_id,
            "{address}"
        );
        assert_eq!(handed.org_id.to_string(), resolved.org_id, "{address}");
        assert_eq!(handed.kind, resolved.kind, "{address}");
        assert_eq!(handed.current_node_id, placed.node_id, "{address}");
        assert_eq!(
            handed.current_node_id.to_string(),
            resolved.current_node_id,
            "{address}"
        );
        assert_eq!(handed.backend_endpoint, backend.endpoint, "{address}");
        assert_eq!(
            handed.route_generation, resolved.route_generation,
            "{address}: the generation resolution stores"
        );
        assert_eq!(handed.revocation_epoch, revocation.epoch, "{address}");
        assert_eq!(
            handed.org_revocation_epoch, revocation.org_epoch,
            "{address}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&handed.adapter_state_json)?,
            serde_json::from_str::<Value>(&resolved.adapter_state_json)?,
            "{address}"
        );
        assert_eq!(handed.execution_generation.get(), generation, "{address}");
        let route_identity = |token: &str| {
            decode_unverified_space_api_route_ed25519_claims(
                &SpaceApiRouteEd25519Token::parse(token.to_string()).unwrap(),
            )
            .unwrap()
            .route_identity
        };
        assert_eq!(
            route_identity(&handed.space_api_route_token),
            route_identity(&resolved.space_api_route_token.unwrap().token),
            "{address}: the same node route identity"
        );
    }
    Ok(())
}

fn eligible_row() -> HandoverRouteRow {
    HandoverRouteRow {
        org_id: OrgId::new(),
        sandbox_id: SandboxId::new(),
        kind: "process".to_string(),
        adapter_state_json: "{}".to_string(),
        route_generation: 1,
        current_node_id: None,
        backend_endpoint: None,
        backend_transport: None,
        revocation_epoch: 0,
        revocation_status: "active".to_string(),
        org_revocation_epoch: 0,
        scopes: vec!["process:connect".to_string()],
        token_hash: Some(hex::encode(Sha256::digest(b"attach-token"))),
        token_unexpired: true,
        backend_process_id: None,
        sandbox_lifecycle: SandboxLifecycle::Started,
        sandbox_active_node_id: Some(NodeId::new()),
        node_address: Some("10.0.0.1".to_string()),
        node_managed_by: Some(NodeManagedBy::Kubernetes),
        direct_node_api_endpoint: None,
        sandbox_pod_uid: Some(PodUid::parse("pod-uid").unwrap()),
        execution_generation: Some(3),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
    }
}

#[test]
fn only_attaches_that_resolution_would_authorize_get_a_handover() {
    let handed = handover_route("proc_1", "attach-token", eligible_row()).unwrap();
    assert_eq!(
        handed.handover.route_generation, 2,
        "the first route bumps it"
    );
    let mut unchanged = eligible_row();
    unchanged.current_node_id = unchanged.sandbox_active_node_id;
    unchanged.backend_endpoint = Some(handed.handover.backend_endpoint.clone());
    unchanged.backend_transport = Some("grpc".to_string());
    assert_eq!(
        handover_route("proc_1", "attach-token", unchanged)
            .unwrap()
            .handover
            .route_generation,
        1,
        "a stored route is not bumped again"
    );

    type Edit = fn(&mut HandoverRouteRow);
    let cases: [(&str, Edit); 11] = [
        ("token_mismatch", |row| {
            row.token_hash = Some(hex::encode(Sha256::digest(b"another-token")));
        }),
        ("token_mismatch", |row| row.token_hash = None),
        ("token_expired", |row| row.token_unexpired = false),
        ("scope_missing", |row| {
            row.scopes = vec!["terminal:connect".to_string()]
        }),
        ("revoked", |row| {
            row.revocation_status = "revoked".to_string()
        }),
        ("process_bound", |row| {
            row.backend_process_id = Some("proc-1".to_string());
        }),
        ("not_started", |row| {
            row.sandbox_lifecycle = SandboxLifecycle::Restoring;
        }),
        ("not_started", |row| {
            row.sandbox_lifecycle = SandboxLifecycle::Paused;
        }),
        ("unplaced", |row| row.sandbox_active_node_id = None),
        ("no_endpoint", |row| row.node_address = None),
        ("unsupported_kind", |row| row.kind = "port".to_string()),
    ];
    for (expected, edit) in cases {
        let mut row = eligible_row();
        edit(&mut row);
        match handover_route("proc_1", "attach-token", row) {
            Err(HandoverError::Ineligible(reason)) => assert_eq!(reason, expected),
            Err(other) => panic!("{expected}: {other}"),
            Ok(_) => panic!("{expected}: minted a handover"),
        }
    }
}
