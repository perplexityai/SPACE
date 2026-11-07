use super::*;
use axum::{Router, body::Body, http::Method, http::Request, response::IntoResponse as _};
use pplx_space_accounts::NodeId;
use pplx_space_api_route::ed25519::{
    SPACE_API_ROUTE_ED25519_ISSUER_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
    SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, SpaceApiRouteEd25519Authority,
};
use pplx_space_common_core::{sql, types::SandboxId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_node_api_sim::{SimProcessSpec, SimWorkerHarness};
use serde_json::json;
use tokio_stream::StreamExt as _;
use tower::ServiceExt as _;

#[test]
fn public_process_omits_environment() {
    let process = public_process(spaced_ops::ProcessInfo {
        process_id: "process-1".to_string(),
        pid: 42,
        command: vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string(),
        ],
        cwd: Some("/workspace".to_string()),
        env: [("SECRET".to_string(), "value".to_string())].into(),
        user: Some("user".to_string()),
        tag: Some("server".to_string()),
        pty: None,
        exit: None,
        ended_at: None,
    })
    .expect("valid process");

    assert_eq!(
        process.process_id,
        ProcessId::parse("process-1").expect("process id parses")
    );
    assert_eq!(process.pid, 42);
    assert_eq!(
        process.command,
        vec![
            "python".to_string(),
            "-m".to_string(),
            "http.server".to_string()
        ]
    );
    assert_eq!(process.cwd.as_deref(), Some("/workspace"));
    assert_eq!(process.user.as_deref(), Some("user"));
    assert_eq!(process.tag.as_deref(), Some("server"));
    // A running process keeps the wire shape it had before exits were
    // reported, so strict clients see no new field.
    assert_eq!(
        serde_json::to_value(&process).unwrap(),
        json!({
            "process_id": "process-1",
            "pid": 42,
            "command": ["python", "-m", "http.server"],
            "cwd": "/workspace",
            "user": "user",
            "tag": "server",
        })
    );
}

// An exited process reports its exit as the stream's `process_exited` does.
#[test]
fn public_process_reports_an_exit_like_process_exited() {
    use spaced_ops::process_exit::Outcome;
    let ended_at = prost_types::Timestamp {
        seconds: 1_790_596_800,
        nanos: 500_000_000,
    };
    for (outcome, exit_code, exit_status, signal) in [
        (
            Outcome::Exited(spaced_ops::ProcessExited { exit_code: 7 }),
            7,
            "exited",
            None,
        ),
        (
            Outcome::Signaled(spaced_ops::ProcessSignaled { signal: Some(9) }),
            137,
            "signaled",
            Some(9),
        ),
        (
            Outcome::TimedOut(spaced_ops::ProcessTimedOut { signal: Some(9) }),
            124,
            "timed_out",
            Some(9),
        ),
        (
            Outcome::Unknown(spaced_ops::ProcessUnknown {}),
            1,
            "unknown",
            None,
        ),
    ] {
        let process = public_process(spaced_ops::ProcessInfo {
            process_id: "process-1".to_string(),
            pid: 42,
            command: vec!["true".to_string()],
            exit: Some(spaced_ops::ProcessExit {
                outcome: Some(outcome),
            }),
            ended_at: Some(ended_at),
            ..Default::default()
        })
        .expect("valid process");
        let json = serde_json::to_value(&process).unwrap();
        assert_eq!(json["state"], "exited", "{json}");
        assert_eq!(json["exit_code"], exit_code, "{json}");
        assert_eq!(json["exit_status"], exit_status, "{json}");
        assert_eq!(
            json.get("signal").and_then(|signal| signal.as_i64()),
            signal.map(i64::from),
            "{json}"
        );
        assert_eq!(json["ended_at"], "2026-09-28T12:00:00.5Z", "{json}");
    }
}

#[test]
fn public_process_rejects_an_invalid_runtime_process_id() {
    let error = public_process(spaced_ops::ProcessInfo {
        process_id: "x".repeat(pplx_space_common_core::types::MAX_PROCESS_ID_BYTES + 1),
        pid: 42,
        command: vec!["true".to_string()],
        cwd: None,
        env: Default::default(),
        user: None,
        tag: None,
        pty: None,
        exit: None,
        ended_at: None,
    })
    .expect_err("invalid process id");

    assert_eq!(
        error.into_response().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn process_action_dispatch_matches_the_published_paths() {
    for (path, expected) in [
        (
            pplx_space_public_api::routes::SIGNAL_PROCESS.path(),
            ProcessAction::Signal,
        ),
        (
            pplx_space_public_api::routes::KILL_PROCESS.path(),
            ProcessAction::Kill,
        ),
    ] {
        let segment = path
            .rsplit('/')
            .next()
            .expect("published process action path has a final segment")
            .replace("{process_id}", "process-1");
        assert_eq!(
            parse_process_action_segment(&segment).expect("published action should dispatch"),
            (
                ProcessId::parse("process-1").expect("process id parses"),
                expected
            )
        );
    }
    for invalid in ["process-1", "process-1:restart", ":signal"] {
        parse_process_action_segment(invalid).expect_err("unknown action should be rejected");
    }
}

#[test]
fn process_page_orders_processes_and_resumes_after_cursor() {
    let processes = || {
        ["process-3", "process-1", "process-2"]
            .into_iter()
            .enumerate()
            .map(|(pid, process_id)| PublicProcess {
                process_id: ProcessId::parse(process_id).expect("process id parses"),
                pid: pid as u32,
                command: vec!["true".to_string()],
                cwd: None,
                user: None,
                tag: None,
                state: None,
                exit_code: None,
                exit_status: None,
                signal: None,
                ended_at: None,
            })
            .collect::<Vec<_>>()
    };

    let first = process_page(processes(), None, 2).unwrap();
    assert_eq!(
        first
            .data
            .iter()
            .map(|process| process.process_id.as_str())
            .collect::<Vec<_>>(),
        ["process-1", "process-2"]
    );
    let cursor = first.next_cursor.expect("first page has another result");
    let cursor = decode_cursor(CursorKind::Process, cursor.as_str()).unwrap();
    let cursor = ProcessId::parse(cursor).unwrap();

    let second = process_page(processes(), Some(&cursor), 2).unwrap();
    assert_eq!(
        second
            .data
            .iter()
            .map(|process| process.process_id.as_str())
            .collect::<Vec<_>>(),
        ["process-3"]
    );
    assert_eq!(second.next_cursor, None);
}

const KEY: &str = "process-routes-test-key";

struct SimFixture {
    db: TestDb,
    worker: SimWorkerHarness,
    app: Router,
    sandbox: SandboxId,
}

/// Signs the route a guest call carries.
fn route_signing_authority() -> std::sync::Arc<SpaceApiRouteEd25519Authority> {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_CLUSTER_ID", "testing");
    env.set(
        SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
        pplx_space_api_route::ed25519::test_signing_key_path()
            .to_str()
            .unwrap(),
    );
    env.set(
        SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
        "process-routes-test-key",
    );
    env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "space-api");
    std::sync::Arc::new(
        SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap(),
    )
}

/// A started sandbox whose guest is the simulated worker's process table.
async fn sim_fixture() -> TestDbResult<SimFixture> {
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let worker = SimWorkerHarness::builder(node.to_string()).start().await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
    ))
    .bind(node)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(worker.endpoint())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'process-routes-test', $3, ARRAY['native'])"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let sandbox = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, active_node_id,
             template_id, network_policy_generation)
         VALUES ($1, $2, $3, 'started', 'on_node', $4, 'seed', 1)"
    ))
    .bind(sandbox)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities
             (sandbox_id, route_generation, current_node_id, substrate_kind,
              execution_generation, activation_attempt_id)
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(sandbox)
    .bind(node)
    .bind(uuid::Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    worker
        .state()
        .ensure_default_running(&sandbox.to_string())
        .await?;
    let state = AppState::from_pools_with_lifecycle(
        db.pool.clone(),
        pplx_space_postgres_pool::CredentialsPool::shared(db.pool.clone()),
        pplx_space_service_lifecycle::DrainSignal::new(),
        tokio_util::sync::CancellationToken::new(),
        tokio_util::task::TaskTracker::new(),
        Some(route_signing_authority()),
    )
    .await?;
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    Ok(SimFixture {
        db,
        worker,
        app,
        sandbox,
    })
}

impl SimFixture {
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> TestDbResult<(StatusCode, Value)> {
        let path = format!(
            "/v1/sandboxes/{}{path}",
            PublicSandboxId::from(self.sandbox)
        );
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {KEY}"))
            .header("idempotency-key", uuid::Uuid::now_v7().to_string());
        let body = match body {
            Some(body) => {
                request = request.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&body)?)
            }
            None => Body::empty(),
        };
        let response = self.app.clone().oneshot(request.body(body)?).await?;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
        Ok((status, serde_json::from_slice(&bytes)?))
    }

    /// Creates a command; returns its session id and process id.
    async fn create_command(&self) -> TestDbResult<(String, String)> {
        let (status, command) = self
            .send(
                Method::POST,
                "/commands",
                Some(json!({"command": ["sleep", "60"]})),
            )
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{command}");
        Ok((
            command["session_id"].as_str().unwrap().to_string(),
            command["process_id"].as_str().unwrap().to_string(),
        ))
    }

    /// Creates a command and starts it as the data plane does: the start
    /// assigns the command's process id, which the guest keeps only when
    /// `guest_keeps_id`, and the session is bound to the id the guest
    /// reports. Returns the command's process id and the guest's.
    async fn start_command(&self, guest_keeps_id: bool) -> TestDbResult<(String, String)> {
        let (session_id, process_id) = self.create_command().await?;
        let guest_process_id = self
            .start_guest_process(guest_keeps_id.then(|| process_id.clone()))
            .await;
        sqlx::query(sql!(
            "UPDATE data_plane_sessions
             SET backend_process_id = $2, backend_process_bound_at = NOW()
             WHERE session_id = $1"
        ))
        .bind(&session_id)
        .bind(&guest_process_id)
        .execute(&self.db.pool)
        .await?;
        Ok((process_id, guest_process_id))
    }

    /// Starts a long-running guest process and returns the id it reports.
    async fn start_guest_process(&self, process_id: Option<String>) -> String {
        let mut events = self
            .worker
            .state()
            .processes()
            .start(
                &self.sandbox.to_string(),
                SimProcessSpec {
                    command: vec!["sleep".to_string(), "60".to_string()],
                    process_id,
                    ..Default::default()
                },
            )
            .await;
        let started = events.next().await.unwrap().unwrap();
        started.process_id
    }

    async fn listed(&self) -> TestDbResult<Vec<String>> {
        let (status, page) = self.send(Method::GET, "/processes", None).await?;
        assert_eq!(status, StatusCode::OK, "{page}");
        Ok(page["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|process| process["process_id"].as_str().unwrap().to_string())
            .collect())
    }
}

// A guest that predates assigned process ids mints its own, and the data
// plane binds the command's session to it. The command's process id still
// names the process everywhere the API takes one, and the list shows it under
// that id alone.
#[tokio::test]
async fn command_process_ids_reach_processes_of_guests_that_mint_their_own() -> TestDbResult<()> {
    use Method;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let fixture = sim_fixture().await?;
    let (signaled, signaled_guest) = fixture.start_command(false).await?;
    let (killed, _) = fixture.start_command(false).await?;
    assert_ne!(signaled, signaled_guest);

    let mut expected = vec![signaled.clone(), killed.clone()];
    expected.sort();
    assert_eq!(fixture.listed().await?, expected);

    let (status, process) = fixture
        .send(Method::GET, &format!("/processes/{signaled}"), None)
        .await?;
    assert_eq!(status, StatusCode::OK, "{process}");
    assert_eq!(process["process_id"], signaled.as_str());
    assert_eq!(process.get("state"), None, "{process}");
    // The guest's own id still answers, under that id.
    let (status, process) = fixture
        .send(Method::GET, &format!("/processes/{signaled_guest}"), None)
        .await?;
    assert_eq!(status, StatusCode::OK, "{process}");
    assert_eq!(process["process_id"], signaled_guest.as_str());

    let (status, body) = fixture
        .send(
            Method::POST,
            &format!("/processes/{signaled}:signal"),
            Some(json!({"signal": 15})),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = fixture
        .send(Method::POST, &format!("/processes/{killed}:kill"), None)
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    for (process_id, signal) in [(&signaled, 15), (&killed, 9)] {
        let (status, process) = fixture
            .send(Method::GET, &format!("/processes/{process_id}"), None)
            .await?;
        assert_eq!(status, StatusCode::OK, "{process}");
        assert_eq!(process["process_id"], process_id.as_str());
        assert_eq!(process["exit_status"], "signaled", "{process}");
        assert_eq!(process["signal"], signal, "{process}");
    }
    assert_eq!(fixture.listed().await?, Vec::<String>::new());
    Ok(())
}

// A current guest keeps the assigned id, whether or not the session's
// binding has committed, and a process no session started keeps the guest's:
// each is listed and addressed as it was.
#[tokio::test]
async fn processes_of_guests_that_keep_assigned_ids_answer_as_before() -> TestDbResult<()> {
    use Method;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let fixture = sim_fixture().await?;
    let (command, command_guest) = fixture.start_command(true).await?;
    assert_eq!(command, command_guest);
    // Started, but its binding has not committed yet.
    let (_, binding) = fixture.create_command().await?;
    fixture.start_guest_process(Some(binding.clone())).await;
    let unbound = fixture.start_guest_process(None).await;

    let mut expected = vec![command.clone(), binding.clone(), unbound.clone()];
    expected.sort();
    assert_eq!(fixture.listed().await?, expected);
    for process_id in [&command, &binding, &unbound] {
        let (status, process) = fixture
            .send(Method::GET, &format!("/processes/{process_id}"), None)
            .await?;
        assert_eq!(status, StatusCode::OK, "{process}");
        assert_eq!(process["process_id"], process_id.as_str());
        let (status, body) = fixture
            .send(Method::POST, &format!("/processes/{process_id}:kill"), None)
            .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(fixture.listed().await?, Vec::<String>::new());
    Ok(())
}
