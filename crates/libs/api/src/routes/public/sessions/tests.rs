use super::*;
use pplx_space_common_core::types::MAX_GUEST_USER_NAME_BYTES;

const LINUX_CWD: &str = "/workspace";
const WINDOWS_DRIVE_CWD: &str = "C:\\Users\\me";
const WINDOWS_UNC_CWD: &str = "\\\\server\\share";

fn process_request_with_cwd(cwd: &str) -> CreateProcessRequest {
    process_request(Some(cwd.to_string()), None)
}

fn process_request(cwd: Option<String>, user: Option<String>) -> CreateProcessRequest {
    CreateProcessRequest {
        command: vec!["echo".to_string()],
        cwd,
        env: Default::default(),
        pty: None,
        stdin: None,
        user,
        timeout: None,
    }
}

#[test]
fn process_request_user_is_optional_and_bounded() {
    validate_process_request(&process_request(None, None)).expect("absent user accepted");
    validate_process_request(&process_request(None, Some("root".to_string())))
        .expect("named user accepted");

    for rejected in [
        String::new(),
        "u".repeat(MAX_GUEST_USER_NAME_BYTES + 1),
        "ro\0ot".to_string(),
    ] {
        let error = validate_process_request(&process_request(None, Some(rejected)))
            .expect_err("invalid user rejected");
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
}

#[test]
fn terminal_size_validation_accepts_absent_or_nonzero_size() {
    validate_terminal_size(None).expect("absent size accepted");
    validate_terminal_size(Some(&TerminalSize { rows: 24, cols: 80 }))
        .expect("nonzero size accepted");
}

#[test]
fn terminal_size_validation_rejects_zero_dimensions() {
    let rows = validate_terminal_size(Some(&TerminalSize { rows: 0, cols: 80 }))
        .expect_err("zero rows rejected");
    assert_eq!(rows.into_response().status(), StatusCode::BAD_REQUEST);

    let cols = validate_terminal_size(Some(&TerminalSize { rows: 24, cols: 0 }))
        .expect_err("zero cols rejected");
    assert_eq!(cols.into_response().status(), StatusCode::BAD_REQUEST);
}

#[test]
fn process_request_cwd_requires_a_nonempty_linux_guest_path() {
    validate_process_request(&process_request_with_cwd(LINUX_CWD))
        .expect("Linux guest path accepted");

    for cwd in ["", WINDOWS_DRIVE_CWD, WINDOWS_UNC_CWD] {
        let error = validate_process_request(&process_request_with_cwd(cwd))
            .expect_err("non-Linux guest path rejected");
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }
}

#[test]
fn session_list_limit_defaults_and_caps() {
    assert_eq!(
        ListSessionsQuery {
            cursor: None,
            limit: None,
            kind: None,
        }
        .limit(),
        DEFAULT_SESSION_LIST_LIMIT
    );
    assert_eq!(
        ListSessionsQuery {
            cursor: None,
            limit: Some(MAX_SESSION_LIST_LIMIT + 1),
            kind: None,
        }
        .limit(),
        MAX_SESSION_LIST_LIMIT
    );
}

#[test]
fn session_cursor_round_trips() {
    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let session_id = DataPlaneSessionId::new_for_kind(SessionKind::Process);
    let cursor = session_cursor(created_at, session_id).unwrap();
    let payload = decode_cursor(CursorKind::Session, cursor.as_str()).unwrap();
    let parsed = parse_session_cursor(&payload).unwrap();

    assert_eq!(parsed.created_at, created_at);
    assert_eq!(parsed.session_id, session_id);
}

fn wake_policy(on: &[&str]) -> AutoLifecyclePolicy {
    serde_json::from_value(serde_json::json!({ "wake": { "on": on } })).unwrap()
}

#[test]
fn command_admission_follows_lifecycle_and_wake_policy() {
    use CommandAdmission::*;
    use SandboxLifecycle::*;

    for lifecycle in SandboxLifecycle::all().iter().copied() {
        // (with a command or access wake source, without one)
        let (waking, without_wake) = match lifecycle {
            Started | Creating | Starting | Resuming | Restoring | Checkpointing => (Admit, Admit),
            Created | Paused | Suspended | Pausing | Suspending => (AdmitAndWake, Refuse),
            Stopped | Stopping | Deleting | Failed | Quarantined => (Refuse, Refuse),
        };
        for on in [&["command"][..], &["access"], &["access", "command"]] {
            assert_eq!(
                command_admission(lifecycle, &wake_policy(on)),
                waking,
                "{lifecycle:?} with wake.on={on:?}"
            );
        }
        assert_eq!(
            command_admission(lifecycle, &AutoLifecyclePolicy::default()),
            without_wake,
            "{lifecycle:?} with no wake source"
        );
    }
}

/// `createCommand` and `createProcessSession` against stored sandboxes,
/// through auth, owner scope and the handler, for the answers a caller
/// branches on.
mod not_running {
    use super::*;
    use axum::{
        Router,
        body::Body,
        http::{Method, Request, header},
    };
    use pplx_space_common_core::sql;
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use uuid::Uuid;

    const KEY: &str = "native-command-admission-key";

    struct Fixture {
        db: TestDb,
        app: Router,
    }

    async fn fixture() -> TestDbResult<Fixture> {
        let db = pg_test_db().await?;
        sqlx::query(sql!(
            "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
             VALUES ($1, $2, 'native-command-admission', $3, ARRAY['native'])"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(crate::auth::hash_token(KEY))
        .execute(&db.pool)
        .await?;
        let state = AppState::from_pool(db.pool.clone()).await?;
        let app = Router::new()
            .nest("/v1", crate::routes::public::v1_router(state.clone()))
            .with_state(state);
        Ok(Fixture { db, app })
    }

    /// A sandbox with no node, as a suspended Kubernetes sandbox is.
    async fn sandbox(
        fixture: &Fixture,
        lifecycle: &str,
        wake_on: &[&str],
    ) -> TestDbResult<PublicSandboxId> {
        let sandbox_id = PublicSandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, auto_lifecycle_json,
                 network_policy_generation)
             VALUES ($1, $2, $3, $4, $5, 1)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .bind(sqlx::types::Json(wake_policy(wake_on)))
        .execute(&fixture.db.pool)
        .await?;
        Ok(sandbox_id)
    }

    /// The two routes that start a process: commands and process sessions.
    const PROCESS_ROUTES: [&str; 2] = ["commands", "process-sessions"];

    async fn command(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
        route: &str,
    ) -> TestDbResult<(StatusCode, Option<String>, Value)> {
        let key = Uuid::now_v7().to_string();
        send(
            fixture,
            sandbox_id,
            route,
            &key,
            json!({"command": ["echo", "hello"]}),
        )
        .await
    }

    async fn send(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
        route: &str,
        key: &str,
        body: Value,
    ) -> TestDbResult<(StatusCode, Option<String>, Value)> {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/v1/sandboxes/{sandbox_id}/{route}"))
            .header("authorization", format!("Bearer {KEY}"))
            .header("content-type", "application/json")
            .header("idempotency-key", key)
            .body(Body::from(serde_json::to_vec(&body)?))?;
        let response = fixture.app.clone().oneshot(request).await?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(header::RETRY_AFTER)
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()?;
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
        Ok((status, retry_after, serde_json::from_slice(&body)?))
    }

    /// The 409 a caller branches on; `action` is the lifecycle action the
    /// `wake` detail names, or `None` when no action returns it to running.
    fn assert_not_running(
        sandbox_id: PublicSandboxId,
        state: &str,
        action: Option<&str>,
        (status, retry_after, body): &(StatusCode, Option<String>, Value),
    ) {
        assert_eq!(*status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "conflict", "{body}");
        assert_eq!(body["retryable"], false, "{body}");
        assert_eq!(retry_after.as_deref(), None, "{body}");
        let mut details = vec![
            json!({"key": "reason", "value": "sandbox_not_running"}),
            json!({"key": "state", "value": state}),
        ];
        if let Some(action) = action {
            details.push(json!({
                "key": "wake",
                "value": format!("POST /v1/sandboxes/{sandbox_id}:{action}"),
            }));
        }
        assert_eq!(body["details"], Value::Array(details), "{body}");
    }

    #[tokio::test]
    async fn a_command_that_cannot_wake_the_sandbox_is_a_conflict() -> TestDbResult<()> {
        let fixture = fixture().await?;
        for (lifecycle, wake_on, state, action) in [
            ("suspended", &[][..], "suspended", Some("wake")),
            ("paused", &[], "paused", Some("wake")),
            ("suspending", &[], "suspending", Some("wake")),
            ("stopped", &[], "stopped", Some("start")),
            ("stopped", &["command"], "stopped", Some("start")),
            ("deleting", &["command"], "deleting", None),
            ("failed", &["access"], "failed", None),
            // Reports `creating`, but its creation has not started: at rest.
            ("created", &[], "creating", Some("wake")),
        ] {
            let sandbox_id = sandbox(&fixture, lifecycle, wake_on).await?;
            for route in PROCESS_ROUTES {
                let answer = command(&fixture, sandbox_id, route).await?;
                assert_not_running(sandbox_id, state, action, &answer);
            }
        }
        let sessions: i64 = sqlx::query_scalar(sql!("SELECT count(*) FROM data_plane_sessions"))
            .fetch_one(&fixture.db.pool)
            .await?;
        assert_eq!(sessions, 0, "a refused command must not persist a session");
        Ok(())
    }

    #[tokio::test]
    async fn a_command_the_sandbox_will_run_is_created() -> TestDbResult<()> {
        let fixture = fixture().await?;
        for (lifecycle, wake_on) in [
            ("suspended", &["access"][..]),
            ("paused", &["command"]),
            ("suspending", &["access"]),
            ("created", &["command"]),
            // Heading to running: the attach waits, whatever the policy.
            ("creating", &[]),
            ("restoring", &[]),
        ] {
            let sandbox_id = sandbox(&fixture, lifecycle, wake_on).await?;
            for route in PROCESS_ROUTES {
                let (status, _, body) = command(&fixture, sandbox_id, route).await?;
                assert_eq!(status, StatusCode::CREATED, "{route} {lifecycle}: {body}");
            }
        }
        Ok(())
    }

    async fn set_lifecycle(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
        lifecycle: &str,
    ) -> TestDbResult<()> {
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id.as_uuid())
            .bind(lifecycle)
            .execute(&fixture.db.pool)
            .await?;
        Ok(())
    }

    /// The wake operations for `sandbox_id`: actor, product.
    async fn wakes(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
    ) -> TestDbResult<Vec<(Option<Uuid>, String)>> {
        Ok(sqlx::query_as(sql!(
            "SELECT actor_user_id, api_product
               FROM public_operations
              WHERE kind = 'wake_sandbox' AND target_id = $1"
        ))
        .bind(sandbox_id.to_string())
        .fetch_all(&fixture.db.pool)
        .await?)
    }

    #[tokio::test]
    async fn many_commands_on_a_sandbox_at_rest_share_one_system_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["access"]).await?;
        let answers = futures_util::future::try_join_all((0..20).map(|index| {
            let route = PROCESS_ROUTES[index % PROCESS_ROUTES.len()];
            command(&fixture, sandbox_id, route)
        }))
        .await?;
        for (status, _, body) in &answers {
            assert_eq!(*status, StatusCode::CREATED, "{body}");
        }
        assert_eq!(
            wakes(&fixture, sandbox_id).await?,
            vec![(None, "native".to_string())],
            "one actorless command wake for every request"
        );

        // A running sandbox needs none.
        let running = sandbox(&fixture, "started", &["access"]).await?;
        command(&fixture, running, "commands").await?;
        assert!(wakes(&fixture, running).await?.is_empty());
        Ok(())
    }

    /// The sandbox's active operations in the dispatcher's order: kind, actor.
    async fn queue(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
    ) -> TestDbResult<Vec<(String, Option<Uuid>)>> {
        Ok(sqlx::query_as(sql!(
            "SELECT kind, actor_user_id
               FROM public_operations
              WHERE target_type = 'sandbox' AND target_id = $1
                AND state IN ('queued', 'running')
              ORDER BY requested_at, operation_id"
        ))
        .bind(sandbox_id.to_string())
        .fetch_all(&fixture.db.pool)
        .await?)
    }

    #[tokio::test]
    async fn a_command_behind_a_queued_suspend_queues_its_own_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        for action in ["wake", "suspend"] {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("/v1/sandboxes/{sandbox_id}:{action}"))
                .header("authorization", format!("Bearer {KEY}"))
                .header("idempotency-key", Uuid::now_v7().to_string())
                .body(Body::empty())?;
            let response = fixture.app.clone().oneshot(request).await?;
            assert_eq!(response.status(), StatusCode::ACCEPTED, "{action}");
        }
        let caller = Some(SEED_USER_ID);

        // Joining the caller's wake would run it before the suspend and leave
        // the sandbox suspended for the command.
        let (status, _, body) = command(&fixture, sandbox_id, "commands").await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let expected = vec![
            ("wake_sandbox".to_string(), caller),
            ("suspend_sandbox".to_string(), caller),
            ("wake_sandbox".to_string(), None),
        ];
        assert_eq!(queue(&fixture, sandbox_id).await?, expected);

        // Nothing is queued behind that wake, so the next command joins it.
        let (status, _, body) = command(&fixture, sandbox_id, "process-sessions").await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(queue(&fixture, sandbox_id).await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn a_replayed_request_answers_as_it_did_before_the_sandbox_suspended() -> TestDbResult<()>
    {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "started", &[]).await?;
        let body = json!({"command": ["echo", "hello"]});
        for route in PROCESS_ROUTES {
            let key = Uuid::now_v7().to_string();
            set_lifecycle(&fixture, sandbox_id, "started").await?;
            let (status, _, created) =
                send(&fixture, sandbox_id, route, &key, body.clone()).await?;
            assert_eq!(status, StatusCode::CREATED, "{created}");

            set_lifecycle(&fixture, sandbox_id, "suspended").await?;
            let (status, _, replayed) =
                send(&fixture, sandbox_id, route, &key, body.clone()).await?;
            assert_eq!(status, StatusCode::CREATED, "{route} replay: {replayed}");
            let id = if route == "commands" {
                "session_id"
            } else {
                "id"
            };
            assert!(created[id].is_string(), "{route}: {created}");
            assert_eq!(replayed[id], created[id], "{route}");
            let fresh = command(&fixture, sandbox_id, route).await?;
            assert_not_running(sandbox_id, "suspended", Some("wake"), &fresh);
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_invalid_request_is_refused_before_the_sandbox_state() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &[]).await?;
        for route in PROCESS_ROUTES {
            let key = Uuid::now_v7().to_string();
            let (status, _, body) =
                send(&fixture, sandbox_id, route, &key, json!({"command": []})).await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route}: {body}");
        }
        Ok(())
    }
}
