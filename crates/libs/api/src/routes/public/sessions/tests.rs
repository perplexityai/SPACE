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
    use crate::inline_operations::{
        INLINE_CAPABLE_KINDS, InlineExecution, InlineExecutor, InlineLifecycle, InlineOperations,
        InlineOperationsConfig,
    };
    use crate::public_operation_driver::{OperationExecutionConfig, OperationExecutionResult};
    use crate::public_operations::{
        ApiProduct, ClaimLimits, ClaimPublicOperations, ClaimedPublicOperation,
        CreatePublicOperation, CreatePublicOperationIdempotency, CreatePublicOperationResult,
        PublicOperationRequeueReason,
    };
    use axum::{
        Router,
        body::Body,
        http::{Method, Request, header},
    };
    use pplx_space_common_core::sql;
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db,
    };
    use pplx_space_public_api::OperationState;
    use pplx_space_public_api::{OperationKind, OperationTargetType, RequestFingerprint};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use uuid::Uuid;

    const KEY: &str = "native-command-admission-key";

    struct Fixture {
        db: TestDb,
        app: Router,
        state: AppState,
    }

    async fn fixture() -> TestDbResult<Fixture> {
        fixture_with(None).await
    }

    /// [`fixture`] whose routes offer lifecycle work to `inline`.
    async fn fixture_with(inline: Option<InlineLifecycle>) -> TestDbResult<Fixture> {
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
        let mut state = AppState::from_pool(db.pool.clone()).await?;
        if let Some(inline) = inline {
            state.inline_lifecycle = inline;
        }
        let app = Router::new()
            .nest("/v1", crate::routes::public::v1_router(state.clone()))
            .with_state(state.clone());
        Ok(Fixture { db, app, state })
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

    /// `POST /v1/sandboxes/{sandbox_id}:{action}` with `key`: the 202's
    /// operation id.
    async fn lifecycle(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
        action: &str,
        key: &str,
    ) -> TestDbResult<Uuid> {
        lifecycle_as(fixture, KEY, sandbox_id, action, key).await
    }

    /// [`lifecycle`] authenticated with the API key `bearer`.
    async fn lifecycle_as(
        fixture: &Fixture,
        bearer: &str,
        sandbox_id: PublicSandboxId,
        action: &str,
        key: &str,
    ) -> TestDbResult<Uuid> {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/v1/sandboxes/{sandbox_id}:{action}"))
            .header("authorization", format!("Bearer {bearer}"))
            .header("idempotency-key", key)
            .body(Body::empty())?;
        let response = fixture.app.clone().oneshot(request).await?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
        let body: Value = serde_json::from_slice(&body)?;
        assert_eq!(status, StatusCode::ACCEPTED, "{action}: {body}");
        Ok(body["operation_id"]
            .as_str()
            .and_then(|id| id.strip_prefix("op_")?.parse().ok())
            .unwrap_or_else(|| panic!("{action}: {body}")))
    }

    /// The wake operations for `sandbox_id` in the dispatcher's order: id,
    /// actor.
    async fn wake_ids(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
    ) -> TestDbResult<Vec<(Uuid, Option<Uuid>)>> {
        Ok(sqlx::query_as(sql!(
            "SELECT operation_id, actor_user_id
               FROM public_operations
              WHERE kind = 'wake_sandbox' AND target_id = $1
              ORDER BY requested_at, operation_id"
        ))
        .bind(sandbox_id.to_string())
        .fetch_all(&fixture.db.pool)
        .await?)
    }

    async fn succeed(fixture: &Fixture, operation_id: Uuid) -> TestDbResult<()> {
        sqlx::query(sql!(
            "UPDATE public_operations
                SET state = 'succeeded', completed_at = NOW()
              WHERE operation_id = $1"
        ))
        .bind(operation_id)
        .execute(&fixture.db.pool)
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_after_a_command_joins_the_command_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        let (status, _, body) = command(&fixture, sandbox_id, "commands").await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let [(wake, None)] = wake_ids(&fixture, sandbox_id).await?[..] else {
            panic!("one system wake");
        };

        // The joined wake becomes the caller's, so it runs as their `:wake`
        // and their owner scope can read it.
        let key = Uuid::now_v7().to_string();
        assert_eq!(lifecycle(&fixture, sandbox_id, "wake", &key).await?, wake);
        let joined = vec![(wake, Some(SEED_USER_ID))];
        assert_eq!(wake_ids(&fixture, sandbox_id).await?, joined);

        // A reconnecting client repeats both requests with new keys.
        let (status, _, body) = command(&fixture, sandbox_id, "process-sessions").await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let key = Uuid::now_v7().to_string();
        assert_eq!(lifecycle(&fixture, sandbox_id, "wake", &key).await?, wake);
        assert_eq!(wake_ids(&fixture, sandbox_id).await?, joined);
        Ok(())
    }

    #[tokio::test]
    async fn a_replayed_wake_answers_with_the_wake_it_joined() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        let key = Uuid::now_v7().to_string();
        let wake = lifecycle(&fixture, sandbox_id, "wake", &key).await?;
        assert_eq!(
            wake_ids(&fixture, sandbox_id).await?,
            vec![(wake, Some(SEED_USER_ID))]
        );

        // Nothing remains to join, so only the key can answer the replay.
        succeed(&fixture, wake).await?;
        assert_eq!(lifecycle(&fixture, sandbox_id, "wake", &key).await?, wake);
        assert_eq!(wake_ids(&fixture, sandbox_id).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_join_under_a_recorded_key_answers_from_the_record() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        let key = Uuid::now_v7().to_string();
        let wake = lifecycle(&fixture, sandbox_id, "wake", &key).await?;

        // The route answers a recorded key before it joins anything; a
        // caller of the store that does not still gets the record's answer.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("idempotency-key", key.parse()?);
        let key = crate::routes::public::sandboxes::idempotency_key(&headers)
            .map_err(|_| "idempotency key")?;
        let fingerprint = RequestFingerprint::parse("0".repeat(64))?;
        let idempotency = CreatePublicOperationIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(
                pplx_space_public_api::routes::WAKE_SANDBOX.path(),
            ),
            key: &key,
            request_fingerprint: &fingerprint,
        };
        let org_id = OrgId::from_uuid(SEED_ORG_ID);
        let target_id = sandbox_id.to_string();
        let result = PublicOperationStore::new(fixture.db.pool.clone())
            .begin_operation_admission(org_id, ApiProduct::Native, &idempotency)
            .await?
            .create_or_join_active_wake(CreatePublicOperation {
                org_id,
                actor_user_id: Some(UserId::from_uuid(SEED_USER_ID)),
                api_product: ApiProduct::Native,
                kind: OperationKind::WakeSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(&target_id),
                payload_json: json!({}),
                idempotency: Some(idempotency.clone()),
                traceparent: None,
            })
            .await?;
        let CreatePublicOperationResult::IdempotencyConflict {
            operation_id: Some(recorded),
            ..
        } = result
        else {
            panic!("a conflict with the recorded wake: {result:?}");
        };
        assert_eq!(recorded.as_uuid(), wake);
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_after_the_wake_it_would_join_finished_queues_its_own() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        let [(system, None)] = wake_ids(&fixture, sandbox_id).await?[..] else {
            panic!("one system wake");
        };
        succeed(&fixture, system).await?;

        let key = Uuid::now_v7().to_string();
        let wake = lifecycle(&fixture, sandbox_id, "wake", &key).await?;
        assert_eq!(
            wake_ids(&fixture, sandbox_id).await?,
            vec![(system, None), (wake, Some(SEED_USER_ID))]
        );

        // Nor does the caller's own wake, once it finished.
        succeed(&fixture, wake).await?;
        let key = Uuid::now_v7().to_string();
        let next = lifecycle(&fixture, sandbox_id, "wake", &key).await?;
        assert_eq!(
            wake_ids(&fixture, sandbox_id).await?,
            vec![
                (system, None),
                (wake, Some(SEED_USER_ID)),
                (next, Some(SEED_USER_ID))
            ]
        );
        Ok(())
    }

    /// Take `operation_id` as the dispatcher's claim does, in `tx`.
    async fn claim(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        operation_id: Uuid,
    ) -> TestDbResult<()> {
        sqlx::query(sql!(
            "UPDATE public_operations
                SET state = 'running', claim_token = 'test-claim', attempts = attempts + 1,
                    claimed_at = NOW(), started_at = NOW()
              WHERE operation_id = $1"
        ))
        .bind(operation_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Who claimed the command wake a `:wake` arrives behind.
    #[derive(Debug, Clone, Copy)]
    enum CommandWakeClaimant {
        Dispatcher,
        /// Admission, as the API claims a wake it runs inline.
        Inline,
    }

    #[tokio::test]
    async fn a_wake_does_not_join_a_command_wake_already_claimed() -> TestDbResult<()> {
        for claimant in [CommandWakeClaimant::Dispatcher, CommandWakeClaimant::Inline] {
            let script = ScriptedWake::gated(WakeScript::Succeed);
            let fixture = match claimant {
                CommandWakeClaimant::Dispatcher => fixture().await?,
                CommandWakeClaimant::Inline => fixture_with(Some(script.inline())).await?,
            };
            let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
            command(&fixture, sandbox_id, "commands").await?;
            let [(system, None)] = wake_ids(&fixture, sandbox_id).await?[..] else {
                panic!("{claimant:?}: one system wake");
            };
            match claimant {
                CommandWakeClaimant::Dispatcher => {
                    let mut tx = fixture.db.pool.begin().await?;
                    claim(&mut tx, system).await?;
                    tx.commit().await?;
                }
                CommandWakeClaimant::Inline => {
                    script.0.started.notified().await;
                    assert_eq!(state_and_attempts(&fixture, system).await?, ("running", 1));
                }
            }

            // Its attempt runs by the actor its claim read: a command wake,
            // which the policy could still refuse, so this `:wake` must not
            // be answered with it. Nor does it run inline beside it.
            let wake = lifecycle(&fixture, sandbox_id, "wake", &Uuid::now_v7().to_string()).await?;
            let queued = vec![(system, None), (wake, Some(SEED_USER_ID))];
            assert_eq!(
                wake_ids(&fixture, sandbox_id).await?,
                queued,
                "{claimant:?}"
            );
            assert_eq!(state_and_attempts(&fixture, wake).await?, ("queued", 0));

            // A reconnect during that attempt joins the caller's own wake.
            command(&fixture, sandbox_id, "commands").await?;
            let again =
                lifecycle(&fixture, sandbox_id, "wake", &Uuid::now_v7().to_string()).await?;
            assert_eq!(again, wake, "{claimant:?}");
            assert_eq!(
                wake_ids(&fixture, sandbox_id).await?,
                queued,
                "{claimant:?}"
            );

            // Once the command wake places the sandbox, the dispatcher takes
            // the caller's wake, which finds it running.
            match claimant {
                CommandWakeClaimant::Dispatcher => succeed(&fixture, system).await?,
                CommandWakeClaimant::Inline => {
                    script.open_gate();
                    wait_for_state(&fixture, system, "succeeded").await?;
                }
            }
            set_lifecycle(&fixture, sandbox_id, "started").await?;
            let result = dispatcher_runs(&fixture, wake).await?;
            assert_eq!(result.state, OperationState::Succeeded, "{claimant:?}");
            assert_eq!(
                result
                    .result_json
                    .as_ref()
                    .and_then(|result| result["controller_response"].get("already_completed")),
                Some(&json!(true)),
                "{claimant:?}: {result:?}"
            );
            assert_eq!(
                script.executed(),
                match claimant {
                    CommandWakeClaimant::Dispatcher => vec![],
                    CommandWakeClaimant::Inline => vec![system],
                }
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_overtaken_by_the_claim_of_the_command_wake_queues_its_own() -> TestDbResult<()>
    {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        let [(system, None)] = wake_ids(&fixture, sandbox_id).await?[..] else {
            panic!("one system wake");
        };

        // A claim in flight: the `:wake` still sees the wake queued, then
        // waits on its row to hand it over, and the claim commits first. Both
        // connections are outside the pool the route uses.
        use sqlx::{Connection, PgConnection};
        let mut claimant = PgConnection::connect(&fixture.db.database_url).await?;
        let mut observer = PgConnection::connect(&fixture.db.database_url).await?;
        let mut tx = claimant.begin().await?;
        claim(&mut tx, system).await?;
        let key = Uuid::now_v7().to_string();
        let commit_once_waited = async {
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                while !sqlx::query_scalar::<_, bool>(sql!(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_stat_activity
                          WHERE datname = current_database()
                            AND wait_event_type = 'Lock'
                            AND query LIKE '%SET actor_user_id%')"
                ))
                .fetch_one(&mut observer)
                .await?
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                TestDbResult::Ok(())
            })
            .await??;
            tx.commit().await?;
            TestDbResult::Ok(())
        };
        let (wake, ()) = tokio::try_join!(
            lifecycle(&fixture, sandbox_id, "wake", &key),
            commit_once_waited
        )?;
        assert_eq!(
            wake_ids(&fixture, sandbox_id).await?,
            vec![(system, None), (wake, Some(SEED_USER_ID))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_never_joins_another_users_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let other_user = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'unused', 'Other Caller', $3, 'member')"
        ))
        .bind(other_user)
        .bind(format!("other-{other_user}@space.invalid"))
        .bind(SEED_ORG_ID)
        .execute(&fixture.db.pool)
        .await?;
        const OTHER_KEY: &str = "native-command-admission-other-key";
        sqlx::query(sql!(
            "INSERT INTO api_keys (id, org_id, name, key_hash, created_by, allowed_surfaces)
             VALUES ($1, $2, 'native-command-admission-other', $3, $4, ARRAY['native'])"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(crate::auth::hash_token(OTHER_KEY))
        .bind(other_user)
        .execute(&fixture.db.pool)
        .await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        let [(system, None)] = wake_ids(&fixture, sandbox_id).await?[..] else {
            panic!("one system wake");
        };

        // The first caller to join the system's wake owns it, so the next
        // caller's owner scope could not read it.
        let key = Uuid::now_v7().to_string();
        let joined = lifecycle_as(&fixture, OTHER_KEY, sandbox_id, "wake", &key).await?;
        assert_eq!(joined, system);
        let key = Uuid::now_v7().to_string();
        let wake = lifecycle(&fixture, sandbox_id, "wake", &key).await?;
        assert_eq!(
            wake_ids(&fixture, sandbox_id).await?,
            vec![(system, Some(other_user)), (wake, Some(SEED_USER_ID))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_never_joins_an_admin_force_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        // A force-wake the caller sent through the admin API.
        let target_id = sandbox_id.to_string();
        let admin = PublicOperationStore::new(fixture.db.pool.clone())
            .create_operation(CreatePublicOperation {
                org_id: OrgId::from_uuid(SEED_ORG_ID),
                actor_user_id: Some(UserId::from_uuid(SEED_USER_ID)),
                api_product: ApiProduct::Admin,
                kind: OperationKind::WakeSandbox,
                target_type: Some(OperationTargetType::Sandbox),
                target_id: Some(&target_id),
                payload_json: SandboxLifecycleOperationPayload {
                    sandbox_id,
                    action: SandboxLifecycleAction::Wake,
                    rollback: None,
                    wake_activity: None,
                }
                .into_json(),
                idempotency: None,
                traceparent: None,
            })
            .await?;
        let CreatePublicOperationResult::Created(admin) = admin else {
            panic!("admin wake created: {admin:?}");
        };

        let wake = lifecycle(&fixture, sandbox_id, "wake", &Uuid::now_v7().to_string()).await?;
        assert_ne!(wake, admin.as_uuid());
        assert_eq!(wake_ids(&fixture, sandbox_id).await?.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_wake_behind_a_queued_suspend_queues_its_own() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        command(&fixture, sandbox_id, "commands").await?;
        lifecycle(&fixture, sandbox_id, "suspend", &Uuid::now_v7().to_string()).await?;

        // Joining the command's wake would run it before the suspend and
        // leave the sandbox suspended.
        lifecycle(&fixture, sandbox_id, "wake", &Uuid::now_v7().to_string()).await?;
        let caller = Some(SEED_USER_ID);
        assert_eq!(
            queue(&fixture, sandbox_id).await?,
            vec![
                ("wake_sandbox".to_string(), None),
                ("suspend_sandbox".to_string(), caller),
                ("wake_sandbox".to_string(), caller),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_command_behind_a_queued_suspend_queues_its_own_wake() -> TestDbResult<()> {
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        for action in ["wake", "suspend"] {
            lifecycle(&fixture, sandbox_id, action, &Uuid::now_v7().to_string()).await?;
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

    /// The activity recorded on each wake operation for `sandbox_id`.
    async fn wake_activities(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
    ) -> TestDbResult<Vec<Option<String>>> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT payload_json->>'wake_activity'
               FROM public_operations
              WHERE kind = 'wake_sandbox' AND target_id = $1
              ORDER BY requested_at, operation_id"
        ))
        .bind(sandbox_id.to_string())
        .fetch_all(&fixture.db.pool)
        .await?)
    }

    #[tokio::test]
    async fn an_attach_on_a_sandbox_at_rest_queues_the_wake_its_policy_allows() -> TestDbResult<()>
    {
        use crate::controllers::substrate::ExecutionActivityKind::{Access, Command};
        let fixture = fixture().await?;
        let for_command = Some("command");
        let for_access = Some("access");
        for (lifecycle, wake_on, activity, queued, recorded) in [
            ("suspended", &["access"][..], Access, true, for_access),
            ("suspended", &["access"], Command, true, for_command),
            ("suspended", &["command"], Command, true, for_command),
            // A reattach to a process that already started, or a file watch,
            // is access, which a `command` policy does not wake for.
            ("suspended", &["command"], Access, false, None),
            ("paused", &["command"], Access, false, None),
            ("paused", &["command"], Command, true, for_command),
            ("suspended", &[], Command, false, None),
            ("stopped", &["command"], Command, false, None),
            // Running or heading there since the resolver looked: no wake,
            // but the attach keeps waiting.
            ("started", &["access"], Access, true, None),
            ("restoring", &[], Access, true, None),
        ] {
            let sandbox_id = sandbox(&fixture, lifecycle, wake_on).await?;
            let stored =
                crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                    .await?
                    .expect("seeded sandbox");
            let case = format!("{lifecycle} {wake_on:?} {activity:?}");
            for _ in 0..2 {
                assert_eq!(
                    queue_implicit_wake(&fixture.db.pool, None, &stored, activity, true)
                        .await
                        .map_err(|e| format!("{e:?}"))?,
                    queued,
                    "{case}"
                );
            }
            let expected: Vec<_> = recorded.map(str::to_owned).into_iter().map(Some).collect();
            assert_eq!(
                wake_activities(&fixture, sandbox_id).await?,
                expected,
                "{case}"
            );
            if recorded.is_some() {
                assert_eq!(
                    wakes(&fixture, sandbox_id).await?,
                    vec![(None, "native".to_string())]
                );
            }
        }

        // A start queued since the resolver looked is about to place a
        // stopped sandbox: keep waiting, with no wake of its own.
        let sandbox_id = sandbox(&fixture, "stopped", &["command"]).await?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("/v1/sandboxes/{sandbox_id}:start"))
            .header("authorization", format!("Bearer {KEY}"))
            .header("idempotency-key", Uuid::now_v7().to_string())
            .body(Body::empty())?;
        assert_eq!(
            fixture.app.clone().oneshot(request).await?.status(),
            StatusCode::ACCEPTED
        );
        let stored =
            crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                .await?
                .expect("seeded sandbox");
        assert!(
            queue_implicit_wake(&fixture.db.pool, None, &stored, Command, true)
                .await
                .map_err(|e| format!("{e:?}"))?
        );
        assert!(wake_activities(&fixture, sandbox_id).await?.is_empty());

        // A start that completed since the resolver looked, so nothing is
        // queued any more: keep waiting, with no wake.
        let sandbox_id = sandbox(&fixture, "stopped", &["command"]).await?;
        let stored =
            crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                .await?
                .expect("seeded sandbox");
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(stored.id)
        .execute(&fixture.db.pool)
        .await?;
        assert!(
            queue_implicit_wake(&fixture.db.pool, None, &stored, Command, true)
                .await
                .map_err(|e| format!("{e:?}"))?
        );
        assert!(wake_activities(&fixture, sandbox_id).await?.is_empty());

        // With the record off, the wake is queued in the payload every
        // executor parses, which runs it as a command's.
        let sandbox_id = sandbox(&fixture, "suspended", &["access"]).await?;
        let stored =
            crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                .await?
                .expect("seeded sandbox");
        assert!(
            queue_implicit_wake(&fixture.db.pool, None, &stored, Access, false)
                .await
                .map_err(|e| format!("{e:?}"))?
        );
        assert_eq!(
            wake_payloads(&fixture, sandbox_id).await?,
            vec![json!({ "sandbox_id": sandbox_id, "action": "wake" })]
        );

        // So is a new command's, with the record off by default.
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        let (status, _, body) = command(&fixture, sandbox_id, "commands").await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(
            wake_payloads(&fixture, sandbox_id).await?,
            vec![json!({ "sandbox_id": sandbox_id, "action": "wake" })]
        );
        Ok(())
    }

    /// The payload of each wake operation for `sandbox_id`.
    async fn wake_payloads(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
    ) -> TestDbResult<Vec<Value>> {
        Ok(sqlx::query_scalar::<_, sqlx::types::Json<Value>>(sql!(
            "SELECT payload_json
               FROM public_operations
              WHERE kind = 'wake_sandbox' AND target_id = $1
              ORDER BY requested_at, operation_id"
        ))
        .bind(sandbox_id.to_string())
        .fetch_all(&fixture.db.pool)
        .await?
        .into_iter()
        .map(|payload| payload.0)
        .collect())
    }

    /// A policy updated to allow the attach's activity after the resolver's
    /// sandbox read, while the sandbox stays at rest, is judged as it is now:
    /// the wake is queued and the attach keeps waiting, rather than getting
    /// the final at-rest answer the earlier policy gave.
    #[tokio::test]
    async fn an_attach_is_judged_by_the_policy_read_with_the_lifecycle() -> TestDbResult<()> {
        use crate::controllers::substrate::ExecutionActivityKind::Access;
        let fixture = fixture().await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["command"]).await?;
        let stored =
            crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                .await?
                .expect("seeded sandbox");
        sqlx::query(sql!(
            "UPDATE sandboxes SET auto_lifecycle_json = $2 WHERE id = $1"
        ))
        .bind(stored.id)
        .bind(sqlx::types::Json(wake_policy(&["access"])))
        .execute(&fixture.db.pool)
        .await?;

        assert!(
            queue_implicit_wake(&fixture.db.pool, None, &stored, Access, true)
                .await
                .map_err(|e| format!("{e:?}"))?
        );
        assert_eq!(
            wake_activities(&fixture, sandbox_id).await?,
            vec![Some("access".to_owned())]
        );
        Ok(())
    }

    /// What a scripted inline wake does once it starts.
    #[derive(Clone, Copy)]
    enum WakeScript {
        Succeed,
        /// The process running it dies.
        Panic,
    }

    struct WakeScriptState {
        script: WakeScript,
        executed: Mutex<Vec<Uuid>>,
        started: Notify,
        /// When set, the execution waits here after starting.
        gate: Option<Notify>,
    }

    /// An inline executor for lifecycle work, standing in for the
    /// dispatcher's lifecycle executor.
    #[derive(Clone)]
    struct ScriptedWake(Arc<WakeScriptState>);

    impl ScriptedWake {
        fn new(script: WakeScript) -> Self {
            Self::build(script, false)
        }

        fn gated(script: WakeScript) -> Self {
            Self::build(script, true)
        }

        fn build(script: WakeScript, gated: bool) -> Self {
            Self(Arc::new(WakeScriptState {
                script,
                executed: Mutex::new(Vec::new()),
                started: Notify::new(),
                gate: gated.then(Notify::new),
            }))
        }

        fn open_gate(&self) {
            self.0.gate.as_ref().expect("gated script").notify_one();
        }

        /// The operations it executed, in order.
        fn executed(&self) -> Vec<Uuid> {
            self.0.executed.lock().expect("executed").clone()
        }

        /// Inline execution of every inline-capable kind for every
        /// organization, through this script.
        fn inline(&self) -> InlineLifecycle {
            InlineLifecycle::new(
                InlineOperations::new(
                    InlineOperationsConfig {
                        enabled: true,
                        kinds: INLINE_CAPABLE_KINDS.to_vec(),
                        ..Default::default()
                    },
                    tokio_util::task::TaskTracker::new(),
                    pplx_space_common_svc::DrainSignal::new(),
                    OperationExecutionConfig::default(),
                    true,
                ),
                Arc::new(self.clone()),
                pplx_space_feature_flags::FeatureFlagClient::disabled(),
            )
        }
    }

    impl InlineExecutor for ScriptedWake {
        fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution {
            let state = Arc::clone(&self.0);
            Box::pin(async move {
                state
                    .executed
                    .lock()
                    .expect("executed")
                    .push(operation.id.as_uuid());
                state.started.notify_one();
                if let Some(gate) = &state.gate {
                    gate.notified().await;
                }
                match state.script {
                    WakeScript::Succeed => OperationExecutionResult {
                        state: OperationState::Succeeded,
                        progress_message: "sandbox lifecycle action dispatched".to_string(),
                        result_json: Some(woken_result(&operation)),
                        terminal_error_code: None,
                        terminal_error_json: None,
                        retry_after: None,
                        requeue_reason: PublicOperationRequeueReason::TransientFailure,
                    },
                    WakeScript::Panic => panic!("the API process running the wake dies"),
                }
            })
        }
    }

    /// The result the lifecycle executor records for an action that placed
    /// the sandbox.
    fn woken_result(operation: &ClaimedPublicOperation) -> Value {
        let payload: crate::public_sandbox_operations::SandboxLifecycleOperationPayload =
            serde_json::from_value(operation.payload.clone()).expect("a lifecycle payload");
        serde_json::to_value(pplx_space_public_api::PublicOperationResult::Sandbox(
            pplx_space_public_api::PublicSandboxOperationResult {
                sandbox_id: payload.sandbox_id,
                state: pplx_space_public_api::SandboxLifecycleState::Running,
                action: Some(payload.action),
                controller_response: Some(json!({"ok": true})),
            },
        ))
        .expect("a result serializes")
    }

    async fn state_and_attempts(
        fixture: &Fixture,
        operation_id: Uuid,
    ) -> TestDbResult<(&'static str, i32)> {
        let (state, attempts): (String, i32) = sqlx::query_as(sql!(
            "SELECT state, attempts FROM public_operations WHERE operation_id = $1"
        ))
        .bind(operation_id)
        .fetch_one(&fixture.db.pool)
        .await?;
        let state = match state.as_str() {
            "queued" => "queued",
            "running" => "running",
            "succeeded" => "succeeded",
            other => panic!("unexpected state {other}"),
        };
        Ok((state, attempts))
    }

    async fn wait_for_state(
        fixture: &Fixture,
        operation_id: Uuid,
        state: &str,
    ) -> TestDbResult<()> {
        for _ in 0..500 {
            if state_and_attempts(fixture, operation_id).await?.0 == state {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("operation {operation_id} never reached {state}");
    }

    /// A dispatcher claim of every due lifecycle operation for the fixture's
    /// sandboxes, at its default staleness.
    async fn dispatcher_claim(
        fixture: &Fixture,
    ) -> TestDbResult<crate::public_operations::PublicOperationClaim> {
        Ok(PublicOperationStore::new(fixture.db.pool.clone())
            .claim_operations(ClaimPublicOperations {
                limits: ClaimLimits::any(16),
                claim_stale_after: OperationExecutionConfig::default().claim_stale_after,
                claimable_kinds: INLINE_CAPABLE_KINDS,
            })
            .await?)
    }

    /// The dispatcher claims `operation_id` and runs it through the real
    /// lifecycle executor, recording its outcome.
    async fn dispatcher_runs(
        fixture: &Fixture,
        operation_id: Uuid,
    ) -> TestDbResult<OperationExecutionResult> {
        let claim = dispatcher_claim(fixture).await?;
        let [operation] = &claim.operations[..] else {
            panic!(
                "the dispatcher claims one operation: {:?}",
                claim.operations
            );
        };
        assert_eq!(operation.id.as_uuid(), operation_id);
        let result: OperationExecutionResult =
            crate::public_sandbox_operations::SandboxLifecycleOperationExecutor::new(
                fixture.db.pool.clone(),
                fixture.state.sandbox_controller.clone(),
            )
            .execute(operation)
            .await
            .into();
        crate::public_operation_driver::drive_claimed_operation(
            PublicOperationStore::new(fixture.db.pool.clone()),
            claim.claim_token,
            OperationExecutionConfig::default(),
            operation.clone(),
            crate::public_operation_driver::OperationExecutor::Dispatcher,
            std::future::ready(result.clone()),
        )
        .await?;
        assert_eq!(
            state_and_attempts(fixture, operation_id).await?.0,
            "succeeded"
        );
        Ok(result)
    }

    /// How a test asks for a sandbox to run.
    #[derive(Debug, Clone, Copy)]
    enum Waker {
        /// `POST /v1/sandboxes/{sandbox_id}:{action}`.
        Action(&'static str),
        /// A command or process session, on this route.
        Command(&'static str),
        /// The session route resolver's attach, through the activation RPC.
        Attach,
    }

    /// Ask for `sandbox_id` to run by `waker`: the operation it recorded.
    async fn wake_by(
        fixture: &Fixture,
        sandbox_id: PublicSandboxId,
        waker: Waker,
    ) -> TestDbResult<Uuid> {
        match waker {
            Waker::Action(action) => {
                lifecycle(fixture, sandbox_id, action, &Uuid::now_v7().to_string()).await
            }
            Waker::Command(route) => {
                let (status, _, body) = command(fixture, sandbox_id, route).await?;
                assert_eq!(status, StatusCode::CREATED, "{body}");
                last_operation(fixture, sandbox_id).await
            }
            Waker::Attach => {
                let stored =
                    crate::controllers::sandbox::fetch_sandbox(&fixture.db.pool, sandbox_id.into())
                        .await?
                        .expect("seeded sandbox");
                assert!(
                    queue_implicit_wake(
                        &fixture.db.pool,
                        Some(&fixture.state.inline_lifecycle),
                        &stored,
                        crate::controllers::substrate::ExecutionActivityKind::Access,
                        true,
                    )
                    .await
                    .map_err(|e| format!("{e:?}"))?
                );
                last_operation(fixture, sandbox_id).await
            }
        }
    }

    /// The sandbox's most recently requested operation.
    async fn last_operation(fixture: &Fixture, sandbox_id: PublicSandboxId) -> TestDbResult<Uuid> {
        Ok(sqlx::query_scalar(sql!(
            "SELECT operation_id
               FROM public_operations
              WHERE target_type = 'sandbox' AND target_id = $1
              ORDER BY requested_at DESC, operation_id DESC
              LIMIT 1"
        ))
        .bind(sandbox_id.to_string())
        .fetch_one(&fixture.db.pool)
        .await?)
    }

    /// Every waker, with the sandbox state it acts on.
    const WAKERS: [(Waker, &str); 6] = [
        (Waker::Action("wake"), "suspended"),
        (Waker::Action("resume"), "paused"),
        (Waker::Action("start"), "stopped"),
        (Waker::Command("commands"), "suspended"),
        (Waker::Command("process-sessions"), "suspended"),
        (Waker::Attach, "suspended"),
    ];

    /// On an idle sandbox, each waker's operation is claimed as it is
    /// recorded and runs in the API, to the same durable record a
    /// dispatcher execution leaves; the dispatcher never sees it.
    #[tokio::test]
    async fn a_wake_on_an_idle_sandbox_runs_inline_and_completes() -> TestDbResult<()> {
        for (waker, lifecycle_state) in WAKERS {
            let script = ScriptedWake::new(WakeScript::Succeed);
            let fixture = fixture_with(Some(script.inline())).await?;
            let sandbox_id = sandbox(&fixture, lifecycle_state, &["access"]).await?;
            let operation_id = wake_by(&fixture, sandbox_id, waker).await?;
            wait_for_state(&fixture, operation_id, "succeeded").await?;
            assert_eq!(script.executed(), vec![operation_id], "{waker:?}");
            // One claim, at admission: no dispatcher runs here.
            let events: Vec<String> = sqlx::query_scalar(sql!(
                "SELECT event_type FROM public_operation_events
                  WHERE operation_id = $1 ORDER BY sequence"
            ))
            .bind(operation_id)
            .fetch_all(&fixture.db.pool)
            .await?;
            assert_eq!(state_and_attempts(&fixture, operation_id).await?.1, 1);
            assert_eq!(
                events,
                vec![
                    "operation.created",
                    "operation.claimed",
                    "operation.completed"
                ],
                "{waker:?}"
            );
        }
        Ok(())
    }

    /// Behind other work on its sandbox, here a queued `:suspend`, each
    /// waker's operation is recorded queued for the dispatcher, after it.
    #[tokio::test]
    async fn a_wake_behind_queued_work_is_queued_for_the_dispatcher() -> TestDbResult<()> {
        for (waker, lifecycle_state) in WAKERS {
            let script = ScriptedWake::new(WakeScript::Succeed);
            let fixture = fixture_with(Some(script.inline())).await?;
            let sandbox_id = sandbox(&fixture, lifecycle_state, &["access"]).await?;
            let suspend =
                lifecycle(&fixture, sandbox_id, "suspend", &Uuid::now_v7().to_string()).await?;
            let operation_id = wake_by(&fixture, sandbox_id, waker).await?;
            assert_ne!(operation_id, suspend, "{waker:?}");
            assert_eq!(
                state_and_attempts(&fixture, operation_id).await?,
                ("queued", 0),
                "{waker:?}"
            );
            assert_eq!(
                sqlx::query_scalar::<_, Vec<Uuid>>(sql!(
                    "SELECT ARRAY(SELECT operation_id FROM public_operations
                                   WHERE target_type = 'sandbox' AND target_id = $1
                                     AND state IN ('queued', 'running')
                                   ORDER BY requested_at, operation_id)"
                ))
                .bind(sandbox_id.to_string())
                .fetch_one(&fixture.db.pool)
                .await?,
                vec![suspend, operation_id],
                "{waker:?}: the wake sorts after the suspend"
            );
            assert!(script.executed().is_empty(), "{waker:?}");
        }
        Ok(())
    }

    /// The API process running an inline wake dies before recording its
    /// outcome: the wake stays the claimant's until its claim goes stale,
    /// then the dispatcher takes the same operation over.
    #[tokio::test]
    async fn an_inline_wake_whose_process_dies_is_redriven_by_the_dispatcher() -> TestDbResult<()> {
        let script = ScriptedWake::new(WakeScript::Panic);
        let fixture = fixture_with(Some(script.inline())).await?;
        let sandbox_id = sandbox(&fixture, "suspended", &["access"]).await?;
        let wake = lifecycle(&fixture, sandbox_id, "wake", &Uuid::now_v7().to_string()).await?;
        script.0.started.notified().await;
        // The task is gone; its claim is still live.
        assert_eq!(state_and_attempts(&fixture, wake).await?, ("running", 1));
        assert!(dispatcher_claim(&fixture).await?.operations.is_empty());

        sqlx::query(sql!(
            "UPDATE public_operations
             SET claimed_at = NOW() - INTERVAL '10 minutes'
             WHERE operation_id = $1"
        ))
        .bind(wake)
        .execute(&fixture.db.pool)
        .await?;
        let claim = dispatcher_claim(&fixture).await?;
        let [reclaimed] = &claim.operations[..] else {
            panic!("the dispatcher reclaims the wake: {:?}", claim.operations);
        };
        assert_eq!(reclaimed.id.as_uuid(), wake);
        assert_eq!(reclaimed.claim_generation, 2);
        assert_eq!(state_and_attempts(&fixture, wake).await?, ("running", 2));
        assert_eq!(script.executed(), vec![wake]);
        Ok(())
    }

    /// A replay of an inline `:wake` or `:start` answers with the operation
    /// it recorded, and neither records nor runs anything again.
    #[tokio::test]
    async fn a_replayed_inline_wake_answers_with_the_same_operation() -> TestDbResult<()> {
        for (action, lifecycle_state) in [("wake", "suspended"), ("start", "stopped")] {
            let script = ScriptedWake::new(WakeScript::Succeed);
            let fixture = fixture_with(Some(script.inline())).await?;
            let sandbox_id = sandbox(&fixture, lifecycle_state, &["access"]).await?;
            let key = Uuid::now_v7().to_string();
            let first = lifecycle(&fixture, sandbox_id, action, &key).await?;
            wait_for_state(&fixture, first, "succeeded").await?;
            assert_eq!(lifecycle(&fixture, sandbox_id, action, &key).await?, first);
            assert_eq!(queue(&fixture, sandbox_id).await?, vec![], "{action}");
            let recorded: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM public_operations WHERE target_id = $1"
            ))
            .bind(sandbox_id.to_string())
            .fetch_one(&fixture.db.pool)
            .await?;
            assert_eq!(recorded, 1, "{action}");
            assert_eq!(script.executed(), vec![first], "{action}");
        }
        Ok(())
    }
}
