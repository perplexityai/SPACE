use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_db_test_fixtures::try_pg_test_db;

use super::*;

use pplx_space_test_env::TestResult;

#[test]
fn events_url_is_only_exposed_for_event_stream_sessions() {
    let process_id = DataPlaneSessionId::Process(Default::default());
    assert_eq!(
        events_url("https://dp.example/", process_id, SessionKind::Process),
        Some(format!(
            "https://dp.example/v1/sessions/{process_id}/events"
        ))
    );

    let file_watch_id = DataPlaneSessionId::FileWatch(Default::default());
    assert_eq!(
        events_url("", file_watch_id, SessionKind::FileWatch),
        Some(format!("/v1/sessions/{file_watch_id}/events"))
    );

    let terminal_id = DataPlaneSessionId::Terminal(Default::default());
    assert_eq!(
        events_url("https://dp.example", terminal_id, SessionKind::Terminal),
        None
    );
}

#[tokio::test]
async fn create_terminal_session_persists_scoped_token_and_operation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"command": [], "env": {}});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Terminal,
        None,
        None,
        None,
        &request,
    );
    let key = IdempotencyKey::parse("terminal-session-1")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "https://dp.example");

    let session = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Terminal,
            target_port: None,
            target_protocol: None,
            target_path: None,
            adapter_state: request,
            fingerprint_payload: &fingerprint,
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/terminal-sessions",
                ),
                key: &key,
            },
        })
        .await?;

    assert_eq!(session.kind, SessionKind::Terminal);
    assert_eq!(session.sandbox_id, public_sandbox_id);
    assert_eq!(session.scopes, vec![DataPlaneScope::TerminalConnect]);
    assert_eq!(session.reconnect_count, 0);
    assert_eq!(session.last_reconnect_reason, None);
    let attach = session.attach.expect("create returns attach token");
    assert!(attach.token.starts_with(DATA_PLANE_TOKEN_PREFIX));
    assert_eq!(
        attach.url,
        format!("https://dp.example/v1/sessions/{}/attach", session.id)
    );
    assert_eq!(attach.events_url, None);
    assert_eq!(
        attach.token_transport,
        vec![
            DataPlaneTokenTransport::FirstFrame,
            DataPlaneTokenTransport::SecWebSocketProtocol,
            DataPlaneTokenTransport::AuthorizationHeader,
        ]
    );

    let stored: (String, Vec<String>, i64) = sqlx::query_as(sql!(
        "SELECT token_hash, scopes, COUNT(o.operation_id)
         FROM data_plane_sessions d
         JOIN public_operations o ON o.target_id = d.session_id
         WHERE d.session_id = $1
         GROUP BY d.token_hash, d.scopes"
    ))
    .bind(session.id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_ne!(stored.0, attach.token);
    assert_eq!(stored.1, vec!["terminal:connect".to_string()]);
    assert_eq!(stored.2, 1);

    let operation_id = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id
         FROM public_operations
         WHERE org_id = $1 AND target_id = $2"
    ))
    .bind(org_id)
    .bind(session.id.to_string())
    .fetch_one(&db.pool)
    .await?;
    let operation = crate::public_operations::PublicOperationStore::new(db.pool.clone())
        .fetch_operation(
            OrgId::from_uuid(org_id),
            pplx_space_public_api::OperationId::from_uuid(operation_id),
        )
        .await?
        .expect("session operation should be tenant-visible");
    assert_eq!(
        operation.result,
        Some(PublicOperationResult::Session(
            PublicSessionOperationResult {
                session_id: session.id,
                sandbox_id: public_sandbox_id,
                kind: SessionKind::Terminal,
            }
        ))
    );

    let fetched = store
        .fetch_session(OrgId::from_uuid(org_id), public_sandbox_id, session.id)
        .await?
        .expect("created session should be fetchable");
    assert_eq!(fetched.id, session.id);
    assert_eq!(fetched.reconnect_count, 0);
    assert_eq!(fetched.last_reconnect_reason, None);
    assert!(fetched.attach.is_none());
    Ok(())
}

#[tokio::test]
async fn idempotency_replay_reuses_session_and_mints_new_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"protocol": "cdp", "port": 9222});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Protocol,
        Some(9222),
        Some(SessionTargetProtocol::Cdp),
        None,
        &request,
    );
    let key = IdempotencyKey::parse("protocol-session-1")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");

    let input = CreatePublicSession {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        sandbox_id: public_sandbox_id,
        kind: SessionKind::Protocol,
        target_port: Some(9222),
        target_protocol: Some(SessionTargetProtocol::Cdp),
        target_path: None,
        adapter_state: request,
        fingerprint_payload: &fingerprint,
        idempotency: CreatePublicSessionIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(
                "/v1/sandboxes/{sandbox_id}/protocol-sessions",
            ),
            key: &key,
        },
    };

    let first = store.create_session(input.clone()).await?;
    let second = store.create_session(input).await?;
    assert_eq!(first.id, second.id);
    assert_ne!(
        first.attach.as_ref().expect("first token").token,
        second.attach.as_ref().expect("second token").token,
    );
    assert_eq!(second.target_protocol, Some(SessionTargetProtocol::Cdp));
    assert_eq!(second.target_port, Some(9222));
    assert_eq!(second.scopes, vec![DataPlaneScope::ProtocolConnect]);
    let attach = second.attach.expect("replay returns attach token");
    assert_eq!(attach.url, format!("/v1/sessions/{}/proxy", second.id));
    assert_eq!(attach.events_url, None);
    assert_eq!(
        attach.token_transport,
        vec![
            DataPlaneTokenTransport::AuthorizationHeader,
            DataPlaneTokenTransport::SecWebSocketProtocol,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn create_session_reuses_key_after_idempotency_retention_expires() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"command": [], "env": {}});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Terminal,
        None,
        None,
        None,
        &request,
    );
    let key = IdempotencyKey::parse("expired-terminal-session-key")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");
    let input = CreatePublicSession {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        sandbox_id: public_sandbox_id,
        kind: SessionKind::Terminal,
        target_port: None,
        target_protocol: None,
        target_path: None,
        adapter_state: request,
        fingerprint_payload: &fingerprint,
        idempotency: CreatePublicSessionIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(
                "/v1/sandboxes/{sandbox_id}/terminal-sessions",
            ),
            key: &key,
        },
    };

    let first = store.create_session(input.clone()).await?;
    sqlx::query(sql!(
        "UPDATE public_idempotency_records
         SET expires_at = NOW() - INTERVAL '1 second'
         WHERE org_id = $1 AND idempotency_key = $2"
    ))
    .bind(org_id)
    .bind(key.as_str())
    .execute(&db.pool)
    .await?;

    let second = store.create_session(input).await?;
    assert_ne!(second.id, first.id);
    let session_count = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
         FROM data_plane_sessions
         WHERE org_id = $1 AND sandbox_id = $2"
    ))
    .bind(org_id)
    .bind(public_sandbox_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(session_count, 2);
    Ok(())
}

/// Keep `sqlx::query` events enabled for the whole test process.
///
/// Scoped subscribers on other test threads come and go concurrently, and a
/// callsite-interest rebuild racing ours can compute the global max level (and
/// cached interest) from a dispatcher set that no longer includes ours, which
/// silently drops every event we meant to count. One process-lifetime
/// dispatcher that accepts everything keeps each rebuild's answer "enabled";
/// events still reach only the subscriber in scope on this thread.
fn keep_query_events_enabled() {
    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
    tracing::callsite::rebuild_interest_cache();
}

/// The session and its `sql!` locators (or the leading keyword, such as
/// `COMMIT`) of every statement one `create_session` sends, in order.
async fn create_statements(
    store: &PublicSessionStore,
    input: CreatePublicSession<'_>,
) -> Result<(PublicSession, Vec<String>), PublicSessionStoreError> {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    let session = {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        keep_query_events_enabled();
        store.create_session(input).await?
    };
    let statements = events
        .events()
        .iter()
        .map(|event| {
            let summary = field_value(event, "summary").unwrap_or_default();
            let statement = field_value(event, "db.statement").unwrap_or_default();
            let sql = if statement.is_empty() {
                summary
            } else {
                statement
            };
            match sql
                .trim_start()
                .strip_prefix("/*")
                .and_then(|rest| rest.split_once("*/"))
            {
                Some((locator, _)) => locator.to_string(),
                None => sql
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_string(),
            }
        })
        .collect();
    Ok((session, statements))
}

#[tokio::test(flavor = "current_thread")]
async fn create_session_sends_one_statement_per_write() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"command": ["echo", "hello"]});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Process,
        None,
        None,
        None,
        &request,
    );
    let key = IdempotencyKey::parse("counted-command-key")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");
    let input = CreatePublicSession {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        sandbox_id: public_sandbox_id,
        kind: SessionKind::Process,
        target_port: None,
        target_protocol: None,
        target_path: None,
        adapter_state: request,
        fingerprint_payload: &fingerprint,
        idempotency: CreatePublicSessionIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new("/v1/sandboxes/{sandbox_id}/commands"),
            key: &key,
        },
    };

    // Lock, lookup (discarding an expired record), session, operation with
    // its event, idempotency record, commit. This was nine before the
    // expired-record delete, the event, and the session re-read were folded.
    let (first, statements) = create_statements(&store, input.clone()).await?;
    assert_eq!(statements.len(), 6, "{statements:#?}");
    assert_eq!(statements.last().map(String::as_str), Some("COMMIT"));
    assert_eq!(first.kind, SessionKind::Process);
    assert_eq!(first.state, SessionState::Pending);
    assert!(first.attach.is_some());
    let fetched = store
        .fetch_session(OrgId::from_uuid(org_id), public_sandbox_id, first.id)
        .await?
        .expect("created session should be fetchable");
    assert_eq!(fetched.created_at, first.created_at);
    assert_eq!(fetched.updated_at, first.updated_at);
    assert_eq!(fetched.expires_at, first.expires_at);

    // The folded event is still the operation's first and only one.
    let operation_id = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT operation_id FROM public_operations WHERE org_id = $1 AND target_id = $2"
    ))
    .bind(org_id)
    .bind(first.id.to_string())
    .fetch_one(&db.pool)
    .await?;
    let events = crate::public_operations::PublicOperationStore::new(db.pool.clone())
        .fetch_operation_events(
            OrgId::from_uuid(org_id),
            pplx_space_public_api::OperationId::from_uuid(operation_id),
            None,
            10,
        )
        .await?
        .expect("session operation should be tenant-visible");
    assert_eq!(events.len(), 1, "{events:#?}");
    let event = events.first().expect("one event");
    assert_eq!(event.sequence, 1);
    assert_eq!(event.event_type, PublicOperationEventType::Completed);
    assert_eq!(
        event.event.pointer("/state"),
        Some(&serde_json::json!("succeeded"))
    );
    assert_eq!(
        event.event.pointer("/result/session_id"),
        Some(&serde_json::json!(first.id.to_string()))
    );

    // A replay reads the session the record points to; it is unchanged.
    let (replayed, statements) = create_statements(&store, input.clone()).await?;
    assert_eq!(replayed.id, first.id);
    assert_eq!(statements.len(), 6, "{statements:#?}");

    // Past retention the key starts a new session in the same six, and its
    // record replaces the expired one rather than colliding with it.
    sqlx::query(sql!(
        "UPDATE public_idempotency_records
         SET expires_at = NOW() - INTERVAL '1 second'
         WHERE org_id = $1 AND idempotency_key = $2"
    ))
    .bind(org_id)
    .bind(key.as_str())
    .execute(&db.pool)
    .await?;
    let (renewed, statements) = create_statements(&store, input).await?;
    assert_ne!(renewed.id, first.id);
    assert_eq!(statements.len(), 6, "{statements:#?}");
    let records: Vec<(Uuid, bool)> = sqlx::query_as(sql!(
        "SELECT r.operation_id, r.expires_at > NOW()
         FROM public_idempotency_records r
         WHERE r.org_id = $1 AND r.idempotency_key = $2"
    ))
    .bind(org_id)
    .bind(key.as_str())
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(records.len(), 1, "{records:#?}");
    assert_ne!(records.first().map(|r| r.0), Some(operation_id));
    assert_eq!(records.first().map(|r| r.1), Some(true));
    Ok(())
}

#[tokio::test]
async fn concurrent_idempotent_create_replays_one_session() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"protocol": "cdp", "port": 9222});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Protocol,
        Some(9222),
        Some(SessionTargetProtocol::Cdp),
        None,
        &request,
    );
    let key = IdempotencyKey::parse("protocol-session-concurrent")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");
    let input = CreatePublicSession {
        org_id: OrgId::from_uuid(org_id),
        actor_user_id: Some(UserId::from_uuid(actor_id)),
        sandbox_id: public_sandbox_id,
        kind: SessionKind::Protocol,
        target_port: Some(9222),
        target_protocol: Some(SessionTargetProtocol::Cdp),
        target_path: None,
        adapter_state: request,
        fingerprint_payload: &fingerprint,
        idempotency: CreatePublicSessionIdempotency {
            method: HttpOperationMethod::Post,
            path_template: OperationPathTemplate::new(
                "/v1/sandboxes/{sandbox_id}/protocol-sessions",
            ),
            key: &key,
        },
    };

    let results =
        futures_util::future::join_all((0..16).map(|_| store.create_session(input.clone()))).await;
    let mut sessions = Vec::new();
    for result in results {
        sessions.push(result?);
    }

    let first_id = sessions[0].id;
    assert!(sessions.iter().all(|session| session.id == first_id));
    let session_count = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
         FROM data_plane_sessions
         WHERE org_id = $1 AND sandbox_id = $2"
    ))
    .bind(org_id)
    .bind(public_sandbox_id.as_uuid())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(session_count, 1);

    let operation_count = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
         FROM public_operations
         WHERE org_id = $1 AND target_id = $2"
    ))
    .bind(org_id)
    .bind(first_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(operation_count, 1);
    Ok(())
}

#[tokio::test]
async fn create_port_session_advertises_proxy_endpoint_and_header_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"port": 8080, "protocol": "http"});
    let fingerprint = session_fingerprint_payload(
        public_sandbox_id,
        SessionKind::Port,
        Some(8080),
        Some(SessionTargetProtocol::Http),
        None,
        &request,
    );
    let key = IdempotencyKey::parse("port-session-proxy-contract")?;
    let store =
        PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "https://dp.example/");

    let session = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Port,
            target_port: Some(8080),
            target_protocol: Some(SessionTargetProtocol::Http),
            target_path: None,
            adapter_state: request,
            fingerprint_payload: &fingerprint,
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/port-sessions",
                ),
                key: &key,
            },
        })
        .await?;

    assert_eq!(session.kind, SessionKind::Port);
    assert_eq!(session.target_port, Some(8080));
    assert_eq!(session.target_protocol, Some(SessionTargetProtocol::Http));
    assert_eq!(session.scopes, vec![DataPlaneScope::PortConnect]);
    let attach = session.attach.expect("create returns attach token");
    assert_eq!(
        attach.url,
        format!("https://dp.example/v1/sessions/{}/proxy", session.id)
    );
    assert_eq!(attach.events_url, None);
    assert_eq!(
        attach.token_transport,
        vec![
            DataPlaneTokenTransport::AuthorizationHeader,
            DataPlaneTokenTransport::SecWebSocketProtocol,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn idempotency_rejects_fingerprint_mismatch() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let first_request = serde_json::json!({"port": 8080, "protocol": "http"});
    let second_request = serde_json::json!({"port": 9090, "protocol": "http"});
    let key = IdempotencyKey::parse("port-session-1")?;
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");

    store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Port,
            target_port: Some(8080),
            target_protocol: Some(SessionTargetProtocol::Http),
            target_path: None,
            adapter_state: first_request.clone(),
            fingerprint_payload: &session_fingerprint_payload(
                public_sandbox_id,
                SessionKind::Port,
                Some(8080),
                Some(SessionTargetProtocol::Http),
                None,
                &first_request,
            ),
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/port-sessions",
                ),
                key: &key,
            },
        })
        .await?;

    let conflict = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Port,
            target_port: Some(9090),
            target_protocol: Some(SessionTargetProtocol::Http),
            target_path: None,
            adapter_state: second_request.clone(),
            fingerprint_payload: &session_fingerprint_payload(
                public_sandbox_id,
                SessionKind::Port,
                Some(9090),
                Some(SessionTargetProtocol::Http),
                None,
                &second_request,
            ),
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/port-sessions",
                ),
                key: &key,
            },
        })
        .await
        .expect_err("mismatched idempotency fingerprint must fail");
    assert!(matches!(
        conflict,
        PublicSessionStoreError::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn list_sessions_filters_by_kind_and_returns_without_attach_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");
    let terminal_key = IdempotencyKey::parse("list-terminal-session")?;

    let terminal_request = serde_json::json!({"command": [], "env": {}});
    let terminal = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Terminal,
            target_port: None,
            target_protocol: None,
            target_path: None,
            adapter_state: terminal_request.clone(),
            fingerprint_payload: &session_fingerprint_payload(
                public_sandbox_id,
                SessionKind::Terminal,
                None,
                None,
                None,
                &terminal_request,
            ),
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/terminal-sessions",
                ),
                key: &terminal_key,
            },
        })
        .await?;

    let port_key = IdempotencyKey::parse("list-port-session")?;
    let port_request = serde_json::json!({"port": 8080, "protocol": "http"});
    let port = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Port,
            target_port: Some(8080),
            target_protocol: Some(SessionTargetProtocol::Http),
            target_path: None,
            adapter_state: port_request.clone(),
            fingerprint_payload: &session_fingerprint_payload(
                public_sandbox_id,
                SessionKind::Port,
                Some(8080),
                Some(SessionTargetProtocol::Http),
                None,
                &port_request,
            ),
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/port-sessions",
                ),
                key: &port_key,
            },
        })
        .await?;

    let all = store
        .list_sessions(ListPublicSessions {
            org_id: OrgId::from_uuid(org_id),
            sandbox_id: public_sandbox_id,
            kind: None,
            cursor: None,
            limit: 10,
        })
        .await?;
    assert_eq!(all.len(), 2);
    assert!(all.iter().all(|session| session.attach.is_none()));
    assert!(all.iter().any(|session| session.id == terminal.id));
    assert!(all.iter().any(|session| session.id == port.id));

    let terminals = store
        .list_sessions(ListPublicSessions {
            org_id: OrgId::from_uuid(org_id),
            sandbox_id: public_sandbox_id,
            kind: Some(SessionKind::Terminal),
            cursor: None,
            limit: 10,
        })
        .await?;
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].id, terminal.id);
    Ok(())
}

#[tokio::test]
async fn revoke_session_marks_revoked_and_is_idempotent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let actor_id = seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(org_id),
        UserId::from_uuid(actor_id),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let request = serde_json::json!({"command": [], "env": {}});
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "");
    let key = IdempotencyKey::parse("revoke-terminal-session")?;
    let session = store
        .create_session(CreatePublicSession {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: Some(UserId::from_uuid(actor_id)),
            sandbox_id: public_sandbox_id,
            kind: SessionKind::Terminal,
            target_port: None,
            target_protocol: None,
            target_path: None,
            adapter_state: request.clone(),
            fingerprint_payload: &session_fingerprint_payload(
                public_sandbox_id,
                SessionKind::Terminal,
                None,
                None,
                None,
                &request,
            ),
            idempotency: CreatePublicSessionIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new(
                    "/v1/sandboxes/{sandbox_id}/terminal-sessions",
                ),
                key: &key,
            },
        })
        .await?;

    let revoked = store
        .revoke_session(RevokePublicSession {
            org_id: OrgId::from_uuid(org_id),
            sandbox_id: public_sandbox_id,
            session_id: session.id,
            reason: "user_requested",
        })
        .await?
        .expect("session should be revoked");
    assert_eq!(revoked.state, SessionState::Revoked);
    assert!(revoked.attach.is_none());

    let first_row: (i64, String, String, Option<String>, Option<OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT revocation_epoch,
                    revocation_status,
                    state,
                    token_hash,
                    attach_deadline_at
             FROM data_plane_sessions
             WHERE session_id = $1"
        ))
        .bind(session.id.to_string())
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(first_row.0, 1);
    assert_eq!(first_row.1, "revoked");
    assert_eq!(first_row.2, "revoked");
    assert!(first_row.3.is_none());
    assert!(first_row.4.is_none());

    let revoked_again = store
        .revoke_session(RevokePublicSession {
            org_id: OrgId::from_uuid(org_id),
            sandbox_id: public_sandbox_id,
            session_id: session.id,
            reason: "user_requested",
        })
        .await?
        .expect("second revoke should return the session");
    assert_eq!(revoked_again.state, SessionState::Revoked);

    let (session_epoch, org_epoch): (i64, i64) = sqlx::query_as(sql!(
        "SELECT d.revocation_epoch,
                COALESCE(o.epoch, 0)
         FROM data_plane_sessions d
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         WHERE d.session_id = $1"
    ))
    .bind(session.id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(session_epoch, 1);
    assert_eq!(org_epoch, 1);
    Ok(())
}

/// End-to-end for the guest user: the value on the request has to survive
/// into the session row the data plane later reads to build the guest spawn.
/// The unit tests cover the two ends; this covers the hand-off between them.
#[tokio::test]
async fn command_session_persists_the_requested_guest_user() -> TestResult {
    use pplx_space_public_api::{CreateCommandRequest, CreateProcessRequest};

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let org = seed_org(&db.pool).await?;
    let user = seed_user(&db.pool, OrgId::from_uuid(org)).await?;
    let sandbox_id = seed_sandbox(&db.pool, OrgId::from_uuid(org), UserId::from_uuid(user)).await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "https://dp.example");

    let request = |guest_user: Option<String>| CreateCommandRequest {
        command: vec!["id".to_string()],
        cwd: None,
        env: Default::default(),
        pty: None,
        stdin: Some(false),
        user: guest_user,
        timeout: None,
    };

    for (guest_user, key) in [
        (Some("root".to_string()), "guest-user-root"),
        (None, "guest-user-default"),
    ] {
        let command = crate::routes::public::sessions::create_command_session(
            &store,
            OrgId::from_uuid(org),
            UserId::from_uuid(user),
            public_sandbox_id,
            request(guest_user.clone()),
            IdempotencyKey::parse(key)?,
        )
        .await
        .expect("command session is created");

        let adapter_state = sqlx::query_scalar::<_, serde_json::Value>(sql!(
            "SELECT adapter_state FROM data_plane_sessions WHERE session_id = $1"
        ))
        .bind(command.session_id.to_string())
        .fetch_one(&db.pool)
        .await?;

        // Absent rather than null when unset: this value feeds the
        // idempotency fingerprint, so a null would change the hash of a
        // request that has not otherwise changed.
        assert_eq!(adapter_state.get("user").is_some(), guest_user.is_some());
        let stored = serde_json::from_value::<CreateProcessRequest>(adapter_state)
            .expect("adapter state is a process request");
        assert_eq!(stored.user, guest_user);
    }

    Ok(())
}

/// The operator console runs commands through `create_command_session`
/// with the org taken from the sandbox row, not the caller's session. This
/// is what lets an admin run a command in a sandbox owned by another org —
/// the org-scoped public path returns "sandbox not found" for exactly this
/// case. Guard that the session and its operation land under the sandbox's
/// org while still being attributed to the operator who issued the command.
#[tokio::test]
async fn command_session_runs_under_sandbox_org_for_cross_org_caller() -> TestResult {
    use pplx_space_public_api::CreateCommandRequest;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    // Sandbox owned by a customer org.
    let owner_org = seed_org(&db.pool).await?;
    let owner_user = seed_user(&db.pool, OrgId::from_uuid(owner_org)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(owner_org),
        UserId::from_uuid(owner_user),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);

    // The operator running the command belongs to a different (managing) org.
    let managing_org = seed_org(&db.pool).await?;
    let managing_admin = seed_user(&db.pool, OrgId::from_uuid(managing_org)).await?;
    assert_ne!(owner_org, managing_org);

    let store = PublicSessionStore::with_data_plane_base_url(db.pool.clone(), "https://dp.example");
    let request = CreateCommandRequest {
        command: vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo hi".to_string(),
        ],
        cwd: None,
        env: Default::default(),
        pty: None,
        stdin: Some(false),
        user: None,
        timeout: None,
    };
    let key = IdempotencyKey::parse("admin-command-cross-org")?;

    let command = crate::routes::public::sessions::create_command_session(
        &store,
        OrgId::from_uuid(owner_org),
        UserId::from_uuid(managing_admin),
        public_sandbox_id,
        request,
        key,
    )
    .await
    .expect("admin command session is created for a cross-org sandbox");

    assert_eq!(command.sandbox_id, public_sandbox_id);
    assert!(
        command.attach.is_some(),
        "command returns a data-plane attach token"
    );

    // The data-plane session is scoped to the sandbox's org, not the caller's.
    let session_org = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT org_id FROM data_plane_sessions WHERE session_id = $1"
    ))
    .bind(command.session_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(session_org, owner_org);
    assert_ne!(session_org, managing_org);

    // The operation is recorded under the sandbox's org and attributed to
    // the operator who issued the command.
    let op_actor = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
        "SELECT actor_user_id FROM public_operations WHERE org_id = $1 AND target_id = $2"
    ))
    .bind(owner_org)
    .bind(command.session_id.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(op_actor, Some(managing_admin));

    Ok(())
}

async fn seed_org(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id,
             name,
             slug,
             byok_enabled
         )
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_user(pool: &PgPool, org_id: OrgId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (
             id,
             email,
             password_hash,
             display_name,
             org_id,
             role
         )
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_sandbox(pool: &PgPool, org_id: OrgId, user_id: UserId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'k8s', 'started', 'placed', 1)"
    ))
    .bind(id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}
