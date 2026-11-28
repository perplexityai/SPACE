use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_test_env::TestResult;

use super::*;

fn new_session<'a>(
    session_id: &'a str,
    sandbox_id: SandboxId,
    scopes: &'a [String],
    adapter_state: &'a serde_json::Value,
) -> NewSession<'a> {
    let now = OffsetDateTime::now_utc();
    NewSession {
        session_id,
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        actor_user_id: Some(UserId::from_uuid(SEED_USER_ID)),
        sandbox_id,
        kind: "process",
        state: "pending",
        scopes,
        token_hash: "token-hash",
        token_expires_at: now + time::Duration::minutes(5),
        attach_deadline_at: now + time::Duration::minutes(1),
        target_port: None,
        target_protocol: None,
        target_path: None,
        adapter_state,
        expires_at: now + time::Duration::hours(24),
    }
}

async fn bind_process(
    pool: &sqlx::PgPool,
    session_id: &str,
    process_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET backend_process_id = $2
         WHERE session_id = $1"
    ))
    .bind(session_id)
    .bind(process_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn inserted_sessions_read_back_with_their_process_bindings() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    let other_sandbox =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    let scopes = vec!["process:connect".to_string()];
    let adapter_state = serde_json::json!({"command": ["echo", "hello"]});

    let inserted = insert_session(
        &db.pool,
        &new_session("dps_bound", sandbox_id, &scopes, &adapter_state),
    )
    .await?;
    assert_eq!(inserted.session_id, "dps_bound");
    assert_eq!(inserted.sandbox_id, sandbox_id);
    assert_eq!(inserted.kind, "process");
    assert_eq!(inserted.state, "pending");
    assert_eq!(inserted.reconnect_count, 0);
    assert_eq!(inserted.scopes, scopes);
    insert_session(
        &db.pool,
        &new_session("dps_unbound", sandbox_id, &scopes, &adapter_state),
    )
    .await?;
    insert_session(
        &db.pool,
        &new_session("dps_elsewhere", other_sandbox, &scopes, &adapter_state),
    )
    .await?;

    assert_eq!(
        bound_process_id(&db.pool, org_id, sandbox_id, "dps_bound").await?,
        None
    );
    bind_process(&db.pool, "dps_bound", "proc-1").await?;
    bind_process(&db.pool, "dps_elsewhere", "proc-1").await?;
    assert_eq!(
        bound_process_id(&db.pool, org_id, sandbox_id, "dps_bound")
            .await?
            .as_deref(),
        Some("proc-1")
    );
    assert_eq!(
        bound_process_id(&db.pool, org_id, sandbox_id, "dps_missing").await?,
        None
    );
    assert_eq!(
        bound_process_id(&db.pool, org_id, other_sandbox, "dps_bound").await?,
        None
    );

    let bound = sessions_bound_to_processes(
        &db.pool,
        org_id,
        sandbox_id,
        &["proc-1".to_string(), "proc-2".to_string()],
    )
    .await?;
    assert_eq!(
        bound,
        vec![SessionProcessBinding {
            session_id: "dps_bound".to_string(),
            backend_process_id: "proc-1".to_string(),
        }]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn org_revocation_epoch_starts_at_one_and_advances() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    assert_eq!(org_revocation_epoch(&db.pool, org_id).await?, None);

    advance_org_revocation_epoch(&db.pool, org_id).await?;
    let first = org_revocation_epoch(&db.pool, org_id)
        .await?
        .expect("epoch row");
    assert_eq!(first.epoch, 1);

    advance_org_revocation_epoch(&db.pool, org_id).await?;
    let second = org_revocation_epoch(&db.pool, org_id)
        .await?
        .expect("epoch row");
    assert_eq!(second.epoch, 2);
    Ok(())
}

async fn insert(
    pool: &sqlx::PgPool,
    session_id: &str,
    sandbox_id: SandboxId,
    kind: &str,
) -> Result<Session, sqlx::Error> {
    let scopes = vec!["process:connect".to_string()];
    let adapter_state = serde_json::json!({});
    insert_session(
        pool,
        &NewSession {
            kind,
            ..new_session(session_id, sandbox_id, &scopes, &adapter_state)
        },
    )
    .await
    .map_err(sqlx::Error::from)
}

async fn expire(pool: &sqlx::PgPool, session_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET expires_at = NOW() - INTERVAL '1 minute'
         WHERE session_id = $1"
    ))
    .bind(session_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Columns that show whether a write touched the row: each verb that updates
/// a session sets `updated_at`, so a write that got through changes it.
#[derive(Debug, PartialEq, Eq, FromRow)]
struct WritableColumns {
    state: String,
    revocation_status: String,
    revocation_epoch: i64,
    revocation_reason: Option<String>,
    token_hash: Option<String>,
    backend_process_id: Option<String>,
    reconnect_count: i64,
    updated_at: OffsetDateTime,
}

async fn writable_columns(
    pool: &sqlx::PgPool,
    session_id: &str,
) -> Result<WritableColumns, sqlx::Error> {
    sqlx::query_as::<_, WritableColumns>(sql!(
        "SELECT state,
                revocation_status,
                revocation_epoch,
                revocation_reason,
                token_hash,
                backend_process_id,
                reconnect_count,
                updated_at
         FROM data_plane_sessions
         WHERE session_id = $1"
    ))
    .bind(session_id)
    .fetch_one(pool)
    .await
}

/// Catches a read or list that returns expired sessions, a list that ignores
/// its kind, cursor or limit, or one ordered oldest first.
#[tokio::test(flavor = "current_thread")]
async fn live_reads_skip_expired_sessions() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    let first = insert(&db.pool, "dps_first", sandbox_id, "process").await?;
    insert(&db.pool, "dps_expired", sandbox_id, "process").await?;
    expire(&db.pool, "dps_expired").await?;
    let terminal = insert(&db.pool, "dps_terminal", sandbox_id, "terminal").await?;
    let last = insert(&db.pool, "dps_last", sandbox_id, "process").await?;

    assert_eq!(
        live_session(&db.pool, org_id, sandbox_id, "dps_first").await?,
        Some(first.clone())
    );
    assert_eq!(
        live_session(&db.pool, org_id, sandbox_id, "dps_expired").await?,
        None
    );

    let ids = |sessions: Vec<Session>| -> Vec<String> {
        sessions
            .into_iter()
            .map(|session| session.session_id)
            .collect()
    };
    assert_eq!(
        ids(list_live_sessions(&db.pool, org_id, sandbox_id, None, None, 10).await?),
        ["dps_last", "dps_terminal", "dps_first"]
    );
    assert_eq!(
        ids(list_live_sessions(&db.pool, org_id, sandbox_id, Some("process"), None, 10).await?),
        ["dps_last", "dps_first"]
    );
    let after_last = SessionCursor {
        created_at: last.created_at,
        session_id: &last.session_id,
    };
    assert_eq!(
        ids(list_live_sessions(&db.pool, org_id, sandbox_id, None, Some(after_last), 1).await?),
        [terminal.session_id]
    );
    Ok(())
}

/// Catches a revoke that advances the epoch other than when the caller asks,
/// replaces the first reason, or leaves the token in place.
#[tokio::test(flavor = "current_thread")]
async fn revoke_clears_the_token_and_advances_the_epoch_only_when_asked() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_revoked", sandbox_id, "process").await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(
        lock_live_revocation_status(&mut *tx, org_id, sandbox_id, "dps_revoked")
            .await?
            .as_deref(),
        Some("active")
    );
    let revoked = revoke_live_session(&mut *tx, org_id, sandbox_id, "dps_revoked", "first", true)
        .await?
        .expect("live session revokes");
    tx.commit().await?;
    assert_eq!(revoked.state, "revoked");
    assert_eq!(
        writable_columns(&db.pool, "dps_revoked")
            .await?
            .revocation_epoch,
        1
    );

    let again =
        revoke_live_session(&db.pool, org_id, sandbox_id, "dps_revoked", "second", false).await?;
    assert!(again.is_some());
    let stored = writable_columns(&db.pool, "dps_revoked").await?;
    assert_eq!(stored.revocation_status, "revoked");
    assert_eq!(stored.revocation_epoch, 1);
    assert_eq!(stored.revocation_reason.as_deref(), Some("first"));
    assert_eq!(stored.token_hash, None);

    revoke_live_session(&db.pool, org_id, sandbox_id, "dps_revoked", "third", true)
        .await?
        .expect("a revoked live session still matches");
    let stored = writable_columns(&db.pool, "dps_revoked").await?;
    assert_eq!(stored.revocation_epoch, 2);
    assert_eq!(stored.revocation_reason.as_deref(), Some("first"));
    Ok(())
}

/// The losing case: an expired session does not revoke, and nothing changes.
#[tokio::test(flavor = "current_thread")]
async fn revoke_leaves_an_expired_session_alone() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_expired", sandbox_id, "process").await?;
    expire(&db.pool, "dps_expired").await?;
    let before = writable_columns(&db.pool, "dps_expired").await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(
        lock_live_revocation_status(&mut *tx, org_id, sandbox_id, "dps_expired").await?,
        None
    );
    tx.commit().await?;
    assert_eq!(
        revoke_live_session(&db.pool, org_id, sandbox_id, "dps_expired", "late", true).await?,
        None
    );
    assert_eq!(writable_columns(&db.pool, "dps_expired").await?, before);
    Ok(())
}

/// The losing case: a revoked session gets no new attach token.
#[tokio::test(flavor = "current_thread")]
async fn attach_token_refreshes_only_while_the_session_is_active() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_token", sandbox_id, "process").await?;
    let later = OffsetDateTime::now_utc() + time::Duration::minutes(5);

    assert!(refresh_live_attach_token(&db.pool, "dps_token", "fresh-hash", later, later).await?);
    assert_eq!(
        writable_columns(&db.pool, "dps_token")
            .await?
            .token_hash
            .as_deref(),
        Some("fresh-hash")
    );

    revoke_live_session(&db.pool, org_id, sandbox_id, "dps_token", "revoked", true).await?;
    let before = writable_columns(&db.pool, "dps_token").await?;
    assert!(!refresh_live_attach_token(&db.pool, "dps_token", "late-hash", later, later).await?);
    assert!(!refresh_live_attach_token(&db.pool, "dps_missing", "late-hash", later, later).await?);
    assert_eq!(writable_columns(&db.pool, "dps_token").await?, before);
    Ok(())
}

/// Catches a bind that rebinds a session to another process, opens a
/// session when the caller does not record exits, or reports a repeat as a
/// new binding. The losing case: a session bound to another process, or of
/// another kind, does not bind and nothing changes.
#[tokio::test(flavor = "current_thread")]
async fn a_session_binds_one_process_once() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_quiet", sandbox_id, "process").await?;
    insert(&db.pool, "dps_exits", sandbox_id, "terminal").await?;
    insert(&db.pool, "dps_port", sandbox_id, "port").await?;

    let bound = bind_session_process(&db.pool, "dps_quiet", None, "proc-1", false)
        .await?
        .expect("unbound session binds");
    assert_eq!(
        bound,
        BoundSessionProcess {
            session_id: "dps_quiet".to_string(),
            protocol_session_id: None,
            backend_process_id: "proc-1".to_string(),
            newly_bound: true,
        }
    );
    assert_eq!(
        writable_columns(&db.pool, "dps_quiet").await?.state,
        "pending"
    );
    let repeat = bind_session_process(&db.pool, "dps_quiet", None, "proc-1", false)
        .await?
        .expect("rebinding the same process succeeds");
    assert!(!repeat.newly_bound);

    bind_session_process(&db.pool, "dps_exits", None, "proc-2", true)
        .await?
        .expect("unbound session binds");
    assert_eq!(writable_columns(&db.pool, "dps_exits").await?.state, "open");

    let before = writable_columns(&db.pool, "dps_quiet").await?;
    assert_eq!(
        bind_session_process(&db.pool, "dps_quiet", None, "proc-other", true).await?,
        None
    );
    assert_eq!(writable_columns(&db.pool, "dps_quiet").await?, before);
    assert_eq!(
        live_process_session_bound_process_id(&db.pool, "dps_quiet", None)
            .await?
            .as_deref(),
        Some("proc-1")
    );
    assert_eq!(
        bind_session_process(
            &db.pool,
            "dps_quiet",
            Some("other-protocol"),
            "proc-1",
            true
        )
        .await?,
        None
    );

    let before = writable_columns(&db.pool, "dps_port").await?;
    assert_eq!(
        bind_session_process(&db.pool, "dps_port", None, "proc-3", true).await?,
        None
    );
    assert_eq!(writable_columns(&db.pool, "dps_port").await?, before);
    assert_eq!(
        live_process_session_bound_process_id(&db.pool, "dps_port", None).await?,
        None
    );
    Ok(())
}

/// The losing cases: a report about another process, a repeated report, and
/// a report for a revoked session close nothing and change nothing.
#[tokio::test(flavor = "current_thread")]
async fn only_the_bound_process_exiting_closes_a_live_session() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_closes", sandbox_id, "process").await?;
    insert(&db.pool, "dps_revoked", sandbox_id, "process").await?;
    bind_session_process(&db.pool, "dps_closes", None, "proc-1", true).await?;
    bind_session_process(&db.pool, "dps_revoked", None, "proc-2", true).await?;
    revoke_live_session(&db.pool, org_id, sandbox_id, "dps_revoked", "revoked", true).await?;

    let before = writable_columns(&db.pool, "dps_closes").await?;
    assert!(!close_exited_process_session(&db.pool, "dps_closes", None, "proc-other").await?);
    assert_eq!(writable_columns(&db.pool, "dps_closes").await?, before);

    assert!(close_exited_process_session(&db.pool, "dps_closes", None, "proc-1").await?);
    let closed = writable_columns(&db.pool, "dps_closes").await?;
    assert_eq!(closed.state, "closed");
    assert!(!close_exited_process_session(&db.pool, "dps_closes", None, "proc-1").await?);
    assert_eq!(writable_columns(&db.pool, "dps_closes").await?, closed);

    let before = writable_columns(&db.pool, "dps_revoked").await?;
    assert!(!close_exited_process_session(&db.pool, "dps_revoked", None, "proc-2").await?);
    assert_eq!(writable_columns(&db.pool, "dps_revoked").await?, before);
    Ok(())
}

/// Catches a reconnect that does not count, keeps the old reason, or counts
/// on an expired session.
#[tokio::test(flavor = "current_thread")]
async fn reconnects_count_on_live_sessions() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_live", sandbox_id, "process").await?;
    insert(&db.pool, "dps_expired", sandbox_id, "process").await?;
    expire(&db.pool, "dps_expired").await?;

    record_reconnect(&db.pool, "dps_live", None, "backend_restart").await?;
    let second = record_reconnect(&db.pool, "dps_live", None, "data_plane_restart")
        .await?
        .expect("live session reconnects");
    assert_eq!(
        second,
        SessionReconnect {
            session_id: "dps_live".to_string(),
            protocol_session_id: None,
            reconnect_count: 2,
            last_reconnect_reason: "data_plane_restart".to_string(),
        }
    );

    let before = writable_columns(&db.pool, "dps_expired").await?;
    assert_eq!(
        record_reconnect(&db.pool, "dps_expired", None, "backend_restart").await?,
        None
    );
    assert_eq!(writable_columns(&db.pool, "dps_expired").await?, before);
    Ok(())
}

/// Catches the revocation read missing its org's epoch, or reading an
/// expired session.
#[tokio::test(flavor = "current_thread")]
async fn revocation_state_reads_the_org_epoch_alongside() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_state", sandbox_id, "process").await?;
    insert(&db.pool, "dps_expired", sandbox_id, "process").await?;
    expire(&db.pool, "dps_expired").await?;

    let active = live_session_revocation(&db.pool, "dps_state", None)
        .await?
        .expect("live session");
    assert_eq!(active.org_id, org_id);
    assert_eq!(active.revocation_status, "active");
    assert_eq!(active.revocation_epoch, 0);
    assert_eq!(active.org_revocation_epoch, 0);

    revoke_live_session(&db.pool, org_id, sandbox_id, "dps_state", "gone", true).await?;
    advance_org_revocation_epoch(&db.pool, org_id).await?;
    let revoked = live_session_revocation(&db.pool, "dps_state", None)
        .await?
        .expect("live session");
    assert_eq!(revoked.revocation_status, "revoked");
    assert_eq!(revoked.revocation_reason.as_deref(), Some("gone"));
    assert_eq!(revoked.revocation_epoch, 1);
    assert_eq!(revoked.org_revocation_epoch, 1);

    assert_eq!(
        live_session_revocation(&db.pool, "dps_expired", None).await?,
        None
    );
    assert_eq!(
        live_session_revocation(&db.pool, "dps_state", Some("other-protocol")).await?,
        None
    );
    Ok(())
}

/// A guest that predates assigned process ids binds a session to an id it
/// minted, and the process routes find the process through that row for as
/// long as it runs, however long ago its session expired or ended. The sweep
/// keeps such a row until its sandbox is deleted, and prunes every other
/// expired or ended row as before.
#[tokio::test(flavor = "current_thread")]
async fn sweep_keeps_a_binding_to_a_guest_minted_process_id_until_its_sandbox_goes() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    let unbound = "proc_00000000-0000-7000-8000-000000000001";
    let self_bound = "proc_00000000-0000-7000-8000-000000000002";
    let guest_bound = "proc_00000000-0000-7000-8000-000000000003";
    let ended_guest_bound = "proc_00000000-0000-7000-8000-000000000004";
    let live = "proc_00000000-0000-7000-8000-000000000005";
    // (session, state, expired, bound process id, kept)
    let rows = [
        (unbound, "open", true, None, false),
        (self_bound, "open", true, Some(self_bound), false),
        (guest_bound, "open", true, Some("guest-minted-1"), true),
        (
            ended_guest_bound,
            "revoked",
            false,
            Some("guest-minted-2"),
            true,
        ),
        (live, "open", false, None, true),
    ];
    for (session_id, state, expired, process_id, _) in rows {
        sqlx::query(sql!(
            "INSERT INTO data_plane_sessions (
                 session_id, org_id, sandbox_id, kind, state, expires_at, updated_at,
                 backend_process_id
             )
             SELECT $1, org_id, id, 'process', $2,
                    NOW() + CASE WHEN $3 THEN INTERVAL '-2 hours' ELSE INTERVAL '20 hours' END,
                    NOW() - INTERVAL '2 hours',
                    $4
               FROM sandboxes
              WHERE id = $5"
        ))
        .bind(session_id)
        .bind(state)
        .bind(expired)
        .bind(process_id)
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    }
    let deleted = delete_ended_sessions(pool, 3600, 100).await?;

    let expected: Vec<&str> = rows.iter().filter(|row| row.4).map(|row| row.0).collect();
    assert_eq!(remaining_sessions(pool).await?, expected);
    assert_eq!(deleted, rows.len() - expected.len());

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    assert_eq!(remaining_sessions(pool).await?, Vec::<String>::new());
    Ok(())
}

/// A closed terminal or process session is a command that exited, which a
/// client may still read, replay or reattach to for the exit, so the sweep
/// keeps it until it expires. A closed session of another kind, and a
/// revoked one of any kind, go once the grace has passed.
#[tokio::test(flavor = "current_thread")]
async fn sweep_keeps_a_closed_command_session_until_it_expires() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    // (session, kind, state, expired, kept)
    let rows = [
        (
            "proc_00000000-0000-7000-8000-000000000011",
            "process",
            "closed",
            false,
            true,
        ),
        (
            "term_00000000-0000-7000-8000-000000000012",
            "terminal",
            "closed",
            false,
            true,
        ),
        (
            "proc_00000000-0000-7000-8000-000000000013",
            "process",
            "closed",
            true,
            false,
        ),
        (
            "proc_00000000-0000-7000-8000-000000000014",
            "process",
            "revoked",
            false,
            false,
        ),
        (
            "port_00000000-0000-7000-8000-000000000015",
            "port",
            "closed",
            false,
            false,
        ),
    ];
    for (session_id, kind, state, expired, _) in rows {
        sqlx::query(sql!(
            "INSERT INTO data_plane_sessions (
                 session_id, org_id, sandbox_id, kind, state, expires_at, updated_at,
                 backend_process_id
             )
             SELECT $1, org_id, id, $2, $3,
                    NOW() + CASE WHEN $4 THEN INTERVAL '-2 hours' ELSE INTERVAL '20 hours' END,
                    NOW() - INTERVAL '2 hours',
                    CASE WHEN $2 = 'port' THEN NULL ELSE $1 END
               FROM sandboxes
              WHERE id = $5"
        ))
        .bind(session_id)
        .bind(kind)
        .bind(state)
        .bind(expired)
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    }
    let deleted = delete_ended_sessions(pool, 3600, 100).await?;

    let mut expected: Vec<&str> = rows.iter().filter(|row| row.4).map(|row| row.0).collect();
    expected.sort_unstable();
    assert_eq!(remaining_sessions(pool).await?, expected);
    assert_eq!(deleted, rows.len() - expected.len());
    Ok(())
}

async fn remaining_sessions(pool: &sqlx::PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT session_id FROM data_plane_sessions ORDER BY session_id"
    ))
    .fetch_all(pool)
    .await
}
