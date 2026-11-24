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
