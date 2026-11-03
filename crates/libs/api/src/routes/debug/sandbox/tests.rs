use super::*;
use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

/// `GET /admin/api/v1/debug/sandbox/{id}` takes the lock wait as a retryable
/// 503 and never takes the write conflict.
///
/// This is the surface an operator reaches for while a sandbox is wedged, so
/// answering a lock wait with "Internal server error" is worst exactly when it
/// matters: the point of the read is to find out what is stuck. Two statements
/// could produce it — the authz preflight on `clusters` and the lifecycle read
/// that gates the whole response — and both went through arms that discarded
/// the error entirely (`Err(_) =>`) or logged it and returned 500.
///
/// Only what this wrapper adds is asserted: that it pins the scope to
/// `ReadOnly` — so a serialization abort takes the generic retryable 503, not
/// the write-specific 409 — and shapes a `Response`. The SQLSTATE table lives
/// once in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`,
/// and the handler arms that call this wrapper are covered by
/// `the_debug_handler_answers_a_contended_read_with_a_503` below — this one
/// alone would stay green if either arm dropped the call.
#[tokio::test]
async fn a_sandbox_debug_read_pins_the_scope_to_read_only()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    let lock_wait = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await;
    assert_eq!(
        contention_response(&lock_wait).map(|response| response.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "a lock wait on the debug read is retryable, not internal"
    );

    let deadlock = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await;
    assert_eq!(
        contention_response(&deadlock).map(|response| response.status()),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "a debug read never takes the write-specific 409, but the retry succeeds"
    );

    // Outside the contention set: this one really is a 500.
    let violation = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert!(contention_response(&violation).is_none());

    // Closed like the sibling `raise_sqlstate` tests: each injection fails a
    // statement on the connection it borrows.
    db.pool.close().await;
    Ok(())
}

/// Both of the handler's database arms are actually wired to the disposition.
///
/// The test above asserts `contention_response` in isolation, which survives a
/// handler arm being changed or dropped — an independent review proved exactly
/// that by reverting both arms and watching it stay green. So this one drives
/// `get_sandbox_debug` itself, once per arm, against a real held lock:
/// `clusters` for the authz preflight and `sandboxes` for the lifecycle read
/// that gates the whole response. Same technique and same reason as
/// `controllers::sandbox::lifecycle::tests::a_contended_delete_precheck_stays_retryable`.
#[tokio::test]
async fn the_debug_handler_answers_a_contended_read_with_a_503()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use pplx_space_db_test_fixtures::SEED_ORG_ID;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;

    // The role-level bound, so a blocked read aborts instead of waiting for the
    // lock to be released.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;
    let state = crate::routes::AppState::from_pool(bounded)
        .await
        .expect("app state from the test pool");

    // An org_admin of the managing org, so the gate reaches its `clusters`
    // read rather than short-circuiting on the role check.
    let actor = User {
        id: UserId::new(),
        email: "contention-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "contention probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    // `ACCESS EXCLUSIVE` is what DDL takes; it conflicts with each read's
    // `ACCESS SHARE`, so the read blocks and then aborts with `55P03`.
    for table in ["clusters", "sandboxes"] {
        let mut blocker = db.pool.begin().await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
        )))
        .execute(&mut *blocker)
        .await?;

        let response = get_sandbox_debug(
            State(state.clone()),
            axum::Extension(crate::middleware::auth::AuthUser(actor.clone())),
            Path(SandboxId::new().to_string()),
        )
        .await;

        blocker.rollback().await?;

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a debug read blocked behind DDL on {table} is retryable, not internal"
        );
    }

    db.pool.close().await;
    Ok(())
}

/// The debug read carries the quarantine section and each snapshot's storage
/// placement, and a sandbox with neither reads as not quarantined with empty
/// lists. The placement and quarantine rules are the stores' own tests; this
/// pins that the route attaches them.
#[tokio::test]
async fn the_debug_read_reports_quarantine_and_snapshot_storage()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use axum::extract::{Path, State};
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::NodeId;
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let state = crate::routes::AppState::from_pool(db.pool.clone())
        .await
        .expect("app state from the test pool");
    let actor = User {
        id: UserId::new(),
        email: "debug-read@example.test".to_string(),
        password_hash: String::new(),
        display_name: "debug read".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let artifact = uuid::Uuid::now_v7();
    let plain = SandboxId::new();
    let quarantined = SandboxId::new();
    for (sandbox_id, artifact) in [(plain, None), (quarantined, Some(artifact))] {
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation, lifecycle,
                                    placement, active_node_id, current_local_artifact_id,
                                    current_local_artifact_kind, current_local_artifact_origin)
             VALUES ($1, $2, $3, 1, 'quarantined', 'placed', $4, $5,
                     CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'full' END,
                     CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'suspend' END)"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(node_id)
        .bind(artifact)
        .execute(&db.pool)
        .await?;
    }
    let quarantine_id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state
             (sandbox_id, quarantine_id, source, reason_code, next_action)
         VALUES ($1, $2, 'sbx_manager', 'pause_failed', 'requires_operator_attention')"
    ))
    .bind(quarantined)
    .bind(quarantine_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history
             (id, quarantine_id, sandbox_id, source, action, reason_code)
         VALUES ($1, $2, $3, 'sbx_manager', 'entered_quarantine', 'pause_failed')"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(quarantine_id)
    .bind(quarantined)
    .execute(&db.pool)
    .await?;
    let snapshot_id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, org_id, local_artifact_id,
                                artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'ready', $3, $4, 'full', 'suspend')"
    ))
    .bind(snapshot_id)
    .bind(quarantined)
    .bind(SEED_ORG_ID)
    .bind(artifact)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://synthetic-bucket/snapshots/x', $2)"
    ))
    .bind(snapshot_id)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id) VALUES ($1, $2)"
    ))
    .bind(quarantined)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_snapshot_inventory (sandbox_id, snapshot_id, type, state)
         VALUES ($1, 'f-synthetic', 'full', 'ready')"
    ))
    .bind(quarantined)
    .execute(&db.pool)
    .await?;

    let read = |sandbox_id: SandboxId| {
        let state = state.clone();
        let actor = actor.clone();
        async move {
            let response = get_sandbox_debug(
                State(state),
                axum::Extension(crate::middleware::auth::AuthUser(actor)),
                Path(pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), 1_000_000).await?;
            pplx_space_db_test_fixtures::TestDbResult::Ok(serde_json::from_slice::<
                serde_json::Value,
            >(&body)?)
        }
    };

    let body = read(plain).await?;
    assert_eq!(
        body["quarantine"],
        serde_json::json!({"quarantined": false, "active": null, "history": []})
    );
    assert_eq!(body["snapshots"]["snapshots"], serde_json::json!([]));

    let body = read(quarantined).await?;
    assert_eq!(body["quarantine"]["quarantined"], true);
    assert_eq!(
        body["quarantine"]["active"]["quarantine_id"],
        quarantine_id.to_string()
    );
    assert_eq!(body["quarantine"]["active"]["reason_code"], "pause_failed");
    assert_eq!(
        body["quarantine"]["history"][0]["action"],
        "entered_quarantine"
    );
    // A quarantined sandbox is not settled on its node, so only the object
    // store copy is reported (the placement rule is the store's own test).
    assert_eq!(
        body["snapshots"]["snapshots"][0]["storage"],
        serde_json::json!({"location": "object_store", "node_id": null})
    );
    assert_eq!(body["snapshots"].get("storage_error"), None);
    assert_eq!(
        body["snapshots"]["inventory"][0]["snapshot_id"],
        "f-synthetic"
    );
    Ok(())
}
