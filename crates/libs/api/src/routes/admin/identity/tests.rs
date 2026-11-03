use super::*;
use axum::body::to_bytes;
use pplx_space_common_core::types::OrgId;

#[tokio::test(flavor = "current_thread")]
async fn password_update_revokes_existing_sessions_but_profile_update_does_not()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = sqlx::query_as::<_, User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let initial_password_hash = hash_password("initial-password")?;
    sqlx::query(sql!("UPDATE users SET password_hash = $2 WHERE id = $1"))
        .bind(actor.id)
        .bind(initial_password_hash)
        .execute(&db.pool)
        .await?;
    let mut raw_tokens = Vec::new();
    for _ in 0..2 {
        let (raw_token, token_hash) = crate::auth::generate_session_token();
        sqlx::query(sql!(
            "INSERT INTO sessions (id, user_id, token_hash, expires_at)
             VALUES ($1, $2, $3, NOW() + INTERVAL '1 hour')"
        ))
        .bind(Uuid::now_v7())
        .bind(actor.id)
        .bind(token_hash)
        .execute(&db.pool)
        .await?;
        raw_tokens.push(raw_token);
    }

    let _ = update_user(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(actor.id),
        Json(UpdateUserRequest {
            email: None,
            password: None,
            display_name: Some("Updated display name".to_string()),
        }),
    )
    .await?;
    for raw_token in &raw_tokens {
        assert!(
            crate::auth::get_session_user(&db.pool, raw_token)
                .await?
                .is_some(),
            "a profile-only update must preserve every active session"
        );
    }

    let _ = update_user(
        State(state),
        Extension(AuthUser(actor.clone())),
        Path(actor.id),
        Json(UpdateUserRequest {
            email: None,
            password: Some("replacement-password".to_string()),
            display_name: None,
        }),
    )
    .await?;
    for raw_token in &raw_tokens {
        assert!(
            crate::auth::get_session_user(&db.pool, raw_token)
                .await?
                .is_none(),
            "a password update must revoke every existing session"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn password_update_is_rejected_for_federated_user() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = sqlx::query_as::<_, User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let issuer = "https://identity.example.invalid";
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         ) VALUES ($1, 'Federated Admin Test Org', $2, 'federated_identity', $3, $4)"
    ))
    .bind(org_id)
    .bind(format!("federated-admin-{org_id}"))
    .bind(issuer)
    .bind(format!("org-{org_id}"))
    .execute(&db.pool)
    .await?;
    let original_hash = "federated-user-no-local-password";
    sqlx::query(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         ) VALUES ($1, $2, $3, 'Federated Admin Test User', $4, 'member',
                   'federated_identity', $5, $6)"
    ))
    .bind(user_id)
    .bind(format!("federated-{user_id}@example.invalid"))
    .bind(original_hash)
    .bind(org_id)
    .bind(issuer)
    .bind(format!("subject-{user_id}"))
    .execute(&db.pool)
    .await?;

    let error = update_user(
        State(state),
        Extension(AuthUser(actor)),
        Path(user_id),
        Json(UpdateUserRequest {
            email: None,
            password: Some("must-not-be-installed".to_string()),
            display_name: None,
        }),
    )
    .await
    .expect_err("federated users must not accept local passwords");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert!(error.to_string().contains("federated users"));
    let stored_hash: String =
        sqlx::query_scalar(sql!("SELECT password_hash FROM users WHERE id = $1"))
            .bind(user_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(stored_hash, original_hash);
    Ok(())
}

/// Without a lazy platform provider, enabling BYOK for a keyless org would
/// create encrypted sandboxes that cannot obtain a volume DEK.
#[test]
fn byok_enable_is_refused_for_keyless_org_without_lazy_provisioning() {
    let err = ensure_byok_enable_allowed(None, None)
        .expect_err("enabling BYOK without an active master key must be refused");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.to_string().contains("master key"), "{err}");
}

#[test]
fn byok_enable_is_allowed_with_an_authority_supported_active_master_key() {
    for provider in [KekProvider::InMemory, KekProvider::IntermediateKeyBroker] {
        ensure_byok_enable_allowed(Some(provider), None)
            .expect("org with an authority-supported active key may enable BYOK");
    }
}

/// A historical AWS KMS row blocks lazy provisioning but cannot wrap a new
/// volume DEK because the credential authority no longer serves that backend.
#[test]
fn byok_enable_is_refused_for_legacy_aws_kms_active_master_key() {
    let err = ensure_byok_enable_allowed(
        Some(KekProvider::AwsKms),
        Some(KekProvider::IntermediateKeyBroker),
    )
    .expect_err("legacy AWS KMS active key must not enable BYOK");

    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.to_string().contains("not supported"), "{err}");
}

#[test]
fn byok_enable_is_refused_for_unknown_active_master_key_provider() {
    let err = parse_active_kek_provider(Some("unknown-provider".to_string()))
        .expect_err("unknown active key provider must not enable BYOK");

    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.to_string().contains("not supported"), "{err}");
}

#[tokio::test(flavor = "current_thread")]
async fn active_master_key_provider_is_loaded_for_byok_policy() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = Uuid::from_u128(1);
    sqlx::query(sql!(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier) \
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(KekProvider::AwsKms.as_str())
    .bind("legacy-key")
    .execute(&db.pool)
    .await?;

    let credentials_pool = pplx_space_common_svc::sql::CredentialsPool::shared(db.pool.clone());
    assert_eq!(
        active_kek_provider(&credentials_pool, OrgId::from_uuid(org_id)).await?,
        Some(KekProvider::AwsKms),
        "the BYOK gate must evaluate the active row's provider, not mere row existence"
    );
    Ok(())
}

/// A keyless org may enable BYOK when the platform default provisions its
/// KEK lazily (intermediate-key-broker), because the credmgr authority mints it
/// on the org's first Encrypt / sandbox volume DEK. This closes the
/// chicken-and-egg where enablement required a KEK that could only be minted
/// after enablement.
#[test]
fn byok_enable_is_allowed_for_keyless_org_when_platform_provisions_lazily() {
    ensure_byok_enable_allowed(None, Some(KekProvider::IntermediateKeyBroker))
        .expect("keyless org on a lazy-provisioning platform may enable BYOK");
}

/// The identity handlers carry their own error enum instead of returning
/// `support::db_error`'s tuple, so the disposition has to come from the shared
/// helper rather than being restated here — a surface that re-derives the split
/// is a surface where one handler's `40P01` and the next one's describe the
/// same abort two ways.
///
/// What the helper answers depends on the `ContentionScope` the caller passes,
/// and this conversion passes `ReadOnly`. That is a deliberate under-claim:
/// it is one blanket `From` covering both `set_org_master_key`'s
/// `SELECT ... FOR UPDATE` and five pure-read handlers, so `Writing` would tell
/// a `list_orgs` caller that its own write lost a race — false, where the 503
/// is merely less specific. The divergence from `db_error`'s hardcoded
/// `Writing` is asserted below so it stays deliberate rather than becoming a
/// drift nobody notices.
#[tokio::test(flavor = "current_thread")]
async fn identity_handlers_take_the_admin_surface_contention_disposition()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };
    use super::super::support::{ContentionScope, contention_disposition};

    // Under `ReadOnly` every contention SQLSTATE is retryable and none takes
    // the write-specific 409.
    let expected = [
        ("55P03", StatusCode::SERVICE_UNAVAILABLE),
        ("40001", StatusCode::SERVICE_UNAVAILABLE),
        ("40P01", StatusCode::SERVICE_UNAVAILABLE),
        ("23505", StatusCode::INTERNAL_SERVER_ERROR),
    ];
    for (sqlstate, status) in expected {
        let error = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        let via_enum = IdentityAdminError::from(error).status();
        assert_eq!(via_enum, status, "{sqlstate} disposition");

        // The same input through the shared helper: this enum must not invent
        // its own split.
        let via_helper = contention_disposition(
            ContentionScope::ReadOnly,
            &pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await,
        )
        .map_or(StatusCode::INTERNAL_SERVER_ERROR, |(status, _)| status);
        assert_eq!(
            via_enum, via_helper,
            "{sqlstate} must come from the shared disposition"
        );
    }

    // And the divergence from `db_error` is real, not incidental: a `40P01`
    // earns the 409 there because that path hardcodes `Writing`. If this ever
    // stops holding, the two have converged and the doc above is stale.
    let (via_db_error, _) = super::super::support::db_error(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await,
    );
    assert_eq!(via_db_error, StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_user_answers_conflict_while_the_user_owns_sandboxes()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = sqlx::query_as::<_, User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Sandbox Owner', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("owner-{user_id}@example.invalid"))
    .bind(actor.org_id)
    .execute(&db.pool)
    .await?;
    let delete = || {
        delete_user(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(user_id),
        )
    };

    let sandbox_id =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(&db.pool, actor.org_id, user_id)
            .await?;
    let error = delete()
        .await
        .expect_err("an owned sandbox must block the delete");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("user_owns_sandboxes"));

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_archives (
             sandbox_id, org_id, user_id, sandbox_created_at, manifest_id,
             manifest_sha256, stored_bytes
         ) VALUES ($1, $2, $3, NOW(), $4, '\\x00', 0)"
    ))
    .bind(sandbox_id)
    .bind(actor.org_id)
    .bind(user_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let error = delete()
        .await
        .expect_err("an archived sandbox must block the delete");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("user_owns_archived_sandboxes"));

    sqlx::query(sql!("DELETE FROM sandbox_archives WHERE sandbox_id = $1"))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    // A deleted sandbox's row is kept for retention and still holds the user.
    let tombstone =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(&db.pool, actor.org_id, user_id)
            .await?;
    let deleted_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1 RETURNING updated_at"
    ))
    .bind(tombstone)
    .fetch_one(&db.pool)
    .await?;
    let error = delete()
        .await
        .expect_err("a retained deleted sandbox must block the delete");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("user_owns_retained_deleted_sandboxes"));
    let body = to_bytes(error.into_response().into_body(), usize::MAX).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    let retained_until = time::OffsetDateTime::parse(
        body["retained_until"].as_str().expect("retained_until"),
        &time::format_description::well_known::Rfc3339,
    )?;
    assert_eq!(retained_until, deleted_at + time::Duration::days(7));

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(tombstone)
        .execute(&db.pool)
        .await?;
    assert_eq!(delete().await?, StatusCode::NO_CONTENT);

    // The self-target 409 stays distinguishable from the ownership 409s.
    let error = delete_user(
        State(state),
        Extension(AuthUser(actor.clone())),
        Path(actor.id),
    )
    .await
    .expect_err("self-delete must be refused");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("self_mutation"));
    Ok(())
}

async fn managing_org_admin(pool: &sqlx::PgPool) -> Result<User, sqlx::Error> {
    sqlx::query_as::<_, User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(pool)
    .await
}

async fn insert_org(pool: &sqlx::PgPool) -> Result<OrgId, sqlx::Error> {
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Delete Test Org {org_id}"))
    .bind(format!("delete-test-{org_id}"))
    .execute(pool)
    .await?;
    Ok(org_id)
}

/// Each row kind that refuses an organization's delete answers a 409 naming
/// it, and the delete succeeds once it is gone. Rows whose foreign key
/// cascades, such as API keys, do not block it, and child organizations are
/// detached.
#[tokio::test(flavor = "current_thread")]
async fn delete_org_answers_conflict_naming_what_still_references_it()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let org_id = insert_org(&db.pool).await?;
    let delete = || {
        delete_org(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(org_id),
        )
    };
    let refused_with = |reason: &'static str| {
        let delete = delete();
        async move {
            let error = delete.await.expect_err(reason);
            assert_eq!(error.status(), StatusCode::CONFLICT, "{reason}");
            let body = to_bytes(error.into_response().into_body(), usize::MAX).await?;
            let body: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(body["reason"], reason);
            pplx_space_test_env::TestResult::Ok(body)
        }
    };

    // Each blocker is inserted alone and removed after its refusal. The
    // sandbox rows belong to the actor, a user outside the org, so the org's
    // own users never stand in for them.
    let member_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Org Member', $3, 'member')"
    ))
    .bind(member_id)
    .bind(format!("member-{member_id}@example.invalid"))
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    refused_with("org_has_users").await?;
    sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(member_id)
        .execute(&db.pool)
        .await?;

    // A deleted sandbox's row is kept for retention and still blocks.
    let sandbox_id =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(&db.pool, org_id, actor.id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    refused_with("org_has_sandboxes").await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_archives (
             sandbox_id, org_id, user_id, sandbox_created_at, manifest_id,
             manifest_sha256, stored_bytes
         ) VALUES ($1, $2, $3, NOW(), $4, '\\x00', 0)"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(actor.id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    refused_with("org_has_archived_sandboxes").await?;
    sqlx::query(sql!("DELETE FROM sandbox_archives WHERE sandbox_id = $1"))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

    let template_name = format!("delete-test-{org_id}");
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id, visibility)
         VALUES ($1, $2, $3, 'org_specific')"
    ))
    .bind(Uuid::now_v7())
    .bind(&template_name)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    refused_with("org_has_templates").await?;

    // A deleted template's row is kept for retention, measured from its
    // delete request, and still blocks; the template routes no longer show
    // it, so the refusal names when it goes rather than asking for a delete.
    let delete_requested_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "UPDATE templates
         SET desired_state = 'deleted',
             delete_requested_at = NOW() - INTERVAL '1 day'
         WHERE name = $1
         RETURNING delete_requested_at"
    ))
    .bind(&template_name)
    .fetch_one(&db.pool)
    .await?;
    let body = refused_with("org_has_retained_deleted_templates").await?;
    let retained_until = time::OffsetDateTime::parse(
        body["retained_until"].as_str().expect("retained_until"),
        &time::format_description::well_known::Rfc3339,
    )?;
    assert_eq!(
        retained_until,
        delete_requested_at + time::Duration::days(7)
    );

    // While a node still caches it, retention has no end to name.
    let node_id = pplx_space_common_core::types::NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (node_id, template_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(&template_name)
    .execute(&db.pool)
    .await?;
    let body = refused_with("org_has_retained_deleted_templates").await?;
    assert!(
        body.get("retained_until").is_none(),
        "a cached template's retention has no known end"
    );
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!("DELETE FROM templates WHERE org_id = $1"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;

    // A snapshot storage location its deleted snapshots left behind, still
    // encrypted under the org's master key, outlives its sandboxes and blocks
    // through the master key the org's delete cascades to.
    let master_key_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier)
         VALUES ($1, $2, 'in_memory', 'delete-test')"
    ))
    .bind(master_key_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    let location_id =
        insert_orphaned_encrypted_location(&mut *db.pool.acquire().await?, org_id, master_key_id)
            .await?;
    refused_with("org_has_orphaned_snapshot_storage").await?;
    // While the org still has sandbox rows, the location may be one of
    // theirs, so they are what the operator removes first.
    let sandbox_id =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(&db.pool, org_id, actor.id).await?;
    assert_eq!(
        OrgDeleteBlocker::OrphanedSnapshotStorage
            .refine(&state, org_id)
            .await?,
        OrgDeleteBlocker::Sandboxes
    );
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!("DELETE FROM snapshot_storage_locations WHERE id = $1"))
        .bind(location_id)
        .execute(&db.pool)
        .await?;

    sqlx::query(sql!(
        "INSERT INTO node_groups (id, name, org_id) VALUES ($1, 'delete-test-group', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    refused_with("org_has_node_groups").await?;
    sqlx::query(sql!("DELETE FROM node_groups WHERE org_id = $1"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;

    sqlx::query(sql!(
        "INSERT INTO api_keys (org_id, name, key_hash) VALUES ($1, 'cascades', $2)"
    ))
    .bind(org_id)
    .bind(format!("delete-test-{org_id}"))
    .execute(&db.pool)
    .await?;
    let child_id = insert_org(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE organizations SET parent_id = $2 WHERE id = $1"
    ))
    .bind(child_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(delete().await?, StatusCode::NO_CONTENT);
    let api_keys: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM api_keys WHERE org_id = $1"))
        .bind(org_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(api_keys, 0, "the org's API keys go with it");
    let child_parent: Option<OrgId> =
        sqlx::query_scalar(sql!("SELECT parent_id FROM organizations WHERE id = $1"))
            .bind(child_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(child_parent, None, "the org's children are detached");
    Ok(())
}

/// A snapshot storage location detached from its snapshot, with its
/// connection string encrypted under `master_key_id`.
async fn insert_orphaned_encrypted_location(
    conn: &mut sqlx::PgConnection,
    org_id: OrgId,
    master_key_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let location_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO snapshot_storage_locations (storage_connection_string, org_id, orphaned_at)
         VALUES ('', $1, NOW())
         RETURNING id"
    ))
    .bind(org_id)
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_location_encrypted_fields (
             snapshot_storage_location_id, org_id, master_key_id, wrapped_dek,
             storage_connection_string
         ) VALUES ($1, $2, $3, '\\x00', '\\x00')"
    ))
    .bind(location_id)
    .bind(org_id)
    .bind(master_key_id)
    .execute(conn)
    .await?;
    Ok(location_id)
}

/// The blockers the handler does not reach in one request: the managing
/// organization, which it refuses first, and the encrypted rows that restrict
/// the delete of the master key an organization's delete cascades to, which
/// the database reports only if it checks them before the sandbox or template
/// row itself. Each is classified from the database's own error, one for each
/// of the five constraints that restrict a master key's delete.
#[tokio::test(flavor = "current_thread")]
async fn org_delete_blockers_classify_the_database_constraint() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = managing_org_admin(&db.pool).await?;
    let org_id = insert_org(&db.pool).await?;
    let master_key_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO org_master_keys (id, org_id, provider, key_identifier)
         VALUES ($1, $2, 'in_memory', 'delete-test')"
    ))
    .bind(master_key_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    let sandbox_id =
        pplx_space_db_test_fixtures::insert_suspended_sandbox(&db.pool, org_id, actor.id).await?;
    let template_name = format!("delete-test-{org_id}");
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id, visibility)
         VALUES ($1, $2, $3, 'org_specific')"
    ))
    .bind(Uuid::now_v7())
    .bind(&template_name)
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    let delete_master_key = sql!("DELETE FROM org_master_keys WHERE id = $1");

    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_encrypted_fields (
             sandbox_id, org_id, master_key_id, wrapped_dek, env, metadata
         ) VALUES ($1, $2, $3, '\\x00', '\\x00', '\\x00')"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(master_key_id)
    .execute(tx.as_mut())
    .await?;
    let error = sqlx::query(delete_master_key)
        .bind(master_key_id)
        .execute(tx.as_mut())
        .await
        .expect_err("encrypted fields restrict their master key's delete");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::Sandboxes)
    );
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_volume_encryption (sandbox_id, org_id, master_key_id, wrapped_dek)
         VALUES ($1, $2, $3, '\\x00')"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(master_key_id)
    .execute(tx.as_mut())
    .await?;
    let error = sqlx::query(delete_master_key)
        .bind(master_key_id)
        .execute(tx.as_mut())
        .await
        .expect_err("volume encryption restricts its master key's delete");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::Sandboxes)
    );
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO template_encrypted_fields (
             template_name, org_id, master_key_id, wrapped_dek, network_policy_json
         ) VALUES ($1, $2, $3, '\\x00', '\\x00')"
    ))
    .bind(&template_name)
    .bind(org_id)
    .bind(master_key_id)
    .execute(tx.as_mut())
    .await?;
    let error = sqlx::query(delete_master_key)
        .bind(master_key_id)
        .execute(tx.as_mut())
        .await
        .expect_err("template encrypted fields restrict their master key's delete");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::Templates)
    );
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    insert_orphaned_encrypted_location(tx.as_mut(), org_id, master_key_id).await?;
    let error = sqlx::query(delete_master_key)
        .bind(master_key_id)
        .execute(tx.as_mut())
        .await
        .expect_err("encrypted snapshot storage restricts its master key's delete");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::OrphanedSnapshotStorage)
    );
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    let release_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, org_id)
         VALUES ($1, 'fc', 'delete-test', $2)
         RETURNING id"
    ))
    .bind(&template_name)
    .bind(org_id)
    .fetch_one(tx.as_mut())
    .await?;
    let location_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_release_storage_locations (
             template_release_id, storage_connection_string, org_id
         ) VALUES ($1, '', $2)
         RETURNING id"
    ))
    .bind(release_id)
    .bind(org_id)
    .fetch_one(tx.as_mut())
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_location_encrypted_fields (
             template_release_storage_location_id, org_id, master_key_id, wrapped_dek,
             storage_connection_string
         ) VALUES ($1, $2, $3, '\\x00', '\\x00')"
    ))
    .bind(location_id)
    .bind(org_id)
    .bind(master_key_id)
    .execute(tx.as_mut())
    .await?;
    let error = sqlx::query(delete_master_key)
        .bind(master_key_id)
        .execute(tx.as_mut())
        .await
        .expect_err("encrypted template release storage restricts its master key's delete");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::Templates)
    );
    tx.rollback().await?;

    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE org_id = $1"))
        .bind(org_id)
        .execute(tx.as_mut())
        .await?;
    sqlx::query(sql!("DELETE FROM templates WHERE org_id = $1"))
        .bind(org_id)
        .execute(tx.as_mut())
        .await?;
    sqlx::query(sql!(
        "UPDATE clusters SET managing_org_id = $1 WHERE singleton"
    ))
    .bind(org_id)
    .execute(tx.as_mut())
    .await?;
    let error = sqlx::query(sql!("DELETE FROM organizations WHERE id = $1"))
        .bind(org_id)
        .execute(tx.as_mut())
        .await
        .expect_err("the managing organization cannot be deleted");
    assert_eq!(
        OrgDeleteBlocker::from_delete_error(&error),
        Some(OrgDeleteBlocker::ManagesCluster)
    );
    tx.rollback().await?;
    Ok(())
}

/// Creating a user with, or changing a user to, an email another user has
/// answers a 409 rather than surfacing the unique violation as internal.
#[tokio::test(flavor = "current_thread")]
async fn an_email_in_use_answers_conflict_on_create_and_update() -> pplx_space_test_env::TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let create = |email: String| {
        create_user_for_org(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(actor.org_id),
            Json(CreateUserRequest {
                email,
                password: Uuid::now_v7().to_string(),
                display_name: "Email Test User".to_string(),
                role: UserRole::Member,
            }),
        )
    };
    let suffix = Uuid::now_v7();
    let (status, Json(first)) = create(format!("first-{suffix}@example.invalid")).await?;
    assert_eq!(status, StatusCode::CREATED);
    let (_, Json(second)) = create(format!("second-{suffix}@example.invalid")).await?;

    let error = create(format!("first-{suffix}@example.invalid"))
        .await
        .expect_err("a second user cannot take the email");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("email_in_use"));
    let body = to_bytes(error.into_response().into_body(), usize::MAX).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body,
        serde_json::json!({"error": "email already in use", "reason": "email_in_use"})
    );

    let error = update_user(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(second.user.id),
        Json(UpdateUserRequest {
            email: Some(first.user.email.clone()),
            password: None,
            display_name: None,
        }),
    )
    .await
    .expect_err("an update cannot take another user's email");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.reason(), Some("email_in_use"));
    let email: String = sqlx::query_scalar(sql!("SELECT email FROM users WHERE id = $1"))
        .bind(second.user.id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        email, second.user.email,
        "the refused update changes nothing"
    );
    Ok(())
}
