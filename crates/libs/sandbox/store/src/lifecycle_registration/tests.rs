use super::*;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

async fn sandbox(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation, last_access_at,
            updated_at, command_ready_pending, restore_attempt_count, next_restore_attempt_at,
            restore_lease_token, restore_lease_expires_at, preserve_local_artifact_on_restore_failure)
         VALUES ($1, '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001', 1, '2026-01-01T00:00:00Z',
                 '2026-01-01T00:00:00Z', TRUE, 7, NOW(), $2, NOW(), TRUE)"
    ))
    .bind(id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(id)
}

fn binding() -> PolicyBinding {
    PolicyBinding {
        user_id: 42,
        organization_uuid: None,
        session_id: "session_test".into(),
        context_uuid: None,
    }
}

#[tokio::test]
async fn registration_is_revision_guarded_and_does_not_touch_sandbox() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = sandbox(&db.pool).await?;
    let before: serde_json::Value = sqlx::query_scalar(sql!(
        "SELECT to_jsonb(s) - 'lifecycle_registration' FROM sandboxes s WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await?;
    assert!(candidates(&db.pool, None, 10).await?.is_empty());
    assert_eq!(register(&db.pool, id, false, &binding(), 0).await?, Some(1));
    assert!(candidates(&db.pool, None, 10).await?.is_empty());
    assert_eq!(register(&db.pool, id, true, &binding(), 0).await?, None);
    assert_eq!(register(&db.pool, id, true, &binding(), 1).await?, Some(2));
    let rows = candidates(&db.pool, None, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sandbox_id, id);
    assert_eq!(rows[0].registration.revision, 2);
    assert_eq!(rows[0].registration.binding.user_id, binding().user_id);
    let after: serde_json::Value = sqlx::query_scalar(sql!(
        "SELECT to_jsonb(s) - 'lifecycle_registration' FROM sandboxes s WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(before, after);

    let binding = binding();
    let (first, second) = tokio::join!(
        register(&db.pool, id, false, &binding, 2),
        register(&db.pool, id, true, &binding, 2),
    );
    assert_eq!(
        [first?, second?]
            .into_iter()
            .filter(Option::is_some)
            .count(),
        1
    );
    assert_eq!(revision(&db.pool, id).await?, Some(3));

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
    ))
    .bind(id)
    .execute(&db.pool)
    .await?;
    let transitioned: serde_json::Value =
        sqlx::query_scalar(sql!("SELECT to_jsonb(s) FROM sandboxes s WHERE id = $1"))
            .bind(id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(transitioned["command_ready_pending"], false);
    assert_eq!(transitioned["restore_attempt_count"], 0);
    assert!(transitioned["next_restore_attempt_at"].is_null());
    assert!(transitioned["restore_lease_token"].is_null());
    assert!(transitioned["restore_lease_expires_at"].is_null());
    assert_eq!(
        transitioned["preserve_local_artifact_on_restore_failure"],
        false
    );
    assert_ne!(transitioned["updated_at"], before["updated_at"]);
    Ok(())
}

#[tokio::test]
async fn enabled_inventory_is_bounded_and_keyset_paginated() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut ids = Vec::new();
    for _ in 0..3 {
        let id = sandbox(&db.pool).await?;
        register(&db.pool, id, true, &binding(), 0).await?;
        ids.push(id);
    }
    ids.sort();
    let first = candidates(&db.pool, None, 2).await?;
    assert_eq!(
        first.iter().map(|r| r.sandbox_id).collect::<Vec<_>>(),
        ids[..2]
    );
    let tail = candidates(&db.pool, Some(first[1].sandbox_id), 2).await?;
    assert_eq!(
        tail.iter().map(|r| r.sandbox_id).collect::<Vec<_>>(),
        ids[2..]
    );
    assert!(
        candidates(&db.pool, Some(tail[0].sandbox_id), 2)
            .await?
            .is_empty()
    );
    assert!(matches!(
        candidates(&db.pool, None, 502).await,
        Err(RegistrationError::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn registration_column_rejects_malformed_envelopes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = sandbox(&db.pool).await?;
    let initial: Option<serde_json::Value> = sqlx::query_scalar(sql!(
        "SELECT lifecycle_registration FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(initial, None);
    for value in [
        serde_json::json!(null),
        serde_json::json!({}),
        serde_json::json!({"enabled":"true", "revision":1, "binding":{}}),
        serde_json::json!({"enabled":true, "revision":0, "binding":{}}),
        serde_json::json!({"enabled":true, "revision":1, "binding":[]}),
    ] {
        let error = sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle_registration = $2 WHERE id = $1"
        ))
        .bind(id)
        .bind(value)
        .execute(&db.pool)
        .await
        .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("23514")
        );
    }
    Ok(())
}
