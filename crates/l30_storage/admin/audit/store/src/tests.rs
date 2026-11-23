use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

fn event<'a>(path: &'a str) -> NewAdminAuditEvent<'a> {
    NewAdminAuditEvent {
        id: Uuid::now_v7(),
        actor_user_id: UserId::from_uuid(Uuid::now_v7()),
        actor_org_id: OrgId::from_uuid(Uuid::now_v7()),
        actor_email: "admin@example.test",
        method: "POST",
        path,
        status_code: 201,
        success: true,
        duration_ms: 3,
        target_org_id: None,
        target_type: None,
        target_id: None,
        target_user_id: None,
    }
}

/// Insert an event for `path` created `age_days` ago.
async fn insert_aged(pool: &PgPool, path: &str, age_days: i64) -> TestResult<Uuid> {
    let event = event(path);
    insert(pool, event).await?;
    sqlx::query(sql!(
        "UPDATE admin_audit_events
            SET created_at = NOW() - ($2::BIGINT * INTERVAL '1 day')
          WHERE id = $1"
    ))
    .bind(event.id)
    .bind(age_days)
    .execute(pool)
    .await?;
    Ok(event.id)
}

fn paths(events: &[AdminAuditEvent]) -> Vec<&str> {
    events.iter().map(|event| event.path.as_str()).collect()
}

/// Catches a column bound to the wrong placeholder or dropped from the
/// `SELECT` list, and a target written as empty instead of NULL.
#[tokio::test(flavor = "current_thread")]
async fn insert_round_trips_every_column() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let target_org_id = OrgId::from_uuid(Uuid::now_v7());
    let target_user_id = UserId::from_uuid(Uuid::now_v7());
    let targeted = NewAdminAuditEvent {
        method: "DELETE",
        status_code: 404,
        success: false,
        duration_ms: 17,
        target_org_id: Some(target_org_id),
        target_type: Some("api_key"),
        target_id: Some("key-1"),
        target_user_id: Some(target_user_id),
        ..event("/targeted")
    };
    insert(&db.pool, targeted).await?;
    let untargeted = event("/untargeted");
    insert(&db.pool, untargeted).await?;

    let events = list_newest(&db.pool, 10).await?;
    let read = |id| events.iter().find(|event| event.id == id).unwrap();

    let stored = read(targeted.id);
    assert_eq!(stored.actor_user_id, targeted.actor_user_id);
    assert_eq!(stored.actor_org_id, targeted.actor_org_id);
    assert_eq!(stored.actor_email, "admin@example.test");
    assert_eq!(stored.method, "DELETE");
    assert_eq!(stored.path, "/targeted");
    assert_eq!(stored.status_code, 404);
    assert!(!stored.success);
    assert_eq!(stored.duration_ms, 17);
    assert_eq!(stored.target_org_id, Some(target_org_id));
    assert_eq!(stored.target_type.as_deref(), Some("api_key"));
    assert_eq!(stored.target_id.as_deref(), Some("key-1"));
    assert_eq!(stored.target_user_id, Some(target_user_id));

    let stored = read(untargeted.id);
    assert_eq!(stored.target_org_id, None);
    assert_eq!(stored.target_type, None);
    assert_eq!(stored.target_id, None);
    assert_eq!(stored.target_user_id, None);
    Ok(())
}

/// Catches the list ordered oldest first or ignoring its limit.
#[tokio::test(flavor = "current_thread")]
async fn list_newest_returns_the_newest_first() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert_aged(&db.pool, "/oldest", 3).await?;
    insert_aged(&db.pool, "/newest", 1).await?;
    insert_aged(&db.pool, "/middle", 2).await?;

    assert_eq!(
        paths(&list_newest(&db.pool, 2).await?),
        ["/newest", "/middle"]
    );
    Ok(())
}

/// Catches the cap deleting newer rows than the newest `keep`, deleting past
/// its limit, deleting the newest of the overflow first, or ranking rows by
/// `id` instead of `created_at` (the rows are inserted out of age order, so
/// their UUIDv7 ids don't follow their ages).
#[tokio::test(flavor = "current_thread")]
async fn delete_beyond_newest_removes_the_oldest_overflow() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (path, age_days) in [("/c", 3), ("/a", 5), ("/e", 1), ("/b", 4), ("/d", 2)] {
        insert_aged(&db.pool, path, age_days).await?;
    }

    assert_eq!(delete_beyond_newest(&db.pool, 2, 2).await?, 2);
    assert_eq!(count(&db.pool).await?, 3);
    assert_eq!(paths(&list_newest(&db.pool, 10).await?), ["/e", "/d", "/c"]);

    assert_eq!(delete_beyond_newest(&db.pool, 2, 10).await?, 1);
    assert_eq!(delete_beyond_newest(&db.pool, 2, 10).await?, 0);
    assert_eq!(paths(&list_newest(&db.pool, 10).await?), ["/e", "/d"]);
    Ok(())
}
