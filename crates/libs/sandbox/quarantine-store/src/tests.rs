use pplx_space_common_core::types::{OrgId, UserId};
use pplx_space_db_test_fixtures::{insert_suspended_sandbox, pg_test_db};
use pplx_space_test_env::TestResult;

use super::*;

const SEED_ORG: &str = "00000000-0000-0000-0000-000000000001";
const SEED_USER: &str = "00000000-0000-0000-0000-000000000010";

async fn seed_sandbox(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(Uuid::parse_str(SEED_ORG)?),
        UserId::from_uuid(Uuid::parse_str(SEED_USER)?),
    )
    .await?;
    Ok(sandbox_id)
}

async fn insert_history(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    quarantine_id: Uuid,
    action: &str,
    seconds_ago: i32,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history
             (id, quarantine_id, sandbox_id, occurred_at, source, action, reason_code)
         VALUES ($1, $2, $3, NOW() - make_interval(secs => $5), 'sbx_manager', $4,
                 'pause_failed')"
    ))
    .bind(id)
    .bind(quarantine_id)
    .bind(sandbox_id)
    .bind(action)
    .bind(seconds_ago)
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn reads_a_sandboxs_open_episode_and_its_history_newest_first() -> TestResult {
    let db = pg_test_db().await?;
    let never = seed_sandbox(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool).await?;
    let other = seed_sandbox(&db.pool).await?;
    assert_eq!(active_quarantine(&db.pool, never).await?, None);
    assert!(quarantine_history(&db.pool, never, 10).await?.is_empty());

    let released = Uuid::now_v7();
    let open = Uuid::now_v7();
    let first = insert_history(&db.pool, sandbox, released, "entered_quarantine", 300).await?;
    let recovered =
        insert_history(&db.pool, sandbox, released, "recovered_to_suspended", 200).await?;
    let entered = insert_history(&db.pool, sandbox, open, "entered_quarantine", 100).await?;
    insert_history(&db.pool, other, Uuid::now_v7(), "entered_quarantine", 50).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state
             (sandbox_id, quarantine_id, entered_from_lifecycle, source, reason_code,
              reason_message, next_action)
         VALUES ($1, $2, 'paused', 'sbx_manager', 'pause_failed', 'pause timed out',
                 'requires_operator_attention')"
    ))
    .bind(sandbox)
    .bind(open)
    .execute(&db.pool)
    .await?;

    let active = active_quarantine(&db.pool, sandbox)
        .await?
        .expect("open episode");
    assert_eq!(active.quarantine_id, open);
    assert_eq!(
        (
            active.source.as_str(),
            active.reason_code.as_str(),
            active.reason_message.as_deref(),
            active.entered_from_lifecycle.as_deref(),
        ),
        (
            "sbx_manager",
            "pause_failed",
            Some("pause timed out"),
            Some("paused")
        )
    );
    assert_eq!(active_quarantine(&db.pool, never).await?, None);

    let history = quarantine_history(&db.pool, sandbox, 10).await?;
    assert_eq!(
        history.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![entered, recovered, first]
    );
    assert_eq!(history[1].quarantine_id, released);
    let newest = quarantine_history(&db.pool, sandbox, 1).await?;
    assert_eq!(
        newest.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        vec![entered]
    );
    Ok(())
}
