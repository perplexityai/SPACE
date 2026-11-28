use pplx_space_accounts::{OrgId, UserId};
use pplx_space_db_test_fixtures::{insert_suspended_sandbox, pg_test_db};
use pplx_space_test_env::TestResult;

use super::*;

const SEED_ORG: &str = "00000000-0000-0000-0000-000000000001";
const SEED_USER: &str = "00000000-0000-0000-0000-000000000010";

/// A suspended sandbox whose open episode `quarantine_id` the sbx manager
/// opened for a pause failure.
async fn seed_episode(pool: &sqlx::PgPool, quarantine_id: Uuid) -> TestResult<SandboxId> {
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(Uuid::parse_str(SEED_ORG)?),
        UserId::from_uuid(Uuid::parse_str(SEED_USER)?),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state
             (sandbox_id, quarantine_id, entered_from_lifecycle, source, reason_code,
              reason_message, next_action)
         VALUES ($1, $2, 'paused', 'sbx_manager', 'pause_failed', 'pause timed out',
                 'requires_operator_attention')"
    ))
    .bind(sandbox_id)
    .bind(quarantine_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

fn recovery(sandbox_id: SandboxId, quarantine_id: Uuid) -> NodeRecovery {
    NodeRecovery {
        history_id: Uuid::now_v7(),
        sandbox_id,
        quarantine_id,
        quarantined_by: SandboxQuarantineSource::SbxManager,
        reason_code: SandboxQuarantineReasonCode::PauseFailed,
        recorded_by: SandboxQuarantineSource::NodeStatePublish,
        action: SandboxQuarantineHistoryAction::RecoveredToSuspended,
        node_id: NodeId::new(),
    }
}

async fn history_count(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_quarantine_history WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn open_episode(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult<Option<Uuid>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn record_inserts_nothing_unless_the_episode_source_and_reason_all_match() -> TestResult {
    let db = pg_test_db().await?;
    let quarantine_id = Uuid::now_v7();
    let sandbox_id = seed_episode(&db.pool, quarantine_id).await?;
    let mut conn = db.pool.acquire().await?;

    let stale_episode = recovery(sandbox_id, Uuid::now_v7());
    let other_source = NodeRecovery {
        quarantined_by: SandboxQuarantineSource::ClusterController,
        ..recovery(sandbox_id, quarantine_id)
    };
    let other_reason = NodeRecovery {
        reason_code: SandboxQuarantineReasonCode::RestoreResumeFailed,
        ..recovery(sandbox_id, quarantine_id)
    };
    for (label, losing) in [
        ("stale quarantine_id", stale_episode),
        ("other source", other_source),
        ("other reason", other_reason),
    ] {
        assert_eq!(
            record_node_recovery(&mut *conn, &losing).await?,
            0,
            "{label}"
        );
        assert_eq!(history_count(&db.pool, sandbox_id).await?, 0, "{label}");
    }

    let winning = recovery(sandbox_id, quarantine_id);
    assert_eq!(record_node_recovery(&mut *conn, &winning).await?, 1);
    let (id, recorded_episode, source, action, reason_code, node): (
        Uuid,
        Uuid,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(sql!(
        "SELECT id, quarantine_id, source, action, reason_code,
                evidence ->> 'recovered_by_node_id'
           FROM sandbox_quarantine_history
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        (
            id,
            recorded_episode,
            source.as_str(),
            action.as_str(),
            reason_code.as_deref(),
            node,
        ),
        (
            winning.history_id,
            quarantine_id,
            "node_state_publish",
            "recovered_to_suspended",
            Some("pause_failed"),
            Some(winning.node_id.to_string()),
        )
    );
    assert_eq!(
        open_episode(&db.pool, sandbox_id).await?,
        Some(quarantine_id)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retire_deletes_nothing_for_a_stale_quarantine_id() -> TestResult {
    let db = pg_test_db().await?;
    let quarantine_id = Uuid::now_v7();
    let sandbox_id = seed_episode(&db.pool, quarantine_id).await?;
    let mut conn = db.pool.acquire().await?;

    assert_eq!(
        retire_node_recovered_episode(&mut *conn, sandbox_id, Uuid::now_v7()).await?,
        0
    );
    assert_eq!(
        open_episode(&db.pool, sandbox_id).await?,
        Some(quarantine_id)
    );

    assert_eq!(
        retire_node_recovered_episode(&mut *conn, sandbox_id, quarantine_id).await?,
        1
    );
    assert_eq!(open_episode(&db.pool, sandbox_id).await?, None);
    Ok(())
}
