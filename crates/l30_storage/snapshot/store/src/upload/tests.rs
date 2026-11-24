use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::in_flight_idle_for;

async fn insert_snapshot_with_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state)
         VALUES ($1, $2, 'full', $3)"
    ))
    .bind(SnapshotId::new())
    .bind(sandbox_id)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// An upload is in flight when the sandbox has a `creating` snapshot that is
/// not a fork copy: not when it has no snapshots, only `ready` ones, or only a
/// `creating` fork copy.
#[tokio::test(flavor = "current_thread")]
async fn in_flight_idle_for_classifies_states() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 'suspending', 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;

    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "no snapshots → not in flight"
    );

    insert_snapshot_with_state(&db.pool, sandbox_id, "ready").await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "only 'ready' snapshots → not in flight"
    );

    crate::disk_artifact::insert_creating_fork_copy(
        &db.pool,
        SnapshotId::new(),
        sandbox_id,
        OrgId::from_uuid(SEED_ORG_ID),
    )
    .await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_none(),
        "only a 'creating' fork copy → not in flight"
    );

    insert_snapshot_with_state(&db.pool, sandbox_id, "creating").await?;
    assert!(
        in_flight_idle_for(&db.pool, sandbox_id).await?.is_some(),
        "one 'creating' snapshot → in flight"
    );
    Ok(())
}
