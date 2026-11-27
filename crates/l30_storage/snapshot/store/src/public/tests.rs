use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{get, page};

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test(flavor = "current_thread")]
async fn public_snapshot_queries_hide_fork_origin_rows() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let public_snapshot_id = SnapshotId::new();
    let fork_snapshot_id = SnapshotId::new();
    for (id, origin) in [(public_snapshot_id, "snapshot"), (fork_snapshot_id, "fork")] {
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id,
                 artifact_kind, artifact_origin,
                 artifact_snapshot_name
             ) VALUES ($1, $2, 'disk', 'ready', $3, 'disk', $4, $5)"
        ))
        .bind(id)
        .bind(sandbox_id)
        .bind((origin == "snapshot").then(Uuid::now_v7))
        .bind(origin)
        .bind((origin == "snapshot").then_some("checkpoint"))
        .execute(&db.pool)
        .await?;
    }

    let listed = page(&db.pool, org_id, sandbox_id, None, 101).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, public_snapshot_id);
    assert!(
        get(&db.pool, org_id, sandbox_id, fork_snapshot_id)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn public_snapshot_queries_hide_deleting_rows() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let ready_snapshot_id = SnapshotId::new();
    let deleting_snapshot_id = SnapshotId::new();
    for (id, state) in [
        (ready_snapshot_id, "ready"),
        (deleting_snapshot_id, "deleting"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id,
                 artifact_kind, artifact_origin, artifact_snapshot_name
             ) VALUES ($1, $2, 'disk', $3, $4, 'disk', 'snapshot', 'checkpoint')"
        ))
        .bind(id)
        .bind(sandbox_id)
        .bind(state)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
    }

    // A claimed row is de-advertised: absent from the list and a 404 (None)
    // on point lookup, never a parse failure on the closed wire enum.
    let listed = page(&db.pool, org_id, sandbox_id, None, 101).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, ready_snapshot_id);
    assert!(
        get(&db.pool, org_id, sandbox_id, deleting_snapshot_id)
            .await?
            .is_none()
    );
    Ok(())
}
