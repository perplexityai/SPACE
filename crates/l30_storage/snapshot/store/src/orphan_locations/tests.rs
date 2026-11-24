use std::time::Duration;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{OrphanLocation, delete_if_orphan, page};

/// A storage location with route `route`, attached to `snapshot_id` or
/// detached, and stamped orphaned `orphaned_secs` ago or not at all.
async fn insert_location(
    pool: &PgPool,
    snapshot_id: Option<SnapshotId>,
    route: &str,
    orphaned_secs: Option<i64>,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
            (id, snapshot_id, storage_connection_string, org_id, orphaned_at)
         VALUES ($1, $2, $3, $4::UUID,
                 CASE WHEN $5::BIGINT IS NULL THEN NULL
                      ELSE NOW() - make_interval(secs => $5::BIGINT) END)"
    ))
    .bind(id)
    .bind(snapshot_id)
    .bind(route)
    .bind(SEED_ORG_ID)
    .bind(orphaned_secs)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_snapshot(pool: &PgPool) -> TestResult<SnapshotId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, org_id)
         VALUES ($1, $2, 'full', 'ready', $3::UUID)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn location_exists(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM snapshot_storage_locations WHERE id = $1)"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The page lists only orphans, past the cursor and the age gate, in id
/// order, and flags an encrypted-fields row; a row with only one half of the
/// orphan marking is never listed.
#[tokio::test(flavor = "current_thread")]
async fn page_lists_orphans_past_the_cursor_and_age() -> TestResult {
    let db = pg_test_db().await?;
    let snapshot_id = insert_snapshot(&db.pool).await?;
    let before_cursor = insert_location(&db.pool, None, "s3://b/old", Some(7_200)).await?;
    let attached = insert_snapshot(&db.pool).await?;
    let _stamped_attached =
        insert_location(&db.pool, Some(attached), "s3://b/stamped", Some(7_200)).await?;
    let _detached_unstamped = insert_location(&db.pool, None, "s3://b/unstamped", None).await?;
    let old = insert_location(&db.pool, None, "s3://b/old-2", Some(7_200)).await?;
    let encrypted = insert_location(&db.pool, None, "", Some(7_200)).await?;
    let young = insert_location(&db.pool, None, "s3://b/young", Some(60)).await?;
    let _live = insert_location(&db.pool, Some(snapshot_id), "s3://b/live", None).await?;
    let master_key_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO org_master_keys (org_id, provider, key_identifier)
         VALUES ($1::UUID, 'test', 'test-key')
         RETURNING id"
    ))
    .bind(SEED_ORG_ID)
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_location_encrypted_fields
             (snapshot_storage_location_id, org_id, master_key_id, wrapped_dek,
              storage_connection_string)
         VALUES ($1, $2::UUID, $3, '\\x01', '\\x02')"
    ))
    .bind(encrypted)
    .bind(SEED_ORG_ID)
    .bind(master_key_id)
    .execute(&db.pool)
    .await?;

    let gated = page(
        &db.pool,
        Some(before_cursor),
        10,
        Some(Duration::from_secs(3_600)),
    )
    .await?;
    assert_eq!(
        gated,
        vec![
            OrphanLocation {
                id: old,
                storage_connection_string: "s3://b/old-2".to_owned(),
                has_encrypted_fields: false,
            },
            OrphanLocation {
                id: encrypted,
                storage_connection_string: String::new(),
                has_encrypted_fields: true,
            },
        ]
    );

    let ungated: Vec<Uuid> = page(&db.pool, Some(before_cursor), 10, None)
        .await?
        .into_iter()
        .map(|location| location.id)
        .collect();
    assert_eq!(ungated, vec![old, encrypted, young]);

    let limited: Vec<Uuid> = page(&db.pool, None, 1, None)
        .await?
        .into_iter()
        .map(|location| location.id)
        .collect();
    assert_eq!(limited.len(), 1);
    Ok(())
}

/// An orphan is deleted. The losing cases: a live location, rows with only one
/// half of the orphan marking (stamped but attached, detached but unstamped),
/// and a missing row match nothing, and the rows stay.
#[tokio::test(flavor = "current_thread")]
async fn delete_if_orphan_deletes_only_an_orphan() -> TestResult {
    let db = pg_test_db().await?;
    let snapshot_id = insert_snapshot(&db.pool).await?;
    let orphan = insert_location(&db.pool, None, "s3://b/orphan", Some(60)).await?;
    let live = insert_location(&db.pool, Some(snapshot_id), "s3://b/live", None).await?;
    let unstamped = insert_location(&db.pool, None, "s3://b/unstamped", None).await?;
    let attached = insert_snapshot(&db.pool).await?;
    let stamped = insert_location(&db.pool, Some(attached), "s3://b/stamped", Some(60)).await?;

    assert!(!delete_if_orphan(&db.pool, live).await?);
    assert!(!delete_if_orphan(&db.pool, unstamped).await?);
    assert!(!delete_if_orphan(&db.pool, stamped).await?);
    assert!(!delete_if_orphan(&db.pool, Uuid::now_v7()).await?);
    assert!(location_exists(&db.pool, live).await?);
    assert!(location_exists(&db.pool, unstamped).await?);
    assert!(location_exists(&db.pool, stamped).await?);

    assert!(delete_if_orphan(&db.pool, orphan).await?);
    assert!(!location_exists(&db.pool, orphan).await?);
    Ok(())
}
