use pplx_space_accounts::OrgId;
use pplx_space_artifact_model::ExactStorageUri;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{orphan_pending, orphan_unowned_location};

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
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
    Ok(sandbox_id)
}

/// A ready snapshot of `(artifact_id, kind)` with one storage location and
/// one artifact digest.
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    kind: &str,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    let origin = if kind == "full" { "suspend" } else { "stop" };
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, timestamp, local_artifact_id,
             artifact_kind, artifact_origin, org_id)
         VALUES ($1, $2, $3, 'ready', NOW(), $4, $3, $5, $6::UUID)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(kind)
    .bind(artifact_id)
    .bind(origin)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
            (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3::UUID)"
    ))
    .bind(snapshot_id)
    .bind(uri_of(sandbox_id, snapshot_id))
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

/// The location URI [`insert_snapshot`] gives a snapshot.
fn uri_of(sandbox_id: SandboxId, snapshot_id: SnapshotId) -> String {
    format!("s3://test-bucket/{sandbox_id}/{snapshot_id}?region=us-east-1")
}

/// Every orphaned location's URI, sorted. Exact URIs, not a count or a
/// prefix match: the sweeper deletes exactly what these name.
async fn orphaned_uris(pool: &PgPool) -> TestResult<Vec<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT storage_connection_string
           FROM snapshot_storage_locations
          WHERE snapshot_id IS NULL
            AND orphaned_at IS NOT NULL
          ORDER BY storage_connection_string"
    ))
    .fetch_all(pool)
    .await?)
}

/// The URI of the location still attached to `snapshot_id`, if any.
async fn attached_uri(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT storage_connection_string
           FROM snapshot_storage_locations
          WHERE snapshot_id = $1
            AND orphaned_at IS NULL"
    ))
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn an_unowned_prefix_goes_to_the_sweeper_and_an_owned_one_doesnt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let unowned = SnapshotId::new();
    let uri = ExactStorageUri::parse(&uri_of(sandbox_id, unowned))?;

    assert!(orphan_unowned_location(&db.pool, unowned, &uri, org_id).await?);
    let unowned_only = vec![uri_of(sandbox_id, unowned)];
    assert_eq!(orphaned_uris(&db.pool).await?, unowned_only);

    let owned = insert_snapshot(&db.pool, sandbox_id, LocalArtifactId::new(), "disk").await?;
    let uri = ExactStorageUri::parse(&uri_of(sandbox_id, owned))?;
    assert!(!orphan_unowned_location(&db.pool, owned, &uri, org_id).await?);
    assert_eq!(orphaned_uris(&db.pool).await?, unowned_only);
    assert_eq!(
        attached_uri(&db.pool, owned).await?,
        Some(uri_of(sandbox_id, owned))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_prefix_is_pending_only_while_its_orphan_row_exists() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandbox_id = insert_sandbox(&db.pool).await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let orphaned = SnapshotId::new();
    let uri = ExactStorageUri::parse(&uri_of(sandbox_id, orphaned))?;
    assert!(!orphan_pending(&db.pool, &uri).await?);

    assert!(orphan_unowned_location(&db.pool, orphaned, &uri, org_id).await?);
    assert!(orphan_pending(&db.pool, &uri).await?);

    // A location still attached to a snapshot isn't the sweeper's.
    let owned = insert_snapshot(&db.pool, sandbox_id, LocalArtifactId::new(), "disk").await?;
    let owned_uri = ExactStorageUri::parse(&uri_of(sandbox_id, owned))?;
    assert!(!orphan_pending(&db.pool, &owned_uri).await?);

    // The sweeper removes the row once it has deleted the bytes.
    sqlx::query(sql!(
        "DELETE FROM snapshot_storage_locations
          WHERE snapshot_id IS NULL AND storage_connection_string = $1"
    ))
    .bind(uri.as_wire())
    .execute(&db.pool)
    .await?;
    assert!(!orphan_pending(&db.pool, &uri).await?);
    Ok(())
}
