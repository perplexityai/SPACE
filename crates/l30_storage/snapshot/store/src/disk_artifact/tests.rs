use pplx_space_accounts::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    attach_fork_copy_location, delete_unlocated_fork_copy, insert_creating_fork_copy,
    mark_fork_copy_ready, orphan_fork_copy_location, rebind_fork_copy_owner,
};

const ROUTE: &str = "s3://fork-copies/copy?region=us-east-1";

fn org() -> OrgId {
    OrgId::from_uuid(SEED_ORG_ID)
}

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

/// A `creating` fork copy owned by `owner` with a live location.
async fn fork_copy(pool: &PgPool, owner: SandboxId) -> TestResult<(SnapshotId, Uuid)> {
    let snapshot_id = SnapshotId::new();
    let location_id = Uuid::now_v7();
    insert_creating_fork_copy(pool, snapshot_id, owner, org()).await?;
    attach_fork_copy_location(pool, location_id, snapshot_id, ROUTE, org()).await?;
    Ok((snapshot_id, location_id))
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct StoredCopy {
    sandbox_id: SandboxId,
    state: String,
    size_bytes: Option<i64>,
}

async fn stored_copy(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<Option<StoredCopy>> {
    Ok(sqlx::query_as(sql!(
        "SELECT sandbox_id, state, size_bytes FROM snapshots WHERE id = $1"
    ))
    .bind(snapshot_id)
    .fetch_optional(pool)
    .await?)
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct StoredLocation {
    snapshot_id: Option<SnapshotId>,
    storage_connection_string: String,
    orphaned: bool,
}

async fn stored_location(pool: &PgPool, location_id: Uuid) -> TestResult<StoredLocation> {
    Ok(sqlx::query_as(sql!(
        "SELECT snapshot_id, storage_connection_string, orphaned_at IS NOT NULL AS orphaned
           FROM snapshot_storage_locations
          WHERE id = $1"
    ))
    .bind(location_id)
    .fetch_one(pool)
    .await?)
}

/// An orphaned location is re-adopted by a retried copy; a live one is left
/// pointing at its snapshot.
#[tokio::test(flavor = "current_thread")]
async fn attach_fork_copy_location_adopts_only_an_orphan() -> TestResult {
    let db = pg_test_db().await?;
    let owner = insert_sandbox(&db.pool).await?;
    let (live, location_id) = fork_copy(&db.pool, owner).await?;
    let other = SnapshotId::new();
    insert_creating_fork_copy(&db.pool, other, owner, org()).await?;

    assert!(
        !attach_fork_copy_location(&db.pool, location_id, other, "s3://elsewhere/x", org()).await?
    );
    assert_eq!(
        stored_location(&db.pool, location_id).await?,
        StoredLocation {
            snapshot_id: Some(live),
            storage_connection_string: ROUTE.to_owned(),
            orphaned: false,
        }
    );

    assert!(orphan_fork_copy_location(&db.pool, live, location_id).await?);
    assert!(
        attach_fork_copy_location(&db.pool, location_id, other, "s3://elsewhere/x", org()).await?
    );
    assert_eq!(
        stored_location(&db.pool, location_id).await?,
        StoredLocation {
            snapshot_id: Some(other),
            storage_connection_string: "s3://elsewhere/x".to_owned(),
            orphaned: false,
        }
    );
    Ok(())
}

/// A `creating` copy is marked `ready`, and marking it again succeeds again.
/// The losing case: the wrong owner matches nothing and changes nothing.
#[tokio::test(flavor = "current_thread")]
async fn mark_fork_copy_ready_requires_the_owner() -> TestResult {
    let db = pg_test_db().await?;
    let owner = insert_sandbox(&db.pool).await?;
    let stranger = insert_sandbox(&db.pool).await?;
    let (snapshot_id, _) = fork_copy(&db.pool, owner).await?;

    assert!(!mark_fork_copy_ready(&db.pool, snapshot_id, stranger, 128).await?);
    assert_eq!(
        stored_copy(&db.pool, snapshot_id).await?,
        Some(StoredCopy {
            sandbox_id: owner,
            state: "creating".to_owned(),
            size_bytes: None,
        })
    );

    for size_bytes in [128, 256] {
        assert!(mark_fork_copy_ready(&db.pool, snapshot_id, owner, size_bytes).await?);
        assert_eq!(
            stored_copy(&db.pool, snapshot_id).await?,
            Some(StoredCopy {
                sandbox_id: owner,
                state: "ready".to_owned(),
                size_bytes: Some(size_bytes),
            })
        );
    }
    Ok(())
}

/// The losing case for a copy no longer `creating` or `ready`: nothing
/// changes.
#[tokio::test(flavor = "current_thread")]
async fn mark_fork_copy_ready_leaves_a_failed_copy() -> TestResult {
    let db = pg_test_db().await?;
    let owner = insert_sandbox(&db.pool).await?;
    let (snapshot_id, _) = fork_copy(&db.pool, owner).await?;
    sqlx::query(sql!("UPDATE snapshots SET state = 'failed' WHERE id = $1"))
        .bind(snapshot_id)
        .execute(&db.pool)
        .await?;

    assert!(!mark_fork_copy_ready(&db.pool, snapshot_id, owner, 128).await?);
    assert_eq!(
        stored_copy(&db.pool, snapshot_id)
            .await?
            .map(|copy| copy.state),
        Some("failed".to_owned())
    );
    Ok(())
}

/// A ready copy is handed to the target, and handing it again succeeds again.
/// The losing cases: a copy not yet `ready`, and a source that owns nothing,
/// match nothing and change nothing.
#[tokio::test(flavor = "current_thread")]
async fn rebind_fork_copy_owner_moves_only_a_ready_copy_of_either_sandbox() -> TestResult {
    let db = pg_test_db().await?;
    let source = insert_sandbox(&db.pool).await?;
    let target = insert_sandbox(&db.pool).await?;
    let stranger = insert_sandbox(&db.pool).await?;
    let (snapshot_id, _) = fork_copy(&db.pool, source).await?;

    assert!(!rebind_fork_copy_owner(&db.pool, snapshot_id, source, target).await?);
    assert_eq!(
        stored_copy(&db.pool, snapshot_id)
            .await?
            .map(|copy| copy.sandbox_id),
        Some(source)
    );

    assert!(mark_fork_copy_ready(&db.pool, snapshot_id, source, 128).await?);
    let other_target = insert_sandbox(&db.pool).await?;
    assert!(!rebind_fork_copy_owner(&db.pool, snapshot_id, stranger, other_target).await?);
    assert_eq!(
        stored_copy(&db.pool, snapshot_id)
            .await?
            .map(|copy| copy.sandbox_id),
        Some(source)
    );

    for _ in 0..2 {
        assert!(rebind_fork_copy_owner(&db.pool, snapshot_id, source, target).await?);
        assert_eq!(
            stored_copy(&db.pool, snapshot_id)
                .await?
                .map(|copy| copy.sandbox_id),
            Some(target)
        );
    }
    Ok(())
}

/// Orphaning detaches a live location once. The losing cases: a location
/// already orphaned, and one that belongs to another snapshot, match nothing
/// and change nothing.
#[tokio::test(flavor = "current_thread")]
async fn orphan_fork_copy_location_detaches_only_its_live_location() -> TestResult {
    let db = pg_test_db().await?;
    let owner = insert_sandbox(&db.pool).await?;
    let (snapshot_id, location_id) = fork_copy(&db.pool, owner).await?;
    let (other, other_location) = fork_copy(&db.pool, owner).await?;

    assert!(!orphan_fork_copy_location(&db.pool, snapshot_id, other_location).await?);
    assert_eq!(
        stored_location(&db.pool, other_location).await?.snapshot_id,
        Some(other)
    );

    assert!(orphan_fork_copy_location(&db.pool, snapshot_id, location_id).await?);
    let orphaned = stored_location(&db.pool, location_id).await?;
    assert_eq!(orphaned.snapshot_id, None);
    assert!(orphaned.orphaned);

    assert!(!orphan_fork_copy_location(&db.pool, snapshot_id, location_id).await?);
    Ok(())
}

/// A fork copy is deleted once its location is orphaned. The losing cases: a
/// copy with a live location, and a snapshot of another origin, match nothing
/// and stay.
#[tokio::test(flavor = "current_thread")]
async fn delete_unlocated_fork_copy_needs_no_live_location_and_a_fork_origin() -> TestResult {
    let db = pg_test_db().await?;
    let owner = insert_sandbox(&db.pool).await?;
    let (snapshot_id, location_id) = fork_copy(&db.pool, owner).await?;

    assert!(!delete_unlocated_fork_copy(&db.pool, snapshot_id).await?);
    assert!(stored_copy(&db.pool, snapshot_id).await?.is_some());

    let suspend = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, org_id)
         VALUES ($1, $2, 'full', 'ready', $3::UUID)"
    ))
    .bind(suspend)
    .bind(owner)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    assert!(!delete_unlocated_fork_copy(&db.pool, suspend).await?);
    assert!(stored_copy(&db.pool, suspend).await?.is_some());

    assert!(orphan_fork_copy_location(&db.pool, snapshot_id, location_id).await?);
    assert!(delete_unlocated_fork_copy(&db.pool, snapshot_id).await?);
    assert!(stored_copy(&db.pool, snapshot_id).await?.is_none());
    Ok(())
}
