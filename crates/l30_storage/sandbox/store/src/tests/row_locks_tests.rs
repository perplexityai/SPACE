//! The sandbox row locks at the store layer: what each returns, that a
//! matched lock lasts until the caller's transaction ends in the mode its
//! verb names, and that a guarded lock takes no lock on a row whose guard
//! already fails when the statement reads it.
use super::*;
use crate::{restore_dispatch_fence, restore_lease, row_locks, suspend};
use pplx_space_snapshot_model::ArtifactKind;

/// Whether another transaction finds the row locked: its own
/// `FOR UPDATE NOWAIT` fails with `lock_not_available`. Every row-lock mode
/// refuses it.
async fn row_locked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR KEY SHARE NOWAIT` on the row, the lock
/// a child-row insert's foreign-key check takes, fails with
/// `lock_not_available`. Only `FOR UPDATE` refuses it, so this pins the
/// lock's mode.
async fn key_share_blocked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

/// Whether another transaction's `FOR SHARE NOWAIT` on the row fails with
/// `lock_not_available`. `FOR NO KEY UPDATE` and `FOR UPDATE` refuse it, so
/// with [`key_share_blocked`] it pins a `FOR NO KEY UPDATE` lock exactly.
async fn share_blocked(pool: &PgPool, id: Uuid) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let result = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    lock_not_available(result)
}

fn lock_not_available(
    result: Result<Option<sqlx::postgres::PgRow>, sqlx::Error>,
) -> TestResult<bool> {
    match result {
        Ok(_) => Ok(false),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(true),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn id_only_row_locks_report_the_row_and_hold_until_the_transaction_ends() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = insert_sandbox(&db.pool, None, "started").await?;
    let sandbox = SandboxId::from_uuid(id);
    let missing = SandboxId::new();

    let mut tx = db.pool.begin().await?;
    assert_eq!(row_locks::lock_id(&mut tx, sandbox).await?, Some(sandbox));
    assert!(row_locked(&db.pool, id).await?);
    assert!(key_share_blocked(&db.pool, id).await?);
    assert_eq!(
        row_locks::lock_lifecycle(&mut tx, sandbox)
            .await?
            .as_deref(),
        Some("started")
    );
    assert!(row_locks::lock_exists(&mut tx, sandbox).await?);
    assert_eq!(row_locks::lock_id(&mut tx, missing).await?, None);
    assert_eq!(row_locks::lock_lifecycle(&mut tx, missing).await?, None);
    assert!(!row_locks::lock_exists(&mut tx, missing).await?);
    tx.commit().await?;
    assert!(!row_locked(&db.pool, id).await?);
    assert!(!key_share_blocked(&db.pool, id).await?);

    // Each verb takes the lock itself, not only under `lock_id`'s.
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        row_locks::lock_lifecycle(&mut tx, sandbox)
            .await?
            .as_deref(),
        Some("started")
    );
    assert!(row_locked(&db.pool, id).await?);
    assert!(key_share_blocked(&db.pool, id).await?);
    tx.rollback().await?;
    let mut tx = db.pool.begin().await?;
    assert!(row_locks::lock_exists(&mut tx, sandbox).await?);
    assert!(row_locked(&db.pool, id).await?);
    assert!(key_share_blocked(&db.pool, id).await?);
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn no_key_update_row_locks_report_the_row_and_let_key_share_through_until_the_transaction_ends()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    let placed = insert_sandbox(pool, None, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(placed)
    .bind(node_id)
    .execute(pool)
    .await?;
    let placed_id = SandboxId::from_uuid(placed);
    let unplaced = insert_sandbox(pool, None, "started").await?;
    let unplaced_id = SandboxId::from_uuid(unplaced);
    let missing = SandboxId::new();

    // A `FOR SHARE` probe is refused and a `FOR KEY SHARE` one, the lock a
    // child-row insert's foreign-key check takes, is not: exactly
    // `FOR NO KEY UPDATE`.
    let mut tx = pool.begin().await?;
    assert_eq!(
        row_locks::lock_id_no_key_update(&mut tx, placed_id).await?,
        Some(placed_id)
    );
    assert!(share_blocked(pool, placed).await?);
    assert!(!key_share_blocked(pool, placed).await?);
    assert_eq!(
        row_locks::lock_id_no_key_update(&mut tx, missing).await?,
        None
    );
    assert!(share_blocked(pool, placed).await?);
    tx.commit().await?;
    assert!(!share_blocked(pool, placed).await?);
    assert!(!key_share_blocked(pool, placed).await?);

    // A row with no active node is locked too, and reads `Some(None)`.
    for (sandbox, id, active_node) in [
        (placed_id, placed, Some(node_id)),
        (unplaced_id, unplaced, None),
    ] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            row_locks::lock_active_node_id_no_key_update(&mut tx, sandbox).await?,
            Some(active_node)
        );
        assert!(share_blocked(pool, id).await?);
        assert!(!key_share_blocked(pool, id).await?);
        tx.rollback().await?;
        assert!(!share_blocked(pool, id).await?);
        assert!(!key_share_blocked(pool, id).await?);
    }
    let mut tx = pool.begin().await?;
    assert_eq!(
        row_locks::lock_active_node_id_no_key_update(&mut tx, missing).await?,
        None
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn guarded_row_locks_leave_a_row_that_already_fails_their_guard_unlocked() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let token = Uuid::now_v7();
    let other_token = Uuid::now_v7();

    // A `restoring` row under `token`.
    let restoring = insert_sandbox(pool, None, "restoring").await?;
    let restoring_id = SandboxId::from_uuid(restoring);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '1 hour'
          WHERE id = $1"
    ))
    .bind(restoring)
    .bind(token)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    assert!(!restore_lease::lock_restore_claim(&mut *tx, restoring_id, Some(other_token)).await?);
    assert!(!row_locked(pool, restoring).await?);
    assert!(!key_share_blocked(pool, restoring).await?);
    assert_eq!(
        restore_lease::lock_restore_lease_token(&mut *tx, restoring_id).await?,
        Some(Some(token))
    );
    assert!(row_locked(pool, restoring).await?);
    assert!(key_share_blocked(pool, restoring).await?);
    tx.rollback().await?;

    for claim_token in [Some(token), None] {
        let mut tx = pool.begin().await?;
        assert!(restore_lease::lock_restore_claim(&mut *tx, restoring_id, claim_token).await?);
        assert!(row_locked(pool, restoring).await?);
        assert!(key_share_blocked(pool, restoring).await?);
        tx.rollback().await?;
    }

    // The dispatch lock answers whether the row is still the owner's, and
    // locks it whatever the answer.
    for (owner_token, owned) in [(token, true), (other_token, false)] {
        let mut tx = pool.begin().await?;
        assert_eq!(
            restore_dispatch_fence::lock_dispatch_ownership(&mut *tx, restoring_id, owner_token)
                .await?,
            Some(owned)
        );
        assert!(row_locked(pool, restoring).await?);
        assert!(key_share_blocked(pool, restoring).await?);
        tx.rollback().await?;
    }

    // A row that left `restoring` matches no revert claim.
    let suspended = insert_sandbox(pool, None, "suspended").await?;
    let suspended_id = SandboxId::from_uuid(suspended);
    let mut tx = pool.begin().await?;
    assert!(!restore_lease::lock_restore_claim(&mut *tx, suspended_id, None).await?);
    assert!(!row_locked(pool, suspended).await?);
    assert!(!key_share_blocked(pool, suspended).await?);
    assert_eq!(
        restore_lease::lock_restore_lease_token(&mut *tx, SandboxId::new()).await?,
        None
    );
    assert_eq!(
        restore_dispatch_fence::lock_dispatch_ownership(&mut *tx, SandboxId::new(), token).await?,
        None
    );
    tx.rollback().await?;

    // The pod retirement's lock needs the committed suspend artifact.
    let artifact = LocalArtifactId::from_uuid(Uuid::now_v7());
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspending',
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(suspended)
    .bind(artifact.as_uuid())
    .execute(pool)
    .await?;
    let other_artifact = LocalArtifactId::from_uuid(Uuid::now_v7());
    let mut tx = pool.begin().await?;
    assert_eq!(
        suspend::lock_committed_artifact(&mut tx, suspended_id, other_artifact, ArtifactKind::Full)
            .await?,
        None
    );
    assert!(!row_locked(pool, suspended).await?);
    assert!(!key_share_blocked(pool, suspended).await?);
    assert_eq!(
        suspend::lock_committed_artifact(&mut tx, suspended_id, artifact, ArtifactKind::Full)
            .await?
            .as_deref(),
        Some("suspending")
    );
    assert!(row_locked(pool, suspended).await?);
    assert!(key_share_blocked(pool, suspended).await?);
    tx.rollback().await?;
    Ok(())
}
