//! [`direct::lock_restore_lease_owner`] takes the row's `FOR UPDATE` lock
//! only while the row holds the caller's restore lease token, and the lock
//! lasts until the caller's transaction ends.

use super::*;
use crate::direct;

/// Whether another transaction can take `FOR KEY SHARE` on the row without
/// waiting. Of the row-level locks, only `FOR UPDATE` refuses it, so `false`
/// pins that mode exactly. The probe's transaction is rolled back.
async fn key_share_is_free(pool: &PgPool, id: SandboxId) -> TestResult<bool> {
    let mut probe = pool.begin().await?;
    let probed = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(id)
    .fetch_optional(&mut *probe)
    .await;
    probe.rollback().await?;
    match probed {
        Ok(_) => Ok(true),
        Err(error)
            if error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref()
                == Some("55P03") =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lock_restore_lease_owner_locks_the_row_for_update_only_for_the_holder() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = SandboxId::from_uuid(insert_sandbox(&db.pool, None, "suspended").await?);
    let token = Uuid::now_v7();
    assert!(
        direct::claim_restore_lease(&db.pool, id, 60, token)
            .await?
            .is_some()
    );

    let mut owner = db.pool.begin().await?;
    assert!(direct::lock_restore_lease_owner(&mut *owner, id, token).await?);
    assert!(
        !key_share_is_free(&db.pool, id).await?,
        "the holder's lock is FOR UPDATE"
    );
    owner.rollback().await?;
    assert!(
        key_share_is_free(&db.pool, id).await?,
        "a rollback ends the lock"
    );

    let mut owner = db.pool.begin().await?;
    assert!(direct::lock_restore_lease_owner(&mut *owner, id, token).await?);
    assert!(!key_share_is_free(&db.pool, id).await?);
    owner.commit().await?;
    assert!(
        key_share_is_free(&db.pool, id).await?,
        "a commit ends the lock"
    );

    let mut other = db.pool.begin().await?;
    assert!(!direct::lock_restore_lease_owner(&mut *other, id, Uuid::now_v7()).await?);
    assert!(
        key_share_is_free(&db.pool, id).await?,
        "another token takes no lock"
    );
    assert!(
        !direct::lock_restore_lease_owner(&mut *other, SandboxId::new(), token).await?,
        "a missing row is not held"
    );
    other.rollback().await?;
    Ok(())
}
