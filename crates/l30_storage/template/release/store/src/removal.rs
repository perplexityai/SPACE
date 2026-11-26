//! Deletes of releases and their storage locations, steps of the template
//! delete and the admin release delete. The caller holds the template's row
//! lock, runs its own checks, and removes the releases' tags before it
//! deletes the releases, in one transaction. The admin release delete also
//! locks the release's digest with [`lock_releases_of_digest`] before its
//! checks.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Delete the storage locations of every release of `template_name`.
pub async fn delete_storage_locations_of_template(
    executor: impl Executor<'_>,
    template_name: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations
         WHERE template_release_id IN (
           SELECT id FROM template_releases WHERE template_name = $1
         )"
    ))
    .bind(template_name)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete every release of `template_name`. A row that still references one
/// fails it with a foreign-key violation, which the caller maps.
pub async fn delete_releases_of_template(
    executor: impl Executor<'_>,
    template_name: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = $1"
    ))
    .bind(template_name)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete the storage locations of release `release_id`.
pub async fn delete_storage_locations_of_release(
    executor: impl Executor<'_>,
    release_id: Uuid,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations WHERE template_release_id = $1"
    ))
    .bind(release_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete release `release_id`; `false` when there is none. A row that still
/// references it fails the delete with a foreign-key violation, which the
/// caller maps.
pub async fn delete_release(
    executor: impl Executor<'_>,
    release_id: Uuid,
) -> Result<bool, DbError> {
    let deleted = sqlx::query(sql!("DELETE FROM template_releases WHERE id = $1"))
        .bind(release_id)
        .execute(executor.into_sqlx())
        .await?
        .rows_affected();
    Ok(deleted > 0)
}

/// Lock `FOR UPDATE` every release of the digest release `release_id` has,
/// under either spelling, itself included, until the caller's transaction
/// ends; nothing when there is no such release. The admin release delete
/// takes it so that a tag insert naming another release of the digest
/// waits for the delete's tag check. The caller must pass its open
/// transaction.
pub async fn lock_releases_of_digest<C>(tx: &mut C, release_id: Uuid) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT 1
             FROM template_releases r
             JOIN template_releases digest
               ON digest.template_name = r.template_name
              AND digest.hash IN (replace(r.hash, 'sha256:', ''),
                                  'sha256:' || replace(r.hash, 'sha256:', ''))
             WHERE r.id = $1
             FOR UPDATE OF digest"
    ))
    .bind(release_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}
