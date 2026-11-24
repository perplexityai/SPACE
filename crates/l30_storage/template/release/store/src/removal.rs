//! Deletes of releases and their storage locations, steps of the template
//! delete and the admin release delete. The caller holds the template's row
//! lock, runs its own checks, and removes the releases' tags before it
//! deletes the releases, in one transaction.

use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// Delete the storage locations of every release of `template_name`.
pub async fn delete_storage_locations_of_template(
    executor: impl PgExecutor<'_>,
    template_name: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations
         WHERE template_release_id IN (
           SELECT id FROM template_releases WHERE template_name = $1
         )"
    ))
    .bind(template_name)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete every release of `template_name`. A row that still references one
/// fails it with a foreign-key violation, which the caller maps.
pub async fn delete_releases_of_template(
    executor: impl PgExecutor<'_>,
    template_name: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = $1"
    ))
    .bind(template_name)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete the storage locations of release `release_id`.
pub async fn delete_storage_locations_of_release(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations WHERE template_release_id = $1"
    ))
    .bind(release_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete release `release_id`; `false` when there is none. A row that still
/// references it fails the delete with a foreign-key violation, which the
/// caller maps.
pub async fn delete_release(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let deleted = sqlx::query(sql!("DELETE FROM template_releases WHERE id = $1"))
        .bind(release_id)
        .execute(executor)
        .await?
        .rows_affected();
    Ok(deleted > 0)
}
