//! The tags that name a template's releases (`template_release_tags`).
//!
//! The tag move (the admin route that points a tag at another release)
//! locks the tag row with [`current_release_locked`] and compares it at its
//! call site, in its transaction; the retarget of a tag it holds locked is
//! [`retarget`], and the insert of a tag it found absent is
//! [`insert_if_absent`]. Release registration share-locks the tags it
//! confirms with [`share_lock_tags_of_digest`]. Each locking verb takes the
//! caller's transaction, and its lock lasts until that transaction ends.

use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateName, TemplateTag};
use sqlx::{FromRow, PgExecutor, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

/// One tag of a release.
#[derive(Debug, FromRow)]
pub struct ReleaseTag {
    pub template_release_id: Uuid,
    pub tag: String,
}

/// The tags of `release_ids`, by release, then tag.
pub async fn list_for_releases(
    executor: impl PgExecutor<'_>,
    release_ids: &[Uuid],
) -> Result<Vec<ReleaseTag>, sqlx::Error> {
    sqlx::query_as::<_, ReleaseTag>(sql!(
        "SELECT template_release_id, tag
         FROM template_release_tags
         WHERE template_release_id = ANY($1)
         ORDER BY template_release_id, tag"
    ))
    .bind(release_ids)
    .fetch_all(executor)
    .await
}

/// Point `tag` of `template_name` at `release_id`. `false` when the template
/// has no such tag.
pub async fn retarget(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &TemplateTag,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE template_release_tags
                     SET template_release_id = $1,
                         modified_at = NOW()
                     WHERE template_name = $2 AND tag = $3"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag.as_str())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Create `tag` of `template_name`, pointing at `release_id`. `false` when
/// the template already has a tag of that name, which may have committed
/// after the caller found it absent.
pub async fn insert_if_absent(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &TemplateTag,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (template_name, tag) DO NOTHING"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag.as_str())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// A tag as release registration reads it back.
#[derive(Debug, FromRow)]
pub struct RegisteredTag {
    pub id: Uuid,
    pub tag: String,
    pub created_at: OffsetDateTime,
}

/// Point `tag` of `template_name` at `release_id`, creating the tag if the
/// template has none of that name.
pub async fn upsert(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &str,
) -> Result<RegisteredTag, sqlx::Error> {
    sqlx::query_as::<_, RegisteredTag>(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)
         ON CONFLICT (template_name, tag) DO UPDATE
             SET template_release_id = EXCLUDED.template_release_id,
                 modified_at = NOW()
         RETURNING id, tag, created_at"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag)
    .fetch_one(executor)
    .await
}

/// Delete `tag` from `release_id`, if it names that release.
pub async fn delete(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    tag: &TemplateTag,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags
         WHERE template_release_id = $1 AND tag = $2"
    ))
    .bind(release_id)
    .bind(tag.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete every tag of `release_id`.
pub async fn delete_for_release(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_release_id = $1"
    ))
    .bind(release_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Delete every tag of every release of `template`.
pub async fn delete_for_template(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// The release `tag` of `template_name` points at, or `None` when the
/// template has no such tag, locking the tag's row `FOR UPDATE` until the
/// caller's transaction ends.
pub async fn current_release_locked(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
    tag: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT template_release_id
         FROM template_release_tags
         WHERE template_name = $1 AND tag = $2
         FOR UPDATE"
    ))
    .bind(template_name)
    .bind(tag)
    .fetch_optional(&mut **tx)
    .await
}

/// The tags among `tags` of `template_name` that name a release of the
/// digest stored as `hash` (bare `bare_hash`), under either spelling, by
/// tag, share-locking each tag's row `FOR SHARE` until the caller's
/// transaction ends.
pub async fn share_lock_tags_of_digest(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
    tags: &[String],
    hash: &str,
    bare_hash: &str,
) -> Result<Vec<RegisteredTag>, sqlx::Error> {
    sqlx::query_as::<_, RegisteredTag>(sql!(
        "SELECT t.id, t.tag, t.created_at
         FROM template_release_tags t
         JOIN template_releases r ON r.id = t.template_release_id
         WHERE t.template_name = $1
           AND t.tag = ANY($2)
           AND r.hash IN ($3, $4, 'sha256:' || $4)
         ORDER BY t.tag
         FOR SHARE OF t"
    ))
    .bind(template_name)
    .bind(tags)
    .bind(hash)
    .bind(bare_hash)
    .fetch_all(&mut **tx)
    .await
}
