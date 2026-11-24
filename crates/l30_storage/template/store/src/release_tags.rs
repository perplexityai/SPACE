//! The tags that name a template's releases (`template_release_tags`).
//!
//! The tag move (the admin route that points a tag at another release)
//! locks the tag row with [`current_release_locked`] and compares it at its
//! call site, in its transaction; the retarget of a tag it holds locked is
//! [`retarget`], and the insert of a tag it found absent is
//! [`insert_if_absent`]. Release registration share-locks the tags it
//! confirms with [`share_lock_tags_of_digest`]. Each locking verb takes the
//! caller's transaction, and its lock lasts until that transaction ends.
//!
//! Every verb that points a tag at a release also records, in the same
//! statement, when a mutable tag first landed on it:
//! `template_releases.first_mutable_tag_at`, set when the tag isn't the
//! release's own digest in either spelling and the release has no time yet.
//! The tag is compared with both spellings exactly: a bare hash is always
//! SHA-256, so it and its `sha256:` form are one digest, and a hash with
//! another algorithm's prefix has only its own spelling. The time is the
//! transaction's, but never before the release's `created_at`, which
//! registration stamps with `clock_timestamp()` in the same transaction. A
//! release's digest tag never sets it, and moving a tag off the release never
//! clears it.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{TemplateName, TemplateTag};
use sqlx::FromRow;
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
    executor: impl Executor<'_>,
    release_ids: &[Uuid],
) -> Result<Vec<ReleaseTag>, DbError> {
    Ok(sqlx::query_as::<_, ReleaseTag>(sql!(
        "SELECT template_release_id, tag
         FROM template_release_tags
         WHERE template_release_id = ANY($1)
         ORDER BY template_release_id, tag"
    ))
    .bind(release_ids)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Point `tag` of `template_name` at `release_id`. `false` when the template
/// has no such tag.
pub async fn retarget(
    executor: impl Executor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &TemplateTag,
) -> Result<bool, DbError> {
    let moved = sqlx::query_scalar::<_, i64>(sql!(
        "WITH moved AS (
             UPDATE template_release_tags
             SET template_release_id = $1,
                 modified_at = NOW()
             WHERE template_name = $2 AND tag = $3
             RETURNING template_release_id, tag
         ), marked AS (
             UPDATE template_releases release
             SET first_mutable_tag_at = GREATEST(NOW(), release.created_at)
             FROM moved
             WHERE release.id = moved.template_release_id
               AND release.first_mutable_tag_at IS NULL
               AND moved.tag <> release.hash
               AND moved.tag <> CASE
                   WHEN release.hash LIKE 'sha256:%' THEN substr(release.hash, 8)
                   WHEN release.hash LIKE '%:%' THEN release.hash
                   ELSE 'sha256:' || release.hash
               END
         )
         SELECT COUNT(*) FROM moved"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag.as_str())
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(moved == 1)
}

/// Create `tag` of `template_name`, pointing at `release_id`. `false` when
/// the template already has a tag of that name, which may have committed
/// after the caller found it absent.
pub async fn insert_if_absent(
    executor: impl Executor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &TemplateTag,
) -> Result<bool, DbError> {
    let inserted = sqlx::query_scalar::<_, i64>(sql!(
        "WITH inserted AS (
             INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, $3)
             ON CONFLICT (template_name, tag) DO NOTHING
             RETURNING template_release_id, tag
         ), marked AS (
             UPDATE template_releases release
             SET first_mutable_tag_at = GREATEST(NOW(), release.created_at)
             FROM inserted
             WHERE release.id = inserted.template_release_id
               AND release.first_mutable_tag_at IS NULL
               AND inserted.tag <> release.hash
               AND inserted.tag <> CASE
                   WHEN release.hash LIKE 'sha256:%' THEN substr(release.hash, 8)
                   WHEN release.hash LIKE '%:%' THEN release.hash
                   ELSE 'sha256:' || release.hash
               END
         )
         SELECT COUNT(*) FROM inserted"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag.as_str())
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(inserted == 1)
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
    executor: impl Executor<'_>,
    release_id: Uuid,
    template_name: &str,
    tag: &str,
) -> Result<RegisteredTag, DbError> {
    Ok(sqlx::query_as::<_, RegisteredTag>(sql!(
        "WITH upserted AS (
             INSERT INTO template_release_tags (template_release_id, template_name, tag)
             VALUES ($1, $2, $3)
             ON CONFLICT (template_name, tag) DO UPDATE
                 SET template_release_id = EXCLUDED.template_release_id,
                     modified_at = NOW()
             RETURNING id, template_release_id, tag, created_at
         ), marked AS (
             UPDATE template_releases release
             SET first_mutable_tag_at = GREATEST(NOW(), release.created_at)
             FROM upserted
             WHERE release.id = upserted.template_release_id
               AND release.first_mutable_tag_at IS NULL
               AND upserted.tag <> release.hash
               AND upserted.tag <> CASE
                   WHEN release.hash LIKE 'sha256:%' THEN substr(release.hash, 8)
                   WHEN release.hash LIKE '%:%' THEN release.hash
                   ELSE 'sha256:' || release.hash
               END
         )
         SELECT id, tag, created_at FROM upserted"
    ))
    .bind(release_id)
    .bind(template_name)
    .bind(tag)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Delete `tag` from `release_id`, if it names that release.
pub async fn delete(
    executor: impl Executor<'_>,
    release_id: Uuid,
    tag: &TemplateTag,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags
         WHERE template_release_id = $1 AND tag = $2"
    ))
    .bind(release_id)
    .bind(tag.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete every tag of `release_id`.
pub async fn delete_for_release(
    executor: impl Executor<'_>,
    release_id: Uuid,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_release_id = $1"
    ))
    .bind(release_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Delete every tag of every release of `template`.
pub async fn delete_for_template(
    executor: impl Executor<'_>,
    template: &TemplateName,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// The release `tag` of `template_name` points at, or `None` when the
/// template has no such tag, locking the tag's row `FOR UPDATE` until the
/// caller's transaction ends. The caller must pass its open transaction.
pub async fn current_release_locked<C>(
    tx: &mut C,
    template_name: &str,
    tag: &str,
) -> Result<Option<Uuid>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT template_release_id
         FROM template_release_tags
         WHERE template_name = $1 AND tag = $2
         FOR UPDATE"
    ))
    .bind(template_name)
    .bind(tag)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// The tags among `tags` of `template_name` that name a release of the
/// digest stored as `hash` (bare `bare_hash`), under either spelling, by
/// tag, share-locking each tag's row `FOR SHARE` until the caller's
/// transaction ends. The caller must pass its open transaction.
pub async fn share_lock_tags_of_digest<C>(
    tx: &mut C,
    template_name: &str,
    tags: &[String],
    hash: &str,
    bare_hash: &str,
) -> Result<Vec<RegisteredTag>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_as::<_, RegisteredTag>(sql!(
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
    .fetch_all(tx.into_sqlx())
    .await?)
}
