//! Tag seeding for other crates' tests. Compiled for this crate's tests and
//! behind the `test-support` feature; enable it only from
//! `[dev-dependencies]`.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// Point `tag` of `template_name` at `release`, as a tag written before the
/// shared original-release rule could: nothing here checks that `release` is
/// the digest original. Tests over
/// `pplx_space_template_release_store::spelling_pairs` tag the pair's other
/// release with it.
pub async fn tag_release(
    executor: impl Executor<'_>,
    template_name: &str,
    release: Uuid,
    tag: &str,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)
         ON CONFLICT (template_name, tag) DO UPDATE
         SET template_release_id = EXCLUDED.template_release_id,
             modified_at = clock_timestamp()"
    ))
    .bind(release)
    .bind(template_name)
    .bind(tag)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
