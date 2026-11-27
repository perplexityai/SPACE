//! Statements release registration (`RegisterTemplateRelease`, in
//! internal-api) runs under the template's publication lock, in its
//! transaction. None takes a lock of its own but
//! [`lock_execution_metadata`], whose release row lock lasts until the
//! caller's transaction ends.

use pplx_space_accounts::OrgId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_release_original::digest_original;
use std::sync::LazyLock;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::PartitionSelector;

/// A release as registration reads and inserts it.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct RegisteredRelease {
    pub id: Uuid,
    pub template_name: String,
    #[sqlx(rename = "type")]
    pub release_type: String,
    /// The stored spelling of the hash, bare or `sha256:`.
    pub hash: String,
    pub org_id: Option<OrgId>,
    pub created_at: OffsetDateTime,
    /// `execution_metadata->>'environment'`: the environment the release is
    /// bound to, `None` when unbound.
    pub environment: Option<String>,
}

/// A storage location as registration reads and inserts it.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct RegisteredStorageLocation {
    pub id: Uuid,
    pub storage_connection_string: String,
    pub created_at: OffsetDateTime,
}

/// The first registered release of `template_name` stored under exactly
/// `hash`: that spelling's original.
pub async fn first_registered_release_id(
    executor: impl Executor<'_>,
    template_name: &str,
    hash: &str,
) -> Result<Option<Uuid>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases
         WHERE template_name = $1 AND hash = $2
         ORDER BY created_at ASC, id ASC
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(hash)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The release of `template_name` stored under exactly `hash` that `selector`
/// selects among releases with execution metadata, or, when
/// `include_unrecorded` and the selector includes unbound releases, one with
/// no execution metadata. Releases with a storage location come first, then
/// those with execution metadata, then by age.
///
/// Unlike [`crate::release_of_hash_in_partition`], it matches only `hash`'s
/// exact spelling and doesn't skip releases without a storage location.
pub async fn partition_release(
    executor: impl Executor<'_>,
    template_name: &str,
    hash: &str,
    selector: PartitionSelector,
    include_unrecorded: bool,
) -> Result<Option<RegisteredRelease>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredRelease>(sql!(
        "SELECT r.id, r.template_name, r.type, r.hash, r.org_id, r.created_at,
                r.execution_metadata->>'environment' AS environment
         FROM template_releases r
         WHERE r.template_name = $1
           AND r.hash = $2
           AND ((r.execution_metadata IS NULL AND $5 AND $3)
                OR (r.execution_metadata IS NOT NULL
                    AND (r.execution_metadata->>'environment' = $4
                         OR ($3 AND r.execution_metadata->>'environment' IS NULL))))
         ORDER BY EXISTS (
                      SELECT 1
                      FROM template_release_storage_locations l
                      WHERE l.template_release_id = r.id) DESC,
                  (r.execution_metadata IS NOT NULL) DESC,
                  r.created_at ASC,
                  r.id ASC
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(hash)
    .bind(selector.includes_unbound)
    .bind(selector.environment.to_string())
    .bind(include_unrecorded)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Insert the release, or nothing (`None`) when it conflicts with one already
/// registered. The conflict target is left out on purpose: Postgres then
/// treats a violation of any unique index as the conflict, so the statement
/// works with the `(template_name, hash)` index, which admits no twin, and
/// with `template_releases_template_name_hash_environment_key`, which admits
/// one release per environment, together or alone. Nothing is inserted
/// either when `template_name` has no template row. A release recorded
/// without `org_id` takes the template's owner.
///
/// The untargeted conflict clause also turns a violation of any other unique
/// index into "nothing inserted"; today the others all include the generated
/// `id`, so only the two `(template_name, hash, ..)` indexes can conflict.
///
/// `created_at` is the insert's own time, not the transaction's start: the
/// caller holds the template's publication lock, so insert order is
/// registration order, and the original release of a hash (the earliest by
/// `created_at`) is the one inserted first even when a twin's transaction
/// began earlier.
pub async fn insert_release(
    executor: impl Executor<'_>,
    template_name: &str,
    runtime: &str,
    hash: &str,
    org_id: Option<OrgId>,
    execution_metadata: Option<&serde_json::Value>,
) -> Result<Option<RegisteredRelease>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredRelease>(sql!(
        "INSERT INTO template_releases
             (template_name, type, hash, org_id, execution_metadata, created_at)
         SELECT $1, $2, $3, COALESCE($4, org_id), $5, clock_timestamp()
         FROM templates
         WHERE name = $1
         ON CONFLICT DO NOTHING
         RETURNING id, template_name, type, hash, org_id, created_at,
                   execution_metadata->>'environment' AS environment"
    ))
    .bind(template_name)
    .bind(runtime)
    .bind(hash)
    .bind(org_id)
    .bind(execution_metadata)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The storage location of release `release_id` at exactly
/// `connection_string`, or `None`.
pub async fn storage_location(
    executor: impl Executor<'_>,
    release_id: Uuid,
    connection_string: &str,
) -> Result<Option<RegisteredStorageLocation>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredStorageLocation>(sql!(
        "SELECT id, storage_connection_string, created_at
         FROM template_release_storage_locations
         WHERE template_release_id = $1
           AND storage_connection_string = $2"
    ))
    .bind(release_id)
    .bind(connection_string)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Insert the storage location `connection_string` of release `release_id`,
/// owned by `org_id` or, without one, by the release's owner. When the
/// release already has that location, its owner is kept, or set to the new
/// one when it has none, and the location comes back all the same. `None`
/// when there is no release `release_id`.
pub async fn insert_storage_location(
    executor: impl Executor<'_>,
    release_id: Uuid,
    org_id: Option<OrgId>,
    connection_string: &str,
) -> Result<Option<RegisteredStorageLocation>, DbError> {
    Ok(sqlx::query_as::<_, RegisteredStorageLocation>(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         SELECT $1, $3, COALESCE($2, org_id)
         FROM template_releases
         WHERE id = $1
         ON CONFLICT (template_release_id, storage_connection_string) DO UPDATE
             SET org_id = COALESCE(
                 template_release_storage_locations.org_id,
                 EXCLUDED.org_id
             )
         RETURNING id, storage_connection_string, created_at"
    ))
    .bind(release_id)
    .bind(org_id)
    .bind(connection_string)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Record `execution` as release `release_id`'s execution metadata when it
/// records none. `false` when it already records some, or there is no
/// release `release_id`. A binding another release of the build already
/// holds fails with the partition index's unique violation. The caller holds
/// the release's row lock while it compares or fills the metadata.
pub async fn fill_execution_metadata(
    executor: impl Executor<'_>,
    release_id: Uuid,
    execution: &serde_json::Value,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE template_releases SET execution_metadata = $2
         WHERE id = $1 AND execution_metadata IS NULL"
    ))
    .bind(release_id)
    .bind(execution)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Give the storage location `location_id` its release's owner when it has
/// none. `true` whenever the location had no owner, including when its
/// release has none either, which leaves the location ownerless. `false`
/// when it already has an owner, or there is no such location.
pub async fn adopt_storage_location_owner(
    executor: impl Executor<'_>,
    location_id: Uuid,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE template_release_storage_locations location
         SET org_id = release.org_id
         FROM template_releases release
         WHERE location.id = $1
           AND release.id = location.template_release_id
           AND location.org_id IS NULL"
    ))
    .bind(location_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Release `release_id`'s execution metadata, `None` when it records none,
/// locking the release's row `FOR UPDATE` until the caller's transaction
/// ends, so a check and fill of the metadata
/// ([`fill_execution_metadata`]) is one step against other writers. Fails
/// with a [`DbError`] whose `kind()` is [`DbErrorKind::NotFound`] (the
/// driver's `RowNotFound`, which callers still see after converting) when
/// there is no release `release_id`. The caller must pass its open
/// transaction.
///
/// [`DbErrorKind::NotFound`]: pplx_space_postgres_pool::DbErrorKind::NotFound
pub async fn lock_execution_metadata<C>(
    tx: &mut C,
    release_id: Uuid,
) -> Result<Option<serde_json::Value>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT execution_metadata FROM template_releases WHERE id = $1 FOR UPDATE"
    ))
    .bind(release_id)
    .fetch_one(tx.into_sqlx())
    .await?)
}

/// The original release of `template_name` stored as `canonical_hash` or
/// `bare_hash`, the two spellings of one digest, chosen by
/// `pplx_space_template_release_original::digest_original`: the release a
/// lookup with no environment resolves, never a twin. `None` when no
/// release has the digest.
pub async fn original_release(
    executor: impl Executor<'_>,
    template_name: &str,
    canonical_hash: &str,
    bare_hash: &str,
) -> Result<Option<RegisteredRelease>, DbError> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id, r.template_name, r.type, r.hash, r.org_id, r.created_at,
                    r.execution_metadata->>'environment' AS environment
             FROM template_releases r
             WHERE r.template_name = $1
               AND r.hash IN ($2, $3)
               AND {digest_original}",
            digest_original = digest_original("r"),
        ))
    });
    Ok(
        sqlx::query_as::<_, RegisteredRelease>(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(template_name)
            .bind(canonical_hash)
            .bind(bare_hash)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}
