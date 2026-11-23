//! Statements release registration (`RegisterTemplateRelease`, in
//! internal-api) runs under the template's publication lock, in its
//! transaction. None takes a lock of its own.

use pplx_space_accounts::OrgId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
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
    executor: impl PgExecutor<'_>,
    template_name: &str,
    hash: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases
         WHERE template_name = $1 AND hash = $2
         ORDER BY created_at ASC, id ASC
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(hash)
    .fetch_optional(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    template_name: &str,
    hash: &str,
    selector: PartitionSelector,
    include_unrecorded: bool,
) -> Result<Option<RegisteredRelease>, sqlx::Error> {
    sqlx::query_as::<_, RegisteredRelease>(sql!(
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
    .fetch_optional(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    template_name: &str,
    runtime: &str,
    hash: &str,
    org_id: Option<OrgId>,
    execution_metadata: Option<&serde_json::Value>,
) -> Result<Option<RegisteredRelease>, sqlx::Error> {
    sqlx::query_as::<_, RegisteredRelease>(sql!(
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
    .fetch_optional(executor)
    .await
}

/// The storage location of release `release_id` at exactly
/// `connection_string`, or `None`.
pub async fn storage_location(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    connection_string: &str,
) -> Result<Option<RegisteredStorageLocation>, sqlx::Error> {
    sqlx::query_as::<_, RegisteredStorageLocation>(sql!(
        "SELECT id, storage_connection_string, created_at
         FROM template_release_storage_locations
         WHERE template_release_id = $1
           AND storage_connection_string = $2"
    ))
    .bind(release_id)
    .bind(connection_string)
    .fetch_optional(executor)
    .await
}

/// Insert the storage location `connection_string` of release `release_id`,
/// owned by `org_id` or, without one, by the release's owner. When the
/// release already has that location, its owner is kept, or set to the new
/// one when it has none, and the location comes back all the same. `None`
/// when there is no release `release_id`.
pub async fn insert_storage_location(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
    org_id: Option<OrgId>,
    connection_string: &str,
) -> Result<Option<RegisteredStorageLocation>, sqlx::Error> {
    sqlx::query_as::<_, RegisteredStorageLocation>(sql!(
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
    .fetch_optional(executor)
    .await
}
