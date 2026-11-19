//! Data access for template releases (`template_releases`), their storage
//! locations (`template_release_storage_locations`), and the per-environment
//! `metadata.json` of a release's twin in object storage.
//!
//! A *twin* of a release is a second release of the same template name and
//! hash, bound to another execution environment in `execution_metadata`. It
//! shares its original's artifacts in object storage, so it shares its
//! storage locations too, and has its own `metadata.json` under the build's
//! `environments/<environment>/` key. A runtime upgrade campaign creates
//! twins; this store is the campaign's reader and writer of releases.
//!
//! Release registration (`RegisterTemplateRelease`, in internal-api) and the
//! api's lookups and admin routes still run their own statements against these
//! tables. Moving them here belongs to the change that touches them.
//!
//! # Rules the store enforces
//!
//! * **Originals come first.** A build's original release is the earliest one
//!   registered for its digest, preferring an original that owns a storage
//!   location when both spellings of a SHA-256 digest have one. A twin's
//!   insert timestamps it with `clock_timestamp()`, after its original, and
//!   takes its original's exact `hash` spelling, owner and runtime.
//! * **One release per partition.** Nodes cache an unbound build and one bound
//!   to the initial environment in the same legacy partition, so a selector
//!   for the initial environment also matches an unbound release
//!   ([`PartitionSelector`]), and no twin is inserted into a partition that
//!   already has a usable release of the hash (an unattached release without
//!   storage under the original's exact spelling still conflicts on the
//!   partition's unique index; see [`InsertTwin`]).
//! * **Locks follow registration's.** A twin's insert locks its template's
//!   row (`FOR NO KEY UPDATE`) first, which conflicts with the `FOR UPDATE`
//!   registration holds while it inserts and with the lock the admin
//!   release delete takes before its twin check, but not with the key-share
//!   locks of foreign-key checks.
//! * **No tags on twins.** Tags resolve to originals; this store never
//!   writes `template_release_tags`.
//! * **Locks end with the verb.** A verb that opens a transaction commits or
//!   rolls it back before it returns (unless rolling back itself fails, in
//!   which case [`sqlx`] returns that error and queues cleanup on drop). Run
//!   inside a caller's transaction, it opens a savepoint instead: when the
//!   verb succeeds, the locks it took last until the caller's transaction
//!   ends, and when it fails, rolling back the savepoint releases them once
//!   that rollback succeeds.

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;
use sqlx::{PgExecutor, Postgres};
use uuid::Uuid;

mod objects;
mod resolution;
mod twin;

pub use objects::{ObjectAccessError, ReleaseObjects, S3ReleaseObjects, TwinMetadataTarget};
pub use resolution::{resolved_release, resolved_release_sql};
pub use twin::{InsertTwin, insert_twin};

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Which releases a lookup selects by execution environment: those bound to
/// `environment`, and, when the environment caches in the legacy partition
/// (the initial environment), those recorded without one.
///
/// The caller decides `includes_unbound` from the execution catalog
/// (`template_cache_partition`), which this layer doesn't depend on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionSelector {
    pub environment: EnvironmentReference,
    pub includes_unbound: bool,
}

impl PartitionSelector {
    /// Whether a release recording `environment`
    /// (`execution_metadata->>'environment'`, unparsed) is in this
    /// partition, as the store's statements select it.
    pub fn contains(&self, environment: Option<&str>) -> bool {
        match environment {
            Some(environment) => environment == self.environment.to_string(),
            None => self.includes_unbound,
        }
    }
}

/// The release a campaign item names, as the item's upgrade reads it.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct ReleaseSubject {
    pub id: Uuid,
    pub template_name: String,
    /// The stored spelling of the hash, bare or `sha256:`.
    pub hash: String,
    pub execution_metadata: Option<serde_json::Value>,
    /// The environment `execution_metadata` binds, unparsed; `None` when
    /// unbound.
    pub environment: Option<String>,
    /// Whether the template is `present`, not being deleted.
    pub template_present: bool,
}

/// Up to `limit` releases selected by `source`, after `after`, by id. A
/// release's id is a UUIDv7, so the order is stable and roughly that of
/// registration.
///
/// When a build owns a storage location under either spelling of its SHA-256
/// digest, a release of that digest without one (an unattached original under
/// the other spelling) is skipped; a build with no storage locations on any
/// release still appears so the caller surfaces it.
pub async fn source_releases_page(
    executor: impl PgExecutor<'_>,
    source: PartitionSelector,
    after: Option<Uuid>,
    limit: u32,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT r.id
         FROM template_releases r
         WHERE (r.execution_metadata->>'environment' = $1
                OR ($2 AND r.execution_metadata->>'environment' IS NULL))
           AND ($3::UUID IS NULL OR r.id > $3)
           AND (EXISTS (
                    SELECT 1
                    FROM template_release_storage_locations l
                    WHERE l.template_release_id = r.id)
                OR NOT EXISTS (
                    SELECT 1
                    FROM template_releases any_r
                    JOIN template_release_storage_locations al
                      ON al.template_release_id = any_r.id
                    WHERE any_r.template_name = r.template_name
                      AND any_r.hash IN (
                          r.hash,
                          CASE
                              WHEN r.hash ~ '^sha256:[0-9a-f]{64}$'
                                  THEN substr(r.hash, 8)
                              ELSE r.hash
                          END,
                          CASE
                              WHEN r.hash ~ '^[0-9a-f]{64}$'
                                  THEN 'sha256:' || r.hash
                              ELSE r.hash
                          END)))
         ORDER BY r.id
         LIMIT $4"
    ))
    .bind(source.environment.to_string())
    .bind(source.includes_unbound)
    .bind(after)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await
}

/// The release `release_id`, or `None` when there is none. Takes no lock.
pub async fn release_subject(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<Option<ReleaseSubject>, sqlx::Error> {
    sqlx::query_as::<_, ReleaseSubject>(sql!(
        "SELECT r.id,
                r.template_name,
                r.hash,
                r.execution_metadata,
                r.execution_metadata->>'environment' AS environment,
                t.desired_state = 'present' AS template_present
         FROM template_releases r
         JOIN templates t ON t.name = r.template_name
         WHERE r.id = $1"
    ))
    .bind(release_id)
    .fetch_optional(executor)
    .await
}

/// The release of `template_name` at `hash` that `selector` selects, under
/// either spelling of the hash, bare or `sha256:<hex>`, whichever `hash` is
/// (a release registered before the prefix was required is stored bare, and
/// one registered since under the canonical spelling), preferring `hash`'s
/// own spelling, or `None`.
///
/// Registration never attaches to the other spelling's original, so a digest
/// can carry an unattached release without storage alongside storage-backed
/// releases under the preferred spelling; when any release of the build owns a
/// storage location, a release without one is ignored here so it neither
/// blocks a twin insert nor stands in for a base release. A derived build
/// names its base by template and hash, so this finds the base's release in a
/// campaign's source partition, an original or a twin. Takes no lock.
pub async fn release_of_hash_in_partition(
    executor: impl PgExecutor<'_>,
    template_name: &str,
    hash: &str,
    selector: PartitionSelector,
) -> Result<Option<Uuid>, sqlx::Error> {
    let other_spelling = match objects::sha256_hex(hash) {
        Some(hex) if hash.starts_with("sha256:") => hex.to_owned(),
        Some(hex) => format!("sha256:{hex}"),
        None => hash.to_owned(),
    };
    sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT r.id
         FROM template_releases r
         WHERE r.template_name = $1
           AND r.hash IN ($2, $3)
           AND (r.execution_metadata->>'environment' = $4
                OR ($5 AND r.execution_metadata->>'environment' IS NULL))
           AND (EXISTS (
                    SELECT 1
                    FROM template_release_storage_locations l
                    WHERE l.template_release_id = r.id)
                OR NOT EXISTS (
                    SELECT 1
                    FROM template_releases any_r
                    JOIN template_release_storage_locations al
                      ON al.template_release_id = any_r.id
                    WHERE any_r.template_name = r.template_name
                      AND any_r.hash IN ($2, $3)))
         ORDER BY (r.hash = $2) DESC, r.created_at, r.id
         LIMIT 1"
    ))
    .bind(template_name)
    .bind(hash)
    .bind(&other_spelling)
    .bind(selector.environment.to_string())
    .bind(selector.includes_unbound)
    .fetch_optional(executor)
    .await
}

/// The storage connection strings of `release_id`, oldest first, encrypted
/// or not: the application pool can't see the encryption sidecar, so check
/// [`has_encrypted_storage_locations`] on the credentials pool before
/// treating them as plaintext. Takes no lock.
pub async fn storage_locations(
    executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!(
        "SELECT storage_connection_string
         FROM template_release_storage_locations
         WHERE template_release_id = $1
         ORDER BY created_at, id"
    ))
    .bind(release_id)
    .fetch_all(executor)
    .await
}

/// Whether any storage location of `release_id` carries an owner-scoped
/// encryption sidecar (`template_release_storage_location_encrypted_fields`).
///
/// The sidecar is credential-fenced: run this on the credentials pool, since
/// the application pool has no grant on it.
pub async fn has_encrypted_storage_locations(
    credentials_executor: impl PgExecutor<'_>,
    release_id: Uuid,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1
             FROM template_release_storage_location_encrypted_fields e
             JOIN template_release_storage_locations l
               ON l.id = e.template_release_storage_location_id
             WHERE l.template_release_id = $1)"
    ))
    .bind(release_id)
    .fetch_one(credentials_executor)
    .await
}

/// End a verb's transaction: commit it when `result` is `Ok`, roll it back
/// otherwise.
///
/// When the rollback itself fails, that error is returned instead of
/// `result`'s, so a caller never reads a failed cleanup as a confirmed one.
/// The connection is then suspect, and sqlx queues another rollback for it
/// when the transaction drops. Nothing the verb wrote commits.
pub(crate) async fn end_transaction<T, E: From<sqlx::Error>>(
    tx: sqlx::Transaction<'_, Postgres>,
    result: Result<T, E>,
) -> Result<T, E> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}
