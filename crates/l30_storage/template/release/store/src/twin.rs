//! Inserting a release's twin.

use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};
use uuid::Uuid;

use crate::{PartitionSelector, end_transaction};

/// How [`insert_twin`] ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertTwin {
    /// The twin was inserted, with the storage locations it was given.
    Inserted(Uuid),
    /// A release of the hash, under either spelling, in the target's
    /// partition already exists: an earlier run's twin, or one a registration
    /// made.
    Existing(Uuid),
    /// The original release is gone, or its template is no longer present.
    OriginalGone,
    /// The insert conflicted and no usable release is in the target's
    /// partition: a unique index still admits only one release per
    /// `(template_name, hash)` (`idx_template_releases_template_name_hash_unique`,
    /// which the contract migration drops).
    Refused,
    /// A release without storage under `original`'s exact spelling already
    /// occupies the target partition's `(template_name, hash, environment)`
    /// slot, so no twin fits there until it's removed. Carries its id.
    SlotTaken(Uuid),
}

/// Insert the twin of `original` bound to `target`, carrying
/// `execution_metadata`, and give it those of the original's storage
/// locations named in `locations`, in one transaction. A location belongs on
/// the twin only once the twin's `metadata.json` is there, so the caller names
/// the ones it wrote.
///
/// This copies no encryption sidecar, and the application pool can't see
/// one, so the caller checks [`crate::has_encrypted_storage_locations`] on the
/// credentials pool first and never twins a release with encrypted locations.
///
/// Fails with [`sqlx::Error::InvalidArgument`], writing nothing, when
/// `execution_metadata` doesn't bind `target.environment`, or when it would
/// insert the twin and none of `locations` is one of the original's: a twin
/// with no location would hold the environment's release slot with nothing
/// fetchable and turn a retry into [`InsertTwin::SlotTaken`]. `locations` isn't
/// checked when nothing is inserted.
///
/// The twin's `created_at` is never earlier than its original's, so the
/// original stays first even if the database clock steps back. Each copied
/// location takes its `created_at` on the original plus, in microseconds, its
/// position among the copied locations in the original's `(created_at, id)`
/// order, which keeps readers
/// ordering by `(created_at, id)` in the original's order even where the
/// original's locations share a timestamp and only their random ids order
/// them.
///
/// For the initial environment, `target.includes_unbound` must be true, so an
/// unbound original counts as already filling the partition; the store can't
/// tell which environment is initial.
///
/// The twin takes its original's template name, runtime, exact hash spelling
/// and owner, and no tags. When a release of the hash is already in the
/// target's partition, nothing is written and that release is returned.
///
/// Locks the template's row (`FOR NO KEY UPDATE`) first, serializing with
/// release registration and with the admin release delete. Opens a
/// transaction, or a savepoint inside the caller's.
///
/// A caller's transaction must be `READ COMMITTED`, Postgres's default. Under
/// `REPEATABLE READ` or `SERIALIZABLE`, the queries after the lock read the
/// caller's snapshot, which can still show an original that a delete removed
/// while this verb waited. The delete only locks the template's row, so the
/// lock raises no serialization error, and the twin would outlive its
/// original.
pub async fn insert_twin(
    conn: impl Acquire<'_, Database = Postgres>,
    original: Uuid,
    target: PartitionSelector,
    execution_metadata: &serde_json::Value,
    locations: &[String],
) -> Result<InsertTwin, sqlx::Error> {
    let bound = execution_metadata
        .get("environment")
        .and_then(serde_json::Value::as_str);
    if bound != Some(target.environment.to_string().as_str()) {
        return Err(sqlx::Error::InvalidArgument(format!(
            "a twin bound to {} needs execution_metadata binding that environment, not {bound:?}",
            target.environment
        )));
    }
    let mut tx = conn.begin().await?;
    let inserted = insert_in(&mut tx, original, target, execution_metadata, locations).await;
    end_transaction(tx, inserted).await
}

async fn insert_in(
    tx: &mut PgConnection,
    original: Uuid,
    target: PartitionSelector,
    execution_metadata: &serde_json::Value,
    locations: &[String],
) -> Result<InsertTwin, sqlx::Error> {
    let Some((template_name, hash)) = sqlx::query_as::<_, (String, String)>(sql!(
        "SELECT r.template_name, r.hash
         FROM template_releases r
         JOIN templates t ON t.name = r.template_name
         WHERE r.id = $1
           AND t.desired_state = 'present'
         FOR NO KEY UPDATE OF t"
    ))
    .bind(original)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(InsertTwin::OriginalGone);
    };
    if let Some(existing) =
        crate::release_of_hash_in_partition(&mut *tx, &template_name, &hash, target).await?
    {
        return Ok(InsertTwin::Existing(existing));
    }
    // No conflict target: Postgres treats a violation of any unique index as
    // the conflict, so the statement works whether or not the old
    // `(template_name, hash)` index still stands.
    let Some(twin) = sqlx::query_scalar::<_, Uuid>(sql!(
        "INSERT INTO template_releases
             (template_name, type, hash, org_id, execution_metadata, created_at)
         SELECT template_name, type, hash, org_id, $2,
                GREATEST(clock_timestamp(), created_at + interval '1 microsecond')
         FROM template_releases
         WHERE id = $1
         ON CONFLICT DO NOTHING
         RETURNING id"
    ))
    .bind(original)
    .bind(execution_metadata)
    .fetch_optional(&mut *tx)
    .await?
    else {
        // Nothing inserted: the original went away while this waited on the
        // template lock (the admin delete locks the template row without
        // updating it, so the locking read above still returned it), or the
        // old index refused the row, or an inserter that doesn't take the
        // template lock filled the partition since the check above.
        // Registration, this store and the Kind bootstrap script
        // (`scripts/bootstrap-templates.sh`) take it, but a checkout of that
        // script from before it took the lock inserts without it. Looking again
        // reports that last case as `Existing` rather than as the old index.
        if let Some(existing) =
            crate::release_of_hash_in_partition(&mut *tx, &template_name, &hash, target).await?
        {
            return Ok(InsertTwin::Existing(existing));
        }
        let original_present = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT EXISTS (SELECT 1 FROM template_releases WHERE id = $1)"
        ))
        .bind(original)
        .fetch_one(&mut *tx)
        .await?;
        if !original_present {
            return Ok(InsertTwin::OriginalGone);
        }
        // The target slot under the exact spelling, which the partition
        // lookup above skips when it has no storage: that release, rather
        // than the old index, is what refused the row.
        let occupant = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id FROM template_releases
             WHERE template_name = $1
               AND hash = $2
               AND (execution_metadata->>'environment') IS NOT DISTINCT FROM $3"
        ))
        .bind(&template_name)
        .bind(&hash)
        .bind(target.environment.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        return Ok(match occupant {
            Some(occupant) => InsertTwin::SlotTaken(occupant),
            None => InsertTwin::Refused,
        });
    };
    let copied = sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id, created_at)
         SELECT $2, storage_connection_string, org_id,
                created_at + row_number() OVER (ORDER BY created_at, id) * interval '1 microsecond'
         FROM template_release_storage_locations
         WHERE template_release_id = $1
           AND storage_connection_string = ANY($3)
         ON CONFLICT (template_release_id, storage_connection_string) DO NOTHING"
    ))
    .bind(original)
    .bind(twin)
    .bind(locations)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if copied == 0 {
        return Err(sqlx::Error::InvalidArgument(format!(
            "none of the {} named storage locations is one of release {original}'s",
            locations.len()
        )));
    }
    Ok(InsertTwin::Inserted(twin))
}
