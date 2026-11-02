//! Postgres-backed implementation of [`super::SweeperSink`].
//!
//! Two SQL statements:
//!
//! - [`SnapshotStorageDbStore::list_candidates`] — scans
//!   `snapshot_storage_locations` with `orphaned_at IS NOT NULL`,
//!   `LEFT JOIN`ed against
//!   `snapshot_storage_location_encrypted_fields` so each row carries
//!   a precomputed `has_encrypted_fields` flag. Ordered by `id` ASC
//!   with a cursor on `id > $after` so the reconciler can sweep past
//!   stuck rows without re-scanning the same prefix every tick.
//! - [`SnapshotStorageDbStore::delete_storage_location_if_orphan`] —
//!   conditional DELETE re-asserts `orphaned_at IS NOT NULL` under
//!   the row's lock. The `snapshot_storage_location_encrypted_fields`
//!   sidecar (when present) cascades via the FK declared in migration
//!   050.

use pplx_space_common_core::sql;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use super::{SweepCandidate, SweeperSink};

pub struct SnapshotStorageDbStore {
    pool: PgPool,
}

impl SnapshotStorageDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl SweeperSink for SnapshotStorageDbStore {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<Uuid>,
        min_age: Option<Duration>,
    ) -> Result<Vec<SweepCandidate>, sqlx::Error> {
        // The `EXISTS` subquery against the sidecar is what powers the
        // sweeper's encrypted-row short-circuit. Joining would also
        // work, but `EXISTS` keeps the result shape one row per
        // storage-location regardless of any sidecar oddities, and
        // doesn't risk row multiplication if the sidecar ever grows
        // non-1:1 (it won't — there's a UNIQUE constraint — but the
        // shape is more robust to future schema drift).
        //
        // Defense-in-depth on the orphan contract: the writer
        // (`delete_with_dependents`) sets `snapshot_id = NULL`
        // and stamps `orphaned_at` in the same statement, so today
        // either column alone identifies an orphan. Both filters here
        // (and in `delete_storage_location_if_orphan` below) make the
        // pairing explicit, so any future code path that stamps
        // `orphaned_at` without nulling `snapshot_id` (admin
        // tombstone, retention policy, soft-cleanup migration) can't
        // accidentally feed live rows to the sweeper.
        //
        // `min_age` filter (when `Some`): hide rows whose orphan
        // timestamp is too recent to safely sweep. A sandbox DELETE
        // can race with an in-flight suspend-snapshot upload that's
        // still writing objects to the same prefix; reaping the row
        // mid-upload would orphan every late-arriving object with no
        // DB anchor for a future sweep. Pass the grace duration in
        // as a typed `INTERVAL` so SQL injection is impossible. The
        // user-CLAUDE.md rule on `Option<Duration>` calls out exactly
        // this destructive sentinel-zero case
        // (`NOW() - INTERVAL '0s'` matches every row); using
        // `Option<Duration>` means a forgotten check at any layer
        // collapses to the inner `is_some` filter, not to "no
        // grace".
        let min_age_secs = min_age.map(|d| d.as_secs() as i64);
        let rows: Vec<(Uuid, String, bool)> = sqlx::query_as(sql!(
            "SELECT
                 ssl.id,
                 ssl.storage_connection_string,
                 EXISTS (
                     SELECT 1
                       FROM snapshot_storage_location_encrypted_fields ef
                      WHERE ef.snapshot_storage_location_id = ssl.id
                 ) AS has_encrypted_fields
             FROM snapshot_storage_locations ssl
             WHERE ssl.orphaned_at IS NOT NULL
               AND ssl.snapshot_id IS NULL
               AND ($1::UUID IS NULL OR ssl.id > $1)
               AND ($3::BIGINT IS NULL
                    OR ssl.orphaned_at < NOW() - make_interval(secs => $3::BIGINT))
             ORDER BY ssl.id ASC
             LIMIT $2"
        ))
        .bind(after)
        .bind(limit)
        .bind(min_age_secs)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, storage_connection_string, has_encrypted_fields)| SweepCandidate {
                    id,
                    storage_connection_string,
                    has_encrypted_fields,
                },
            )
            .collect())
    }

    async fn delete_storage_location_if_orphan(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        // Re-assert both halves of the orphan contract
        // (`orphaned_at IS NOT NULL AND snapshot_id IS NULL`). See
        // the candidate-scan above for the rationale; the symmetric
        // filter here means a row that drifted out of the orphan
        // state between scan and DELETE (any future admin tooling
        // that resets either column) is left alone.
        let result = sqlx::query(sql!(
            "DELETE FROM snapshot_storage_locations
             WHERE id = $1
               AND orphaned_at IS NOT NULL
               AND snapshot_id IS NULL"
        ))
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }
}
