//! Postgres-backed implementation of [`super::SweeperSink`].
//!
//! Each method below calls one
//! `pplx_space_snapshot_store::orphan_locations` verb:
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
        // The page's `EXISTS` subquery against the sidecar is what powers the
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
        // either column alone identifies an orphan. Both filters in the
        // page (and in `delete_storage_location_if_orphan` below) make the
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
        let rows =
            pplx_space_snapshot_store::orphan_locations::page(&self.pool, after, limit, min_age)
                .await?;
        Ok(rows
            .into_iter()
            .map(|row| SweepCandidate {
                id: row.id,
                storage_connection_string: row.storage_connection_string,
                has_encrypted_fields: row.has_encrypted_fields,
            })
            .collect())
    }

    async fn delete_storage_location_if_orphan(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        // Re-assert both halves of the orphan contract
        // (`orphaned_at IS NOT NULL AND snapshot_id IS NULL`). See
        // the candidate-scan above for the rationale; the symmetric
        // filter here means a row that drifted out of the orphan
        // state between scan and DELETE (any future admin tooling
        // that resets either column) is left alone.
        pplx_space_snapshot_store::orphan_locations::delete_if_orphan(&self.pool, id)
            .await
            .map_err(sqlx::Error::from)
    }
}
