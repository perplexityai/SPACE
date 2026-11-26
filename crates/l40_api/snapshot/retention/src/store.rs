//! Postgres retention store: page fresh work and durable claims, claim under
//! the sandbox lock, then finalize only after the object-store reap.

use pplx_space_sandbox_model::SandboxId;

use pplx_space_sql_locator::sql;
use std::time::Duration;

use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};
use pplx_space_sandbox_store::snapshot_retention_claim;
use pplx_space_sandbox_store::snapshot_retention_finalize::lock_owner_marks_snapshot;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_snapshot_store::claimed::{self, ClaimedSnapshot};
use pplx_space_snapshot_store::retention;
use pplx_space_snapshot_store::{delete_snapshot_catalog, snapshot_ids_after};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    CandidatePage, ClaimOutcome, ClaimRefusalReason, ClaimedScanEpoch, PruneCandidate,
    PruneFinalize, RetentionSink, SnapshotStorageLocation,
};

type PruneCandidateRow = (
    SnapshotId,
    SandboxId,
    Option<Uuid>,
    Option<String>,
    bool,
    i64,
);

fn candidate_row(row: retention::Candidate) -> PruneCandidateRow {
    (
        row.snapshot_id,
        row.sandbox_id,
        row.storage_location_id,
        row.storage_connection_string,
        row.has_encrypted_fields,
        row.age_secs,
    )
}

fn claimed_row(row: ClaimedSnapshot) -> PruneCandidateRow {
    (
        row.snapshot_id,
        row.sandbox_id,
        row.storage_location_id,
        row.storage_connection_string,
        row.has_encrypted_fields,
        row.age_secs,
    )
}

fn prune_candidate_from_row(row: PruneCandidateRow) -> Result<PruneCandidate, sqlx::Error> {
    let (snapshot_id, sandbox_id, location_id, route, encrypted, age_secs) = row;
    let storage_location = match (location_id, route) {
        (None, None) => None,
        (Some(id), Some(route)) => Some(SnapshotStorageLocation::new(id, route, encrypted)),
        _ => {
            return Err(sqlx::Error::Protocol(
                "snapshot retention location projection had mismatched identity and route".into(),
            ));
        }
    };
    Ok(PruneCandidate {
        snapshot_id,
        sandbox_id,
        storage_location,
        age_secs,
    })
}

/// How many snapshot ids one discovery page reads before eligibility.
const RAW_PAGE_SIZE: usize = 10_000;

pub struct SnapshotRetentionDbStore {
    pool: PgPool,
    /// [`RAW_PAGE_SIZE`] outside tests.
    raw_page_size: usize,
}

impl SnapshotRetentionDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            raw_page_size: RAW_PAGE_SIZE,
        }
    }

    /// A store whose discovery reads `raw_page_size` ids a page, so a test can
    /// cross page boundaries without seeding tens of thousands of rows.
    #[cfg(test)]
    pub fn with_raw_page_size(pool: PgPool, raw_page_size: usize) -> Self {
        Self {
            pool,
            raw_page_size,
        }
    }

    #[cfg(test)]
    pub async fn claim_prune(
        &self,
        snapshot_id: SnapshotId,
        keep_n: i64,
    ) -> Result<ClaimOutcome, sqlx::Error> {
        let (sandbox_id, age_secs): (SandboxId, i64) = sqlx::query_as(sql!(
            "SELECT sandbox_id,
                    EXTRACT(EPOCH FROM (NOW() - timestamp))::BIGINT AS age_secs
               FROM snapshots
              WHERE id = $1"
        ))
        .bind(snapshot_id)
        .fetch_one(&self.pool)
        .await?;
        let storage_location: Option<(Uuid, String, bool)> = sqlx::query_as(sql!(
            "SELECT ssl.id,
                    ssl.storage_connection_string,
                    EXISTS (
                        SELECT 1
                          FROM snapshot_storage_location_encrypted_fields ef
                         WHERE ef.snapshot_storage_location_id = ssl.id
                    ) AS has_encrypted_fields
               FROM snapshot_storage_locations ssl
              WHERE ssl.snapshot_id = $1"
        ))
        .bind(snapshot_id)
        .fetch_optional(&self.pool)
        .await?;
        let storage_location = storage_location
            .map(|(id, route, encrypted)| SnapshotStorageLocation::new(id, route, encrypted));
        let candidate = PruneCandidate {
            snapshot_id,
            sandbox_id,
            storage_location,
            age_secs,
        };
        <Self as RetentionSink>::claim_prune(self, candidate, keep_n, None).await
    }
}

impl RetentionSink for SnapshotRetentionDbStore {
    async fn list_candidates(
        &self,
        keep_n: i64,
        limit: i64,
        after: Option<SnapshotId>,
        min_age: Option<Duration>,
    ) -> Result<CandidatePage, sqlx::Error> {
        let ids = snapshot_ids_after(&self.pool, after, self.raw_page_size as i64).await?;
        let mut page = CandidatePage {
            scanned_through: ids.last().copied(),
            exhausted: ids.len() < self.raw_page_size,
            candidates: Vec::new(),
        };
        if ids.is_empty() {
            return Ok(page);
        }
        // Eligibility observes the second read's database state, including siblings
        // outside this ID page. Deleted page rows disappear; newly eligible rows
        // behind the cursor are revisited after wrap. Discovery grants no deletion
        // permission: claim revalidates the live floor and restore source.
        // Only a `disk` row of origin `stop` (a stopped sandbox's evacuation
        // or retire backup) is swept, and it has no keep-set: past the age
        // gate, only the restore source guard keeps it. Named (`snapshot`)
        // and `fork` disk rows are never candidates. A `full` candidate is
        // outside the keep-set exactly when the indexed sibling probe can
        // skip `keep_n - 1` newer ready/full rows with a usable attached
        // location and still find one more. The tuple comparison preserves
        // UUID tie-breaking for equal timestamps. Cursor and age constrain only
        // the candidate; newer siblings of any age still count toward the
        // keep floor. `keep_n` is always positive by configuration, so the
        // OFFSET is non-negative. The unique snapshot-location constraint makes
        // the left join zero-or-one and therefore preserves one row per snapshot.
        // `make_interval` takes the grace as a typed INTERVAL so SQL injection
        // is impossible and a missing `min_age` collapses to "no extra
        // filter", not "NOW() - INTERVAL '0s'".
        //
        // `NOT (restore_source)` excludes any snapshot a restore path would
        // currently load. Being outside the keep-set is not sufficient
        // grounds to delete: restore only picks by recency for *markerless*
        // rows. A marked row restores by exact artifact identity with no
        // newest-snapshot fallback, and a `restoring` row retries from its
        // pre-restore source window — so in both cases the snapshot restore
        // needs can sit outside the keep-set while newer rows (including a
        // failed restore branch's later artifacts) push it down.
        //
        // Rows in `deleting` are deliberately absent: outstanding claims
        // page through [`Self::list_claimed`] on their own cursor, so this
        // scan is purely fresh `ready` work and its keyset cursor can
        // never be dragged backward by a low-id parked claim.
        // Both location probes use the same `orphaned_at IS NULL` definition as
        // the claim's location lock and safety re-check. The PostgreSQL
        // unusable-location tests pin discovery/claim agreement for missing,
        // orphaned, and newly usable candidate/sibling locations.
        // The range narrows the bulk access plan; membership still requires an
        // exact supplied ID, including when rows are inserted between the reads.
        // OFFSET 0 keeps eligibility outside the bounded snapshot lookup and
        // prevents fleet-wide decorrelated joins for the keyed safety probes.
        // Sibling work still depends on keep-N and usable history within a sandbox.
        // A failed bulk read returns no page, so callers cannot advance its cursor.
        let rows = retention::candidates(&self.pool, &ids, keep_n, limit, min_age).await?;
        page.candidates = rows
            .into_iter()
            .map(|row| prune_candidate_from_row(candidate_row(row)))
            .collect::<Result<_, _>>()?;
        if page.candidates.len() as i64 == limit {
            // LIMIT may have left eligible rows inside the raw page unseen.
            page.scanned_through = page.candidates.last().map(|c| c.snapshot_id);
            page.exhausted = false;
        }
        Ok(page)
    }

    async fn claimed_scan_epoch(&self) -> Result<Option<ClaimedScanEpoch>, sqlx::Error> {
        let through = claimed::scan_epoch(&self.pool).await?;
        Ok(through.map(|through| ClaimedScanEpoch { through }))
    }

    async fn list_claimed(
        &self,
        limit: i64,
        after: Option<SnapshotId>,
        through: SnapshotId,
    ) -> Result<Vec<PruneCandidate>, sqlx::Error> {
        // Claims are already de-advertised; page them separately to finish forward.
        let rows = claimed::list(&self.pool, limit, after, through).await?;
        rows.into_iter()
            .map(|row| prune_candidate_from_row(claimed_row(row)))
            .collect()
    }

    async fn claim_prune(
        &self,
        candidate: PruneCandidate,
        keep_n: i64,
        min_age: Option<Duration>,
    ) -> Result<ClaimOutcome, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Fork admission, source deletion, and retention all acquire this
        // advisory lock before the sandbox row lock. Once held, no queued or
        // running fork can freeze or consume this source while its snapshot
        // set is being changed.
        lock_fork_source_lifecycle(&mut tx, candidate.sandbox_id).await?;

        // Serialize marker/restore writers and sibling claims. Revalidate in
        // a later statement so READ COMMITTED observes a writer that committed
        // while this transaction waited for the sandbox lock.
        let locked = snapshot_retention_claim::lock_snapshot_owner(
            &mut tx,
            candidate.snapshot_id,
            candidate.sandbox_id,
        )
        .await?;
        if locked.is_none() {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(
                ClaimRefusalReason::MissingSandboxOrSnapshot,
            ));
        }

        if active_fork_source_operation_exists(&mut tx, candidate.sandbox_id).await? {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(ClaimRefusalReason::ActiveForkSource));
        }

        // Lock the snapshot before reading its child. The storage-location FK
        // makes a concurrent insert take a conflicting parent key lock, so the
        // optional location below cannot change through commit.
        let state: Option<String> = retention::lock_sandbox_snapshot_state(
            &mut *tx,
            candidate.snapshot_id,
            candidate.sandbox_id,
        )
        .await?;
        let refusal = match state.as_deref() {
            None => Some(ClaimRefusalReason::MissingSnapshot),
            Some("ready") => None,
            Some(_) => Some(ClaimRefusalReason::SnapshotNotReady),
        };
        if let Some(reason) = refusal {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(reason));
        }

        let storage_location =
            retention::lock_usable_storage_location(&mut *tx, candidate.snapshot_id).await?;
        let storage_location = storage_location.map(|location| {
            SnapshotStorageLocation::new(
                location.id,
                location.storage_connection_string,
                location.has_encrypted_fields,
            )
        });
        if storage_location
            .as_ref()
            .is_none_or(|location| location.has_encrypted_fields)
        {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(
                ClaimRefusalReason::StorageLocationUnusable,
            ));
        }
        if storage_location.as_ref() != candidate.storage_location.as_ref() {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(
                ClaimRefusalReason::StorageLocationChanged,
            ));
        }

        // `floor_ok` is also what keeps unswept rows unclaimed: a `disk` row of
        // any origin but `stop` fails it and is refused as `KeepNFloor`.
        let safe = retention::claim_safety(&mut *tx, candidate.snapshot_id, keep_n, min_age)
            .await?
            .map(|safe| (safe.floor_ok, safe.restore_source_ok, safe.age_ok));
        // Report the first failing predicate, without another read or changing
        // the bounded sibling probe and database-clock age comparison.
        let refusal = match safe {
            None => Some(ClaimRefusalReason::MissingSandboxOrSnapshot),
            Some((false, _, _)) => Some(ClaimRefusalReason::KeepNFloor),
            Some((_, false, _)) => Some(ClaimRefusalReason::ProtectedRestoreSource),
            Some((_, _, false)) => Some(ClaimRefusalReason::AgeGate),
            Some((true, true, true)) => None,
        };
        if let Some(reason) = refusal {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(reason));
        }

        let claimed = sqlx::query(sql!(
            "UPDATE snapshots SET state = 'deleting' WHERE id = $1 AND state = 'ready'"
        ))
        .bind(candidate.snapshot_id)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            == 1;
        if !claimed {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(ClaimRefusalReason::ConditionalUpdate));
        }

        tx.commit().await?;
        Ok(ClaimOutcome::Claimed(candidate))
    }

    async fn finalize_prune(
        &self,
        snapshot_id: SnapshotId,
        keep_n: i64,
    ) -> Result<PruneFinalize, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Lock the owning sandbox row *first*, before the snapshot row.
        //
        // Two reasons. It makes the marker re-read trustworthy: a
        // concurrent marker writer blocks on this lock until we commit, so
        // `marker_ok` cannot be computed from a value a writer then changes
        // before our DELETE lands. Parent-before-child matches marker-writer
        // ordering and avoids deadlocks between concurrent finalizers. The
        // lock is held only for the catalog transaction, never across object
        // storage.
        //
        // No row means the snapshot is already gone (e.g. a concurrent
        // sandbox-delete cascade reaped it), so there is nothing to
        // finalize — reaping its blobs was harmless since that data was
        // being removed anyway.
        //
        // The marker is projected by the locking statement itself rather
        // than read back separately. Under READ COMMITTED, EvalPlanQual
        // refreshes the locked `sandboxes` row after a wait, so its marker
        // is post-lock. That guarantee does not refresh the unlocked
        // `snapshots` row; projecting its artifact identity is safe only
        // because those columns are immutable after insertion.
        //
        // What this projects is narrower than what the guards match, on
        // purpose. The guards are enforcement, so erring wide is free. This
        // is telemetry asserting "a restore target was destroyed", and it
        // is only true when the row was the snapshot the *applicable*
        // selector would have loaded. Two ways to get that wrong:
        //
        // - Reporting on the broad `snapshot_is_live_restore_source` would
        //   fire for any row inside a `restoring` row's retry window, while
        //   the selector loads only the newest row in it.
        // - Reporting on the exact marker alone would fire for a `restoring`
        //   row, whose selector deliberately *ignores* the marker and reads
        //   the pre-restore window instead — so a failed branch's late
        //   artifact becoming the marker is not a lost restore source.
        //
        // So the breach is scoped to the marked-restore mode: exact marker,
        // and the row is not currently `restoring`. The `restoring` mode is
        // deliberately not detected here — pinning its selected source would
        // need a correlated newest-in-window probe, and this counter is
        // already documented as a floor rather than an exact count. On a
        // metric whose contract is "expected zero, investigate if not", a
        // false positive costs more than a missed rare case.
        //
        // Bind the boolean here rather than re-unwrapping later: a
        // `unwrap_or(false)` further down would read as "unknown means not
        // pinned", which is the wrong default for a data-loss signal even
        // when the branch is provably dead.
        let Some(is_pinned) = lock_owner_marks_snapshot(&mut tx, snapshot_id).await? else {
            tx.rollback().await?;
            return Ok(PruneFinalize::AlreadyGone);
        };

        // Now lock the candidate snapshot row itself and assert the durable
        // claim in the same read. Callers normally reach finalize only after
        // `claim_prune`, but the store enforces that protocol boundary too:
        // deleting an advertised `ready` row here would make PostgreSQL
        // promise bytes the reaper may already have removed.
        let locked: Option<String> = retention::lock_snapshot_state(&mut *tx, snapshot_id).await?;
        let Some(state) = locked else {
            tx.rollback().await?;
            return Ok(PruneFinalize::AlreadyGone);
        };
        if state != "deleting" {
            tx.rollback().await?;
            return Ok(PruneFinalize::NotClaimed);
        }

        // Recheck the keep-N floor purely for telemetry; a swept `disk` row
        // (origin `stop`) has none, so it always passes. This remains bounded
        // at the configured floor while the hot sandbox lock is held.
        // It is not a gate: bytes are already gone, so drift is reported and
        // the unrestorable row still finishes forward.
        let floor_ok = retention::floor_holds(&mut *tx, snapshot_id, keep_n).await?;

        // Delete storage locations first (the FK on snapshot_id is
        // RESTRICT, so the snapshot delete would fail otherwise), then
        // the snapshot row. Unconditional: we hold the row lock and have
        // already reaped its blobs.

        delete_snapshot_catalog(&mut *tx, snapshot_id).await?;

        tx.commit().await?;
        Ok(PruneFinalize::Pruned {
            floor_ok,
            marker_ok: !is_pinned,
        })
    }
}
