//! Postgres retention store: page fresh work and durable claims, claim under
//! the sandbox lock, then finalize only after the object-store reap.

use pplx_space_common_core::types::SandboxId;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::{SandboxLifecycle, SnapshotId};
use pplx_space_sandbox_portability::{
    snapshot_is_current_local_artifact, snapshot_is_live_restore_source,
};
use pplx_space_sandbox_store::fork_source_lifecycle::{
    active_fork_source_operation_exists, lock_fork_source_lifecycle,
};
use pplx_space_snapshot_store::delete_snapshot_catalog;
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

/// Upper bound of the durable claims present when a recovery epoch starts.
///
/// The `OFFSET 0` fence keeps the claim filter on the `(state, type)` index.
/// Without it, `ORDER BY id DESC LIMIT 1` lets the planner walk the primary
/// key backward and filter every row, expecting a claim near the newest IDs.
/// A `deleting` estimate larger than the live claim set, such as statistics
/// sampled while a tick holds its claims, makes that plan look cheap. Claims
/// are old snapshots with low UUIDv7 IDs, so the walk reads nearly the whole
/// table and can exceed the statement timeout while fresh discovery keeps
/// working. With the fence, the cost scales with the claims, not the table.
pub(crate) const CLAIMED_SCAN_EPOCH_SQL: &str = sql!(
    "SELECT claimed.id
       FROM (
             SELECT id
               FROM snapshots
              WHERE type = 'full'
                AND state = 'deleting'
             OFFSET 0
            ) claimed
      ORDER BY claimed.id DESC
      LIMIT 1"
);

/// One recovery page of durable claims in `($2, $3]`, oldest first.
///
/// Same `OFFSET 0` fence as [`CLAIMED_SCAN_EPOCH_SQL`]: the `LIMIT` must not
/// turn the ID range into a primary-key walk that filters unclaimed rows.
pub(crate) const LIST_CLAIMED_SQL: &str = sql!(
    "SELECT
         s.id AS snapshot_id,
         s.sandbox_id,
         ssl.id AS storage_location_id,
         ssl.storage_connection_string,
         EXISTS (
             SELECT 1
               FROM snapshot_storage_location_encrypted_fields ef
              WHERE ef.snapshot_storage_location_id = ssl.id
         ) AS has_encrypted_fields,
         EXTRACT(EPOCH FROM (NOW() - s.timestamp))::BIGINT AS age_secs
     FROM (
           SELECT id, sandbox_id, timestamp
             FROM snapshots
            WHERE type = 'full'
              AND state = 'deleting'
              AND id > COALESCE($2::UUID, '00000000-0000-0000-0000-000000000000'::UUID)
              AND id <= $3
           OFFSET 0
          ) s
     LEFT JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
     ORDER BY s.id ASC
     LIMIT $1"
);

/// SQL fragment for the one usable storage location a snapshot may own.
///
/// All callers pass static aliases from this module. Discovery keeps its SQL
/// whole and repeats this definition in keyed probes; PostgreSQL regressions
/// pin its agreement with claim admission for usable and unusable locations.
fn usable_storage_location_filter(location_alias: &str) -> String {
    format!("{location_alias}.orphaned_at IS NULL")
}

fn usable_snapshot_location_exists(snapshot_alias: &str, location_alias: &str) -> String {
    let usable_location = usable_storage_location_filter(location_alias);
    format!(
        "EXISTS (
             SELECT 1
              FROM snapshot_storage_locations {location_alias}
              WHERE {location_alias}.snapshot_id = {snapshot_alias}.id
                AND {usable_location}
         )"
    )
}

pub struct SnapshotRetentionDbStore {
    pool: PgPool,
}

impl SnapshotRetentionDbStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
        const RAW_PAGE_SIZE: usize = 10_000;
        // One array avoids per-row wire/decoder overhead for the raw ID page.
        let ids: Vec<SnapshotId> = sqlx::query_scalar(sql!(
            "SELECT ARRAY(
                 SELECT id FROM snapshots
                  WHERE id > COALESCE($1::UUID, '00000000-0000-0000-0000-000000000000'::UUID)
                  ORDER BY id
                  LIMIT 10000
             )"
        ))
        .bind(after)
        .fetch_one(&self.pool)
        .await?;
        let mut page = CandidatePage {
            scanned_through: ids.last().copied(),
            exhausted: ids.len() < RAW_PAGE_SIZE,
            candidates: Vec::new(),
        };
        if ids.is_empty() {
            return Ok(page);
        }
        // Eligibility observes the second read's database state, including siblings
        // outside this ID page. Deleted page rows disappear; newly eligible rows
        // behind the cursor are revisited after wrap. Discovery grants no deletion
        // permission: claim revalidates the live floor and restore source.
        let min_age_secs = min_age.map(|d| d.as_secs() as i64);
        // A candidate is outside the keep-set exactly when the indexed
        // sibling probe can skip `keep_n - 1` newer ready/full rows with a
        // usable attached location and still find one more. The tuple
        // comparison preserves UUID
        // tie-breaking for equal timestamps. Cursor and age constrain only
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
        // usable_storage_location_filter / usable_snapshot_location_exists.
        // Keep this statement whole and its keyed OFFSET 0 fences visible;
        // the PostgreSQL unusable-location tests pin discovery/claim agreement
        // for missing, orphaned, and newly usable candidate/sibling locations.
        let restore_source = snapshot_is_live_restore_source("sb", "s");
        let query = format!(
            "SELECT
                 s.id AS snapshot_id,
                 s.sandbox_id,
                 ssl.id AS storage_location_id,
                 ssl.storage_connection_string,
                 EXISTS (
                     SELECT 1
                       FROM snapshot_storage_location_encrypted_fields ef
                      WHERE ef.snapshot_storage_location_id = ssl.id
                 ) AS has_encrypted_fields,
                 EXTRACT(EPOCH FROM (NOW() - s.timestamp))::BIGINT AS age_secs
             FROM (
                 SELECT id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind
                   FROM snapshots
                  WHERE id >= $5::UUID AND id <= $6::UUID
                    AND id = ANY($3::UUID[])
                  OFFSET 0
             ) s
             JOIN LATERAL (
                 SELECT id, lifecycle, lifecycle_changed_at, placement_changed_at,
                        current_local_artifact_id, current_local_artifact_kind
                   FROM sandboxes
                  WHERE id = s.sandbox_id
                  OFFSET 0
             ) sb ON TRUE
             LEFT JOIN LATERAL (
                 SELECT id, storage_connection_string
                   FROM snapshot_storage_locations
                  WHERE snapshot_id = s.id AND orphaned_at IS NULL
                  OFFSET 0
             ) ssl ON TRUE
             WHERE s.type = 'full'
               AND s.state = 'ready'
               AND ($4::BIGINT IS NULL
                    OR s.timestamp < NOW() - make_interval(secs => $4::BIGINT))
               AND NOT {restore_source}
               AND EXISTS (
                   SELECT 1
                     FROM snapshots newer
                    WHERE newer.sandbox_id = s.sandbox_id
                      AND newer.state = 'ready'
                      AND newer.type = 'full'
                      AND EXISTS (
                          SELECT 1 FROM snapshot_storage_locations newer_ssl
                           WHERE newer_ssl.snapshot_id = newer.id
                             AND newer_ssl.orphaned_at IS NULL
                           OFFSET 0
                      )
                      AND (newer.timestamp, newer.id) > (s.timestamp, s.id)
                   ORDER BY newer.timestamp DESC
                   OFFSET ($1 - 1)
                   LIMIT 1
               )
             ORDER BY s.id ASC
             LIMIT $2",
        );
        // The range narrows the bulk access plan; membership still requires an
        // exact supplied ID, including when rows are inserted between the reads.
        // OFFSET 0 keeps eligibility outside the bounded snapshot lookup and
        // prevents fleet-wide decorrelated joins for the keyed safety probes.
        // Sibling work still depends on keep-N and usable history within a sandbox.
        // A failed bulk read returns no page, so callers cannot advance its cursor.
        let rows: Vec<PruneCandidateRow> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(keep_n)
                .bind(limit)
                .bind(&ids)
                .bind(min_age_secs)
                .bind(ids.first().copied())
                .bind(ids.last().copied())
                .fetch_all(&self.pool)
                .await?;
        page.candidates = rows
            .into_iter()
            .map(prune_candidate_from_row)
            .collect::<Result<_, _>>()?;
        if page.candidates.len() as i64 == limit {
            // LIMIT may have left eligible rows inside the raw page unseen.
            page.scanned_through = page.candidates.last().map(|c| c.snapshot_id);
            page.exhausted = false;
        }
        Ok(page)
    }

    async fn claimed_scan_epoch(&self) -> Result<Option<ClaimedScanEpoch>, sqlx::Error> {
        let through: Option<SnapshotId> = sqlx::query_scalar(CLAIMED_SCAN_EPOCH_SQL)
            .fetch_optional(&self.pool)
            .await?;
        Ok(through.map(|through| ClaimedScanEpoch { through }))
    }

    async fn list_claimed(
        &self,
        limit: i64,
        after: Option<SnapshotId>,
        through: SnapshotId,
    ) -> Result<Vec<PruneCandidate>, sqlx::Error> {
        // Claims are already de-advertised; page them separately to finish forward.
        let rows: Vec<PruneCandidateRow> = sqlx::query_as(LIST_CLAIMED_SQL)
            .bind(limit)
            .bind(after)
            .bind(through)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(prune_candidate_from_row).collect()
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
        let locked: Option<(Uuid,)> = sqlx::query_as(sql!(
            "SELECT sb.id
               FROM sandboxes sb
               JOIN snapshots cur ON cur.sandbox_id = sb.id
              WHERE cur.id = $1
                AND sb.id = $2
              FOR UPDATE OF sb"
        ))
        .bind(candidate.snapshot_id)
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
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
        let state: Option<(String, String)> = sqlx::query_as(sql!(
            "SELECT state, type
               FROM snapshots
              WHERE id = $1
                AND sandbox_id = $2
              FOR UPDATE"
        ))
        .bind(candidate.snapshot_id)
        .bind(candidate.sandbox_id)
        .fetch_optional(&mut *tx)
        .await?;
        let refusal = match state.as_ref() {
            None => Some(ClaimRefusalReason::MissingSnapshot),
            Some((_, kind)) if kind != "full" => Some(ClaimRefusalReason::SnapshotNotFull),
            Some((state, _)) if state != "ready" => Some(ClaimRefusalReason::SnapshotNotReady),
            Some(_) => None,
        };
        if let Some(reason) = refusal {
            tx.rollback().await?;
            return Ok(ClaimOutcome::Refused(reason));
        }

        let usable_current_location = usable_storage_location_filter("ssl");
        let location_query = format!(
            "SELECT ssl.id,
                    ssl.storage_connection_string,
                    EXISTS (
                        SELECT 1
                          FROM snapshot_storage_location_encrypted_fields ef
                         WHERE ef.snapshot_storage_location_id = ssl.id
                    ) AS has_encrypted_fields
               FROM snapshot_storage_locations ssl
              WHERE ssl.snapshot_id = $1
                AND {usable_current_location}
              FOR UPDATE OF ssl"
        );
        let storage_location: Option<(Uuid, String, bool)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(location_query.as_str())))
                .bind(candidate.snapshot_id)
                .fetch_optional(&mut *tx)
                .await?;
        let storage_location = storage_location
            .map(|(id, route, encrypted)| SnapshotStorageLocation::new(id, route, encrypted));
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

        let restore_source = snapshot_is_live_restore_source("sb", "cur");
        let newer_location = usable_snapshot_location_exists("newer", "newer_ssl");
        let min_age_secs = min_age.map(|duration| duration.as_secs() as i64);
        let query = format!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM snapshots newer
                  WHERE newer.sandbox_id = cur.sandbox_id
                    AND newer.state = 'ready'
                    AND newer.type = 'full'
                    AND {newer_location}
                    AND (newer.timestamp, newer.id) > (cur.timestamp, cur.id)
                  OFFSET ($2 - 1)
                  LIMIT 1
             ) AS floor_ok,
             NOT {restore_source} AS restore_source_ok,
             ($3::BIGINT IS NULL
                OR cur.timestamp < NOW() - make_interval(secs => $3::BIGINT)) AS age_ok
             FROM snapshots cur
             JOIN sandboxes sb ON sb.id = cur.sandbox_id
             WHERE cur.id = $1
               AND cur.state = 'ready'
               AND cur.type = 'full'",
        );
        let safe: Option<(bool, bool, bool)> =
            sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
                .bind(candidate.snapshot_id)
                .bind(keep_n)
                .bind(min_age_secs)
                .fetch_optional(&mut *tx)
                .await?;
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
        // The marker is projected by this same statement rather than read
        // back separately. Under READ COMMITTED, EvalPlanQual refreshes the
        // locked `sandboxes` row after a wait, so its marker is post-lock.
        // That guarantee does not refresh the unlocked `snapshots` row;
        // projecting its artifact identity is safe only because those
        // columns are immutable after insertion.
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
        let restoring = SandboxLifecycle::Restoring.as_str();
        let marker_query = format!(
            "SELECT (sb.lifecycle <> '{restoring}' AND {marked})
               FROM snapshots cur
               JOIN sandboxes sb ON sb.id = cur.sandbox_id
              WHERE cur.id = $1
                FOR UPDATE OF sb",
            marked = snapshot_is_current_local_artifact("sb", "cur"),
        );
        // No row means the snapshot is already gone. Bind the boolean here
        // rather than re-unwrapping later: a `unwrap_or(false)` further down
        // would read as "unknown means not pinned", which is the wrong
        // default for a data-loss signal even when the branch is provably
        // dead.
        let Some(is_pinned) =
            sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(marker_query.as_str())))
                .bind(snapshot_id)
                .fetch_optional(&mut *tx)
                .await?
        else {
            tx.rollback().await?;
            return Ok(PruneFinalize::AlreadyGone);
        };

        // Now lock the candidate snapshot row itself and assert the durable
        // claim in the same read. Callers normally reach finalize only after
        // `claim_prune`, but the store enforces that protocol boundary too:
        // deleting an advertised `ready` row here would make PostgreSQL
        // promise bytes the reaper may already have removed.
        let locked: Option<String> = sqlx::query_scalar(sql!(
            "SELECT state
               FROM snapshots
              WHERE id = $1
              FOR UPDATE"
        ))
        .bind(snapshot_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(state) = locked else {
            tx.rollback().await?;
            return Ok(PruneFinalize::AlreadyGone);
        };
        if state != "deleting" {
            tx.rollback().await?;
            return Ok(PruneFinalize::NotClaimed);
        }

        // Recheck the keep-N floor purely for telemetry. This remains bounded
        // at the configured floor while the hot sandbox lock is held.
        // It is not a gate: bytes are already gone, so drift is reported and
        // the unrestorable row still finishes forward.
        let newer_location = usable_snapshot_location_exists("newer", "newer_ssl");
        let floor_query = format!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM snapshots newer
                   JOIN snapshots cur ON cur.id = $1
                  WHERE newer.sandbox_id = cur.sandbox_id
                    AND newer.state = 'ready'
                    AND newer.type = 'full'
                    AND {newer_location}
                    AND (newer.timestamp, newer.id) > (cur.timestamp, cur.id)
                  OFFSET ($2 - 1)
                  LIMIT 1
             )"
        );
        let floor_ok: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(floor_query.as_str())))
            .bind(snapshot_id)
            .bind(keep_n)
            .fetch_one(&mut *tx)
            .await?;

        // Delete storage locations first (the FK on snapshot_id is
        // RESTRICT, so the snapshot delete would fail otherwise), then
        // the snapshot row. Unconditional: we hold the row lock and have
        // already reaped its blobs.

        delete_snapshot_catalog(&mut tx, snapshot_id).await?;

        tx.commit().await?;
        Ok(PruneFinalize::Pruned {
            floor_ok,
            marker_ok: !is_pinned,
        })
    }
}
