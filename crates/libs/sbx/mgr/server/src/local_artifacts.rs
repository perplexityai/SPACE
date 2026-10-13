//! Typed access to the `local_artifacts` table.
//!
//! This module is the sole entry point for all reads and writes of
//! `local_artifacts` rows. Callers do not import `crate::db` for
//! anything local-artifact-related; they go through [`LocalArtifacts`]
//! and the [`RestoreLease`] / [`DeleteLease`] handles it returns.
//!
//! ## State machine
//!
//! Each row carries a `state` column (migration 029) — one of
//! [`ArtifactState::Ready`], [`ArtifactState::Restoring`],
//! [`ArtifactState::Deleting`]. Rows are published in `Ready` and
//! transitioned via lease checkout:
//!
//! ```text
//!                publish (insert)
//!                     │
//!                     ▼
//!                  ready
//!                 /     \
//!     check_out  /       \ check_out
//!     _for_     /         \ _for_
//!     restore  /           \ delete
//!             ▼             ▼
//!         restoring      deleting
//!             │             │
//!     release │             │ commit
//!             │             ▼
//!             └─► ready   (row gone)
//!                            │
//!                       Drop │ (no commit)
//!                            ▼
//!                          ready
//! ```
//!
//! `Restoring → Deleting` and `Deleting → Restoring` are never legal;
//! the CAS-based checkouts reject them as
//! [`CheckoutError::NotReady`].
//!
//! ## Why leases
//!
//! Operations that mutate `local_path` on disk (restore reads the
//! bytes, delete renames-and-removes the directory) must hold the
//! row in a transient non-`Ready` state so no concurrent operation
//! can race them. The lease handles encapsulate that contract:
//! checkout atomically CAS the row into the right state; the
//! caller's work happens with the lease held; an explicit
//! [`RestoreLease::release`] / [`DeleteLease::commit`] resolves the
//! transition; `Drop` rolls back best-effort if neither was called
//! (e.g. on a panic or early `?` return). Process death (SIGKILL,
//! OOM, reboot) skips `Drop` entirely —
//! [`LocalArtifacts::recover_stuck_states`] runs at startup to
//! resolve rows stranded in a non-`Ready` state.
//!
//! ## Reaper interaction
//!
//! The reaper's guarded checkout validates pin and activity evidence in the
//! same transaction as delete admission. It shares the `local_path` safety
//! gate with explicit deletion, whose semantics do not require idle time.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId, SnapshotName};
use pplx_space_common_svc::sql::sqlite::SqliteU64;
use rusqlite::{Connection, OptionalExtension, params};

use crate::db::LocalArtifactRow;

/// One sandbox's current lifecycle artifact that this node cannot prove has
/// a durable remote copy.
///
/// Identity comes from the `sandboxes` pin rather than from the artifact
/// row, because the case where there *is* no artifact row is one of the
/// cases this reports. Migration 026's CHECK keeps `current_local_artifact_id`
/// and `current_local_artifact_kind` set or unset together, so the pin alone
/// always carries a whole identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingBackup {
    pub sandbox_id: String,
    pub local_artifact_id: LocalArtifactId,
    pub artifact_kind: ArtifactKind,
}

/// Logical state of a `local_artifacts` row. Encoded on the row's
/// `state` column; transitions are CAS via
/// [`LocalArtifacts::check_out_for_restore`] /
/// [`LocalArtifacts::check_out_for_delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactState {
    /// Published and available. The only state from which a checkout
    /// (restore or delete) can succeed.
    Ready,
    /// A restore is currently reading this artifact's `local_path`.
    /// Reapers must skip; deleters must wait or fail with NotReady.
    Restoring,
    /// A delete has reserved this row. The on-disk directory has
    /// usually been renamed to a `.gc-pending-<id>` sibling. Readers
    /// should treat the artifact as gone.
    Deleting,
}

impl ArtifactState {
    pub fn as_str(self) -> &'static str {
        match self {
            ArtifactState::Ready => "ready",
            ArtifactState::Restoring => "restoring",
            ArtifactState::Deleting => "deleting",
        }
    }
}

/// Error parsing an [`ArtifactState`] from its wire/DB string. The
/// CHECK constraint on `local_artifacts.state` should make this
/// unreachable for rows the schema accepted; it surfaces table
/// corruption or migration drift.
#[derive(Debug, thiserror::Error)]
#[error("unrecognized artifact state {0:?}")]
pub struct ParseArtifactStateError(String);

impl std::str::FromStr for ArtifactState {
    type Err = ParseArtifactStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ready" => Ok(ArtifactState::Ready),
            "restoring" => Ok(ArtifactState::Restoring),
            "deleting" => Ok(ArtifactState::Deleting),
            _ => Err(ParseArtifactStateError(s.to_string())),
        }
    }
}

impl std::fmt::Display for ArtifactState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Reason a checkout (restore or delete) failed.
#[derive(Debug, thiserror::Error)]
pub enum CheckoutError {
    #[error("local artifact {0} is pinned by a sandbox; refusing deletion")]
    Pinned(LocalArtifactId),
    #[error("local artifact {0} not found")]
    NotFound(LocalArtifactId),
    /// Named-snapshot lookup found no row for `(sandbox_id,
    /// snapshot_name)`. Distinct from [`Self::NotFound`] because at
    /// this point there is no artifact id to report.
    #[error("no local artifact for sandbox {sandbox_id} snapshot {snapshot_name}")]
    NamedNotFound {
        sandbox_id: String,
        snapshot_name: SnapshotName,
    },
    #[error("local artifact {id} is in state {state}, cannot check out")]
    NotReady {
        id: LocalArtifactId,
        state: ArtifactState,
    },
    /// Delete-only: refused because more than one `local_artifacts`
    /// row points at this `local_path`. Acting would corrupt the
    /// live sandbox's bytes. The deeper fix (publish must dedupe) is
    /// tracked separately; this is the defensive shield.
    #[error(
        "local artifact {id} shares local_path with {sibling_count} other row(s); refusing to act"
    )]
    SharedLocalPath {
        id: LocalArtifactId,
        sibling_count: u64,
    },
    /// The DB row's `state` column did not parse to a known
    /// [`ArtifactState`] value. The CHECK constraint should make
    /// this unreachable; if it ever fires, it indicates table
    /// corruption or a migration drift and the caller is expected
    /// to surface it loudly.
    #[error("local artifact {id} has unrecognized state value {raw_state:?}")]
    CorruptState {
        id: LocalArtifactId,
        raw_state: String,
    },
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
}

#[derive(Clone, Copy)]
pub(crate) struct GcEligibility {
    pub now_secs: u64,
    pub idle_secs: u64,
    pub grace_secs: u64,
}

pub(crate) enum GcDeleteCheckout {
    Leased(DeleteLease),
    Pinned,
    Recent,
    Unbacked,
}

/// All `local_artifacts` operations route through this handle. The
/// underlying connection is the same `Arc<Mutex<rusqlite::Connection>>`
/// that drives the rest of sbx-mgr's SQLite state, so transactions
/// here serialize against every other path that touches that
/// connection.
#[derive(Clone)]
pub struct LocalArtifacts {
    db: Arc<Mutex<Connection>>,
    orphan_scan: Arc<Mutex<OrphanScan>>,
}

/// Point-in-time publication evidence for a named snapshot, not a checkout lease.
pub(crate) struct NamedArtifactPublication {
    pub snapshot_name: SnapshotName,
    pub local_path: String,
    pub artifact_kind: ArtifactKind,
    pub state: ArtifactState,
}

#[derive(Clone, Copy)]
struct OrphanScan {
    next: i64,
    through: Option<i64>,
}

impl Default for OrphanScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

impl LocalArtifacts {
    pub fn new(db: Arc<Mutex<Connection>>) -> Self {
        Self {
            db,
            orphan_scan: Arc::new(Mutex::new(OrphanScan::default())),
        }
    }

    /// Read-only fetch by `local_artifact_id`. Does **not** change
    /// state; use [`Self::check_out_for_restore`] before reading the
    /// artifact's `local_path` bytes if the read is part of a
    /// long-lived lifecycle operation (e.g. restore).
    pub fn get(
        &self,
        local_artifact_id: LocalArtifactId,
    ) -> rusqlite::Result<Option<LocalArtifactRow>> {
        let conn = self.db_conn()?;
        crate::db::get_local_artifact(&conn, local_artifact_id)
    }

    /// Read-only fetch by `(sandbox_id, snapshot_name)` for the
    /// named-snapshot origin.
    pub fn get_named(
        &self,
        sandbox_id: &str,
        snapshot_name: &SnapshotName,
    ) -> rusqlite::Result<Option<LocalArtifactRow>> {
        let conn = self.db_conn()?;
        crate::db::get_named_local_artifact(&conn, sandbox_id, snapshot_name)
    }

    /// Every sandbox whose **current lifecycle artifact** this node cannot
    /// prove is backed up.
    ///
    /// Scoped to the pin, not to the disk. Two kinds of unbacked local
    /// bytes are deliberately outside it, because the control-plane queue
    /// this feeds (`node_local_backup_pending`, in
    /// `db/postgres/20261002070625_baseline.up.sql`) is
    /// keyed `PRIMARY KEY (node_id, sandbox_id)` and so holds one artifact
    /// per sandbox: a suspended sandbox carrying **no** pin
    /// (`sandbox_eviction::LocalEvidence::Unpinned`), and `snapshot`-origin
    /// named snapshots whose upload is still in flight. Both are states
    /// sbx-mgr's own reapers protect, so a caller must not read this as
    /// "nothing else on this disk matters".
    ///
    /// Not reporting them does not risk releasing a node over them: the
    /// drain gate is a separate predicate over `sandboxes`
    /// (`drain_gate_blocker_pairs`, `... AND NOT
    /// snapshot_durable_for_source_release(s)`), and with no ready snapshot
    /// an unpinned row is not durable and holds the node. The exposure is
    /// the opposite one — a node held by rows no queue-driven pass
    /// enumerates for backup, so the drain parks. The controller side owns
    /// that; it falls back to the history statement when the queue is empty
    /// and the gate disagrees that the node is finished.
    ///
    /// Three cases produce a row, and only the first is the ordinary one:
    ///
    /// * the pinned artifact exists and has no `backup_completed_at` stamp;
    /// * the pin names a `local_artifacts` row that is **gone**;
    /// * the pin names a row owned by a **different** sandbox.
    ///
    /// The last two are why the join is a LEFT join driven from
    /// `sandboxes`. An inner join would omit exactly those sandboxes from a
    /// report that claims to be the node's whole answer for the artifacts
    /// it does cover, and the drain would then release the node over bytes
    /// nobody could account for. Reporting them only ever *adds* a
    /// candidate: the drain re-applies the authoritative durability
    /// predicate to every row, so a pin whose bytes really are durable
    /// still clears.
    ///
    /// `sandbox_eviction::gather_local_evidence` reaches the same verdict
    /// on both: its lookup is keyed on the same `(local_artifact_id,
    /// sandbox_id)` pair, and either mismatch becomes `PinnedRowMissing`,
    /// which refuses to evict. The two diverge only on an identity that
    /// will not parse — and they do not read it from the same columns:
    /// eviction passes the pin through as an opaque string and parses the
    /// artifact row's own `local_artifact_id` / `artifact_kind`, while
    /// this query parses the sandbox row's two pin columns. The
    /// difference in outcome is deliberate either way. Eviction degrades
    /// that one sandbox to `PinnedRowMissing`; this call fails outright,
    /// because a per-sandbox decision can afford to degrade per sandbox
    /// while a whole-node report cannot be wrong about one row and still
    /// claim to be the node's answer.
    ///
    /// Dangling pins stay rare rather than routine: the snapshot GC's
    /// second criterion is "not pinned to a live instance", filtered by its
    /// planner and re-checked in `snapshot_gc::execute_one`. That re-check
    /// sits deliberately *outside* the delete lease — pin status lives on
    /// `sandboxes`, not on the artifact row, so the lease cannot cover it —
    /// which narrows the window rather than closing it. One more reason to
    /// report the state than to drop it.
    ///
    /// The stamp is the node's own evidence, so this over-reports one
    /// class it cannot see past: a **restored** artifact.
    /// `db::publish_restored_suspended_artifact` inserts the row with no
    /// `backup_completed_at`, so a restored sandbox enters this set even
    /// though the backup it was restored *from* is durable.
    ///
    /// It can leave again without a re-suspend, but only sometimes:
    /// `snapshot_upload_jobs::backfill_backup_completed_markers` re-stamps
    /// from completed upload-job payloads keyed on `local_artifact_id`
    /// alone, and a restore reuses the id from the backup's metadata
    /// sidecar, so a restore landing back on the node that produced the
    /// backup is re-stamped if that job row survives pruning. A cross-node
    /// restore, or one whose job row is gone, stays reported until it is
    /// re-suspended and re-uploaded. It
    /// resolves on the reader's side rather than here: the restore reuses
    /// the artifact id carried in the backup's metadata sidecar
    /// (`recovery_cross_node::transition_restored_sandbox_to_suspended_parts`
    /// passes `meta.local_artifact_id`), so a ready `snapshots` row for
    /// that exact id already exists and the durability predicate clears
    /// it. `sandbox_eviction` resolves the same gap the same way, by
    /// asking the cluster (`PreDecision::ProvePinned`). The cost is
    /// volume, not correctness: on a restore-heavy node this set is larger
    /// than the in-flight backup count, so queue depth is a weaker signal
    /// than it looks.
    ///
    /// Identity comes from the pin columns, not the artifact row — in two
    /// of the three cases there is no artifact row to read. The two agree
    /// whenever both exist: every publish path writes the row and the pin
    /// in one `immediate_transaction` with the same kind.
    ///
    /// At most one row per sandbox, because `local_artifact_id` is the
    /// artifact table's primary key. That is what the queue's
    /// `PRIMARY KEY (node_id, sandbox_id)` needs from this surface — a
    /// report naming one sandbox twice could not be stored.
    ///
    /// A named snapshot cannot reach the pin because no publish path puts
    /// it there — `db::publish_named_local_artifact` inserts the artifact
    /// and deliberately leaves `current_local_artifact_id` alone. (Migration
    /// 026's CHECK is a weaker, separate guarantee: it constrains the
    /// sandbox row's own `current_local_artifact_origin` label, and nothing
    /// couples that column to `local_artifacts.artifact_origin`.)
    ///
    /// A pin that does not decode fails the whole call. Skipping it would
    /// hand the caller a set that looks complete and is short by exactly
    /// the artifact nobody can name; a failure instead leaves the caller
    /// with no report, which every consumer already has to handle.
    pub fn list_pending_backups(&self) -> rusqlite::Result<Vec<PendingBackup>> {
        let conn = self.db_conn()?;
        conn.prepare(
            "SELECT s.id, s.current_local_artifact_id, s.current_local_artifact_kind
               FROM sandboxes AS s
               LEFT JOIN local_artifacts AS a
                 ON a.local_artifact_id = s.current_local_artifact_id
                AND a.sandbox_id = s.id
              WHERE s.current_local_artifact_id IS NOT NULL
                AND (a.local_artifact_id IS NULL OR a.backup_completed_at IS NULL)
              ORDER BY s.id",
        )?
        .query_map([], |row| {
            Ok(PendingBackup {
                sandbox_id: row.get(0)?,
                local_artifact_id: row.get::<_, String>(1)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
            })
        })?
        .collect()
    }

    /// Inventory-only observation. Callers must still acquire a checkout before
    /// using the bytes; publication state may change after this read.
    pub(crate) fn list_named_publications(
        &self,
        sandbox_id: &str,
    ) -> Result<Vec<NamedArtifactPublication>, crate::db::inventory_read::InventoryReadError> {
        use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};
        let conn = self.db_conn()?;
        let mut statement = conn.prepare(sql!(
            "SELECT snapshot_name, local_path, artifact_kind, state
             FROM local_artifacts WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot'"
        ))?;
        let mut rows = statement.query([sandbox_id])?;
        let mut budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
        let mut publications = Vec::new();
        while let Some(row) = rows.next()? {
            crate::db::inventory_read::check_row(row, &mut budget)?;
            publications.push(NamedArtifactPublication {
                snapshot_name: row.get::<_, String>(0)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                local_path: row.get(1)?,
                artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                state: row.get::<_, String>(3)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
            });
        }
        Ok(publications)
    }

    /// Mark the artifact's remote backup as durably completed. This
    /// local marker lets eviction and GC prove backup durability
    /// without another cluster API request.
    pub fn mark_backup_completed(
        &self,
        local_artifact_id: LocalArtifactId,
    ) -> rusqlite::Result<()> {
        let conn = self.db_conn()?;
        crate::db::mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string())
    }

    /// Publish a stopped-sandbox disk artifact. INSERTs the row in
    /// `Ready` and atomically flips the sandbox row to `Stopped`
    /// with the new artifact pinned.
    pub fn publish_stopped(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> rusqlite::Result<()> {
        let conn = self.db_conn()?;
        crate::db::publish_stopped_artifact(&conn, sandbox_id, local_artifact_id, local_path)
    }

    /// Publish a named user snapshot. INSERTs the row in `Ready`;
    /// does NOT touch the sandbox row's pin (named snapshots are
    /// orthogonal to `current_local_artifact_id`).
    pub fn publish_named_snapshot(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        artifact_kind: ArtifactKind,
        snapshot_name: &SnapshotName,
        local_path: &str,
    ) -> rusqlite::Result<()> {
        let conn = self.db_conn()?;
        crate::db::publish_named_local_artifact(
            &conn,
            sandbox_id,
            local_artifact_id,
            artifact_kind,
            snapshot_name,
            local_path,
        )
    }

    /// Legacy adoption path: when a Suspended sandbox row exists
    /// without `current_local_artifact_id` and on-disk files are
    /// present, retroactively assign the artifact id and pin it.
    /// Used by the restore-from-suspended code path for pre-026
    /// sandboxes.
    pub fn adopt_suspended(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> rusqlite::Result<bool> {
        let conn = self.db_conn()?;
        crate::db::adopt_suspended_local_artifact(&conn, sandbox_id, local_artifact_id, local_path)
    }

    /// Legacy adoption path for background repair. It pins the current
    /// suspended bytes without changing the sandbox's idle timestamp.
    pub fn adopt_suspended_preserving_updated_at(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> rusqlite::Result<bool> {
        let conn = self.db_conn()?;
        crate::db::adopt_suspended_local_artifact_preserving_updated_at(
            &conn,
            sandbox_id,
            local_artifact_id,
            local_path,
        )
    }

    /// Atomically CAS `state = 'ready' → 'restoring'` on the named
    /// row. Returns a [`RestoreLease`] on success; the artifact is
    /// held in `Restoring` until the lease is released or dropped.
    ///
    /// Errors:
    /// - [`CheckoutError::NotFound`]: no such row.
    /// - [`CheckoutError::NotReady`]: row is in `Restoring` or
    ///   `Deleting` — someone else holds it.
    pub fn check_out_for_restore(
        &self,
        local_artifact_id: LocalArtifactId,
    ) -> Result<RestoreLease, CheckoutError> {
        let conn = self.db_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        let id_str = local_artifact_id.to_string();
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT local_path, state FROM local_artifacts WHERE local_artifact_id = ?1",
                params![id_str],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((local_path, state)) = row else {
            return Err(CheckoutError::NotFound(local_artifact_id));
        };
        let Ok(state) = state.parse::<ArtifactState>() else {
            return Err(CheckoutError::CorruptState {
                id: local_artifact_id,
                raw_state: state,
            });
        };
        if state != ArtifactState::Ready {
            return Err(CheckoutError::NotReady {
                id: local_artifact_id,
                state,
            });
        }
        // CAS — only flip if still ready. If a concurrent path won
        // the race between the SELECT and the UPDATE, the UPDATE
        // affects 0 rows and we surface NotReady.
        let changed = tx.execute(
            "UPDATE local_artifacts SET state = 'restoring'
             WHERE local_artifact_id = ?1 AND state = 'ready'",
            params![id_str],
        )?;
        if changed == 0 {
            return Err(CheckoutError::NotReady {
                id: local_artifact_id,
                state: ArtifactState::Restoring,
            });
        }
        tx.commit()?;
        drop(conn);
        Ok(RestoreLease {
            id: local_artifact_id,
            local_path,
            db: Arc::clone(&self.db),
            released: false,
        })
    }

    /// Atomically CAS `state = 'ready' → 'deleting'` on the named
    /// row, after enforcing the shared-`local_path` safety gate.
    /// Returns a [`DeleteLease`] on success.
    ///
    /// Errors include [`CheckoutError::SharedLocalPath`] when more
    /// than one row points at the same `local_path`; that's the
    /// defensive shield against successive suspended artifacts reusing a
    /// per-sandbox path.
    pub fn check_out_for_delete(
        &self,
        local_artifact_id: LocalArtifactId,
    ) -> Result<DeleteLease, CheckoutError> {
        let conn = self.db_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        let local_path = Self::checkout_delete_in_transaction(&tx, local_artifact_id)?;
        tx.commit()?;
        drop(conn);
        Ok(DeleteLease {
            id: local_artifact_id,
            local_path,
            db: Arc::clone(&self.db),
            committed: false,
        })
    }

    /// Eligibility and Ready→Deleting share a write transaction. A restore
    /// cannot release back to Ready and refresh activity between those checks.
    pub(crate) fn check_out_for_gc(
        &self,
        local_artifact_id: LocalArtifactId,
        sandbox_id: &str,
        eligibility: GcEligibility,
    ) -> Result<GcDeleteCheckout, CheckoutError> {
        use crate::db::timestamps::EpochSeconds;

        let conn = self.db_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT created_at, COALESCE(last_touched_at, created_at),
                    EXISTS(
                        SELECT 1 FROM sandboxes
                         WHERE current_local_artifact_id = ?1
                            OR suspend_predecessor_artifact_id = ?1
                    ),
                    backup_completed_at IS NOT NULL
               FROM local_artifacts WHERE local_artifact_id = ?1 AND sandbox_id = ?2",
                params![local_artifact_id.to_string(), sandbox_id],
                |row| {
                    Ok((
                        row.get::<_, EpochSeconds>(0)?,
                        row.get::<_, EpochSeconds>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((created_at, touched_at, pinned, backed_up)) = row else {
            return Err(CheckoutError::NotFound(local_artifact_id));
        };
        if pinned {
            return Ok(GcDeleteCheckout::Pinned);
        }
        if !backed_up {
            return Ok(GcDeleteCheckout::Unbacked);
        }
        let created_at = created_at.unsigned();
        let touched_at = touched_at.unsigned();
        if eligibility.now_secs.saturating_sub(touched_at) < eligibility.idle_secs
            || eligibility
                .now_secs
                .saturating_sub(created_at.max(touched_at))
                < eligibility.grace_secs
        {
            return Ok(GcDeleteCheckout::Recent);
        }
        let local_path = Self::checkout_delete_in_transaction(&tx, local_artifact_id)?;
        tx.commit()?;
        drop(conn);
        Ok(GcDeleteCheckout::Leased(DeleteLease {
            id: local_artifact_id,
            local_path,
            db: Arc::clone(&self.db),
            committed: false,
        }))
    }

    fn checkout_delete_in_transaction(
        tx: &rusqlite::Transaction<'_>,
        local_artifact_id: LocalArtifactId,
    ) -> Result<String, CheckoutError> {
        let id_str = local_artifact_id.to_string();
        // The IMMEDIATE transaction serializes pin changes with checkout for
        // every deleter, including named-snapshot deletion outside GC.
        let pinned: bool = tx.query_row(
            sql!(
                "SELECT EXISTS(SELECT 1 FROM sandboxes
                   WHERE current_local_artifact_id = ?1 OR suspend_predecessor_artifact_id = ?1)"
            ),
            [&id_str],
            |row| row.get(0),
        )?;
        if pinned {
            return Err(CheckoutError::Pinned(local_artifact_id));
        }
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT local_path, state FROM local_artifacts WHERE local_artifact_id = ?1",
                params![id_str],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((local_path, state)) = row else {
            return Err(CheckoutError::NotFound(local_artifact_id));
        };
        let Ok(state) = state.parse::<ArtifactState>() else {
            return Err(CheckoutError::CorruptState {
                id: local_artifact_id,
                raw_state: state,
            });
        };
        if state != ArtifactState::Ready {
            return Err(CheckoutError::NotReady {
                id: local_artifact_id,
                state,
            });
        }
        // Shared-path safety gate. The reaper's old plan-time
        // check moved here so DeleteSnapshots gets it too.
        let path_count: u64 = tx.query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE local_path = ?1",
            params![local_path],
            |r| Ok(r.get::<_, SqliteU64>(0)?.0),
        )?;
        if path_count != 1 {
            return Err(CheckoutError::SharedLocalPath {
                id: local_artifact_id,
                sibling_count: path_count.saturating_sub(1),
            });
        }
        let changed = tx.execute(
            "UPDATE local_artifacts SET state = 'deleting'
             WHERE local_artifact_id = ?1 AND state = 'ready'",
            params![id_str],
        )?;
        if changed == 0 {
            return Err(CheckoutError::NotReady {
                id: local_artifact_id,
                state: ArtifactState::Deleting,
            });
        }
        Ok(local_path)
    }

    /// `check_out_for_delete` keyed by `(sandbox_id, snapshot_name)`
    /// — the lookup shape DeleteSnapshots uses. Returns
    /// [`CheckoutError::NamedNotFound`] if no row matches.
    pub fn check_out_named_for_delete(
        &self,
        sandbox_id: &str,
        snapshot_name: &SnapshotName,
    ) -> Result<DeleteLease, CheckoutError> {
        let id = {
            let conn = self.db_conn()?;
            let row = crate::db::get_named_local_artifact(&conn, sandbox_id, snapshot_name)?;
            match row {
                Some(r) => r.local_artifact_id,
                None => {
                    return Err(CheckoutError::NamedNotFound {
                        sandbox_id: sandbox_id.to_string(),
                        snapshot_name: snapshot_name.clone(),
                    });
                }
            }
        };
        self.check_out_for_delete(id)
    }

    /// Crash recovery for rows stranded in a non-`Ready` state.
    /// Lease `Drop` rollback only runs on a live process; SIGKILL,
    /// OOM, or a node reboot skips it and leaves rows stuck in
    /// `restoring`/`deleting` forever (the reaper pins non-ready
    /// rows, so a stuck row is never reclaimed). Call once at
    /// startup, before any lease can be handed out.
    ///
    /// Decision per state:
    ///
    /// - `restoring` → reset to `ready`. A restore only reads the
    ///   artifact's bytes; whatever the crash interrupted, the disk
    ///   contents are intact.
    /// - `deleting` → decide by disk state. The delete sequence is
    ///   checkout → `rename(local_path, <local_path>.gc-pending-<id>)`
    ///   → row DELETE, so:
    ///     - original path present and no staging dir → crash was
    ///       before the rename; the bytes are intact → reset to
    ///       `ready`.
    ///     - staging dir present, or neither path present → the
    ///       rename already happened (or the dir was already gone
    ///       at rename time) → finish the delete by removing the
    ///       row. Resetting to `ready` here would advertise an
    ///       artifact whose `local_path` no longer exists — and the
    ///       row would dangle indefinitely if the reaper never
    ///       selects it again (disabled, or under budget). The
    ///       staging dir itself is the orphan sweep's job, after
    ///       this transaction confirms the artifact remains unpinned.
    ///     - historical pinned rows with missing or ambiguous paths
    ///       retain `deleting` and their evidence. Never invent Ready
    ///       from a pin: adoption can reuse an existing artifact ID.
    ///       Explicit restore or cleanup owns progress; this recovery
    ///       does not promise an automatic repair deadline.
    pub fn recover_stuck_states(&self) -> rusqlite::Result<StuckStateRecovery> {
        let conn = self.db_conn()?;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        let restoring_reset = tx.execute(
            "UPDATE local_artifacts SET state = 'ready' WHERE state = 'restoring'",
            [],
        )?;
        let stuck_deleting: Vec<(String, String)> = tx
            .prepare(
                "SELECT local_artifact_id, local_path FROM local_artifacts
                 WHERE state = 'deleting'",
            )?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut deleting_rolled_back = 0usize;
        let mut deleting_finished = 0usize;
        let mut deleting_deferred = 0usize;
        for (id, local_path) in stuck_deleting {
            let staging = format!("{local_path}.gc-pending-{id}");
            let (original_present, staging_present) = match (
                std::path::Path::new(&local_path).try_exists(),
                std::path::Path::new(&staging).try_exists(),
            ) {
                (Ok(original), Ok(staging)) => (original, staging),
                (original, staging) => {
                    tracing::warn!(artifact_id = %id, ?original, ?staging,
                        "retaining deleting artifact with unreadable recovery paths");
                    deleting_deferred += 1;
                    continue;
                }
            };
            let pinned: bool = tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sandboxes
                      WHERE current_local_artifact_id = ?1
                         OR suspend_predecessor_artifact_id = ?1
                 )",
                params![id],
                |r| r.get(0),
            )?;
            if original_present && !staging_present {
                tx.execute(
                    "UPDATE local_artifacts SET state = 'ready'
                     WHERE local_artifact_id = ?1 AND state = 'deleting'",
                    params![id],
                )?;
                deleting_rolled_back += 1;
            } else if pinned {
                tracing::warn!(artifact_id = %id, original_present, staging_present,
                    "retaining pinned deleting artifact evidence; explicit restore or cleanup required");
                deleting_deferred += 1;
            } else {
                tx.execute(
                    "DELETE FROM local_artifacts
                     WHERE local_artifact_id = ?1 AND state = 'deleting'",
                    params![id],
                )?;
                deleting_finished += 1;
            }
        }
        tx.commit()?;
        Ok(StuckStateRecovery {
            restoring_reset,
            deleting_rolled_back,
            deleting_finished,
            deleting_deferred,
        })
    }

    /// GC staging is disposable only after both its registry and pin are gone.
    /// Retain rows in every state, including historical deletion conflicts.
    pub(crate) fn gc_staging_is_unowned(&self, id: LocalArtifactId) -> rusqlite::Result<bool> {
        self.db_conn()?.query_row(
            sql!(
                "SELECT NOT EXISTS(SELECT 1 FROM local_artifacts WHERE local_artifact_id = ?1)
                  AND NOT EXISTS(SELECT 1 FROM sandboxes
                   WHERE current_local_artifact_id = ?1 OR suspend_predecessor_artifact_id = ?1)"
            ),
            [id.to_string()],
            |row| row.get(0),
        )
    }

    /// Examine at most `max_records` raw rows, advancing past owned, present,
    /// malformed, or unreadable entries. A fixed rowid ceiling makes each sweep
    /// finite under insertion churn; retained rows are reconsidered after wrap.
    /// Probes and guarded deletion retain the same immediate transaction.
    pub fn prune_missing_orphan_rows(&self, max_records: usize) -> rusqlite::Result<usize> {
        if max_records == 0 {
            return Ok(0);
        }

        let conn = self.db_conn()?;
        let mut cursor = self.orphan_scan.lock().map_err(|error| {
            rusqlite::Error::InvalidParameterName(format!("artifact scan mutex poisoned: {error}"))
        })?;
        let mut scan = *cursor;
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)?;
        if scan.through.is_none() {
            scan.through =
                tx.query_row("SELECT MAX(rowid) FROM local_artifacts", [], |r| r.get(0))?;
        }
        let Some(through) = scan.through else {
            return Ok(0);
        };
        let rows = tx
            .prepare(
                "SELECT la.rowid, la.local_artifact_id, la.local_path,
                    EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = la.sandbox_id)
               FROM local_artifacts la
              WHERE la.rowid >= ?1 AND la.rowid <= ?2
              ORDER BY la.rowid LIMIT ?3",
            )?
            .query_map(
                params![
                    scan.next,
                    through,
                    i64::try_from(max_records).unwrap_or(i64::MAX)
                ],
                |row| {
                    let decoded = (|| -> rusqlite::Result<(String, String, bool)> {
                        Ok((row.get(1)?, row.get(2)?, row.get(3)?))
                    })();
                    Ok((row.get::<_, i64>(0)?, decoded))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut deleted = 0usize;
        let examined = rows.len();
        for (rowid, decoded) in rows {
            scan.next = rowid.saturating_add(1);
            if rowid == through {
                scan = OrphanScan::default();
            }
            let (id, local_path, owned) = match decoded {
                Ok(row) => row,
                Err(error) => {
                    tracing::warn!(rowid, %error, "preserving malformed local artifact metadata");
                    continue;
                }
            };
            if owned {
                continue;
            }
            match Path::new(&local_path).try_exists() {
                Ok(false) => {}
                Ok(true) => continue,
                Err(error) => {
                    tracing::warn!(artifact_id = %id, %error, "preserving local artifact with unreadable path");
                    continue;
                }
            }
            deleted += tx.execute(
                "DELETE FROM local_artifacts
                  WHERE local_artifact_id = ?1
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandboxes s
                         WHERE s.id = local_artifacts.sandbox_id
                    )",
                params![id],
            )?;
        }
        if examined < max_records {
            scan = OrphanScan::default();
        }
        tx.commit()?;
        *cursor = scan;
        Ok(deleted)
    }

    fn db_conn(&self) -> rusqlite::Result<MutexGuard<'_, Connection>> {
        self.db.lock().map_err(|error| {
            rusqlite::Error::InvalidParameterName(format!("database mutex poisoned: {error}"))
        })
    }
}

/// Outcome of [`LocalArtifacts::recover_stuck_states`], for the
/// caller's startup log line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StuckStateRecovery {
    /// `restoring` rows reset to `ready`.
    pub restoring_reset: usize,
    /// `deleting` rows rolled back to `ready` (original bytes intact).
    pub deleting_rolled_back: usize,
    /// `deleting` rows whose delete was finished (row removed; bytes
    /// already renamed away or gone).
    pub deleting_finished: usize,
    /// Ambiguous, unreadable, or missing pinned evidence retained for repair.
    pub deleting_deferred: usize,
}

impl StuckStateRecovery {
    pub fn total(&self) -> usize {
        self.restoring_reset
            + self.deleting_rolled_back
            + self.deleting_finished
            + self.deleting_deferred
    }
}

/// Active checkout of an artifact in `Restoring` state. Holding this
/// lease prevents concurrent reaps and prevents another restore
/// from racing. The artifact's `local_path` is exposed via
/// [`Self::local_path`] for read-only use.
///
/// On drop without an explicit [`Self::release`], the lease attempts
/// a best-effort rollback to `Ready`. A failure there logs and
/// leaves the row in `Restoring` — the next restore attempt will
/// surface it as `NotReady` and an operator can recover manually.
pub struct RestoreLease {
    id: LocalArtifactId,
    local_path: String,
    db: Arc<Mutex<Connection>>,
    released: bool,
}

impl RestoreLease {
    pub fn id(&self) -> LocalArtifactId {
        self.id
    }

    pub fn local_path(&self) -> &str {
        &self.local_path
    }

    /// Explicitly release the lease, transitioning `Restoring →
    /// Ready` **and** bumping `last_touched_at` to now. The touch is
    /// part of the release semantics: a successful restore (or any
    /// other operation that held the lease) means the artifact has
    /// just been used, so the reaper's idle clock should reset from
    /// this point. Prefer this over relying on `Drop` so failures
    /// surface as `Result` instead of being swallowed, and so the
    /// idle-clock reset actually happens.
    pub fn release(mut self) -> rusqlite::Result<()> {
        let result = self.release_with_touch();
        self.released = true;
        result
    }

    fn release_with_touch(&self) -> rusqlite::Result<()> {
        let conn = lock_db(&self.db)?;
        conn.execute(
            "UPDATE local_artifacts
             SET state = 'ready', last_touched_at = ?1
             WHERE local_artifact_id = ?2 AND state = 'restoring'",
            params![crate::db::now()?, self.id.to_string()],
        )?;
        Ok(())
    }

    /// Drop-path rollback. Restores `state = 'ready'` without
    /// bumping `last_touched_at` — Drop is the abort path (e.g. an
    /// early `?` or panic before `release()`), and the caller may
    /// not have actually read the artifact's bytes. The reaper's
    /// idle clock should reflect that.
    fn release_without_touch(&self) -> rusqlite::Result<()> {
        let conn = lock_db(&self.db)?;
        conn.execute(
            "UPDATE local_artifacts SET state = 'ready'
             WHERE local_artifact_id = ?1 AND state = 'restoring'",
            params![self.id.to_string()],
        )?;
        Ok(())
    }
}

impl Drop for RestoreLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Err(error) = self.release_without_touch() {
            tracing::warn!(
                local_artifact_id = %self.id,
                %error,
                "RestoreLease drop: failed to roll back state to 'ready'; row may be stuck in 'restoring' until manual intervention",
            );
        }
    }
}

/// Active checkout of an artifact in `Deleting` state. The caller is
/// expected to perform any on-disk cleanup (rename, rmdir) and then
/// call [`Self::commit`] to remove the row. Dropping the lease
/// without commit best-effort rolls back the state to `Ready`.
pub struct DeleteLease {
    id: LocalArtifactId,
    local_path: String,
    db: Arc<Mutex<Connection>>,
    committed: bool,
}

impl DeleteLease {
    pub fn id(&self) -> LocalArtifactId {
        self.id
    }

    pub fn local_path(&self) -> &str {
        &self.local_path
    }

    /// Finalize the delete by removing the row. The CAS condition
    /// `AND state = 'deleting'` makes this idempotent: a second
    /// commit (e.g. after rollback already ran) is a no-op rather
    /// than a foot-gun.
    pub fn commit(mut self) -> rusqlite::Result<()> {
        let conn = lock_db(&self.db)?;
        let deleted = conn.execute(
            "DELETE FROM local_artifacts
             WHERE local_artifact_id = ?1 AND state = 'deleting'",
            params![self.id.to_string()],
        )?;
        drop(conn);
        if deleted == 0 {
            tracing::warn!(
                local_artifact_id = %self.id,
                "DeleteLease commit: row was not in 'deleting' state (concurrent rollback?); no row removed",
            );
        }
        self.committed = true;
        Ok(())
    }
}

impl Drop for DeleteLease {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let conn = match lock_db(&self.db) {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(
                    local_artifact_id = %self.id,
                    %error,
                    "DeleteLease drop: failed to lock database for rollback; row may be stuck in 'deleting' until manual intervention",
                );
                return;
            }
        };
        if let Err(error) = conn.execute(
            "UPDATE local_artifacts SET state = 'ready'
             WHERE local_artifact_id = ?1 AND state = 'deleting'",
            params![self.id.to_string()],
        ) {
            tracing::warn!(
                local_artifact_id = %self.id,
                %error,
                "DeleteLease drop: failed to roll back state to 'ready'; row may be stuck in 'deleting' until manual intervention",
            );
        }
    }
}

fn lock_db(db: &Arc<Mutex<Connection>>) -> rusqlite::Result<MutexGuard<'_, Connection>> {
    db.lock().map_err(|error| {
        rusqlite::Error::InvalidParameterName(format!("database mutex poisoned: {error}"))
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod refusal_tests;
