//! Sandbox-manager persistence and guarded lifecycle transitions.
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

use pplx_space_common_core::types::JobId;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub use pplx_space_common_core::types::PolicyAuthority;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, AutoLifecycleRuntimeState,
    ExecutionGeneration, ExecutionMetadata, LocalArtifactId, NodeManagedBy, ORPHANED_SANDBOX_OWNER,
    ORPHANED_SANDBOX_TEMPLATE, RuntimeKind, SandboxActivationAttemptId, SandboxCleanupEpisodeId,
    SandboxId, SandboxOwner, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource, SandboxRuntimeLabel,
    SandboxTransitionReceipt, SandboxTransitionReceiptInvariantError, SandboxTransitionReceiptKind,
    SandboxTransitionTargetState, SnapshotName,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
#[cfg(test)]
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::{NetworkPolicy, proto::NetworkPolicySource};
use pplx_space_sqlite::SqliteU64;
use rusqlite::types::Type;

use pplx_space_common_core::sql;
use rusqlite::{OptionalExtension as _, params};

mod handle;
pub use handle::OptionalExtension;
use handle::db_private_outside_tests;
pub use handle::{Connection, ConnectionGuard, Error, Result, SharedConnection, Transaction};

pub mod auto_lifecycle_delete_receipts;
pub mod cleanup_jobs;
pub mod cleanup_target_scan;
pub mod eviction_scan;
pub mod guest_health_scan;
pub mod inventory;
pub mod inventory_read;
pub mod local_artifact_leases;
pub mod migration;
pub mod quarantine_history;
pub mod route_activation_claims;
pub mod snapshot_gc;
pub mod snapshot_upload_jobs;
pub mod timestamps;
pub mod transfer;

use crate::endpoint_proof::SpacedEndpointProof;
use crate::restore_operation::{RemoteRestoreOperation, RestoreOrigin};
use crate::state::{SandboxState, StateTransitionError};
use pplx_space_common_core::types::{PodUid, RouteGeneration, SpaceApiRouteIdentity};

const DB_PATH: &str = pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_DB;

mod quarantine_scan;
mod sandbox_row;
mod teardown_jobs;
use auto_lifecycle_delete_receipts::discard_pending_receipt_for_execution_advance;
pub use teardown_jobs::{
    TeardownSuspendFailure, defer_teardown_suspend_retry, fail_teardown_suspend_backup_operation,
    fail_teardown_suspend_operation, reconcile_terminal_teardown_suspend_jobs,
};
#[cfg(not(any(test, feature = "test-support")))]
use teardown_jobs::{teardown_suspend_retry_admissible, teardown_suspend_retry_due};
#[cfg(any(test, feature = "test-support"))]
pub use teardown_jobs::{teardown_suspend_retry_admissible, teardown_suspend_retry_due};
#[cfg(any(test, feature = "test-support"))]
mod test_schema_image_cache;
pub use quarantine_scan::PeriodicQuarantineRecoveryScan;
pub use sandbox_row::SandboxListRow;
pub use sandbox_row::SandboxRow;
use sandbox_row::{row_to_list_sandbox, row_to_sandbox};

mod auto_lifecycle;
mod cleanup_episodes;
mod eviction;
mod local_artifacts;
mod network_policy;
mod pool;
mod quarantine;
mod quarantine_recovery;
mod restore_transfer_scopes;
mod runtime_upgrades;
mod sandboxes;
mod startup_recovery;
mod state_transitions;
mod state_write;
mod teardown_suspend;
pub use auto_lifecycle::*;
pub use cleanup_episodes::*;
pub use eviction::*;
pub use local_artifacts::*;
pub use network_policy::*;
pub use pool::*;
pub use quarantine::*;
pub use quarantine_recovery::*;
pub use restore_transfer_scopes::*;
pub use runtime_upgrades::*;
pub use sandboxes::*;
pub use startup_recovery::*;
pub use state_transitions::*;
pub use state_write::*;
pub use teardown_suspend::*;

// Most of the no-progress vocabulary is durable: its rendered form is persisted
// into `sandbox_quarantine_state.last_error`. Pre-claim reasons and a detached
// task failure are metric-only, but share the same bounded classification so one
// pass does not invent a second reason type. It therefore belongs in common-core
// rather than in the metrics crate whose tag it also happens to be. Re-exported here so
// `crate::db::QuarantineRecoveryNoProgressReason` stays the name callers use.
pub use pplx_space_common_core::types::QuarantineRecoveryNoProgressReason;

/// Typed reads: one sandbox's whole row, the narrow views that keep working
/// without one, a State RPC batch, one local artifact, one cleanup job and
/// the node-wide counts. Each keeps the decoder, missing-row rule and
/// tolerance of the reader it consolidates.
pub mod read {
    mod cleanup_job;
    mod counts;
    mod local_artifact;
    mod runtime_upgrade;
    mod sandbox;
    mod sandbox_view;
    mod state_page;
    pub use cleanup_job::*;
    pub use counts::*;
    pub use local_artifact::*;
    pub use runtime_upgrade::*;
    pub use sandbox::*;
    pub use sandbox_view::*;
    pub use state_page::*;

    #[cfg(test)]
    mod tests;
}

/// Named scans over many rows. Each keeps its own pinning, filter, cap,
/// budget and policy for rows that do not decode.
pub mod scan {
    mod all_sandboxes;
    mod by_peer;
    mod due_auto_lifecycle;
    mod eviction_candidates;
    mod list_page;
    mod local_artifacts;
    mod node_dirty_batch;
    mod pending_backups;
    mod pool_donor;
    mod quarantine_recovery;
    mod resolve_selector;
    mod rowless_restore_scopes;
    mod sandbox_pages;
    mod teardown_owned_suspending;
    mod unbound_executions;
    mod unsettled_runtime_upgrades;
    pub use all_sandboxes::*;
    pub use by_peer::*;
    pub use due_auto_lifecycle::*;
    pub use eviction_candidates::*;
    pub use list_page::*;
    pub use local_artifacts::*;
    pub use node_dirty_batch::*;
    pub use pending_backups::*;
    pub use pool_donor::*;
    pub use quarantine_recovery::*;
    pub use resolve_selector::*;
    pub use rowless_restore_scopes::*;
    pub use sandbox_pages::*;
    pub use teardown_owned_suspending::*;
    pub use unbound_executions::*;
    pub use unsettled_runtime_upgrades::*;

    #[cfg(test)]
    mod tests;
}

const EXECUTION_DELETE_RECEIPT_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

fn parse_policy_authority(value: &str, column: usize) -> rusqlite::Result<PolicyAuthority> {
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

impl fmt::Display for CleanupCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CleanupCompletion {
    type Err = CleanupCompletionParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "mark_created" => Ok(Self::MarkCreated),
            "mark_stopped" => Ok(Self::MarkStopped),
            "mark_suspended" => Ok(Self::MarkSuspended),
            "mark_failed" => Ok(Self::Quarantine),
            "delete_row" => Ok(Self::DeleteRow),
            "delete_pool_clone" => Ok(Self::DeletePoolClone),
            _ => Err(CleanupCompletionParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error(
    "cleanup completion must be mark_created, mark_stopped, mark_suspended, mark_failed, delete_row, or delete_pool_clone"
)]
pub struct CleanupCompletionParseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupCompletion {
    MarkCreated,
    MarkStopped,
    MarkSuspended,
    Quarantine,
    DeleteRow,
    DeletePoolClone,
}

impl CleanupCompletion {
    fn as_str(self) -> &'static str {
        match self {
            Self::MarkCreated => "mark_created",
            Self::MarkStopped => "mark_stopped",
            Self::MarkSuspended => "mark_suspended",
            // This spelling is enforced by the deployed SQLite CHECK constraint.
            Self::Quarantine => "mark_failed",
            Self::DeleteRow => "delete_row",
            Self::DeletePoolClone => "delete_pool_clone",
        }
    }

    pub const fn deletes_row(self) -> bool {
        matches!(self, Self::DeleteRow | Self::DeletePoolClone)
    }

    const fn target_state(self) -> Option<SandboxState> {
        match self {
            Self::MarkCreated => Some(SandboxState::Created),
            Self::MarkStopped => Some(SandboxState::Stopped),
            Self::MarkSuspended => Some(SandboxState::Suspended),
            Self::Quarantine => Some(SandboxState::Quarantined),
            Self::DeleteRow | Self::DeletePoolClone => None,
        }
    }

    const fn transition_target(self) -> Option<SandboxTransitionTargetState> {
        match self {
            Self::MarkCreated => Some(SandboxTransitionTargetState::Created),
            Self::MarkStopped => Some(SandboxTransitionTargetState::Stopped),
            Self::MarkSuspended => Some(SandboxTransitionTargetState::Suspended),
            Self::Quarantine | Self::DeleteRow | Self::DeletePoolClone => None,
        }
    }

    pub fn owns_row_intent(self, row: &SandboxRow) -> bool {
        self.owns_intent(row.state, row.cleanup_completion)
    }

    fn owns_intent(self, state: SandboxState, intent: Option<Self>) -> bool {
        let persisted_intent = matches!(
            state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) && intent == Some(self);
        match self {
            Self::MarkCreated => persisted_intent,
            Self::MarkStopped => state == SandboxState::Stopping || persisted_intent,
            Self::MarkSuspended => state == SandboxState::Suspending || persisted_intent,
            Self::Quarantine => {
                persisted_intent || (state == SandboxState::CleanupRequired && intent.is_none())
            }
            Self::DeleteRow | Self::DeletePoolClone => {
                state == SandboxState::Deleting || persisted_intent
            }
        }
    }
}

/// Prepared statements kept per connection. Hot-path reads and writes use
/// `prepare_cached`; rusqlite's default of 16 lets the statements of
/// background loops evict them.
const STATEMENT_CACHE_CAPACITY: usize = 128;

/// Opening or migrating the database failed. `Display` and `source` are the
/// shared SQLite helpers' error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct OpenError(#[from] pplx_space_sqlite::SqliteError);

/// Apply the service and common schema migrations to the database.
pub async fn migrate() -> Result<(), OpenError> {
    migrate_at(Path::new(DB_PATH)).await
}

/// [`migrate`] for the database at `db_path`.
pub async fn migrate_at(db_path: &Path) -> Result<(), OpenError> {
    use pplx_space_sbxmgr_migrations::COMMON_SCHEMA_MIGRATOR;
    let target = migration::target();
    Ok(pplx_space_sqlite::migrate_embedded(
        target.name,
        db_path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: target.service,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
    )
    .await?)
}

/// Open the migrated database, with the connection settings every store
/// operation assumes.
pub fn open() -> Result<Connection, OpenError> {
    let conn = pplx_space_sqlite::open_rusqlite(
        Path::new(DB_PATH),
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    Ok(Connection::from_raw(conn))
}

pub fn describe_error(error: &Error) -> String {
    describe_raw_error(error.raw())
}

fn describe_raw_error(error: &rusqlite::Error) -> String {
    match error.sqlite_error() {
        Some(sqlite) => format!(
            "{error} (sqlite_code={:?}, sqlite_extended_code={})",
            sqlite.code, sqlite.extended_code
        ),
        None => error.to_string(),
    }
}

pub(crate) fn immediate_transaction(conn: &Connection) -> Result<Transaction<'_>> {
    Transaction::immediate(conn)
}

pub(crate) fn now() -> Result<String> {
    #[cfg(test)]
    if let Some(seconds) = test_clock::tick() {
        return Ok(seconds.to_string());
    }
    // Preserve whole epoch-second storage without fabricating persisted evidence
    // when the host clock is invalid.
    now_epoch_i64().map(|seconds| seconds.to_string())
}

/// A clock for tests that need two reads of [`now`] to land in different
/// seconds, which the wall clock only does by chance.
#[cfg(test)]
pub(crate) mod test_clock {
    use std::cell::Cell;

    thread_local! {
        static NEXT: Cell<Option<i64>> = const { Cell::new(None) };
    }

    /// Until the guard drops, every [`super::now`] on this thread returns
    /// `start`, then `start + 1`, and so on.
    pub(crate) fn ticking_from(start: i64) -> Guard {
        NEXT.with(|next| next.set(Some(start)));
        Guard
    }

    pub(crate) struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            NEXT.with(|next| next.set(None));
        }
    }

    pub(super) fn tick() -> Option<i64> {
        NEXT.with(|next| {
            let seconds = next.get()?;
            next.set(Some(seconds + 1));
            Some(seconds)
        })
    }
}

pub fn now_epoch_i64() -> Result<i64> {
    persisted_epoch_seconds(jiff::Timestamp::now())
}

#[derive(Debug, thiserror::Error)]
#[error("cannot persist a timestamp before the Unix epoch")]
struct PersistedClockBeforeEpoch;

fn persisted_epoch_seconds(timestamp: jiff::Timestamp) -> Result<i64> {
    if timestamp < jiff::Timestamp::UNIX_EPOCH {
        return Err(Error::to_sql_conversion(PersistedClockBeforeEpoch));
    }
    Ok(timestamp.as_second())
}

fn u64_to_sql_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn emit_sandbox_state_event(
    id: &str,
    event_type: SandboxEventType,
    from_state: Option<&str>,
    reason: &str,
    operation: &'static str,
    to_state: SandboxState,
    error: Option<&str>,
) {
    let mut details = SandboxEventDetails::default()
        .reason(reason)
        .operation(operation)
        .trigger(SandboxEventTrigger::System)
        .with_to_state(to_state.as_str());
    if let Some(from_state) = from_state {
        details = details.with_from_state(from_state);
    }
    if let Some(error) = error {
        details = details.error(error);
    }
    emit_sandbox_event(id, event_type, SandboxEventSource::SbxManager, details);
}

/// The sandbox entered `quarantined`: its `sandbox_quarantined` event and
/// the entry counter, emitted together after the transition commits.
fn emit_sandbox_quarantined_event(
    id: &str,
    from_state: Option<&str>,
    reason_code: SandboxQuarantineReasonCode,
    operation: &'static str,
    error: Option<&str>,
) {
    emit_sandbox_state_event(
        id,
        SandboxEventType::SandboxQuarantined,
        from_state,
        reason_code.as_str(),
        operation,
        SandboxState::Quarantined,
        error,
    );
    pplx_space_metrics::sandbox::worker::quarantine_entered(reason_code, operation);
}

fn emit_auto_lifecycle_delete_receipt_superseded(
    sandbox_id: SandboxId,
    superseded_receipt: crate::db::auto_lifecycle_delete_receipts::SupersededPendingReceipt,
    current_generation: ExecutionGeneration,
) {
    emit_sandbox_event(
        &sandbox_id.to_string(),
        SandboxEventType::DeleteSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation("delete")
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .receipt_id(superseded_receipt.receipt_id.to_string())
            .execution_generation(superseded_receipt.execution_generation.to_string())
            .reason(format!(
                "pending delete receipt superseded by execution generation {current_generation}"
            )),
    );
}

pub fn emit_auto_lifecycle_delete_receipt_superseded_if_present(
    sandbox_id: SandboxId,
    supersession: Option<crate::db::auto_lifecycle_delete_receipts::PendingReceiptSupersession>,
) {
    if let Some(supersession) = supersession {
        emit_auto_lifecycle_delete_receipt_superseded(
            sandbox_id,
            supersession.receipt,
            supersession.superseding_generation,
        );
    }
}

/// Delete every `sandbox_id`-scoped child row of a sandbox inside the
/// caller's open transaction. This manual cascade is the single source of
/// truth for sandbox child tables: add new child tables here, not at the
/// individual delete call sites. Foreign keys are enforced (the bundled SQLite
/// build defaults `foreign_keys` on and the store never turns it off), but only
/// some child tables declare `ON DELETE CASCADE`, so the cascade alone is not
/// the cleanup. It deliberately skips `network_policy_reconcile_queue`, which
/// nothing writes or reads any more and a later release drops.
fn delete_sandbox_child_rows(tx: &Connection, id: &str) -> Result<()> {
    teardown_jobs::fail_active(tx, id, TeardownSuspendFailure::Deleted)?;
    crate::db::route_activation_claims::close_claim(tx, id)?;
    tx.execute(
        "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM local_artifacts WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_quarantine_history WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM sandbox_runtime_upgrades WHERE sandbox_id = ?1",
        params![id],
    )?;
    Ok(())
}

fn generic_writer_transition_blocked(
    previous_state: Option<SandboxState>,
    target_state: SandboxState,
) -> bool {
    previous_state
        .filter(|state| *state == SandboxState::Quarantined)
        .is_some_and(|state| state.transition(target_state).is_err())
}

fn record_execution_delete_receipt(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: Option<ExecutionGeneration>,
) -> Result<()> {
    let Some(execution_generation) = execution_generation else {
        return Ok(());
    };
    let deleted_at = now_epoch_i64()?;
    conn.execute(
        "DELETE FROM sandbox_execution_delete_receipts WHERE deleted_at < ?1",
        params![deleted_at.saturating_sub(EXECUTION_DELETE_RECEIPT_RETENTION_SECS)],
    )?;
    conn.execute(
        "INSERT INTO sandbox_execution_delete_receipts (
             sandbox_id, execution_generation, deleted_at
         ) VALUES (?1, ?2, ?3)
         ON CONFLICT(sandbox_id) DO UPDATE SET
             execution_generation = MAX(execution_generation, excluded.execution_generation),
             deleted_at = MAX(deleted_at, excluded.deleted_at)",
        params![sandbox_id, execution_generation.get(), deleted_at],
    )?;
    Ok(())
}

db_private_outside_tests! {
    fn execution_delete_receipt_covers(
        conn: &Connection,
        sandbox_id: &str,
        execution_generation: ExecutionGeneration,
    ) -> Result<bool> {
        let now = now_epoch_i64()?;
        read::sandbox_view(conn, sandbox_id)
            .delete_receipts()
            .execution_covers(execution_generation, now)
    }
}

/// Only use at read/decode boundaries. Statement, schema and write failures
/// remain fatal; a retained malformed row must not disable independent work.
fn is_row_decode_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)
            | rusqlite::Error::Utf8Error(..)
    )
}

#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub fn create_test_schema(conn: &mut Connection) {
    use pplx_space_sbxmgr_migrations::{COMMON_SCHEMA_MIGRATOR, SANDBOX_MANAGER_MIGRATOR};
    for migration in COMMON_SCHEMA_MIGRATOR
        .iter()
        .chain(SANDBOX_MANAGER_MIGRATOR.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

/// Byte image of a freshly migrated database, built once per nextest run of
/// this test binary (once per process outside nextest).
///
/// Every migration statement runs in its own autocommit transaction, so
/// replaying the set against a file-backed database journals and fsyncs
/// hundreds of times per test case. `sqlite3_serialize` yields exactly the
/// bytes SQLite would have written for that same schema, so a case can
/// materialise a real database file from it and keep opening it the normal way.
///
/// nextest runs each case in its own process, so a per-process cache alone
/// would still replay every migration once per case. The schema is a function
/// of this executable — the migrations are embedded in it and SQLite is linked
/// into it — so the first process of a run to build the image leaves a copy
/// beside the executable, and later processes of the same run and build read
/// that copy (see the `test_schema_image_cache` module).
#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
fn test_schema_image() -> &'static [u8] {
    static IMAGE: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    IMAGE.get_or_init(|| {
        let cache = test_schema_image_cache::SchemaImageCache::for_current_exe();
        if let Some(image) = cache.as_ref().and_then(|cache| cache.read()) {
            return image;
        }
        let mut conn = Connection::from_raw(rusqlite::Connection::open_in_memory().unwrap());
        create_test_schema(&mut conn);
        let image = conn.raw().serialize(rusqlite::MAIN_DB).unwrap().to_vec();
        if let Some(cache) = &cache {
            cache.write(&image);
        }
        image
    })
}

/// Create a file-backed database carrying the migrated schema at `db_path`.
///
/// For tests that need durability across a close and reopen: the file left
/// behind is the one `create_test_schema` would have produced on that path, so
/// the connections the test then opens are ordinary file connections.
#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub fn create_test_schema_on_disk(db_path: &std::path::Path) {
    std::fs::write(db_path, test_schema_image()).unwrap();
}

/// Fresh SQLite database using the embedded service and common migrations.
/// Upgrade/history-stamping behavior is covered by the migrations crate.
///
/// The schema arrives as a copy of the process-wide image rather than a fresh
/// replay of every migration, which is what makes a test that opens dozens of
/// connections affordable. Nothing else is shared: each call owns its pages,
/// and the per-connection settings are the ones `Connection::open_in_memory`
/// installs, exactly as a replay would leave them.
#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub fn test_conn() -> Connection {
    let image = test_schema_image();
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.deserialize_read_exact(rusqlite::MAIN_DB, image, image.len(), false)
        .unwrap();
    Connection::from_raw(conn)
}

/// Seed pre-validation lifecycle evidence without weakening subsequent writes.
/// Transactional DDL also restores the guards if the seed closure panics.
///
/// The validators are read back from this connection rather than reconstructed
/// from a pinned migration, so a later migration that replaces them is restored
/// as it actually exists. A rename is a loud failure here, not a silent
/// reinstatement of a superseded vocabulary in one test's database.
#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub fn seed_historical_lifecycle_rows<T>(
    conn: &Connection,
    seed: impl FnOnce(&Connection) -> T,
) -> T {
    let tx = Transaction::deferred(conn).unwrap();
    let validators: Vec<(String, String)> = tx
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'trigger'
               AND tbl_name = 'sandboxes'
               AND name LIKE 'validate_sandbox_lifecycle%'
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(
        !validators.is_empty(),
        "no lifecycle validators to suspend; a renamed validator must update this helper"
    );
    for (name, _) in &validators {
        tx.execute_batch(&format!("DROP TRIGGER {name}")).unwrap();
    }
    let result = seed(&tx);
    for (_, sql) in &validators {
        tx.execute_batch(sql).unwrap();
    }
    tx.commit().unwrap();
    result
}

/// What tests outside the store need for raw SQL fixtures and tripwires,
/// besides the statement methods of [`Connection`].
#[cfg(any(test, feature = "test-support"))]
#[allow(clippy::unwrap_used)]
pub mod fixture {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use pplx_space_common_core::types::{
        AutoLifecyclePolicy, SandboxId, SandboxOwner, SandboxRuntimeLabel,
    };

    use crate::state::SandboxState;

    pub use rusqlite::trace::{TraceEvent, TraceEventCodes};
    pub use rusqlite::types::{Type, Value};
    pub use rusqlite::{StatementStatus, params};

    pub fn insert_test_sandbox(conn: &crate::db::Connection, id: &str, state: SandboxState) {
        insert_test_sandbox_with_policy(conn, id, state, &AutoLifecyclePolicy::disabled());
    }

    pub fn insert_test_sandbox_with_policy(
        conn: &crate::db::Connection,
        id: &str,
        state: SandboxState,
        auto_lifecycle: &AutoLifecyclePolicy,
    ) {
        let sandbox_id = SandboxId::from_str(id).unwrap();
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
        let user = SandboxOwner::parse("user").unwrap();
        crate::db::insert_sandbox(
            conn,
            crate::db::InsertSandboxParams {
                guest_image: Default::default(),
                id,
                uuid8: &uuid8,
                state,
                template: "seed",
                template_hash: Some("hash"),
                user: &user,
                runtime: "fc",
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: None,
                volume_encryption: false,
                process_env: &BTreeMap::new(),
                auto_lifecycle,
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
    }
}

#[cfg(any(test, feature = "test-support"))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
pub mod test_fixtures;
#[cfg(any(test, feature = "test-support"))]
pub use test_fixtures::{
    begin_activation, complete_cleanup_episode, quarantine_cleanup_episode, update_runtime_fields,
};

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod dirty_outbox_tests;

#[cfg(test)]
mod cleanup_discovery_tests;

#[cfg(test)]
mod discovery_indexes_tests;

#[cfg(test)]
mod lifecycle_validation_tests;
