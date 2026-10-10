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
use pplx_space_common_svc::sql::sqlite::SqliteU64;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
    emit_sandbox_event,
};
#[cfg(test)]
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::{NetworkPolicy, proto::NetworkPolicySource};
use rusqlite::types::Type;

use pplx_space_common_core::sql;
use rusqlite::{Connection, OptionalExtension, Result, params};

pub(crate) mod cleanup_jobs;
pub(crate) mod cleanup_target_scan;
pub(crate) mod inventory;
pub(crate) mod inventory_read;
pub(crate) mod quarantine_history;
pub(crate) mod snapshot_gc;
pub(crate) mod timestamps;
pub(crate) mod transfer;

use crate::endpoint_proof::SpacedEndpointProof;
use crate::restore_operation::{RemoteRestoreOperation, RestoreOrigin};
use crate::state::{SandboxState, StateTransitionError};
use pplx_space_common_core::types::{PodUid, RouteGeneration, SpaceApiRouteIdentity};

const DB_PATH: &str = pplx_space_common_core::host::linux::paths::SANDBOX_MANAGER_DB;

mod quarantine_scan;
mod sandbox_row;
mod teardown_jobs;
pub(crate) use teardown_jobs::{
    TeardownSuspendFailure, fail_teardown_suspend_backup_operation,
    fail_teardown_suspend_operation, reconcile_terminal_teardown_suspend_jobs,
    teardown_suspend_retry_admissible,
};
#[cfg(test)]
mod test_schema_image_cache;
pub(crate) use quarantine_scan::PeriodicQuarantineRecoveryScan;
pub(crate) use sandbox_row::SandboxListRow;
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
pub use teardown_suspend::*;

pub(crate) struct ExecutionReleaseState {
    pub(crate) state_label: String,
    pub(crate) runtime_label: SandboxRuntimeLabel,
    pub(crate) failure_reason: Option<String>,
}

/// Minimal lifecycle evidence that remains readable when optional metadata or
/// a newer lifecycle label cannot be decoded by this binary.
#[derive(Debug)]
pub(crate) struct RawSandboxLifecycle {
    pub state: String,
    pub state_changed_at: String,
}

pub(crate) fn raw_sandbox_lifecycle(
    conn: &Connection,
    id: &str,
) -> Result<Option<RawSandboxLifecycle>> {
    conn.query_row(
        "SELECT state, state_changed_at FROM sandboxes WHERE id = ?1",
        [id],
        |row| {
            Ok(RawSandboxLifecycle {
                state: row.get(0)?,
                state_changed_at: timestamps::diagnostic_column(row, 1)?,
            })
        },
    )
    .optional()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SandboxLifecycleCount {
    pub state: SandboxState,
    pub count: u64,
}

// Most of the no-progress vocabulary is durable: its rendered form is persisted
// into `sandbox_quarantine_state.last_error`. Pre-claim reasons and a detached
// task failure are metric-only, but share the same bounded classification so one
// pass does not invent a second reason type. It therefore belongs in common-core
// rather than in the metrics crate whose tag it also happens to be. Re-exported here so
// `crate::db::QuarantineRecoveryNoProgressReason` stays the name callers use.
pub use pplx_space_common_core::types::QuarantineRecoveryNoProgressReason;

pub struct RuntimeFields<'a> {
    pub uid: u32,
    pub slot: u32,
    pub guest_cid: u32,
    pub pid: u32,
    pub spaced_host_pid: u32,
    pub netgtw_pid: u32,
    pub vsock_uds_path: &'a str,
    pub api_socket_path: &'a str,
    pub space_api_route_identity: SpaceApiRouteIdentity,
    pub route_identity_provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub spaced_endpoint_proof: Option<&'a SpacedEndpointProof>,
}

const EXECUTION_DELETE_RECEIPT_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

pub struct InsertSandboxParams<'a> {
    pub id: &'a str,
    pub uuid8: &'a SandboxRuntimeLabel,
    pub state: SandboxState,
    pub template: &'a str,
    pub template_hash: Option<&'a str>,
    pub user: &'a SandboxOwner,
    pub runtime: &'a str,
    pub snapshots_enabled: bool,
    pub rootfs_size_mb: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub pool_template: Option<&'a str>,
    pub volume_encryption: bool,
    pub process_env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicy>,
    pub network_policy_generation: u64,
    pub network_policy_authority: PolicyAuthority,
    pub auto_lifecycle_authority: PolicyAuthority,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreAdoptOrphanInsertError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("restore sandbox row state must be restoring, got {state}")]
    NonRestoringState { state: SandboxState },
    #[error("existing sandbox row for id {id} is not a recoverable synthetic orphan")]
    ExistingRowNotSyntheticOrphan { id: String },
}

fn parse_policy_authority(value: &str, column: usize) -> Result<PolicyAuthority> {
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

#[derive(Debug, thiserror::Error)]
pub enum InsertRecoveredSuspendedSandboxError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("recovered suspended sandbox insert must use suspended state, got {state}")]
    NonSuspendedInsert { state: SandboxState },
    #[error("recovered suspended sandbox artifact pin updated {changed} rows instead of one")]
    ArtifactPinInvariant { changed: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationKind {
    ColdStart,
    Restore,
}

impl ActivationKind {
    pub const fn activation_state(self) -> SandboxState {
        match self {
            Self::ColdStart => SandboxState::Starting,
            Self::Restore => SandboxState::Restoring,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BeginActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} cannot begin a {kind:?} activation")]
    UnsupportedState {
        kind: ActivationKind,
        state: SandboxState,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RollbackActivationEpisodeError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} is not an activation state")]
    UnsupportedActivationState { state: SandboxState },
    #[error("an activation episode cannot roll back to quarantined")]
    QuarantinedTarget,
}

#[derive(Debug, thiserror::Error)]
pub enum SupersedeActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox quarantine recovery is already in progress")]
    QuarantineRecoveryInProgress,
    #[error(transparent)]
    StateWriteNotApplied(#[from] SandboxStateWriteNotApplied),
}

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
    pub fn as_str(self) -> &'static str {
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

    pub const fn target_state(self) -> Option<SandboxState> {
        match self {
            Self::MarkCreated => Some(SandboxState::Created),
            Self::MarkStopped => Some(SandboxState::Stopped),
            Self::MarkSuspended => Some(SandboxState::Suspended),
            Self::Quarantine => Some(SandboxState::Quarantined),
            Self::DeleteRow | Self::DeletePoolClone => None,
        }
    }

    pub const fn transition_target(self) -> Option<SandboxTransitionTargetState> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "callers must distinguish an applied state write from a superseded one"]
pub enum SandboxStateWriteOutcome {
    Applied,
    Missing,
    BlockedByQuarantine,
    SupersededByCleanupEpisode,
}

impl SandboxStateWriteOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Missing => "missing",
            Self::BlockedByQuarantine => "blocked_by_quarantine",
            Self::SupersededByCleanupEpisode => "superseded_by_cleanup_episode",
        }
    }

    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub fn require_applied(self) -> std::result::Result<(), SandboxStateWriteNotApplied> {
        if self.is_applied() {
            Ok(())
        } else {
            Err(SandboxStateWriteNotApplied { outcome: self })
        }
    }
}

impl fmt::Display for SandboxStateWriteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sandbox state write was not applied: {outcome}")]
pub struct SandboxStateWriteNotApplied {
    pub outcome: SandboxStateWriteOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupRecoveryOutcome {
    State(SandboxState),
    CleanupRequired {
        keep_filesystem: bool,
        completion: CleanupCompletion,
    },
    Deferred,
}

impl StartupRecoveryOutcome {
    pub fn changed(self) -> bool {
        !matches!(self, Self::Deferred)
    }

    pub fn requires_quarantine(self) -> bool {
        matches!(self, Self::State(SandboxState::Quarantined))
            || matches!(
                self,
                Self::CleanupRequired {
                    completion: CleanupCompletion::Quarantine,
                    ..
                }
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupRecoveryRecord {
    pub id: String,
    pub previous_state: SandboxState,
    pub outcome: StartupRecoveryOutcome,
}

/// Prepared statements kept per connection. Hot-path reads and writes use
/// `prepare_cached`; rusqlite's default of 16 lets the statements of
/// background loops evict them.
const STATEMENT_CACHE_CAPACITY: usize = 128;

pub fn open() -> Result<Connection, pplx_space_common_svc::sql::sqlite::SqliteError> {
    let conn = pplx_space_common_svc::sql::sqlite::open_rusqlite(
        Path::new(DB_PATH),
        pplx_space_common_svc::sql::sqlite::RusqliteOpenOptions::default(),
    )?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    Ok(conn)
}

/// Count every durable sandbox-manager row by its bounded lifecycle. This stays
/// in SQL so the sampler's memory use is independent of the number of sandboxes
/// on the node.
pub(crate) fn sandbox_lifecycle_counts(conn: &Connection) -> Result<Vec<SandboxLifecycleCount>> {
    let mut statement = conn.prepare(
        "SELECT state, COUNT(*)
           FROM sandboxes
       GROUP BY state
       ORDER BY state",
    )?;
    let rows = statement.query_map([], |row| {
        let state_label: String = row.get(0)?;
        let state = state_label.parse::<SandboxState>().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                format!("unknown sandbox state '{state_label}'").into(),
            )
        })?;
        let count = row.get::<_, SqliteU64>(1)?.0;
        Ok(SandboxLifecycleCount { state, count })
    })?;
    rows.collect()
}

pub(crate) fn describe_error(error: &rusqlite::Error) -> String {
    match error.sqlite_error() {
        Some(sqlite) => format!(
            "{error} (sqlite_code={:?}, sqlite_extended_code={})",
            sqlite.code, sqlite.extended_code
        ),
        None => error.to_string(),
    }
}

pub(crate) fn immediate_transaction(conn: &Connection) -> Result<rusqlite::Transaction<'_>> {
    rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
}

pub(crate) fn now() -> Result<String> {
    // Preserve whole epoch-second storage without fabricating persisted evidence
    // when the host clock is invalid.
    now_epoch_i64().map(|seconds| seconds.to_string())
}

pub(crate) fn now_epoch_i64() -> Result<i64> {
    persisted_epoch_seconds(jiff::Timestamp::now())
}

#[derive(Debug, thiserror::Error)]
#[error("cannot persist a timestamp before the Unix epoch")]
struct PersistedClockBeforeEpoch;

fn persisted_epoch_seconds(timestamp: jiff::Timestamp) -> Result<i64> {
    if timestamp < jiff::Timestamp::UNIX_EPOCH {
        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            PersistedClockBeforeEpoch,
        )));
    }
    Ok(timestamp.as_second())
}

fn u64_to_sql_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

pub(crate) fn current_state_label(conn: &Connection, id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state FROM sandboxes WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .optional()
}

pub(crate) fn execution_release_state(
    conn: &Connection,
    id: &str,
) -> Result<Option<ExecutionReleaseState>> {
    conn.query_row(
        "SELECT state, uuid8, failure_reason FROM sandboxes WHERE id = ?1",
        params![id],
        |row| {
            let runtime_label_raw = row.get::<_, String>(1)?;
            let runtime_label = SandboxRuntimeLabel::parse(runtime_label_raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
            Ok(ExecutionReleaseState {
                state_label: row.get(0)?,
                runtime_label,
                failure_reason: row.get(2)?,
            })
        },
    )
    .optional()
}

pub fn record_remote_restore_artifact_staging(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_origin = 'remote',
             restore_phase = 'artifact_staging',
             updated_at = ?1
         WHERE id = ?2
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')",
        params![now()?, sandbox_id],
    )?;
    Ok(changed == 1)
}

pub fn replace_retryable_remote_restore_source(
    conn: &Connection,
    sandbox_id: &str,
    expected_source: &str,
    selected_source: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_storage_uri = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')
           AND restore_storage_uri = ?4
           AND uid IS NULL
           AND slot IS NULL
           AND guest_cid IS NULL
           AND pid IS NULL
           AND spaced_host_pid IS NULL
           AND netgtw_pid IS NULL
           AND vsock_uds_path IS NULL
           AND api_socket_path IS NULL
           AND current_local_artifact_id IS NULL
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL",
        params![selected_source, now()?, sandbox_id, expected_source],
    )?;
    Ok(changed == 1)
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

fn initial_auto_lifecycle_state(
    policy: &AutoLifecyclePolicy,
    state: SandboxState,
) -> AutoLifecycleRuntimeState {
    if state == SandboxState::Started {
        return crate::auto_lifecycle::initial_runtime_state(
            policy,
            crate::auto_lifecycle::now_secs(),
        );
    }
    crate::auto_lifecycle::initial_disarmed_runtime_state(policy)
}

pub fn insert_sandbox(conn: &Connection, params: InsertSandboxParams<'_>) -> Result<()> {
    let InsertSandboxParams {
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?21, ?22, ?23, ?24, 1)",
        params![
            id,
            uuid8.as_str(),
            state.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            volume_encryption,
            ts,
            ts,
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // Staging can fail before the first activation. Give its cleanup
            // a durable request identity, but not an allocated predecessor.
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;
    Ok(())
}

pub(crate) fn node_execution_fence(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<pplx_space_node_mgr_client::execution_fence::ExecutionFence>> {
    use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
    let (current, previous): (Option<String>, Option<String>) = conn.query_row(
        "SELECT node_execution_id, previous_node_execution_id FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let parse = |raw: String| {
        raw.parse::<SandboxActivationAttemptId>().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    };
    match current {
        Some(current) => Ok(Some(ExecutionFence::new(
            parse(current)?,
            previous.map(parse).transpose()?,
        ))),
        None if previous.is_none() => Ok(None),
        None => Err(rusqlite::Error::InvalidQuery),
    }
}

/// Bounded startup preflight; legacy NULL rows require the adoption baseline.
pub(crate) fn unbound_node_executions(conn: &Connection) -> Result<Vec<String>> {
    let mut statement = conn
        .prepare("SELECT id FROM sandboxes WHERE node_execution_id IS NULL ORDER BY id LIMIT ?1")?;
    statement
        .query_map(
            [pplx_space_node_mgr_client::execution_fence::EXECUTION_PREFLIGHT_LIMIT as i64],
            |row| row.get(0),
        )?
        .collect()
}

/// Import receiver authority only while inserting a previously absent orphan
/// row in this transaction. Never use inspection to re-authorize a rejected
/// cleanup of an existing row: the receiver may own a successor execution.
pub(crate) fn inherit_orphan_node_execution(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE sandboxes SET node_execution_id = ?2, previous_node_execution_id = ?3,
                              node_execution_retired = 0
         WHERE id = ?1 AND node_execution_retired = 1 AND previous_node_execution_id IS NULL",
        params![
            sandbox_id,
            fence.current().to_string(),
            fence.previous().map(|id| id.to_string())
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// A cleanup reply may acknowledge the predecessor when allocation never
/// reached the node. Never overwrite an activation that superseded this request.
pub(crate) fn acknowledge_node_cleanup(
    conn: &Connection,
    sandbox_id: &str,
    expected: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    retained: Option<SandboxActivationAttemptId>,
) -> Result<bool> {
    if retained.is_some_and(|id| !expected.owns(id)) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?4, previous_node_execution_id = NULL,
                              node_execution_retired = ?5
         WHERE id = ?1 AND node_execution_id = ?2 AND previous_node_execution_id IS ?3",
        params![
            sandbox_id,
            expected.current().to_string(),
            expected.previous().map(|id| id.to_string()),
            retained.unwrap_or(expected.current()).to_string(),
            retained.is_none(),
        ],
    )? == 1)
}

pub fn upsert_sandbox_execution_metadata(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_sandbox_execution_metadata_in_transaction(&tx, sandbox_id, execution)?;
    tx.commit()?;
    Ok(())
}

fn upsert_sandbox_execution_metadata_in_transaction(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let runtime: String = conn.query_row(
        "SELECT runtime FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |row| row.get(0),
    )?;
    let runtime_matches = runtime
        .parse::<RuntimeKind>()
        .is_ok_and(|runtime| runtime == execution.runtime().kind);
    if !runtime_matches {
        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "sandbox runtime column disagrees with execution metadata",
            ),
        )));
    }

    let execution_json = serde_json::to_string(execution)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET execution_metadata_json = ?2,
             updated_at = ?3
         WHERE id = ?1",
        params![sandbox_id, execution_json, now()?],
    )?;
    Ok(())
}

pub(crate) fn insert_recovered_suspended_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    execution: &ExecutionMetadata,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> std::result::Result<(), InsertRecoveredSuspendedSandboxError> {
    if params.state != SandboxState::Suspended {
        return Err(InsertRecoveredSuspendedSandboxError::NonSuspendedInsert {
            state: params.state,
        });
    }
    let sandbox_id = params.id.to_string();
    insert_sandbox(conn, params)?;
    upsert_sandbox_execution_metadata_in_transaction(conn, &sandbox_id, execution)?;
    insert_local_artifact(
        conn,
        &sandbox_id,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        local_path,
    )?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend',
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspended'
           AND current_local_artifact_id IS NULL",
        params![local_artifact_id.to_string(), now()?, sandbox_id],
    )?;
    if changed != 1 {
        return Err(InsertRecoveredSuspendedSandboxError::ArtifactPinInvariant { changed });
    }
    Ok(())
}

/// Adopts or inserts a restore's sandbox row in a transaction of its own.
///
/// Callers already inside a transaction — the quarantine-recovery paths, which
/// have to commit the row and its claim together — use
/// `restore_adopt_orphan_or_insert_sandbox_in_transaction` directly.
pub fn restore_adopt_orphan_or_insert_sandbox(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let tx = immediate_transaction(conn)?;
    restore_adopt_orphan_or_insert_sandbox_in_transaction(&tx, params, restore_operation)?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn restore_adopt_orphan_or_insert_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let InsertSandboxParams {
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    if state != SandboxState::Restoring {
        return Err(RestoreAdoptOrphanInsertError::NonRestoringState { state });
    }
    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let orphan_auto_lifecycle_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

    let rows_affected = conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, restore_origin, restore_phase, restore_storage_uri, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired)
         VALUES (?1, ?2, 'restoring', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?23, ?18, ?18, ?18, 'remote', ?24, ?25, ?26, ?27, ?28, 1)
         ON CONFLICT(id) DO UPDATE SET
             uuid8 = excluded.uuid8,
             state = excluded.state,
             state_changed_at = CASE
                 WHEN sandboxes.state IS NOT excluded.state THEN excluded.state_changed_at
                 ELSE sandboxes.state_changed_at
             END,
             template = excluded.template,
             template_hash = excluded.template_hash,
             user = excluded.user,
             runtime = excluded.runtime,
             uid = NULL,
             slot = NULL,
             guest_cid = NULL,
             pid = NULL,
             spaced_host_pid = NULL,
             netgtw_pid = NULL,
             vsock_uds_path = NULL,
             api_socket_path = NULL,
             space_api_route_generation = NULL,
             space_api_route_node_id = NULL,
             space_api_route_execution_generation = NULL,
             space_api_route_pod_uid = NULL,
             space_api_route_substrate_kind = NULL,
             space_api_route_activation_attempt_id = NULL,
             space_api_route_identity_provenance = NULL,
             spaced_endpoint_proof_json = NULL,
             snapshots_enabled = excluded.snapshots_enabled,
             rootfs_size_mb = excluded.rootfs_size_mb,
             disk_io_max_operations_per_sec = excluded.disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec = excluded.disk_io_max_bandwidth_mib_per_sec,
             pool_template = excluded.pool_template,
             volume_encryption = excluded.volume_encryption,
             guest_uptime_secs = NULL,
             process_env_json = excluded.process_env_json,
             auto_lifecycle_json = excluded.auto_lifecycle_json,
             auto_lifecycle_state_json = excluded.auto_lifecycle_state_json,
             next_auto_event_at = excluded.next_auto_event_at,
             auto_lifecycle_delete_block_reason = NULL,
             auto_lifecycle_delete_blocked_at = NULL,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             network_tx_bytes = 0,
             network_rx_bytes = 0,
             applied_network_policy_generation = 0,
             applied_network_policy_source = '',
             auto_lifecycle_authority = excluded.auto_lifecycle_authority,
             restore_origin = excluded.restore_origin,
             restore_phase = excluded.restore_phase,
             restore_storage_uri = excluded.restore_storage_uri,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at
         WHERE sandboxes.state IN ('quarantined', 'restoring')
           AND EXISTS (
               SELECT 1 FROM sandbox_quarantine_state qs
                WHERE qs.sandbox_id = sandboxes.id
                  AND qs.source = 'sbx_manager'
                  AND qs.reason_code = 'orphaned_sandbox_row_missing'
           )
           AND sandboxes.template = ?19
           AND sandboxes.template_hash IS NULL
           AND sandboxes.user = ?20
           AND sandboxes.runtime = ?21
           AND sandboxes.uid IS NULL
           AND sandboxes.slot IS NULL
           AND sandboxes.guest_cid IS NULL
           AND sandboxes.pid IS NULL
           AND sandboxes.spaced_host_pid IS NULL
           AND sandboxes.netgtw_pid IS NULL
           AND sandboxes.vsock_uds_path IS NULL
           AND sandboxes.api_socket_path IS NULL
           AND sandboxes.snapshots_enabled = 0
           AND sandboxes.pool_template IS NULL
           AND sandboxes.guest_uptime_secs IS NULL
           AND sandboxes.process_env_json = '{}'
           AND sandboxes.auto_lifecycle_json = ?22
           AND sandboxes.cleanup_completion IS NULL
           AND sandboxes.cleanup_keep_filesystem IS NULL
           AND sandboxes.applied_network_policy_generation = 0
           AND sandboxes.applied_network_policy_source = ''
           AND sandboxes.network_policy_authority = 'cluster'
           AND sandboxes.auto_lifecycle_authority = 'cluster'",
        params![
            id,
            uuid8.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            ts,
            ORPHANED_SANDBOX_TEMPLATE,
            ORPHANED_SANDBOX_OWNER.as_str(),
            RuntimeKind::Firecracker.alias(),
            orphan_auto_lifecycle_json,
            volume_encryption,
            restore_operation.phase().as_str(),
            restore_operation.source().as_wire(),
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // New restore staging needs cleanup authority before activation;
            // the conflict branch preserves the adopted row's authority.
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;

    if rows_affected == 1 {
        return Ok(());
    }

    // The synthetic orphan reconciler has no dedicated marker column today, so
    // adoption is gated on its exact stable column tuple. Quarantined rollout
    // residue additionally requires the durable quarantine marker; a conflict
    // that misses these fences is a real sandbox row and must not be clobbered.
    Err(RestoreAdoptOrphanInsertError::ExistingRowNotSyntheticOrphan { id: id.to_string() })
}

pub fn update_snapshots_enabled(conn: &Connection, id: &str, enabled: bool) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET snapshots_enabled = ?1, updated_at = ?2 WHERE id = ?3",
        params![enabled, now()?, id],
    )?;
    Ok(())
}

pub fn update_state(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> Result<SandboxStateWriteOutcome> {
    if matches!(state, SandboxState::Quarantined | SandboxState::Failed) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if state == SandboxState::Deleting && conn.is_autocommit() {
        let tx = immediate_transaction(conn)?;
        let outcome = update_state(&tx, id, state)?;
        tx.commit()?;
        return Ok(outcome);
    }
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    if generic_writer_transition_blocked(previous_state_value, state) {
        tracing::warn!(
            sandbox_id = %id,
            target_state = %state,
            "ignored generic sandbox state update because sandbox is quarantined"
        );
        return Ok(SandboxStateWriteOutcome::BlockedByQuarantine);
    }
    if cleanup_episode(conn, id)?
        .is_some_and(|episode| !episode.completion.deletes_row() || episode.requested_at.is_some())
        || (matches!(
            previous_state_value,
            Some(
                SandboxState::Deleting
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && state != SandboxState::Deleting
            && cleanup_intent_current(conn, id, CleanupCompletion::DeleteRow)?)
        || (matches!(
            previous_state_value,
            Some(
                SandboxState::Stopping
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            )
        ) && matches!(
            state,
            SandboxState::Started
                | SandboxState::Paused
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
        ))
    {
        tracing::warn!(
            sandbox_id = %id,
            target_state = %state,
            "ignored generic sandbox state update because cleanup owns the transition"
        );
        return Ok(SandboxStateWriteOutcome::SupersededByCleanupEpisode);
    }

    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN restore_origin ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN restore_phase ELSE NULL END,
             restore_storage_uri = CASE WHEN ?1 = 'restoring' THEN restore_storage_uri ELSE NULL END,
             transition_receipt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             updated_at = ?2
         WHERE id = ?3
           AND (?1 = 'deleting' OR state != 'quarantined')",
        params![state.as_str(), now()?, id],
    )?;
    if changed != 0 && state == SandboxState::Deleting {
        teardown_jobs::fail_active(conn, id, TeardownSuspendFailure::Deleting)?;
    }
    Ok(if changed == 0 {
        SandboxStateWriteOutcome::Missing
    } else {
        SandboxStateWriteOutcome::Applied
    })
}

/// Check the observation against the current incarnation before replaying
/// guest setup. The caller holds the wake and cleanup fences across setup.
pub(crate) fn startup_resident_witness_current(
    conn: &Connection,
    observed: &SandboxRow,
) -> Result<bool> {
    let Some(current) = get_sandbox(conn, &observed.id)? else {
        return Ok(false);
    };
    Ok(inventory_deletion_witness_matches(&current, observed)
        && current.uuid8 == observed.uuid8
        && current.space_api_route_identity == observed.space_api_route_identity
        && current.process_env == observed.process_env
        && current.spaced_endpoint_proof == observed.spaced_endpoint_proof
        && current.cleanup_completion.is_none()
        && current.cleanup_keep_filesystem.is_none()
        // Presence alone blocks competing recovery, including ownership written
        // by a newer binary that this reader cannot decode.
        && !has_cleanup_episode(conn, &observed.id)?
        && !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1)",
            [&observed.id],
            |row| row.get::<_, bool>(0),
        )?)
}

/// The settlement determines both durable state and event semantics. Healthy
/// outcomes cannot carry quarantine reasons or errors.
pub(crate) enum StartupResidentSettlement<'a> {
    Started,
    Paused,
    Quarantined {
        reason_code: SandboxQuarantineReasonCode,
        message: &'a str,
    },
}

/// Commit lifecycle and quarantine evidence together. A changed row or durable
/// cleanup owner supersedes the entire observation.
pub(crate) fn settle_startup_resident(
    conn: &Connection,
    observed: &SandboxRow,
    settlement: StartupResidentSettlement<'_>,
) -> Result<bool> {
    if !matches!(
        observed.state,
        SandboxState::Started
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Checkpointing
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let (target, reason, error) = match &settlement {
        StartupResidentSettlement::Started => (
            SandboxState::Started,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Paused => (
            SandboxState::Paused,
            "startup_recovery_runtime_state_observed",
            None,
        ),
        StartupResidentSettlement::Quarantined {
            reason_code,
            message,
        } => (
            SandboxState::Quarantined,
            reason_code.as_str(),
            Some(*message),
        ),
    };
    let tx = immediate_transaction(conn)?;
    if !startup_resident_witness_current(&tx, observed)? {
        return Ok(false);
    }
    let timestamp = now()?;
    // A receipt acknowledges its current target, not historical success.
    // Preserve a matching healthy acknowledgement across recovery, but retire
    // every receipt on quarantine in the same transaction as its evidence.
    tx.execute(
        "UPDATE sandboxes SET state = ?1, failure_reason = ?2,
             transition_receipt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 != 'quarantined' AND ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             state_changed_at = ?3, updated_at = ?3 WHERE id = ?4",
        params![target.as_str(), error, timestamp, observed.id],
    )?;
    if let StartupResidentSettlement::Quarantined {
        reason_code,
        message,
    } = settlement
    {
        crate::route_activation_claims::close_claim(&tx, &observed.id)?;
        insert_new_quarantine_episode(
            &tx,
            &QuarantineEpisode {
                sandbox_id: &observed.id,
                entered_from_state: Some(observed.state.as_str()),
                source: SandboxQuarantineSource::SbxManager,
                reason_code,
                reason_message: Some(message),
                next_action: SandboxQuarantineNextAction::RequiresOperatorAttention,
                timestamp: &timestamp,
            },
        )?;
    }
    tx.commit()?;
    emit_sandbox_state_event(
        &observed.id,
        if target == SandboxState::Quarantined {
            SandboxEventType::SandboxQuarantined
        } else {
            SandboxEventType::LifecycleConverged
        },
        Some(observed.state.as_str()),
        reason,
        "startup_resident_recovery",
        target,
        error,
    );
    Ok(true)
}

pub(crate) fn restore_checkpoint_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'checkpointing'",
        params![stable_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Fence policy installation and Paused -> Resuming in one transaction.
/// An absent generation matches only an identity-free legacy resident; it is
/// never a wildcard for an execution that has acquired a generation.
pub(crate) fn begin_resident_resume_with_policy(
    conn: &Connection,
    id: &str,
    generation: Option<ExecutionGeneration>,
    policy: &AutoLifecyclePolicy,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(sandbox) = get_sandbox(&tx, id)? else {
        return Ok(false);
    };
    if sandbox.state != SandboxState::Paused
        || sandbox.space_api_route_identity.execution_generation != generation
    {
        return Ok(false);
    }
    if update_state(&tx, id, SandboxState::Resuming)? != SandboxStateWriteOutcome::Applied {
        return Ok(false);
    }
    install_activation_auto_lifecycle(&tx, id, policy)?;
    tx.commit()?;
    Ok(true)
}

pub(crate) fn restore_resident_resume_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = rollback_activation_state(&tx, id, SandboxState::Resuming, stable_state)?;
    if changed && stable_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed)
}

pub(crate) fn recover_interrupted_pause_state(
    conn: &Connection,
    id: &str,
    target: PauseFailedRecoveryTarget,
) -> Result<bool> {
    let target_state = target.sandbox_state();
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'pausing'",
        params![target_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Transfer lifecycle ownership from an armed activation to an explicit
/// stop/delete teardown. The caller is responsible for completing that
/// teardown; the activation guard becomes a superseded no-op.
fn update_state_superseding_activation(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    let tx = immediate_transaction(conn)?;
    update_state_superseding_activation_in_transaction(&tx, id, state)?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn update_state_superseding_activation_in_transaction(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    if let Some(episode) = cleanup_episode(conn, id)? {
        if state == SandboxState::Stopping
            && (episode.entered_from_state == Some(SandboxState::Quarantined)
                || episode.quarantine_recovery.is_some())
        {
            return Err(SupersedeActivationError::QuarantineRecoveryInProgress);
        }
        conn.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, episode.id.to_string()],
        )?;
        conn.execute(
            "UPDATE sandboxes
                SET cleanup_completion = NULL,
                    cleanup_keep_filesystem = NULL
              WHERE id = ?1
                AND cleanup_completion = ?2
                AND cleanup_keep_filesystem = ?3",
            params![id, episode.completion.as_str(), episode.keep_filesystem],
        )?;
    }
    update_state(conn, id, state)?.require_applied()?;
    Ok(())
}

pub fn mark_stopping_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Stopping)
}

pub fn mark_deleting_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Deleting)
}

/// Claim the caller-observed sandbox state for activation.
///
/// A false result means the row was removed or another state transition won;
/// the caller must not allocate or launch runtime resources.
pub fn begin_activation(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation_with_route_evidence(
        conn,
        id,
        expected_state,
        kind,
        execution_generation,
        activation_attempt_id,
        ActivationRouteAdmission::default(),
    )
}

#[derive(Default)]
pub(crate) struct ActivationRouteAdmission<'a> {
    pub(crate) auto_lifecycle: Option<&'a AutoLifecyclePolicy>,
    pub(crate) operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(crate) evidence: Option<crate::auto_lifecycle_delete_receipts::RouteEvidence>,
    pub(crate) open_claim: Option<&'a crate::route_activation_claims::OpenRouteActivationClaim>,
}

pub(crate) fn begin_activation_with_route_evidence(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    route_admission: ActivationRouteAdmission<'_>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    let completion = match (kind, expected_state) {
        (ActivationKind::ColdStart, SandboxState::Created) => CleanupCompletion::MarkCreated,
        (ActivationKind::ColdStart, SandboxState::Stopped) => CleanupCompletion::MarkStopped,
        (ActivationKind::ColdStart, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        (ActivationKind::Restore, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        _ => {
            return Err(BeginActivationError::UnsupportedState {
                kind,
                state: expected_state,
            });
        }
    };
    let activation_state = kind.activation_state();
    let tx = immediate_transaction(conn)?;
    if cleanup_episode(&tx, id)?.is_some() {
        tx.commit()?;
        return Ok(None);
    }
    let keep_filesystem = true;
    // Only activation authorizes node allocation. Filesystem-only staging keeps
    // an unbound request identity and must not become an allocation predecessor.
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN 'local' ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN 'activation' ELSE NULL END,
             restore_storage_uri = NULL,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             previous_node_execution_id = CASE WHEN node_execution_retired = 0 THEN node_execution_id END,
             node_execution_id = ?8,
             node_execution_retired = 0,
             transition_receipt_id = NULL,
             transition_receipt_kind = NULL,
             transition_receipt_target_state = NULL,
             transition_receipt_execution_generation = NULL,
             transition_receipt_reason_code = NULL,
             transition_receipt_activation_attempt_id = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?6 ELSE state_changed_at END,
             updated_at = ?6
         WHERE id = ?4
           AND state = ?5
           AND state != 'quarantined'
           AND NOT EXISTS (
               SELECT 1
                 FROM auto_lifecycle_delete_outbox
                WHERE sandbox_id = ?4
                  AND state = 'pending'
                  AND (?7 IS NULL OR execution_generation >= ?7)
           )
           AND (
               (cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL)
               OR (cleanup_completion = ?2 AND cleanup_keep_filesystem = ?3)
           )",
        params![
            activation_state.as_str(),
            completion.as_str(),
            keep_filesystem,
            id,
            expected_state.as_str(),
            now()?,
            execution_generation.map(ExecutionGeneration::get),
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    if let Some(policy) = route_admission.auto_lifecycle {
        install_activation_auto_lifecycle(&tx, id, policy)?;
    }
    let sandbox_id = (route_admission.evidence.is_some() || execution_generation.is_some())
        .then(|| {
            id.parse::<SandboxId>()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })
        .transpose()?;
    match route_admission.open_claim {
        Some(open_route_activation) => {
            crate::route_activation_claims::insert_create_claim(&tx, id, open_route_activation)?;
        }
        None => {
            // Beginning any other activation supersedes an older Kubernetes
            // continuation ticket in the same transaction that claims the row.
            crate::route_activation_claims::close_claim(&tx, id)?;
        }
    }
    if let Some(route_evidence) = route_admission.evidence {
        let sandbox_id = sandbox_id.ok_or(rusqlite::Error::InvalidQuery)?;
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id,
            route_evidence,
        )?;
    }
    let receipt_supersession = match sandbox_id {
        Some(sandbox_id) => crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            execution_generation,
        )?,
        None => None,
    };
    let cleanup_id = SandboxCleanupEpisodeId::new();
    insert_cleanup_episode(
        &tx,
        id,
        CleanupEpisodeSpec {
            id: cleanup_id,
            completion,
            keep_filesystem,
            entered_from_state: Some(expected_state),
            activation_state,
            execution_generation,
            activation_attempt_id,
        },
    )?;
    tx.execute(
        "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
         WHERE sandbox_id = ?1 AND cleanup_id = ?2",
        params![
            id,
            cleanup_id.to_string(),
            route_admission
                .operation_activation_attempt_id
                .map(|id| id.to_string())
        ],
    )?;
    let episode = cleanup_episode(&tx, id)?.filter(|episode| episode.id == cleanup_id);
    tx.commit()?;
    if let Some(sandbox_id) = sandbox_id {
        emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    }
    Ok(episode)
}

pub fn sandbox_is_retryable_interrupted_restore(row: &SandboxRow) -> bool {
    row.state == SandboxState::Restoring
        && row
            .restore_operation
            .as_ref()
            .is_some_and(|operation| operation.origin() == RestoreOrigin::Remote)
        && !sandbox_has_runtime_fields(row)
        && row.current_local_artifact_id.is_none()
        && row.cleanup_completion.is_none()
        && row.cleanup_keep_filesystem.is_none()
}

/// Stamp the terminal-teardown-suspend marker on a `suspending` row whose
/// durable backup the cluster refused permanently.
///
/// Scoped to the exact staged artifact the failed backup covered, so a row
/// that has already moved on keeps its own witness. The write is conditional
/// on the reason actually changing: the shim re-polls `PrepareTeardownSuspend`
/// for as long as the pod is terminating, and each poll re-drives the suspend
/// through this same terminal job. An unconditional stamp would bump
/// `updated_at` on every poll and hold the row permanently inside the
/// cluster-deleted eviction's quiescence window — the one thing that can
/// reclaim it.
pub fn mark_teardown_suspend_backup_permanently_failed(
    conn: &Connection,
    id: &str,
    staged_artifact: LocalArtifactId,
) -> Result<bool> {
    let reason = pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspending'
           AND current_local_artifact_id = ?4
           AND (failure_reason IS NULL OR failure_reason != ?1)",
        params![reason, now()?, id, staged_artifact.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn mark_cross_node_restore_staging_failed(
    conn: &Connection,
    id: &str,
    failure_reason: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'",
        params![failure_reason, now()?, id],
    )?;
    Ok(changed == 1)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivationOperationError {
    #[error("activation receipt database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("sandbox no longer exists")]
    MissingSandbox,
    #[error("activation route evidence does not match the selected execution")]
    RouteMismatch,
    #[error("activation operation was superseded")]
    Superseded,
    #[error("activation target has not been reached or is unsupported")]
    InvalidTarget,
    #[error("activation cleanup is still pending")]
    CleanupPending,
}

/// Join an armed activation without changing its route-ownership fence. The
/// update is durable before a Wake waiter can disappear; cleanup will report
/// the joined operation even if the original worker finishes asynchronously.
pub(crate) fn bind_activation_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    if let Some(episode) = cleanup_episode(&tx, id)?
        && episode.requested_at.is_none()
    {
        if episode.execution_generation != route.execution_generation
            || episode.activation_attempt_id != route.activation_attempt_id
        {
            return Err(ActivationOperationError::RouteMismatch);
        }
        if episode
            .operation_activation_attempt_id
            .is_some_and(|current| current > operation)
        {
            return Err(ActivationOperationError::Superseded);
        }
        tx.execute(
            "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
             WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND phase = 'armed'",
            params![id, episode.id.to_string(), operation.to_string()],
        )?;
    }
    tx.commit().map_err(Into::into)
}

/// A resident Wake has no cleanup episode. Persist fresh completion evidence
/// while its wake lock is held, after identity adoption and before fallible
/// gateway sync. Lifecycle completion does not depend on that network call. This
/// also closes a join whose activation completed before we bound its episode.
pub(crate) fn complete_resident_wake_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
    target: SandboxState,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    let row = get_sandbox(&tx, id)?.ok_or(ActivationOperationError::MissingSandbox)?;
    let actual = &row.space_api_route_identity;
    if row.state != target {
        return Err(ActivationOperationError::InvalidTarget);
    }
    if cleanup_episode(&tx, id)?.is_some() {
        return Err(ActivationOperationError::CleanupPending);
    }
    if actual.clone().try_into_complete().is_err()
        || actual.execution_generation != route.execution_generation
        || actual.route_generation != route.route_generation
        || actual.activation_attempt_id != route.activation_attempt_id
        || actual.pod_uid != route.pod_uid
        || actual.effective_substrate_kind() != route.effective_substrate_kind()
    {
        return Err(ActivationOperationError::RouteMismatch);
    }
    let generation = route
        .execution_generation
        .ok_or(ActivationOperationError::RouteMismatch)?;
    let target =
        activation_completed_target(target).ok_or(ActivationOperationError::InvalidTarget)?;
    let previous: (Option<i64>, Option<String>, Option<String>, Option<String>) = tx.query_row(
        "SELECT transition_receipt_execution_generation, transition_receipt_activation_attempt_id,
                transition_receipt_kind, transition_receipt_target_state
         FROM sandboxes WHERE id = ?1",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if previous.0 == Some(generation.get())
        && let Some(previous_id) = previous.1
    {
        let previous_operation: SandboxActivationAttemptId =
            previous_id.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
        if previous_operation > operation {
            // Do not replace a potentially newer completion. UUID ordering is
            // only a conservative receipt-retention policy: PostgreSQL clears
            // completed owners, so clock skew can make a legitimate later
            // no-op Wake's ID lower. It must not reject that healthy Wake.
            // Its caller can release the exact lease from lifecycle readiness;
            // no activation cleanup obligation exists on this resident path.
            return tx.commit().map_err(Into::into);
        }
        if previous_operation == operation
            && previous.2.as_deref() == Some("activation_completed")
            && previous.3.as_deref() == Some(target.as_str())
        {
            return tx.commit().map_err(Into::into);
        }
    }
    tx.execute(
        "UPDATE sandboxes SET transition_receipt_id = ?2,
            transition_receipt_kind = 'activation_completed',
            transition_receipt_target_state = ?3,
            transition_receipt_execution_generation = ?4,
            transition_receipt_activation_attempt_id = ?5,
            transition_receipt_reason_code = NULL, updated_at = ?6
         WHERE id = ?1",
        params![
            id,
            SandboxCleanupEpisodeId::new().to_string(),
            target.as_str(),
            generation.get(),
            operation.to_string(),
            now()?
        ],
    )?;
    tx.commit().map_err(Into::into)
}

fn activation_completed_target(state: SandboxState) -> Option<SandboxTransitionTargetState> {
    match state {
        SandboxState::Created => Some(SandboxTransitionTargetState::Created),
        SandboxState::Started => Some(SandboxTransitionTargetState::Started),
        SandboxState::Paused => Some(SandboxTransitionTargetState::Paused),
        _ => None,
    }
}

/// Leave a durable acknowledgement for the exact control-plane activation
/// attempt before retiring its cleanup episode. The API may clear the attempt
/// eagerly, but this receipt closes the crash window between node success and
/// that best-effort control-plane write.
fn persist_activation_completed_receipt(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    target_state: SandboxState,
) -> Result<()> {
    let Some(episode) = cleanup_episode(tx, sandbox_id)?.filter(|episode| episode.id == cleanup_id)
    else {
        return Ok(());
    };
    let (Some(execution_generation), Some(activation_attempt_id), Some(target_state)) = (
        episode.execution_generation,
        // Explicit operation ownership wins; old episodes retain legacy receipts.
        episode
            .operation_activation_attempt_id
            .or(episode.activation_attempt_id),
        activation_completed_target(target_state),
    ) else {
        return Ok(());
    };
    let receipt = SandboxTransitionReceipt::<SandboxQuarantineReasonCode>::activation_completed(
        cleanup_id,
        target_state,
        execution_generation,
        activation_attempt_id,
    );
    tx.execute(
        "UPDATE sandboxes
            SET transition_receipt_id = ?1,
                transition_receipt_kind = ?2,
                transition_receipt_target_state = ?3,
                transition_receipt_execution_generation = ?4,
                transition_receipt_activation_attempt_id = ?5,
                transition_receipt_reason_code = NULL
          WHERE id = ?6 AND state = ?3",
        params![
            receipt.id().to_string(),
            receipt.kind().as_str(),
            receipt.target_state().as_str(),
            receipt.execution_generation().get(),
            receipt.activation_attempt_id().map(|id| id.to_string()),
            sandbox_id,
        ],
    )?;
    Ok(())
}

pub fn mark_start_succeeded(
    conn: &Connection,
    id: &str,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'started',
             failure_reason = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             state_changed_at = CASE WHEN state != 'started' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'starting'
           AND (?3 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?3
                  AND episode.phase = 'armed'
           ))",
        params![now()?, id, cleanup_id.map(|id| id.to_string())],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, SandboxState::Started)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Publish a disposable clone only while its original cleanup episode is armed.
/// Publication and retiring deletion ownership are one transaction.
pub(crate) fn complete_pool_clone(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'suspended',
                state_changed_at = ?1,
                updated_at = ?1,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL
          WHERE id = ?2 AND state = 'creating'
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
                   AND episode.activation_state = 'creating'
                   AND episode.completion = 'delete_pool_clone'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn complete_create(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    publish_activation_completion: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL,
                updated_at = ?1
          WHERE id = ?2
            AND state IN ('created', 'suspended')
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        if publish_activation_completion {
            let target_state = get_sandbox(&tx, id)?
                .map(|row| row.state)
                .unwrap_or(SandboxState::Created);
            persist_activation_completed_receipt(&tx, id, cleanup_id, target_state)?;
        }
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn mark_restore_succeeded(
    conn: &Connection,
    id: &str,
    final_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    if !matches!(final_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND (?4 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?4
                  AND episode.phase = 'armed'
           ))",
        params![
            final_state.as_str(),
            now()?,
            id,
            cleanup_id.map(|id| id.to_string())
        ],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, final_state)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 && final_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    if changed == 1 {
        emit_sandbox_state_event(
            id,
            SandboxEventType::RestoreCompleted,
            Some(SandboxState::Restoring.as_str()),
            "restore_succeeded",
            "mark_restore_succeeded",
            final_state,
            None,
        );
    }
    Ok(changed == 1)
}

fn arm_auto_lifecycle_after_activation(conn: &Connection, id: &str) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime_state = sandbox.auto_lifecycle_state;
    crate::auto_lifecycle::arm_inactivity_conditions(
        &mut runtime_state,
        &sandbox.auto_lifecycle,
        crate::auto_lifecycle::now_secs(),
    );
    let runtime_state_json = serde_json::to_string(&runtime_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET auto_lifecycle_state_json = ?1,
             next_auto_event_at = ?2
         WHERE id = ?3",
        params![
            runtime_state_json,
            runtime_state.next_auto_event_at.map(SqliteU64),
            id
        ],
    )?;
    Ok(())
}

pub fn begin_suspend(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2 AND state = 'paused'
           AND cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn begin_teardown_suspend(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'paused'",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

fn emit_auto_lifecycle_delete_receipt_superseded(
    sandbox_id: SandboxId,
    superseded_receipt: crate::auto_lifecycle_delete_receipts::db::SupersededPendingReceipt,
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

pub(crate) fn emit_auto_lifecycle_delete_receipt_superseded_if_present(
    sandbox_id: SandboxId,
    supersession: Option<crate::auto_lifecycle_delete_receipts::db::PendingReceiptSupersession>,
) {
    if let Some(supersession) = supersession {
        emit_auto_lifecycle_delete_receipt_superseded(
            sandbox_id,
            supersession.receipt,
            supersession.superseding_generation,
        );
    }
}

pub fn rollback_suspending_to_paused(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'paused',
             current_local_artifact_id = suspend_predecessor_artifact_id,
             current_local_artifact_kind = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'full'
             END,
             current_local_artifact_origin = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'suspend'
             END,
             suspend_predecessor_artifact_id = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'paused' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'suspending'",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn mark_suspending_quarantined(
    conn: &Connection,
    id: &str,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Suspending,
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn mark_starting_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Starting,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn mark_restoring_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Restoring,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

pub fn rollback_starting(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Starting, state)
}

pub fn rollback_restoring(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Restoring, state)
}

pub fn rollback_restoring_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Restoring, state, cleanup_id)
}

pub fn rollback_starting_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Starting, state, cleanup_id)
}

fn rollback_activation_episode(
    conn: &Connection,
    id: &str,
    activation_state: SandboxState,
    restored_state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    if !matches!(
        activation_state,
        SandboxState::Starting | SandboxState::Restoring
    ) {
        return Err(RollbackActivationEpisodeError::UnsupportedActivationState {
            state: activation_state,
        });
    }
    if restored_state == SandboxState::Failed {
        return Err(rusqlite::Error::InvalidQuery.into());
    }
    if restored_state == SandboxState::Quarantined {
        return Err(RollbackActivationEpisodeError::QuarantinedTarget);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                failure_reason = NULL,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3
            AND state = ?4
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?5
                   AND episode.phase = 'armed'
            )",
        params![
            restored_state.as_str(),
            now()?,
            id,
            activation_state.as_str(),
            cleanup_id.to_string()
        ],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

fn rollback_activation_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    restored_state: SandboxState,
) -> Result<bool> {
    if matches!(
        restored_state,
        SandboxState::Quarantined | SandboxState::Failed
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_origin END,
             restore_phase = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_phase END,
             restore_storage_uri = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_storage_uri END,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = ?4",
        params![restored_state.as_str(), now()?, id, expected_state.as_str()],
    )?;
    Ok(changed == 1)
}

/// Delete every `sandbox_id`-scoped child row of a sandbox inside the
/// caller's open transaction. The `foreign_keys` pragma is disabled, so this
/// manual cascade is the single source of truth for sandbox child tables:
/// add new child tables here, not at the individual delete call sites.
fn delete_sandbox_child_rows(tx: &Connection, id: &str) -> Result<()> {
    teardown_jobs::fail_active(tx, id, TeardownSuspendFailure::Deleted)?;
    crate::route_activation_claims::close_claim(tx, id)?;
    tx.execute(
        "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM local_artifacts WHERE sandbox_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM network_policy_reconcile_queue WHERE sandbox_id = ?1",
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
    Ok(())
}

pub fn sandbox_state_changed_at(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<HashMap<String, String>> {
    if sandbox_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT id, state_changed_at
         FROM sandboxes
         WHERE id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&query)?;
    let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
        Ok((row.get(0)?, timestamps::diagnostic_column(row, 1)?))
    })?;
    rows.collect()
}

/// Read one row's `state_changed_at` exactly as stored.
///
/// Raw, unlike [`sandbox_state_changed_at`]'s diagnostic rendering, and for
/// the same reason [`SandboxRow::updated_at`] is raw: the value doubles as a
/// CAS witness, so it has to compare byte-for-byte against the column it came
/// from. Time-based decisions decode it strictly through
/// [`timestamps::EpochSeconds::parse`] instead of trusting the text.
///
/// `None` means no such row. A non-text value is returned rendered rather
/// than rejected, so a corrupt timestamp reaches the caller's explicit
/// refusal path instead of looking like a missing sandbox.
pub fn sandbox_state_changed_at_witness(conn: &Connection, id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
        params![id],
        |row| {
            Ok(match row.get::<_, rusqlite::types::Value>(0)? {
                rusqlite::types::Value::Text(raw) => raw,
                rusqlite::types::Value::Integer(raw) => raw.to_string(),
                rusqlite::types::Value::Real(raw) => raw.to_string(),
                rusqlite::types::Value::Null | rusqlite::types::Value::Blob(_) => String::new(),
            })
        },
    )
    .optional()
}

fn generic_writer_transition_blocked(
    previous_state: Option<SandboxState>,
    target_state: SandboxState,
) -> bool {
    previous_state
        .filter(|state| *state == SandboxState::Quarantined)
        .is_some_and(|state| state.transition(target_state).is_err())
}

pub fn update_uuid8(conn: &Connection, id: &str, uuid8: &SandboxRuntimeLabel) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uuid8 = ?1, updated_at = ?2 WHERE id = ?3",
        params![uuid8.as_str(), now()?, id],
    )?;
    Ok(())
}

/// Publish route identity and invalidate evidence bound to a replaced execution.
///
/// Unlike [`update_runtime_fields`] this touches no runtime process state and
/// does not advance the teardown suspend lease: it exists for activations that
/// republish the route identity of an already-running VM, where the runtime
/// fields and the execution generation are unchanged.
///
/// Forward-moving callers validate the route epoch before entering this
/// writer. Pool-adoption rollback is the exception: it restores a snapshot of
/// this same row taken before adoption. The pending-receipt predicate still
/// prevents that rollback from crossing a receipt for the restored or a newer
/// execution generation.
///
/// Route-only adoption cannot establish a successor execution. It is limited
/// to lifecycle-stable rows without cleanup ownership; a new runtime must use
/// the activation path that claims its lifecycle state and cleanup episode.
/// A new execution generation or activation attempt invalidates endpoint
/// evidence bound to the prior activation.
pub fn update_space_api_route_identity(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let receipt_supersession =
        update_space_api_route_identity_in_transaction(&tx, sandbox_id, identity, provenance)?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    Ok(())
}

pub(crate) fn update_space_api_route_identity_in_transaction(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<Option<crate::auto_lifecycle_delete_receipts::db::PendingReceiptSupersession>> {
    let sandbox_id_string = sandbox_id.to_string();
    let previous_generation = runtime_execution_generation(conn, &sandbox_id_string)?;
    let updated = conn.execute(
        "UPDATE sandboxes SET space_api_route_generation = ?1, space_api_route_node_id = ?2,
         space_api_route_execution_generation = ?3, space_api_route_pod_uid = ?4,
         space_api_route_substrate_kind = ?5, space_api_route_activation_attempt_id = ?6,
         space_api_route_identity_provenance = ?7,
         spaced_endpoint_proof_json = CASE
             WHEN space_api_route_execution_generation IS ?3
              AND space_api_route_activation_attempt_id IS ?6
             THEN spaced_endpoint_proof_json
             ELSE NULL
         END,
         updated_at = ?8
         WHERE id = ?9
           AND state IN ('started', 'paused')
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?9
                 AND state = 'pending'
                  AND (?3 IS NULL OR execution_generation >= ?3)
           )",
        params![
            identity
                .route_generation
                .map(|value| SqliteU64(value.get())),
            identity.current_node_id.map(|value| value.to_string()),
            identity.execution_generation.map(|value| value.get()),
            identity.pod_uid.as_ref().map(PodUid::as_str),
            identity.substrate_kind.map(|value| value.to_string()),
            identity
                .activation_attempt_id
                .map(|value| value.to_string()),
            provenance.as_str(),
            now()?,
            sandbox_id_string
        ],
    )?;
    if updated != 1 {
        return Err(
            crate::auto_lifecycle_delete_receipts::db::execution_write_miss_error(
                conn,
                sandbox_id,
                identity.execution_generation,
            ),
        );
    }
    reset_access_floor_for_execution_change(
        conn,
        &sandbox_id_string,
        previous_generation,
        identity.execution_generation,
    )?;
    if let Some(evidence) =
        crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(identity, provenance)
    {
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            conn, sandbox_id, evidence,
        )?;
    }
    // Provenance controls whether route evidence may mint a receipt. Once this
    // transaction persists a newer execution generation, an older receipt no
    // longer describes the row, including when the new route is incomplete.
    let receipt_supersession =
        crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            conn,
            sandbox_id,
            identity.execution_generation,
        )?;
    Ok(receipt_supersession)
}

pub(crate) fn update_runtime_fields(
    conn: &Connection,
    sandbox_id: SandboxId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    update_runtime_fields_with_activation_owner(conn, sandbox_id, None, fields)
}

pub fn update_runtime_fields_for_activation(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: SandboxCleanupEpisodeId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    update_runtime_fields_with_activation_owner(conn, sandbox_id, Some(cleanup_id), fields)
}

fn update_runtime_fields_with_activation_owner(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    let id = sandbox_id.to_string();
    let RuntimeFields {
        uid,
        slot,
        guest_cid,
        pid,
        spaced_host_pid,
        netgtw_pid,
        vsock_uds_path,
        api_socket_path,
        route_identity_provenance,
        spaced_endpoint_proof,
        space_api_route_identity:
            SpaceApiRouteIdentity {
                route_generation: space_api_route_generation,
                current_node_id: space_api_route_node_id,
                substrate_kind: space_api_route_substrate_kind,
                execution_generation: space_api_route_execution_generation,
                pod_uid: space_api_route_pod_uid,
                activation_attempt_id: space_api_route_activation_attempt_id,
                ..
            },
    } = fields;
    let execution_generation = space_api_route_execution_generation;
    let spaced_endpoint_proof_json = spaced_endpoint_proof
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

    let tx = immediate_transaction(conn)?;
    let previous_generation = runtime_execution_generation(&tx, &id)?;
    let claim = execution_generation
        .map(|execution_generation| advance_teardown_suspend_lease(&tx, &id, execution_generation))
        .transpose()?
        .unwrap_or(None);
    let updated = tx.execute(
        "UPDATE sandboxes SET uid = ?1, slot = ?2, guest_cid = ?3, pid = ?4,
         spaced_host_pid = ?5, netgtw_pid = ?6, vsock_uds_path = ?7, api_socket_path = ?8,
         space_api_route_generation = ?9, space_api_route_node_id = ?10,
         space_api_route_execution_generation = ?11, space_api_route_pod_uid = ?12,
         space_api_route_substrate_kind = ?13,
         space_api_route_activation_attempt_id = ?14,
         space_api_route_identity_provenance = ?15,
         spaced_endpoint_proof_json = ?16,
         updated_at = ?17
         WHERE id = ?18
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?18
                 AND state = 'pending'
                  AND (?11 IS NULL OR execution_generation >= ?11)
           )
           AND (
               (
                   ?19 IS NULL
                   AND state NOT IN ('deleting', 'cleanup_failed')
                   AND cleanup_completion IS NULL
                   AND cleanup_keep_filesystem IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM sandbox_cleanup_episodes
                        WHERE sandbox_id = ?18
                   )
               )
               OR EXISTS (
                   SELECT 1
                     FROM sandbox_cleanup_episodes AS episode
                    WHERE episode.sandbox_id = ?18
                      AND episode.cleanup_id = ?19
                      AND episode.phase = 'armed'
                      AND episode.activation_state = sandboxes.state
                      AND episode.completion = sandboxes.cleanup_completion
                      AND episode.keep_filesystem = sandboxes.cleanup_keep_filesystem
               )
           )",
        params![
            uid,
            slot,
            guest_cid,
            pid,
            spaced_host_pid,
            netgtw_pid,
            vsock_uds_path,
            api_socket_path,
            space_api_route_generation.map(|value| SqliteU64(value.get())),
            space_api_route_node_id.map(|value| value.to_string()),
            space_api_route_execution_generation.map(|value| value.get()),
            space_api_route_pod_uid.as_ref().map(PodUid::as_str),
            space_api_route_substrate_kind.map(|value| value.to_string()),
            space_api_route_activation_attempt_id.map(|value| value.to_string()),
            route_identity_provenance.as_str(),
            spaced_endpoint_proof_json,
            now()?,
            id,
            cleanup_id.map(|value| value.to_string())
        ],
    )?;
    if updated != 1 {
        return Err(
            crate::auto_lifecycle_delete_receipts::db::execution_write_miss_error(
                &tx,
                sandbox_id,
                space_api_route_execution_generation,
            ),
        );
    }
    reset_access_floor_for_execution_change(&tx, &id, previous_generation, execution_generation)?;
    if let (Some(execution_generation), Some(activation_attempt_id), Some(substrate)) = (
        space_api_route_execution_generation,
        space_api_route_activation_attempt_id,
        space_api_route_substrate_kind,
    ) {
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id,
            crate::auto_lifecycle_delete_receipts::RouteEvidence {
                execution_generation,
                activation_attempt_id,
                substrate,
                provenance: route_identity_provenance,
            },
        )?;
    }
    let receipt_supersession =
        crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            space_api_route_execution_generation,
        )?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    if let Some(execution_generation) = execution_generation {
        emit_teardown_suspend_lease_claim(&id, execution_generation, claim);
    }
    Ok(claim)
}

// Called inside the identity writer's transaction. Same-generation refresh and
// pause/resume preserve the floor; a new execution cannot inherit it.
fn reset_access_floor_for_execution_change(
    conn: &Connection,
    id: &str,
    previous: Option<ExecutionGeneration>,
    next: Option<ExecutionGeneration>,
) -> Result<()> {
    if previous == next {
        return Ok(());
    }
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime = sandbox.auto_lifecycle_state;
    if runtime.access_extended_until.take().is_some() {
        crate::auto_lifecycle::recompute_runtime_state(&mut runtime, &sandbox.auto_lifecycle);
        update_auto_lifecycle_state(conn, id, &runtime)?;
    }
    Ok(())
}

pub fn runtime_execution_generation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<ExecutionGeneration>> {
    let generation = conn
        .query_row(
            "SELECT space_api_route_execution_generation
         FROM sandboxes
         WHERE id = ?1",
            params![sandbox_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten();
    generation
        .map(ExecutionGeneration::new)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Integer, Box::new(error))
        })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActivationRouteProvenance {
    pub current_route_generation: Option<RouteGeneration>,
    pub current_execution_generation: Option<ExecutionGeneration>,
    pub current_substrate_kind: Option<NodeManagedBy>,
    pub retained_route_generation: Option<RouteGeneration>,
    pub retained_execution_generation: Option<ExecutionGeneration>,
    pub retained_substrate_kind: Option<NodeManagedBy>,
}

fn activation_route_provenance_from_row(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> Result<ActivationRouteProvenance> {
    let values = (
        row.get::<_, Option<SqliteU64>>(offset)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 1)?,
        row.get::<_, Option<String>>(offset + 2)?,
        row.get::<_, Option<SqliteU64>>(offset + 3)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 4)?,
        row.get::<_, Option<String>>(offset + 5)?,
        row.get::<_, Option<String>>(offset + 6)?,
    );
    let parse_route_generation = |value: Option<u64>, column| {
        value
            .map(RouteGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_execution_generation = |value: Option<i64>, column| {
        value
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_substrate = |value: Option<String>, column| {
        value
            .map(|value| value.parse::<NodeManagedBy>())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
            })
    };
    let retained_route_substrate = parse_substrate(values.5, offset + 5)?;
    let retained_legacy_substrate = parse_substrate(values.6, offset + 6)?;
    let retained_substrate_kind = match (retained_route_substrate, retained_legacy_substrate) {
        (Some(NodeManagedBy::Kubernetes), _) | (_, Some(NodeManagedBy::Kubernetes)) => {
            Some(NodeManagedBy::Kubernetes)
        }
        (Some(NodeManagedBy::Space), _) | (_, Some(NodeManagedBy::Space)) => {
            Some(NodeManagedBy::Space)
        }
        (None, None) => None,
    };
    Ok(ActivationRouteProvenance {
        current_route_generation: parse_route_generation(values.0, offset)?,
        current_execution_generation: parse_execution_generation(values.1, offset + 1)?,
        current_substrate_kind: parse_substrate(values.2, offset + 2)?,
        retained_route_generation: parse_route_generation(values.3, offset + 3)?,
        retained_execution_generation: parse_execution_generation(values.4, offset + 4)?,
        retained_substrate_kind,
    })
}

pub fn activation_route_provenance(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<ActivationRouteProvenance> {
    let values = conn.query_row(
        "SELECT space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id = ?1",
        params![sandbox_id],
        |row| activation_route_provenance_from_row(row, 0),
    )?;
    Ok(values)
}

pub fn activation_route_provenance_for_sandboxes(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<HashMap<String, Result<ActivationRouteProvenance>>> {
    if sandbox_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT id,
                space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&query)?;
    let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
        Ok((
            row.get::<_, String>(0)?,
            activation_route_provenance_from_row(row, 1),
        ))
    })?;
    rows.collect()
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

pub fn execution_delete_receipt_covers(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let minimum_deleted_at =
        now_epoch_i64()?.saturating_sub(EXECUTION_DELETE_RECEIPT_RETENTION_SECS);
    conn.query_row(
        "SELECT execution_generation >= ?1
         FROM sandbox_execution_delete_receipts
         WHERE sandbox_id = ?2
           AND deleted_at >= ?3",
        params![execution_generation.get(), sandbox_id, minimum_deleted_at],
        |row| row.get(0),
    )
    .optional()
    .map(|covers| covers.unwrap_or(false))
}

pub fn clear_runtime_fields(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uid = NULL, slot = NULL, guest_cid = NULL, pid = NULL,
         spaced_host_pid = NULL, netgtw_pid = NULL, vsock_uds_path = NULL, api_socket_path = NULL,
         space_api_route_generation = NULL, space_api_route_node_id = NULL,
         space_api_route_execution_generation = NULL, space_api_route_pod_uid = NULL,
         space_api_route_substrate_kind = NULL, space_api_route_activation_attempt_id = NULL,
         space_api_route_identity_provenance = NULL,
         spaced_endpoint_proof_json = NULL,
         guest_uptime_secs = NULL, updated_at = ?1 WHERE id = ?2",
        params![now()?, id],
    )?;
    Ok(())
}

pub fn sandbox_exists(conn: &Connection, id: &str) -> Result<bool> {
    conn.query_row(
        sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = ?1)"),
        [id],
        |row| row.get(0),
    )
}

const GET_SANDBOX_SQL: &str =
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes WHERE id = ?1";

pub fn get_sandbox(conn: &Connection, id: &str) -> Result<Option<SandboxRow>> {
    // Every guest file operation and process start looks its sandbox up
    // several times under the one database connection; parsing this wide
    // SELECT each time dominated that work.
    let mut stmt = conn.prepare_cached(GET_SANDBOX_SQL)?;
    let mut rows = stmt.query_map(params![id], row_to_sandbox)?;
    rows.next().transpose()
}

/// Select at most two identities: two matches prove ambiguity without decoding
/// unrelated or ambiguous sandbox metadata. Callers decode only a unique match.
pub fn find_by_prefix(conn: &Connection, prefix: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT id FROM sandboxes ORDER BY id")?;
    let mut rows = stmt.query([])?;
    let mut matches = Vec::new();
    while let Some(row) = rows.next()? {
        let id = row.get::<_, String>(0)?;
        if pplx_space_common_core::ids::sandbox_id_matches(&id, prefix) {
            matches.push(id);
            if matches.len() == 2 {
                break;
            }
        }
    }
    Ok(matches)
}

const LIST_SANDBOXES_SQL: &str =
    "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes ORDER BY created_at";

pub fn list_sandboxes(conn: &Connection) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
    let rows = stmt.query_map([], row_to_sandbox)?;
    rows.collect()
}

pub fn list_sandboxes_for_recovery(conn: &Connection) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
    let mut rows = stmt.query([])?;
    let mut valid = Vec::new();
    while let Some(row) = rows.next()? {
        match row_to_sandbox(row) {
            Ok(row) => valid.push(row),
            Err(error) if is_row_decode_error(&error) => tracing::warn!(
                sandbox_id = ?row.get_ref(0)?,
                error = %describe_error(&error),
                "skipping undecodable sandbox row during local recovery"
            ),
            Err(error) => return Err(error),
        }
    }
    Ok(valid)
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

/// A finite sweep: concurrent inserts are visited on the next sweep instead of
/// keeping this one open forever and starving previously deferred candidates.
/// New candidates wait for a later sweep to reach their rowid; admission pressure
/// and recovery failures can delay progress further. No recovery deadline is promised.
#[derive(Debug)]
pub(crate) struct PeriodicLifecycleRecoveryScan {
    next: i64,
    through: Option<i64>,
}

impl Default for PeriodicLifecycleRecoveryScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

/// Bounded discovery, not a complete inventory or authority to mutate a row.
/// Examine at most 128 raw rowids and return at most eight successfully decoded
/// candidates per tick. Failed decodes do not fill candidate slots, so eligible
/// malformed rows can cause up to 128 individual full-row decode attempts.
/// Advance past malformed/ineligible rows as well as candidates, so a retained
/// or busy first page cannot starve the rest. Rowid is only a scan position;
/// each consumer must revalidate the execution witness under its own fences.
pub(crate) fn periodic_lifecycle_recovery_batch(
    conn: &Connection,
    scan: &mut PeriodicLifecycleRecoveryScan,
) -> Result<Vec<SandboxRow>> {
    if scan.through.is_none() {
        scan.through = conn.query_row(sql!("SELECT MAX(rowid) FROM sandboxes"), [], |row| {
            row.get(0)
        })?;
    }
    let Some(through) = scan.through else {
        return Ok(Vec::new());
    };
    let mut statement = conn.prepare(sql!(
        "SELECT rowid, id, state, current_local_artifact_id FROM sandboxes
         WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128"
    ))?;
    let mut rows = statement.query([scan.next, through])?;
    let mut candidates = Vec::new();
    for _ in 0..128 {
        let Some(row) = rows.next()? else {
            *scan = PeriodicLifecycleRecoveryScan::default();
            break;
        };
        let rowid: i64 = row.get(0)?;
        if rowid == through {
            *scan = PeriodicLifecycleRecoveryScan::default();
        } else {
            // The upper witness is greater, so this cannot overflow.
            scan.next = rowid + 1;
        }
        let (id, state) = match (row.get::<_, String>(1), row.get::<_, String>(2)) {
            (Ok(id), Ok(state)) => (id, state),
            (Err(error), _) | (_, Err(error)) => {
                tracing::warn!(
                    rowid,
                    error = %describe_error(&error),
                    "skipping undecodable sandbox key/state during periodic recovery"
                );
                continue;
            }
        };
        let eligible = match state.parse::<SandboxState>() {
            Ok(SandboxState::Paused | SandboxState::Resuming | SandboxState::Checkpointing) => true,
            Ok(SandboxState::Suspending) => {
                matches!(row.get_ref(3)?, rusqlite::types::ValueRef::Null)
            }
            _ => false,
        };
        if !eligible {
            continue;
        }
        match get_sandbox(conn, &id) {
            Ok(Some(row)) => candidates.push(row),
            Ok(None) => {}
            Err(error) => tracing::warn!(
                sandbox_id = %id,
                error = %describe_error(&error),
                "skipping undecodable sandbox row during periodic recovery"
            ),
        }
        if candidates.len() == 8 {
            break;
        }
    }
    Ok(candidates)
}

pub fn list_sandbox_id_states(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = conn.prepare("SELECT id, state FROM sandboxes ORDER BY created_at")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect()
}

/// Host-control net-gateway auth lookup: rows whose registered
/// `(netgtw_pid, uid)` matches the caller's peer credentials.
///
/// Restricted to states where a live gateway can legitimately exist
/// (`SandboxState::can_authorize_net_gateway`): launch-failure and other
/// terminal rows retain the `(uid, netgtw_pid)` of dead processes, and since
/// uids are slot-derived and pids recycle, an unfiltered match would let a
/// stale row shadow — or be served in place of — a later sandbox's gateway.
/// The state list below must stay in sync with that predicate; the
/// per-state lookup test cross-checks them.
pub fn list_by_netgtw_pid_uid(
    conn: &Connection,
    netgtw_pid: u32,
    uid: u32,
) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes
         WHERE netgtw_pid = ?1 AND uid = ?2
           AND state IN ('starting', 'started', 'stopping', 'pausing', 'paused',
                         'resuming', 'checkpointing', 'suspending', 'restoring')
         LIMIT 2",
    )?;
    let rows = stmt.query_map(params![netgtw_pid, uid], row_to_sandbox)?;
    rows.collect()
}

/// Host-control spaced-host auth lookup: rows whose registered
/// `(spaced_host_pid, uid)` matches the caller's peer credentials.
pub fn list_by_spaced_host_pid_uid(
    conn: &Connection,
    spaced_host_pid: u32,
    uid: u32,
) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id
         FROM sandboxes
         WHERE spaced_host_pid = ?1 AND uid = ?2
         LIMIT 2",
    )?;
    let sandbox_ids = stmt
        .query_map(params![spaced_host_pid, uid], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>>>()?;
    drop(stmt);

    sandbox_ids
        .into_iter()
        .map(|sandbox_id| {
            get_sandbox(conn, &sandbox_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
        })
        .collect()
}

pub fn list_by_state(conn: &Connection, state: SandboxState) -> Result<Vec<SandboxRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id
         FROM sandboxes WHERE state = ?1",
    )?;
    let rows = stmt.query_map(params![state.as_str()], row_to_sandbox)?;
    rows.collect()
}

pub fn delete_sandbox(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    delete_sandbox_child_rows(&tx, id)?;
    tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![id])
        .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
    tx.commit()?;
    Ok(())
}

fn inventory_deletion_witness_matches(current: &SandboxRow, observed: &SandboxRow) -> bool {
    current.id == observed.id
        && current.created_at == observed.created_at
        && current.updated_at == observed.updated_at
        && current.state == observed.state
        && current.restore_operation == observed.restore_operation
        && current.failure_reason == observed.failure_reason
        && current.uid == observed.uid
        && current.slot == observed.slot
        && current.guest_cid == observed.guest_cid
        && current.pid == observed.pid
        && current.spaced_host_pid == observed.spaced_host_pid
        && current.netgtw_pid == observed.netgtw_pid
        && current.vsock_uds_path == observed.vsock_uds_path
        && current.api_socket_path == observed.api_socket_path
        && current.cleanup_completion == observed.cleanup_completion
        && current.cleanup_keep_filesystem == observed.cleanup_keep_filesystem
        && current.current_local_artifact_id == observed.current_local_artifact_id
        && current.current_local_artifact_kind == observed.current_local_artifact_kind
        && current.current_local_artifact_origin == observed.current_local_artifact_origin
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryOwnershipReason {
    DurableCleanupEpisode,
    RowCleanupIntent,
    MalformedCleanupIntent,
    LifecycleOwned,
    RestoreTransferScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryStaleRowMutation {
    Removed,
    Missing,
    WitnessChanged,
    Deferred(InventoryOwnershipReason),
}

/// Delete a row selected by inventory only if its lifecycle ownership witness
/// is unchanged and no durable lifecycle owner exists. The immediate
/// transaction makes the ownership reads and manual child-row cascade
/// indivisible from concurrent lifecycle writers.
pub(crate) fn delete_sandbox_if_inventory_witness_matches(
    conn: &Connection,
    observed: &SandboxRow,
) -> Result<InventoryStaleRowMutation> {
    let tx = immediate_transaction(conn)?;
    let Some(current) = get_sandbox(&tx, &observed.id)? else {
        return Ok(InventoryStaleRowMutation::Missing);
    };
    if !inventory_deletion_witness_matches(&current, observed) {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    if cleanup_episode(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::DurableCleanupEpisode,
        ));
    }
    match (current.cleanup_completion, current.cleanup_keep_filesystem) {
        (Some(_), Some(_)) => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::RowCleanupIntent,
            ));
        }
        (None, None) => {}
        _ => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::MalformedCleanupIntent,
            ));
        }
    }
    if matches!(
        current.state,
        SandboxState::Stopping | SandboxState::Deleting
    ) {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::LifecycleOwned,
        ));
    }
    if restore_transfer_scope(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::RestoreTransferScope,
        ));
    }
    let execution_generation = current.space_api_route_identity.execution_generation;
    delete_sandbox_child_rows(&tx, &observed.id)?;
    let deleted = tx
        .execute("DELETE FROM sandboxes WHERE id = ?1", params![&observed.id])
        .map_err(crate::auto_lifecycle_delete_receipts::db::map_pending_receipt_delete_error)?;
    if deleted != 1 {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    record_execution_delete_receipt(&tx, &observed.id, execution_generation)?;
    tx.commit()?;
    Ok(InventoryStaleRowMutation::Removed)
}

pub fn sandbox_has_runtime_fields(row: &SandboxRow) -> bool {
    row.uid.is_some()
        || row.slot.is_some()
        || row.guest_cid.is_some()
        || row.pid.is_some()
        || row.spaced_host_pid.is_some()
        || row.netgtw_pid.is_some()
        || row.vsock_uds_path.is_some()
        || row.api_socket_path.is_some()
}

pub fn sandbox_has_full_suspend_artifact_pin(row: &SandboxRow) -> bool {
    row.current_local_artifact_id.is_some()
        && row.current_local_artifact_kind == Some(ArtifactKind::Full)
        && row.current_local_artifact_origin == Some(ArtifactOrigin::Suspend)
}

pub fn sandbox_has_registered_full_suspend_artifact(
    conn: &Connection,
    row: &SandboxRow,
) -> Result<bool> {
    let Some(local_artifact_id) = row.current_local_artifact_id else {
        return Ok(false);
    };
    if row.current_local_artifact_kind != Some(ArtifactKind::Full)
        || row.current_local_artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Ok(false);
    }
    Ok(
        get_local_artifact(conn, local_artifact_id)?.is_some_and(|artifact| {
            artifact.sandbox_id == row.id
                && artifact.artifact_kind == ArtifactKind::Full
                && artifact.artifact_origin == ArtifactOrigin::Suspend
        }),
    )
}

pub fn cleanup_intent_for_startup_recovery(row: &SandboxRow) -> Option<(bool, CleanupCompletion)> {
    if let (Some(keep_filesystem), Some(completion)) =
        (row.cleanup_keep_filesystem, row.cleanup_completion)
        && completion.owns_row_intent(row)
    {
        return Some((keep_filesystem, completion));
    }

    match row.state {
        // Transient cleanup states represent an unfinished lifecycle operation.
        // Finish them even if runtime fields were already cleared when the
        // target terminal state is self-contained. Suspending is stricter:
        // recovery may only mark it suspended once a full suspend artifact is
        // already pinned; otherwise service-level startup recovery must either
        // finish a complete staged publish or quarantine the row.
        SandboxState::Stopping => Some((true, CleanupCompletion::MarkStopped)),
        SandboxState::Suspending if sandbox_has_full_suspend_artifact_pin(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::Suspending => None,
        SandboxState::Deleting => Some((false, CleanupCompletion::DeleteRow)),
        SandboxState::CleanupRequired => Some((true, CleanupCompletion::Quarantine)),
        SandboxState::Starting if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkStopped))
        }
        SandboxState::Restoring if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::MarkSuspended))
        }
        SandboxState::CleanupFailed if sandbox_has_runtime_fields(row) => {
            Some((true, CleanupCompletion::Quarantine))
        }
        _ => None,
    }
}

/// On startup: reconcile interrupted DB states to a safe local outcome.
/// Also check Started sandboxes for dead PIDs.
pub fn recover_interrupted_startup_states(
    conn: &Connection,
) -> std::result::Result<Vec<StartupRecoveryRecord>, RecoverStartupStatesError> {
    let rows = list_sandboxes_for_recovery(conn)?;
    let mut recovered = Vec::new();

    for row in rows {
        if row.state.is_transient() {
            let episode = match cleanup_episode(conn, &row.id) {
                Ok(episode) => episode,
                Err(error) if is_row_decode_error(&error) => {
                    tracing::warn!(sandbox_id = %row.id, error = %describe_error(&error), "retaining malformed startup cleanup ownership; operator repair required");
                    recovered.push(StartupRecoveryRecord {
                        id: row.id,
                        previous_state: row.state,
                        outcome: StartupRecoveryOutcome::Deferred,
                    });
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(episode) = episode.filter(|episode| {
                episode.requested_at.is_none() && episode.activation_state == row.state
            }) {
                // Hand off the exact episode before startup can admit work.
                // Generic lifecycle rewriting loses its receipt; merely
                // deferring leaves no periodic owner if later recovery fails.
                let reason = match row.state {
                    SandboxState::Creating => SandboxQuarantineReasonCode::CreateInterrupted,
                    SandboxState::Starting => {
                        SandboxQuarantineReasonCode::ColdActivationInterrupted
                    }
                    SandboxState::Restoring => {
                        SandboxQuarantineReasonCode::RestoreActivationInterrupted
                    }
                    state => {
                        return Err(RecoverStartupStatesError::UnsupportedActivationState(state));
                    }
                };
                let requested = request_cleanup_episode(
                    conn,
                    &row.id,
                    episode.id,
                    reason,
                    "startup recovery resumed interrupted activation cleanup",
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: match requested {
                        CleanupEpisodeRequestOutcome::Requested
                        | CleanupEpisodeRequestOutcome::AlreadyRequested => {
                            StartupRecoveryOutcome::CleanupRequired {
                                keep_filesystem: episode.keep_filesystem,
                                completion: episode.completion,
                            }
                        }
                        CleanupEpisodeRequestOutcome::Superseded => {
                            StartupRecoveryOutcome::Deferred
                        }
                    },
                });
                continue;
            }
            if row.state == SandboxState::Creating {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
                continue;
            }

            if matches!(
                row.state,
                SandboxState::Pausing | SandboxState::Resuming | SandboxState::Checkpointing
            ) || (row.state == SandboxState::Suspending
                && !sandbox_has_full_suspend_artifact_pin(&row))
                || sandbox_is_retryable_interrupted_restore(&row)
            {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            } else if let Some((keep_filesystem, completion)) =
                cleanup_intent_for_startup_recovery(&row)
            {
                mark_cleanup_required(
                    conn,
                    &row.id,
                    keep_filesystem,
                    completion,
                    &format!(
                        "startup recovery: sandbox was left in transient state '{}'",
                        row.state
                    ),
                )?;
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::CleanupRequired {
                        keep_filesystem,
                        completion,
                    },
                });
            } else if row.state == SandboxState::Restoring {
                let outcome = update_state(conn, &row.id, SandboxState::Suspended)?;
                if !outcome.is_applied() {
                    continue;
                }
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Suspended),
                });
            } else {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::Deferred,
                });
            }
            continue;
        }

        if row.state == SandboxState::Started
            && !row
                .pid
                .is_some_and(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists())
        {
            // The state-only Firecracker probe cannot reconstruct trusted
            // process credentials. Preserve all resources until the missing
            // runtime identity can be explained; an older artifact is not
            // evidence of the customer's current state.
            if settle_startup_resident(
                conn,
                &row,
                StartupResidentSettlement::Quarantined {
                    reason_code: SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                    message: "startup recovery: started sandbox has no live registered VM pid",
                },
            )? {
                recovered.push(StartupRecoveryRecord {
                    id: row.id,
                    previous_state: row.state,
                    outcome: StartupRecoveryOutcome::State(SandboxState::Quarantined),
                });
            }
        }
    }

    Ok(recovered)
}

#[derive(Debug, thiserror::Error)]
pub enum RecoverStartupStatesError {
    #[error("startup state recovery database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("failed to hand interrupted activation to cleanup: {0}")]
    CleanupRequest(#[from] RequestCleanupEpisodeError),
    #[error("armed cleanup episode has unsupported activation state {0}")]
    UnsupportedActivationState(SandboxState),
}

/// Update the user field on a sandbox.
pub fn update_user(conn: &Connection, id: &str, user: &SandboxOwner) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET user = ?1, updated_at = ?2 WHERE id = ?3",
        params![user.as_str(), now()?, id],
    )?;
    Ok(())
}

pub fn update_process_env(
    conn: &Connection,
    id: &str,
    process_env: &BTreeMap<String, String>,
) -> Result<()> {
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes SET process_env_json = ?1, updated_at = ?2 WHERE id = ?3",
        params![process_env_json, now()?, id],
    )?;
    Ok(())
}

pub fn set_sandbox_owner_if_current(
    conn: &Connection,
    id: &str,
    expected_current_owner: &SandboxOwner,
    owner: &SandboxOwner,
) -> Result<Option<SandboxRow>> {
    let tx = immediate_transaction(conn)?;
    let Some(previous) = get_sandbox(&tx, id)? else {
        tx.commit()?;
        return Ok(None);
    };
    if previous.user.as_str() == owner.as_str() {
        tx.commit()?;
        return Ok(Some(previous));
    }
    if previous.user.as_str() != expected_current_owner.as_str() {
        tx.commit()?;
        return Ok(None);
    }

    let changed = tx.execute(
        "UPDATE sandboxes
         SET user = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND user = ?4",
        params![owner.as_str(), now()?, id, expected_current_owner.as_str()],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(previous))
}

/// Called inside activation admission's transaction, before runtime launch.
/// Snapshot policy is historical; a cluster activation carries current intent.
/// Preserve local overrides and unchanged clocks, including reconnect floors.
fn install_activation_auto_lifecycle(
    conn: &Connection,
    id: &str,
    policy: &AutoLifecyclePolicy,
) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if sandbox.auto_lifecycle_authority == PolicyAuthority::Local {
        return Ok(());
    }
    let runtime_state = if sandbox.auto_lifecycle.normalized() == *policy {
        sandbox.auto_lifecycle_state
    } else {
        AutoLifecycleRuntimeState::default()
    };
    update_auto_lifecycle_with_state(conn, id, policy, &runtime_state, PolicyAuthority::Cluster)
}

pub fn update_guest_uptime(conn: &Connection, id: &str, secs: u64) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET guest_uptime_secs = ?1 WHERE id = ?2",
        params![secs as i64, id],
    )?;
    Ok(())
}

/// Persist the net-gateway `(uid, pid)` registration that host-control auth
/// matches against. Launch paths rely on this being durable before the
/// gateway arm returns, so an UPDATE that matched no row (the sandbox was
/// deleted by concurrent cleanup mid-launch) is an error: silently returning
/// `Ok` would leave the just-launched gateway permanently unauthorized
/// without tripping the caller's launch-failure cleanup.
pub fn update_netgtw_pid(conn: &Connection, id: &str, uid: u32, netgtw_pid: u32) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes SET uid = ?1, netgtw_pid = ?2, updated_at = ?3 WHERE id = ?4",
        params![uid, netgtw_pid, now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Commit a watchdog replacement only while the persisted endpoint authority
/// still matches the observation that initiated the restart. A concurrent
/// caller may already have committed the same live replacement; confirming
/// that PID is idempotent, while any different PID remains protected.
pub fn replace_netgtw_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_netgtw_pid: Option<u32>,
    netgtw_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET netgtw_pid = ?1, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (netgtw_pid IS ?5 OR netgtw_pid = ?1)",
        params![netgtw_pid, now()?, id, uid, expected_netgtw_pid],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Persist the replacement spaced-host PID that endpoint authentication
/// matches against. The owner UID is already part of the sandbox runtime row
/// and is deliberately included in the guard so a stale watchdog observation
/// cannot transfer endpoint authority to a replacement after ownership moved.
/// A concurrent caller may confirm the same replacement PID idempotently.
pub fn update_spaced_host_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_spaced_host_pid: Option<u32>,
    spaced_host_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_host_pid = ?1, spaced_endpoint_proof_json = NULL, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (spaced_host_pid IS ?5 OR spaced_host_pid = ?1)",
        params![spaced_host_pid, now()?, id, uid, expected_spaced_host_pid],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Memoize a proof for an execution that predates proof capture.
///
/// Every runtime identity dimension participates in the compare-and-set so a
/// bridge request racing activation replacement cannot attach its observation
/// to the replacement execution.
pub fn persist_spaced_endpoint_proof_if_missing(
    conn: &Connection,
    runtime: &crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
    proof: &SpacedEndpointProof,
) -> Result<bool> {
    proof
        .validate_runtime(runtime)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let proof_json = serde_json::to_string(proof)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_endpoint_proof_json = ?1, updated_at = ?2
         WHERE id = ?3
           AND spaced_endpoint_proof_json IS NULL
           AND pid = ?4
           AND spaced_host_pid = ?5
           AND uid = ?6
           AND guest_cid = ?7
           AND vsock_uds_path = ?8
           AND space_api_route_execution_generation = ?9
           AND space_api_route_activation_attempt_id = ?10",
        params![
            proof_json,
            now()?,
            runtime.sandbox_id.to_string(),
            runtime.runtime_pid.get().get(),
            runtime.spaced_host_pid.get().get(),
            runtime.sandbox_uid.get(),
            runtime.guest_cid.get().get(),
            runtime.vsock_path.to_string_lossy(),
            runtime.execution_generation.get(),
            runtime.activation_attempt_id.to_string(),
        ],
    )?;
    Ok(changed == 1)
}

/// Persist a net-gateway stats observation, if it actually advances the
/// counters.
///
/// The counters only ever climb (`MAX`), so an observation that carries no
/// new bytes has nothing to persist — and must not be written anyway. The
/// watchdog re-probes every sandbox with a live net-gateway every 30 s for as
/// long as its runtime is resident, and an unconditional write makes each of
/// those probes bump `updated_at` on a row where nothing changed. That column
/// is the quiescence clock the cluster-deleted eviction reads, so an idle
/// sandbox's purely informational probe would hold the row inside the window
/// forever. Same shape as [`mark_network_policy_applied`]: write only when
/// the observation is news.
pub fn update_network_counters(
    conn: &Connection,
    id: &str,
    network_tx_bytes: u64,
    network_rx_bytes: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes
         SET network_tx_bytes = MAX(network_tx_bytes, ?1),
             network_rx_bytes = MAX(network_rx_bytes, ?2),
             updated_at = ?3
         WHERE id = ?4
           AND (?1 > network_tx_bytes OR ?2 > network_rx_bytes)",
        params![
            u64_to_sql_i64(network_tx_bytes),
            u64_to_sql_i64(network_rx_bytes),
            now()?,
            id
        ],
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn create_test_schema(conn: &mut Connection) {
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
#[cfg(test)]
fn test_schema_image() -> &'static [u8] {
    static IMAGE: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    IMAGE.get_or_init(|| {
        let cache = test_schema_image_cache::SchemaImageCache::for_current_exe();
        if let Some(image) = cache.as_ref().and_then(|cache| cache.read()) {
            return image;
        }
        let mut conn = Connection::open_in_memory().unwrap();
        create_test_schema(&mut conn);
        let image = conn.serialize(rusqlite::MAIN_DB).unwrap().to_vec();
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
#[cfg(test)]
pub(crate) fn create_test_schema_on_disk(db_path: &std::path::Path) {
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
#[cfg(test)]
pub(crate) fn test_conn() -> Connection {
    let image = test_schema_image();
    let mut conn = Connection::open_in_memory().unwrap();
    conn.deserialize_read_exact(rusqlite::MAIN_DB, image, image.len(), false)
        .unwrap();
    conn
}

/// Seed pre-validation lifecycle evidence without weakening subsequent writes.
/// Transactional DDL also restores the guards if the seed closure panics.
///
/// The validators are read back from this connection rather than reconstructed
/// from a pinned migration, so a later migration that replaces them is restored
/// as it actually exists. A rename is a loud failure here, not a silent
/// reinstatement of a superseded vocabulary in one test's database.
#[cfg(test)]
pub(crate) fn seed_historical_lifecycle_rows<T>(
    conn: &Connection,
    seed: impl FnOnce(&Connection) -> T,
) -> T {
    let tx = conn.unchecked_transaction().unwrap();
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
        .collect::<Result<_>>()
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
