//! Shared seeds and whole-database diffs for the node-local SQLite
//! `sandboxes.state` writer characterizations.
//!
//! A snapshot keys every row of every table by its primary key, so a diff can
//! tell an updated row from a replaced one, and an expectation can name exactly which
//! columns of which rows a writer may change. Anything the expectation does not
//! name must be identical before and after.

use std::collections::BTreeMap;

use crate::db::Connection;
use pplx_space_common_core::types::{
    Arch, AutoLifecyclePolicy, ExecutionGeneration, LinuxPid, LinuxUid, LocalArtifactId,
    RuntimeKind, SandboxActivationAttemptId, SandboxCleanupEpisodeId, SandboxId, SandboxOwner,
    SandboxQuarantineReasonCode, SandboxRuntimeLabel, VsockCid,
};
use rusqlite::params;
use rusqlite::types::Value;

use super::{TestSandboxInsert, insert_test_sandbox};
use crate::db::{
    ActivationKind, ActivationRouteAdmission, CleanupCompletion, CleanupEpisode,
    CleanupEpisodeSpec, Edge, InsertSandboxParams, ORPHANED_SANDBOX_OWNER,
    ORPHANED_SANDBOX_TEMPLATE, PolicyAuthority, StateWriteOutcome,
    begin_activation_with_route_evidence, begin_suspend, complete_row_intent_cleanup_for_job,
    ensure_teardown_suspend_operation, immediate_transaction,
    insert_sandbox_with_cleanup_episode_and_route_evidence, set_cleanup_intent, update_state_from,
    update_state_superseding_activation_in_transaction,
};
use crate::state::SandboxState;

pub type Row = BTreeMap<String, Value>;

/// Every row of every table, keyed by table name and primary key (`rowid`
/// for a table without one), so a row deleted and re-inserted under a new key
/// reads as a deletion and an insertion even when SQLite reuses its `rowid`.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot(BTreeMap<String, BTreeMap<String, Row>>);

pub fn snapshot(conn: &Connection) -> Snapshot {
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    Snapshot(
        tables
            .into_iter()
            .map(|table| {
                let mut key: Vec<(i64, String)> = conn
                    .prepare(&format!(
                        "SELECT pk, name FROM pragma_table_info('{table}') WHERE pk > 0"
                    ))
                    .unwrap()
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                key.sort();
                let mut statement = conn
                    .prepare(&format!("SELECT rowid, * FROM \"{table}\""))
                    .unwrap();
                let names: Vec<String> = statement
                    .column_names()
                    .into_iter()
                    .skip(1)
                    .map(str::to_owned)
                    .collect();
                let rows = statement
                    .query_map([], |row| {
                        let rowid: i64 = row.get(0)?;
                        let values = names
                            .iter()
                            .enumerate()
                            .map(|(index, name)| {
                                Ok((name.clone(), row.get::<_, Value>(index + 1)?))
                            })
                            .collect::<rusqlite::Result<Row>>()?;
                        let identity = if key.is_empty() {
                            format!("rowid {rowid}")
                        } else {
                            format!(
                                "{:?}",
                                key.iter()
                                    .map(|(_, name)| &values[name])
                                    .collect::<Vec<_>>()
                            )
                        };
                        Ok((identity, values))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<BTreeMap<_, _>>>()
                    .unwrap();
                (table, rows)
            })
            .collect(),
    )
}

/// What one column of an expected row must hold after the write.
#[derive(Clone, Debug)]
pub enum Cell {
    Is(Value),
    /// An epoch-seconds timestamp written by this write: text holding a value
    /// no earlier than the test's start. Seeds backdate every timestamp, so a
    /// fresh write is always visible as a change.
    NowText,
    /// As [`Cell::NowText`], stored as an integer.
    NowInteger,
    /// A value this write generated, such as a fresh UUID: any non-null value
    /// that differs from the previous one.
    Fresh,
}

pub fn text(value: impl Into<String>) -> Cell {
    Cell::Is(Value::Text(value.into()))
}

pub fn int(value: i64) -> Cell {
    Cell::Is(Value::Integer(value))
}

pub const NULL: Cell = Cell::Is(Value::Null);

#[derive(Clone, Debug)]
enum Change {
    Updated {
        table: &'static str,
        key: (&'static str, Value),
        cells: Vec<(&'static str, Cell)>,
    },
    Inserted {
        table: &'static str,
        cells: Vec<(&'static str, Cell)>,
    },
    Deleted {
        table: &'static str,
        key: (&'static str, Value),
    },
}

/// The exact set of row changes one write may make.
#[derive(Clone, Debug, Default)]
pub struct ExpectedDiff {
    changes: Vec<Change>,
}

impl ExpectedDiff {
    pub fn new() -> Self {
        Self::default()
    }

    /// The row of `table` whose `key.0` column equals `key.1` changes exactly
    /// the listed columns.
    pub fn updated(
        mut self,
        table: &'static str,
        key: (&'static str, &str),
        cells: impl IntoIterator<Item = (&'static str, Cell)>,
    ) -> Self {
        self.changes.push(Change::Updated {
            table,
            key: (key.0, Value::Text(key.1.to_owned())),
            cells: cells.into_iter().collect(),
        });
        self
    }

    /// The `sandboxes` row `id` changes exactly the listed columns.
    pub fn sandbox(self, id: &str, cells: impl IntoIterator<Item = (&'static str, Cell)>) -> Self {
        self.updated("sandboxes", ("id", id), cells)
    }

    /// One new row of `table`; every column must be listed.
    pub fn inserted(
        mut self,
        table: &'static str,
        cells: impl IntoIterator<Item = (&'static str, Cell)>,
    ) -> Self {
        self.changes.push(Change::Inserted {
            table,
            cells: cells.into_iter().collect(),
        });
        self
    }

    pub fn deleted(mut self, table: &'static str, key: (&'static str, &str)) -> Self {
        self.changes.push(Change::Deleted {
            table,
            key: (key.0, Value::Text(key.1.to_owned())),
        });
        self
    }
}

#[derive(Debug)]
struct ActualChange<'a> {
    table: &'a str,
    before: Option<&'a Row>,
    after: Option<&'a Row>,
}

fn actual_changes<'a>(before: &'a Snapshot, after: &'a Snapshot) -> Vec<ActualChange<'a>> {
    static NO_ROWS: BTreeMap<String, Row> = BTreeMap::new();
    let mut changes = Vec::new();
    for (table, after_rows) in &after.0 {
        let before_rows = before.0.get(table).unwrap_or(&NO_ROWS);
        for (key, row) in after_rows {
            match before_rows.get(key) {
                Some(previous) if previous == row => {}
                previous => changes.push(ActualChange {
                    table,
                    before: previous,
                    after: Some(row),
                }),
            }
        }
        for (key, row) in before_rows {
            if !after_rows.contains_key(key) {
                changes.push(ActualChange {
                    table,
                    before: Some(row),
                    after: None,
                });
            }
        }
    }
    changes
}

fn cell_matches(cell: &Cell, previous: Option<&Value>, actual: &Value, start: i64) -> bool {
    let fresh_timestamp = |value: Option<i64>| value.is_some_and(|value| value >= start);
    match cell {
        Cell::Is(expected) => expected == actual,
        Cell::NowText => {
            matches!(actual, Value::Text(value) if fresh_timestamp(value.parse().ok()))
        }
        Cell::NowInteger => {
            matches!(actual, Value::Integer(value) if fresh_timestamp(Some(*value)))
        }
        Cell::Fresh => *actual != Value::Null && previous != Some(actual),
    }
}

/// Columns that differ between two versions of one row, with the cells the
/// expectation lists, as a readable mismatch report or `None` when they agree.
fn row_mismatch(
    before: Option<&Row>,
    after: &Row,
    cells: &[(&'static str, Cell)],
    start: i64,
) -> Option<String> {
    let mut problems = Vec::new();
    let listed: BTreeMap<&str, &Cell> = cells.iter().map(|(name, cell)| (*name, cell)).collect();
    for (name, value) in after {
        let previous = before.and_then(|row| row.get(name));
        match listed.get(name.as_str()) {
            Some(cell) => {
                if !cell_matches(cell, previous, value, start) {
                    problems.push(format!("{name}: expected {cell:?}, found {value:?}"));
                } else if before.is_some() && previous == Some(value) {
                    problems.push(format!("{name}: listed as changed but kept {value:?}"));
                }
            }
            None if before.is_none() => problems.push(format!("{name}: unlisted {value:?}")),
            None if previous != Some(value) => {
                problems.push(format!(
                    "{name}: unexpected change {previous:?} -> {value:?}"
                ));
            }
            None => {}
        }
    }
    for name in listed.keys() {
        if !after.contains_key(*name) {
            problems.push(format!("{name}: no such column"));
        }
    }
    (!problems.is_empty()).then(|| problems.join("; "))
}

/// Seconds since the epoch, captured before a write so [`Cell::NowText`] can
/// tell a fresh timestamp from a seeded one.
pub fn test_start() -> i64 {
    jiff::Timestamp::now().as_second()
}

/// Assert that `after` differs from `before` by exactly `expected`.
pub fn assert_diff(
    case: &str,
    before: &Snapshot,
    after: &Snapshot,
    start: i64,
    expected: &ExpectedDiff,
) {
    let actual = actual_changes(before, after);
    let mut unmatched_actual: Vec<usize> = (0..actual.len()).collect();
    let mut problems = Vec::new();
    for change in &expected.changes {
        let found = unmatched_actual.iter().position(|index| {
            let candidate = &actual[*index];
            match change {
                Change::Updated { table, key, .. } => {
                    candidate.table == *table
                        && candidate.after.is_some()
                        && candidate
                            .before
                            .is_some_and(|row| row.get(key.0) == Some(&key.1))
                }
                Change::Inserted { table, .. } => {
                    candidate.table == *table && candidate.before.is_none()
                }
                Change::Deleted { table, key } => {
                    candidate.table == *table
                        && candidate.after.is_none()
                        && candidate
                            .before
                            .is_some_and(|row| row.get(key.0) == Some(&key.1))
                }
            }
        });
        let Some(position) = found else {
            problems.push(format!("missing change {change:?}"));
            continue;
        };
        let candidate = &actual[unmatched_actual.remove(position)];
        let mismatch = match change {
            Change::Updated { cells, .. } | Change::Inserted { cells, .. } => {
                row_mismatch(candidate.before, candidate.after.unwrap(), cells, start)
            }
            Change::Deleted { .. } => None,
        };
        if let Some(mismatch) = mismatch {
            problems.push(format!("{}: {mismatch}", candidate.table));
        }
    }
    for index in unmatched_actual {
        problems.push(format!("unexpected change {:?}", actual[index]));
    }
    assert!(
        problems.is_empty(),
        "{case}: the write's row diff differs from its accepted shape:\n  {}",
        problems.join("\n  ")
    );
}

/// Assert that a refused write left every row of every table as it was.
pub fn assert_unchanged(case: &str, before: &Snapshot, conn: &Connection) {
    let after = snapshot(conn);
    let changed = actual_changes(before, &after);
    assert!(
        changed.is_empty(),
        "{case}: a refused write must leave every row unchanged; changed: {changed:?}"
    );
}

/// Insert a plain sandbox row in `state`.
pub fn insert_row(conn: &Connection, state: SandboxState) -> String {
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_test_sandbox(
        conn,
        TestSandboxInsert::new(
            &id,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            state,
        ),
    );
    id
}

/// Move a seeded row to `state` without any writer's side effects. A
/// `restoring` row carries a remote restore's three restore columns, which the
/// table's `CHECK` ties to that state.
pub fn force_state(conn: &Connection, id: &str, state: SandboxState) {
    let restoring = state == SandboxState::Restoring;
    conn.execute(
        "UPDATE sandboxes
            SET state = ?1,
                restore_origin = CASE WHEN ?2 THEN 'remote' END,
                restore_phase = CASE WHEN ?2 THEN 'metadata_staging' END,
                restore_storage_uri = CASE WHEN ?2 THEN ?4 END
          WHERE id = ?3",
        params![state.as_str(), restoring, id, RESTORE_STORAGE_URI],
    )
    .unwrap();
}

/// Write `edge` with `update_state_from`, which must apply.
pub fn apply_edge(conn: &Connection, id: &str, edge: &'static Edge) {
    let outcome = update_state_from(conn, id, edge).unwrap();
    assert!(
        matches!(outcome, StateWriteOutcome::Applied { .. }),
        "{}: {outcome:?}",
        edge.name()
    );
}

/// A non-blocking Stop's admission transaction, as the cleanup fence runs it:
/// supersede any activation, write `stopping` and persist the `mark_stopped`
/// intent.
pub fn admit_stop(conn: &Connection, id: &str) {
    let tx = immediate_transaction(conn).unwrap();
    update_state_superseding_activation_in_transaction(&tx, id, SandboxState::Stopping).unwrap();
    assert!(set_cleanup_intent(&tx, id, true, CleanupCompletion::MarkStopped).unwrap());
    tx.commit().unwrap();
}

/// Stop's row-intent completion of [`admit_stop`]: `stopping` to `stopped`.
pub fn complete_stop(conn: &Connection, id: &str) {
    complete_row_intent_cleanup_for_job(conn, id, CleanupCompletion::MarkStopped, None, None, None)
        .unwrap();
}

/// A non-blocking Stop from admission to completion.
pub fn stop(conn: &Connection, id: &str) {
    admit_stop(conn, id);
    complete_stop(conn, id);
}

pub const RESTORE_STORAGE_URI: &str = "s3://snapshots/suspend/restore?region=us-east-1";

/// The remote snapshot a seeded cross-node restore stages from.
pub fn restore_source() -> pplx_space_common_core::types::ExactStorageUri {
    RESTORE_STORAGE_URI.parse().unwrap()
}

/// Backdate every timestamp a writer may stamp, so a fresh stamp is visible.
pub fn backdate(conn: &Connection, id: &str) {
    conn.execute(
        "UPDATE sandboxes SET state_changed_at = '1', updated_at = '1' WHERE id = ?1",
        params![id],
    )
    .unwrap();
}

/// A live runtime with its spaced endpoint proof and complete route identity:
/// the columns a teardown writer clears and the route-retention trigger
/// observes.
pub const RUNTIME_ROUTE_GENERATION: i64 = 3;
pub const RUNTIME_EXECUTION_GENERATION: i64 = 7;
pub const RUNTIME_ACTIVATION_ATTEMPT: &str = "019a0000-0000-7000-8000-000000000001";
pub const RUNTIME_POD_UID: &str = "pod-uid-1";

pub fn seed_runtime(conn: &Connection, id: &str) {
    conn.execute(
        "UPDATE sandboxes
            SET uid = 2001, slot = 1, guest_cid = 3, pid = 4242,
                spaced_host_pid = 4243, netgtw_pid = 4244,
                vsock_uds_path = 'vsock.sock', api_socket_path = 'api.sock',
                guest_uptime_secs = 9,
                space_api_route_generation = ?2,
                space_api_route_node_id = '019a0000-0000-7000-8000-0000000000fe',
                space_api_route_execution_generation = ?3,
                space_api_route_pod_uid = ?4,
                space_api_route_substrate_kind = 'k8s',
                space_api_route_activation_attempt_id = ?5,
                space_api_route_identity_provenance = 'cluster_fenced',
                spaced_endpoint_proof_json = ?6
          WHERE id = ?1",
        params![
            id,
            RUNTIME_ROUTE_GENERATION,
            RUNTIME_EXECUTION_GENERATION,
            RUNTIME_POD_UID,
            RUNTIME_ACTIVATION_ATTEMPT,
            serde_json::to_string(&endpoint_proof(id.parse().unwrap())).unwrap(),
        ],
    )
    .unwrap();
}

/// The columns a write that clears [`seed_runtime`]'s runtime and route fields
/// changes, including the route-retention trigger's observation, with
/// `retained` naming the route columns the write keeps.
pub fn runtime_cleared(retained: &[&str]) -> Vec<(&'static str, Cell)> {
    [
        ("uid", NULL),
        ("slot", NULL),
        ("guest_cid", NULL),
        ("pid", NULL),
        ("spaced_host_pid", NULL),
        ("netgtw_pid", NULL),
        ("vsock_uds_path", NULL),
        ("api_socket_path", NULL),
        ("guest_uptime_secs", NULL),
        ("space_api_route_generation", NULL),
        ("space_api_route_node_id", NULL),
        ("space_api_route_execution_generation", NULL),
        ("space_api_route_pod_uid", NULL),
        ("space_api_route_substrate_kind", NULL),
        ("space_api_route_activation_attempt_id", NULL),
        ("space_api_route_identity_provenance", NULL),
        ("spaced_endpoint_proof_json", NULL),
        (
            "last_observed_execution_generation",
            int(RUNTIME_EXECUTION_GENERATION),
        ),
        (
            "last_observed_route_activation_id",
            text(RUNTIME_ACTIVATION_ATTEMPT),
        ),
        ("last_observed_route_substrate_kind", text("k8s")),
        (
            "last_observed_route_identity_provenance",
            text("cluster_fenced"),
        ),
        (
            "last_observed_route_generation",
            int(RUNTIME_ROUTE_GENERATION),
        ),
        ("last_observed_route_generation_substrate_kind", text("k8s")),
    ]
    .into_iter()
    .filter(|(name, _)| !retained.contains(name))
    .collect()
}

/// A transition receipt targeting `target`, in the kind the table's `CHECK`
/// admits for it: an activation completion for created, started and paused,
/// otherwise an activation rollback.
pub fn seed_receipt(conn: &Connection, id: &str, target: SandboxState) {
    let (kind, reason) = match target {
        SandboxState::Created | SandboxState::Started | SandboxState::Paused => {
            ("activation_completed", None)
        }
        SandboxState::Stopped | SandboxState::Suspended => {
            ("activation_rollback", Some("cold_activation_interrupted"))
        }
        other => panic!("{other} cannot be a transition receipt target"),
    };
    conn.execute(
        "UPDATE sandboxes
            SET transition_receipt_id = '019a0000-0000-7000-8000-0000000000aa',
                transition_receipt_kind = ?2,
                transition_receipt_target_state = ?3,
                transition_receipt_execution_generation = 5,
                transition_receipt_activation_attempt_id = '019a0000-0000-7000-8000-0000000000ab',
                transition_receipt_reason_code = ?4
          WHERE id = ?1",
        params![id, kind, target.as_str(), reason],
    )
    .unwrap();
}

/// The receipt columns a write that clears [`seed_receipt`]'s receipt for
/// `target` changes: an activation completion carries no reason code.
pub fn receipt_cleared(target: SandboxState) -> Vec<(&'static str, Cell)> {
    let mut cells = vec![
        ("transition_receipt_id", NULL),
        ("transition_receipt_kind", NULL),
        ("transition_receipt_target_state", NULL),
        ("transition_receipt_execution_generation", NULL),
        ("transition_receipt_activation_attempt_id", NULL),
    ];
    if matches!(target, SandboxState::Stopped | SandboxState::Suspended) {
        cells.push(("transition_receipt_reason_code", NULL));
    }
    cells
}

pub fn set_failure_reason(conn: &Connection, id: &str, reason: &str) {
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?2 WHERE id = ?1",
        params![id, reason],
    )
    .unwrap();
}

pub fn generation(value: i64) -> ExecutionGeneration {
    ExecutionGeneration::new(value).unwrap()
}

/// Run `write` and assert it changed exactly `expected`.
pub fn assert_accepts(case: &str, conn: &Connection, write: impl FnOnce(), expected: ExpectedDiff) {
    let before = snapshot(conn);
    let start = test_start();
    write();
    assert_diff(case, &before, &snapshot(conn), start, &expected);
}

pub fn snapshot_path(id: &str) -> String {
    format!("/cache/{id}/snapshot")
}

/// An active teardown-suspend job: a `deleting` claim must close it in the
/// same transaction.
pub fn seed_teardown_job(conn: &Connection, id: &str) {
    ensure_teardown_suspend_operation(conn, id, generation(7)).unwrap();
}

pub fn teardown_job_failed(id: &str, error: &str) -> ExpectedDiff {
    ExpectedDiff::new().updated(
        "jobs",
        ("subject_id", id),
        [
            ("status", text("failed")),
            ("error_kind", text("permanent")),
            ("error", text(error)),
            ("completed_at", Cell::Fresh),
        ],
    )
}

/// A suspend artifact row the writer inserts for `id`.
pub fn suspend_artifact_inserted(id: &str, artifact: LocalArtifactId) -> Vec<(&'static str, Cell)> {
    local_artifact_inserted(id, artifact, "full", "suspend", &snapshot_path(id))
}

pub fn local_artifact_inserted(
    id: &str,
    artifact: LocalArtifactId,
    kind: &str,
    origin: &str,
    path: &str,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("local_artifact_id", text(artifact.to_string())),
        ("sandbox_id", text(id)),
        ("artifact_kind", text(kind)),
        ("artifact_origin", text(origin)),
        ("snapshot_name", NULL),
        ("local_path", text(path)),
        ("created_at", Cell::NowText),
        ("last_touched_at", Cell::NowText),
        ("backup_completed_at", NULL),
        ("state", text("ready")),
        ("capture_time_comparable", int(1)),
    ]
}

pub fn pinned(artifact: LocalArtifactId, kind: &str, origin: &str) -> Vec<(&'static str, Cell)> {
    vec![
        ("current_local_artifact_id", text(artifact.to_string())),
        ("current_local_artifact_kind", text(kind)),
        ("current_local_artifact_origin", text(origin)),
    ]
}

pub fn unpinned() -> Vec<(&'static str, Cell)> {
    vec![
        ("current_local_artifact_id", NULL),
        ("current_local_artifact_kind", NULL),
        ("current_local_artifact_origin", NULL),
    ]
}

/// Pin a suspend artifact without its `local_artifacts` row, as a re-suspend's
/// predecessor carries it.
pub fn set_suspend_pin(conn: &Connection, id: &str) -> LocalArtifactId {
    let pin = LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        params![pin.to_string(), id],
    )
    .unwrap();
    pin
}

/// Arm a cold start of a `stopped` row, as Start admits it, returning the
/// activation's cleanup episode.
pub fn begin_cold_start(conn: &Connection, id: &str) -> CleanupEpisode {
    begin_activation_with_route_evidence(
        conn,
        id,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
        None,
        None,
        ActivationRouteAdmission::default(),
    )
    .unwrap()
    .unwrap()
}

/// A `suspended` row pinning its published suspend artifact.
pub fn insert_suspended_with_artifact(conn: &Connection) -> (String, LocalArtifactId) {
    let id = insert_row(conn, SandboxState::Paused);
    assert!(begin_suspend(conn, &id).unwrap());
    let artifact = LocalArtifactId::new();
    complete_row_intent_cleanup_for_job(
        conn,
        &id,
        CleanupCompletion::MarkSuspended,
        Some((artifact, &snapshot_path(&id))),
        None,
        None,
    )
    .unwrap();
    (id, artifact)
}

pub fn decorate(conn: &Connection, id: &str, receipt: SandboxState) {
    set_failure_reason(conn, id, "seeded failure");
    seed_receipt(conn, id, receipt);
    backdate(conn, id);
}

/// Build the insert parameters of an ordinary sandbox `id` in `state`, let
/// `customize` adjust them, and pass them to `insert`.
pub fn with_sandbox_params<T>(
    id: &str,
    state: SandboxState,
    customize: impl FnOnce(&mut InsertSandboxParams<'_>),
    insert: impl FnOnce(InsertSandboxParams<'_>) -> T,
) -> T {
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(id.parse().unwrap());
    let user = SandboxOwner::parse("user").unwrap();
    let env = BTreeMap::new();
    let lifecycle = AutoLifecyclePolicy::disabled();
    let mut params = InsertSandboxParams {
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
        process_env: &env,
        auto_lifecycle: &lifecycle,
        network_policy_override: None,
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Cluster,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
    };
    customize(&mut params);
    insert(params)
}

/// The synthetic row the orphan reconciler materializes for a cache it cannot
/// match to a sandbox.
pub fn as_synthetic_orphan(params: &mut InsertSandboxParams<'_>) {
    params.template = ORPHANED_SANDBOX_TEMPLATE;
    params.template_hash = None;
    params.user = &ORPHANED_SANDBOX_OWNER;
    params.runtime = RuntimeKind::Firecracker.alias();
}

/// A creating row owning an armed create episode with `completion`, bound to
/// `execution` when the create carries a route identity.
pub fn insert_creating_with(
    conn: &Connection,
    completion: CleanupCompletion,
    execution: Option<(ExecutionGeneration, SandboxActivationAttemptId)>,
) -> (String, SandboxCleanupEpisodeId) {
    let id = SandboxId::new().to_string();
    let cleanup_id = SandboxCleanupEpisodeId::new();
    with_sandbox_params(
        &id,
        SandboxState::Creating,
        |_| {},
        |params| {
            insert_sandbox_with_cleanup_episode_and_route_evidence(
                conn,
                params,
                CleanupEpisodeSpec {
                    id: cleanup_id,
                    completion,
                    keep_filesystem: false,
                    entered_from_state: None,
                    activation_state: SandboxState::Creating,
                    execution_generation: execution.map(|(generation, _)| generation),
                    activation_attempt_id: execution.map(|(_, attempt)| attempt),
                },
                None,
                None,
                None,
            )
        },
    )
    .unwrap();
    (id, cleanup_id)
}

pub fn insert_creating(
    conn: &Connection,
    completion: CleanupCompletion,
) -> (String, SandboxCleanupEpisodeId) {
    insert_creating_with(conn, completion, None)
}

/// Every column of a fresh `sandbox_quarantine_state` row.
pub fn quarantine_state_entered(
    id: &str,
    from: &str,
    reason: SandboxQuarantineReasonCode,
    message: &str,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("sandbox_id", text(id)),
        ("quarantine_id", Cell::Fresh),
        ("entered_at", Cell::NowText),
        ("updated_at", Cell::NowText),
        ("entered_from_state", text(from)),
        ("source", text("sbx_manager")),
        ("reason_code", text(reason.as_str())),
        ("reason_message", text(message)),
        ("evidence_json", text("{}")),
        ("next_action", text("requires_operator_attention")),
        ("next_action_after", NULL),
        ("next_action_owner", NULL),
        ("attempt_count", int(0)),
        ("claim_token", NULL),
        ("claim_expires_at", NULL),
        ("last_error", NULL),
    ]
}

/// Every column of the history row a fresh quarantine episode records.
pub fn quarantine_history_entered(
    id: &str,
    reason: SandboxQuarantineReasonCode,
    message: &str,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("id", Cell::Fresh),
        ("quarantine_id", Cell::Fresh),
        ("sandbox_id", text(id)),
        ("occurred_at", Cell::NowText),
        ("source", text("sbx_manager")),
        ("action", text("entered_quarantine")),
        ("reason_code", text(reason.as_str())),
        ("message", text(message)),
        ("next_action", text("requires_operator_attention")),
        ("next_action_after", NULL),
        ("attempt_count", NULL),
        ("claim_token", NULL),
        ("evidence_json", text("{}")),
        ("error", NULL),
    ]
}

/// A spaced endpoint proof for `sandbox_id`, matching [`seed_runtime`]'s
/// runtime and route identity.
pub fn endpoint_proof(sandbox_id: SandboxId) -> crate::endpoint_proof::SpacedEndpointProof {
    use crate::endpoint_proof::{
        ProcessActivationIdentity, SpacedEndpointProof, VsockActivationIdentity,
    };
    let process = |pid| ProcessActivationIdentity {
        pid: LinuxPid::try_from_u32(pid).unwrap(),
        start_time_ticks: 11,
        executable_device: 12,
        executable_inode: 13,
    };
    SpacedEndpointProof {
        sandbox_id,
        execution_generation: generation(RUNTIME_EXECUTION_GENERATION),
        activation_attempt_id: RUNTIME_ACTIVATION_ATTEMPT.parse().unwrap(),
        spaced_host: process(4243),
        runtime_process: process(4242),
        vsock: VsockActivationIdentity {
            path: "vsock.sock".into(),
            device: 31,
            inode: 32,
            owner_uid: LinuxUid::new(2001),
            owner_gid: 2001,
            guest_cid: VsockCid::try_from_u32(3).unwrap(),
            runtime_fd: 8,
            runtime_socket_inode: 33,
        },
    }
}

/// The execution metadata quarantine adoption records.
pub fn execution_metadata() -> crate::db::ExecutionMetadata {
    crate::db::ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![])
}
