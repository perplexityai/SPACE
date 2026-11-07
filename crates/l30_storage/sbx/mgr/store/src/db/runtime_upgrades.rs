//! `sandbox_runtime_upgrades`: node-local runtime upgrades and the activation
//! fence they hold.
//!
//! An upgrade cold-boots a stopped or suspended sandbox's disk under another
//! execution environment. Its row fences every other activation of the
//! sandbox from the moment the row is inserted until the boot is verified or
//! the old disk/checkpoint pair is restored. The verbs here move the row
//! through its phases with guarded writes; the service owns the filesystem
//! work between them.

use super::*;
use pplx_space_execution_spec::EnvironmentReference;

/// Caller-chosen identity of one upgrade; retries of the same upgrade reuse it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RuntimeUpgradeOperationId(uuid::Uuid);

impl RuntimeUpgradeOperationId {
    pub const fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }
}

impl fmt::Display for RuntimeUpgradeOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RuntimeUpgradeOperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Where an upgrade is. Only the forward edges in [`Self::can_advance_to`]
/// and the rollback edge from a fencing phase exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeUpgradePhase {
    /// The row exists; no disk has been preserved yet.
    Fenced,
    /// A read-only copy of the instance disk exists.
    Preserved,
    /// The target is the sandbox's consumer and a cold boot may run.
    Booting,
    /// The boot was verified; the old checkpoint still awaits retirement.
    Verified,
    /// The old pair was restored; the preserved disk still awaits deletion.
    RolledBack,
    /// The old checkpoint and preserved disk are gone.
    Retired,
}

impl RuntimeUpgradePhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fenced => "fenced",
            Self::Preserved => "preserved",
            Self::Booting => "booting",
            Self::Verified => "verified",
            Self::RolledBack => "rolled_back",
            Self::Retired => "retired",
        }
    }

    /// Whether a row in this phase refuses other activations of its sandbox.
    pub const fn fences_activation(self) -> bool {
        matches!(self, Self::Fenced | Self::Preserved | Self::Booting)
    }

    const fn can_advance_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Fenced, Self::Preserved)
                | (Self::Preserved, Self::Booting)
                | (Self::Booting, Self::Verified)
                | (Self::Verified, Self::Retired)
        )
    }
}

impl fmt::Display for RuntimeUpgradePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown runtime upgrade phase {0:?}")]
pub struct RuntimeUpgradePhaseParseError(String);

impl FromStr for RuntimeUpgradePhase {
    type Err = RuntimeUpgradePhaseParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "fenced" => Ok(Self::Fenced),
            "preserved" => Ok(Self::Preserved),
            "booting" => Ok(Self::Booting),
            "verified" => Ok(Self::Verified),
            "rolled_back" => Ok(Self::RolledBack),
            "retired" => Ok(Self::Retired),
            other => Err(RuntimeUpgradePhaseParseError(other.to_string())),
        }
    }
}

/// The lifecycle a verified upgrade leaves the sandbox in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeUpgradeTargetState {
    Started,
    Stopped,
}

impl RuntimeUpgradeTargetState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown runtime upgrade target state {0:?}")]
pub struct RuntimeUpgradeTargetStateParseError(String);

impl FromStr for RuntimeUpgradeTargetState {
    type Err = RuntimeUpgradeTargetStateParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "started" => Ok(Self::Started),
            "stopped" => Ok(Self::Stopped),
            other => Err(RuntimeUpgradeTargetStateParseError(other.to_string())),
        }
    }
}

/// The audited operator action an upgrade runs under.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeAuthorization {
    pub id: String,
    pub reason: String,
    /// The operator accepted discarding a suspended sandbox's memory.
    pub memory_loss: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeRecord {
    pub operation_id: RuntimeUpgradeOperationId,
    pub sandbox_id: String,
    pub target_environment: EnvironmentReference,
    pub target_state: RuntimeUpgradeTargetState,
    pub authorization: RuntimeUpgradeAuthorization,
    /// `stopped` or `suspended`: the state the old pair belongs to.
    pub source_state: SandboxState,
    /// The consumer a rollback restores: the sandbox's saved consumer, or for
    /// an unbound sandbox the execution its disk recorded. The boot rewrites
    /// the disk's record, so clearing the consumer again would leave the next
    /// cold start bound to the target.
    pub previous_execution: ExecutionMetadata,
    /// The suspend artifact the sandbox pinned when it was fenced.
    pub checkpoint_artifact_id: Option<LocalArtifactId>,
    /// Filesystem identity of the checkpoint directory when it was fenced,
    /// so retirement never takes a newer checkpoint at the same path.
    pub checkpoint_dir_identity: Option<String>,
    /// The sandbox execution the verified boot started, recorded with the
    /// verification. A later execution is someone else's to stop.
    pub boot_execution_id: Option<SandboxActivationAttemptId>,
    pub phase: RuntimeUpgradePhase,
    pub failure: Option<String>,
}

impl RuntimeUpgradeRecord {
    /// What the operation that wrote this row asked for.
    pub fn identity(&self) -> RuntimeUpgradeRequestIdentity {
        RuntimeUpgradeRequestIdentity {
            operation_id: self.operation_id,
            sandbox_id: self.sandbox_id.clone(),
            target_environment: self.target_environment,
            target_state: self.target_state,
            authorization_id: self.authorization.id.clone(),
        }
    }
}

pub struct NewRuntimeUpgrade<'a> {
    pub operation_id: RuntimeUpgradeOperationId,
    pub sandbox_id: &'a str,
    /// The sandbox as the caller inspected it. The fence is refused if its
    /// state, pin or saved consumer has changed since, because the caller's
    /// disk inspection and edge check described another sandbox.
    pub observed: ObservedSandbox<'a>,
    pub target_environment: EnvironmentReference,
    pub target_state: RuntimeUpgradeTargetState,
    pub authorization: &'a RuntimeUpgradeAuthorization,
    pub checkpoint_dir_identity: Option<&'a str>,
    /// The consumer a rollback restores; see
    /// [`RuntimeUpgradeRecord::previous_execution`].
    pub previous_execution: &'a ExecutionMetadata,
}

#[derive(Clone, Copy, Debug)]
pub struct ObservedSandbox<'a> {
    pub state: SandboxState,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    pub execution: Option<&'a ExecutionMetadata>,
}

impl<'a> ObservedSandbox<'a> {
    pub fn of(sandbox: &'a SandboxRow) -> Self {
        Self {
            state: sandbox.state,
            current_local_artifact_id: sandbox.current_local_artifact_id,
            execution: sandbox.execution_metadata.as_ref(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum BeginRuntimeUpgrade {
    Fenced(Box<RuntimeUpgradeRecord>),
    SandboxMissing,
    /// Only a stopped or suspended sandbox has a disk/checkpoint pair to keep.
    UnsupportedState(SandboxState),
    /// A cleanup episode owns the sandbox.
    CleanupActive,
    /// Another upgrade fences the sandbox.
    OtherOperation(RuntimeUpgradeOperationId),
    /// A row with this operation id already exists.
    OperationExists,
    /// The node recorded this operation id's outcome for another sandbox.
    OperationNamesAnother,
    /// A suspended sandbox's memory is lost, and the authorization didn't accept it.
    MemoryLossUnauthorized,
    /// The sandbox changed since the caller inspected it.
    SandboxChanged,
}

fn conversion_error(
    column: usize,
    kind: Type,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, kind, Box::new(error))
}

fn record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeUpgradeRecord> {
    let operation_id: String = row.get(0)?;
    let target_environment: String = row.get(2)?;
    let target_state: String = row.get(3)?;
    let source_state: String = row.get(7)?;
    let previous: String = row.get(8)?;
    let checkpoint: Option<String> = row.get(9)?;
    let phase: String = row.get(11)?;
    let boot_execution: Option<String> = row.get(13)?;
    Ok(RuntimeUpgradeRecord {
        operation_id: operation_id
            .parse()
            .map_err(|error| conversion_error(0, Type::Text, error))?,
        sandbox_id: row.get(1)?,
        target_environment: target_environment
            .parse()
            .map_err(|error| conversion_error(2, Type::Text, error))?,
        target_state: target_state
            .parse()
            .map_err(|error| conversion_error(3, Type::Text, error))?,
        authorization: RuntimeUpgradeAuthorization {
            id: row.get(4)?,
            reason: row.get(5)?,
            memory_loss: row.get(6)?,
        },
        source_state: source_state
            .parse()
            .map_err(|error| conversion_error(7, Type::Text, error))?,
        previous_execution: serde_json::from_str(&previous)
            .map_err(|error| conversion_error(8, Type::Text, error))?,
        checkpoint_artifact_id: checkpoint
            .map(|id| id.parse())
            .transpose()
            .map_err(|error| conversion_error(9, Type::Text, error))?,
        checkpoint_dir_identity: row.get(10)?,
        phase: phase
            .parse()
            .map_err(|error| conversion_error(11, Type::Text, error))?,
        failure: row.get(12)?,
        boot_execution_id: boot_execution
            .map(|id| id.parse())
            .transpose()
            .map_err(|error| conversion_error(13, Type::Text, error))?,
    })
}

/// The columns [`record_from_row`] decodes, in its order. Only ever the whole
/// `SELECT` list of a statement over `sandbox_runtime_upgrades` alone.
const RECORD_COLUMNS: &str = "operation_id, sandbox_id, target_environment, target_state, \
    authorization_id, authorization_reason, memory_loss_authorized, source_state, \
    previous_execution_metadata_json, checkpoint_artifact_id, checkpoint_dir_identity, \
    phase, failure, boot_execution_id";

db_private_outside_tests! {
    fn runtime_upgrade(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradeRecord>> {
        static BY_OPERATION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "SELECT {RECORD_COLUMNS} FROM sandbox_runtime_upgrades WHERE operation_id = ?1"
            )
        });
        conn.query_row(
            &BY_OPERATION,
            params![operation_id.to_string()],
            record_from_row,
        )
        .optional()
    }
}

db_private_outside_tests! {
    /// The upgrade that fences `sandbox_id`'s activations, if any. The phases
    /// listed are those [`RuntimeUpgradePhase::fences_activation`] names.
    fn fencing_runtime_upgrade(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<(RuntimeUpgradeOperationId, RuntimeUpgradePhase)>> {
        let row: Option<(String, String)> = conn
            .query_row(
                "SELECT operation_id, phase
                   FROM sandbox_runtime_upgrades
                  WHERE sandbox_id = ?1
                    AND phase IN ('fenced', 'preserved', 'booting')
                  ORDER BY created_at, operation_id
                  LIMIT 1",
                params![sandbox_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(operation_id, phase)| {
            Ok((
                operation_id
                    .parse()
                    .map_err(|error| conversion_error(0, Type::Text, error))?,
                phase
                    .parse()
                    .map_err(|error| conversion_error(1, Type::Text, error))?,
            ))
        })
        .transpose()
    }
}

db_private_outside_tests! {
    /// The upgrade of `sandbox_id` that hasn't retired the checkpoint it
    /// superseded yet, if any: a fencing one, or a verified one whose
    /// retirement is pending.
    fn unretired_runtime_upgrade(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<RuntimeUpgradeOperationId>> {
        let row: Option<String> = conn
            .query_row(
                "SELECT operation_id
                   FROM sandbox_runtime_upgrades
                  WHERE sandbox_id = ?1
                    AND phase IN ('fenced', 'preserved', 'booting', 'verified')
                  ORDER BY created_at, operation_id
                  LIMIT 1",
                params![sandbox_id],
                |row| row.get(0),
            )
            .optional()?;
        row.map(|operation_id| {
            operation_id
                .parse()
                .map_err(|error| conversion_error(0, Type::Text, error).into())
        })
        .transpose()
    }
}

db_private_outside_tests! {
    /// Every upgrade that still owes work: everything but `retired`. A row
    /// that doesn't decode is skipped with a warning, so it can't keep
    /// recovery from the others; its fence stays in place.
    fn unsettled_runtime_upgrades(conn: &Connection) -> Result<Vec<RuntimeUpgradeRecord>> {
        static UNSETTLED: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "SELECT {RECORD_COLUMNS}
                   FROM sandbox_runtime_upgrades
                  WHERE phase != 'retired'
                  ORDER BY created_at, operation_id"
            )
        });
        let mut statement = conn.prepare(&UNSETTLED)?;
        let mut rows = statement.query([])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            match record_from_row(row) {
                Ok(record) => records.push(record),
                Err(error) if is_row_decode_error(&error) => tracing::warn!(
                    operation_id = ?row.get_ref(0)?,
                    error = %describe_raw_error(&error),
                    "skipping a runtime upgrade row that doesn't decode; operator repair required"
                ),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(records)
    }
}

/// Why an activation may not claim a sandbox a runtime upgrade fences.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeUpgradeFenceError {
    /// Another runtime upgrade fences the sandbox, or this activation claims
    /// an upgrade that isn't the fencing one or isn't booting yet.
    #[error("runtime upgrade {operation_id} owns sandbox activation")]
    Fenced {
        operation_id: RuntimeUpgradeOperationId,
    },
    /// The activation claims an upgrade that no longer fences the sandbox.
    #[error("runtime upgrade {operation_id} no longer fences this sandbox")]
    NotFencing {
        operation_id: RuntimeUpgradeOperationId,
    },
}

/// The fence an activation of `sandbox_id` must pass, read inside the
/// activation's own claim transaction. Only `claimed`, when it names the
/// fencing upgrade in its `booting` phase, passes a fence.
pub(super) fn admit_past_runtime_upgrade_fence(
    conn: &Connection,
    sandbox_id: &str,
    claimed: Option<RuntimeUpgradeOperationId>,
) -> Result<std::result::Result<(), RuntimeUpgradeFenceError>> {
    Ok(
        match (fencing_runtime_upgrade(conn, sandbox_id)?, claimed) {
            (None, None) => Ok(()),
            (Some((fence, RuntimeUpgradePhase::Booting)), Some(claimed)) if fence == claimed => {
                Ok(())
            }
            (Some((operation_id, _)), _) => Err(RuntimeUpgradeFenceError::Fenced { operation_id }),
            (None, Some(operation_id)) => {
                Err(RuntimeUpgradeFenceError::NotFencing { operation_id })
            }
        },
    )
}

/// Insert a `fenced` row for a stopped or suspended sandbox that nothing else
/// owns. The row, the observed state and the absence of any cleanup or other
/// upgrade are decided in one transaction, so no activation can claim the
/// sandbox between the check and the fence.
pub fn begin_runtime_upgrade(
    conn: &Connection,
    new: NewRuntimeUpgrade<'_>,
) -> Result<BeginRuntimeUpgrade> {
    let tx = immediate_transaction(conn)?;
    if runtime_upgrade(&tx, new.operation_id)?.is_some() {
        return Ok(BeginRuntimeUpgrade::OperationExists);
    }
    // An operation id names one upgrade of one sandbox, across both tables.
    if recorded_runtime_upgrade_request(&tx, new.operation_id)?
        .is_some_and(|recorded| recorded.sandbox_id != new.sandbox_id)
    {
        return Ok(BeginRuntimeUpgrade::OperationNamesAnother);
    }
    let Some(sandbox) = get_sandbox(&tx, new.sandbox_id)? else {
        return Ok(BeginRuntimeUpgrade::SandboxMissing);
    };
    if !matches!(
        sandbox.state,
        SandboxState::Stopped | SandboxState::Suspended
    ) {
        return Ok(BeginRuntimeUpgrade::UnsupportedState(sandbox.state));
    }
    if sandbox.state != new.observed.state
        || sandbox.current_local_artifact_id != new.observed.current_local_artifact_id
        || sandbox.execution_metadata.as_ref() != new.observed.execution
    {
        return Ok(BeginRuntimeUpgrade::SandboxChanged);
    }
    if sandbox.state == SandboxState::Suspended && !new.authorization.memory_loss {
        return Ok(BeginRuntimeUpgrade::MemoryLossUnauthorized);
    }
    if sandbox.cleanup_completion.is_some() || cleanup_episode(&tx, new.sandbox_id)?.is_some() {
        return Ok(BeginRuntimeUpgrade::CleanupActive);
    }
    if let Some((other, _)) = fencing_runtime_upgrade(&tx, new.sandbox_id)? {
        return Ok(BeginRuntimeUpgrade::OtherOperation(other));
    }
    let checkpoint_artifact_id = (sandbox.state == SandboxState::Suspended
        && sandbox.current_local_artifact_kind == Some(ArtifactKind::Full)
        && sandbox.current_local_artifact_origin == Some(ArtifactOrigin::Suspend))
    .then_some(sandbox.current_local_artifact_id)
    .flatten();
    let previous_execution = serde_json::to_string(new.previous_execution)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let now = now()?;
    tx.execute(
        "INSERT INTO sandbox_runtime_upgrades (
             operation_id, sandbox_id, target_environment, target_state,
             authorization_id, authorization_reason, memory_loss_authorized,
             source_state, previous_execution_metadata_json, checkpoint_artifact_id,
             checkpoint_dir_identity, phase, failure, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'fenced', NULL, ?12, ?12)",
        params![
            new.operation_id.to_string(),
            new.sandbox_id,
            new.target_environment.to_string(),
            new.target_state.as_str(),
            new.authorization.id,
            new.authorization.reason,
            new.authorization.memory_loss,
            sandbox.state.as_str(),
            previous_execution,
            checkpoint_artifact_id.map(|id| id.to_string()),
            new.checkpoint_dir_identity,
            now,
        ],
    )?;
    let record = runtime_upgrade(&tx, new.operation_id)?.ok_or_else(Error::no_rows)?;
    tx.commit()?;
    Ok(BeginRuntimeUpgrade::Fenced(Box::new(record)))
}

/// Move an upgrade one forward edge. A false result means the row is gone or
/// another writer moved it first.
pub fn advance_runtime_upgrade(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    from: RuntimeUpgradePhase,
    to: RuntimeUpgradePhase,
) -> Result<bool> {
    if !from.can_advance_to(to) {
        return Err(Error::invalid_query());
    }
    let changed = if to == RuntimeUpgradePhase::Verified {
        // Verified outside the start's own transaction: the execution running
        // now is the boot's, since the fence admitted no other.
        conn.execute(
            "UPDATE sandbox_runtime_upgrades
                SET phase = ?3,
                    boot_execution_id = (
                        SELECT node_execution_id FROM sandboxes
                         WHERE sandboxes.id = sandbox_runtime_upgrades.sandbox_id
                    ),
                    updated_at = ?4
              WHERE operation_id = ?1
                AND phase = ?2",
            params![operation_id.to_string(), from.as_str(), to.as_str(), now()?],
        )?
    } else {
        conn.execute(
            "UPDATE sandbox_runtime_upgrades
                SET phase = ?3, updated_at = ?4
              WHERE operation_id = ?1
                AND phase = ?2",
            params![operation_id.to_string(), from.as_str(), to.as_str(), now()?],
        )?
    };
    Ok(changed == 1)
}

/// Save `consumer` as the sandbox's execution and move a `preserved` upgrade
/// to `booting`, together. The cold boot that follows reads the saved
/// consumer, so a boot can't start under an environment the row doesn't name.
pub fn bind_runtime_upgrade_consumer(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    consumer: &ExecutionMetadata,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(record) = runtime_upgrade(&tx, operation_id)? else {
        return Ok(false);
    };
    if record.phase != RuntimeUpgradePhase::Preserved
        || consumer.environment() != Some(record.target_environment)
    {
        return Ok(false);
    }
    upsert_sandbox_execution_metadata_in_transaction(&tx, &record.sandbox_id, consumer)?;
    let advanced = advance_runtime_upgrade(
        &tx,
        operation_id,
        RuntimeUpgradePhase::Preserved,
        RuntimeUpgradePhase::Booting,
    )?;
    if !advanced {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

/// Restore the sandbox's previous consumer and move a fencing upgrade to
/// `rolled_back`, releasing the fence. The caller restores the disk first.
pub fn roll_back_runtime_upgrade(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    failure: &str,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(record) = runtime_upgrade(&tx, operation_id)? else {
        return Ok(false);
    };
    if !record.phase.fences_activation() {
        return Ok(false);
    }
    upsert_sandbox_execution_metadata_in_transaction(
        &tx,
        &record.sandbox_id,
        &record.previous_execution,
    )?;
    let changed = tx.execute(
        "UPDATE sandbox_runtime_upgrades
            SET phase = 'rolled_back', failure = ?3, updated_at = ?4
          WHERE operation_id = ?1
            AND phase = ?2",
        params![
            operation_id.to_string(),
            record.phase.as_str(),
            failure,
            now()?
        ],
    )?;
    if changed != 1 {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

/// Record a `booting` upgrade of `sandbox_id` as verified, inside the
/// transaction that marks its cold start started. While the fence stands only
/// the upgrade's own boot can start the sandbox, and it is marked started only
/// once its guest is ready, so the boot and its verification commit together:
/// no later stop or suspend can make a verified boot look interrupted.
pub(super) fn verify_booting_runtime_upgrade(conn: &Connection, sandbox_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandbox_runtime_upgrades
            SET phase = 'verified',
                boot_execution_id = (SELECT node_execution_id FROM sandboxes WHERE id = ?1),
                updated_at = ?2
          WHERE sandbox_id = ?1
            AND phase = 'booting'",
        params![sandbox_id, now()?],
    )?;
    Ok(())
}

/// Delete a `rolled_back` row once its preserved disk is gone, keeping its
/// failure as the operation's [`RecordedRuntimeUpgradeOutcome::RolledBack`] outcome.
pub fn finish_runtime_upgrade_rollback(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    tx.execute(
        "INSERT INTO sandbox_runtime_upgrade_outcomes (
             operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at
         )
         SELECT operation_id, sandbox_id, ?2, target_environment, target_state,
                authorization_id, failure, ?3
           FROM sandbox_runtime_upgrades
          WHERE operation_id = ?1
            AND phase = 'rolled_back'
         ON CONFLICT (operation_id) DO UPDATE
            SET outcome = excluded.outcome,
                failure = excluded.failure,
                recorded_at = excluded.recorded_at
          WHERE sandbox_id = excluded.sandbox_id",
        params![
            operation_id.to_string(),
            RecordedRuntimeUpgradeOutcome::RolledBack.as_str(),
            now()?
        ],
    )?;
    let changed = tx.execute(
        "DELETE FROM sandbox_runtime_upgrades
          WHERE operation_id = ?1
            AND phase = 'rolled_back'",
        params![operation_id.to_string()],
    )?;
    if changed != 1 {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

/// What one upgrade operation asks for. A retry under the same operation
/// must ask for the same, whatever the node recorded of it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeRequestIdentity {
    pub operation_id: RuntimeUpgradeOperationId,
    pub sandbox_id: String,
    pub target_environment: EnvironmentReference,
    pub target_state: RuntimeUpgradeTargetState,
    pub authorization_id: String,
}

/// Record that the operation `request` names found its sandbox already on
/// its target and fenced nothing. False, recording nothing, when the node
/// already recorded the operation, as an outcome or a live upgrade, for
/// another sandbox, or as an outcome for another target, target state or
/// authorization.
/// The check and the write are one statement, and `begin_runtime_upgrade`
/// checks this table in its transaction, so two requests reusing one
/// operation id can't both succeed.
pub fn record_runtime_upgrade_already_on_target(
    conn: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
) -> Result<bool> {
    let changed = conn.execute(
        "INSERT INTO sandbox_runtime_upgrade_outcomes (
             operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at
         )
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7
          WHERE NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrades
                 WHERE operation_id = ?1
                   AND sandbox_id != ?2
          )
         ON CONFLICT (operation_id) DO UPDATE
            SET outcome = excluded.outcome,
                failure = NULL,
                recorded_at = excluded.recorded_at
          WHERE sandbox_id = excluded.sandbox_id
            AND target_environment = excluded.target_environment
            AND target_state = excluded.target_state
            AND authorization_id = excluded.authorization_id",
        params![
            request.operation_id.to_string(),
            request.sandbox_id,
            RecordedRuntimeUpgradeOutcome::AlreadyOnTarget.as_str(),
            request.target_environment.to_string(),
            request.target_state.as_str(),
            request.authorization_id,
            now()?
        ],
    )?;
    Ok(changed == 1)
}

db_private_outside_tests! {
    /// What the operation a recorded outcome belongs to asked for, `None`
    /// without a recorded outcome.
    fn recorded_runtime_upgrade_request(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradeRequestIdentity>> {
        let row: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT sandbox_id, target_environment, target_state, authorization_id
                   FROM sandbox_runtime_upgrade_outcomes
                  WHERE operation_id = ?1",
                params![operation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        row.map(|(sandbox_id, target_environment, target_state, authorization_id)| {
            Ok(RuntimeUpgradeRequestIdentity {
                operation_id,
                sandbox_id,
                target_environment: target_environment
                    .parse()
                    .map_err(|error| conversion_error(1, Type::Text, error))?,
                target_state: target_state
                    .parse()
                    .map_err(|error| conversion_error(2, Type::Text, error))?,
                authorization_id,
            })
        })
        .transpose()
    }
}

/// Record how `sandbox_id`'s already-on-target upgrade claims its
/// activation: a start claimed before it runs, or an answer with the
/// execution already running. False when no such outcome is recorded, or an
/// activation is already claimed.
pub fn claim_runtime_upgrade_activation(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    claim: RuntimeUpgradeActivationClaim,
) -> Result<bool> {
    let from_execution = match claim {
        RuntimeUpgradeActivationClaim::Start { from_execution } => from_execution,
        RuntimeUpgradeActivationClaim::Attach => None,
    };
    let changed = conn.execute(
        "UPDATE sandbox_runtime_upgrade_outcomes
            SET activation_claimed_at = ?3,
                activation_claim_kind = ?6,
                activation_claimed_execution_id = ?5
          WHERE operation_id = ?1
            AND sandbox_id = ?2
            AND outcome = ?4
            AND activation_claimed_at IS NULL",
        params![
            operation_id.to_string(),
            sandbox_id,
            now()?,
            RecordedRuntimeUpgradeOutcome::AlreadyOnTarget.as_str(),
            from_execution.map(|id| id.to_string()),
            claim.kind(),
        ],
    )?;
    Ok(changed == 1)
}

/// The outcome of an upgrade that left no row in `sandbox_runtime_upgrades`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordedRuntimeUpgradeOutcome {
    /// The old pair was restored and the preserved disk deleted.
    RolledBack,
    /// The sandbox was already on the target; nothing was fenced or booted.
    AlreadyOnTarget,
}

impl RecordedRuntimeUpgradeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RolledBack => "rolled_back",
            Self::AlreadyOnTarget => "already_on_target",
        }
    }
}

impl fmt::Display for RecordedRuntimeUpgradeOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown runtime upgrade outcome {0:?}")]
pub struct RecordedRuntimeUpgradeOutcomeParseError(String);

impl FromStr for RecordedRuntimeUpgradeOutcome {
    type Err = RecordedRuntimeUpgradeOutcomeParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "rolled_back" => Ok(Self::RolledBack),
            "already_on_target" => Ok(Self::AlreadyOnTarget),
            other => Err(RecordedRuntimeUpgradeOutcomeParseError(other.to_string())),
        }
    }
}

/// Where one upgrade stands on this node, for a caller that may have lost the
/// `UpgradeRuntime` response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeReport {
    pub sandbox_id: String,
    pub status: RuntimeUpgradeStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeUpgradeStatus {
    /// The upgrade still owes work: it fences the sandbox, or its verified
    /// boot hasn't retired the old checkpoint yet.
    InProgress(RuntimeUpgradePhase),
    /// The sandbox booted under the target and the old checkpoint is retired.
    Upgraded,
    /// The old pair is back. The preserved copy may still await deletion.
    RolledBack { failure: Option<String> },
    /// Nothing was fenced or booted. `activation` once the upgrade claimed
    /// its activation for a started target; a claimed start may still have
    /// failed.
    AlreadyOnTarget {
        activation: Option<RuntimeUpgradeActivationClaim>,
    },
}

/// How an already-on-target upgrade into the started lifecycle claimed its
/// activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeUpgradeActivationClaim {
    /// It claimed the start of the sandbox, which then had `from_execution`.
    /// While the sandbox still has it, nothing has started it since.
    Start {
        from_execution: Option<SandboxActivationAttemptId>,
    },
    /// It answered with the execution already running. No retry of the
    /// operation starts the sandbox.
    Attach,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown runtime upgrade activation claim {0:?}")]
struct ActivationClaimKindParseError(String);

impl RuntimeUpgradeActivationClaim {
    const fn kind(self) -> &'static str {
        match self {
            Self::Start { .. } => "start",
            Self::Attach => "attach",
        }
    }
}

db_private_outside_tests! {
    /// The report for `operation_id`, `None` when the node holds no record of
    /// it. The live row, when there is one, wins over a recorded outcome: a
    /// retry after a rollback starts over under the same operation.
    fn runtime_upgrade_report(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradeReport>> {
        if let Some(record) = runtime_upgrade(conn, operation_id)? {
            let status = match record.phase {
                RuntimeUpgradePhase::Retired => RuntimeUpgradeStatus::Upgraded,
                RuntimeUpgradePhase::RolledBack => RuntimeUpgradeStatus::RolledBack {
                    failure: record.failure,
                },
                phase => RuntimeUpgradeStatus::InProgress(phase),
            };
            return Ok(Some(RuntimeUpgradeReport {
                sandbox_id: record.sandbox_id,
                status,
            }));
        }
        type OutcomeRow = (String, String, Option<String>, Option<String>, Option<String>);
        let row: Option<OutcomeRow> = conn
            .query_row(
                "SELECT sandbox_id, outcome, failure, activation_claim_kind,
                        activation_claimed_execution_id
                   FROM sandbox_runtime_upgrade_outcomes
                  WHERE operation_id = ?1",
                params![operation_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()?;
        row.map(|(sandbox_id, outcome, failure, claim_kind, claimed_execution)| {
            let from_execution = claimed_execution
                .map(|id| id.parse())
                .transpose()
                .map_err(|error| conversion_error(4, Type::Text, error))?;
            let activation = match claim_kind.as_deref() {
                None => None,
                Some("start") => Some(RuntimeUpgradeActivationClaim::Start { from_execution }),
                Some("attach") => Some(RuntimeUpgradeActivationClaim::Attach),
                Some(other) => {
                    return Err(conversion_error(
                        3,
                        Type::Text,
                        ActivationClaimKindParseError(other.to_string()),
                    )
                    .into());
                }
            };
            let status = match outcome
                .parse()
                .map_err(|error| conversion_error(1, Type::Text, error))?
            {
                RecordedRuntimeUpgradeOutcome::RolledBack => RuntimeUpgradeStatus::RolledBack { failure },
                RecordedRuntimeUpgradeOutcome::AlreadyOnTarget => {
                    RuntimeUpgradeStatus::AlreadyOnTarget { activation }
                }
            };
            Ok(RuntimeUpgradeReport { sandbox_id, status })
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests;
